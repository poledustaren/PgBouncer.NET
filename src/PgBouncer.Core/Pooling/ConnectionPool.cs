using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;

namespace PgBouncer.Core.Pooling;

public class ConnectionPool : IConnectionPool
{
    private readonly string _database;
    private readonly string _username;
    private readonly string _password;
    private readonly BackendConfig _backendConfig;
    private readonly PoolConfig _poolConfig;
    private readonly ILogger? _logger;

    private readonly LinkedList<IServerConnection> _idleConnections = new();
    private readonly object _lock = new();
    private readonly SemaphoreSlim _poolSemaphore;
    
    private readonly CircuitBreaker _circuitBreaker;
    private readonly Timer? _idleCleanupTimer;

    private int _totalConnections;
    private long _totalAcquired;
    private long _totalReleased;
    private bool _disposed;

    public ConnectionPool(
        string database,
        string username,
        string password,
        BackendConfig backendConfig,
        PoolConfig poolConfig,
        ILogger? logger = null)
    {
        _database = database;
        _username = username;
        _password = password;
        _backendConfig = backendConfig;
        _poolConfig = poolConfig;
        _logger = logger;

        _poolSemaphore = new SemaphoreSlim(poolConfig.MaxSize, poolConfig.MaxSize);

        _circuitBreaker = new CircuitBreaker(
            failureThreshold: 3,
            resetTimeoutSeconds: 30);

        if (poolConfig.IdleTimeout > 0)
        {
            _idleCleanupTimer = new Timer(
                CleanupIdleConnections,
                null,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(30));
        }
    }

    public async Task InitializeAsync(int minConnections, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Initializing pool with {MinConnections} minimum connections...", minConnections);
        
        for (int i = 0; i < minConnections; i++)
        {
            try
            {
                await _poolSemaphore.WaitAsync(cancellationToken);
                
                var connection = await CreateConnectionAsync(cancellationToken);
                
                lock (_lock)
                {
                    _idleConnections.AddFirst(connection);
                    _totalConnections++;
                }

                _poolSemaphore.Release();
                _logger?.LogInformation("Connection {Id} initialized", connection.Id);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to create initial connection");
                if (_poolSemaphore.CurrentCount < _poolConfig.MaxSize)
                    _poolSemaphore.Release();
            }
        }
    }

    public async Task<IServerConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _totalAcquired);

        bool acquired = await _poolSemaphore.WaitAsync(_poolConfig.ConnectionTimeout * 1000, cancellationToken);
        if (!acquired)
        {
            throw new TimeoutException($"Timed out waiting for connection from pool (limit {_poolConfig.MaxSize})");
        }

        IServerConnection? connection = null;
        bool createdNew = false;

        try
        {
            lock (_lock)
            {
                if (_idleConnections.Count > 0)
                {
                    connection = _idleConnections.First!.Value;
                    _idleConnections.RemoveFirst();
                }
                else
                {
                    if (_totalConnections < _poolConfig.MaxSize)
                    {
                        _totalConnections++;
                        createdNew = true;
                    }
                    else
                    {
                        throw new InvalidOperationException("Pool consistency error");
                    }
                }
            }

            if (createdNew)
            {
                try
                {
                    connection = await CreateConnectionAsync(cancellationToken);
                }
                catch
                {
                    lock (_lock) { _totalConnections--; }
                    throw;
                }
            }
            else if (connection != null)
            {
                if (!connection.IsHealthy || !_circuitBreaker.IsAllowed(connection.Id))
                {
                    await connection.DisposeAsync();
                    lock (_lock) { _totalConnections--; }

                    try
                    {
                        lock (_lock) { _totalConnections++; }
                        connection = await CreateConnectionAsync(cancellationToken);
                    }
                    catch
                    {
                         lock (_lock) { _totalConnections--; }
                         throw;
                    }
                }
                else
                {
                    connection.UpdateActivity();
                }
            }
            else
            {
                 throw new InvalidOperationException("Failed to acquire connection (null)");
            }

            return connection!;
        }
        catch
        {
            _poolSemaphore.Release();
            throw;
        }
    }

    public void Release(IServerConnection connection)
    {
        _ = ReleaseAsyncInternal(connection);
    }

    public ValueTask ReleaseAsync(IServerConnection connection)
    {
        return new ValueTask(ReleaseAsyncInternal(connection));
    }

    private async Task ReleaseAsyncInternal(IServerConnection connection)
    {
        Interlocked.Increment(ref _totalReleased);

        try
        {
            if (connection.IsBroken || !connection.IsHealthy || _disposed)
            {
                await DestroyConnectionAsync(connection);
                return;
            }

            if (!string.IsNullOrEmpty(_poolConfig.ServerResetQuery))
            {
                try
                {
                    await connection.ExecuteResetQueryAsync();
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Failed to reset connection {Id}: {Message}", connection.Id, ex.Message);
                    await DestroyConnectionAsync(connection);
                    return;
                }
            }

            connection.UpdateActivity();

            lock (_lock)
            {
                if (_totalConnections > _poolConfig.MaxSize)
                {
                     // Pool overflow
                }
                else
                {
                    _idleConnections.AddFirst(connection);
                    _poolSemaphore.Release();
                    return;
                }
            }

            await DestroyConnectionAsync(connection);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error releasing connection {Id}", connection.Id);
        }
    }

    private async Task DestroyConnectionAsync(IServerConnection connection)
    {
        lock (_lock)
        {
            _totalConnections--;
        }

        await connection.DisposeAsync();
        _poolSemaphore.Release();
    }

    private async Task<IServerConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true,
            SendBufferSize = 64 * 1024,
            ReceiveBufferSize = 64 * 1024
        };

        try
        {
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_poolConfig.ConnectionTimeout));
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            await socket.ConnectAsync(_backendConfig.Host, _backendConfig.Port, linkedCts.Token);

            var connection = new BackendConnection(socket, _database, _username, _logger);
            await connection.ConnectAndAuthenticateAsync(_password, linkedCts.Token);

            return connection;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private void CleanupIdleConnections(object? state)
    {
        if (_disposed) return;

        var idleTimeout = _poolConfig.IdleTimeout;
        if (idleTimeout <= 0) return;

        var toRemove = new List<IServerConnection>();

        lock (_lock)
        {
            var node = _idleConnections.Last;
            while (node != null)
            {
                var conn = node.Value;
                var prev = node.Previous;

                if (conn.IsIdle(idleTimeout))
                {
                    if (_poolSemaphore.Wait(0))
                    {
                        _idleConnections.Remove(node);
                        toRemove.Add(conn);
                    }
                }
                else
                {
                    break;
                }

                node = prev;
            }
        }

        if (toRemove.Count > 0)
        {
            _logger?.LogInformation("Closing {Count} idle connections", toRemove.Count);

            _ = Task.Run(async () =>
            {
                foreach (var conn in toRemove)
                {
                    await DestroyConnectionAsync(conn);
                }
            });
        }
    }

    public void RecordSuccess(Guid connectionId) => _circuitBreaker.RecordSuccess(connectionId);
    public void RecordFailure(Guid connectionId) => _circuitBreaker.RecordFailure(connectionId);

    public void Dispose()
    {
        DisposeAsync().AsTask().Wait();
    }

    public PoolStats GetStats()
    {
        lock (_lock)
        {
             return new PoolStats
             {
                 Database = _database,
                 Username = _username,
                 TotalConnections = _totalConnections,
                 ActiveConnections = _totalConnections - _idleConnections.Count,
                 IdleConnections = _idleConnections.Count,
                 MaxConnections = _poolConfig.MaxSize
             };
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _idleCleanupTimer?.Dispose();
        
        var toDispose = new List<IServerConnection>();
        lock (_lock)
        {
            toDispose.AddRange(_idleConnections);
            _idleConnections.Clear();
        }

        foreach (var conn in toDispose)
        {
            await conn.DisposeAsync();
        }

        _poolSemaphore.Dispose();
    }
}
