using System.Threading.Channels;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;

namespace PgBouncer.Core.Pooling;

public class ChannelConnectionPool : IConnectionPool
{
    private readonly string _database;
    private readonly string _username;
    private readonly string _password;
    private readonly BackendConfig _backendConfig;
    private readonly PoolConfig _poolConfig;
    private readonly ILogger? _logger;
    private readonly Channel<IServerConnection> _idleChannel;
    private readonly CircuitBreaker _circuitBreaker;

    private int _totalConnections;
    private int _idleCount;
    private volatile bool _disposed;

    public ChannelConnectionPool(
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

        var options = new BoundedChannelOptions(poolConfig.MaxSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        };
        _idleChannel = Channel.CreateBounded<IServerConnection>(options);

        _circuitBreaker = new CircuitBreaker(
            failureThreshold: 3,
            resetTimeoutSeconds: 30);
    }

    public async Task InitializeAsync(int minConnections, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Initializing pool with {MinConnections} connections...", minConnections);
        for (int i = 0; i < minConnections; i++)
        {
            if (TryReserveSlot())
            {
                try
                {
                    var conn = await CreateConnectionAsync(cancellationToken);
                    Interlocked.Increment(ref _idleCount);
                    if (!_idleChannel.Writer.TryWrite(conn))
                    {
                        Interlocked.Decrement(ref _idleCount);
                        await DestroyConnectionAsync(conn);
                    }
                }
                catch (Exception ex)
                {
                    ReleaseSlot();
                    _logger?.LogError(ex, "Failed to create initial connection");
                }
            }
        }
    }

    public async Task<IServerConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        IServerConnection? conn;

        // 1. Try to get an idle connection immediately
        if (_idleChannel.Reader.TryRead(out conn))
        {
            Interlocked.Decrement(ref _idleCount);
            if (CheckHealth(conn)) return conn;

            await DestroyConnectionAsync(conn);
            // Fallthrough to creation logic
        }

        // 2. Try to create a new connection if we have capacity
        if (TryReserveSlot())
        {
            try
            {
                return await CreateConnectionAsync(cancellationToken);
            }
            catch
            {
                ReleaseSlot();
                throw;
            }
        }

        // 3. Wait for an available connection from the channel
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_poolConfig.ConnectionTimeout));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            while (true)
            {
                conn = await _idleChannel.Reader.ReadAsync(linkedCts.Token);
                Interlocked.Decrement(ref _idleCount);

                if (CheckHealth(conn)) return conn;

                await DestroyConnectionAsync(conn);

                // Re-check slot availability after destroying unhealthy connection
                if (TryReserveSlot())
                {
                    try
                    {
                        return await CreateConnectionAsync(cancellationToken);
                    }
                    catch
                    {
                        ReleaseSlot();
                        // Loop back to wait
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            if (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"Timed out waiting for connection from pool (limit {_poolConfig.MaxSize})");
            }
            throw;
        }
    }

    private bool CheckHealth(IServerConnection conn)
    {
        // conn.IsHealthy includes strict socket poll check
        if (conn.IsHealthy && _circuitBreaker.IsAllowed(conn.Id))
        {
            return true;
        }
        return false;
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
        if (_disposed)
        {
            await DestroyConnectionAsync(connection);
            return;
        }

        try
        {
            if (!connection.IsHealthy || connection.IsBroken)
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

            Interlocked.Increment(ref _idleCount);
            if (_idleChannel.Writer.TryWrite(connection))
            {
                return;
            }

            // Channel full or closed
            Interlocked.Decrement(ref _idleCount);
            _logger?.LogWarning("Pool channel full, destroying connection {Id}", connection.Id);
            await DestroyConnectionAsync(connection);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error releasing connection {Id}", connection.Id);
            await DestroyConnectionAsync(connection);
        }
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
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
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

    private async Task DestroyConnectionAsync(IServerConnection connection)
    {
        try
        {
            await connection.DisposeAsync();
        }
        catch { }
        finally
        {
            ReleaseSlot();
        }
    }

    private bool TryReserveSlot()
    {
        int current;
        do
        {
            current = _totalConnections;
            if (current >= _poolConfig.MaxSize) return false;
        } while (Interlocked.CompareExchange(ref _totalConnections, current + 1, current) != current);
        return true;
    }

    private void ReleaseSlot()
    {
        Interlocked.Decrement(ref _totalConnections);
    }

    public void RecordSuccess(Guid connectionId) => _circuitBreaker.RecordSuccess(connectionId);
    public void RecordFailure(Guid connectionId) => _circuitBreaker.RecordFailure(connectionId);

    public PoolStats GetStats()
    {
        int total = _totalConnections;
        int idle = _idleCount;
        int active = Math.Max(0, total - idle);

        return new PoolStats
        {
            Database = _database,
            Username = _username,
            TotalConnections = total,
            ActiveConnections = active,
            IdleConnections = idle,
            MaxConnections = _poolConfig.MaxSize
        };
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().Wait();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _idleChannel.Writer.TryComplete();

        while (_idleChannel.Reader.TryRead(out var conn))
        {
            Interlocked.Decrement(ref _idleCount);
            await DestroyConnectionAsync(conn);
        }
    }
}
