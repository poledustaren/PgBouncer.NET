using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;

namespace PgBouncer.Core.Pooling;

/// <summary>
/// Оптимизированный пул соединений на Channel<T>
/// - FIFO ротация (равномерное использование соединений)
/// - Zero-allocation ожидание
/// - Bounded capacity
/// </summary>
public class ConnectionPool : IConnectionPool
{
    private readonly string _database;
    private readonly string _username;
    private readonly string _password;
    private readonly BackendConfig _backendConfig;
    private readonly PoolConfig _poolConfig;
    private readonly ILogger? _logger;

    // Channel обеспечивает FIFO и async wait без SemaphoreSlim overhead
    private readonly Channel<IServerConnection> _idleChannel;
    private readonly ConcurrentDictionary<Guid, IServerConnection> _activeConnections = new();
    private readonly Timer? _idleCleanupTimer;
    // Ограничиваем количество одновременных попыток подключения (Handshakes)
    // 50 слотов, чтобы быстрее прогреваться.
    private readonly SemaphoreSlim _createLock = new(50, 50);
    
    // Circuit breaker for handling failed connections
    private readonly CircuitBreaker _circuitBreaker;

    private int _currentSize;
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
        _circuitBreaker = new CircuitBreaker(
            failureThreshold: 3,
            resetTimeoutSeconds: 30);

        // Создаём bounded channel с FIFO семантикой
        var options = new BoundedChannelOptions(poolConfig.MaxSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        };
        _idleChannel = Channel.CreateBounded<IServerConnection>(options);

        // Запускаем таймер для очистки idle соединений
        if (poolConfig.IdleTimeout > 0)
        {
            _idleCleanupTimer = new Timer(
                CleanupIdleConnections,
                null,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(30));
        }
    }

    /// <summary>
    /// Асинхронная инициализация пула - создает минимальное количество соединений
    /// </summary>
    public async Task InitializeAsync(int minConnections, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Initializing pool with {MinConnections} minimum connections...", minConnections);
        
        for (int i = 0; i < minConnections; i++)
        {
            try
            {
                _logger?.LogInformation("Creating connection {Current}/{Total}...", i + 1, minConnections);
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromSeconds(30));
                
                var connection = await CreateConnectionAsync(cts.Token);
                _logger?.LogInformation("Connection created successfully: {ConnectionId}", connection.Id);
                
                if (_idleChannel.Writer.TryWrite(connection))
                {
                    Interlocked.Increment(ref _currentSize);
                    _logger?.LogInformation("Connection {ConnectionId} added to idle channel. Pool size: {Size}", connection.Id, _currentSize);
                }
                else
                {
                    _logger?.LogError("Failed to add connection to idle channel - channel is full!");
                    await connection.DisposeAsync();
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to create connection {Current}/{Total}: {Message}", i + 1, minConnections, ex.Message);
            }
        }
        
        _logger?.LogInformation("Pool initialized with {CurrentSize} connections", _currentSize);
    }

    /// <summary>
    /// Получить соединение из пула (zero-allocation при наличии idle)
    /// </summary>
    public async Task<IServerConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _totalAcquired);

        // 1. Пытаемся взять из idle (zero-alloc fast path)
        while (_idleChannel.Reader.TryRead(out var connection))
        {
            // Проверяем health первым делом
            if (!connection.IsHealthy)
            {
                _logger?.LogWarning("Dead connection {ConnectionId} found in pool, disposing",
                    connection.Id);
                await connection.DisposeAsync();
                Interlocked.Decrement(ref _currentSize);
                continue;
            }

            // Check circuit breaker
            if (!_circuitBreaker.IsAllowed(connection.Id))
            {
                _logger?.LogWarning("Circuit breaker open for connection {ConnectionId}, disposing",
                    connection.Id);
                await connection.DisposeAsync();
                Interlocked.Decrement(ref _currentSize);
                continue;
            }

            _activeConnections[connection.Id] = connection;
            connection.UpdateActivity();
            _logger?.LogDebug("Reusing connection {ConnectionId} for {Database}/{User}",
                connection.Id, _database, _username);
            return connection;
        }

        // 2. Пытаемся создать новое (если лимит не достигнут)
        var currentSize = Interlocked.Increment(ref _currentSize);
        if (currentSize <= _poolConfig.MaxSize)
        {
            try
            {
                // Ждём слот на создание (троттлинг)
                await _createLock.WaitAsync(cancellationToken);
                try
                {
                    var newConnection = await CreateConnectionAsync(cancellationToken);
                    _activeConnections[newConnection.Id] = newConnection;
                    _logger?.LogInformation("Создано соединение {ConnectionId} для {Database}/{User} (всего: {Total})",
                        newConnection.Id, _database, _username, currentSize);
                    return newConnection;
                }
                finally
                {
                    _createLock.Release();
                }
            }
            catch
            {
                Interlocked.Decrement(ref _currentSize);
                // Backoff to prevent storm. 200ms is enough if we fail fast.
                await Task.Delay(200, cancellationToken);
                throw;
            }
        }

        // 3. Лимит достигнут - ждём освобождения с таймаутом
        Interlocked.Decrement(ref _currentSize);
        _logger?.LogDebug("Лимит пула достигнут ({Max}), ожидание...", _poolConfig.MaxSize);

        // Увеличиваем таймаут ожидания соединения из пула (дефолт 60с)
        var waitTimeout = _poolConfig.ConnectionTimeout > 0 ? _poolConfig.ConnectionTimeout : 60;
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(waitTimeout));

        try
        {
            var waitedConnection = await _idleChannel.Reader.ReadAsync(timeoutCts.Token);

            if (!waitedConnection.IsHealthy)
            {
                await waitedConnection.DisposeAsync();
                // Рекурсивный вызов для получения нового соединения
                return await AcquireAsync(cancellationToken);
            }

            _activeConnections[waitedConnection.Id] = waitedConnection;
            waitedConnection.UpdateActivity();
            return waitedConnection;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Наш таймаут сработал, а не внешний cancellation
            _logger?.LogWarning("Таймаут ожидания соединения из пула ({Timeout}s)", waitTimeout);
            throw new TimeoutException($"Не удалось получить соединение из пула за {waitTimeout} секунд");
        }
    }

    /// <summary>
    /// Вернуть соединение в пул (zero-allocation)
    /// </summary>
    public void Release(IServerConnection connection)
    {
        Interlocked.Increment(ref _totalReleased);

        if (_activeConnections.TryRemove(connection.Id, out _))
        {
            if (connection.IsBroken || !connection.IsHealthy || _disposed)
            {
                connection.DisposeAsync().AsTask().Wait();
                Interlocked.Decrement(ref _currentSize);
                _logger?.LogTrace("Соединение {ConnectionId} закрыто (нездоровое или disposed)", connection.Id);
                return;
            }
            
            connection.UpdateActivity();

            if (_idleChannel.Writer.TryWrite(connection))
            {
                _logger?.LogTrace("Соединение {ConnectionId} возвращено в пул", connection.Id);
            }
            else
            {
                connection.DisposeAsync().AsTask().Wait();
                Interlocked.Decrement(ref _currentSize);
                _logger?.LogWarning("Канал полон, соединение {ConnectionId} закрыто", connection.Id);
            }
        }
    }

    public void RecordSuccess(Guid connectionId)
    {
        _circuitBreaker.RecordSuccess(connectionId);
        _logger?.LogTrace("Connection {ConnectionId} recorded as successful", connectionId);
    }

    public void RecordFailure(Guid connectionId)
    {
        _circuitBreaker.RecordFailure(connectionId);
        _logger?.LogWarning("Connection {ConnectionId} recorded as failed", connectionId);
    }

    /// <summary>
    /// Создать новое соединение к PostgreSQL
    /// </summary>
    private async Task<IServerConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        _logger?.LogInformation("[CreateConnection] Starting connection to {Host}:{Port} for database {Database}, user {User}",
            _backendConfig.Host, _backendConfig.Port, _database, _username);

        try
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
                SendBufferSize = 64 * 1024,
                ReceiveBufferSize = 64 * 1024
            };

            await socket.ConnectAsync(_backendConfig.Host, _backendConfig.Port, cancellationToken);

            _logger?.LogDebug("[CreateConnection] Socket connected, creating BackendConnection...");

            var connection = new BackendConnection(socket, _database, _username, _logger);
            await connection.ConnectAndAuthenticateAsync(_password, cancellationToken);

            _logger?.LogInformation("[CreateConnection] SUCCESS: Connection {Id} created and authenticated", connection.Id);

            return connection;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "[CreateConnection] FAILED: Error connecting to {Host}:{Port} - {Message}",
                _backendConfig.Host, _backendConfig.Port, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Очистка idle соединений (вызывается таймером)
    /// </summary>
    private void CleanupIdleConnections(object? state)
    {
        if (_disposed) return;

        var idleTimeout = _poolConfig.IdleTimeout;
        var toKeep = new List<IServerConnection>();
        var removedCount = 0;

        // Вычитываем все из канала
        while (_idleChannel.Reader.TryRead(out var conn))
        {
            if (conn.IsIdle(idleTimeout) || !conn.IsHealthy)
            {
                conn.DisposeAsync().AsTask().Wait();
                Interlocked.Decrement(ref _currentSize);
                removedCount++;
                _logger?.LogInformation("Закрыто idle соединение {Id} для {Database}/{User}",
                    conn.Id, _database, _username);
            }
            else
            {
                toKeep.Add(conn);
            }
        }

        // Возвращаем живые обратно в канал
        foreach (var conn in toKeep)
        {
            _idleChannel.Writer.TryWrite(conn);
        }

        if (removedCount > 0)
        {
            _logger?.LogDebug("Очищено {Count} idle соединений, осталось: {Total}",
                removedCount, _currentSize);
        }
    }

    /// <summary>
    /// Статистика пула
    /// </summary>
    public PoolStats GetStats()
    {
        // Channel.Reader.Count даёт количество элементов в канале
        var idleCount = _idleChannel.Reader.Count;

        return new PoolStats
        {
            Database = _database,
            Username = _username,
            TotalConnections = _currentSize,
            ActiveConnections = _activeConnections.Count,
            IdleConnections = idleCount,
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

        _idleCleanupTimer?.Dispose();
        
        // Запрещаем добавление новых соединений в канал
        _idleChannel.Writer.Complete();
        
        // Пачка тасков для параллельного закрытия
        var closeTasks = new List<Task>();

        // Закрываем все активные соединения
        foreach (var conn in _activeConnections.Values)
        {
            closeTasks.Add(CloseBackendGracefullyAsync(conn));
        }

        // Вычитываем все свободные соединения из пула
        while (_idleChannel.Reader.TryRead(out var backend))
        {
            closeTasks.Add(CloseBackendGracefullyAsync(backend));
        }

        // Ждем, пока все отправят пакет Terminate
        await Task.WhenAll(closeTasks);

        _createLock.Dispose();
    }

    private async Task CloseBackendGracefullyAsync(IServerConnection backend)
    {
        try
        {
            if (backend.IsHealthy)
            {
                // Отправляем пакет Terminate ('X') серверу PostgreSQL
                // Формат: Type 'X' (1 байт) + Length (4 байта, значение = 4)
                byte[] terminateMsg = { (byte)'X', 0, 0, 0, 4 };
                await backend.Stream.WriteAsync(terminateMsg);
                await backend.Stream.FlushAsync();
            }
        }
        catch 
        { 
            // Игнорируем ошибки сети при выключении 
        }
        finally
        {
            await backend.DisposeAsync();
        }
    }
}

/// <summary>
/// Статистика пула
/// </summary>
public class PoolStats
{
    public string Database { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public int TotalConnections { get; set; }
    public int ActiveConnections { get; set; }
    public int IdleConnections { get; set; }
    public int MaxConnections { get; set; }
}
