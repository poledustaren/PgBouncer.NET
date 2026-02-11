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
public class ConnectionPool : IDisposable
{
    private readonly string _database;
    private readonly string _username;
    private readonly string _password;
    private readonly BackendConfig _backendConfig;
    private readonly PoolConfig _poolConfig;
    private readonly ILogger? _logger;

    // Channel обеспечивает FIFO и async wait без SemaphoreSlim overhead
    private readonly Channel<ServerConnection> _idleChannel;
    private readonly ConcurrentDictionary<Guid, ServerConnection> _activeConnections = new();
    private readonly Timer? _idleCleanupTimer;
    private readonly SemaphoreSlim _createLock = new(1, 1);

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

        // Создаём bounded channel с FIFO семантикой
        var options = new BoundedChannelOptions(poolConfig.MaxSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        };
        _idleChannel = Channel.CreateBounded<ServerConnection>(options);

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
    /// Получить соединение из пула (zero-allocation при наличии idle)
    /// </summary>
    public async ValueTask<ServerConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _totalAcquired);

        // 1. Пытаемся взять из idle (zero-alloc fast path)
        while (_idleChannel.Reader.TryRead(out var connection))
        {
            if (connection.IsHealthy)
            {
                _activeConnections[connection.Id] = connection;
                connection.UpdateActivity();
                _logger?.LogTrace("Переиспользовано соединение {ConnectionId} для {Database}/{User}",
                    connection.Id, _database, _username);
                return connection;
            }

            // Соединение мёртвое, закрываем
            await connection.DisposeAsync();
            Interlocked.Decrement(ref _currentSize);
        }

        // 2. Пытаемся создать новое (если лимит не достигнут)
        var currentSize = Interlocked.Increment(ref _currentSize);
        if (currentSize <= _poolConfig.MaxSize)
        {
            try
            {
                var newConnection = await CreateConnectionAsync(cancellationToken);
                _activeConnections[newConnection.Id] = newConnection;
                _logger?.LogInformation("Создано соединение {ConnectionId} для {Database}/{User} (всего: {Total})",
                    newConnection.Id, _database, _username, currentSize);
                return newConnection;
            }
            catch
            {
                Interlocked.Decrement(ref _currentSize);
                throw;
            }
        }

        // 3. Лимит достигнут - ждём освобождения с таймаутом
        Interlocked.Decrement(ref _currentSize);
        _logger?.LogDebug("Лимит пула достигнут ({Max}), ожидание...", _poolConfig.MaxSize);

        // Добавляем таймаут чтобы не ждать вечно
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(_poolConfig.ConnectionTimeout > 0 ? _poolConfig.ConnectionTimeout : 30));

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
            _logger?.LogWarning("Таймаут ожидания соединения из пула ({Timeout}s)", _poolConfig.ConnectionTimeout);
            throw new TimeoutException($"Не удалось получить соединение из пула за {_poolConfig.ConnectionTimeout} секунд");
        }
    }

    /// <summary>
    /// Вернуть соединение в пул (zero-allocation)
    /// </summary>
    public void Release(ServerConnection connection)
    {
        Interlocked.Increment(ref _totalReleased);

        if (_activeConnections.TryRemove(connection.Id, out _))
        {
            connection.UpdateActivity();

            if (connection.IsHealthy && !_disposed)
            {
                // TryWrite - zero-alloc, если есть место в канале
                if (_idleChannel.Writer.TryWrite(connection))
                {
                    _logger?.LogTrace("Соединение {ConnectionId} возвращено в пул", connection.Id);
                }
                else
                {
                    // Канал полон (не должно быть при правильной логике)
                    connection.DisposeAsync().AsTask().Wait();
                    Interlocked.Decrement(ref _currentSize);
                    _logger?.LogWarning("Канал полон, соединение {ConnectionId} закрыто", connection.Id);
                }
            }
            else
            {
                connection.DisposeAsync().AsTask().Wait();
                Interlocked.Decrement(ref _currentSize);
                _logger?.LogTrace("Соединение {ConnectionId} закрыто (нездоровое или disposed)", connection.Id);
            }
        }
    }

    /// <summary>
    /// Создать новое соединение к PostgreSQL
    /// </summary>
    private async Task<ServerConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var connector = new Protocol.BackendConnector(_backendConfig, _logger);

        try
        {
            // Подключаемся и проходим аутентификацию
            var (socket, stream) = await connector.ConnectAndAuthenticateAsync(
                _database,
                _username,
                _password,
                cancellationToken);

            // Создаем соединение с полученным сокетом и стримом
            var connection = new ServerConnection(socket, stream, _database, _username);

            _logger?.LogInformation("Соединение {Id} успешно аутентифицировано", connection.Id);

            return connection;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Ошибка подключения к {Host}:{Port}", _backendConfig.Host, _backendConfig.Port);
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
        var toKeep = new List<ServerConnection>();
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
        if (_disposed) return;
        _disposed = true;

        _idleCleanupTimer?.Dispose();

        // Завершаем канал
        _idleChannel.Writer.Complete();

        // Закрываем все активные соединения
        foreach (var conn in _activeConnections.Values)
        {
            conn.DisposeAsync().AsTask().Wait();
        }

        // Закрываем все idle соединения
        while (_idleChannel.Reader.TryRead(out var conn))
        {
            conn.DisposeAsync().AsTask().Wait();
        }

        _createLock.Dispose();
    }
}

/// <summary>
/// Серверное соединение к PostgreSQL
/// </summary>
public class ServerConnection : IAsyncDisposable
{
    public Guid Id { get; } = Guid.NewGuid();
    public string Database { get; }
    public string Username { get; }

    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly bool _ownsSocket;
    private DateTime _lastActivity;

    public ServerConnection(Socket socket, string database, string username)
    {
        _socket = socket;
        _stream = new NetworkStream(socket, ownsSocket: false);
        _ownsSocket = true;
        Database = database;
        Username = username;
        _lastActivity = DateTime.UtcNow;
    }

    /// <summary>
    /// Конструктор с готовым NetworkStream (после аутентификации)
    /// </summary>
    public ServerConnection(Socket socket, NetworkStream stream, string database, string username)
    {
        _socket = socket;
        _stream = stream;
        _ownsSocket = false; // stream уже владеет сокетом
        Database = database;
        Username = username;
        _lastActivity = DateTime.UtcNow;
    }

    public bool IsHealthy => _socket.Connected;

    public NetworkStream Stream => _stream;

    public void UpdateActivity()
    {
        _lastActivity = DateTime.UtcNow;
    }

    /// <summary>Время последней активности</summary>
    public DateTime LastActivity => _lastActivity;

    /// <summary>Проверяет истек ли idle timeout</summary>
    public bool IsIdle(int idleTimeoutSeconds) =>
        (DateTime.UtcNow - _lastActivity).TotalSeconds > idleTimeoutSeconds;

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync();
        _socket.Dispose();
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
