using System.Collections.Concurrent;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;

namespace PgBouncer.Core.Pooling;

/// <summary>
/// Пул соединений к одной БД для одного пользователя
/// </summary>
public class ConnectionPool : IDisposable
{
    private readonly string _database;
    private readonly string _username;
    private readonly string _password;
    private readonly BackendConfig _backendConfig;
    private readonly PoolConfig _poolConfig;
    private readonly ILogger? _logger;

    private readonly ConcurrentBag<ServerConnection> _availableConnections = new();
    private readonly ConcurrentDictionary<Guid, ServerConnection> _activeConnections = new();
    private readonly SemaphoreSlim _connectionSemaphore;

    private int _totalConnections;
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

        _connectionSemaphore = new SemaphoreSlim(poolConfig.MaxSize, poolConfig.MaxSize);
    }

    /// <summary>
    /// Получить соединение из пула
    /// </summary>
    public async Task<ServerConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        // Ждём доступности слота
        await _connectionSemaphore.WaitAsync(cancellationToken);

        try
        {
            // Пытаемся взять существующее соединение
            if (_availableConnections.TryTake(out var connection))
            {
                if (connection.IsHealthy)
                {
                    _activeConnections[connection.Id] = connection;
                    _logger?.LogTrace("Переиспользовано соединение {ConnectionId} для {Database}/{User}",
                        connection.Id, _database, _username);
                    return connection;
                }

                // Соединение мёртвое, закрываем
                await connection.DisposeAsync();
                Interlocked.Decrement(ref _totalConnections);
            }

            // Создаём новое соединение
            var newConnection = await CreateConnectionAsync(cancellationToken);
            _activeConnections[newConnection.Id] = newConnection;
            Interlocked.Increment(ref _totalConnections);

            _logger?.LogInformation("Создано новое соединение {ConnectionId} для {Database}/{User} (всего: {Total})",
                newConnection.Id, _database, _username, _totalConnections);

            return newConnection;
        }
        catch
        {
            _connectionSemaphore.Release();
            throw;
        }
    }

    /// <summary>
    /// Вернуть соединение в пул
    /// </summary>
    public void Release(ServerConnection connection)
    {
        if (_activeConnections.TryRemove(connection.Id, out _))
        {
            if (connection.IsHealthy && !_disposed)
            {
                _availableConnections.Add(connection);
                _logger?.LogTrace("Соединение {ConnectionId} возвращено в пул", connection.Id);
            }
            else
            {
                connection.DisposeAsync().AsTask().Wait();
                Interlocked.Decrement(ref _totalConnections);
                _logger?.LogTrace("Соединение {ConnectionId} закрыто", connection.Id);
            }

            _connectionSemaphore.Release();
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

            _logger?.LogDebug("Соединение {Id} успешно аутентифицировано", connection.Id);

            return connection;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Ошибка подключения к {Host}:{Port}", _backendConfig.Host, _backendConfig.Port);
            throw;
        }
    }

    /// <summary>
    /// Статистика пула
    /// </summary>
    public PoolStats GetStats()
    {
        return new PoolStats
        {
            Database = _database,
            Username = _username,
            TotalConnections = _totalConnections,
            ActiveConnections = _activeConnections.Count,
            IdleConnections = _availableConnections.Count,
            MaxConnections = _poolConfig.MaxSize
        };
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Закрываем все соединения
        foreach (var conn in _activeConnections.Values)
        {
            conn.DisposeAsync().AsTask().Wait();
        }

        while (_availableConnections.TryTake(out var conn))
        {
            conn.DisposeAsync().AsTask().Wait();
        }

        _connectionSemaphore.Dispose();
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
