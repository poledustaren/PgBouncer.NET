using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;

namespace PgBouncer.Core.Pooling;

/// <summary>
/// Менеджер пулов - управляет множеством пулов для разных БД/пользователей
/// </summary>
public class PoolManager : IDisposable
{
    private readonly PgBouncerConfig _config;
    private readonly ILogger? _logger;

    private readonly ConcurrentDictionary<string, ConnectionPool> _pools = new();
    private readonly SemaphoreSlim _totalConnectionsSemaphore;

    private int _totalConnections;
    private bool _disposed;

    public PoolManager(PgBouncerConfig config, ILogger? logger = null)
    {
        _config = config;
        _logger = logger;
        _totalConnectionsSemaphore = new SemaphoreSlim(
            config.Pool.MaxTotalConnections,
            config.Pool.MaxTotalConnections);
    }

    /// <summary>
    /// Получить соединение для указанной БД/пользователя
    /// </summary>
    public async Task<ServerConnection> AcquireConnectionAsync(
        string database,
        string username,
        string password,
        CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation(">>> Запрос соединения для {Database}/{User}, текущий счётчик: {Total}", database, username, _totalConnections);

        // Проверяем глобальный лимит
        _logger?.LogInformation("Ожидание семафора...");
        await _totalConnectionsSemaphore.WaitAsync(cancellationToken);
        _logger?.LogInformation("Семафор получен");

        try
        {
            var poolKey = GetPoolKey(database, username);
            _logger?.LogInformation("Ключ пула: {PoolKey}", poolKey);

            var pool = _pools.GetOrAdd(poolKey, _ => CreatePool(database, username, password));

            _logger?.LogInformation("Запрос соединения из пула...");
            var connection = await pool.AcquireAsync(cancellationToken);
            Interlocked.Increment(ref _totalConnections);

            _logger?.LogInformation(">>> Соединение получено: {ConnectionId}", connection.Id);

            return connection;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Ошибка при получении соединения из пула");
            _totalConnectionsSemaphore.Release();
            throw;
        }
    }

    /// <summary>
    /// Вернуть соединение в пул
    /// </summary>
    public void ReleaseConnection(ServerConnection connection)
    {
        var poolKey = GetPoolKey(connection.Database, connection.Username);

        if (_pools.TryGetValue(poolKey, out var pool))
        {
            pool.Release(connection);
            Interlocked.Decrement(ref _totalConnections);
            _totalConnectionsSemaphore.Release();
        }
    }

    /// <summary>
    /// Получить статистику всех пулов
    /// </summary>
    public IEnumerable<PoolStats> GetAllStats()
    {
        return _pools.Values.Select(p => p.GetStats());
    }

    /// <summary>
    /// Получить статистику конкретного пула
    /// </summary>
    public PoolStats? GetStats(string database, string username)
    {
        var poolKey = GetPoolKey(database, username);
        return _pools.TryGetValue(poolKey, out var pool) ? pool.GetStats() : null;
    }

    /// <summary>
    /// Получить пул для database/user (для Transaction Pooling)
    /// </summary>
    public Task<ConnectionPool> GetPoolAsync(string database, string username, string password)
    {
        var poolKey = GetPoolKey(database, username);
        var pool = _pools.GetOrAdd(poolKey, _ => CreatePool(database, username, password));
        return Task.FromResult(pool);
    }

    /// <summary>
    /// Создать новый пул
    /// </summary>
    private ConnectionPool CreatePool(string database, string username, string password)
    {
        _logger?.LogInformation("Создание нового пула для {Database}/{User}", database, username);

        return new ConnectionPool(
            database,
            username,
            password,
            _config.Backend,
            _config.Pool,
            _logger);
    }

    /// <summary>
    /// Ключ пула: database:username
    /// </summary>
    private static string GetPoolKey(string database, string username)
    {
        return $"{database}:{username}";
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var pool in _pools.Values)
        {
            pool.Dispose();
        }

        _totalConnectionsSemaphore.Dispose();
    }
}
