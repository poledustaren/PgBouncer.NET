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
        // Проверяем глобальный лимит
        await _totalConnectionsSemaphore.WaitAsync(cancellationToken);

        try
        {
            var poolKey = GetPoolKey(database, username);
            var pool = _pools.GetOrAdd(poolKey, _ => CreatePool(database, username, password));

            var connection = await pool.AcquireAsync(cancellationToken);
            Interlocked.Increment(ref _totalConnections);

            return connection;
        }
        catch
        {
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
