namespace PgBouncer.Core.Pooling;

/// <summary>
/// Статистика пула соединений
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
