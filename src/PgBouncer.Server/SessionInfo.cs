namespace PgBouncer.Server;

/// <summary>
/// Информация об активной сессии
/// </summary>
public class SessionInfo
{
    public Guid Id { get; init; }
    public DateTime StartedAt { get; init; }
    public string RemoteEndPoint { get; init; } = string.Empty;
    public string? Database { get; set; }
    public string? Username { get; set; }

    /// <summary>Длительность сессии</summary>
    public TimeSpan Duration => DateTime.UtcNow - StartedAt;
}
