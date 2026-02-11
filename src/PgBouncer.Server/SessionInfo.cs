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

    /// <summary>Состояние сессии</summary>
    public SessionState State { get; set; } = SessionState.Connecting;

    /// <summary>Время ожидания в очереди (мс)</summary>
    public long WaitTimeMs { get; set; }

    /// <summary>Время начала ожидания</summary>
    public DateTime? WaitStartedAt { get; set; }

    /// <summary>Длительность сессии</summary>
    public TimeSpan Duration => DateTime.UtcNow - StartedAt;
}

/// <summary>
/// Состояние сессии
/// </summary>
public enum SessionState
{
    Connecting,
    WaitingForSlot,
    Idle,      // Transaction Pooling: ожидание запроса (backend свободен)
    Active,    // Transaction Pooling: выполнение запроса (backend занят)
    Completed,
    Error
}
