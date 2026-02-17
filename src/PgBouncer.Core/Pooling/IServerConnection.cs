namespace PgBouncer.Core.Pooling;

public interface IServerConnection : IAsyncDisposable
{
    Guid Id { get; }
    string Database { get; }
    string Username { get; }
    bool IsHealthy { get; }
    bool IsBroken { get; }
    DateTime LastActivity { get; }
    int Generation { get; set; }
    
    // For Pipelines architecture
    System.IO.Pipelines.PipeWriter Writer { get; }

    // Handlers
    void AttachHandler(IBackendHandler handler);
    void DetachHandler();
    void StartReaderLoop();
    
    // Activity tracking
    void UpdateActivity();
    void MarkDirty();
    bool IsIdle(int idleTimeoutSeconds);

    // Operations
    Task ExecuteResetQueryAsync();
}
