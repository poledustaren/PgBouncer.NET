namespace PgBouncer.Core.Pooling;

public interface IServerConnection : IAsyncDisposable
{
    Guid Id { get; }
    string Database { get; }
    string Username { get; }
    Stream Stream { get; }
    bool IsHealthy { get; }
    DateTime LastActivity { get; }

    void UpdateActivity();
    void MarkDirty();
    bool IsIdle(int idleTimeoutSeconds);
}
