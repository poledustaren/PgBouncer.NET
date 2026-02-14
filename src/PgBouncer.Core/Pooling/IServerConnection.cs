using System.Threading.Channels;

namespace PgBouncer.Core.Pooling;

public interface IServerConnection : IAsyncDisposable
{
    Guid Id { get; }
    string Database { get; }
    string Username { get; }
    Stream Stream { get; }
    bool IsHealthy { get; }
    DateTime LastActivity { get; }
    int Generation { get; set; }
    
    void AttachHandler(IBackendPacketHandler handler);
    void DetachHandler();
    void StartReaderLoop();
    
    void UpdateActivity();
    void MarkDirty();
    bool IsIdle(int idleTimeoutSeconds);
}
