namespace PgBouncer.Core.Pooling;

public interface IConnectionPool : IDisposable
{
    Task<IServerConnection> AcquireAsync(CancellationToken cancellationToken);
    void Release(IServerConnection connection);
    PoolStats GetStats();
    Task InitializeAsync(int minConnections, CancellationToken cancellationToken = default);
    void RecordSuccess(Guid connectionId);
    void RecordFailure(Guid connectionId);
}
