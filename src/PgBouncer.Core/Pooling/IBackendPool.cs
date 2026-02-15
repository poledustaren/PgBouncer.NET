namespace PgBouncer.Core.Pooling;

public interface IBackendPool
{
    Task<BackendConnection> AcquireAsync(CancellationToken cancellationToken = default);
    void Return(BackendConnection connection);
    PoolStats GetStats();
}
