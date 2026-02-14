namespace PgBouncer.Core.Pooling;

public interface IBackendPacketHandler
{
    ValueTask HandlePacketAsync(byte[] packet, int generation);
    void OnBackendDisconnected();
}
