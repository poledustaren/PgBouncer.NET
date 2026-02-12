using System.Net.Sockets;

namespace PgBouncer.Core.Pooling;

/// <summary>
/// Серверное соединение к PostgreSQL
/// </summary>
public class ServerConnection : IServerConnection
{
    public Guid Id { get; } = Guid.NewGuid();
    public string Database { get; }
    public string Username { get; }

    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly bool _ownsSocket;
    private DateTime _lastActivity;

    public ServerConnection(Socket socket, string database, string username)
    {
        _socket = socket;
        _stream = new NetworkStream(socket, ownsSocket: false);
        _ownsSocket = true;
        Database = database;
        Username = username;
        _lastActivity = DateTime.UtcNow;
    }

    /// <summary>
    /// Конструктор с готовым NetworkStream (после аутентификации)
    /// </summary>
    public ServerConnection(Socket socket, NetworkStream stream, string database, string username)
    {
        _socket = socket;
        _stream = stream;
        _ownsSocket = false; // stream уже владеет сокетом
        Database = database;
        Username = username;
        _lastActivity = DateTime.UtcNow;
    }

    public bool IsHealthy => _socket.Connected;

    public Stream Stream => _stream;

    public void UpdateActivity()
    {
        _lastActivity = DateTime.UtcNow;
    }

    /// <summary>Время последней активности</summary>
    public DateTime LastActivity => _lastActivity;

    /// <summary>Проверяет истек ли idle timeout</summary>
    public bool IsIdle(int idleTimeoutSeconds) =>
        (DateTime.UtcNow - _lastActivity).TotalSeconds > idleTimeoutSeconds;

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync();
        _socket.Dispose();
    }
}
