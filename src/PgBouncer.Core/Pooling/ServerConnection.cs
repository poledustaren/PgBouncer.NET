using System.Net.Sockets;

namespace PgBouncer.Core.Pooling;

public class ServerConnection : IServerConnection
{
    public Guid Id { get; } = Guid.NewGuid();
    public string Database { get; }
    public string Username { get; }

    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private DateTime _lastActivity;
    private volatile bool _isDirty;

    public ServerConnection(Socket socket, string database, string username)
    {
        _socket = socket;
        _stream = new NetworkStream(socket, ownsSocket: false);
        Database = database;
        Username = username;
        _lastActivity = DateTime.UtcNow;
    }

    public ServerConnection(Socket socket, NetworkStream stream, string database, string username)
    {
        _socket = socket;
        _stream = stream;
        Database = database;
        Username = username;
        _lastActivity = DateTime.UtcNow;
    }

    public bool IsHealthy
    {
        get
        {
            if (_isDirty) return false;
            if (!_socket.Connected) return false;
            
            try
            {
                return !(_socket.Poll(1, SelectMode.SelectRead) && _socket.Available == 0);
            }
            catch
            {
                return false;
            }
        }
    }

    public Stream Stream => _stream;

    public void UpdateActivity()
    {
        _lastActivity = DateTime.UtcNow;
        _isDirty = false;
    }

    public void MarkDirty()
    {
        _isDirty = true;
    }

    public DateTime LastActivity => _lastActivity;

    public bool IsIdle(int idleTimeoutSeconds) =>
        (DateTime.UtcNow - _lastActivity).TotalSeconds > idleTimeoutSeconds;

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _stream.DisposeAsync();
        }
        catch { }
        
        try
        {
            _socket.Shutdown(SocketShutdown.Both);
            _socket.Close();
        }
        catch { }
    }
}
