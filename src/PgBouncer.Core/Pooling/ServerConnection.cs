using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;

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
    private int _generation;
    
    private volatile IBackendPacketHandler? _currentHandler;
    private volatile bool _readerRunning;
    private CancellationTokenSource? _readerCts;
    private Task? _readerTask;

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

    public int Generation 
    { 
        get => _generation; 
        set => _generation = value;
    }

    public DateTime LastActivity => _lastActivity;

    public void AttachHandler(IBackendPacketHandler handler)
    {
        _currentHandler = handler;
        _generation++;
    }

    public void DetachHandler()
    {
        _currentHandler = null;
    }

    public void StartReaderLoop()
    {
        if (_readerRunning) return;
        
        _readerCts = new CancellationTokenSource();
        _readerTask = ReadLoopAsync(_readerCts.Token);
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        _readerRunning = true;
        var buffer = ArrayPool<byte>.Shared.Rent(65536);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await ReadMessageAsync(_stream, buffer, cancellationToken);
                if (message == null)
                {
                    _currentHandler?.OnBackendDisconnected();
                    break;
                }

                var handler = _currentHandler;
                if (handler != null)
                {
                    await handler.HandlePacketAsync(message, _generation);
                }
                else
                {
                    _isDirty = true;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            _currentHandler?.OnBackendDisconnected();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            _readerRunning = false;
        }
    }

    private static async Task<byte[]?> ReadMessageAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        if (!await ReadExactAsync(stream, buffer.AsMemory(0, 5), cancellationToken))
            return null;

        int length = (buffer[1] << 24) | (buffer[2] << 16) | (buffer[3] << 8) | buffer[4];
        
        var bodyLength = length - 4;
        var totalLength = 5 + bodyLength;

        if (bodyLength > 0)
        {
            if (totalLength > buffer.Length)
                return null;

            if (!await ReadExactAsync(stream, buffer.AsMemory(5, bodyLength), cancellationToken))
                return null;
        }

        var message = new byte[totalLength];
        Buffer.BlockCopy(buffer, 0, message, 0, totalLength);
        return message;
    }

    private static async Task<bool> ReadExactAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.Slice(totalRead), cancellationToken);
            if (read == 0) return false;
            totalRead += read;
        }
        return true;
    }

    public void UpdateActivity()
    {
        _lastActivity = DateTime.UtcNow;
        _isDirty = false;
    }

    public void MarkDirty()
    {
        _isDirty = true;
    }

    public bool IsIdle(int idleTimeoutSeconds) =>
        (DateTime.UtcNow - _lastActivity).TotalSeconds > idleTimeoutSeconds;

    public async ValueTask DisposeAsync()
    {
        _readerCts?.Cancel();
        
        if (_readerTask != null)
        {
            try
            {
                await Task.WhenAny(_readerTask, Task.Delay(1000));
            }
            catch { }
        }
        
        _readerCts?.Dispose();
        
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
