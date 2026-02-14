using System.Buffers;
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
    private volatile bool _isBroken;
    private int _generation;
    
    private volatile IBackendPacketHandler? _currentHandler;
    private volatile bool _readerRunning;
    private CancellationTokenSource? _readerCts;
    private Task? _readerTask;

    public bool IsBroken => _isBroken;

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
            if (_isDirty || _isBroken) return false;
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
        var headerBuffer = new byte[5];
        var payloadBuffer = new byte[65536];
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!await ReadExactAsync(_stream, headerBuffer, cancellationToken))
                {
                    _currentHandler?.OnBackendDisconnected();
                    _isBroken = true;
                    break;
                }

                var messageType = headerBuffer[0];
                var messageLength = (headerBuffer[1] << 24) | (headerBuffer[2] << 16) | (headerBuffer[3] << 8) | headerBuffer[4];
                var payloadLength = messageLength - 4;

                byte[]? payload = null;
                if (payloadLength > 0)
                {
                    if (payloadLength > payloadBuffer.Length)
                    {
                        _isBroken = true;
                        _currentHandler?.OnBackendDisconnected();
                        break;
                    }
                    
                    if (!await ReadExactAsync(_stream, payloadBuffer.AsMemory(0, payloadLength), cancellationToken))
                    {
                        _currentHandler?.OnBackendDisconnected();
                        _isBroken = true;
                        break;
                    }
                    
                    payload = new byte[payloadLength];
                    Buffer.BlockCopy(payloadBuffer, 0, payload, 0, payloadLength);
                }

                var handler = _currentHandler;
                if (handler != null)
                {
                    var fullPacket = new byte[5 + (payloadLength > 0 ? payloadLength : 0)];
                    Buffer.BlockCopy(headerBuffer, 0, fullPacket, 0, 5);
                    if (payload != null)
                    {
                        Buffer.BlockCopy(payload, 0, fullPacket, 5, payloadLength);
                    }
                    await handler.HandlePacketAsync(fullPacket, _generation);
                }
                else
                {
                    ProcessIdleMessage((char)messageType);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            _currentHandler?.OnBackendDisconnected();
            _isBroken = true;
        }
        finally
        {
            _readerRunning = false;
        }
    }

    private void ProcessIdleMessage(char messageType)
    {
        switch (messageType)
        {
            case 'N':
            case 'S':
            case 'A':
                break;

            case 'E':
                _isBroken = true;
                break;

            case 'Z':
                break;

            default:
                _isBroken = true;
                break;
        }
    }

    private static async Task<bool> ReadExactAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(totalRead), cancellationToken);
            if (read == 0) return false;
            totalRead += read;
        }
        return true;
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
