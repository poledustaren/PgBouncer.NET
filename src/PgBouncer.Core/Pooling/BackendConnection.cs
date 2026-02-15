using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PgBouncer.Core.Pooling;

public sealed class BackendConnection : IServerConnection
{
    private readonly Socket _socket;
    private readonly PipeReader _reader;
    private readonly ILogger? _logger;
    private readonly CancellationTokenSource _readLoopCts = new();
    private DateTime _lastActivity;
    private volatile int _isBroken;
    private volatile IBackendHandler? _handler;
    private Task? _readLoopTask;

    public Guid Id { get; }
    public string Database { get; }
    public string Username { get; }
    public PipeWriter Writer { get; }

    Stream IServerConnection.Stream => throw new NotSupportedException("Use Writer for Pipelines I/O");
    public bool IsBroken => _isBroken == 1;
    public bool IsHealthy => !IsBroken && _socket.Connected;
    public DateTime LastActivity => _lastActivity;
    public int Generation { get; set; }

    public BackendConnection(Socket socket, string database, string username, ILogger? logger = null)
    {
        _socket = socket ?? throw new ArgumentNullException(nameof(socket));
        Database = database ?? throw new ArgumentNullException(nameof(database));
        Username = username ?? throw new ArgumentNullException(nameof(username));
        _logger = logger;

        Id = Guid.NewGuid();
        _lastActivity = DateTime.UtcNow;

        var stream = new NetworkStream(_socket, ownsSocket: false);
        _reader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
        Writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
    }

    public async Task ConnectAndAuthenticateAsync(string password, CancellationToken cancellationToken = default)
    {
        WriteStartupMessage(Database, Username);
        await Writer.FlushAsync(cancellationToken);

        while (true)
        {
            var result = await _reader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            while (TryParseMessage(ref buffer, out var message, out var msgType))
            {
                char c = (char)msgType;

                if (c == 'R')
                {
                    await HandleAuthAsync(message, password, cancellationToken);
                }
                else if (c == 'E')
                {
                    throw new Exception("PostgreSQL authentication failed");
                }
                else if (c == 'Z')
                {
                    _reader.AdvanceTo(buffer.Start);
                    _ = ReadLoopAsync();
                    return;
                }
            }

            _reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
                throw new Exception("Connection closed during startup");
        }
    }

    private void WriteStartupMessage(string database, string user)
    {
        Span<byte> buf = stackalloc byte[512];
        int pos = 4;

        BinaryPrimitives.WriteInt32BigEndian(buf.Slice(pos), 196608); // Protocol 3.0
        pos += 4;

        pos += WriteCString(buf.Slice(pos), "user");
        pos += WriteCString(buf.Slice(pos), user);
        pos += WriteCString(buf.Slice(pos), "database");
        pos += WriteCString(buf.Slice(pos), database);
        buf[pos++] = 0;

        BinaryPrimitives.WriteInt32BigEndian(buf, pos);
        Writer.Write(buf.Slice(0, pos));
    }

    private int WriteCString(Span<byte> buf, string value)
    {
        int len = Encoding.UTF8.GetBytes(value, buf);
        buf[len] = 0;
        return len + 1;
    }

    private async Task HandleAuthAsync(ReadOnlySequence<byte> message, string password, CancellationToken ct)
    {
        var data = message.ToArray();
        int authCode = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(5));

        if (authCode == 0) return;

        if (authCode == 3) // CleartextPassword
        {
            WritePasswordMessage(password);
            await Writer.FlushAsync(ct);
        }
        else if (authCode == 5) // MD5Password
        {
            throw new NotSupportedException("MD5 auth not implemented");
        }
        else
        {
            throw new NotSupportedException($"Auth code {authCode} not supported");
        }
    }

    private void WritePasswordMessage(string password)
    {
        var passBytes = Encoding.UTF8.GetBytes(password);
        int len = 4 + passBytes.Length + 1;

        Span<byte> buf = stackalloc byte[1 + 4 + 256];
        buf[0] = (byte)'p';
        BinaryPrimitives.WriteInt32BigEndian(buf.Slice(1), len);
        passBytes.CopyTo(buf.Slice(5));
        buf[5 + passBytes.Length] = 0;

        Writer.Write(buf.Slice(0, 1 + len));
    }

    public void AttachHandler(IBackendHandler handler)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        Generation++;
        UpdateActivity();
    }

    void IServerConnection.AttachHandler(IBackendPacketHandler handler)
    {
        throw new NotSupportedException("Use AttachHandler(IBackendHandler)");
    }

    public void DetachHandler()
    {
        _handler = null;
        UpdateActivity();
    }

    public void StartReaderLoop()
    {
        if (_readLoopTask != null) return;
        _readLoopTask = Task.Run(() => ReadLoopAsync());
    }

    public void UpdateActivity() => _lastActivity = DateTime.UtcNow;
    public void MarkDirty() { }
    public bool IsIdle(int timeout) => timeout > 0 && (DateTime.UtcNow - _lastActivity).TotalSeconds > timeout;

    private async Task ReadLoopAsync()
    {
        try
        {
            while (true)
            {
                var result = await _reader.ReadAsync();
                var buffer = result.Buffer;

                while (TryParseMessage(ref buffer, out var message, out var msgType))
                {
                    var handler = _handler;
                    if (handler != null)
                    {
                        await handler.HandleBackendMessageAsync(message, msgType);
                    }
                    else
                    {
                        ProcessIdleMessage(msgType);
                    }
                }

                _reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch (Exception ex)
        {
            MarkAsBroken();
            _handler?.OnBackendDisconnected(ex);
        }
        finally
        {
            MarkAsBroken();
        }
    }

    private bool TryParseMessage(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> message, out byte msgType)
    {
        message = default;
        msgType = 0;

        if (buffer.Length < 5) return false;

        var first5 = buffer.Slice(0, 5);
        Span<byte> header = stackalloc byte[5];
        first5.CopyTo(header);
        
        msgType = header[0];
        int len = BinaryPrimitives.ReadInt32BigEndian(header.Slice(1));
        
        if (len < 4 || len > 1024 * 1024)
            return false;

        int total = len + 1;

        if (buffer.Length < total) return false;

        message = buffer.Slice(0, total);
        buffer = buffer.Slice(total);
        return true;
    }

    private void ProcessIdleMessage(byte msgType)
    {
        char typeChar = (char)msgType;
        switch (typeChar)
        {
            case 'N': // NoticeResponse
            case 'S': // ParameterStatus  
            case 'A': // NotificationResponse
            case 'Z': // ReadyForQuery
            case '1': // ParseComplete (Extended Query Protocol)
            case '2': // BindComplete (Extended Query Protocol)
            case 'T': // RowDescription (Extended Query Protocol)
            case 'D': // DataRow (Extended Query Protocol)
            case 'C': // CommandComplete (Extended Query Protocol)
                // Нормальный фоновый шум или хвосты Extended Query Protocol
                break;
            case 'E':
                // Ошибка от базы (например, админ сделал pg_terminate_backend)
                Console.WriteLine($"[Backend {_socket.Handle}] Broken while idle: received ErrorResponse (E)");
                MarkAsBroken();
                break;
            default:
                // Сессия не дочитала данные от прошлого запроса - десинхронизация
                Console.WriteLine($"[Backend {_socket.Handle}] Protocol desync! Received unexpected packet '{typeChar}' (0x{msgType:X2}) in Idle state.");
                MarkAsBroken();
                break;
        }
    }

    private void MarkAsBroken() => Interlocked.Exchange(ref _isBroken, 1);

    public async ValueTask DisposeAsync()
    {
        _readLoopCts.Cancel();
        if (_readLoopTask != null)
            await Task.WhenAny(_readLoopTask, Task.Delay(1000));

        try { await _reader.CompleteAsync(); } catch { }
        try { await Writer.CompleteAsync(); } catch { }
        try { _socket.Shutdown(SocketShutdown.Both); } catch { }
        try { _socket.Close(); } catch { }
        _readLoopCts.Dispose();
    }
}
