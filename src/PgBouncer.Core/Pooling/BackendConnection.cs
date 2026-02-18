using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Security.Cryptography;
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

    public bool IsBroken => _isBroken == 1;
    public bool IsHealthy
    {
        get
        {
            if (IsBroken) return false;
            try
            {
                if (_socket == null || !_socket.Connected) return false;
                // Strict check: if poll returns true (data available or closed), and we are in pool (idle),
                // it is considered unhealthy because idle connection shouldn't have data pending.
                if (_socket.Poll(0, SelectMode.SelectRead)) return false;
                return true;
            }
            catch { return false; }
        }
    }

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

            while (TryParseMessage(buffer, out var message, out var msgType, out var remainder))
            {
                buffer = remainder;
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
        int userLen = Encoding.UTF8.GetByteCount(user);
        int dbLen = Encoding.UTF8.GetByteCount(database);

        int totalLen = 4 + 4 // length + protocol
            + 4 + 1 + userLen + 1 // "user\0" + user + "\0"
            + 8 + 1 + dbLen + 1 // "database\0" + database + "\0"
            + 1; // null terminator

        var buf = Writer.GetSpan(totalLen);
        int pos = 4;

        BinaryPrimitives.WriteInt32BigEndian(buf.Slice(pos), 196608); // Protocol 3.0
        pos += 4;

        pos += WriteCString(buf.Slice(pos), "user");
        pos += WriteCString(buf.Slice(pos), user);
        pos += WriteCString(buf.Slice(pos), "database");
        pos += WriteCString(buf.Slice(pos), database);
        buf[pos++] = 0;

        BinaryPrimitives.WriteInt32BigEndian(buf, pos);
        Writer.Advance(pos);
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
            var salt = data.AsSpan().Slice(9, 4).ToArray();
            var response = CalculateMD5Response(password, Username, salt);
            WritePasswordMessage(response);
            await Writer.FlushAsync(ct);
        }
        else
        {
            throw new NotSupportedException($"Auth code {authCode} not supported");
        }
    }

    private void WritePasswordMessage(string password)
    {
        int passByteCount = Encoding.UTF8.GetByteCount(password);
        int messageLen = 4 + passByteCount + 1;
        int totalLen = 1 + messageLen;

        var mem = Writer.GetMemory(totalLen);

        mem.Span[0] = (byte)'p';
        BinaryPrimitives.WriteInt32BigEndian(mem.Span.Slice(1), messageLen);
        Encoding.UTF8.GetBytes(password, mem.Span.Slice(5));
        mem.Span[5 + passByteCount] = 0;

        Writer.Advance(totalLen);
    }

    private static string CalculateMD5Response(string password, string username, byte[] salt)
    {
        string shadow;
        if (password.StartsWith("md5") && password.Length == 35)
        {
            shadow = password;
        }
        else
        {
            shadow = "md5" + CreateMD5(password + username);
        }

        var shadowHash = shadow.Substring(3);

        using var md5 = MD5.Create();
        var shadowBytes = Encoding.UTF8.GetBytes(shadowHash);
        var input = new byte[shadowBytes.Length + salt.Length];
        shadowBytes.CopyTo(input, 0);
        salt.CopyTo(input, shadowBytes.Length);

        var hash = md5.ComputeHash(input);
        return "md5" + Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static string CreateMD5(string input)
    {
        using var md5 = MD5.Create();
        var bytes = Encoding.UTF8.GetBytes(input);
        var hash = md5.ComputeHash(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public void AttachHandler(IBackendHandler handler)
    {
        if (_handler != null)
        {
            throw new InvalidOperationException("Cannot attach handler: backend is already in use by another session. Missing Detach?");
        }
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        Generation++;
        UpdateActivity();
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

    public async Task ExecuteResetQueryAsync()
    {
        if (IsBroken) return;

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ResetHandler(tcs);

        try
        {
            AttachHandler(handler);

            var query = "DISCARD ALL";
            var queryBytes = Encoding.UTF8.GetBytes(query);
            var len = 4 + queryBytes.Length + 1;

            var mem = Writer.GetMemory(1 + len);

            mem.Span[0] = (byte)'Q';
            BinaryPrimitives.WriteInt32BigEndian(mem.Span.Slice(1), len);
            queryBytes.CopyTo(mem.Span.Slice(5));
            mem.Span[5 + queryBytes.Length] = 0;

            Writer.Advance(1 + len);
            await Writer.FlushAsync();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var reg = cts.Token.Register(() => tcs.TrySetCanceled());

            await tcs.Task;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to execute reset query on connection {Id}", Id);
            MarkAsBroken();
            throw;
        }
        finally
        {
            DetachHandler();
        }
    }

    private class ResetHandler : IBackendHandler
    {
        private readonly TaskCompletionSource _tcs;

        public ResetHandler(TaskCompletionSource tcs) => _tcs = tcs;

        public ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)
        {
            if (messageType == (byte)'Z')
            {
                _tcs.TrySetResult();
            }
            else if (messageType == (byte)'E')
            {
                _tcs.TrySetException(new Exception("Reset query failed (ErrorResponse received)"));
            }
            return ValueTask.CompletedTask;
        }

        public void OnBackendDisconnected(Exception? ex)
        {
            _tcs.TrySetException(ex ?? new Exception("Backend disconnected during reset"));
        }
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            while (true)
            {
                var result = await _reader.ReadAsync(_readLoopCts.Token);
                var buffer = result.Buffer;

                while (TryParseMessage(buffer, out var message, out var msgType, out var remainder))
                {
                    buffer = remainder;
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
        catch (OperationCanceledException)
        {
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

    private bool TryParseMessage(ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> message, out byte msgType, out ReadOnlySequence<byte> remainder)
    {
        message = default;
        msgType = 0;
        remainder = buffer;

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
        remainder = buffer.Slice(total);
        return true;
    }

    private void ProcessIdleMessage(byte msgType)
    {
        char typeChar = (char)msgType;
        switch (typeChar)
        {
            case 'N':
            case 'S':
            case 'A':
            case 'Z':
            case '1':
            case '2':
            case 'T':
            case 'D':
            case 'C':
                break;
            case 'E':
                MarkAsBroken();
                break;
            default:
                MarkAsBroken();
                break;
        }
    }

    public void MarkAsBroken() => Interlocked.Exchange(ref _isBroken, 1);

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
