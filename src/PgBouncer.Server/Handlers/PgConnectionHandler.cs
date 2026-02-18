using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Authentication;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server.Handlers;

public class PgConnectionHandler : ConnectionHandler, IBackendHandler
{
    private readonly PgBouncerConfig _config;
    private readonly PoolManager _poolManager;
    private readonly UserRegistry _userRegistry;
    private readonly ProxyServer _proxyServer;
    private readonly ILogger<PgConnectionHandler> _logger;
    private readonly X509Certificate2? _sslCert;

    private PipeWriter? _clientWriter;
    private IConnectionPool? _pool;
    private volatile IServerConnection? _currentBackend;
    private readonly object _backendLock = new();
    private readonly TransactionTracker _txTracker = new();
    private Guid _sessionId;
    private SessionInfo? _sessionInfo;

    public PgConnectionHandler(
        PgBouncerConfig config,
        PoolManager poolManager,
        UserRegistry userRegistry,
        ProxyServer proxyServer,
        ILogger<PgConnectionHandler> logger,
        IEnumerable<X509Certificate2> sslCerts)
    {
        _config = config;
        _poolManager = poolManager;
        _userRegistry = userRegistry;
        _proxyServer = proxyServer;
        _logger = logger;
        _sslCert = System.Linq.Enumerable.FirstOrDefault(sslCerts);
    }

    public override async Task OnConnectedAsync(ConnectionContext connection)
    {
        _sessionId = Guid.NewGuid();
        _sessionInfo = new SessionInfo
        {
            Id = _sessionId,
            StartedAt = DateTime.UtcNow,
            RemoteEndPoint = connection.RemoteEndPoint?.ToString() ?? "unknown",
            State = SessionState.Connecting
        };

        _proxyServer.RegisterSession(_sessionInfo);
        _logger.LogInformation("[Session {Id}] Connected from {RemoteEndPoint}", _sessionId, _sessionInfo.RemoteEndPoint);

        PipeReader clientReader = connection.Transport.Input;
        PipeWriter clientWriter = connection.Transport.Output;
        _clientWriter = clientWriter;

        try
        {
            if (_config.Ssl.Enabled)
            {
                (clientReader, clientWriter) = await HandleSslAsync(connection, clientReader, clientWriter);
                _clientWriter = clientWriter;
            }

            var (authSuccess, database, username, password) = await HandleStartupAndAuthAsync(clientReader, clientWriter);
            if (!authSuccess)
            {
                _logger.LogWarning("[Session {Id}] Authentication failed", _sessionId);
                return;
            }

            _sessionInfo.Database = database;
            _sessionInfo.Username = username;
            _sessionInfo.State = SessionState.Active;

            _pool = await _poolManager.GetPoolAsync(database!, username!, password!);

            await RunMainLoopAsync(clientReader, connection.ConnectionClosed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Error in connection handler", _sessionId);
            _sessionInfo.State = SessionState.Error;
        }
        finally
        {
            ReleaseBackend(force: true);
            _proxyServer.UnregisterSession(_sessionId);
            _logger.LogInformation("[Session {Id}] Disconnected", _sessionId);
        }
    }

    public async ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)
    {
        if (_clientWriter == null) return;

        try
        {
            if (message.IsSingleSegment)
                await _clientWriter.WriteAsync(message.First);
            else
                foreach (var segment in message) await _clientWriter.WriteAsync(segment);

            if (messageType == (byte)'Z')
            {
                byte status = 0;
                if (message.Length >= 6)
                {
                    var payload = message.Slice(5);
                    status = payload.FirstSpan[0];
                }

                _txTracker.ProcessReadyForQuery(status);

                if (_txTracker.State == TransactionState.Idle)
                {
                    await _clientWriter.FlushAsync();
                    ReleaseBackendIfIdle();
                    return;
                }
            }

            if (messageType != (byte)'Z')
            {
                 await _clientWriter.FlushAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Error handling backend message", _sessionId);
            ReleaseBackend(force: true);
            throw;
        }
    }

    public void OnBackendDisconnected(Exception? ex)
    {
        _logger.LogWarning(ex, "[Session {Id}] Backend disconnected", _sessionId);
        _clientWriter?.Complete(ex);
    }

    private async Task RunMainLoopAsync(PipeReader clientReader, CancellationToken connectionClosed)
    {
        try
        {
            while (true)
            {
                var result = await clientReader.ReadAsync(connectionClosed);
                var buffer = result.Buffer;

                if (result.IsCanceled || (buffer.IsEmpty && result.IsCompleted))
                    break;

                while (TryReadPacket(buffer, out var packet, out var type, out var remainder))
                {
                    buffer = remainder;
                    char typeChar = (char)type;

                    if (typeChar == 'X')
                    {
                        return;
                    }

                    await EnsureBackendAsync(connectionClosed);

                    var backend = _currentBackend;
                    if (backend != null)
                    {
                        if (packet.IsSingleSegment)
                            await backend.Writer.WriteAsync(packet.First, connectionClosed);
                        else
                            foreach (var seg in packet) await backend.Writer.WriteAsync(seg, connectionClosed);

                        await backend.Writer.FlushAsync(connectionClosed);
                    }
                }

                clientReader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        finally
        {
        }
    }

    private async Task EnsureBackendAsync(CancellationToken cancellationToken)
    {
        if (_currentBackend != null && _currentBackend.IsHealthy) return;
        if (_currentBackend != null) ReleaseBackend(force: true);

        var backend = await _pool!.AcquireAsync(cancellationToken);

        lock (_backendLock)
        {
            _currentBackend = backend;
            backend.AttachHandler(this);
            backend.StartReaderLoop();
        }
    }

    private void ReleaseBackendIfIdle()
    {
        IServerConnection? backend = null;
        lock (_backendLock)
        {
            backend = _currentBackend;
            _currentBackend = null;
        }

        if (backend != null)
        {
            backend.DetachHandler();
            _pool?.Release(backend);
        }
    }

    private void ReleaseBackend(bool force = false)
    {
        IServerConnection? backend = null;
        lock (_backendLock)
        {
            backend = _currentBackend;
            _currentBackend = null;
        }

        if (backend != null)
        {
            backend.DetachHandler();
            _pool?.Release(backend);
        }
    }

    private async Task<(PipeReader, PipeWriter)> HandleSslAsync(ConnectionContext connection, PipeReader reader, PipeWriter writer)
    {
        var result = await reader.ReadAsync();
        var buffer = result.Buffer;

        if (buffer.Length < 8)
        {
            reader.AdvanceTo(buffer.Start, buffer.End);
            return (reader, writer);
        }

        var head = buffer.Slice(0, 8);
        var headArray = new byte[8];
        head.CopyTo(headArray);

        int length = BinaryPrimitives.ReadInt32BigEndian(headArray);
        int code = BinaryPrimitives.ReadInt32BigEndian(headArray.AsSpan(4));

        if (code == 80877103)
        {
            reader.AdvanceTo(buffer.Slice(8).Start);
            await writer.WriteAsync(new byte[] { (byte)'S' });
            await writer.FlushAsync();

            var cert = _sslCert ?? new X509Certificate2(_config.Ssl.CertificatePath, _config.Ssl.Password);
            var sslStream = new SslStream(new DuplexPipeStream(reader, writer), false);

            await sslStream.AuthenticateAsServerAsync(cert);

            var newReader = PipeReader.Create(sslStream, new StreamPipeReaderOptions(leaveOpen: true));
            var newWriter = PipeWriter.Create(sslStream, new StreamPipeWriterOptions(leaveOpen: true));
            return (newReader, newWriter);
        }
        else
        {
            reader.AdvanceTo(buffer.Start);
            return (reader, writer);
        }
    }

    private async Task<(bool Success, string? Database, string? Username, string? Password)> HandleStartupAndAuthAsync(PipeReader reader, PipeWriter writer)
    {
        while (true)
        {
            var result = await reader.ReadAsync();
            var buffer = result.Buffer;

            if (TryParseStartup(buffer, out var length, out var protocol, out var parameters))
            {
                reader.AdvanceTo(buffer.Slice(length).Start);

                if (protocol == 80877103)
                {
                    await writer.WriteAsync(new byte[] { (byte)'N' });
                    await writer.FlushAsync();
                    continue;
                }

                if (protocol != 196608)
                {
                    _logger.LogError("[Session {Id}] Unsupported protocol: {Ver}", _sessionId, protocol);
                    return (false, null, null, null);
                }

                var database = parameters.GetValueOrDefault("database") ?? "postgres";
                var username = parameters.GetValueOrDefault("user") ?? "postgres";

                if (!await PerformAuthAsync(writer, reader, username))
                {
                    await SendErrorAsync(writer, "FATAL", "08P01", "Authentication failed");
                    return (false, null, null, null);
                }

                var shadow = _userRegistry.GetShadowPassword(username);
                var password = shadow != null && !shadow.StartsWith("md5") && !shadow.StartsWith("SCRAM-SHA-256")
                    ? shadow
                    : _config.Backend.AdminPassword;

                await SendAuthOkAsync(writer);
                return (true, database, username, password);
            }

            if (result.IsCompleted) return (false, null, null, null);
            reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private bool TryParseStartup(ReadOnlySequence<byte> buffer, out int length, out int protocol, out Dictionary<string, string> parameters)
    {
        length = 0;
        protocol = 0;
        parameters = new Dictionary<string, string>();

        if (buffer.Length < 8) return false;

        var head = new byte[8];
        buffer.Slice(0, 8).CopyTo(head);
        length = BinaryPrimitives.ReadInt32BigEndian(head);
        protocol = BinaryPrimitives.ReadInt32BigEndian(head.AsSpan(4));

        if (buffer.Length < length) return false;

        if (protocol == 196608)
        {
            var data = buffer.Slice(8, length - 8);
            var array = data.ToArray();
            int pos = 0;
            while (pos < array.Length)
            {
                int end = Array.IndexOf(array, (byte)0, pos);
                if (end < 0) break;
                string key = Encoding.UTF8.GetString(array, pos, end - pos);
                pos = end + 1;
                if (pos >= array.Length) break;
                end = Array.IndexOf(array, (byte)0, pos);
                if (end < 0) break;
                string val = Encoding.UTF8.GetString(array, pos, end - pos);
                pos = end + 1;
                parameters[key] = val;
            }
        }
        return true;
    }

    private async Task<bool> PerformAuthAsync(PipeWriter writer, PipeReader reader, string username)
    {
        var authType = _config.Auth.Type.ToLowerInvariant();
        if (authType == "trust") return true;

        var shadow = _userRegistry.GetShadowPassword(username);
        if (shadow == null)
        {
            _logger.LogWarning("User {User} not found", username);
            return false;
        }

        if (authType == "md5")
        {
             var salt = new byte[4];
             Random.Shared.NextBytes(salt);

             using var ms = new MemoryStream();
             ms.WriteByte((byte)'R');
             WriteInt32(ms, 12);
             WriteInt32(ms, 5);
             ms.Write(salt);
             await writer.WriteAsync(ms.ToArray());
             await writer.FlushAsync();

             while (true)
             {
                 var res = await reader.ReadAsync();
                 var buf = res.Buffer;
                 if (TryReadPacket(buf, out var msg, out var type, out var rem))
                 {
                     reader.AdvanceTo(rem.Start);
                     if (type == 'p')
                     {
                         var passStr = Encoding.UTF8.GetString(msg.Slice(5, msg.Length - 6).ToArray());
                         string storedHash = shadow.StartsWith("md5") ? shadow.Substring(3) : CreateMD5(shadow + username);
                         if (VerifyMD5(storedHash, salt, passStr)) return true;
                         return false;
                     }
                     return false;
                 }
                 reader.AdvanceTo(buf.Start, buf.End);
                 if (res.IsCompleted) return false;
             }
        }
        else if (authType == "scram-sha-256")
        {
            var scram = new ScramServerAuthenticator(username, shadow);

            using var ms = new MemoryStream();
            ms.WriteByte((byte)'R');
            var mech = Encoding.UTF8.GetBytes("SCRAM-SHA-256");
            int len = 4 + 4 + mech.Length + 1 + 1;
            WriteInt32(ms, len);
            WriteInt32(ms, 10);
            ms.Write(mech);
            ms.WriteByte(0);
            ms.WriteByte(0);
            await writer.WriteAsync(ms.ToArray());
            await writer.FlushAsync();

             while (true)
             {
                 var res = await reader.ReadAsync();
                 var buf = res.Buffer;
                 if (TryReadPacket(buf, out var msg, out var type, out var rem))
                 {
                     reader.AdvanceTo(rem.Start);
                     if (type == 'p')
                     {
                         var payload = msg.Slice(5).ToArray();
                         var packet = ProcessScramInit(payload, scram);
                         if (packet == null) return false;

                         await writer.WriteAsync(packet);
                         await writer.FlushAsync();
                         break;
                     }
                     return false;
                 }
                 reader.AdvanceTo(buf.Start, buf.End);
                 if (res.IsCompleted) return false;
             }

             while (true)
             {
                 var res = await reader.ReadAsync();
                 var buf = res.Buffer;
                 if (TryReadPacket(buf, out var msg, out var type, out var rem))
                 {
                     reader.AdvanceTo(rem.Start);
                     if (type == 'p')
                     {
                         var payload = msg.Slice(5).ToArray();
                         var packet = ProcessScramFinal(payload, scram);
                         if (packet == null) return false;

                         await writer.WriteAsync(packet);
                         await writer.FlushAsync();
                         return true;
                     }
                     return false;
                 }
                 reader.AdvanceTo(buf.Start, buf.End);
                 if (res.IsCompleted) return false;
             }
        }
        return false;
    }

    private byte[]? ProcessScramInit(byte[] payloadArr, ScramServerAuthenticator scram)
    {
         int idx = Array.IndexOf(payloadArr, (byte)0);
         if (idx < 0) return null;

         int clientMsgLen = BinaryPrimitives.ReadInt32BigEndian(payloadArr.AsSpan(idx + 1));
         var clientMsg = payloadArr.AsSpan(idx + 5, clientMsgLen);

         var serverFirst = scram.ProcessClientFirstMessage(clientMsg, out var err);
         if (serverFirst.Length == 0) return null;

         using var respMs = new MemoryStream();
         respMs.WriteByte((byte)'R');
         var b = new byte[4];
         BinaryPrimitives.WriteInt32BigEndian(b, 4 + 4 + serverFirst.Length);
         respMs.Write(b);
         BinaryPrimitives.WriteInt32BigEndian(b, 11);
         respMs.Write(b);
         respMs.Write(serverFirst);
         return respMs.ToArray();
    }

    private byte[]? ProcessScramFinal(byte[] payloadArr, ScramServerAuthenticator scram)
    {
         var serverFinal = scram.ProcessClientFinalMessage(payloadArr, out var err);
         if (serverFinal.Length == 0) return null;

         using var respMs = new MemoryStream();
         respMs.WriteByte((byte)'R');
         var b = new byte[4];
         BinaryPrimitives.WriteInt32BigEndian(b, 4 + 4 + serverFinal.Length);
         respMs.Write(b);
         BinaryPrimitives.WriteInt32BigEndian(b, 12);
         respMs.Write(b);
         respMs.Write(serverFinal);
         return respMs.ToArray();
    }

    private bool VerifyMD5(string storedHash, byte[] salt, string clientResponse)
    {
        var hexDigestBytes = Encoding.UTF8.GetBytes(storedHash);
        var input = new byte[hexDigestBytes.Length + salt.Length];
        hexDigestBytes.CopyTo(input, 0);
        salt.CopyTo(input, hexDigestBytes.Length);

        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(input);
        var expected = "md5" + Convert.ToHexString(hash).ToLowerInvariant();
        return expected == clientResponse;
    }

    private string CreateMD5(string input)
    {
        using var md5 = MD5.Create();
        var bytes = Encoding.UTF8.GetBytes(input);
        var hash = md5.ComputeHash(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private void WriteInt32(Stream s, int val)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(b, val);
        s.Write(b);
    }

    private async Task SendErrorAsync(PipeWriter writer, string severity, string code, string message)
    {
        using var ms = new MemoryStream();
        ms.WriteByte((byte)'E');
        ms.Write(new byte[4]);
        ms.WriteByte((byte)'S');
        ms.Write(Encoding.UTF8.GetBytes(severity));
        ms.WriteByte(0);
        ms.WriteByte((byte)'C');
        ms.Write(Encoding.UTF8.GetBytes(code));
        ms.WriteByte(0);
        ms.WriteByte((byte)'M');
        ms.Write(Encoding.UTF8.GetBytes(message));
        ms.WriteByte(0);
        ms.WriteByte(0);
        var len = (int)ms.Length - 1;
        ms.Position = 1;
        WriteInt32(ms, len);
        await writer.WriteAsync(ms.ToArray());
        await writer.FlushAsync();
    }

    private async Task SendAuthOkAsync(PipeWriter writer)
    {
        using var ms = new MemoryStream();
        ms.WriteByte((byte)'R');
        WriteInt32(ms, 8);
        WriteInt32(ms, 0);
        WriteParam(ms, "server_version", "16.0");
        WriteParam(ms, "client_encoding", "UTF8");
        WriteParam(ms, "server_encoding", "UTF8");
        WriteParam(ms, "DateStyle", "ISO, MDY");
        ms.WriteByte((byte)'K');
        WriteInt32(ms, 12);
        WriteInt32(ms, 1234);
        WriteInt32(ms, 5678);
        ms.WriteByte((byte)'Z');
        WriteInt32(ms, 5);
        ms.WriteByte((byte)'I');
        await writer.WriteAsync(ms.ToArray());
        await writer.FlushAsync();
    }

    private void WriteParam(Stream ms, string key, string val)
    {
        var k = Encoding.UTF8.GetBytes(key);
        var v = Encoding.UTF8.GetBytes(val);
        ms.WriteByte((byte)'S');
        WriteInt32(ms, 4 + k.Length + 1 + v.Length + 1);
        ms.Write(k);
        ms.WriteByte(0);
        ms.Write(v);
        ms.WriteByte(0);
    }

    private bool TryReadPacket(ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> packet, out byte type, out ReadOnlySequence<byte> remainder)
    {
        packet = default;
        type = 0;
        remainder = buffer;
        if (buffer.Length < 5) return false;

        var head = buffer.Slice(0, 5);
        var headArray = new byte[5];
        head.CopyTo(headArray);

        type = headArray[0];
        int len = BinaryPrimitives.ReadInt32BigEndian(headArray.AsSpan(1));
        if (buffer.Length < 1 + len) return false;

        packet = buffer.Slice(0, 1 + len);
        remainder = buffer.Slice(1 + len);
        return true;
    }
}
