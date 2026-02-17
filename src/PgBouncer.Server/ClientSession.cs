using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;
using PgBouncer.Core.Authentication;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Server;

public sealed class ClientSession : IBackendHandler, IDisposable
{
    private const int MaxRetries = 2;

    private readonly Socket _clientSocket;
    private readonly PipeReader _clientReader;
    private readonly PipeWriter _clientWriter;
    private readonly PgBouncerConfig _config;
    private readonly PoolManager _poolManager;
    private readonly UserRegistry _userRegistry;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;

    private BackendConnection? _currentBackend;
    private IConnectionPool? _pool;
    private string? _database;
    private string? _username;
    private string? _password;
    private int _pendingQueries;
    private int _isAcquiring;
    private readonly object _backendLock = new();

    public ClientSession(
        Socket clientSocket,
        PgBouncerConfig config,
        PoolManager poolManager,
        UserRegistry userRegistry,
        ILogger logger,
        SessionInfo sessionInfo)
    {
        _clientSocket = clientSocket;
        _config = config;
        _poolManager = poolManager;
        _userRegistry = userRegistry;
        _logger = logger;
        _sessionInfo = sessionInfo;

        var stream = new NetworkStream(_clientSocket, ownsSocket: false);
        _clientReader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
        _clientWriter = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            bool authenticated = await HandleStartupAsync(cancellationToken);
            if (!authenticated)
            {
                _logger.LogDebug("[Session {Id}] Authentication failed", _sessionInfo.Id);
                return;
            }

            _logger.LogInformation("[Session {Id}] Client connected: {Database}/{User}", 
                _sessionInfo.Id, _database, _username);

            await RunMainLoopAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Error", _sessionInfo.Id);
        }
        finally
        {
            ReleaseBackend();
            _clientSocket.Dispose();
        }
    }

    private async Task<bool> HandleStartupAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            ReadResult result = await _clientReader.ReadAsync(cancellationToken);
            ReadOnlySequence<byte> buffer = result.Buffer;

            if (buffer.Length >= 8)
            {
                if (!TryReadStartupHeader(buffer, out int length, out int protocolCode))
                {
                    _clientReader.AdvanceTo(buffer.Start, buffer.End);
                    if (result.IsCompleted) return false;
                    continue;
                }

                if (buffer.Length < length)
                {
                    _clientReader.AdvanceTo(buffer.Start, buffer.End);
                    if (result.IsCompleted) return false;
                    continue;
                }

                if (protocolCode == 80877103) // SSLRequest
                {
                    _logger.LogDebug("[Session {Id}] SSLRequest - denying", _sessionInfo.Id);
                    _clientReader.AdvanceTo(buffer.Slice(length).Start);
                    await _clientWriter.WriteAsync(new byte[] { (byte)'N' }, cancellationToken);
                    await _clientWriter.FlushAsync(cancellationToken);
                    continue;
                }
                else if (protocolCode == 196608) // Protocol 3.0
                {
                    var parameters = new Dictionary<string, string>();
                    
                    if (length > 8)
                    {
                        var paramBytes = buffer.Slice(8, length - 8).ToArray();
                        ParseParameters(paramBytes, parameters);
                    }
                    
                    _database = parameters.GetValueOrDefault("database") ?? "postgres";
                    _username = parameters.GetValueOrDefault("user") ?? "postgres";

                    _sessionInfo.Database = _database;
                    _sessionInfo.Username = _username;

                    _clientReader.AdvanceTo(buffer.Slice(length).Start);

                    if (!await PerformAuthenticationAsync(cancellationToken))
                    {
                        await SendFatalErrorToClientAsync("Authentication failed");
                        return false;
                    }

                    var shadow = _userRegistry.GetShadowPassword(_username);
                    if (shadow != null && !shadow.StartsWith("md5"))
                    {
                        _password = shadow;
                    }
                    else
                    {
                        _password = _config.Backend.AdminPassword;
                    }

                    _pool = await _poolManager.GetPoolAsync(_database, _username, _password);

                    await SendAuthenticationOkAsync(cancellationToken);
                    return true;
                }
                else
                {
                    _logger.LogWarning("[Session {Id}] Unknown protocol: {Code}", _sessionInfo.Id);
                    return false;
                }
            }

            _clientReader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted) return false;
        }
    }

    private void ParseParameters(byte[] data, Dictionary<string, string> parameters)
    {
        int pos = 0;
        while (pos < data.Length)
        {
            int keyEnd = Array.IndexOf(data, (byte)0, pos);
            if (keyEnd < 0 || keyEnd == pos) break;

            var key = Encoding.UTF8.GetString(data, pos, keyEnd - pos);
            pos = keyEnd + 1;

            if (pos >= data.Length) break;
            
            int valEnd = Array.IndexOf(data, (byte)0, pos);
            if (valEnd < 0) break;

            var value = Encoding.UTF8.GetString(data, pos, valEnd - pos);
            pos = valEnd + 1;

            parameters[key] = value;
        }
    }

    private async Task<bool> PerformAuthenticationAsync(CancellationToken cancellationToken)
    {
        var authType = _config.Auth.Type.ToLowerInvariant();
        var username = _username ?? "postgres";

        if (authType == "trust") return true;

        if (authType == "md5")
        {
            var secret = _userRegistry.GetShadowPassword(username);
            if (secret == null)
            {
                _logger.LogWarning("[Session {Id}] User {User} not found in registry", _sessionInfo.Id, username);
                return false;
            }

            var salt = new byte[4];
            Random.Shared.NextBytes(salt);

            using var ms = new MemoryStream();
            ms.WriteByte((byte)'R');
            WriteInt32BigEndian(ms, 12);
            WriteInt32BigEndian(ms, 5);
            ms.Write(salt);
            await _clientWriter.WriteAsync(ms.ToArray(), cancellationToken);
            await _clientWriter.FlushAsync(cancellationToken);

            while (true)
            {
                var result = await _clientReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (TryParseFrontendMessage(buffer, out var message, out var msgType, out var remainder))
                {
                    buffer = remainder;

                    if (msgType != (byte)'p')
                    {
                        _logger.LogWarning("[Session {Id}] Expected PasswordMessage (p), got {Type}", _sessionInfo.Id, (char)msgType);
                        return false;
                    }

                    var payload = message.Slice(5);
                    var end = payload.PositionOf((byte)0);
                    if (end == null) return false;

                    var passwordSpan = payload.Slice(0, end.Value);
                    var clientResponse = Encoding.UTF8.GetString(passwordSpan.ToArray());

                    _clientReader.AdvanceTo(buffer.Start);

                    if (!secret.StartsWith("md5"))
                    {
                        secret = "md5" + CreateMD5(secret + username);
                    }

                    var shadowHash = secret.Substring(3);

                    if (VerifyMD5(shadowHash, salt, clientResponse))
                    {
                        return true;
                    }
                    else
                    {
                        _logger.LogWarning("[Session {Id}] MD5 verification failed for user {User}", _sessionInfo.Id, username);
                        return false;
                    }
                }

                _clientReader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) return false;
            }
        }
        else if (authType == "plain")
        {
             _logger.LogWarning("Plain auth not implemented yet");
             return false;
        }

        return true;
    }

    private bool VerifyMD5(string shadowHash, byte[] salt, string clientResponse)
    {
        using var md5 = MD5.Create();
        var shadowBytes = Encoding.UTF8.GetBytes(shadowHash);
        var input = new byte[shadowBytes.Length + salt.Length];
        shadowBytes.CopyTo(input, 0);
        salt.CopyTo(input, shadowBytes.Length);

        var hash = md5.ComputeHash(input);
        var expected = "md5" + Convert.ToHexString(hash).ToLowerInvariant();

        return expected == clientResponse;
    }

    private static string CreateMD5(string input)
    {
        using var md5 = MD5.Create();
        var bytes = Encoding.UTF8.GetBytes(input);
        var hash = md5.ComputeHash(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private bool TryReadStartupHeader(ReadOnlySequence<byte> buffer, out int length, out int protocolCode)
    {
        length = 0;
        protocolCode = 0;

        if (buffer.Length < 8) return false;

        Span<byte> header = stackalloc byte[8];
        buffer.Slice(0, 8).CopyTo(header);
        length = BinaryPrimitives.ReadInt32BigEndian(header);
        protocolCode = BinaryPrimitives.ReadInt32BigEndian(header.Slice(4));
        return true;
    }

    private async Task SendAuthenticationOkAsync(CancellationToken cancellationToken)
    {
        using var ms = new MemoryStream();
        
        // AuthenticationOk
        ms.WriteByte((byte)'R');
        WriteInt32BigEndian(ms, 8);
        WriteInt32BigEndian(ms, 0);
        
        // ParameterStatus messages
        WriteParameterStatus(ms, "server_version", "16.0");
        WriteParameterStatus(ms, "server_encoding", "UTF8");
        WriteParameterStatus(ms, "client_encoding", "UTF8");
        WriteParameterStatus(ms, "DateStyle", "ISO, MDY");
        WriteParameterStatus(ms, "TimeZone", "UTC");
        WriteParameterStatus(ms, "integer_datetimes", "on");
        WriteParameterStatus(ms, "standard_conforming_strings", "on");
        
        // BackendKeyData
        ms.WriteByte((byte)'K');
        WriteInt32BigEndian(ms, 12);
        WriteInt32BigEndian(ms, Environment.ProcessId);
        WriteInt32BigEndian(ms, Random.Shared.Next());
        
        // ReadyForQuery
        ms.WriteByte((byte)'Z');
        WriteInt32BigEndian(ms, 5);
        ms.WriteByte((byte)'I');

        await _clientWriter.WriteAsync(ms.ToArray(), cancellationToken);
    }

    private static void WriteParameterStatus(MemoryStream ms, string name, string value)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var valueBytes = Encoding.UTF8.GetBytes(value);
        var length = 4 + nameBytes.Length + 1 + valueBytes.Length + 1;

        ms.WriteByte((byte)'S');
        WriteInt32BigEndian(ms, length);
        ms.Write(nameBytes);
        ms.WriteByte(0);
        ms.Write(valueBytes);
        ms.WriteByte(0);
    }

    private static void WriteInt32BigEndian(MemoryStream ms, int value)
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buf, value);
        ms.Write(buf);
    }

    private async Task RunMainLoopAsync(CancellationToken cancellationToken)
    {
        if (_pool == null)
            return;

        try
        {
            while (true)
            {
                ReadResult result = await _clientReader.ReadAsync(cancellationToken);
                ReadOnlySequence<byte> buffer = result.Buffer;

                if (result.IsCompleted || result.IsCanceled)
                    break;

                while (TryParseFrontendMessage(buffer, out ReadOnlySequence<byte> message, out byte msgType, out var remainder))
                {
                    buffer = remainder;
                    cancellationToken.ThrowIfCancellationRequested();

                    char typeChar = (char)msgType;
                    if (typeChar == 'X')
                    {
                        _logger.LogDebug("[Session {Id}] Terminate", _sessionInfo.Id);
                        return;
                    }

                    bool isQueryOrSync = (typeChar == 'Q' || typeChar == 'S');
                    
                    if (isQueryOrSync)
                    {
                        Interlocked.Increment(ref _pendingQueries);
                    }

                    bool messageSent = false;
                    int currentRetry = 0;

                    while (!messageSent)
                    {
                        BackendConnection backend = await GetOrAcquireBackendAsync(cancellationToken);

                        try
                        {
                            if (message.IsSingleSegment)
                            {
                                await backend.Writer.WriteAsync(message.First, cancellationToken);
                            }
                            else
                            {
                                foreach (var segment in message)
                                {
                                    await backend.Writer.WriteAsync(segment, cancellationToken);
                                }
                            }
                            
                            await backend.Writer.FlushAsync(cancellationToken);
                            messageSent = true;
                        }
                        catch (Exception ex) when (ex is IOException || ex is SocketException)
                        {
                            backend.MarkAsBroken();
                            ReleaseBackend();

                            if (currentRetry >= MaxRetries || Volatile.Read(ref _pendingQueries) > 1)
                            {
                                _logger.LogWarning("[Session {Id}] Retry failed after {Retries} attempts: {Message}", 
                                    _sessionInfo.Id, currentRetry, ex.Message);
                                if (isQueryOrSync) Interlocked.Decrement(ref _pendingQueries);
                                throw;
                            }

                            currentRetry++;
                            _logger.LogDebug("[Session {Id}] Transparent retry {Retry}/{Max}: {Message}", 
                                _sessionInfo.Id, currentRetry, MaxRetries, ex.Message);
                        }
                        catch
                        {
                            backend.MarkAsBroken();
                            ReleaseBackend();
                            if (isQueryOrSync) Interlocked.Decrement(ref _pendingQueries);
                            throw;
                        }
                    }
                }

                _clientReader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        finally
        {
            ReleaseBackend();
        }
    }

    private async ValueTask<BackendConnection> GetOrAcquireBackendAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _isAcquiring);
        
        lock (_backendLock)
        {
            if (_currentBackend != null)
            {
                return _currentBackend;
            }
        }

        try
        {
            var conn = await _pool!.AcquireAsync(cancellationToken);
            var newBackend = (BackendConnection)conn;

            lock (_backendLock)
            {
                if (_currentBackend == null)
                {
                    _currentBackend = newBackend;
                    _currentBackend.AttachHandler(this);
                    return _currentBackend;
                }
                else
                {
                    _pool!.Release(newBackend);
                    return _currentBackend;
                }
            }
        }
        finally
        {
            Interlocked.Decrement(ref _isAcquiring);
        }
    }

    private bool TryParseFrontendMessage(
        ReadOnlySequence<byte> buffer,
        out ReadOnlySequence<byte> message,
        out byte messageType,
        out ReadOnlySequence<byte> remainder)
    {
        message = default;
        messageType = 0;
        remainder = buffer;

        if (buffer.Length < 5)
            return false;

        var firstSpan = buffer.FirstSpan;
        messageType = firstSpan[0];
        int length = BinaryPrimitives.ReadInt32BigEndian(firstSpan.Slice(1));
        int totalPacketLength = length + 1;

        if (buffer.Length < totalPacketLength)
            return false;

        message = buffer.Slice(0, totalPacketLength);
        remainder = buffer.Slice(totalPacketLength);
        return true;
    }

    public async ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)
    {
        try
        {
            if (message.IsSingleSegment)
            {
                _clientWriter.Write(message.FirstSpan);
            }
            else
            {
                foreach (ReadOnlyMemory<byte> segment in message)
                {
                    _clientWriter.Write(segment.Span);
                }
            }

            FlushResult flushResult = await _clientWriter.FlushAsync();

            if (flushResult.IsCanceled || flushResult.IsCompleted)
            {
                AbortSession();
                return;
            }

            if (messageType == (byte)'Z')
            {
                int pending = Interlocked.Decrement(ref _pendingQueries);
                
                if (pending < 0)
                {
                    Interlocked.Exchange(ref _pendingQueries, 0);
                    pending = 0;
                }

                if (pending == 0)
                {
                    ReleaseBackendIfIdle();
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("[Session {Id}] Dropping client connection: {Message}", _sessionInfo.Id, ex.Message);
            AbortSession();
        }
    }

    public void OnBackendDisconnected(Exception? ex)
    {
        _logger.LogWarning(ex, "[Session {Id}] Backend disconnected", _sessionInfo.Id);
        
        try
        {
            SendFatalErrorToClientAsync("Backend connection lost").Wait(TimeSpan.FromSeconds(2));
        }
        catch { }
        
        AbortSession();
    }

    private async Task SendFatalErrorToClientAsync(string message)
    {
        try
        {
            using var ms = new MemoryStream();
            ms.WriteByte((byte)'E');
            int lengthPosition = (int)ms.Position;
            ms.Write(new byte[4], 0, 4);
            
            ms.WriteByte((byte)'S');
            var severity = System.Text.Encoding.UTF8.GetBytes("FATAL");
            ms.Write(severity, 0, severity.Length);
            ms.WriteByte(0);
            
            ms.WriteByte((byte)'C');
            var code = System.Text.Encoding.UTF8.GetBytes("08006");
            ms.Write(code, 0, code.Length);
            ms.WriteByte(0);
            
            ms.WriteByte((byte)'M');
            var msg = System.Text.Encoding.UTF8.GetBytes(message);
            ms.Write(msg, 0, msg.Length);
            ms.WriteByte(0);
            
            ms.WriteByte(0);
            
            int totalLength = (int)ms.Position;
            ms.Position = lengthPosition;
            byte[] lengthBytes = new byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(lengthBytes, totalLength - 1);
            ms.Write(lengthBytes, 0, 4);
            
            await _clientWriter.WriteAsync(ms.ToArray());
            await _clientWriter.FlushAsync();
        }
        catch { }
    }

    private void AbortSession()
    {
        ReleaseBackend();
        
        try 
        {
            _clientSocket.Close(); 
        }
        catch { }
    }

    private void ReleaseBackendIfIdle()
    {
        BackendConnection? backendToRelease = null;

        lock (_backendLock)
        {
            if (Volatile.Read(ref _pendingQueries) > 0)
                return;

            if (Volatile.Read(ref _isAcquiring) > 0)
                return;

            backendToRelease = _currentBackend;
            _currentBackend = null;
        }

        if (backendToRelease != null)
        {
            backendToRelease.DetachHandler();
            _pool?.Release(backendToRelease);
        }
    }

    private void ReleaseBackend()
    {
        BackendConnection? backendToRelease = null;

        lock (_backendLock)
        {
            backendToRelease = _currentBackend;
            _currentBackend = null;
        }

        if (backendToRelease != null)
        {
            backendToRelease.DetachHandler();
            _pool?.Release(backendToRelease);
        }
    }

    public void Dispose()
    {
        ReleaseBackend();
        _clientReader.Complete();
        _clientWriter.Complete();
        _clientSocket.Dispose();
    }
}
