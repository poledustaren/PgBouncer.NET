using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Server;

public sealed class ClientSession : IBackendHandler, IDisposable
{
    private readonly Socket _clientSocket;
    private readonly PipeReader _clientReader;
    private readonly PipeWriter _clientWriter;
    private readonly PgBouncerConfig _config;
    private readonly PoolManager _poolManager;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;

    private BackendConnection? _currentBackend;
    private IConnectionPool? _pool;
    private string? _database;
    private string? _username;
    private string? _password;

    public ClientSession(
        Socket clientSocket,
        PgBouncerConfig config,
        PoolManager poolManager,
        ILogger logger,
        SessionInfo sessionInfo)
    {
        _clientSocket = clientSocket;
        _config = config;
        _poolManager = poolManager;
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
                    _password = _config.Backend.AdminPassword;

                    _sessionInfo.Database = _database;
                    _sessionInfo.Username = _username;

                    _pool = await _poolManager.GetPoolAsync(_database, _username, _password);

                    _clientReader.AdvanceTo(buffer.Slice(length).Start);
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
            // Ищем конец ключа (нулевой байт)
            int keyEnd = Array.IndexOf(data, (byte)0, pos);
            
            // Если не нашли нулевой байт или ключ пустой - выходим
            // Пустой ключ означает конец списка (два нулевых байта подряд)
            if (keyEnd < 0 || keyEnd == pos) break;

            var key = Encoding.UTF8.GetString(data, pos, keyEnd - pos);
            pos = keyEnd + 1;

            // Проверяем что есть место для значения
            if (pos >= data.Length) break;
            
            // Ищем конец значения (нулевой байт)
            int valEnd = Array.IndexOf(data, (byte)0, pos);
            if (valEnd < 0) break; // Не нашли - выходим

            var value = Encoding.UTF8.GetString(data, pos, valEnd - pos);
            pos = valEnd + 1;

            parameters[key] = value;
        }
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
        
        // ParameterStatus messages (required by Npgsql)
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
        while (true)
        {
            ReadResult result = await _clientReader.ReadAsync(cancellationToken);
            ReadOnlySequence<byte> buffer = result.Buffer;

            if (result.IsCompleted || result.IsCanceled)
                break;

            while (TryParseFrontendMessage(ref buffer, out ReadOnlySequence<byte> message, out byte msgType))
            {
                if ((char)msgType == 'X')
                {
                    _logger.LogDebug("[Session {Id}] Terminate", _sessionInfo.Id);
                    return;
                }

                if (_currentBackend == null)
                {
                    var conn = await _pool!.AcquireAsync(cancellationToken);
                    _currentBackend = (BackendConnection)conn;
                    _currentBackend.AttachHandler(this);
                }

                if (message.IsSingleSegment)
                {
                    await _currentBackend.Writer.WriteAsync(message.First, cancellationToken);
                }
                else
                {
                    foreach (var segment in message)
                    {
                        await _currentBackend.Writer.WriteAsync(segment, cancellationToken);
                    }
                }
                await _currentBackend.Writer.FlushAsync(cancellationToken);
            }

            _clientReader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private bool TryParseFrontendMessage(
        ref ReadOnlySequence<byte> buffer,
        out ReadOnlySequence<byte> message,
        out byte messageType)
    {
        message = default;
        messageType = 0;

        if (buffer.Length < 5)
            return false;

        var firstSpan = buffer.FirstSpan;
        messageType = firstSpan[0];
        int length = BinaryPrimitives.ReadInt32BigEndian(firstSpan.Slice(1));
        int totalPacketLength = length + 1;

        if (buffer.Length < totalPacketLength)
            return false;

        message = buffer.Slice(0, totalPacketLength);
        buffer = buffer.Slice(totalPacketLength);
        return true;
    }

    public async ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)
    {
        try
        {
            if (message.IsSingleSegment)
            {
                await _clientWriter.WriteAsync(message.First);
            }
            else
            {
                foreach (ReadOnlyMemory<byte> segment in message)
                {
                    await _clientWriter.WriteAsync(segment);
                }
            }

            if ((char)messageType == 'Z')
            {
                await _clientWriter.FlushAsync();
                ReleaseBackend();
            }
        }
        catch
        {
            ReleaseBackend();
            _clientSocket.Close();
        }
    }

    public void OnBackendDisconnected(Exception? ex)
    {
        _logger.LogWarning(ex, "[Session {Id}] Backend disconnected", _sessionInfo.Id);
        _currentBackend = null;
        _clientSocket.Close();
    }

    private void ReleaseBackend()
    {
        if (_currentBackend != null)
        {
            _currentBackend.DetachHandler();
            _pool!.Release(_currentBackend);
            _currentBackend = null;
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
