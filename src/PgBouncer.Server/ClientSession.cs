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
    
    // Состояние для защиты от Extended Query Protocol race condition
    private bool _isWritingToBackend = false;
    private bool _isMidExtendedQuery = false;
    private char _lastTxStatus = 'I'; // По умолчанию Idle

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
                    _logger.LogWarning("[Session {Id}] Unknown protocol: {Code}", _sessionInfo.Id, protocolCode);
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

                while (TryParseFrontendMessage(ref buffer, out ReadOnlySequence<byte> message, out byte msgType))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    char typeChar = (char)msgType;
                    if (typeChar == 'X')
                    {
                        _logger.LogDebug("[Session {Id}] Terminate", _sessionInfo.Id);
                        return;
                    }

                    // Защита от Extended Query Protocol race condition
                    lock (_backendLock)
                    {
                        _isWritingToBackend = true;
                        
                        if (typeChar == 'Q' || typeChar == 'S')
                        {
                            Interlocked.Increment(ref _pendingQueries);
                            _isMidExtendedQuery = false; // Блок завершен, ожидаем 'Z'
                        }
                        else if (typeChar is 'P' or 'B' or 'E' or 'D' or 'C')
                        {
                            // Сообщения типа Parse (P), Bind (B), Execute (E), Describe (D), Close (C)
                            // Означают, что мы начали длинную команду, и разрывать соединение НЕЛЬЗЯ
                            _isMidExtendedQuery = true; 
                        }
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
                            ReleaseBackend(force: true);

                            if (currentRetry >= MaxRetries || Volatile.Read(ref _pendingQueries) > 1)
                            {
                                _logger.LogWarning("[Session {Id}] Retry failed after {Retries} attempts: {Message}", 
                                    _sessionInfo.Id, currentRetry, ex.Message);
                                if (typeChar == 'Q' || typeChar == 'S') Interlocked.Decrement(ref _pendingQueries);
                                throw;
                            }

                            currentRetry++;
                            _logger.LogDebug("[Session {Id}] Transparent retry {Retry}/{Max}: {Message}", 
                                _sessionInfo.Id, currentRetry, MaxRetries, ex.Message);
                        }
                        catch
                        {
                            backend.MarkAsBroken();
                            ReleaseBackend(force: true);
                            if (typeChar == 'Q' || typeChar == 'S') Interlocked.Decrement(ref _pendingQueries);
                            throw;
                        }
                    }
                    
                    // После отправки пытаемся освободить backend
                    lock (_backendLock)
                    {
                        _isWritingToBackend = false;
                    }
                    TryReleaseBackendIfIdle();
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
            IServerConnection? conn = null;
            int retries = 0;
            while (true)
            {
                try
                {
                    conn = await _pool!.AcquireAsync(cancellationToken);
                    break;
                }
                catch (Exception ex) when (ex.Message.Contains("PostgreSQL authentication failed") && retries < 3)
                {
                    retries++;
                    _logger.LogWarning(ex, "[Session {Id}] Auth failure during acquire. Retrying ({Retry}/3)...", _sessionInfo.Id, retries);
                    await Task.Delay(retries * 50, cancellationToken);
                }
            }

            var newBackend = (BackendConnection)conn!;

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
                // Читаем 1 байт статуса транзакции из тела пакета
                // ReadyForQuery payload: 1 байт статуса ('I', 'T', или 'E')
                // Сообщение включает 5-байтный заголовок (тип + длина), статус на позиции 5
                char txStatus = (char)GetByteAtPosition(message, 5);

                int pending = Interlocked.Decrement(ref _pendingQueries);
                
                if (pending < 0)
                {
                    Interlocked.Exchange(ref _pendingQueries, 0);
                    pending = 0;
                }

                // Сохраняем статус транзакции
                lock (_backendLock)
                {
                    _lastTxStatus = txStatus;
                }

                TryReleaseBackendIfIdle();
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
        
        // Сначала отправляем ошибку клиенту (синхронно), затем закрываем
        try
        {
            SendFatalErrorToClientAsync("Backend connection lost").Wait(TimeSpan.FromSeconds(2));
        }
        catch { /* Игнорируем ошибки при отправке */ }
        
        AbortSession();
    }

    private async Task SendFatalErrorToClientAsync(string message)
    {
        try
        {
            // Формат ErrorResponse 'E': 'S' SEVERITY \0 'C' CODE \0 'M' MESSAGE \0\0
            using var ms = new MemoryStream();
            
            // Type byte
            ms.WriteByte((byte)'E');
            
            // We'll calculate and write length at the end
            int lengthPosition = (int)ms.Position;
            ms.Write(new byte[4], 0, 4); // Placeholder for length
            
            // Severity
            ms.WriteByte((byte)'S');
            var severity = System.Text.Encoding.UTF8.GetBytes("FATAL");
            ms.Write(severity, 0, severity.Length);
            ms.WriteByte(0);
            
            // SQLSTATE Code
            ms.WriteByte((byte)'C');
            var code = System.Text.Encoding.UTF8.GetBytes("08006"); // connection_failure
            ms.Write(code, 0, code.Length);
            ms.WriteByte(0);
            
            // Message
            ms.WriteByte((byte)'M');
            var msg = System.Text.Encoding.UTF8.GetBytes(message);
            ms.Write(msg, 0, msg.Length);
            ms.WriteByte(0);
            
            // Null terminator
            ms.WriteByte(0);
            
            // Update length
            int totalLength = (int)ms.Position;
            ms.Position = lengthPosition;
            byte[] lengthBytes = new byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(lengthBytes, totalLength - 1);
            ms.Write(lengthBytes, 0, 4);
            
            await _clientWriter.WriteAsync(ms.ToArray());
            await _clientWriter.FlushAsync();
        }
        catch { /* Игнорируем, клиент уже мертв */ }
    }

    private void AbortSession()
    {
        ReleaseBackend();
        
        try 
        {
            _clientSocket.Close(); 
        }
        catch { /* Игнорируем ошибки при жестком закрытии */ }
    }

    /// <summary>
    /// Безопасная попытка отвязки бэкенда - проверяет 4 условия безопасности
    /// </summary>
    private void TryReleaseBackendIfIdle()
    {
        BackendConnection? backendToRelease = null;

        lock (_backendLock)
        {
            // 4 Всадника Апокалипсиса: проверяем, что соединение АБСОЛЮТНО чистое
            if (_isWritingToBackend) 
            {
                _logger.LogTrace("[Session {Id}] Not releasing: writing to backend", _sessionInfo.Id);
                return;
            }
            if (_isMidExtendedQuery) 
            {
                _logger.LogTrace("[Session {Id}] Not releasing: mid extended query", _sessionInfo.Id);
                return;
            }
            if (Volatile.Read(ref _pendingQueries) > 0) 
            {
                _logger.LogTrace("[Session {Id}] Not releasing: pending queries ({Count})", _sessionInfo.Id, _pendingQueries);
                return;
            }
            if (_lastTxStatus != 'I') 
            {
                _logger.LogTrace("[Session {Id}] Not releasing: tx status is {Status}", _sessionInfo.Id, _lastTxStatus);
                return;
            }
            if (Volatile.Read(ref _isAcquiring) > 0) 
            {
                _logger.LogTrace("[Session {Id}] Not releasing: acquiring backend", _sessionInfo.Id);
                return;
            }
            
            backendToRelease = _currentBackend;
            _currentBackend = null;
        }

        if (backendToRelease != null)
        {
            _logger.LogDebug("[Session {Id}] Releasing backend {BackendId}", _sessionInfo.Id, backendToRelease.Id);
            backendToRelease.DetachHandler();
            
            // Send reset query before releasing (DISCARD ALL to clean prepared statements)
            if (_config.Pool.Mode == PoolingMode.Transaction)
            {
                try
                {
                    backendToRelease.ExecuteResetQueryAsync().Wait(TimeSpan.FromSeconds(2));
                    _logger.LogDebug("[Session {Id}] Reset query executed on backend {BackendId}", 
                        _sessionInfo.Id, backendToRelease.Id);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[Session {Id}] Reset query failed, marking backend as broken", _sessionInfo.Id);
                    backendToRelease.MarkAsBroken();
                }
            }
            
            _pool?.Release(backendToRelease);
        }
    }

    /// <summary>
    /// Принудительная отвязка бэкенда (например, при ошибках)
    /// </summary>
    private void ReleaseBackend(bool force = false)
    {
        BackendConnection? backendToRelease = null;
        
        lock (_backendLock)
        {
            backendToRelease = _currentBackend;
            _currentBackend = null;
            _isWritingToBackend = false;
            _isMidExtendedQuery = false;
        }
        
        if (backendToRelease != null)
        {
            backendToRelease.DetachHandler();
            _pool?.Release(backendToRelease);
        }
    }

    /// <summary>
    /// Получает байт из ReadOnlySequence по указанной позиции
    /// </summary>
    private static byte GetByteAtPosition(ReadOnlySequence<byte> sequence, long position)
    {
        if (sequence.IsSingleSegment)
        {
            return sequence.FirstSpan[(int)position];
        }

        // Для multi-segment последовательности ищем нужный сегмент
        long currentPosition = 0;
        foreach (var segment in sequence)
        {
            if (currentPosition + segment.Length > position)
            {
                return segment.Span[(int)(position - currentPosition)];
            }
            currentPosition += segment.Length;
        }

        throw new ArgumentOutOfRangeException(nameof(position), "Position is beyond sequence length");
    }

    public void Dispose()
    {
        ReleaseBackend();
        _clientReader.Complete();
        _clientWriter.Complete();
        _clientSocket.Dispose();
    }
}
