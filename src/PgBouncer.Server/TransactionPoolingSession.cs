using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server;

/// <summary>
/// Реализация Transaction Pooling с блекджеком и... Full Duplex.
/// </summary>
public sealed class TransactionPoolingSession : IDisposable
{
    private readonly Stream _clientStream;
    private readonly IConnectionPool _pool;
    private readonly PgBouncerConfig _config;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;

    private readonly Action<long> _recordWaitTime;
    private readonly Action _recordTimeout;
    private readonly Action _onBackendAcquired;
    private readonly Action _onBackendReleased;

    private volatile IServerConnection? _backend;
    private CancellationTokenSource? _backendCts;
    private bool _disposed;
    private bool _inExtendedQuery;
    private readonly byte[] _clientBuffer = new byte[32768];

    public TransactionPoolingSession(
        Stream clientStream,
        IConnectionPool pool,
        PgBouncerConfig config,
        ILogger logger,
        SessionInfo sessionInfo,
        Action<long> recordWaitTime,
        Action recordTimeout,
        Action onBackendAcquired,
        Action onBackendReleased,
        byte[]? initialData = null)
    {
        _pool = pool;
        _config = config;
        _logger = logger;
        _sessionInfo = sessionInfo;
        _recordWaitTime = recordWaitTime;
        _recordTimeout = recordTimeout;
        _onBackendAcquired = onBackendAcquired;
        _onBackendReleased = onBackendReleased;

        if (initialData != null && initialData.Length > 0)
        {
            _clientStream = new PrefixedStream(clientStream, initialData);
            _logger.LogInformation("[Session {Id}] Используем PrefixedStream с {Bytes} байт данных.", sessionInfo.Id, initialData.Length);
        }
        else
        {
            _clientStream = clientStream;
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] Стартуем эту шарманку. Full Duplex mode.", _sessionInfo.Id);

        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                // Читаем заголовок (5 байт). Если тут пусто, значит клиент свалил.
                if (!await ReadExactAsync(_clientStream, _clientBuffer.AsMemory(0, 5), cancellationToken))
                {
                    _logger.LogInformation("[Session {Id}] Client disconnected (EOF) - cannot read message header", _sessionInfo.Id);
                    break;
                }

                if (!PgMessageScanner.TryReadMessageInfo(_clientBuffer.AsSpan(0, 5), out var msgInfo))
                {
                    _logger.LogError("[Session {Id}] Чё за херню прислал клиент? Заголовок битый.", _sessionInfo.Id);
                    break;
                }

                var bodyLength = msgInfo.Length - 4;
                if (bodyLength > 0)
                {
                    if (5 + bodyLength > _clientBuffer.Length)
                    {
                        var errorMsg = $"Message too big: {msgInfo.Length}. Ты охренел такие пакеты слать?";
                        _logger.LogError(errorMsg);
                        await SendErrorToClientAsync(errorMsg, "54000", cancellationToken);
                        break;
                    }

                    if (!await ReadExactAsync(_clientStream, _clientBuffer.AsMemory(5, bodyLength), cancellationToken))
                    {
                        _logger.LogWarning("[Session {Id}] Клиент сдох посередине сообщения. Вот урод.", _sessionInfo.Id);
                        break;
                    }
                }

                var fullMsgLength = 1 + msgInfo.Length;

                // Логируем КАЖДОЕ сообщение для отладки
                _logger.LogDebug("[Session {Id}] Получено сообщение от клиента: Type='{Type}' (0x{TypeHex:X2}), Length={Len}", 
                    _sessionInfo.Id, msgInfo.Type, (byte)msgInfo.Type, msgInfo.Length);

                if (msgInfo.Type == PgMessageTypes.Terminate)
                {
                    _logger.LogDebug("[Session {Id}] Клиент вежливо попрощался (Celebrate).", _sessionInfo.Id);
                    break;
                }

                // Track Extended Query Protocol state
                if (msgInfo.Type == PgMessageTypes.Parse)
                {
                    _inExtendedQuery = true;
                    _logger.LogDebug("[Session {Id}] Entering Extended Query mode (Parse received).", _sessionInfo.Id);
                }
                else if (msgInfo.Type == PgMessageTypes.Sync)
                {
                    _inExtendedQuery = false;
                    _logger.LogDebug("[Session {Id}] Exiting Extended Query mode (Sync received).", _sessionInfo.Id);
                }

                // Нужен backend? Ищем.
                if (PgMessageScanner.RequiresBackend(msgInfo.Type))
                {
                    _logger.LogDebug("[Session {Id}] Message type '{Type}' requires backend", _sessionInfo.Id, msgInfo.Type);
                    if (_backend == null)
                    {
                        _logger.LogDebug("[Session {Id}] No backend available, acquiring from pool...", _sessionInfo.Id);
                        await AcquireBackendAsync(cancellationToken);
                        if (_backend == null)
                        {
                            _logger.LogError("[Session {Id}] Failed to acquire backend from pool", _sessionInfo.Id);
                            break;
                        }
                        _logger.LogDebug("[Session {Id}] Backend acquired successfully", _sessionInfo.Id);
                    }
                    else
                    {
                        _logger.LogDebug("[Session {Id}] Reusing existing backend", _sessionInfo.Id);
                    }
                }

                // Шлем в backend (если есть)
                var backend = _backend;
                if (backend != null)
                {
                    try
                    {
                        await backend.Stream.WriteAsync(_clientBuffer.AsMemory(0, fullMsgLength), cancellationToken);
                        await backend.Stream.FlushAsync(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "[Session {Id}] Ошибка записи в backend. Похоже он сдох, собака.", _sessionInfo.Id);
                        break;
                    }
                }
            }
        }
        catch (Exception ex) when (IsExpectedConnectionError(ex))
        {
            _logger.LogInformation("[Session {Id}] Client disconnected gracefully (RESET/EOF).", _sessionInfo.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Глобальный факап в сессии.", _sessionInfo.Id);
        }
        finally
        {
            CleanupBackendContext();
        }
    }

    private static bool IsExpectedConnectionError(Exception ex)
    {
        if (ex is OperationCanceledException) return true;
        if (ex is IOException ioEx && ioEx.InnerException is SocketException sockEx)
        {
            // 10053: Software caused connection abort
            // 10054: Connection reset by peer
            return sockEx.ErrorCode == 10053 || sockEx.ErrorCode == 10054;
        }
        return false;
    }

    private void CleanupBackendContext()
    {
        try
        {
            _backendCts?.Cancel();
            _backendCts?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error cancelling backend task. Да и хрен с ним.");
        }

        _backendCts = null;
        _backend = null;
    }

    private async Task AcquireBackendAsync(CancellationToken token)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            // Dispose old CTS if exists (cleanup after previous transaction)
            if (_backendCts != null)
            {
                try { _backendCts.Cancel(); _backendCts.Dispose(); } catch { }
                _backendCts = null;
            }

            // Готовим токен, чтобы если эта хреновина зависнет, мы её прибили
            _backendCts = CancellationTokenSource.CreateLinkedTokenSource(token);

            var timeoutSeconds = _config.Pool.ConnectionTimeout > 0 ? _config.Pool.ConnectionTimeout : 60; // 60 секунд на ожидание

            using var acquireCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            acquireCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

            var conn = await _pool.AcquireAsync(acquireCts.Token);
            sw.Stop();

            _recordWaitTime(sw.ElapsedMilliseconds);
            _onBackendAcquired();
            _backend = conn;
            _sessionInfo.State = SessionState.Active;

            _logger.LogDebug("[Session {Id}] Backend {BackendId} захвачен за {Ms}ms. Погнали!", _sessionInfo.Id, conn.Id, sw.ElapsedMilliseconds);

            // Запускаем читалку ответов в фоне
            _ = ProcessBackendResponsesAsync(conn, _clientStream, _backendCts.Token);
        }
        catch (OperationCanceledException)
        {
            _recordTimeout();
            _logger.LogWarning("[Session {Id}] ТАЙМАУТ ПУЛА ({Ms}ms). Нет свободных дырок.", _sessionInfo.Id, sw.ElapsedMilliseconds);
                        await SendErrorToClientAsync("connection pool timeout", "57014", token);
            _backendCts?.Dispose();
            _backendCts = null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Ошибка при захвате backend. Всё плохо.", _sessionInfo.Id);
            await SendErrorToClientAsync("server connection error", "08006", token);
            _backendCts?.Dispose();
            _backendCts = null;
        }
    }

    private async Task ProcessBackendResponsesAsync(IServerConnection connection, Stream clientStream, CancellationToken token)
    {
        var buffer = new byte[32768];
        bool released = false;

        _logger.LogDebug("[Session {Id}] ProcessBackendResponsesAsync started for backend {BackendId}", _sessionInfo.Id, connection.Id);

        try
        {
            while (!token.IsCancellationRequested)
            {
                if (!await ReadExactAsync(connection.Stream, buffer.AsMemory(0, 5), token))
                {
                    _logger.LogWarning("[Session {Id}] Backend {BackendId} EOF - no more data", _sessionInfo.Id, connection.Id);
                    break; // EOF -> Backend closing
                }

                if (!PgMessageScanner.TryReadMessageInfo(buffer.AsSpan(0, 5), out var msgInfo))
                {
                    _logger.LogError("[Session {Id}] Invalid backend message header", _sessionInfo.Id);
                    break;
                }

                _logger.LogDebug("[Session {Id}] Backend {BackendId} sent message: Type='{Type}' (0x{TypeHex:X2}), Length={Len}",
                    _sessionInfo.Id, connection.Id, msgInfo.Type, (byte)msgInfo.Type, msgInfo.Length);

                var bodyLength = msgInfo.Length - 4;
                if (bodyLength > 0)
                {
                    if (5 + bodyLength > buffer.Length)
                    {
                        _logger.LogError("Backend response too big. Ну и жирный же ответ.");
                        break;
                    }
                    if (!await ReadExactAsync(connection.Stream, buffer.AsMemory(5, bodyLength), token))
                        break;
                }

                var fullLen = 1 + msgInfo.Length;

                await clientStream.WriteAsync(buffer.AsMemory(0, fullLen), token);
                await clientStream.FlushAsync(token);

                if (msgInfo.IsReadyForQuery)
                {
                    var txState = msgInfo.PgTransactionState;

                    // Handle all three PostgreSQL transaction states from ReadyForQuery message:
                    // 'I' (Idle) - Not in a transaction block, backend CAN be released
                    // 'T' (InTransaction) - In a transaction block, backend CANNOT be released
                    // 'E' (Failed) - Failed transaction block, backend CANNOT be released (ROLLBACK required)
                    switch (txState)
                    {
                        case PgTransactionState.Idle:
                            // Only release backend if we're not in the middle of an Extended Query
                            // (Parse/Bind/Execute/Sync flow)
                            if (!_inExtendedQuery)
                            {
                                _logger.LogDebug("[Session {Id}] RFQ(Idle). Backend {BackendId} свободен! Возвращаем в стойло.", _sessionInfo.Id, connection.Id);

                                _pool.Release(connection);
                                released = true;
                                _onBackendReleased();

                                _backend = null;
                                _sessionInfo.State = SessionState.Idle;

                                // Cleanup CTS to unlink from session token
                                var cts = _backendCts;
                                _backendCts = null;
                                try { cts?.Dispose(); } catch { }
                            }
                            else
                            {
                                _logger.LogDebug("[Session {Id}] RFQ(Idle) but still in Extended Query, keeping backend.", _sessionInfo.Id);
                            }
                            break;

                        case PgTransactionState.InTransaction:
                            // In transaction block (BEGIN was executed), backend must stay assigned
                            _logger.LogDebug("[Session {Id}] RFQ(InTransaction). Backend {BackendId} занят транзакцией.", _sessionInfo.Id, connection.Id);
                            break;

                        case PgTransactionState.Failed:
                            // Failed transaction block, backend must stay assigned until ROLLBACK
                            _logger.LogDebug("[Session {Id}] RFQ(Failed). Backend {BackendId} в ошибке, нужен ROLLBACK.", _sessionInfo.Id, connection.Id);
                            break;

                        default:
                            _logger.LogWarning("[Session {Id}] Unknown transaction state: {State}. Keeping backend.", _sessionInfo.Id, txState);
                            break;
                    }
                    return;
                }
            }
        }

        catch (OperationCanceledException)
        {
            _logger.LogInformation("Backend task cancelled. Клиент ушел, сворачиваемся.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error processing backend {Id}. Что-то пошло не так.", connection.Id);
        }
        finally
        {
            if (!released)
            {
                _logger.LogWarning("Destroying dirty backend {Id}. Пришлось пристрелить.", connection.Id);
                await connection.DisposeAsync();
                _pool.Release(connection);
                _onBackendReleased();

                if (_backend == connection) _backend = null;
            }
        }
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

    private async Task SendErrorToClientAsync(string message, string sqlState, CancellationToken cancellationToken)
    {
        try
        {
            var msgBytes = System.Text.Encoding.UTF8.GetBytes(message);
            var severity = "FATAL"u8.ToArray();
            var codeBytes = System.Text.Encoding.UTF8.GetBytes(sqlState);
            
            // Length = 4(len) + 1(S) + len(sev) + 1(0) + 1(C) + len(code) + 1(0) + 1(M) + len(msg) + 1(0) + 1(0 end)
            var len = 4 + 1 + severity.Length + 1 + 1 + codeBytes.Length + 1 + 1 + msgBytes.Length + 1 + 1;

            var response = new byte[1 + len];
            var pos = 0;

            response[pos++] = (byte)'E';

            // Int32 Big Endian manually
            response[pos++] = (byte)(len >> 24);
            response[pos++] = (byte)(len >> 16);
            response[pos++] = (byte)(len >> 8);
            response[pos++] = (byte)len;

            // Severity 'S'
            response[pos++] = (byte)'S';
            Array.Copy(severity, 0, response, pos, severity.Length);
            pos += severity.Length;
            response[pos++] = 0;

            // Code 'C' - SQLSTATE error code (required by protocol)
            response[pos++] = (byte)'C';
            Array.Copy(codeBytes, 0, response, pos, codeBytes.Length);
            pos += codeBytes.Length;
            response[pos++] = 0;

            // Message 'M'
            response[pos++] = (byte)'M';
            Array.Copy(msgBytes, 0, response, pos, msgBytes.Length);
            pos += msgBytes.Length;
            response[pos++] = 0;

            // End of fields
            response[pos++] = 0;

            await _clientStream.WriteAsync(response, cancellationToken);
            await _clientStream.FlushAsync(cancellationToken);
        }
        catch { }
    }

    public void Dispose()
    {
        _disposed = true;
        CleanupBackendContext();
    }
    private sealed class PrefixedStream : Stream
    {
        private readonly Stream _inner;
        private ReadOnlyMemory<byte> _prefix;

        public PrefixedStream(Stream inner, byte[] prefix)
        {
            _inner = inner;
            _prefix = prefix;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _inner.Length + _prefix.Length;
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
        public override int Read(Span<byte> buffer)
        {
            if (!_prefix.IsEmpty)
            {
                var toCopy = Math.Min(buffer.Length, _prefix.Length);
                _prefix.Span.Slice(0, toCopy).CopyTo(buffer);
                _prefix = _prefix.Slice(toCopy);
                return toCopy;
            }
            return _inner.Read(buffer);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (!_prefix.IsEmpty)
            {
                var toCopy = Math.Min(buffer.Length, _prefix.Length);
                _prefix.Slice(0, toCopy).CopyTo(buffer);
                _prefix = _prefix.Slice(toCopy);
                return toCopy;
            }
            return await _inner.ReadAsync(buffer, cancellationToken);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.WriteAsync(buffer, offset, count, cancellationToken);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Close() { _inner.Close(); base.Close(); }
    }
}
