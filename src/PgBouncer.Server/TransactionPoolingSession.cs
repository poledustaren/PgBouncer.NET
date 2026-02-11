using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server;

/// <summary>
/// Реализация Transaction Pooling.
/// Backend соединение удерживается только на время транзакции.
/// Используется Full Duplex режим:
/// - Main Loop: читает запросы от клиента и пересылает в backend.
/// - Background Task (ProcessBackendResponsesAsync): владеет backend-соединением, читает ответы и освобождает его.
/// </summary>
public sealed class TransactionPoolingSession : IDisposable
{
    private readonly NetworkStream _clientStream;
    private readonly ConnectionPool _pool;
    private readonly PgBouncerConfig _config;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;

    // Callbacks для статистики
    private readonly Action<long> _recordWaitTime;
    private readonly Action _recordTimeout;
    private readonly Action _onBackendAcquired;
    private readonly Action _onBackendReleased;

    // Текущий backend.
    // Важно: в Full Duplex режиме владение разделено.
    // RunAsync пишет в него.
    // ProcessBackendResponsesAsync читает из него и отвечает за Release/Dispose.
    private volatile ServerConnection? _backend;

    // CTS для отмены фоновой задачи обработки backend
    private CancellationTokenSource? _backendCts;

    private bool _disposed;

    // Буферы (переиспользуемые)
    private readonly byte[] _clientBuffer = new byte[32768];
    // Backend buffer выделяется внутри задачи чтения

    public TransactionPoolingSession(
        NetworkStream clientStream,
        ConnectionPool pool,
        PgBouncerConfig config,
        ILogger logger,
        SessionInfo sessionInfo,
        Action<long> recordWaitTime,
        Action recordTimeout,
        Action onBackendAcquired,
        Action onBackendReleased)
    {
        _clientStream = clientStream;
        _pool = pool;
        _config = config;
        _logger = logger;
        _sessionInfo = sessionInfo;
        _recordWaitTime = recordWaitTime;
        _recordTimeout = recordTimeout;
        _onBackendAcquired = onBackendAcquired;
        _onBackendReleased = onBackendReleased;
    }

    /// <summary>
    /// Основной цикл Transaction Pooling (Full Duplex)
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                // 1. Читаем заголовок сообщения (5 байт)
                if (!await ReadExactAsync(_clientStream, _clientBuffer.AsMemory(0, 5), cancellationToken))
                    break;

                // 2. Парсим длину
                if (!PgMessageScanner.TryReadMessageInfo(_clientBuffer.AsSpan(0, 5), out var msgInfo))
                {
                    _logger.LogWarning("Не удалось распарсить заголовок");
                    break;
                }

                // 3. Читаем тело
                var bodyLength = msgInfo.Length - 4;
                if (bodyLength > 0)
                {
                    if (5 + bodyLength > _clientBuffer.Length)
                    {
                        var errorMsg = $"Message too big: {msgInfo.Length}";
                        _logger.LogError(errorMsg);
                        await SendErrorToClientAsync(errorMsg, cancellationToken);
                        break;
                    }

                    if (!await ReadExactAsync(_clientStream, _clientBuffer.AsMemory(5, bodyLength), cancellationToken))
                        break;
                }

                var fullMsgLength = 1 + msgInfo.Length;

                if (_logger.IsEnabled(LogLevel.Trace) && msgInfo.Type != 0)
                    _logger.LogTrace("C->S: {Type} ({Length})", msgInfo.Type, msgInfo.Length);

                if (msgInfo.Type == PgMessageTypes.Terminate)
                {
                    _logger.LogDebug("Client sent Terminate, closing session");
                    break;
                }

                // 4. Захват backend (если нет и нужен)
                if (PgMessageScanner.RequiresBackend(msgInfo.Type))
                {
                    if (_backend == null)
                    {
                        await AcquireBackendAsync(cancellationToken);
                        if (_backend == null)
                        {
                            // Ошибка захвата, прерываем сессию
                            break;
                        }
                    }
                }

                // 5. Отправка в backend
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
                        _logger.LogWarning(ex, "Ошибка записи в backend");
                        // Если backend упал, фоновая задача это увидит и закроет его.
                        // Мы выходим из цикла, завершая сессию.
                        break;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка сессии TransactionPoolingSession");
        }
        finally
        {
            CleanupBackendContext();
        }
    }

    private void CleanupBackendContext()
    {
        // Сигнализируем фоновой задаче, что пора закругляться
        // Если она висит на чтении backend - она получит Cancel, сделает Dispose и Release.
        // Если она уже завершилась - ничего не произойдет.
        try
        {
            _backendCts?.Cancel();
            _backendCts?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error cancelling backend task");
        }

        _backendCts = null;

        // _backend должен обнулиться фоновой задачей, но на всякий случай
        _backend = null;
    }

    private async Task AcquireBackendAsync(CancellationToken token)
    {
        try
        {
            // Готовим токен для фоновой задачи
            _backendCts = CancellationTokenSource.CreateLinkedTokenSource(token);

            var sw = Stopwatch.StartNew();
            var timeoutSeconds = _config.Pool.ConnectionTimeout > 0 ? _config.Pool.ConnectionTimeout : 30;

            // Таймаут для самого захвата
            using var acquireCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            acquireCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

            var conn = await _pool.AcquireAsync(acquireCts.Token);
            sw.Stop();

            _recordWaitTime(sw.ElapsedMilliseconds);
            _onBackendAcquired();

            // Успех
            _backend = conn;
            _sessionInfo.State = SessionState.Active;

            _logger.LogDebug("Backend {Id} acquired in {Ms}ms", conn.Id, sw.ElapsedMilliseconds);

            // ЗАПУСКАЕМ ФОНОВУЮ ЗАДАЧУ ЧТЕНИЯ
            // Передаем ей владение connection и наш CTS token
            _ = ProcessBackendResponsesAsync(conn, _clientStream, _backendCts.Token);
        }
        catch (OperationCanceledException)
        {
            _recordTimeout();
            _logger.LogWarning("Timeout acquiring backend");
            await SendErrorToClientAsync("connection pool timeout", token);

            // Cleanup cts
            _backendCts?.Dispose();
            _backendCts = null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error acquiring backend");
            await SendErrorToClientAsync("server connection error", token);

            _backendCts?.Dispose();
            _backendCts = null;
        }
    }

    /// <summary>
    /// Фоновая задача чтения ответов от Backend.
    /// ВЛАДЕЕТ connection: обязана сделать Release или Dispose+Release.
    /// </summary>
    private async Task ProcessBackendResponsesAsync(ServerConnection connection, NetworkStream clientStream, CancellationToken token)
    {
        var buffer = new byte[32768];
        bool released = false;

        try
        {
            // Читаем пока не отменят (token - это _backendCts.Token, связанный с сессией)
            while (!token.IsCancellationRequested)
            {
                // 1. Читаем Header
                if (!await ReadExactAsync(connection.Stream, buffer.AsMemory(0, 5), token))
                    break; // EOF -> Connection closed by server

                if (!PgMessageScanner.TryReadMessageInfo(buffer.AsSpan(0, 5), out var msgInfo))
                {
                    _logger.LogError("Invalid backend message header");
                    break;
                }

                // 2. Читаем Body
                var bodyLength = msgInfo.Length - 4;
                if (bodyLength > 0)
                {
                    if (5 + bodyLength > buffer.Length)
                    {
                        _logger.LogError("Backend response too big: {Length}", msgInfo.Length);
                        break;
                    }
                    if (!await ReadExactAsync(connection.Stream, buffer.AsMemory(5, bodyLength), token))
                        break; // EOF mid-message
                }

                var fullLen = 1 + msgInfo.Length;

                if (_logger.IsEnabled(LogLevel.Trace) && msgInfo.Type != 0)
                    _logger.LogTrace("S->C: {Type} ({Len})", msgInfo.Type, fullLen);

                // 3. Шлем клиенту
                await clientStream.WriteAsync(buffer.AsMemory(0, fullLen), token);
                await clientStream.FlushAsync(token);

                // 4. Проверка RFQ
                if (msgInfo.IsReadyForQuery)
                {
                    // Проверяем статус транзакции (первый байт body, offset 5)
                    var status = buffer[5];
                    var isIdle = status == (byte)'I';

                    if (isIdle)
                    {
                        _logger.LogDebug("RFQ(Idle) -> Releasing Backend {Id}", connection.Id);

                        // Возвращаем в пул (Healthy)
                        _pool.Release(connection);
                        released = true;
                        _onBackendReleased();

                        // Сбрасываем _backend в Main Loop
                        if (_backend == connection)
                        {
                            _backend = null;
                            _sessionInfo.State = SessionState.Idle;
                        }

                        return; // Done
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Нормальная отмена (сессия закрыта или таймаут)
            _logger.LogDebug("Backend task cancelled for {Id}", connection.Id);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error processing backend {Id}", connection.Id);
        }
        finally
        {
            if (!released)
            {
                // Если мы вышли из цикла не через RFQ(Idle), значит состояние backend неизвестно или разорвано.
                // МЫ ДОЛЖНЫ УНИЧТОЖИТЬ СОЕДИНЕНИЕ.
                _logger.LogDebug("Destroying dirty backend {Id}", connection.Id);

                // Dispose закрывает сокет
                await connection.DisposeAsync();

                // Release удаляет его из списка активных и декрементирует счетчик
                _pool.Release(connection);
                _onBackendReleased();

                if (_backend == connection)
                {
                    _backend = null;
                }
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

    private async Task SendErrorToClientAsync(string message, CancellationToken cancellationToken)
    {
        try
        {
            var msgBytes = System.Text.Encoding.UTF8.GetBytes(message);
            var response = new byte[1 + 4 + 1 + msgBytes.Length + 1 + 1];
            var pos = 0;
            response[pos++] = (byte)'E';
            var len = response.Length - 1;
            BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(1), len);
            pos += 4;
            response[pos++] = (byte)'M';
            msgBytes.CopyTo(response, pos);
            pos += msgBytes.Length;
            response[pos++] = 0;
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
}
