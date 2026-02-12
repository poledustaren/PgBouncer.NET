using System.IO;
using System.Net.Sockets;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Server;

/// <summary>
/// Клиентская сессия - ПРОЗРАЧНОЕ ПРОКСИРОВАНИЕ с лимитом backend соединений
/// </summary>
public class ClientSession : IDisposable
{
    private readonly Socket _clientSocket;
    private readonly NetworkStream _clientStream;
    private readonly PgBouncerConfig _config;
    private readonly PoolManager _poolManager;
    private readonly SemaphoreSlim _backendConnectionLimit;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;

    // Callbacks для статистики
    private readonly Action _onBackendAcquired;
    private readonly Action _onBackendReleased;
    private readonly Action _onWaitStarted;
    private readonly Action _onWaitEnded;

    private string? _database;
    private string? _username;
    private string? _password;
    private bool _hasBackendConnection;

    // Callbacks для статистики ProxyServer
    private readonly Action<long> _recordWaitTime;
    private readonly Action _recordTimeout;

    public ClientSession(
        Socket clientSocket,
        PgBouncerConfig config,
        PoolManager poolManager,
        SemaphoreSlim backendConnectionLimit,
        ILogger logger,
        SessionInfo sessionInfo,
        Action onBackendAcquired,
        Action onBackendReleased,
        Action onWaitStarted,
        Action onWaitEnded,
        Action<long> recordWaitTime,
        Action recordTimeout)
    {
        _clientSocket = clientSocket;
        _clientStream = new NetworkStream(clientSocket, ownsSocket: false);
        _config = config;
        _poolManager = poolManager;
        _backendConnectionLimit = backendConnectionLimit;
        _logger = logger;
        _sessionInfo = sessionInfo;
        _onBackendAcquired = onBackendAcquired;
        _onBackendReleased = onBackendReleased;
        _onWaitStarted = onWaitStarted;
        _onWaitEnded = onWaitEnded;
        _recordWaitTime = recordWaitTime;
        _recordTimeout = recordTimeout;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        Socket? backendSocket = null;
        NetworkStream? backendStream = null;

        try
        {
            _logger.LogDebug("=== НОВАЯ СЕССИЯ ===");

            // 1. Читаем первый пакет (может быть SSLRequest или StartupMessage)
            var buffer = new byte[8192];
            var bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);

            if (bytesRead < 8)
            {
                if (bytesRead == 0)
                    _logger.LogInformation("Client disconnected (EOF) during handshake.");
                else
                    _logger.LogWarning("Слишком мало данных: {Bytes} байт", bytesRead);
                return;
            }

            // Проверяем на SSLRequest
            var length = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
            var protocolCode = (buffer[4] << 24) | (buffer[5] << 16) | (buffer[6] << 8) | buffer[7];

            if (protocolCode == 0x04D2162F) // SSLRequest
            {
                _logger.LogDebug("SSLRequest -> отвечаем 'N'");
                await _clientStream.WriteAsync(new byte[] { (byte)'N' }, 0, 1, cancellationToken);
                await _clientStream.FlushAsync(cancellationToken);

                // Читаем настоящий StartupMessage
                bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                if (bytesRead < 8)
                {
                    if (bytesRead == 0)
                        _logger.LogInformation("Client disconnected (EOF) after SSLRequest.");
                    else
                        _logger.LogWarning("Слишком мало данных после SSLRequest: {Bytes}", bytesRead);
                    return;
                }
                length = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
            }

            // Сохраняем StartupMessage для форварда
            var startupLength = length;
            var startupMessage = new byte[startupLength];
            Array.Copy(buffer, 0, startupMessage, 0, startupLength);

            // ПРОВЕРЯЕМ ЛИШНИЕ ДАННЫЕ (Pipelining / Packet Coalescing)
            byte[]? excessData = null;
            if (bytesRead > startupLength)
            {
                var excessLen = bytesRead - startupLength;
                excessData = new byte[excessLen];
                Array.Copy(buffer, startupLength, excessData, 0, excessLen);
                _logger.LogInformation("Опа! Клиент прислал {Bytes} лишних байт вместе со Startup. Сохраним, а то потеряем.", excessLen);
            }

            // Парсим параметры для логирования
            var parameters = ParseStartupParameters(buffer, 8, length - 8);
            _database = parameters.GetValueOrDefault("database") ?? "postgres";
            _username = parameters.GetValueOrDefault("user") ?? "postgres";
            _password = _config.Backend.AdminPassword;

            // Обновляем информацию о сессии
            _sessionInfo.Database = _database;
            _sessionInfo.Username = _username;

            _logger.LogInformation(">>> {Database}/{User}", _database, _username);

            // === ВЫБОР РЕЖИМА POOLING ===
            if (_config.Pool.Mode == PoolingMode.Transaction)
            {
                await RunTransactionPoolingAsync(cancellationToken, excessData);
                return;
            }

            // === SESSION POOLING (текущая логика) ===
            // 2. ЖДЁМ ДОСТУПНОГО СЛОТА для backend соединения
            _sessionInfo.State = SessionState.WaitingForSlot;
            _sessionInfo.WaitStartedAt = DateTime.UtcNow;
            _onWaitStarted();

            var waitStopwatch = Stopwatch.StartNew();
            _logger.LogDebug("Ожидание слота для backend соединения...");

            try
            {
                // Ждём с таймаутом
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(_config.Pool.ConnectionTimeout));

                await _backendConnectionLimit.WaitAsync(timeoutCts.Token);

                waitStopwatch.Stop();
                _sessionInfo.WaitTimeMs = waitStopwatch.ElapsedMilliseconds;
                _sessionInfo.State = SessionState.Active;

                // Записываем статистику в ProxyServer
                _recordWaitTime(_sessionInfo.WaitTimeMs);

                _hasBackendConnection = true;
                _onBackendAcquired();
                _logger.LogDebug("Слот получен за {WaitMs}ms, подключение к PostgreSQL...", _sessionInfo.WaitTimeMs);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                waitStopwatch.Stop();
                _sessionInfo.WaitTimeMs = waitStopwatch.ElapsedMilliseconds;
                _sessionInfo.State = SessionState.Error;

                _recordTimeout();

                _logger.LogWarning("Таймаут ожидания слота ({Timeout}s)", _config.Pool.ConnectionTimeout);
                await SendErrorToClientAsync("too many connections - please try again later", "53300", cancellationToken);
                return;
            }
            finally
            {
                _onWaitEnded();
            }

            // 3. Подключаемся к PostgreSQL
            backendSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await backendSocket.ConnectAsync(_config.Backend.Host, _config.Backend.Port, cancellationToken);
            backendStream = new NetworkStream(backendSocket, ownsSocket: true);

            _logger.LogDebug("Подключено к PostgreSQL!");

            // 4. Форвардим StartupMessage
            await backendStream.WriteAsync(startupMessage, 0, startupLength, cancellationToken);
            await backendStream.FlushAsync(cancellationToken);

            // 5. Проксируем ВСЕ данные
            var clientToServer = ProxyDirectionAsync(_clientStream, backendStream, "Client->PG", cancellationToken);
            var serverToClient = ProxyDirectionAsync(backendStream, _clientStream, "PG->Client", cancellationToken);

            await Task.WhenAny(clientToServer, serverToClient);
            _logger.LogDebug("Сессия завершена");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ОШИБКА: {Message}", ex.Message);
        }
        finally
        {
            backendStream?.Close();
            backendSocket?.Close();

            // Освобождаем слот
            if (_hasBackendConnection)
            {
                _backendConnectionLimit.Release();
                _onBackendReleased();
                _logger.LogDebug("Слот освобождён");
            }
        }
    }

    /// <summary>
    /// Transaction Pooling режим — backend захватывается на каждый Query
    /// </summary>
    private async Task RunTransactionPoolingAsync(CancellationToken cancellationToken, byte[]? initialData)
    {
        _logger.LogInformation("=== TRANSACTION POOLING MODE ===");
        _sessionInfo.State = SessionState.Idle;

        // Получаем пул для database/user
        _logger.LogInformation("Getting pool for {DB}/{User}...", _database, _username);
        var pool = await _poolManager.GetPoolAsync(_database!, _username!, _password!);

        // Сначала нужно завершить handshake с клиентом — отправляем AuthOk и ReadyForQuery
        _logger.LogInformation("Sending AuthOk + ReadyForQuery...");
        await SendAuthOkAndReadyAsync(cancellationToken);
        _logger.LogInformation("AuthOk sent. Starting TransactionPoolingSession...");

        // Передаём управление EnhancedTransactionPoolingSessionV2 (Event-driven architecture)
        using var txSession = new EnhancedTransactionPoolingSessionV2(
            _clientStream,
            pool,
            _config,
            _logger,
            _sessionInfo,
            _recordWaitTime,
            _recordTimeout,
            _onBackendAcquired,
            _onBackendReleased);

        await txSession.RunAsync(cancellationToken);
    }

    private async Task SendAuthOkAndReadyAsync(CancellationToken cancellationToken)
    {
        using var ms = new MemoryStream(512);

        ms.WriteByte((byte)'R');
        WriteInt32BigEndian(ms, 8);
        WriteInt32BigEndian(ms, 0);

        WriteParameterStatus(ms, "server_version", "16.0");
        WriteParameterStatus(ms, "server_encoding", "UTF8");
        WriteParameterStatus(ms, "client_encoding", "UTF8");
        WriteParameterStatus(ms, "DateStyle", "ISO, MDY");
        WriteParameterStatus(ms, "TimeZone", "UTC");
        WriteParameterStatus(ms, "integer_datetimes", "on");
        WriteParameterStatus(ms, "standard_conforming_strings", "on");

        ms.WriteByte((byte)'K');
        WriteInt32BigEndian(ms, 12);
        WriteInt32BigEndian(ms, Environment.ProcessId);
        WriteInt32BigEndian(ms, Random.Shared.Next());

        ms.WriteByte((byte)'Z');
        WriteInt32BigEndian(ms, 5);
        ms.WriteByte((byte)'I');

        await _clientStream.WriteAsync(ms.ToArray(), cancellationToken);
        await _clientStream.FlushAsync(cancellationToken);

        _logger.LogDebug("Отправлены AuthOk + ParameterStatus + ReadyForQuery клиенту");
    }

    private static void WriteParameterStatus(MemoryStream ms, string name, string value)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);
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
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buf, value);
        ms.Write(buf);
    }

    private async Task SendErrorToClientAsync(string message, string sqlState, CancellationToken cancellationToken)
    {
        try
        {
            var msg = System.Text.Encoding.UTF8.GetBytes(message);
            var codeBytes = System.Text.Encoding.UTF8.GetBytes(sqlState);
            
            // 'E' + length + 'S' + severity + null + 'C' + code + null + 'M' + message + null + null
            var len = 4 + 1 + 1 + 1 + 1 + codeBytes.Length + 1 + 1 + msg.Length + 1 + 1;
            var response = new byte[1 + len];

            response[0] = (byte)'E';
            response[1] = (byte)(len >> 24);
            response[2] = (byte)(len >> 16);
            response[3] = (byte)(len >> 8);
            response[4] = (byte)len;
            
            // Severity 'S'
            response[5] = (byte)'S';
            response[6] = (byte)'F'; // FATAL
            response[7] = 0;
            
            // Code 'C'
            response[8] = (byte)'C';
            var pos = 9;
            Array.Copy(codeBytes, 0, response, pos, codeBytes.Length);
            pos += codeBytes.Length;
            response[pos++] = 0;
            
            // Message 'M'
            response[pos++] = (byte)'M';
            Array.Copy(msg, 0, response, pos, msg.Length);
            pos += msg.Length;
            response[pos++] = 0;
            
            // End
            response[pos] = 0;

            await _clientStream.WriteAsync(response, cancellationToken);
        }
        catch { }
    }

    private Dictionary<string, string> ParseStartupParameters(byte[] buffer, int offset, int length)
    {
        var parameters = new Dictionary<string, string>();
        if (length <= 0 || offset + length > buffer.Length) return parameters;

        var end = offset + length;
        while (offset < end - 1)
        {
            var keyStart = offset;
            while (offset < end && buffer[offset] != 0) offset++;
            if (offset >= end) break;

            var key = System.Text.Encoding.UTF8.GetString(buffer, keyStart, offset - keyStart);
            offset++;
            if (string.IsNullOrEmpty(key)) break;

            var valueStart = offset;
            while (offset < end && buffer[offset] != 0) offset++;
            var value = System.Text.Encoding.UTF8.GetString(buffer, valueStart, offset - valueStart);
            offset++;

            parameters[key] = value;
        }
        return parameters;
    }

    private async Task ProxyDirectionAsync(Stream source, Stream destination, string direction, CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var bytesRead = await source.ReadAsync(buffer, cancellationToken);
                if (bytesRead == 0) break;

                await destination.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                await destination.FlushAsync(cancellationToken);
            }
        }
        catch (IOException) { }
        catch (OperationCanceledException) { }
    }

    public void Dispose()
    {
        _clientStream.Dispose();
        _clientSocket.Dispose();
    }
}
