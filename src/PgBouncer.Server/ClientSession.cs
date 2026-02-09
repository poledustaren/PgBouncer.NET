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

    /// <summary>
    /// Запустить обработку сессии - ПРОЗРАЧНОЕ ПРОКСИРОВАНИЕ с лимитом
    /// </summary>
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
                    _logger.LogWarning("Слишком мало данных после SSLRequest");
                    return;
                }
                length = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
            }

            // Сохраняем StartupMessage для форварда
            var startupLength = length;
            var startupMessage = new byte[startupLength];
            Array.Copy(buffer, 0, startupMessage, 0, startupLength);

            // Парсим параметры для логирования
            var parameters = ParseStartupParameters(buffer, 8, length - 8);
            _database = parameters.GetValueOrDefault("database") ?? "postgres";
            _username = parameters.GetValueOrDefault("user") ?? "postgres";
            _password = _config.Backend.AdminPassword;

            // Обновляем информацию о сессии
            _sessionInfo.Database = _database;
            _sessionInfo.Username = _username;

            _logger.LogInformation(">>> {Database}/{User}", _database, _username);

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

                // Записываем таймаут
                _recordTimeout();

                _logger.LogWarning("Таймаут ожидания слота ({Timeout}s)", _config.Pool.ConnectionTimeout);
                await SendErrorToClientAsync("too many connections - please try again later", cancellationToken);
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
    /// Отправить ошибку клиенту
    /// </summary>
    private async Task SendErrorToClientAsync(string message, CancellationToken cancellationToken)
    {
        try
        {
            // ErrorResponse: 'E' + length + fields
            var msg = System.Text.Encoding.UTF8.GetBytes(message);
            var response = new byte[7 + 1 + msg.Length + 1 + 1]; // E + len + S + msg + \0 + \0

            response[0] = (byte)'E';
            var len = response.Length - 1;
            response[1] = (byte)(len >> 24);
            response[2] = (byte)(len >> 16);
            response[3] = (byte)(len >> 8);
            response[4] = (byte)len;
            response[5] = (byte)'S'; // Severity
            response[6] = (byte)'F'; // FATAL
            response[7] = 0;
            response[8] = (byte)'M'; // Message
            Array.Copy(msg, 0, response, 9, msg.Length);
            response[9 + msg.Length] = 0;
            response[10 + msg.Length] = 0; // End of fields

            await _clientStream.WriteAsync(response, cancellationToken);
        }
        catch { }
    }

    /// <summary>
    /// Парсит параметры из StartupMessage
    /// </summary>
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

    /// <summary>
    /// Проксирование в одном направлении
    /// </summary>
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
