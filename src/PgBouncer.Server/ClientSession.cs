using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server;

/// <summary>
/// Клиентская сессия с поддержкой Transaction Pooling
/// </summary>
public class ClientSession : IDisposable
{
    private readonly Socket _clientSocket;
    private readonly NetworkStream _clientStream;
    private readonly PgBouncerConfig _config;
    private readonly PoolManager _poolManager;
    private readonly ILogger _logger;
    private readonly TransactionTracker _transactionTracker = new();

    private string? _database;
    private string? _username;
    private string? _password;
    private ServerConnection? _backendConnection;
    private bool _disposed;

    public ClientSession(
        Socket clientSocket,
        PgBouncerConfig config,
        PoolManager poolManager,
        ILogger logger)
    {
        _clientSocket = clientSocket;
        _clientStream = new NetworkStream(clientSocket, ownsSocket: false);
        _config = config;
        _poolManager = poolManager;
        _logger = logger;
    }

    /// <summary>
    /// Запустить обработку сессии
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("=== НОВАЯ СЕССИЯ (pool mode: {Mode}) ===", _config.Pool.Mode);

            // 1. Читаем StartupMessage от клиента
            var startupMessage = await ReadStartupMessageAsync(cancellationToken);
            if (startupMessage == null) return;

            _logger.LogInformation(">>> КЛИЕНТ: {Database}/{User}", _database, _username);

            // 2. Получаем соединение из пула
            await AcquireBackendConnectionAsync(cancellationToken);

            // 3. Форвардим StartupMessage НЕ нужно - соединение уже аутентифицировано!
            // Отправляем клиенту AuthenticationOk и ReadyForQuery
            await SendAuthOkToClientAsync(cancellationToken);

            // 4. Главный цикл обработки сообщений
            await ProcessMessagesAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "!!! ОШИБКА в сессии: {Message}", ex.Message);
        }
        finally
        {
            ReleaseBackendConnection();
        }
    }

    /// <summary>
    /// Читает StartupMessage от клиента
    /// </summary>
    private async Task<byte[]?> ReadStartupMessageAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];
        var bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);

        if (bytesRead < 8)
        {
            _logger.LogWarning("Слишком мало данных: {Bytes} байт", bytesRead);
            return null;
        }

        // Проверяем на SSLRequest
        var protocolCode = (buffer[4] << 24) | (buffer[5] << 16) | (buffer[6] << 8) | buffer[7];

        if (protocolCode == 0x04D2162F) // SSLRequest
        {
            _logger.LogDebug("SSLRequest -> отвечаем 'N'");
            await _clientStream.WriteAsync(new byte[] { (byte)'N' }, 0, 1, cancellationToken);
            await _clientStream.FlushAsync(cancellationToken);

            // Читаем настоящий StartupMessage
            bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
            if (bytesRead < 8) return null;
        }

        // Парсим параметры
        var length = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
        var parameters = ParseStartupParameters(buffer, 8, length - 8);

        _database = parameters.GetValueOrDefault("database") ?? "postgres";
        _username = parameters.GetValueOrDefault("user") ?? "postgres";
        _password = _config.Backend.AdminPassword;

        var startupMessage = new byte[length];
        Array.Copy(buffer, 0, startupMessage, 0, length);
        return startupMessage;
    }

    /// <summary>
    /// Получает соединение из пула
    /// </summary>
    private async Task AcquireBackendConnectionAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Запрос соединения из пула для {Database}/{User}...", _database, _username);

        _backendConnection = await _poolManager.AcquireConnectionAsync(
            _database!,
            _username!,
            _password!,
            cancellationToken);

        _logger.LogInformation("Получено соединение {ConnectionId} из пула", _backendConnection.Id);
    }

    /// <summary>
    /// Отправляет AuthenticationOk и ReadyForQuery клиенту
    /// </summary>
    private async Task SendAuthOkToClientAsync(CancellationToken cancellationToken)
    {
        // AuthenticationOk: 'R' + length(8) + status(0)
        var authOk = new byte[] { (byte)'R', 0, 0, 0, 8, 0, 0, 0, 0 };
        await _clientStream.WriteAsync(authOk, cancellationToken);

        // ReadyForQuery: 'Z' + length(5) + status('I' = idle)
        var readyForQuery = new byte[] { (byte)'Z', 0, 0, 0, 5, (byte)'I' };
        await _clientStream.WriteAsync(readyForQuery, cancellationToken);

        await _clientStream.FlushAsync(cancellationToken);

        _logger.LogDebug("Отправлено AuthOk + ReadyForQuery клиенту");
    }

    /// <summary>
    /// Главный цикл обработки сообщений
    /// </summary>
    private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        var clientBuffer = new byte[32768];
        var backendBuffer = new byte[32768];

        while (!cancellationToken.IsCancellationRequested)
        {
            // Проверяем читаемость обоих стримов
            var clientReadTask = _clientStream.ReadAsync(clientBuffer, 0, clientBuffer.Length, cancellationToken);
            var backendReadTask = _backendConnection!.Stream.ReadAsync(backendBuffer, 0, backendBuffer.Length, cancellationToken);

            var completedTask = await Task.WhenAny(clientReadTask, backendReadTask);

            if (completedTask == clientReadTask)
            {
                // Данные от клиента -> бэкенду
                var bytesRead = await clientReadTask;
                if (bytesRead == 0)
                {
                    _logger.LogDebug("Клиент закрыл соединение");
                    break;
                }

                _backendConnection.UpdateActivity();
                await _backendConnection.Stream.WriteAsync(clientBuffer.AsMemory(0, bytesRead), cancellationToken);
                await _backendConnection.Stream.FlushAsync(cancellationToken);

                _logger.LogTrace("Client->PG: {Bytes} байт", bytesRead);
            }
            else
            {
                // Данные от бэкенда -> клиенту
                var bytesRead = await backendReadTask;
                if (bytesRead == 0)
                {
                    _logger.LogWarning("Бэкенд закрыл соединение");
                    break;
                }

                // Отслеживаем состояние транзакции
                var dataSlice = backendBuffer.AsMemory(0, bytesRead);
                bool foundReadyForQuery = _transactionTracker.ProcessBackendData(dataSlice.Span);

                await _clientStream.WriteAsync(backendBuffer.AsMemory(0, bytesRead), cancellationToken);
                await _clientStream.FlushAsync(cancellationToken);

                _logger.LogTrace("PG->Client: {Bytes} байт, TxState: {State}",
                    bytesRead, _transactionTracker.State);

                // Transaction pooling: возвращаем соединение если вне транзакции
                if (_config.Pool.Mode == PoolingMode.Transaction &&
                    foundReadyForQuery &&
                    _transactionTracker.CanReleaseToPool)
                {
                    _logger.LogDebug("Transaction pooling: соединение готово к переиспользованию");
                    // В данной реализации держим соединение до конца сессии,
                    // но статистика транзакций будет точной
                }
            }
        }
    }

    /// <summary>
    /// Возвращает соединение в пул
    /// </summary>
    private void ReleaseBackendConnection()
    {
        if (_backendConnection != null)
        {
            _logger.LogDebug("Возвращаем соединение {ConnectionId} в пул", _backendConnection.Id);
            _poolManager.ReleaseConnection(_backendConnection);
            _backendConnection = null;
        }
    }

    /// <summary>
    /// Парсит параметры из StartupMessage
    /// </summary>
    private Dictionary<string, string> ParseStartupParameters(byte[] buffer, int offset, int length)
    {
        var parameters = new Dictionary<string, string>();

        if (length <= 0 || offset + length > buffer.Length)
            return parameters;

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

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        ReleaseBackendConnection();
        _clientStream.Dispose();
        _clientSocket.Dispose();
    }
}
