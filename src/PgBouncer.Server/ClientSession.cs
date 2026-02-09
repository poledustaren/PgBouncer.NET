using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Server;

/// <summary>
/// Клиентская сессия - ПРОЗРАЧНОЕ ПРОКСИРОВАНИЕ к PostgreSQL
/// </summary>
public class ClientSession : IDisposable
{
    private readonly Socket _clientSocket;
    private readonly NetworkStream _clientStream;
    private readonly PoolManager _poolManager;
    private readonly PgBouncerConfig _config;
    private readonly ILogger _logger;

    private string? _database;
    private string? _username;
    private string? _password;

    public ClientSession(
        Socket clientSocket,
        PoolManager poolManager,
        PgBouncerConfig config,
        ILogger logger)
    {
        _clientSocket = clientSocket;
        _clientStream = new NetworkStream(clientSocket, ownsSocket: false);
        _poolManager = poolManager;
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Запустить обработку сессии - ПРОЗРАЧНОЕ ПРОКСИРОВАНИЕ
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        Socket? backendSocket = null;
        NetworkStream? backendStream = null;

        try
        {
            _logger.LogInformation("=== НОВАЯ СЕССИЯ (прозрачный режим) ===");

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
                _logger.LogInformation("SSLRequest -> отвечаем 'N'");
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

            // Парсим параметры только для логирования
            var parameters = ParseStartupParameters(buffer, 8, length - 8);
            _database = parameters.GetValueOrDefault("database") ?? "postgres";
            _username = parameters.GetValueOrDefault("user") ?? "postgres";
            _password = GetDefaultPassword(_username);

            _logger.LogInformation(">>> КЛИЕНТ: {Database}/{User}", _database, _username);

            // 2. Подключаемся к PostgreSQL НАПРЯМУЮ
            _logger.LogInformation("Подключение к PostgreSQL {Host}:{Port}...", _config.Backend.Host, _config.Backend.Port);

            backendSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await backendSocket.ConnectAsync(_config.Backend.Host, _config.Backend.Port, cancellationToken);
            backendStream = new NetworkStream(backendSocket, ownsSocket: true);

            _logger.LogInformation("Подключено к PostgreSQL!");

            // 3. Форвардим StartupMessage к PostgreSQL
            _logger.LogInformation("Форвардим StartupMessage ({Bytes} байт) к PostgreSQL", startupLength);
            await backendStream.WriteAsync(startupMessage, 0, startupLength, cancellationToken);
            await backendStream.FlushAsync(cancellationToken);

            // 4. Проксируем ВСЕ данные между клиентом и PostgreSQL
            _logger.LogInformation("Запуск двунаправленного проксирования...");

            var clientToServer = ProxyDirectionAsync(
                _clientStream,
                backendStream,
                "Client->PG",
                cancellationToken);

            var serverToClient = ProxyDirectionAsync(
                backendStream,
                _clientStream,
                "PG->Client",
                cancellationToken);

            // Ждём завершения любого из направлений
            await Task.WhenAny(clientToServer, serverToClient);

            _logger.LogInformation("Проксирование завершено");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "!!! ОШИБКА в сессии: {Message}", ex.Message);
        }
        finally
        {
            backendStream?.Close();
            backendSocket?.Close();
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
            // Читаем ключ (null-terminated)
            var keyStart = offset;
            while (offset < end && buffer[offset] != 0) offset++;

            if (offset >= end) break;

            var key = System.Text.Encoding.UTF8.GetString(buffer, keyStart, offset - keyStart);
            offset++; // пропускаем null

            if (string.IsNullOrEmpty(key)) break;

            // Читаем значение (null-terminated)
            var valueStart = offset;
            while (offset < end && buffer[offset] != 0) offset++;

            var value = System.Text.Encoding.UTF8.GetString(buffer, valueStart, offset - valueStart);
            offset++; // пропускаем null

            parameters[key] = value;
            _logger.LogDebug("Параметр: {Key}={Value}", key, value);
        }

        return parameters;
    }

    /// <summary>
    /// Получить пароль для пользователя
    /// </summary>
    private string GetDefaultPassword(string username)
    {
        // // Сначала ищем в конфигурации users
        // if (_config.Users.TryGetValue(username, out var userConfig))
        // {
        //     return userConfig.Password;
        // }

        // Используем пароль админа как дефолт
        return _config.Backend.AdminPassword;
    }

    /// <summary>
    /// Проксирование в одном направлении
    /// </summary>
    private async Task ProxyDirectionAsync(
        Stream source,
        Stream destination,
        string direction,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];
        int totalBytes = 0;

        try
        {
            _logger.LogInformation("[{Direction}] Запуск проксирования", direction);

            while (!cancellationToken.IsCancellationRequested)
            {
                var bytesRead = await source.ReadAsync(buffer, cancellationToken);
                if (bytesRead == 0)
                {
                    _logger.LogInformation("[{Direction}] Конец потока (прочитано 0 байт)", direction);
                    break;
                }

                await destination.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                await destination.FlushAsync(cancellationToken);

                totalBytes += bytesRead;
                _logger.LogDebug("[{Direction}] {Bytes} байт", direction, bytesRead);
            }

            _logger.LogInformation("[{Direction}] Завершено. Всего: {Total} байт", direction, totalBytes);
        }
        catch (IOException ex) when (ex.InnerException is SocketException)
        {
            _logger.LogInformation("[{Direction}] Соединение закрыто (всего: {Total} байт)", direction, totalBytes);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("[{Direction}] Отменено (всего: {Total} байт)", direction, totalBytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[{Direction}] Ошибка: {Message}", direction, ex.Message);
        }
    }

    public void Dispose()
    {
        _clientStream.Dispose();
        _clientSocket.Dispose();
    }
}
