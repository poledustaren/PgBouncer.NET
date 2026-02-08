using System.Buffers.Binary;
using System.Net.Sockets;
using PgBouncer.Core.Configuration;
using Microsoft.Extensions.Logging;

namespace PgBouncer.Core.Protocol;

/// <summary>
/// Обработчик подключения к PostgreSQL бэкенду
/// </summary>
public class BackendConnector
{
    private readonly BackendConfig _config;
    private readonly ILogger? _logger;

    public BackendConnector(BackendConfig config, ILogger? logger = null)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Установить соединение с PostgreSQL и пройти аутентификацию
    /// </summary>
    public async Task<NetworkStream> ConnectAndAuthenticateAsync(
        string database,
        string username,
        string password,
        CancellationToken cancellationToken = default)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        try
        {
            // Подключаемся к PostgreSQL
            await socket.ConnectAsync(_config.Host, _config.Port, cancellationToken);
            var stream = new NetworkStream(socket, ownsSocket: true);

            _logger?.LogDebug("Подключено к PostgreSQL {Host}:{Port}", _config.Host, _config.Port);

            // Отправляем StartupMessage
            var startupMessage = PostgresAuth.CreateStartupMessage(database, username);
            await stream.WriteAsync(startupMessage, cancellationToken);
            await stream.FlushAsync(cancellationToken);

            _logger?.LogDebug("Отправлен StartupMessage для {Database}/{Username}", database, username);

            // Читаем ответ сервера
            var buffer = new byte[1024];
            var bytesRead = await stream.ReadAsync(buffer, cancellationToken);

            if (bytesRead < 5)
                throw new InvalidOperationException("Invalid response from PostgreSQL");

            // Парсим тип сообщения
            var messageType = (char)buffer[0];
            var length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1));

            if (messageType == 'R') // AuthenticationRequest
            {
                await HandleAuthenticationAsync(stream, buffer, bytesRead, username, password, cancellationToken);
            }
            else if (messageType == 'E') // ErrorResponse
            {
                throw new InvalidOperationException("PostgreSQL returned error during startup");
            }

            _logger?.LogInformation("Успешная аутентификация для {Database}/{Username}", database, username);

            return stream;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Обработка аутентификации
    /// </summary>
    private async Task HandleAuthenticationAsync(
        NetworkStream stream,
        byte[] buffer,
        int bytesRead,
        string username,
        string password,
        CancellationToken cancellationToken)
    {
        // Первые 5 байт: тип (1) + длина (4)
        var authType = (AuthenticationType)BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(5));

        _logger?.LogDebug("Получен запрос аутентификации: {AuthType}", authType);

        switch (authType)
        {
            case AuthenticationType.Ok:
                // Уже аутентифицированы (trust)
                await WaitForReadyForQueryAsync(stream, cancellationToken);
                break;

            case AuthenticationType.CleartextPassword:
                // Отправляем пароль в открытом виде (не рекомендуется)
                var clearPasswordMsg = PostgresAuth.CreatePasswordMessage(password);
                await stream.WriteAsync(clearPasswordMsg, cancellationToken);
                await stream.FlushAsync(cancellationToken);
                await HandleAuthResponseAsync(stream, username, password, cancellationToken);
                break;

            case AuthenticationType.MD5Password:
                // MD5 аутентификация
                var salt = new byte[4];
                Array.Copy(buffer, 9, salt, 0, 4);

                var md5Password = PostgresAuth.GenerateMd5Password(username, password, salt);
                var md5PasswordMsg = PostgresAuth.CreatePasswordMessage(md5Password);

                await stream.WriteAsync(md5PasswordMsg, cancellationToken);
                await stream.FlushAsync(cancellationToken);

                _logger?.LogDebug("Отправлен MD5 пароль");
                await HandleAuthResponseAsync(stream, username, password, cancellationToken);
                break;

            default:
                throw new NotSupportedException($"Тип аутентификации {authType} не поддерживается");
        }
    }

    /// <summary>
    /// Обработка ответа на аутентификацию
    /// </summary>
    private async Task HandleAuthResponseAsync(
        NetworkStream stream,
        string username,
        string password,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[1024];

        while (true)
        {
            var bytesRead = await stream.ReadAsync(buffer, cancellationToken);
            if (bytesRead < 5) break;

            var messageType = (char)buffer[0];

            switch (messageType)
            {
                case 'R': // AuthenticationRequest
                    var authType = (AuthenticationType)BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(5));
                    if (authType == AuthenticationType.Ok)
                    {
                        _logger?.LogDebug("Аутентификация успешна");
                        // Продолжаем читать остальные сообщения
                    }
                    else
                    {
                        throw new InvalidOperationException($"Неожиданный тип аутентификации: {authType}");
                    }
                    break;

                case 'S': // ParameterStatus
                    // Игнорируем параметры сервера
                    break;

                case 'K': // BackendKeyData
                    // Игнорируем (pid и secret key)
                    break;

                case 'Z': // ReadyForQuery
                    _logger?.LogDebug("Сервер готов к запросам");
                    return;

                case 'E': // ErrorResponse
                    throw new InvalidOperationException("Ошибка аутентификации");
            }

            // Если в буфере несколько сообщений, продолжаем парсинг
            // (упрощенная реализация - читаем по одному)
        }
    }

    /// <summary>
    /// Ожидание ReadyForQuery после trust auth
    /// </summary>
    private async Task WaitForReadyForQueryAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var buffer = new byte[1024];

        while (true)
        {
            var bytesRead = await stream.ReadAsync(buffer, cancellationToken);
            if (bytesRead < 1) break;

            var messageType = (char)buffer[0];
            if (messageType == 'Z') // ReadyForQuery
            {
                return;
            }
        }
    }
}
