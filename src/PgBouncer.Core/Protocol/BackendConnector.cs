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
    private readonly Microsoft.Extensions.Logging.ILogger? _logger;

    public BackendConnector(BackendConfig config, Microsoft.Extensions.Logging.ILogger? logger = null)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Установить соединение с PostgreSQL и пройти аутентификацию
    /// Возвращает кортеж (socket, stream) для управления жизненным циклом
    /// </summary>
    public async Task<(Socket Socket, NetworkStream Stream)> ConnectAndAuthenticateAsync(
        string database,
        string username,
        string password,
        CancellationToken cancellationToken = default)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.ReceiveTimeout = 10000; // 10 секунд
        socket.SendTimeout = 10000;

        try
        {
            // Подключаемся к PostgreSQL с таймаутом
            _logger?.LogInformation("Подключаемся к PostgreSQL {Host}:{Port}...", _config.Host, _config.Port);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(10)); // 10 секунд на подключение

            try
            {
                await socket.ConnectAsync(_config.Host, _config.Port, cts.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"Таймаут подключения к PostgreSQL {_config.Host}:{_config.Port}");
            }

            var stream = new NetworkStream(socket, ownsSocket: false);
            stream.ReadTimeout = 10000;
            stream.WriteTimeout = 10000;

            _logger?.LogInformation("Подключено к PostgreSQL {Host}:{Port}", _config.Host, _config.Port);

            // Отправляем StartupMessage
            var startupMessage = PostgresAuth.CreateStartupMessage(database, username);
            await stream.WriteAsync(startupMessage, cancellationToken);
            await stream.FlushAsync(cancellationToken);

            _logger?.LogInformation("Отправлен StartupMessage для {Database}/{Username}", database, username);

            // Читаем ответ сервера
            _logger?.LogInformation("Ожидание ответа от PostgreSQL...");
            var buffer = new byte[1024];
            var bytesRead = await stream.ReadAsync(buffer, cancellationToken);
            _logger?.LogInformation("Получено {BytesRead} байт от PostgreSQL", bytesRead);

            if (bytesRead < 5)
                throw new InvalidOperationException("Invalid response from PostgreSQL");

            // Парсим тип сообщения
            var messageType = (char)buffer[0];
            var length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1));

            if (messageType == 'R') // AuthenticationRequest
            {
                _logger?.LogInformation("Получен AuthenticationRequest, обрабатываем...");
                await HandleAuthenticationAsync(stream, buffer, bytesRead, username, password, cancellationToken);
            }
            else if (messageType == 'E') // ErrorResponse
            {
                _logger?.LogError("PostgreSQL вернул ошибку при старте");
                throw new InvalidOperationException("PostgreSQL returned error during startup");
            }
            else
            {
                _logger?.LogWarning("Неожиданный тип сообщения от PostgreSQL: {MessageType}", messageType);
            }

            _logger?.LogInformation("Успешная аутентификация для {Database}/{Username}", database, username);

            return (socket, stream);
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

        _logger?.LogInformation("Получен запрос аутентификации: {AuthType}", authType);

        switch (authType)
        {
            case AuthenticationType.Ok:
                _logger?.LogInformation("AuthenticationType.Ok (trust), ожидание ReadyForQuery...");
                await WaitForReadyForQueryAsync(stream, cancellationToken);
                _logger?.LogInformation("ReadyForQuery получен после trust auth");
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

                _logger?.LogInformation("Отправлен MD5 пароль");
                await HandleAuthResponseAsync(stream, username, password, cancellationToken);
                break;

            case AuthenticationType.SASL:
                // SCRAM-SHA-256 аутентификация
                _logger?.LogInformation("Начинаем SASL/SCRAM-SHA-256 аутентификацию");
                await HandleSaslAuthenticationAsync(stream, buffer, bytesRead, username, password, cancellationToken);
                break;

            default:
                throw new NotSupportedException($"Тип аутентификации {authType} не поддерживается");
        }
    }

    /// <summary>
    /// Обработка SASL/SCRAM-SHA-256 аутентификации
    /// </summary>
    private async Task HandleSaslAuthenticationAsync(
        NetworkStream stream,
        byte[] initialBuffer,
        int initialBytesRead,
        string username,
        string password,
        CancellationToken cancellationToken)
    {
        var scram = new ScramSha256Authenticator(username, password);

        // 1. Отправляем SASLInitialResponse
        var initialResponse = scram.CreateInitialResponse();
        await stream.WriteAsync(initialResponse, cancellationToken);
        await stream.FlushAsync(cancellationToken);
        _logger?.LogInformation("Отправлен SASLInitialResponse");

        // 2. Читаем AuthenticationSASLContinue
        var buffer = new byte[8192];
        var bytesRead = await stream.ReadAsync(buffer, cancellationToken);

        if (bytesRead < 9)
            throw new InvalidOperationException("Неполный ответ AuthenticationSASLContinue");

        var msgType = (char)buffer[0];
        var msgLen = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1));
        var authCode = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(5));

        if (msgType != 'R' || authCode != (int)AuthenticationType.SASLContinue)
            throw new InvalidOperationException($"Ожидался AuthenticationSASLContinue, получено: {msgType}/{authCode}");

        // Server-first-message начинается после 9 байт (type + length + authCode)
        var serverFirstLength = msgLen - 8;
        var serverFirstData = new byte[serverFirstLength];
        Array.Copy(buffer, 9, serverFirstData, 0, serverFirstLength);
        _logger?.LogInformation("Получен AuthenticationSASLContinue: {Data}", System.Text.Encoding.UTF8.GetString(serverFirstData));

        // 3. Обрабатываем server-first и отправляем client-final
        var clientFinal = scram.ProcessServerFirstMessage(serverFirstData);
        await stream.WriteAsync(clientFinal, cancellationToken);
        await stream.FlushAsync(cancellationToken);
        _logger?.LogInformation("Отправлен SASLResponse (client-final)");

        // 4. Читаем AuthenticationSASLFinal
        bytesRead = await stream.ReadAsync(buffer, cancellationToken);

        if (bytesRead < 9)
            throw new InvalidOperationException("Неполный ответ AuthenticationSASLFinal");

        msgType = (char)buffer[0];
        msgLen = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1));
        authCode = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(5));

        if (msgType != 'R' || authCode != (int)AuthenticationType.SASLFinal)
            throw new InvalidOperationException($"Ожидался AuthenticationSASLFinal, получено: {msgType}/{authCode}");

        // Можем проверить server signature (опционально)
        var serverFinalLength = msgLen - 8;
        var serverFinalData = new byte[serverFinalLength];
        Array.Copy(buffer, 9, serverFinalData, 0, serverFinalLength);
        scram.VerifyServerFinalMessage(serverFinalData);
        _logger?.LogInformation("Получен AuthenticationSASLFinal, верификация прошла");

        // 5. Ждём AuthenticationOk и ReadyForQuery
        await HandleAuthResponseAsync(stream, username, password, cancellationToken);
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
        var buffer = new byte[8192];
        int bufferOffset = 0;
        int bufferLength = 0;

        while (true)
        {
            // Читаем данные если буфер пуст
            if (bufferOffset >= bufferLength)
            {
                bufferOffset = 0;
                bufferLength = await stream.ReadAsync(buffer, cancellationToken);
                if (bufferLength < 5) break;
            }

            // Парсим сообщение
            var messageType = (char)buffer[bufferOffset];
            var messageLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(bufferOffset + 1));
            var totalMessageLength = 1 + messageLength; // тип (1 байт) + длина + данные

            // Проверяем что у нас есть полное сообщение
            if (bufferOffset + totalMessageLength > bufferLength)
            {
                // Неполное сообщение - нужно дочитать
                // Сдвигаем оставшиеся данные в начало буфера и читаем ещё
                var remaining = bufferLength - bufferOffset;
                Array.Copy(buffer, bufferOffset, buffer, 0, remaining);
                bufferOffset = 0;
                bufferLength = remaining;

                var read = await stream.ReadAsync(buffer.AsMemory(bufferLength), cancellationToken);
                bufferLength += read;
                continue;
            }

            switch (messageType)
            {
                case 'R': // AuthenticationRequest
                    var authType = (AuthenticationType)BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(bufferOffset + 5));
                    if (authType == AuthenticationType.Ok)
                    {
                        _logger?.LogInformation("Аутентификация успешна");
                    }
                    else
                    {
                        throw new InvalidOperationException($"Неожиданный тип аутентификации: {authType}");
                    }
                    break;

                case 'S': // ParameterStatus
                    _logger?.LogInformation("Получен ParameterStatus");
                    break;

                case 'K': // BackendKeyData
                    _logger?.LogInformation("Получен BackendKeyData");
                    break;

                case 'Z': // ReadyForQuery
                    _logger?.LogInformation("Сервер готов к запросам (ReadyForQuery)");
                    return;

                case 'E': // ErrorResponse
                    throw new InvalidOperationException("Ошибка аутентификации");
            }

            // Переходим к следующему сообщению
            bufferOffset += totalMessageLength;
        }
    }

    /// <summary>
    /// Ожидание ReadyForQuery после trust auth
    /// </summary>
    private async Task WaitForReadyForQueryAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];
        int bufferOffset = 0;
        int bufferLength = 0;

        while (true)
        {
            // Читаем данные если буфер пуст
            if (bufferOffset >= bufferLength)
            {
                bufferOffset = 0;
                bufferLength = await stream.ReadAsync(buffer, cancellationToken);
                if (bufferLength < 5) break;
            }

            // Парсим сообщение
            var messageType = (char)buffer[bufferOffset];
            var messageLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(bufferOffset + 1));
            var totalMessageLength = 1 + messageLength;

            // Проверяем что у нас есть полное сообщение
            if (bufferOffset + totalMessageLength > bufferLength)
            {
                // Неполное сообщение - дочитываем
                var remaining = bufferLength - bufferOffset;
                Array.Copy(buffer, bufferOffset, buffer, 0, remaining);
                bufferOffset = 0;
                bufferLength = remaining;

                var read = await stream.ReadAsync(buffer.AsMemory(bufferLength), cancellationToken);
                bufferLength += read;
                continue;
            }

            if (messageType == 'Z') // ReadyForQuery
            {
                _logger?.LogInformation("Получен ReadyForQuery после trust auth");
                return;
            }

            // Пропускаем другие сообщения (ParameterStatus, BackendKeyData)
            bufferOffset += totalMessageLength;
        }
    }
}
