using System.IO.Pipelines;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server;

/// <summary>
/// Клиентская сессия - обрабатывает одно клиентское соединение
/// </summary>
public class ClientSession : IDisposable
{
    private readonly Socket _clientSocket;
    private readonly NetworkStream _clientStream;
    private readonly PoolManager _poolManager;
    private readonly PgBouncerConfig _config;
    private readonly ILogger _logger;

    private ServerConnection? _currentServerConnection;
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
    /// Запустить обработку сессии
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogDebug("=== НОВАЯ СЕССИЯ ===");

            // 1. Читаем первый пакет (может быть SSLRequest или StartupMessage)
            var buffer = new byte[8192];
            var bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);

            _logger.LogDebug("Получено {Bytes} байт от клиента", bytesRead);

            if (bytesRead < 8)
            {
                _logger.LogWarning("Слишком мало данных: {Bytes} байт", bytesRead);
                return;
            }

            // Проверяем на SSLRequest (код 80877103 = 0x04D2162F)
            var length = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
            var protocolCode = (buffer[4] << 24) | (buffer[5] << 16) | (buffer[6] << 8) | buffer[7];

            _logger.LogDebug("Пакет: length={Length}, code={Code:X8}", length, protocolCode);

            if (protocolCode == 0x04D2162F) // SSLRequest
            {
                _logger.LogDebug("Получен SSLRequest, отправляем 'N' (SSL не поддерживается)");
                await _clientStream.WriteAsync(new byte[] { (byte)'N' }, 0, 1, cancellationToken);
                await _clientStream.FlushAsync(cancellationToken);

                // Читаем настоящий StartupMessage
                _logger.LogInformation("Ожидание StartupMessage после SSLRequest...");
                bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                _logger.LogInformation("После SSLRequest получено {Bytes} байт", bytesRead);

                if (bytesRead < 8)
                {
                    _logger.LogWarning("Слишком мало данных после SSLRequest");
                    return;
                }

                length = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
                protocolCode = (buffer[4] << 24) | (buffer[5] << 16) | (buffer[6] << 8) | buffer[7];
                _logger.LogDebug("StartupMessage: length={Length}, protocol={Code:X8}", length, protocolCode);
            }

            // 2. Парсим параметры StartupMessage
            var paramsLength = length - 8;
            _logger.LogInformation("Длина пакета: {Length}, длина параметров: {ParamsLength}", length, paramsLength);
            
            if (paramsLength <= 0)
            {
                _logger.LogWarning("Некорректная длина параметров: {Length}", paramsLength);
                return;
            }
            
            _logger.LogInformation("Вызов ParseStartupParameters с offset=8, length={ParamsLength}", paramsLength);
            var parameters = ParseStartupParameters(buffer, 8, paramsLength);
            _logger.LogInformation("ParseStartupParameters вернул {Count} параметров", parameters.Count);

            _database = parameters.GetValueOrDefault("database") ?? "postgres";
            _username = parameters.GetValueOrDefault("user") ?? "postgres";

            _logger.LogInformation(">>> КЛИЕНТ: database={Database}, user={User}", _database, _username);
            _logger.LogDebug("Параметров получено: {Count}", parameters?.Count ?? -1);

            // 3. Отправляем AuthenticationOk
            _logger.LogDebug("Начинаем отправку AuthenticationOk...");
            try
            {
                await SendAuthenticationOkAsync(cancellationToken);
                _logger.LogDebug("AuthenticationOk успешно отправлен");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ОШИБКА при отправке AuthenticationOk: {Message}", ex.Message);
                throw;
            }

            // 4. Получаем пароль
            _password = GetDefaultPassword(_username);
            _logger.LogDebug("Пароль для {User}: {Password}", _username, _password);

            // 5. Получаем серверное соединение из пула
            _logger.LogDebug("Запрашиваем соединение из пула для {Database}/{User}", _database, _username);

            _currentServerConnection = await _poolManager.AcquireConnectionAsync(
                _database,
                _username,
                _password,
                cancellationToken);

            _logger.LogInformation(">>> СОЕДИНЕНИЕ: {ConnectionId} для {Database}/{User}",
                _currentServerConnection.Id, _database, _username);

            // 6. Проксируем данные
            await ProxyDataAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "!!! ОШИБКА в сессии: {Message}", ex.Message);
        }
        finally
        {
            if (_currentServerConnection != null)
            {
                _poolManager.ReleaseConnection(_currentServerConnection);
                _logger.LogDebug("Соединение возвращено в пул");
            }
        }
    }

    /// <summary>
    /// Парсит параметры из StartupMessage
    /// </summary>
    private Dictionary<string, string> ParseStartupParameters(byte[] buffer, int offset, int length)
    {
        _logger.LogDebug("ParseStartupParameters: offset={Offset}, length={Length}", offset, length);
        
        var parameters = new Dictionary<string, string>();
        var end = offset + length;
        
        // Защита от некорректных данных
        if (length <= 0 || end > buffer.Length)
        {
            _logger.LogWarning("Некорректные параметры: length={Length}, end={End}, buffer.Length={BufferLength}", 
                length, end, buffer.Length);
            return parameters;
        }

        int iterations = 0;
        int maxIterations = 100; // Защита от бесконечного цикла
        
        while (offset < end - 1 && iterations < maxIterations)
        {
            iterations++;
            
            // Читаем ключ (null-terminated)
            var keyStart = offset;
            while (offset < end && buffer[offset] != 0) offset++;
            
            if (offset >= end) break; // Не нашли null-terminator
            
            var key = System.Text.Encoding.UTF8.GetString(buffer, keyStart, offset - keyStart);
            offset++; // пропускаем null

            if (string.IsNullOrEmpty(key)) break;

            // Читаем значение (null-terminated)
            var valueStart = offset;
            while (offset < end && buffer[offset] != 0) offset++;
            
            if (offset >= end) break; // Не нашли null-terminator
            
            var value = System.Text.Encoding.UTF8.GetString(buffer, valueStart, offset - valueStart);
            offset++; // пропускаем null

            parameters[key] = value;
            _logger.LogDebug("Спарсен параметр: {Key}={Value}", key, value);
        }
        
        if (iterations >= maxIterations)
        {
            _logger.LogWarning("Достигнут лимит итераций при парсинге параметров");
        }

        return parameters;
    }

    /// <summary>
    /// Отправить клиенту AuthenticationOk + ParameterStatus + ReadyForQuery
    /// </summary>
    private async Task SendAuthenticationOkAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Начинаем формирование ответа...");
        
        // Собираем все сообщения в один буфер
        var messages = new System.Collections.Generic.List<byte[]>();
        
        // AuthenticationOk: 'R' + length(8) + auth_type(0)
        messages.Add(new byte[] { (byte)'R', 0, 0, 0, 8, 0, 0, 0, 0 });

        // ParameterStatus сообщения - Npgsql ТРЕБУЕТ эти параметры!
        messages.Add(CreateParameterStatus("server_version", "14.0"));
        messages.Add(CreateParameterStatus("server_encoding", "UTF8"));
        messages.Add(CreateParameterStatus("client_encoding", "UTF8"));
        messages.Add(CreateParameterStatus("DateStyle", "ISO, MDY"));
        messages.Add(CreateParameterStatus("TimeZone", "UTC"));
        messages.Add(CreateParameterStatus("integer_datetimes", "on"));
        messages.Add(CreateParameterStatus("standard_conforming_strings", "on"));

        // BackendKeyData: 'K' + length(12) + pid(4) + secret(4)
        var pid = Environment.ProcessId;
        messages.Add(new byte[] {
            (byte)'K', 0, 0, 0, 12,
            (byte)((pid >> 24) & 0xFF), (byte)((pid >> 16) & 0xFF),
            (byte)((pid >> 8) & 0xFF), (byte)(pid & 0xFF),
            0, 0, 0, 1
        });

        // ReadyForQuery: 'Z' + length(5) + status('I' = idle)
        messages.Add(new byte[] { (byte)'Z', 0, 0, 0, 5, (byte)'I' });

        // Вычисляем общий размер
        int totalSize = messages.Sum(m => m.Length);
        _logger.LogDebug("Общий размер ответа: {TotalSize} байт", totalSize);
        
        // Объединяем все сообщения в один буфер
        var buffer = new byte[totalSize];
        int offset = 0;
        foreach (var msg in messages)
        {
            Buffer.BlockCopy(msg, 0, buffer, offset, msg.Length);
            offset += msg.Length;
        }

        // Отправляем синхронно с таймаутом
        _logger.LogDebug("Отправка {Bytes} байт клиенту...", buffer.Length);
        
        try
        {
            _clientStream.WriteTimeout = 5000; // 5 секунд
            _clientStream.Write(buffer, 0, buffer.Length);
            _clientStream.Flush();
        }
        catch (System.IO.IOException ex) when (ex.InnerException is System.Net.Sockets.SocketException)
        {
            _logger.LogError("Таймаут при отправке данных клиенту");
            throw;
        }

        _logger.LogDebug("Отправлен AuthenticationOk + ParameterStatus + ReadyForQuery клиенту ({Bytes} байт)", buffer.Length);
    }

    /// <summary>
    /// Создать ParameterStatus сообщение
    /// </summary>
    private byte[] CreateParameterStatus(string name, string value)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);
        var length = 4 + nameBytes.Length + 1 + valueBytes.Length + 1;

        var message = new byte[1 + length];
        message[0] = (byte)'S';
        message[1] = (byte)((length >> 24) & 0xFF);
        message[2] = (byte)((length >> 16) & 0xFF);
        message[3] = (byte)((length >> 8) & 0xFF);
        message[4] = (byte)(length & 0xFF);
        
        Buffer.BlockCopy(nameBytes, 0, message, 5, nameBytes.Length);
        message[5 + nameBytes.Length] = 0;
        Buffer.BlockCopy(valueBytes, 0, message, 6 + nameBytes.Length, valueBytes.Length);
        message[6 + nameBytes.Length + valueBytes.Length] = 0;
        
        return message;
    }

    /// <summary>
    /// Отправить ParameterStatus сообщение
    /// </summary>
    private async Task SendParameterStatusAsync(string name, string value, CancellationToken cancellationToken)
    {
        // ParameterStatus: 'S' + length + name\0 + value\0
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);
        var length = 4 + nameBytes.Length + 1 + valueBytes.Length + 1;

        var message = new byte[1 + length];
        message[0] = (byte)'S';
        message[1] = (byte)((length >> 24) & 0xFF);
        message[2] = (byte)((length >> 16) & 0xFF);
        message[3] = (byte)((length >> 8) & 0xFF);
        message[4] = (byte)(length & 0xFF);

        Array.Copy(nameBytes, 0, message, 5, nameBytes.Length);
        message[5 + nameBytes.Length] = 0; // null terminator
        Array.Copy(valueBytes, 0, message, 6 + nameBytes.Length, valueBytes.Length);
        message[6 + nameBytes.Length + valueBytes.Length] = 0; // null terminator

        await _clientStream.WriteAsync(message, cancellationToken);
    }

    /// <summary>
    /// Получить пароль по умолчанию для пользователя
    /// </summary>
    private string GetDefaultPassword(string username)
    {
        // Для testuser1 -> testpass1, и т.д.
        if (username.StartsWith("testuser"))
        {
            var num = username.Replace("testuser", "");
            return $"testpass{num}";
        }
        return _config.Backend.AdminPassword ?? "password";
    }

    /// <summary>
    /// Проксирование данных между клиентом и сервером
    /// </summary>
    private async Task ProxyDataAsync(CancellationToken cancellationToken)
    {
        if (_currentServerConnection == null)
            return;

        var clientToServer = ProxyDirectionAsync(
            _clientStream,
            _currentServerConnection.Stream,
            "Client->Server",
            cancellationToken);

        var serverToClient = ProxyDirectionAsync(
            _currentServerConnection.Stream,
            _clientStream,
            "Server->Client",
            cancellationToken);

        // Ждём завершения любого из направлений
        await Task.WhenAny(clientToServer, serverToClient);

        _logger.LogInformation("Проксирование завершено");
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

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var bytesRead = await source.ReadAsync(buffer, cancellationToken);
                if (bytesRead == 0)
                    break;

                await destination.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                await destination.FlushAsync(cancellationToken);

                _logger.LogTrace("{Direction}: {Bytes} байт", direction, bytesRead);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Ошибка проксирования {Direction}", direction);
        }
    }

    public void Dispose()
    {
        _clientStream.Dispose();
        _clientSocket.Dispose();
    }
}
