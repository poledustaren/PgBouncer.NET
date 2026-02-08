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
            // 1. Читаем Startup Message от клиента
            var pipe = PipeReader.Create(_clientStream);
            var reader = new PostgresProtocolReader(pipe);

            var startupMessage = await reader.ReadStartupMessageAsync(cancellationToken);
            if (startupMessage == null)
            {
                _logger.LogWarning("Не удалось прочитать startup message");
                return;
            }

            _database = startupMessage.Database ?? "postgres";
            _username = startupMessage.User ?? "postgres";
            
            _logger.LogInformation("Клиент подключается к {Database} как {User}", 
                _database, _username);

            // 2. Аутентификация (упрощённая - passthrough)
            // TODO: реализовать MD5 auth
            _password = "password"; // В реальности нужно получить от клиента

            // 3. Получаем серверное соединение из пула
            _currentServerConnection = await _poolManager.AcquireConnectionAsync(
                _database, 
                _username, 
                _password, 
                cancellationToken);

            _logger.LogInformation("Получено серверное соединение {ConnectionId}", 
                _currentServerConnection.Id);

            // 4. Проксируем данные между клиентом и сервером
            await ProxyDataAsync(cancellationToken);
        }
        finally
        {
            // Возвращаем соединение в пул
            if (_currentServerConnection != null)
            {
                _poolManager.ReleaseConnection(_currentServerConnection);
            }
        }
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
