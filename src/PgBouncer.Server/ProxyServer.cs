using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Server;

/// <summary>
/// TCP прокси-сервер для приёма клиентских соединений
/// </summary>
public class ProxyServer : IDisposable
{
    private readonly PgBouncerConfig _config;
    private readonly PoolManager _poolManager;
    private readonly ILogger<ProxyServer> _logger;

    private Socket? _listenerSocket;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;

    public ProxyServer(
        PgBouncerConfig config,
        PoolManager poolManager,
        ILogger<ProxyServer> logger)
    {
        _config = config;
        _poolManager = poolManager;
        _logger = logger;
    }

    /// <summary>
    /// Запустить сервер
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listenerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listenerSocket.Bind(new IPEndPoint(IPAddress.Any, _config.ListenPort));
        _listenerSocket.Listen(128);

        _logger.LogInformation("PgBouncer.NET запущен на порту {Port}", _config.ListenPort);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _acceptTask = AcceptConnectionsAsync(_cts.Token);

        await Task.CompletedTask;
    }

    /// <summary>
    /// Остановить сервер
    /// </summary>
    public async Task StopAsync()
    {
        _logger.LogInformation("Остановка PgBouncer.NET...");

        _cts?.Cancel();
        _listenerSocket?.Close();

        if (_acceptTask != null)
        {
            await _acceptTask;
        }

        _logger.LogInformation("PgBouncer.NET остановлен");
    }

    /// <summary>
    /// Принимать входящие соединения
    /// </summary>
    private async Task AcceptConnectionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var clientSocket = await _listenerSocket!.AcceptAsync(cancellationToken);

                _logger.LogInformation("Новое клиентское соединение от {RemoteEndPoint}",
                    clientSocket.RemoteEndPoint);

                // Обрабатываем каждого клиента в отдельной задаче
                _ = Task.Run(async () => await HandleClientAsync(clientSocket, cancellationToken),
                    cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при приёме соединения");
            }
        }
    }

    /// <summary>
    /// Обработать клиентское соединение
    /// </summary>
    private async Task HandleClientAsync(Socket clientSocket, CancellationToken cancellationToken)
    {
        using var session = new ClientSession(
            clientSocket,
            _poolManager,
            _config,
            _logger);

        try
        {
            await session.RunAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка в клиентской сессии");
        }
    }

    public void Dispose()
    {
        _cts?.Dispose();
        _listenerSocket?.Dispose();
    }
}
