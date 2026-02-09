using System.Net;
using System.Net.Sockets;
using System.Collections.Concurrent;
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

    // Статистика активных сессий
    private int _activeSessions;
    private long _totalConnections;
    private readonly ConcurrentDictionary<Guid, SessionInfo> _sessions = new();

    /// <summary>Количество активных сессий</summary>
    public int ActiveSessions => _activeSessions;

    /// <summary>Всего соединений с момента запуска</summary>
    public long TotalConnections => _totalConnections;

    /// <summary>Информация о текущих сессиях</summary>
    public IReadOnlyDictionary<Guid, SessionInfo> Sessions => _sessions;

    /// <summary>Менеджер пулов для статистики</summary>
    public PoolManager PoolManager => _poolManager;

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
        var sessionId = Guid.NewGuid();
        var sessionInfo = new SessionInfo
        {
            Id = sessionId,
            StartedAt = DateTime.UtcNow,
            RemoteEndPoint = clientSocket.RemoteEndPoint?.ToString() ?? "unknown"
        };

        _sessions[sessionId] = sessionInfo;
        Interlocked.Increment(ref _activeSessions);
        Interlocked.Increment(ref _totalConnections);

        using var session = new ClientSession(
            clientSocket,
            _config,
            _poolManager,
            _logger);

        try
        {
            await session.RunAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка в клиентской сессии");
        }
        finally
        {
            _sessions.TryRemove(sessionId, out _);
            Interlocked.Decrement(ref _activeSessions);
        }
    }

    public void Dispose()
    {
        _cts?.Dispose();
        _listenerSocket?.Dispose();
    }
}
