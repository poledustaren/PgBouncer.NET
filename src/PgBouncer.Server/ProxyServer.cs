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
    private readonly SemaphoreSlim _backendConnectionLimit;

    private Socket? _listenerSocket;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;

    // Статистика активных сессий
    private int _activeSessions;
    private int _activeBackendConnections;
    private int _waitingClients;
    private long _totalConnections;
    private long _totalWaitTimeMs;
    private long _maxWaitTimeMs;
    private long _timeoutCount;
    private readonly ConcurrentDictionary<Guid, SessionInfo> _sessions = new();

    /// <summary>Количество активных сессий</summary>
    public int ActiveSessions => _activeSessions;

    /// <summary>Активных backend соединений</summary>
    public int ActiveBackendConnections => _activeBackendConnections;

    /// <summary>Клиентов в очереди ожидания</summary>
    public int WaitingClients => _waitingClients;

    /// <summary>Максимум backend соединений</summary>
    public int MaxBackendConnections => _config.Pool.MaxSize;

    /// <summary>Всего соединений с момента запуска</summary>
    public long TotalConnections => _totalConnections;

    /// <summary>Среднее время ожидания (мс)</summary>
    public long AvgWaitTimeMs => _totalConnections > 0 ? _totalWaitTimeMs / _totalConnections : 0;

    /// <summary>Максимальное время ожидания (мс)</summary>
    public long MaxWaitTimeMs => _maxWaitTimeMs;

    /// <summary>Количество таймаутов</summary>
    public long TimeoutCount => _timeoutCount;

    /// <summary>Информация о текущих сессиях</summary>
    public IReadOnlyDictionary<Guid, SessionInfo> Sessions => _sessions;

    /// <summary>Менеджер пулов для статистики</summary>
    public PoolManager PoolManager => _poolManager;

    /// <summary>Семафор для ограничения backend соединений</summary>
    public SemaphoreSlim BackendConnectionLimit => _backendConnectionLimit;

    /// <summary>Конфигурация</summary>
    public PgBouncerConfig Config => _config;

    /// <summary>Записать время ожидания</summary>
    public void RecordWaitTime(long waitTimeMs)
    {
        Interlocked.Add(ref _totalWaitTimeMs, waitTimeMs);

        // Атомарно обновляем максимум
        long current;
        do
        {
            current = _maxWaitTimeMs;
            if (waitTimeMs <= current) return;
        } while (Interlocked.CompareExchange(ref _maxWaitTimeMs, waitTimeMs, current) != current);
    }

    /// <summary>Записать таймаут</summary>
    public void RecordTimeout()
    {
        Interlocked.Increment(ref _timeoutCount);
    }

    public ProxyServer(
        PgBouncerConfig config,
        PoolManager poolManager,
        ILogger<ProxyServer> logger)
    {
        _config = config;
        _poolManager = poolManager;
        _logger = logger;

        // Ограничиваем количество backend соединений
        _backendConnectionLimit = new SemaphoreSlim(config.Pool.MaxSize, config.Pool.MaxSize);
        _logger.LogInformation("Лимит backend соединений: {MaxSize}", config.Pool.MaxSize);
    }

    /// <summary>
    /// Запустить сервер
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listenerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listenerSocket.Bind(new IPEndPoint(IPAddress.Any, _config.ListenPort));
        _listenerSocket.Listen(500);

        _logger.LogInformation("PgBouncer.NET запущен на порту {Port}", _config.ListenPort);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _acceptTask = AcceptConnectionsAsync(_cts.Token);

        if (_config.Pool.MinSize > 0)
        {
            _logger.LogInformation("Starting connection warming with {MinSize} connections...", _config.Pool.MinSize);
            try
            {
                await _poolManager.WarmupAsync(
                    _config.Backend.DefaultDatabase,
                    _config.Backend.AdminUser,
                    _config.Backend.AdminPassword,
                    _config.Pool.MinSize,
                    cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Connection warming failed, will create connections on demand");
            }
        }

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
    /// Graceful shutdown - ждёт завершения активных сессий
    /// </summary>
    public async Task ShutdownAsync(TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(30);
        _logger.LogInformation("Graceful shutdown initiated. Waiting for {ActiveSessions} active sessions (timeout: {Timeout}s)",
            _activeSessions, timeout.Value.TotalSeconds);

        _cts?.Cancel();
        _listenerSocket?.Close();

        var startTime = DateTime.UtcNow;
        while (_activeSessions > 0 && (DateTime.UtcNow - startTime) < timeout.Value)
        {
            _logger.LogDebug("Waiting for {ActiveSessions} active sessions to complete...", _activeSessions);
            await Task.Delay(100);
        }

        if (_activeSessions > 0)
        {
            _logger.LogWarning("Graceful shutdown timeout. {ActiveSessions} sessions still active, forcing close.", _activeSessions);
        }

        _poolManager.Dispose();

        _logger.LogInformation("PgBouncer.NET shutdown complete");
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

                // Обрабатываем каждого клиента в отдельной задаче
                // Логгирование перенесено внутрь, чтобы не тормозить Accept loop
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

        using var session = new PipelinesClientSession(
            clientSocket,
            _config,
            _poolManager,
            _logger,
            sessionInfo);

        try
        {
            await session.RunAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка в клиентской сессии Pipelines");
            sessionInfo.State = SessionState.Error;
        }
        finally
        {
            sessionInfo.State = SessionState.Completed;
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
