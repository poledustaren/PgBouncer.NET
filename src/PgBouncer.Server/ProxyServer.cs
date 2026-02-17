using System.Net;
using System.Net.Sockets;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Authentication;

namespace PgBouncer.Server;

public class ProxyServer : IDisposable
{
    private readonly PgBouncerConfig _config;
    private readonly PoolManager _poolManager;
    private readonly UserRegistry _userRegistry;
    private readonly ILogger<ProxyServer> _logger;
    private readonly SemaphoreSlim _backendConnectionLimit;

    private Socket? _listenerSocket;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;

    private int _activeSessions;
    private int _activeBackendConnections;
    private int _waitingClients;
    private long _totalConnections;
    private long _totalWaitTimeMs;
    private long _maxWaitTimeMs;
    private long _timeoutCount;
    private readonly ConcurrentDictionary<Guid, SessionInfo> _sessions = new();

    public int ActiveSessions => _activeSessions;
    public int ActiveBackendConnections => _activeBackendConnections;
    public int WaitingClients => _waitingClients;
    public int MaxBackendConnections => _config.Pool.MaxSize;
    public long TotalConnections => _totalConnections;
    public long AvgWaitTimeMs => _totalConnections > 0 ? _totalWaitTimeMs / _totalConnections : 0;
    public long MaxWaitTimeMs => _maxWaitTimeMs;
    public long TimeoutCount => _timeoutCount;
    public IReadOnlyDictionary<Guid, SessionInfo> Sessions => _sessions;
    public PoolManager PoolManager => _poolManager;
    public SemaphoreSlim BackendConnectionLimit => _backendConnectionLimit;
    public PgBouncerConfig Config => _config;

    public void RecordWaitTime(long waitTimeMs)
    {
        Interlocked.Add(ref _totalWaitTimeMs, waitTimeMs);
        long current;
        do
        {
            current = _maxWaitTimeMs;
            if (waitTimeMs <= current) return;
        } while (Interlocked.CompareExchange(ref _maxWaitTimeMs, waitTimeMs, current) != current);
    }

    public void RecordTimeout()
    {
        Interlocked.Increment(ref _timeoutCount);
    }

    public ProxyServer(
        PgBouncerConfig config,
        PoolManager poolManager,
        UserRegistry userRegistry,
        ILogger<ProxyServer> logger)
    {
        _config = config;
        _poolManager = poolManager;
        _userRegistry = userRegistry;
        _logger = logger;
        _backendConnectionLimit = new SemaphoreSlim(config.Pool.MaxSize, config.Pool.MaxSize);
        _logger.LogInformation("Лимит backend соединений: {MaxSize}", config.Pool.MaxSize);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _userRegistry.Load(_config.Auth.AuthFile);

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
                    "postgres",
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

    private async Task AcceptConnectionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var clientSocket = await _listenerSocket!.AcceptAsync(cancellationToken);
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
            _userRegistry,
            _logger,
            sessionInfo);

        try
        {
            await session.RunAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка в клиентской сессии");
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
