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

        // Listener is now handled by Kestrel in Program.cs
        await Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _logger.LogInformation("Остановка PgBouncer.NET...");
        // Cleanup logic if needed
        _logger.LogInformation("PgBouncer.NET остановлен");
    }

    // Methods for PgConnectionHandler to update stats
    public void RegisterSession(SessionInfo session)
    {
        _sessions[session.Id] = session;
        Interlocked.Increment(ref _activeSessions);
        Interlocked.Increment(ref _totalConnections);
    }

    public void UnregisterSession(Guid sessionId)
    {
        _sessions.TryRemove(sessionId, out _);
        Interlocked.Decrement(ref _activeSessions);
    }

    public void Dispose()
    {
        // _cts?.Dispose();
        // _listenerSocket?.Dispose();
    }
}
