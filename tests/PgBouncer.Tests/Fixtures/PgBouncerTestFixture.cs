using Microsoft.Extensions.Logging;
using Npgsql;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Server;
using System.Net;
using System.Net.Sockets;
using Testcontainers.PostgreSql;

namespace PgBouncer.Tests.Fixtures;

/// <summary>
/// Shared fixture that manages PostgreSQL container and PgBouncer instance for integration tests.
/// Ensures proper lifecycle management and resource cleanup.
/// </summary>
public class PgBouncerTestFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _postgres;
    private PoolManager? _poolManager;
    private ProxyServer? _proxyServer;
    private CancellationTokenSource? _cts;
    private int _pgbouncerPort;
    
    public int PgBouncerPort => _pgbouncerPort;
    public string PostgresConnectionString => _postgres?.GetConnectionString() ?? throw new InvalidOperationException("PostgreSQL not initialized");
    public string PgBouncerConnectionString => $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false;Timeout=60";
    
    public async Task InitializeAsync()
    {
        // Start PostgreSQL container
        _postgres = new PostgreSqlBuilder()
            .WithDatabase("testdb")
            .WithUsername("postgres")
            .WithPassword("testpass123")
            .WithName($"pgbouncer-test-{Guid.NewGuid():N}")
            .Build();
            
        await _postgres.StartAsync();
        
        // Find free port for PgBouncer
        _pgbouncerPort = GetFreePort();
        
        // Parse PostgreSQL port from connection string
        var postgresPort = ParsePortFromConnectionString(_postgres.GetConnectionString());
        
        // Create configuration
        var config = new PgBouncerConfig
        {
            ListenPort = _pgbouncerPort,
            Backend = new BackendConfig
            {
                Host = "localhost",
                Port = postgresPort,
                AdminUser = "postgres",
                AdminPassword = "testpass123"
            },
            Pool = new PoolConfig
            {
                Mode = PoolingMode.Transaction,
                MaxSize = 10,
                MinSize = 2,
                DefaultSize = 5,
                ConnectionTimeout = 30
            },
            Auth = new AuthConfig
            {
                Type = "passthrough"
            }
        };
        
        // Create and start PgBouncer
        using var loggerFactory = Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
        _poolManager = new PoolManager(config, loggerFactory.CreateLogger<PoolManager>());
        _proxyServer = new ProxyServer(config, _poolManager, loggerFactory.CreateLogger<ProxyServer>());
        
        _cts = new CancellationTokenSource();
        await _proxyServer.StartAsync(_cts.Token);
        
        // Wait for PgBouncer to be ready
        await Task.Delay(1000);
        
        // Verify connection works
        await using var conn = new NpgsqlConnection(PgBouncerConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        var result = await cmd.ExecuteScalarAsync();
        if (result?.ToString() != "1")
        {
            throw new InvalidOperationException("Failed to verify PgBouncer connection");
        }
    }
    
    public async Task DisposeAsync()
    {
        _cts?.Cancel();
        _proxyServer?.Dispose();
        _poolManager?.Dispose();
        if (_postgres != null)
        {
            await _postgres.DisposeAsync();
        }
    }
    
    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
    
    private static int ParsePortFromConnectionString(string connString)
    {
        var portPart = connString.Split(';').FirstOrDefault(s => s.StartsWith("Port="));
        return portPart != null && int.TryParse(portPart.Split('=')[1], out var port)
            ? port
            : 5432;
    }
}

[CollectionDefinition("PgBouncerIntegration")]
public class PgBouncerCollection : ICollectionFixture<PgBouncerTestFixture> { }
