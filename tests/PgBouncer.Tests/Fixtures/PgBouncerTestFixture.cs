using Microsoft.Extensions.Logging;
using Npgsql;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Authentication;
using PgBouncer.Server;
using PgBouncer.Server.Handlers;
using System.Net;
using System.Net.Sockets;
using Testcontainers.PostgreSql;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Connections;

namespace PgBouncer.Tests.Fixtures;

/// <summary>
/// Shared fixture that manages PostgreSQL container and PgBouncer instance for integration tests.
/// Ensures proper lifecycle management and resource cleanup.
/// </summary>
public class PgBouncerTestFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _postgres;
    private WebApplication? _app;
    private PoolManager? _poolManager;
    private ProxyServer? _proxyServer;
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
                Type = "trust"
            }
        };
        
        // Create Host
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddSingleton(config);
        builder.Services.AddSingleton<UserRegistry>();
        builder.Services.AddSingleton(sp =>
        {
             var cfg = sp.GetRequiredService<PgBouncerConfig>();
             var logger = sp.GetRequiredService<ILogger<PoolManager>>();
             return new PoolManager(cfg, logger);
        });
        builder.Services.AddSingleton<ProxyServer>();
        builder.Services.AddTransient<PgConnectionHandler>();

        builder.Logging.ClearProviders(); // Reduce noise

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.ListenAnyIP(_pgbouncerPort, l => l.UseConnectionHandler<PgConnectionHandler>());
        });

        _app = builder.Build();
        await _app.StartAsync();

        _poolManager = _app.Services.GetRequiredService<PoolManager>();
        _proxyServer = _app.Services.GetRequiredService<ProxyServer>();
        
        // Warmup manually
        try
        {
            await _poolManager.WarmupAsync("testdb", "postgres", "testpass123", 2);
        }
        catch { /* ignore warmup error */ }
        
        // Wait for PgBouncer to be ready
        await Task.Delay(1000);
        
        // Verify connection works
        try
        {
            await using var conn = new NpgsqlConnection(PgBouncerConnectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            var result = await cmd.ExecuteScalarAsync();
            if (result?.ToString() != "1")
            {
                throw new InvalidOperationException("Failed to verify PgBouncer connection");
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to verify PgBouncer connection: {ex.Message}", ex);
        }
    }
    
    public async Task DisposeAsync()
    {
        if (_app != null) await _app.StopAsync();
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
