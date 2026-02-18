using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Authentication;
using PgBouncer.Server;
using PgBouncer.Server.Handlers;
using PgBouncer.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Connections;

namespace PgBouncer.Tests;

[Collection("PostgreSQL")]
public class TransactionPoolingDebugTests : IAsyncLifetime
{
    private readonly PostgreSqlFixture _postgres;
    private readonly ITestOutputHelper _output;
    private WebApplication? _app;
    private PoolManager? _poolManager;
    private int _pgbouncerPort;
    
    public TransactionPoolingDebugTests(PostgreSqlFixture postgres, ITestOutputHelper output)
    {
        _postgres = postgres;
        _output = output;
    }
    
    public async Task InitializeAsync()
    {
        _pgbouncerPort = GetFreePort();
        var postgresPort = ParsePortFromConnectionString(_postgres.ConnectionString);
        
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
                MaxSize = 5,
                ConnectionTimeout = 30,
                DefaultSize = 5
            },
            Auth = new AuthConfig
            {
                Type = "trust"
            }
        };
        
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
        builder.Logging.ClearProviders();

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.ListenAnyIP(_pgbouncerPort, l => l.UseConnectionHandler<PgConnectionHandler>());
        });

        _app = builder.Build();
        await _app.StartAsync();

        _poolManager = _app.Services.GetRequiredService<PoolManager>();
        
        await Task.Delay(1000);
        
        try
        {
            var pool = await _poolManager.GetPoolAsync("testdb", "postgres", "testpass123");
            await pool.InitializeAsync(1, default);
            _output.WriteLine($"Pool initialized. Stats: {System.Text.Json.JsonSerializer.Serialize(pool.GetStats())}");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Pool init failed (expected if DB not ready): {ex.Message}");
        }
    }
    
    [Fact]
    public async Task SimpleQuery_ShouldWork()
    {
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false;Timeout=60;Command Timeout=60";
        
        _output.WriteLine("Connecting to PgBouncer...");
        await using var conn = new NpgsqlConnection(connString);
        
        _output.WriteLine("Opening connection...");
        await conn.OpenAsync();
        _output.WriteLine("Connection opened successfully!");
        
        _output.WriteLine("Executing simple query...");
        await using var cmd = new NpgsqlCommand("SELECT 42 as answer", conn);
        var result = await cmd.ExecuteScalarAsync();
        
        _output.WriteLine($"Query result: {result}");
        Assert.Equal(42, Convert.ToInt32(result));
    }
    
    public async Task DisposeAsync()
    {
        if (_app != null) await _app.StopAsync();
    }
    
    private static int GetFreePort()
    {
        using var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
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
