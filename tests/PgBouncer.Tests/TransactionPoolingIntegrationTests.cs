using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Authentication;
using PgBouncer.Server;
using PgBouncer.Server.Handlers;
using PgBouncer.Tests.Fixtures;
using FluentAssertions;
using Xunit;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Connections;

namespace PgBouncer.Tests;

[Collection("PostgreSQL")]
public class TransactionPoolingIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlFixture _postgres;
    private WebApplication? _app;
    private PoolManager? _poolManager;
    private int _pgbouncerPort;
    private CancellationTokenSource? _cts;
    
    public TransactionPoolingIntegrationTests(PostgreSqlFixture postgres)
    {
        _postgres = postgres;
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
        
        await Task.Delay(2000);
        
        await using var conn = new NpgsqlConnection(_postgres.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        var result = await cmd.ExecuteScalarAsync();
        if (result?.ToString() != "1")
        {
            throw new InvalidOperationException("PostgreSQL container is not responding");
        }
        
        await Task.Delay(1000);
    }
    
    [Fact]
    public async Task MultipleClients_ShouldShareBackendConnections()
    {
        const int clientCount = 20;
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        
        var results = new List<int>();
        var tasks = Enumerable.Range(0, clientCount).Select(async i =>
        {
            await using var conn = new NpgsqlConnection(connString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand($"SELECT {i}", conn);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result!);
        });
        
        results = (await Task.WhenAll(tasks)).ToList();
        
        results.Should().HaveCount(clientCount);
        results.Should().BeEquivalentTo(Enumerable.Range(0, clientCount));
        
        var stats = _poolManager!.GetAllStats().ToList();
        stats.Should().NotBeEmpty();
        
        var totalConnections = stats.Sum(s => s.TotalConnections);
        totalConnections.Should().BeLessThanOrEqualTo(5);
    }
    
    [Fact]
    public async Task Transaction_ShouldKeepBackendAssigned()
    {
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        await using var transaction = await conn.BeginTransactionAsync();
        await using var cmd = new NpgsqlCommand("SELECT 1", conn, transaction);
        var result = await cmd.ExecuteScalarAsync();
        result.Should().Be(1);
        
        await using var cmd2 = new NpgsqlCommand("SELECT 2", conn, transaction);
        var result2 = await cmd2.ExecuteScalarAsync();
        result2.Should().Be(2);
        
        await transaction.CommitAsync();
        
        await using var cmd3 = new NpgsqlCommand("SELECT 3", conn);
        var result3 = await cmd3.ExecuteScalarAsync();
        result3.Should().Be(3);
    }
    
    [Fact]
    public async Task SimpleQuery_ThroughProxy_ShouldReturnCorrectResults()
    {
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        for (int i = 0; i < 10; i++)
        {
            await using var cmd = new NpgsqlCommand($"SELECT {i}", conn);
            var result = await cmd.ExecuteScalarAsync();
            result.Should().Be(i);
        }
    }
    
    [Fact]
    public async Task CurrentDatabase_ThroughProxy_ShouldMatchExpected()
    {
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        await using var cmd = new NpgsqlCommand("SELECT current_database()", conn);
        var result = await cmd.ExecuteScalarAsync();
        
        result.Should().Be("testdb");
    }
    
    public async Task DisposeAsync()
    {
        if (_app != null) await _app.StopAsync();
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
        var portPart = connString.Split(';').FirstOrDefault(s => s.Trim().StartsWith("Port=", StringComparison.OrdinalIgnoreCase));
        if (portPart != null)
        {
            var portValue = portPart.Split('=')[1];
            if (int.TryParse(portValue, out var port))
            {
                return port;
            }
        }
        return 5432;
    }
}
