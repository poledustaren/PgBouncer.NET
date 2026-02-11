using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Server;
using PgBouncer.Tests.Fixtures;

namespace PgBouncer.Tests;

[Collection("PostgreSQL")]
public class TransactionPoolingIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlFixture _postgres;
    private ProxyServer? _proxyServer;
    private PoolManager? _poolManager;
    private int _pgbouncerPort;
    private CancellationTokenSource? _cts;
    
    public TransactionPoolingIntegrationTests(PostgreSqlFixture postgres)
    {
        _postgres = postgres;
    }
    
    public async Task InitializeAsync()
    {
        // Find free port for PgBouncer
        _pgbouncerPort = GetFreePort();
        
        // Parse PostgreSQL container port from connection string
        var postgresPort = ParsePortFromConnectionString(_postgres.ConnectionString);
        
        // Create configuration with transaction pooling
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
                MaxSize = 5, // Limit to 5 backend connections
                ConnectionTimeout = 30,
                DefaultSize = 5
            }
        };
        
        // Create pool manager
        var loggerFactory = new NullLoggerFactory();
        _poolManager = new PoolManager(config, loggerFactory.CreateLogger<PoolManager>());
        
        // Create and start proxy server
        _proxyServer = new ProxyServer(
            config, 
            _poolManager, 
            loggerFactory.CreateLogger<ProxyServer>()
        );
        
        _cts = new CancellationTokenSource();
        await _proxyServer.StartAsync(_cts.Token);
        
        // Wait for server to be ready
        await Task.Delay(2000);
        
        Console.WriteLine($"PgBouncer started on port {_pgbouncerPort}, connecting to PostgreSQL on port {postgresPort}");
        
        // Verify PostgreSQL is accessible directly
        await using var conn = new NpgsqlConnection(_postgres.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        var result = await cmd.ExecuteScalarAsync();
        if (result?.ToString() != "1")
        {
            throw new InvalidOperationException("PostgreSQL container is not responding");
        }
    }
    
    [Fact]
    public async Task MultipleClients_ShouldShareBackendConnections()
    {
        // Arrange
        const int clientCount = 20;
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        
        // Act - Open many connections simultaneously
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
        
        // Assert
        results.Should().HaveCount(clientCount);
        results.Should().BeEquivalentTo(Enumerable.Range(0, clientCount));
        
        // Get stats - verify max backends used was limited
        var stats = _poolManager!.GetAllStats().ToList();
        stats.Should().NotBeEmpty();
        
        var totalActive = stats.Sum(s => s.ActiveConnections);
        var totalConnections = stats.Sum(s => s.TotalConnections);
        
        // We should have created at most 5 backend connections (MaxSize)
        totalConnections.Should().BeLessThanOrEqualTo(5);
    }
    
    [Fact]
    public async Task Transaction_ShouldKeepBackendAssigned()
    {
        // Arrange
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        // Act - Start transaction
        await using var transaction = await conn.BeginTransactionAsync();
        await using var cmd = new NpgsqlCommand("SELECT 1", conn, transaction);
        var result = await cmd.ExecuteScalarAsync();
        
        // Assert - Query succeeded during transaction
        result.Should().Be(1);
        
        // Execute another query within the same transaction
        await using var cmd2 = new NpgsqlCommand("SELECT 2", conn, transaction);
        var result2 = await cmd2.ExecuteScalarAsync();
        result2.Should().Be(2);
        
        // Commit transaction
        await transaction.CommitAsync();
        
        // After commit, should be able to execute more queries
        await using var cmd3 = new NpgsqlCommand("SELECT 3", conn);
        var result3 = await cmd3.ExecuteScalarAsync();
        result3.Should().Be(3);
    }
    
    [Fact]
    public async Task SimpleQuery_ThroughProxy_ShouldReturnCorrectResults()
    {
        // Arrange
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        // Act & Assert - Multiple queries
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
        // Arrange
        var connString = $"Host=127.0.0.1;Port={_pgbouncerPort};Database=testdb;Username=postgres;Password=testpass123;Pooling=false";
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        // Act
        await using var cmd = new NpgsqlCommand("SELECT current_database()", conn);
        var result = await cmd.ExecuteScalarAsync();
        
        // Assert
        result.Should().Be("testdb");
    }
    
    public async Task DisposeAsync()
    {
        if (_cts != null)
        {
            await _cts.CancelAsync();
            _cts.Dispose();
        }
        
        if (_proxyServer != null)
        {
            await _proxyServer.StopAsync();
            _proxyServer.Dispose();
        }
        
        _poolManager?.Dispose();
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
        // Parse port from "Host=localhost;Port=5432;..."
        var portPart = connString.Split(';').FirstOrDefault(s => s.Trim().StartsWith("Port=", StringComparison.OrdinalIgnoreCase));
        if (portPart != null)
        {
            var portValue = portPart.Split('=')[1];
            if (int.TryParse(portValue, out var port))
            {
                return port;
            }
        }
        return 5432; // Default PostgreSQL port
    }
}
