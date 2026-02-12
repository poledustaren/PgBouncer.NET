using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Server;
using PgBouncer.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace PgBouncer.Tests;

[Collection("PostgreSQL")]
public class TransactionPoolingDebugTests : IAsyncLifetime
{
    private readonly PostgreSqlFixture _postgres;
    private readonly ITestOutputHelper _output;
    private ProxyServer? _proxyServer;
    private PoolManager? _poolManager;
    private int _pgbouncerPort;
    private CancellationTokenSource? _cts;
    
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
            }
        };
        
        _poolManager = new PoolManager(config, NullLogger<PoolManager>.Instance);
        _proxyServer = new ProxyServer(config, _poolManager, NullLogger<ProxyServer>.Instance);
        
        _cts = new CancellationTokenSource();
        await _proxyServer.StartAsync(_cts.Token);
        await Task.Delay(1000);
        
        // Initialize pool with 1 connection
        var pool = await _poolManager.GetPoolAsync("testdb", "postgres", "testpass123");
        await pool.InitializeAsync(1, _cts.Token);
        _output.WriteLine($"Pool initialized. Stats: {System.Text.Json.JsonSerializer.Serialize(pool.GetStats())}");
    }
    
    [Fact]
    public async Task SimpleQuery_ShouldWork()
    {
        // Test with increased timeout
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
    
    public Task DisposeAsync()
    {
        _proxyServer?.Dispose();
        _cts?.Cancel();
        return Task.CompletedTask;
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
