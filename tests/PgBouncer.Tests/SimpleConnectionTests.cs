using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace PgBouncer.Tests;

public class SimpleConnectionTests
{
    private readonly ITestOutputHelper _output;
    private const int PgBouncerPort = 6432;
    
    public SimpleConnectionTests(ITestOutputHelper output)
    {
        _output = output;
    }
    
    [Fact]
    public async Task Connect_To_PgBouncer_Should_Succeed()
    {
        // Arrange - connect to the manually started PgBouncer
        var connString = $"Host=127.0.0.1;Port={PgBouncerPort};Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";
        
        _output.WriteLine($"Connecting to PgBouncer on port {PgBouncerPort}...");
        
        // Act
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        _output.WriteLine("Connected successfully!");
        
        // Execute a simple query
        await using var cmd = new NpgsqlCommand("SELECT 42 as answer", conn);
        var result = await cmd.ExecuteScalarAsync();
        
        _output.WriteLine($"Query result: {result}");
        
        // Assert
        Assert.Equal(42, Convert.ToInt32(result));
    }
    
    [Fact]
    public async Task Multiple_Queries_Should_Work()
    {
        // Arrange
        var connString = $"Host=127.0.0.1;Port={PgBouncerPort};Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";
        
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        // Act - Execute multiple queries
        for (int i = 1; i <= 5; i++)
        {
            await using var cmd = new NpgsqlCommand($"SELECT {i} as num", conn);
            var result = await cmd.ExecuteScalarAsync();
            
            _output.WriteLine($"Query {i}: {result}");
            Assert.Equal(i, Convert.ToInt32(result));
        }
    }
    
    [Fact]
    public async Task Check_Current_Database()
    {
        // Arrange
        var connString = $"Host=127.0.0.1;Port={PgBouncerPort};Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";
        
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        // Act
        await using var cmd = new NpgsqlCommand("SELECT current_database()", conn);
        var result = await cmd.ExecuteScalarAsync();
        
        _output.WriteLine($"Current database: {result}");
        
        // Assert
        Assert.Equal("postgres", result);
    }
}
