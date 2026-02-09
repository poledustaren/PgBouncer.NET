using Npgsql;

namespace PgBouncer.Tests;

/// <summary>
/// Integration тесты для подключения через PgBouncer
/// </summary>
public class ConnectionTests : IAsyncLifetime
{
    private const string PgBouncerHost = "localhost";
    private const int PgBouncerPort = 6432;
    private const string DirectHost = "localhost";
    private const int DirectPort = 5432;
    private const string Database = "postgres";
    private const string Username = "postgres";
    private const string Password = "123";

    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task DirectConnection_ShouldWork()
    {
        // Arrange
        var connString = $"Host={DirectHost};Port={DirectPort};Database={Database};Username={Username};Password={Password}";
        
        // Act & Assert
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        conn.State.Should().Be(System.Data.ConnectionState.Open);
        
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        var result = await cmd.ExecuteScalarAsync();
        result.Should().Be(1);
    }

    [Fact]
    public async Task PgBouncerConnection_ShouldWork()
    {
        // Arrange
        var connString = $"Host={PgBouncerHost};Port={PgBouncerPort};Database={Database};Username={Username};Password={Password}";
        
        // Act & Assert
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        conn.State.Should().Be(System.Data.ConnectionState.Open);
        
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        var result = await cmd.ExecuteScalarAsync();
        result.Should().Be(1);
    }

    [Fact]
    public async Task PgBouncer_ShouldForwardToCorrectDatabase()
    {
        // Arrange
        var connString = $"Host={PgBouncerHost};Port={PgBouncerPort};Database={Database};Username={Username};Password={Password}";
        
        // Act
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        await using var cmd = new NpgsqlCommand("SELECT current_database()", conn);
        var result = await cmd.ExecuteScalarAsync();
        
        // Assert
        result.Should().Be(Database);
    }

    [Fact]
    public async Task PgBouncer_ShouldHandleMultipleQueries()
    {
        // Arrange
        var connString = $"Host={PgBouncerHost};Port={PgBouncerPort};Database={Database};Username={Username};Password={Password}";
        
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        // Act - выполняем несколько запросов подряд
        for (int i = 0; i < 10; i++)
        {
            await using var cmd = new NpgsqlCommand($"SELECT {i}", conn);
            var result = await cmd.ExecuteScalarAsync();
            
            // Assert
            result.Should().Be(i);
        }
    }

    [Fact]
    public async Task PgBouncer_ShouldHandleTransaction()
    {
        // Arrange
        var connString = $"Host={PgBouncerHost};Port={PgBouncerPort};Database={Database};Username={Username};Password={Password}";
        
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        
        // Act
        await using var transaction = await conn.BeginTransactionAsync();
        
        await using var cmd1 = new NpgsqlCommand("SELECT 1", conn, transaction);
        var result1 = await cmd1.ExecuteScalarAsync();
        
        await using var cmd2 = new NpgsqlCommand("SELECT 2", conn, transaction);
        var result2 = await cmd2.ExecuteScalarAsync();
        
        await transaction.CommitAsync();
        
        // Assert
        result1.Should().Be(1);
        result2.Should().Be(2);
    }
}
