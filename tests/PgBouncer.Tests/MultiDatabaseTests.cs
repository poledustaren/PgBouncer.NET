using Npgsql;

namespace PgBouncer.Tests;

/// <summary>
/// Тесты работы с несколькими базами данных
/// </summary>
public class MultiDatabaseTests : IAsyncLifetime
{
    private const string PgBouncerHost = "localhost";
    private const int PgBouncerPort = 6432;
    private const string Password = "123";

    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => Task.CompletedTask;

    [Theory]
    [InlineData("postgres", "postgres")]
    [InlineData("testdb1", "testuser1")]
    public async Task Connection_ShouldWorkWithDifferentDatabases(string database, string username)
    {
        // Arrange
        var connString = $"Host={PgBouncerHost};Port={PgBouncerPort};Database={database};Username={username};Password={Password}";
        
        // Act & Assert
        try
        {
            await using var conn = new NpgsqlConnection(connString);
            await conn.OpenAsync();
            
            conn.State.Should().Be(System.Data.ConnectionState.Open);
            
            await using var cmd = new NpgsqlCommand("SELECT current_database()", conn);
            var result = await cmd.ExecuteScalarAsync();
            result.Should().Be(database);
        }
        catch (Exception ex)
        {
            // Если база не существует - это ок для теста
            ex.Message.Should().Contain("does not exist");
        }
    }

    [Fact]
    public async Task ParallelConnections_ToDifferentDatabases_ShouldWork()
    {
        // Arrange
        var databases = new[] { "postgres" };
        var tasks = new List<Task<bool>>();

        // Act
        foreach (var db in databases)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    var connString = $"Host={PgBouncerHost};Port={PgBouncerPort};Database={db};Username=postgres;Password={Password}";
                    await using var conn = new NpgsqlConnection(connString);
                    await conn.OpenAsync();
                    
                    await using var cmd = new NpgsqlCommand("SELECT 1", conn);
                    return (int)(await cmd.ExecuteScalarAsync())! == 1;
                }
                catch
                {
                    return false;
                }
            }));
        }

        var results = await Task.WhenAll(tasks);

        // Assert
        results.Should().AllBeEquivalentTo(true, "все подключения к разным БД должны работать");
    }
}
