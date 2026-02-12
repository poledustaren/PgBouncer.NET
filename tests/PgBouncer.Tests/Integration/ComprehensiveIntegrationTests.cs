using Npgsql;
using PgBouncer.Tests.Fixtures;
using System.Diagnostics;
using Xunit;
using Xunit.Abstractions;

namespace PgBouncer.Tests.Integration;

[Collection("PgBouncerIntegration")]
public class ComprehensiveIntegrationTests
{
    private readonly PgBouncerTestFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ComprehensiveIntegrationTests(PgBouncerTestFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Test_01_BasicConnection_ShouldSucceed()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();
        Assert.True(conn.State == System.Data.ConnectionState.Open);
    }

    [Fact]
    public async Task Test_02_SimpleQuery_ShouldReturnCorrectResult()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT 42", conn);
        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(42, Convert.ToInt32(result));
    }

    [Fact]
    public async Task Test_03_MultipleQueries_ShouldAllSucceed()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();

        for (int i = 1; i <= 10; i++)
        {
            await using var cmd = new NpgsqlCommand($"SELECT {i}", conn);
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(i, Convert.ToInt32(result));
        }
    }

    [Fact]
    public async Task Test_04_Transaction_ShouldWork()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();
        await using var transaction = await conn.BeginTransactionAsync();

        await using var cmd = new NpgsqlCommand("SELECT 1", conn, transaction);
        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(1, Convert.ToInt32(result));

        await transaction.CommitAsync();
    }

    [Fact]
    public async Task Test_05_RollbackTransaction_ShouldWork()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();
        await using var transaction = await conn.BeginTransactionAsync();

        await using var cmd = new NpgsqlCommand("SELECT 1", conn, transaction);
        await cmd.ExecuteScalarAsync();

        await transaction.RollbackAsync();
        // No exception means success
    }

    [Fact]
    public async Task Test_06_PreparedStatement_ShouldWork()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand("SELECT $1 + $2", conn);
        cmd.Parameters.AddWithValue("a", 10);
        cmd.Parameters.AddWithValue("b", 20);

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(30, Convert.ToInt32(result));
    }

    [Fact]
    public async Task Test_07_CreateTableAndInsert_ShouldWork()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();

        // Create table
        await using var createCmd = new NpgsqlCommand(
            "CREATE TEMP TABLE test_table (id INT PRIMARY KEY, name TEXT)", conn);
        await createCmd.ExecuteNonQueryAsync();

        // Insert data
        await using var insertCmd = new NpgsqlCommand(
            "INSERT INTO test_table (id, name) VALUES (1, 'test')", conn);
        var rowsAffected = await insertCmd.ExecuteNonQueryAsync();
        Assert.Equal(1, rowsAffected);

        // Select data
        await using var selectCmd = new NpgsqlCommand(
            "SELECT name FROM test_table WHERE id = 1", conn);
        var result = await selectCmd.ExecuteScalarAsync();
        Assert.Equal("test", result);
    }

    [Fact]
    public async Task Test_08_ConcurrentConnections_ShouldWork()
    {
        const int concurrentCount = 20;
        var tasks = Enumerable.Range(0, concurrentCount).Select(async i =>
        {
            await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand($"SELECT {i}", conn);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        });

        var results = await Task.WhenAll(tasks);
        Assert.Equal(concurrentCount, results.Length);
        Assert.Equal(Enumerable.Range(0, concurrentCount).Sum(), results.Sum());
    }

    [Fact]
    public async Task Test_09_LargeResultSet_ShouldWork()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            "SELECT generate_series(1, 1000) as num", conn);

        var count = 0;
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            count++;
        }

        Assert.Equal(1000, count);
    }

    [Fact]
    public async Task Test_10_ConnectionPoolReuse_ShouldBeEfficient()
    {
        const int totalQueries = 50;
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < totalQueries; i++)
        {
            await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            await cmd.ExecuteScalarAsync();
        }

        sw.Stop();
        var avgTime = sw.ElapsedMilliseconds / (double)totalQueries;

        _output.WriteLine($"Average connection time: {avgTime:F2}ms");
        Assert.True(avgTime < 100, $"Average connection time {avgTime:F2}ms is too high");
    }

    [Fact]
    public async Task Test_11_ErrorHandling_InvalidQuery_ShouldThrow()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand("SELECT * FROM non_existent_table", conn);
        await Assert.ThrowsAsync<PostgresException>(async () => await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Test_12_ExtendedQueryProtocol_ShouldWork()
    {
        await using var conn = new NpgsqlConnection(_fixture.PgBouncerConnectionString);
        await conn.OpenAsync();

        // Test extended query protocol with parameters
        await using var cmd = new NpgsqlCommand();
        cmd.Connection = conn;
        cmd.CommandText = "SELECT CASE WHEN $1 > $2 THEN 'greater' ELSE 'less' END";
        cmd.Parameters.AddWithValue("p1", 10);
        cmd.Parameters.AddWithValue("p2", 5);

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal("greater", result);
    }
}
