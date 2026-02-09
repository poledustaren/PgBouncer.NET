using System.Diagnostics;
using Npgsql;

namespace PgBouncer.Tests;

/// <summary>
/// Нагрузочные тесты PgBouncer
/// </summary>
public class LoadTests : IAsyncLifetime
{
    private const string PgBouncerHost = "localhost";
    private const int PgBouncerPort = 6432;
    private const string Database = "postgres";
    private const string Username = "postgres";
    private const string Password = "123";
    
    private string ConnectionString => 
        $"Host={PgBouncerHost};Port={PgBouncerPort};Database={Database};Username={Username};Password={Password};Pooling=false;Timeout=30";

    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => Task.CompletedTask;

    /// <summary>
    /// Тест максимального количества одновременных соединений
    /// </summary>
    [Theory]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(100)]
    public async Task MaxConnections_ShouldHandleConcurrentConnections(int connectionCount)
    {
        // Arrange
        var connections = new List<NpgsqlConnection>();
        var successCount = 0;
        var failCount = 0;

        try
        {
            // Act - открываем много соединений одновременно
            var tasks = Enumerable.Range(0, connectionCount).Select(async i =>
            {
                try
                {
                    var conn = new NpgsqlConnection(ConnectionString);
                    await conn.OpenAsync();
                    connections.Add(conn);
                    
                    // Выполняем простой запрос
                    await using var cmd = new NpgsqlCommand("SELECT 1", conn);
                    await cmd.ExecuteScalarAsync();
                    
                    Interlocked.Increment(ref successCount);
                }
                catch
                {
                    Interlocked.Increment(ref failCount);
                }
            });

            await Task.WhenAll(tasks);

            // Assert
            successCount.Should().BeGreaterThan(0, "хотя бы часть соединений должна открыться");
        }
        finally
        {
            // Cleanup
            foreach (var conn in connections)
            {
                await conn.CloseAsync();
                await conn.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Тест быстрых подключений/отключений
    /// </summary>
    [Theory]
    [InlineData(100)]
    [InlineData(500)]
    public async Task RapidConnectionTest_ShouldHandleQuickConnectDisconnect(int iterations)
    {
        // Arrange
        var successCount = 0;
        var sw = Stopwatch.StartNew();

        // Act
        for (int i = 0; i < iterations; i++)
        {
            try
            {
                await using var conn = new NpgsqlConnection(ConnectionString);
                await conn.OpenAsync();
                await using var cmd = new NpgsqlCommand("SELECT 1", conn);
                await cmd.ExecuteScalarAsync();
                successCount++;
            }
            catch
            {
                // Ignore failures
            }
        }

        sw.Stop();

        // Assert
        successCount.Should().Be(iterations, "все быстрые подключения должны успеть");
        
        var avgMs = sw.ElapsedMilliseconds / (double)iterations;
        avgMs.Should().BeLessThan(500, "среднее время подключения не должно превышать 500ms");
    }

    /// <summary>
    /// Тест производительности QPS (queries per second)
    /// </summary>
    [Theory]
    [InlineData(1000)]
    [InlineData(5000)]
    public async Task QPS_ShouldAchieveTargetRate(int queryCount)
    {
        // Arrange
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();
        
        var sw = Stopwatch.StartNew();

        // Act
        for (int i = 0; i < queryCount; i++)
        {
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            await cmd.ExecuteScalarAsync();
        }

        sw.Stop();

        // Assert
        var qps = queryCount / (sw.ElapsedMilliseconds / 1000.0);
        qps.Should().BeGreaterThan(100, $"QPS должен быть больше 100 (фактически: {qps:F0})");
    }

    /// <summary>
    /// Стресс-тест с параллельными запросами
    /// </summary>
    [Theory]
    [InlineData(10, 100)]  // 10 соединений, 100 запросов каждое
    [InlineData(20, 50)]   // 20 соединений, 50 запросов каждое
    public async Task StressTest_ParallelQueriesOnMultipleConnections(int connectionCount, int queriesPerConnection)
    {
        // Arrange
        var totalQueries = 0;
        var errors = 0;
        var sw = Stopwatch.StartNew();

        // Act
        var tasks = Enumerable.Range(0, connectionCount).Select(async connId =>
        {
            try
            {
                await using var conn = new NpgsqlConnection(ConnectionString);
                await conn.OpenAsync();

                for (int i = 0; i < queriesPerConnection; i++)
                {
                    await using var cmd = new NpgsqlCommand($"SELECT {i}", conn);
                    var result = await cmd.ExecuteScalarAsync();
                    
                    if (result?.ToString() == i.ToString())
                        Interlocked.Increment(ref totalQueries);
                    else
                        Interlocked.Increment(ref errors);
                }
            }
            catch
            {
                Interlocked.Increment(ref errors);
            }
        });

        await Task.WhenAll(tasks);
        sw.Stop();

        // Assert
        var expectedTotal = connectionCount * queriesPerConnection;
        totalQueries.Should().Be(expectedTotal, "все запросы должны выполниться успешно");
        errors.Should().Be(0, "ошибок быть не должно");
        
        var qps = totalQueries / (sw.ElapsedMilliseconds / 1000.0);
        // Логируем результат
    }

    /// <summary>
    /// Тест длительной нагрузки (упрощённая версия)
    /// </summary>
    [Fact]
    public async Task LongRunning_ShouldStayStableFor30Seconds()
    {
        // Arrange
        var duration = TimeSpan.FromSeconds(30);
        var successCount = 0;
        var errorCount = 0;
        var sw = Stopwatch.StartNew();

        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();

        // Act
        while (sw.Elapsed < duration)
        {
            try
            {
                await using var cmd = new NpgsqlCommand("SELECT 1", conn);
                await cmd.ExecuteScalarAsync();
                successCount++;
            }
            catch
            {
                errorCount++;
            }
            
            // Небольшая пауза чтобы не забивать CPU
            await Task.Delay(10);
        }

        sw.Stop();

        // Assert
        errorCount.Should().Be(0, "за 30 секунд не должно быть ошибок");
        successCount.Should().BeGreaterThan(100, "должно выполниться много запросов");
    }
}
