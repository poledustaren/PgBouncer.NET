using Npgsql;

namespace PgBouncer.StressTester.Scenarios;

/// <summary>
/// Сценарий тестирования Batch операций (множественные запросы)
/// </summary>
public class BatchScenario : BaseScenario
{
    private readonly string _tableName;
    
    public override string Name => "Batch Operation Test";
    public override string Description => "Тестирование множественных запросов: bulk insert, multiple queries";
    
    public BatchScenario(string tableName = "test_batch")
    {
        _tableName = tableName;
    }

    protected override async Task ExecuteAsync(int iteration, Random random, CancellationToken ct)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct);
        
        // Выбираем тип операции
        var opType = random.Next(100);
        
        if (opType < 40)
        {
            // 40% - множественные INSERT в цикле (имитация EF Core AddRange)
            await ExecuteBulkInsert(conn, iteration, random, ct);
        }
        else if (opType < 70)
        {
            // 30% - несколько запросов через NpgsqlBatch
            await ExecuteNpgsqlBatch(conn, iteration, ct);
        }
        else
        {
            // 30% - множественные SELECT
            await ExecuteMultipleQueries(conn, iteration, random, ct);
        }
    }

    /// <summary>
    /// Выполняет bulk insert (имитация EF Core AddRange)
    /// </summary>
    private async Task ExecuteBulkInsert(NpgsqlConnection conn, int iteration, Random random, CancellationToken ct)
    {
        var count = random.Next(5, 20); // Количество записей
        
        // В PostgreSQL через Npgsql нет настоящего batch, но можно отправить несколько команд
        // через NpgsqlBatch или через несколько statement через точку с запятой
        await using var tx = await conn.BeginTransactionAsync(ct);
        
        try
        {
            for (int i = 0; i < count; i++)
            {
                await using var cmd = new NpgsqlCommand(
                    $"INSERT INTO {_tableName} (iteration, operation, data) VALUES (@i, @op, @data)", 
                    conn, tx);
                
                cmd.Parameters.AddWithValue("i", iteration * 1000 + i);
                cmd.Parameters.AddWithValue("op", $"batch_{i}");
                cmd.Parameters.AddWithValue("data", $"Batch data {i}");
                
                await cmd.ExecuteNonQueryAsync(ct);
            }
            
            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
    }

    /// <summary>
    /// Выполняет несколько запросов через NpgsqlBatch
    /// </summary>
    private async Task ExecuteNpgsqlBatch(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        await using var batch = new NpgsqlBatch(conn);
        
        // Добавляем несколько команд
        var cmd1 = new NpgsqlBatchCommand(
            $"INSERT INTO {_tableName} (iteration, operation) VALUES (@i1, 'batch1')");
        cmd1.Parameters.AddWithValue("i1", iteration);
        batch.BatchCommands.Add(cmd1);
        
        var cmd2 = new NpgsqlBatchCommand(
            $"INSERT INTO {_tableName} (iteration, operation) VALUES (@i2, 'batch2')");
        cmd2.Parameters.AddWithValue("i2", iteration + 1);
        batch.BatchCommands.Add(cmd2);
        
        var cmd3 = new NpgsqlBatchCommand(
            $"SELECT COUNT(*) FROM {_tableName} WHERE iteration IN (@i1, @i2)");
        cmd3.Parameters.AddWithValue("i1", iteration);
        cmd3.Parameters.AddWithValue("i2", iteration + 1);
        batch.BatchCommands.Add(cmd3);
        
        await using var reader = await batch.ExecuteReaderAsync(ct);
        
        // Читаем результаты
        while (await reader.ReadAsync(ct))
        {
            var count = reader.GetInt64(0);
        }
    }

    /// <summary>
    /// Выполняет множественные SELECT запросы
    /// </summary>
    private async Task ExecuteMultipleQueries(NpgsqlConnection conn, int iteration, Random random, CancellationToken ct)
    {
        var queryCount = random.Next(3, 10);
        
        for (int i = 0; i < queryCount; i++)
        {
            await using var cmd = new NpgsqlCommand(
                $"SELECT {iteration + i}, 'query_{i}', current_timestamp", conn);
            
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var value = reader.GetValue(0);
            }
        }
    }
}
