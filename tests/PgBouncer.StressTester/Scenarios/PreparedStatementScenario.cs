using Npgsql;

namespace PgBouncer.StressTester.Scenarios;

/// <summary>
/// Сценарий тестирования Prepared Statements (Extended Query Protocol)
/// </summary>
public class PreparedStatementScenario : BaseScenario
{
    private readonly string _tableName;
    
    public override string Name => "Prepared Statement Test";
    public override string Description => "Тестирование Extended Query Protocol: Prepare → Bind → Execute";
    
    public PreparedStatementScenario(string tableName = "test_prepared")
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
            // 40% - подготовленный INSERT с RETURNING
            await ExecutePreparedInsert(conn, iteration, ct);
        }
        else if (opType < 70)
        {
            // 30% - подготовленный SELECT с параметрами
            await ExecutePreparedSelect(conn, iteration, ct);
        }
        else if (opType < 85)
        {
            // 15% - повторное использование подготовленного statement
            await ExecuteReusePreparedStatement(conn, iteration, random, ct);
        }
        else
        {
            // 15% - несколько statement в одной транзакции
            await ExecuteMultiplePreparedStatements(conn, iteration, ct);
        }
    }

    /// <summary>
    /// Выполняет подготовленный INSERT с RETURNING
    /// </summary>
    private async Task ExecutePreparedInsert(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand();
        cmd.Connection = conn;
        cmd.CommandText = $@"
            INSERT INTO {_tableName} (iteration, operation, data) 
            VALUES (@iteration, @operation, @data) 
            RETURNING id";
        
        cmd.Parameters.AddWithValue("iteration", iteration);
        cmd.Parameters.AddWithValue("operation", $"prepared_insert_{iteration}");
        cmd.Parameters.AddWithValue("data", $"Data for iteration {iteration}");
        
        // Prepare заставляет использовать Extended Query Protocol
        await cmd.PrepareAsync(ct);
        
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null)
            throw new Exception("INSERT with RETURNING returned null");
    }

    /// <summary>
    /// Выполняет подготовленный SELECT с параметрами
    /// </summary>
    private async Task ExecutePreparedSelect(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand();
        cmd.Connection = conn;
        cmd.CommandText = $@"
            SELECT id, iteration, operation, data 
            FROM {_tableName} 
            WHERE iteration = @iteration";
        
        cmd.Parameters.AddWithValue("iteration", iteration);
        
        await cmd.PrepareAsync(ct);
        
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            // Читаем данные
            var id = reader.GetInt32(0);
            var iter = reader.GetInt32(1);
        }
    }

    /// <summary>
    /// Повторно использует подготовленный statement (тест кеширования)
    /// </summary>
    private async Task ExecuteReusePreparedStatement(NpgsqlConnection conn, int iteration, Random random, CancellationToken ct)
    {
        // Создаём и подготавливаем statement один раз
        await using var cmd = new NpgsqlCommand();
        cmd.Connection = conn;
        cmd.CommandText = $@"
            SELECT COUNT(*) 
            FROM {_tableName} 
            WHERE iteration BETWEEN @min AND @max";
        
        cmd.Parameters.AddWithValue("min", iteration);
        cmd.Parameters.AddWithValue("max", iteration + 10);
        
        await cmd.PrepareAsync(ct);
        
        // Выполняем несколько раз с разными параметрами
        for (int i = 0; i < 3; i++)
        {
            // Удаляем старые параметры и добавляем новые
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("min", iteration + i);
            cmd.Parameters.AddWithValue("max", iteration + i + 10);
            
            var count = await cmd.ExecuteScalarAsync(ct);
        }
    }

    /// <summary>
    /// Выполняет несколько подготовленных statements в транзакции
    /// </summary>
    private async Task ExecuteMultiplePreparedStatements(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        await using var tx = await conn.BeginTransactionAsync(ct);
        
        try
        {
            // INSERT
            await using (var cmd1 = new NpgsqlCommand(
                $"INSERT INTO {_tableName} (iteration, operation) VALUES (@i, @op)", conn, tx))
            {
                cmd1.Parameters.AddWithValue("i", iteration);
                cmd1.Parameters.AddWithValue("op", $"multi_{iteration}");
                await cmd1.PrepareAsync(ct);
                await cmd1.ExecuteNonQueryAsync(ct);
            }

            // UPDATE
            await using (var cmd2 = new NpgsqlCommand(
                $"UPDATE {_tableName} SET operation = @op WHERE iteration = @i", conn, tx))
            {
                cmd2.Parameters.AddWithValue("i", iteration);
                cmd2.Parameters.AddWithValue("op", $"updated_{iteration}");
                await cmd2.PrepareAsync(ct);
                await cmd2.ExecuteNonQueryAsync(ct);
            }

            // SELECT
            await using (var cmd3 = new NpgsqlCommand(
                $"SELECT COUNT(*) FROM {_tableName} WHERE iteration = @i", conn, tx))
            {
                cmd3.Parameters.AddWithValue("i", iteration);
                await cmd3.PrepareAsync(ct);
                await cmd3.ExecuteScalarAsync(ct);
            }

            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
    }
}
