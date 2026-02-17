using Npgsql;

namespace PgBouncer.StressTester.Scenarios;

/// <summary>
/// Сценарий тестирования параллельных операций
/// </summary>
public class ParallelScenario : BaseScenario
{
    public override string Name => "Parallel Operation Test";
    public override string Description => "Тестирование конкурентных операций: multiple connections, concurrent transactions";

    protected override async Task ExecuteAsync(int iteration, Random random, CancellationToken ct)
    {
        // Выбираем тип параллельной операции
        var opType = random.Next(100);
        
        if (opType < 40)
        {
            // 40% - несколько параллельных транзакций
            await ExecuteConcurrentTransactions(iteration, random, ct);
        }
        else if (opType < 70)
        {
            // 30% - конкурентный доступ к одной таблице
            await ExecuteConcurrentTableAccess(iteration, random, ct);
        }
        else
        {
            // 30% - множественные соединения одновременно
            await ExecuteMultipleConnections(iteration, random, ct);
        }
    }

    /// <summary>
    /// Выполняет несколько параллельных транзакций
    /// </summary>
    private async Task ExecuteConcurrentTransactions(int iteration, Random random, CancellationToken ct)
    {
        var connStr = _connectionString;
        
        // Запускаем несколько транзакций параллельно
        var tasks = new List<Task<bool>>();
        var transactionCount = random.Next(3, 8);
        
        for (int i = 0; i < transactionCount; i++)
        {
            var txIteration = iteration * 100 + i;
            tasks.Add(ExecuteSingleTransactionAsync(connStr, txIteration, ct));
        }
        
        // Ждём выполнения всех транзакций
        var results = await Task.WhenAll(tasks);
        
        // Проверяем что хотя бы часть транзакций выполнилась успешно
        var successCount = results.Count(r => r);
        if (successCount == 0)
            throw new Exception("All transactions failed");
    }

    /// <summary>
    /// Выполняет одну транзакцию
    /// </summary>
    private async Task<bool> ExecuteSingleTransactionAsync(string connStr, int iteration, CancellationToken ct)
    {
        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);
            
            await using var tx = await conn.BeginTransactionAsync(ct);
            
            // INSERT
            await using (var cmd1 = new NpgsqlCommand(
                "INSERT INTO test_transactions (iteration, operation) VALUES (@i, 'parallel')", conn, tx))
            {
                cmd1.Parameters.AddWithValue("i", iteration);
                await cmd1.ExecuteNonQueryAsync(ct);
            }
            
            // SELECT
            await using (var cmd2 = new NpgsqlCommand(
                "SELECT COUNT(*) FROM test_transactions WHERE iteration = @i", conn, tx))
            {
                cmd2.Parameters.AddWithValue("i", iteration);
                await cmd2.ExecuteScalarAsync(ct);
            }
            
            await tx.CommitAsync(ct);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Выполняет конкурентный доступ к одной таблице
    /// </summary>
    private async Task ExecuteConcurrentTableAccess(int iteration, Random random, CancellationToken ct)
    {
        var connStr = _connectionString;
        var tasks = new List<Task<bool>>();
        
        // Запускаем параллельные операции чтения/записи
        var operationCount = random.Next(5, 15);
        
        for (int i = 0; i < operationCount; i++)
        {
            var opIteration = iteration * 100 + i;
                var isRead = random.Next(2) == 1;
            
            if (isRead)
            {
                tasks.Add(ExecuteReadOperationAsync(connStr, opIteration, ct));
            }
            else
            {
                tasks.Add(ExecuteWriteOperationAsync(connStr, opIteration, ct));
            }
        }
        
        var results = await Task.WhenAll(tasks);
        var successCount = results.Count(r => r);
        
        // Допускаем что некоторые операции могут не выполниться из-за конфликтов
        if (successCount == 0)
            throw new Exception("All concurrent operations failed");
    }

    /// <summary>
    /// Выполняет операцию чтения
    /// </summary>
    private async Task<bool> ExecuteReadOperationAsync(string connStr, int iteration, CancellationToken ct)
    {
        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);
            
            await using var cmd = new NpgsqlCommand(
                "SELECT COUNT(*) FROM test_transactions", conn);
            
            var count = await cmd.ExecuteScalarAsync(ct);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Выполняет операцию записи
    /// </summary>
    private async Task<bool> ExecuteWriteOperationAsync(string connStr, int iteration, CancellationToken ct)
    {
        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);
            
            await using var tx = await conn.BeginTransactionAsync(ct);
            
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO test_transactions (iteration, operation) VALUES (@i, 'concurrent')", 
                conn, tx);
            cmd.Parameters.AddWithValue("i", iteration);
            await cmd.ExecuteNonQueryAsync(ct);
            
            await tx.CommitAsync(ct);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Выполняет множественные соединения одновременно
    /// </summary>
    private async Task ExecuteMultipleConnections(int iteration, Random random, CancellationToken ct)
    {
        var connStr = _connectionString;
        var connectionCount = random.Next(5, 20);
        
        // Создаём много соединений одновременно
        var connections = new List<NpgsqlConnection>();
        
        try
        {
            for (int i = 0; i < connectionCount; i++)
            {
                var conn = new NpgsqlConnection(connStr);
                await conn.OpenAsync(ct);
                connections.Add(conn);
            }
            
            // Выполняем запросы на всех соединениях
            var tasks = new List<Task<bool>>();
            
            for (int i = 0; i < connections.Count; i++)
            {
                var conn = connections[i];
                tasks.Add(ExecuteQueryOnConnectionAsync(conn, ct));
            }
            
            var results = await Task.WhenAll(tasks);
            var successCount = results.Count(r => r);
            
            if (successCount < connections.Count * 0.5)
                throw new Exception("More than 50% of connections failed");
        }
        finally
        {
            foreach (var conn in connections)
            {
                try { await conn.DisposeAsync(); } catch { }
            }
        }
    }

    /// <summary>
    /// Выполняет запрос на соединении
    /// </summary>
    private async Task<bool> ExecuteQueryOnConnectionAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            await cmd.ExecuteScalarAsync(ct);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
