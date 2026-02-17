using Npgsql;

namespace PgBouncer.StressTester.Scenarios;

/// <summary>
/// Сценарий тестирования транзакций (EF Core Style)
/// Включает: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
/// </summary>
public class TransactionScenario : BaseScenario
{
    private readonly string _tableName;
    
    public override string Name => "Transaction Test";
    public override string Description => "Тестирование транзакций: BEGIN → queries → COMMIT/ROLLBACK/SAVEPOINT";
    
    public TransactionScenario(string tableName = "test_transactions")
    {
        _tableName = tableName;
    }

    protected override async Task ExecuteAsync(int iteration, Random random, CancellationToken ct)
    {
        // Создаем соединение без пулинга (как в EF Core)
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct);
        
        // Выбираем тип транзакции
        var txType = random.Next(100);
        
        if (txType < 60) 
        {
            // 60% - обычная транзакция: BEGIN → COMMIT
            await ExecuteCommitTransaction(conn, iteration, ct);
        }
        else if (txType < 85)
        {
            // 25% - транзакция с откатом: BEGIN → ROLLBACK
            await ExecuteRollbackTransaction(conn, iteration, ct);
        }
        else
        {
            // 15% - транзакция с SAVEPOINT
            await ExecuteSavepointTransaction(conn, iteration, random, ct);
        }
    }

    /// <summary>
    /// Выполняет успешную транзакцию: BEGIN → INSERT/SELECT/UPDATE → COMMIT
    /// </summary>
    private async Task ExecuteCommitTransaction(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        await using var tx = await conn.BeginTransactionAsync(ct);
        
        try
        {
            // INSERT
            await using (var cmd1 = new NpgsqlCommand(
                $"INSERT INTO {_tableName} (iteration, operation) VALUES (@i, 'insert')", conn, tx))
            {
                cmd1.Parameters.AddWithValue("i", iteration);
                await cmd1.ExecuteNonQueryAsync(ct);
            }

            // SELECT (проверка)
            await using (var cmd2 = new NpgsqlCommand(
                $"SELECT COUNT(*) FROM {_tableName} WHERE iteration = @i", conn, tx))
            {
                cmd2.Parameters.AddWithValue("i", iteration);
                await cmd2.ExecuteScalarAsync(ct);
            }

            // UPDATE
            await using (var cmd3 = new NpgsqlCommand(
                $"UPDATE {_tableName} SET operation = 'updated' WHERE iteration = @i", conn, tx))
            {
                cmd3.Parameters.AddWithValue("i", iteration);
                await cmd3.ExecuteNonQueryAsync(ct);
            }

            // COMMIT
            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
    }

    /// <summary>
    /// Выполняет транзакцию с откатом: BEGIN → query → ROLLBACK
    /// </summary>
    private async Task ExecuteRollbackTransaction(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        await using var tx = await conn.BeginTransactionAsync(ct);
        
        try
        {
            // INSERT (будет откачен)
            await using (var cmd1 = new NpgsqlCommand(
                $"INSERT INTO {_tableName} (iteration, operation) VALUES (@i, 'rollback_test')", conn, tx))
            {
                cmd1.Parameters.AddWithValue("i", iteration);
                await cmd1.ExecuteNonQueryAsync(ct);
            }

            // SELECT
            await using (var cmd2 = new NpgsqlCommand(
                $"SELECT COUNT(*) FROM {_tableName} WHERE iteration = @i", conn, tx))
            {
                cmd2.Parameters.AddWithValue("i", iteration);
                await cmd2.ExecuteScalarAsync(ct);
            }

            // ROLLBACK
            await tx.RollbackAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
    }

    /// <summary>
    /// Выполняет транзакцию с SAVEPOINT
    /// </summary>
    private async Task ExecuteSavepointTransaction(NpgsqlConnection conn, int iteration, Random random, CancellationToken ct)
    {
        await using var tx = await conn.BeginTransactionAsync(ct);
        
        try
        {
            // SAVEPOINT
            await using (var savepointCmd = new NpgsqlCommand("SAVEPOINT sp1", conn, tx))
            {
                await savepointCmd.ExecuteNonQueryAsync(ct);
            }

            // INSERT после SAVEPOINT
            await using (var cmd1 = new NpgsqlCommand(
                $"INSERT INTO {_tableName} (iteration, operation) VALUES (@i, 'savepoint')", conn, tx))
            {
                cmd1.Parameters.AddWithValue("i", iteration);
                await cmd1.ExecuteNonQueryAsync(ct);
            }

            // 50% - откат к SAVEPOINT
            if (random.Next(2) == 1)
            {
                await using (var rollbackCmd = new NpgsqlCommand("ROLLBACK TO SAVEPOINT sp1", conn, tx))
                {
                    await rollbackCmd.ExecuteNonQueryAsync(ct);
                }
            }

            // COMMIT
            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
    }
}
