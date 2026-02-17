using Npgsql;

namespace PgBouncer.StressTester.Scenarios;

/// <summary>
/// Сценарий тестирования обработки ошибок
/// </summary>
public class ErrorHandlingScenario : BaseScenario
{
    public override string Name => "Error Handling Test";
    public override string Description => "Тестирование обработки ошибок: timeout, syntax errors, connection loss";

    protected override async Task ExecuteAsync(int iteration, Random random, CancellationToken ct)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct);
        
        // Выбираем тип ошибки для тестирования
        var errorType = random.Next(100);
        
        if (errorType < 30)
        {
            // 30% - тест синтаксической ошибки (должен вернуть ошибку)
            await ExecuteSyntaxErrorTest(conn, iteration, ct);
        }
        else if (errorType < 50)
        {
            // 20% - тест с нарушением constraints
            await ExecuteConstraintErrorTest(conn, iteration, ct);
        }
        else if (errorType < 70)
        {
            // 20% - тест таймаута запроса
            await ExecuteTimeoutTest(conn, ct);
        }
        else if (errorType < 85)
        {
            // 15% - тест отката при ошибке в транзакции
            await ExecuteTransactionRollbackTest(conn, iteration, ct);
        }
        else
        {
            // 15% - тест повторного подключения после ошибки
            await ExecuteReconnectTest(iteration, random, ct);
        }
    }

    /// <summary>
    /// Тестирует обработку синтаксической ошибки
    /// </summary>
    private async Task ExecuteSyntaxErrorTest(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        // intentionally: синтаксическая ошибка
        await using var cmd = new NpgsqlCommand(
            "SELEC * FROM nonexistent_table", conn); // Намеренная ошибка
        
        try
        {
            await cmd.ExecuteScalarAsync(ct);
            // Если нет ошибки - это проблема
            throw new Exception("Expected syntax error but query succeeded");
        }
        catch (NpgsqlException ex) when (ex.Message.Contains("syntax"))
        {
            // Ожидаемая ошибка - это нормально
            // Проверяем что соединение всё ещё работает
            await using var verifyCmd = new NpgsqlCommand("SELECT 1", conn);
            await verifyCmd.ExecuteScalarAsync(ct);
        }
    }

    /// <summary>
    /// Тестирует обработку ошибки constraint
    /// </summary>
    private async Task ExecuteConstraintErrorTest(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        // Создаём временную таблицу с ограничением
        await using (var setupCmd = new NpgsqlCommand(
            "CREATE TEMP TABLE IF NOT EXISTS test_constraint (id INT PRIMARY KEY)", conn))
        {
            await setupCmd.ExecuteNonQueryAsync(ct);
        }
        
        // Вставляем начальное значение
        await using (var insertCmd = new NpgsqlCommand(
            "INSERT INTO test_constraint VALUES (@id)", conn))
        {
            insertCmd.Parameters.AddWithValue("id", iteration);
            await insertCmd.ExecuteNonQueryAsync(ct);
        }
        
        // Пытаемся вставить дубликат - ожидаем ошибку
        await using var dupCmd = new NpgsqlCommand(
            "INSERT INTO test_constraint VALUES (@id)", conn);
        dupCmd.Parameters.AddWithValue("id", iteration);
        
        try
        {
            await dupCmd.ExecuteNonQueryAsync(ct);
            throw new Exception("Expected constraint violation but query succeeded");
        }
        catch (NpgsqlException ex) when (ex.Message.Contains("duplicate") || ex.SqlState == "23505")
        {
            // Ожидаемая ошибка - проверяем что соединение работает
            await using var verifyCmd = new NpgsqlCommand("SELECT 1", conn);
            await verifyCmd.ExecuteScalarAsync(ct);
        }
    }

    /// <summary>
    /// Тестирует обработку таймаута запроса
    /// </summary>
    private async Task ExecuteTimeoutTest(NpgsqlConnection conn, CancellationToken ct)
    {
        // Создаём запрос с очень коротким таймаутом
        await using var cmd = new NpgsqlCommand("SELECT pg_sleep(30)", conn);
        cmd.CommandTimeout = 1; // 1 секунда таймаут
        
        try
        {
            await cmd.ExecuteScalarAsync(ct);
            throw new Exception("Expected timeout but query succeeded");
        }
        catch (NpgsqlException ex) when (ex.Message.Contains("timeout") || ex.Message.Contains("timed out"))
        {
            // Ожидаемый таймаут - проверяем что соединение закрыто
            // После таймаута соединение должно быть закрыто сервером
            // Проверяем состояние
            if (conn.State == System.Data.ConnectionState.Open)
            {
                // Соединение может быть всё ещё открыто, попробуем простой запрос
                try
                {
                    await using var verifyCmd = new NpgsqlCommand("SELECT 1", conn);
                    await verifyCmd.ExecuteScalarAsync(ct);
                }
                catch
                {
                    // Соединение мёртвое - это нормально для таймаута
                }
            }
        }
    }

    /// <summary>
    /// Тестирует автоматический откат транзакции при ошибке
    /// </summary>
    private async Task ExecuteTransactionRollbackTest(NpgsqlConnection conn, int iteration, CancellationToken ct)
    {
        await using var tx = await conn.BeginTransactionAsync(ct);
        
        try
        {
            // Вставляем данные
            await using (var cmd1 = new NpgsqlCommand(
                "INSERT INTO test_transactions (iteration, operation) VALUES (@i, 'error_test')", conn, tx))
            {
                cmd1.Parameters.AddWithValue("i", iteration);
                await cmd1.ExecuteNonQueryAsync(ct);
            }
            
            // Намеренная ошибка - запрос к несуществующей таблице
            await using var badCmd = new NpgsqlCommand(
                "SELECT * FROM nonexistent_table_12345", conn, tx);
            await badCmd.ExecuteScalarAsync(ct);
            
            // Если дошли сюда - ошибки не было
            await tx.RollbackAsync(ct);
            throw new Exception("Expected query error but transaction succeeded");
        }
        catch (NpgsqlException)
        {
            // Ожидаемая ошибка - транзакция должна быть уже откатана
            // Проверяем что можем начать новую транзакцию
            await using var newTx = await conn.BeginTransactionAsync(ct);
            await using var verifyCmd = new NpgsqlCommand("SELECT 1", conn, newTx);
            await verifyCmd.ExecuteScalarAsync(ct);
            await newTx.CommitAsync(ct);
        }
    }

    /// <summary>
    /// Тестирует повторное подключение после ошибки
    /// </summary>
    private async Task ExecuteReconnectTest(int iteration, Random random, CancellationToken ct)
    {
        var host = "localhost";
        var port = 6432;
        var database = "pm_analytics";
        var username = "analytics";
        var password = "analytics_password";
        
        var connStr = $"Host={host};Port={port};Database={database};Username={username};Password={password};Pooling=false;Timeout=5";
        
        try
        {
            // Первая попытка - может быть ошибка
            await using var conn1 = new NpgsqlConnection(connStr);
            await conn1.OpenAsync(ct);
            
            // Намеренная ошибка
            await using var badCmd = new NpgsqlCommand("SELEC * FROM error", conn1);
            try { await badCmd.ExecuteScalarAsync(ct); } catch { }
        }
        catch
        {
            // Игнорируем ошибку первого подключения
        }
        
        // Пытаемся подключиться снова
        await using var conn2 = new NpgsqlConnection(connStr);
        await conn2.OpenAsync(ct);
        
        // Проверяем что новое соединение работает
        await using var verifyCmd = new NpgsqlCommand("SELECT 1", conn2);
        await verifyCmd.ExecuteScalarAsync(ct);
    }
}
