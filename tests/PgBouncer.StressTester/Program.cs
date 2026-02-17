using System.Collections.Concurrent;
using Npgsql;
using System.Linq;

namespace PgBouncer.StressTester;

/// <summary>
/// Стресс-тестер для PgBouncer.NET - 10 виртуальных проектов на 10 баз
/// </summary>
class Program
{
    // Конфигурация - ИЗМЕНИ ПОД СЕБЯ
    static readonly string[] Databases =
    {
        "db_ecommerce",      // E-Commerce система
        "db_analytics",      // Аналитика
        "db_users",          // Пользователи
        "db_orders",         // Заказы
        "db_inventory",      // Склад
        "db_payments",       // Платежи
        "db_notifications",  // Уведомления
        "db_logs",           // Логи
        "db_reports",        // Отчёты
        "db_sessions"        // Сессии
    };

    // Если баз нет - используй одну с разными схемами
    static bool UseSingleDatabase = true;
    static string SingleDatabaseName = "postgres";

    static readonly int ProxyPort = 6432;
    static readonly string ProxyHost = "localhost";
    static readonly string Username = "postgres";
    static readonly string Password = "123";

    // Настройки проектов
    static readonly VirtualProject[] Projects =
    {
        new("E-Commerce API", 0, 50, 100, 300),         // 50-100 соединений, запрос 100-300ms
        new("Analytics Service", 1, 20, 40, 2000),      // 20-40 соединений, долгие запросы
        new("User Service", 2, 100, 200, 50),           // 100-200 соединений, быстрые запросы
        new("Order Processor", 3, 30, 60, 150),         // 30-60 соединений
        new("Inventory Sync", 4, 10, 20, 500),          // 10-20 соединений
        new("Payment Gateway", 5, 40, 80, 200),         // 40-80 соединений
        new("Notification Worker", 6, 15, 30, 100),     // 15-30 соединений
        new("Log Collector", 7, 5, 15, 1000),           // 5-15 соединений, очень долгие
        new("Report Generator", 8, 25, 50, 3000),       // 25-50 соединений, самые долгие
        new("Session Manager", 9, 80, 150, 30),         // 80-150 соединений, самые быстрые
        new("EF Core Transaction Test", 0, 10, 20, 100) // 10-20 соединений, тест транзакций
    };
    
    // Включить тестирование транзакций в общем стрессе
    static bool EnableTransactionTesting = true;

    // Статистика
    static readonly ConcurrentDictionary<string, ProjectStats> Stats = new();
    static long TotalOperations = 0;
    static long TotalErrors = 0;
    static DateTime StartTime;

    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        if (!Console.IsOutputRedirected)
        {
            try
            {
                Console.Clear();
                Console.CursorVisible = false;
            }
            catch { /* Игнорируем ошибки консоли */ }
        }
        
        // Если передан аргумент --transaction-test - запускаем тест транзакций
        if (args.Contains("--transaction-test"))
        {
            await RunTransactionTestAsync();
            return;
        }

        StartTime = DateTime.UtcNow;

        Console.WriteLine("╔════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║              🚀 PgBouncer.NET Stress Tester - 10 Projects                  ║");
        Console.WriteLine("╠════════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine($"║  Прокси: {ProxyHost}:{ProxyPort}                                                         ║");
        Console.WriteLine($"║  Режим: {(UseSingleDatabase ? "Одна БД с виртуальными схемами" : "10 отдельных баз данных")}              ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine("Для запуска теста транзакций используйте: dotnet run -- --transaction-test");
        Console.WriteLine();

        // Инициализируем статистику
        foreach (var project in Projects)
        {
            Stats[project.Name] = new ProjectStats { ProjectName = project.Name };
        }

        // Запускаем все проекты
        var cts = new CancellationTokenSource();
        var tasks = Projects.Select(p => RunProjectAsync(p, cts.Token)).ToList();

        // Запускаем отображение статистики
        var displayTask = DisplayStatsAsync(cts.Token);

        // Запускаем на фиксированное время (30 секунд) если нет интерактивной консоли
        if (Console.IsInputRedirected || Console.IsOutputRedirected)
        {
            Console.WriteLine("Запущен неинтерактивный режим, тест будет работать 30 секунд...\n");
            await Task.Delay(TimeSpan.FromSeconds(30), cts.Token);
        }
        else
        {
            Console.WriteLine("Нажми любую клавишу для остановки...\n");
            Console.ReadKey(true);
        }

        cts.Cancel();
        await Task.WhenAll(tasks);

        Console.CursorVisible = true;
        Console.WriteLine("\n\n✅ Тест завершён!");
        Console.WriteLine($"   Всего операций: {TotalOperations:N0}");
        Console.WriteLine($"   Всего ошибок: {TotalErrors:N0}");
        Console.WriteLine($"   Время работы: {DateTime.UtcNow - StartTime:hh\\:mm\\:ss}");
    }

    static async Task RunProjectAsync(VirtualProject project, CancellationToken ct)
    {
        var stats = Stats[project.Name];
        var random = new Random(project.DbIndex);
        var connections = new List<Task>();

        try
        {
            // Постепенно наращиваем нагрузку
            var targetConnections = random.Next(project.MinConnections, project.MaxConnections + 1);

            for (int i = 0; i < targetConnections && !ct.IsCancellationRequested; i++)
            {
                // Для транзакционного теста используем специальную логику
                Task connectionTask;
                if (project.Name.Contains("Transaction"))
                {
                    connectionTask = RunTransactionConnectionAsync(project, stats, random, ct);
                }
                else
                {
                    connectionTask = RunConnectionAsync(project, stats, random, ct);
                }
                connections.Add(connectionTask);

                // Небольшая задержка между запуском соединений
                await Task.Delay(random.Next(50, 200), ct);
            }

            await Task.WhenAll(connections);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Interlocked.Increment(ref TotalErrors);
            stats.Errors++;
            stats.LastError = ex.Message;
        }
    }

    /// <summary>
    /// Выполняет транзакции в стиле EF Core (BEGIN -> запросы -> COMMIT)
    /// </summary>
    static async Task RunTransactionConnectionAsync(VirtualProject project, ProjectStats stats, Random random, CancellationToken ct)
    {
        var dbName = UseSingleDatabase ? SingleDatabaseName : Databases[project.DbIndex];
        // Важно: отключаем внутренний пул Npgsql как на проде
        var connStr = $"Host={ProxyHost};Port={ProxyPort};Database={dbName};Username={Username};Password={Password};Pooling=false;Command Timeout=120;Timeout=60";
        
        // Создаем таблицу при первом запуске
        await EnsureTransactionTableExists(connStr, ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                Interlocked.Increment(ref stats.ActiveConnections);

                await using var conn = new NpgsqlConnection(connStr);
                await conn.OpenAsync(ct);
                stats.ConnectionsOpened++;

                // Выполняем 5-10 транзакций на одном соединении
                var txCount = random.Next(5, 11);
                for (int t = 0; t < txCount && !ct.IsCancellationRequested; t++)
                {
                    await using var tx = await conn.BeginTransactionAsync(ct);
                    
                    try
                    {
                        // Имитируем работу EF Core: INSERT + UPDATE + SELECT
                        var iteration = random.Next(1, 1000000);
                        
                        // INSERT
                        await using (var cmd1 = new NpgsqlCommand(
                            "INSERT INTO test_transactions (iteration, operation) VALUES (@i, 'insert')", conn, tx))
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

                        // UPDATE
                        await using (var cmd3 = new NpgsqlCommand(
                            "UPDATE test_transactions SET operation = 'updated' WHERE iteration = @i", conn, tx))
                        {
                            cmd3.Parameters.AddWithValue("i", iteration);
                            await cmd3.ExecuteNonQueryAsync(ct);
                        }

                        // COMMIT
                        await tx.CommitAsync(ct);
                        
                        Interlocked.Increment(ref TotalOperations);
                        stats.QueriesExecuted += 3; // 3 операции в транзакции
                    }
                    catch
                    {
                        await tx.RollbackAsync(ct);
                        throw;
                    }

                    // Пауза между транзакциями
                    await Task.Delay(random.Next(50, 150), ct);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Interlocked.Increment(ref TotalErrors);
                stats.Errors++;
                stats.LastError = ex.Message[..Math.Min(50, ex.Message.Length)];
                
                // При ошибке ждем подольше
                await Task.Delay(random.Next(1000, 3000), ct);
            }
            finally
            {
                Interlocked.Decrement(ref stats.ActiveConnections);
            }

            // Пауза перед следующим соединением
            await Task.Delay(random.Next(200, 800), ct);
        }
    }

    /// <summary>
    /// Создает таблицу для тестирования транзакций если не существует
    /// </summary>
    static async Task EnsureTransactionTableExists(string connStr, CancellationToken ct)
    {
        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(@"
                CREATE TABLE IF NOT EXISTS test_transactions (
                    id SERIAL PRIMARY KEY,
                    iteration INTEGER NOT NULL,
                    operation VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )", conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch { /* Игнорируем ошибки создания таблицы */ }
    }

    static async Task RunConnectionAsync(VirtualProject project, ProjectStats stats, Random random, CancellationToken ct)
    {
        var dbName = UseSingleDatabase ? SingleDatabaseName : Databases[project.DbIndex];
        var connStr = $"Host={ProxyHost};Port={ProxyPort};Database={dbName};Username={Username};Password={Password};Timeout=60;Command Timeout=120";

        while (!ct.IsCancellationRequested)
        {
            try
            {
                Interlocked.Increment(ref stats.ActiveConnections);

                await using var conn = new NpgsqlConnection(connStr);
                await conn.OpenAsync(ct);
                stats.ConnectionsOpened++;

                // Выполняем несколько запросов в рамках одного соединения
                var queriesPerConnection = random.Next(5, 20);
                for (int q = 0; q < queriesPerConnection && !ct.IsCancellationRequested; q++)
                {
                    try
                    {
                        // Симулируем разные типы запросов
                        var queryType = random.Next(100);
                        string sql;

                        if (queryType < 60) // 60% - простые SELECT
                        {
                            sql = "SELECT 1";
                        }
                        else if (queryType < 85) // 25% - SELECT с небольшой нагрузкой
                        {
                            sql = "SELECT generate_series(1, 100)";
                        }
                        else // 15% - тяжёлые запросы (используем pg_sleep с параметром)
                        {
                            var sleepSeconds = (project.AvgQueryTimeMs / 1000.0).ToString(System.Globalization.CultureInfo.InvariantCulture);
                            sql = $"SELECT pg_sleep({sleepSeconds})";
                        }

                        await using var cmd = new NpgsqlCommand(sql, conn);
                        await cmd.ExecuteNonQueryAsync(ct);

                        Interlocked.Increment(ref TotalOperations);
                        stats.QueriesExecuted++;
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        Interlocked.Increment(ref TotalErrors);
                        stats.Errors++;
                        stats.LastError = ex.Message[..Math.Min(50, ex.Message.Length)];
                    }

                    // Задержка между запросами
                    await Task.Delay(random.Next(10, 100), ct);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Interlocked.Increment(ref TotalErrors);
                stats.Errors++;
                stats.LastError = ex.Message[..Math.Min(50, ex.Message.Length)];

                // При ошибке ждём подольше
                await Task.Delay(random.Next(1000, 3000), ct);
            }
            finally
            {
                Interlocked.Decrement(ref stats.ActiveConnections);
            }

            // Небольшая пауза перед следующим соединением
            await Task.Delay(random.Next(100, 500), ct);
        }
    }

    static async Task DisplayStatsAsync(CancellationToken ct)
    {
        const int headerLines = 7;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(500, ct);

                if (!Console.IsOutputRedirected)
                {
                    try
                    {
                        Console.SetCursorPosition(0, headerLines);
                    }
                    catch { /* Игнорируем ошибки позиционирования */ }
                }

                // Заголовок таблицы
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("┌──────────────────────────┬────────┬──────────┬──────────┬────────┬──────────────────────────────┐");
                Console.WriteLine("│ Проект                   │ Актив  │ Открыто  │ Запросов │ Ошибок │ Последняя ошибка             │");
                Console.WriteLine("├──────────────────────────┼────────┼──────────┼──────────┼────────┼──────────────────────────────┤");
                Console.ResetColor();

                foreach (var project in Projects)
                {
                    var stats = Stats[project.Name];

                    // Цвет в зависимости от состояния
                    if (stats.Errors > 0)
                        Console.ForegroundColor = ConsoleColor.Red;
                    else if (stats.ActiveConnections > project.MaxConnections * 0.8)
                        Console.ForegroundColor = ConsoleColor.Yellow;
                    else
                        Console.ForegroundColor = ConsoleColor.Green;

                    var lastError = string.IsNullOrEmpty(stats.LastError) ? "-" : stats.LastError;
                    if (lastError.Length > 28) lastError = lastError[..28] + "..";

                    Console.WriteLine($"│ {project.Name,-24} │ {stats.ActiveConnections,6} │ {stats.ConnectionsOpened,8} │ {stats.QueriesExecuted,8} │ {stats.Errors,6} │ {lastError,-28} │");
                    Console.ResetColor();
                }

                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("└──────────────────────────┴────────┴──────────┴──────────┴────────┴──────────────────────────────┘");
                Console.ResetColor();

                // Общая статистика
                var elapsed = DateTime.UtcNow - StartTime;
                var opsPerSec = elapsed.TotalSeconds > 0 ? TotalOperations / elapsed.TotalSeconds : 0;
                var totalActive = Stats.Values.Sum(s => s.ActiveConnections);

                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine($"  📊 Всего: {TotalOperations:N0} операций | {opsPerSec:N0} ops/sec | {totalActive} активных | {TotalErrors} ошибок | {elapsed:hh\\:mm\\:ss}");
                Console.ResetColor();

                // Прогресс бар нагрузки
                var loadPercent = Math.Min(100, (int)(totalActive / 500.0 * 100));
                Console.Write("  [");
                Console.ForegroundColor = loadPercent > 80 ? ConsoleColor.Red : loadPercent > 50 ? ConsoleColor.Yellow : ConsoleColor.Green;
                Console.Write(new string('█', loadPercent / 5));
                Console.Write(new string('░', 20 - loadPercent / 5));
                Console.ResetColor();
                Console.WriteLine($"] {loadPercent}% нагрузки");
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    }

    /// <summary>
    /// Запускает тест транзакций для проверки фикса пулера
    /// </summary>
    static async Task RunTransactionTestAsync()
    {
        var dbName = UseSingleDatabase ? SingleDatabaseName : Databases[0];
        var tester = new TransactionTester(ProxyHost, ProxyPort, Username, Password, dbName);

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (s, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            await tester.RunTestAsync(100, cts.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("\n\n⚠️ Тест прерван пользователем");
        }

        // Возвращаем код ошибки если были ошибки
        if (tester.ErrorCount > 0)
        {
            Environment.Exit(1);
        }
    }
}

record VirtualProject(string Name, int DbIndex, int MinConnections, int MaxConnections, int AvgQueryTimeMs);

class ProjectStats
{
    public string ProjectName { get; init; } = "";
    public long ActiveConnections;
    public long ConnectionsOpened;
    public long QueriesExecuted;
    public long Errors;
    public string? LastError;
}

/// <summary>
/// Тестер для проверки корректности работы транзакций с пулером
/// Имитирует поведение EF Core: BEGIN -> запросы -> COMMIT
/// </summary>
class TransactionTester
{
    private readonly string _proxyHost;
    private readonly int _proxyPort;
    private readonly string _username;
    private readonly string _password;
    private readonly string _database;

    public long SuccessCount { get; private set; }
    public long ErrorCount { get; private set; }
    public List<string> Errors { get; } = new();

    public TransactionTester(string proxyHost, int proxyPort, string username, string password, string database)
    {
        _proxyHost = proxyHost;
        _proxyPort = proxyPort;
        _username = username;
        _password = password;
        _database = database;
    }

    public async Task RunTestAsync(int iterations, CancellationToken ct)
    {
        // Connection string с отключенным внутренним пулом (как в проде)
        var connStr = $"Host={_proxyHost};Port={_proxyPort};Database={_database};" +
                      $"Username={_username};Password={_password};" +
                      $"Pooling=false;Command Timeout=300;Timeout=60";

        Console.WriteLine("╔════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                 🧪 Transaction Pooling Test - EF Core Style                  ║");
        Console.WriteLine("╠════════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine($"║  Target: {_proxyHost}:{_proxyPort,-16}                                  ║");
        Console.WriteLine($"║  Database: {_database,-20}                            ║");
        Console.WriteLine($"║  Iterations: {iterations,-10}                                          ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine("Тест имитирует EF Core SaveChanges():");
        Console.WriteLine("  1. Открытие соединения");
        Console.WriteLine("  2. BEGIN (начало транзакции)");
        Console.WriteLine("  3. Выполнение нескольких запросов");
        Console.WriteLine("  4. COMMIT (фиксация транзакции)");
        Console.WriteLine("  5. Закрытие соединения");
        Console.WriteLine();

        // Создаем таблицу если не существует
        try
        {
            await using var setupConn = new NpgsqlConnection(connStr);
            await setupConn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(@"
                CREATE TABLE IF NOT EXISTS test_transactions (
                    id SERIAL PRIMARY KEY,
                    iteration INTEGER NOT NULL,
                    operation VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )", setupConn);
            await cmd.ExecuteNonQueryAsync(ct);
            Console.WriteLine("✅ Таблица test_transactions создана/проверена");
            Console.WriteLine();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️ Не удалось создать таблицу: {ex.Message}");
            Console.WriteLine("   Продолжаем тест...");
            Console.WriteLine();
        }

        for (int i = 1; i <= iterations && !ct.IsCancellationRequested; i++)
        {
            try
            {
                await RunSingleTransactionAsync(connStr, i, ct);
                SuccessCount++;

                if (i % 10 == 0)
                {
                    Console.WriteLine($"✅ Итерация {i}/{iterations} - OK (Success: {SuccessCount}, Errors: {ErrorCount})");
                }
            }
            catch (Exception ex)
            {
                ErrorCount++;
                var errorMsg = $"❌ Итерация {i}: {ex.Message}";
                Errors.Add(errorMsg);
                Console.WriteLine(errorMsg);

                if (Errors.Count >= 5)
                {
                    Console.WriteLine("\n⚠️ Слишком много ошибок, останавливаем тест");
                    break;
                }
            }

            // Небольшая задержка между итерациями
            await Task.Delay(100, ct);
        }

        PrintResults();
    }

    private async Task RunSingleTransactionAsync(string connStr, int iteration, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync(ct);

        // Начинаем транзакцию (как EF Core при SaveChanges())
        await using var tx = await conn.BeginTransactionAsync(ct);

        try
        {
            // Имитируем несколько операций внутри транзакции
            // INSERT
            await using (var cmd1 = new NpgsqlCommand(
                "INSERT INTO test_transactions (iteration, operation) VALUES (@i, 'insert')", conn, tx))
            {
                cmd1.Parameters.AddWithValue("i", iteration);
                await cmd1.ExecuteNonQueryAsync(ct);
            }

            // SELECT
            await using (var cmd2 = new NpgsqlCommand(
                "SELECT COUNT(*) FROM test_transactions WHERE iteration = @i", conn, tx))
            {
                cmd2.Parameters.AddWithValue("i", iteration);
                var count = (long)(await cmd2.ExecuteScalarAsync(ct) ?? 0);
                if (count != 1)
                    throw new Exception($"Expected 1 row, got {count}");
            }

            // UPDATE
            await using (var cmd3 = new NpgsqlCommand(
                "UPDATE test_transactions SET operation = 'updated' WHERE iteration = @i", conn, tx))
            {
                cmd3.Parameters.AddWithValue("i", iteration);
                await cmd3.ExecuteNonQueryAsync(ct);
            }

            // Фиксируем транзакцию
            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
        // Соединение закроется автоматически (Dispose)
    }

    private void PrintResults()
    {
        Console.WriteLine("\n╔════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                           📊 РЕЗУЛЬТАТЫ ТЕСТА                              ║");
        Console.WriteLine("╠════════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine($"║  Успешно:    {SuccessCount,10}                                                    ║");
        Console.WriteLine($"║  Ошибок:     {ErrorCount,10}                                                    ║");
        Console.WriteLine($"║  Результат:  {(ErrorCount == 0 ? "✅ PASS" : "❌ FAIL"),10}                                                    ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════════════════╝");

        if (Errors.Any())
        {
            Console.WriteLine("\nПоследние ошибки:");
            foreach (var error in Errors.Take(5))
            {
                Console.WriteLine($"  {error}");
            }
        }
    }
}
