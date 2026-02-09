using System.Collections.Concurrent;
using Npgsql;

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
        new("Session Manager", 9, 80, 150, 30)          // 80-150 соединений, самые быстрые
    };

    // Статистика
    static readonly ConcurrentDictionary<string, ProjectStats> Stats = new();
    static long TotalOperations = 0;
    static long TotalErrors = 0;
    static DateTime StartTime;

    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        Console.Clear();
        Console.CursorVisible = false;

        StartTime = DateTime.UtcNow;

        Console.WriteLine("╔════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║              🚀 PgBouncer.NET Stress Tester - 10 Projects                  ║");
        Console.WriteLine("╠════════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine($"║  Прокси: {ProxyHost}:{ProxyPort}                                                         ║");
        Console.WriteLine($"║  Режим: {(UseSingleDatabase ? "Одна БД с виртуальными схемами" : "10 отдельных баз данных")}              ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════════════════╝");
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

        Console.WriteLine("Нажми любую клавишу для остановки...\n");
        Console.ReadKey(true);

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
                var connectionTask = RunConnectionAsync(project, stats, random, ct);
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
        const int tableHeaderLines = 3;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(500, ct);

                Console.SetCursorPosition(0, headerLines);

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
