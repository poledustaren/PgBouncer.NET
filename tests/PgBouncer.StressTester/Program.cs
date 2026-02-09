using System.Collections.Concurrent;
using System.Diagnostics;
using Npgsql;

namespace PgBouncer.StressTester;

class Program
{
    // === НАСТРОЙКИ ===
    static readonly string Host = "127.0.0.1";
    static readonly int Port = 6432; // Порт PgBouncer
    static readonly string User = "postgres";
    static readonly string Password = "123";

    // Симуляция разных проектов/сервисов
    static readonly ProjectConfig[] Projects =
    {
        new("🛒 E-Commerce API",    "postgres",  50,  100, OperationType.Mixed),
        new("📊 Analytics Service", "testdb1",   30,  80,  OperationType.ReadHeavy),
        new("✉️ Email Worker",      "postgres",  20,  50,  OperationType.WriteHeavy),
        new("🔔 Notifications",     "testdb2",   40,  100, OperationType.BurstRead),
        new("📈 Reporting",         "testdb1",   10,  30,  OperationType.LongQueries),
        new("🔐 Auth Service",      "postgres",  60,  150, OperationType.QuickRead),
    };

    // Уровни нагрузки
    static readonly int[] LoadMultipliers = { 1, 2, 5, 10, 20 };

    // Статистика
    static readonly ConcurrentDictionary<string, ProjectStats> _projectStats = new();
    static long _totalOperations;
    static long _totalErrors;
    static int _currentConnections;
    static int _peakConnections;
    static bool _running = true;

    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        Console.CursorVisible = false;

        // Инициализация
        foreach (var project in Projects)
        {
            _projectStats[project.Name] = new ProjectStats();
        }

        PrintBanner();
        await InitializeDatabasesAsync();

        Console.WriteLine("\n🚀 Нажмите ENTER для запуска стресс-теста...");
        Console.ReadLine();
        Console.Clear();

        // Запуск мониторинга в отдельном потоке
        var monitorTask = Task.Run(MonitorLoop);

        // Запуск нарастающей нагрузки
        foreach (var multiplier in LoadMultipliers)
        {
            await RunLoadPhaseAsync(multiplier, TimeSpan.FromSeconds(15));

            if (!_running) break;

            // Пауза между фазами
            await Task.Delay(3000);
        }

        _running = false;
        await monitorTask;

        // Финальные результаты
        Console.Clear();
        PrintFinalResults();
        Console.CursorVisible = true;
    }

    static void PrintBanner()
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine(@"
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║     ██████╗  ██████╗ ██████╗  ██████╗ ██╗   ██╗███╗   ██╗ ██████╗███████╗     ║
║     ██╔══██╗██╔════╝ ██╔══██╗██╔═══██╗██║   ██║████╗  ██║██╔════╝██╔════╝     ║
║     ██████╔╝██║  ███╗██████╔╝██║   ██║██║   ██║██╔██╗ ██║██║     █████╗       ║
║     ██╔═══╝ ██║   ██║██╔══██╗██║   ██║██║   ██║██║╚██╗██║██║     ██╔══╝       ║
║     ██║     ╚██████╔╝██████╔╝╚██████╔╝╚██████╔╝██║ ╚████║╚██████╗███████╗     ║
║     ╚═╝      ╚═════╝ ╚═════╝  ╚═════╝  ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝╚══════╝     ║
║                                                                               ║
║                        🔥 MEGA STRESS TESTER 🔥                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
");
        Console.ResetColor();

        Console.WriteLine($"  🎯 Target: {Host}:{Port}");
        Console.WriteLine($"\n  📦 Simulated Projects:");
        foreach (var p in Projects)
        {
            Console.WriteLine($"     {p.Name} → {p.Database} ({p.Type})");
        }
    }

    static async Task InitializeDatabasesAsync()
    {
        Console.WriteLine("\n  📦 Initializing databases...");

        var databases = Projects.Select(p => p.Database).Distinct();

        foreach (var db in databases)
        {
            try
            {
                var connStr = $"Host={Host};Port={Port};Database={db};Username={User};Password={Password};Timeout=30";
                await using var conn = new NpgsqlConnection(connStr);
                await conn.OpenAsync();

                await using var cmd = new NpgsqlCommand(@"
                    CREATE TABLE IF NOT EXISTS stress_test (
                        id SERIAL PRIMARY KEY,
                        project_name VARCHAR(100),
                        data TEXT,
                        value INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )", conn);
                await cmd.ExecuteNonQueryAsync();

                await using var truncateCmd = new NpgsqlCommand("TRUNCATE stress_test RESTART IDENTITY", conn);
                await truncateCmd.ExecuteNonQueryAsync();

                Console.WriteLine($"     ✅ {db}: ready");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"     ⚠️ {db}: {ex.Message}");
            }
        }
    }

    static async Task RunLoadPhaseAsync(int multiplier, TimeSpan duration)
    {
        var tasks = new List<Task>();

        foreach (var project in Projects)
        {
            var workers = project.BaseWorkers * multiplier;
            workers = Math.Min(workers, project.MaxWorkers);

            for (int i = 0; i < workers; i++)
            {
                tasks.Add(WorkerAsync(project, i, duration));
            }
        }

        using var cts = new CancellationTokenSource(duration);

        try
        {
            await Task.WhenAll(tasks);
        }
        catch { }
    }

    static async Task WorkerAsync(ProjectConfig project, int workerId, TimeSpan duration)
    {
        var connStr = $"Host={Host};Port={Port};Database={project.Database};Username={User};Password={Password};Timeout=30;Pooling=false";
        var stats = _projectStats[project.Name];
        var deadline = DateTime.UtcNow.Add(duration);

        Interlocked.Increment(ref _currentConnections);
        UpdatePeak();

        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync();

            stats.ActiveConnections++;

            var random = new Random(workerId * 1000 + project.Name.GetHashCode());

            while (DateTime.UtcNow < deadline && _running)
            {
                try
                {
                    var sw = Stopwatch.StartNew();

                    switch (project.Type)
                    {
                        case OperationType.Mixed:
                            await ExecuteMixedAsync(conn, project.Name, random);
                            break;
                        case OperationType.ReadHeavy:
                            await ExecuteReadHeavyAsync(conn, project.Name, random);
                            break;
                        case OperationType.WriteHeavy:
                            await ExecuteWriteHeavyAsync(conn, project.Name, random);
                            break;
                        case OperationType.BurstRead:
                            await ExecuteBurstReadAsync(conn, project.Name, random);
                            break;
                        case OperationType.LongQueries:
                            await ExecuteLongQueryAsync(conn, project.Name, random);
                            break;
                        case OperationType.QuickRead:
                            await ExecuteQuickReadAsync(conn, project.Name, random);
                            break;
                    }

                    sw.Stop();
                    stats.TotalOperations++;
                    stats.TotalLatencyMs += sw.ElapsedMilliseconds;
                    Interlocked.Increment(ref _totalOperations);
                }
                catch
                {
                    stats.Errors++;
                    Interlocked.Increment(ref _totalErrors);
                }
            }
        }
        catch
        {
            stats.Errors++;
            Interlocked.Increment(ref _totalErrors);
        }
        finally
        {
            stats.ActiveConnections--;
            Interlocked.Decrement(ref _currentConnections);
        }
    }

    static async Task ExecuteMixedAsync(NpgsqlConnection conn, string project, Random random)
    {
        var op = random.Next(4);
        switch (op)
        {
            case 0:
                await InsertAsync(conn, project, random);
                break;
            case 1:
                await UpdateAsync(conn, random);
                break;
            case 2:
                await DeleteAsync(conn, random);
                break;
            case 3:
                await SelectAsync(conn, random);
                break;
        }
    }

    static async Task ExecuteReadHeavyAsync(NpgsqlConnection conn, string project, Random random)
    {
        if (random.Next(10) < 8) // 80% reads
            await SelectAsync(conn, random);
        else
            await InsertAsync(conn, project, random);
    }

    static async Task ExecuteWriteHeavyAsync(NpgsqlConnection conn, string project, Random random)
    {
        if (random.Next(10) < 8) // 80% writes
            await InsertAsync(conn, project, random);
        else
            await SelectAsync(conn, random);
    }

    static async Task ExecuteBurstReadAsync(NpgsqlConnection conn, string project, Random random)
    {
        // 5 быстрых SELECT подряд
        for (int i = 0; i < 5; i++)
        {
            await SelectAsync(conn, random);
        }
    }

    static async Task ExecuteLongQueryAsync(NpgsqlConnection conn, string project, Random random)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_sleep(0.1), COUNT(*) FROM stress_test", conn);
        await cmd.ExecuteScalarAsync();
    }

    static async Task ExecuteQuickReadAsync(NpgsqlConnection conn, string project, Random random)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT 1", conn);
        await cmd.ExecuteScalarAsync();
    }

    static async Task InsertAsync(NpgsqlConnection conn, string project, Random random)
    {
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO stress_test (project_name, data, value) VALUES (@project, @data, @value)", conn);
        cmd.Parameters.AddWithValue("project", project);
        cmd.Parameters.AddWithValue("data", $"Data-{Guid.NewGuid():N}");
        cmd.Parameters.AddWithValue("value", random.Next(1, 10000));
        await cmd.ExecuteNonQueryAsync();
    }

    static async Task UpdateAsync(NpgsqlConnection conn, Random random)
    {
        await using var cmd = new NpgsqlCommand(
            "UPDATE stress_test SET value = @value WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("value", random.Next(1, 10000));
        cmd.Parameters.AddWithValue("id", random.Next(1, 1000));
        await cmd.ExecuteNonQueryAsync();
    }

    static async Task DeleteAsync(NpgsqlConnection conn, Random random)
    {
        await using var cmd = new NpgsqlCommand(
            "DELETE FROM stress_test WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", random.Next(1, 100));
        await cmd.ExecuteNonQueryAsync();
    }

    static async Task SelectAsync(NpgsqlConnection conn, Random random)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT * FROM stress_test ORDER BY id DESC LIMIT @limit", conn);
        cmd.Parameters.AddWithValue("limit", random.Next(1, 50));
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) { }
    }

    static void UpdatePeak()
    {
        var current = _currentConnections;
        int previous;
        do
        {
            previous = _peakConnections;
            if (current <= previous) return;
        } while (Interlocked.CompareExchange(ref _peakConnections, current, previous) != previous);
    }

    static async Task MonitorLoop()
    {
        var startTime = DateTime.UtcNow;
        long lastOps = 0;

        while (_running)
        {
            Console.SetCursorPosition(0, 0);
            DrawDashboard(startTime, ref lastOps);
            await Task.Delay(500);
        }
    }

    static void DrawDashboard(DateTime startTime, ref long lastOps)
    {
        var elapsed = DateTime.UtcNow - startTime;
        var currentOps = _totalOperations;
        var opsPerSec = (currentOps - lastOps) * 2; // x2 потому что обновляемся каждые 500мс
        lastOps = currentOps;

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                    🔥 PgBouncer.NET STRESS TEST LIVE 🔥                      ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════════╣");
        Console.ResetColor();

        // Общая статистика
        Console.Write("║ ");
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.Write($"⏱️ {elapsed:mm\\:ss}");
        Console.ResetColor();
        Console.Write($"   📊 Ops: {_totalOperations:N0}".PadRight(20));
        Console.Write($"⚡ {opsPerSec:N0}/s".PadRight(15));
        Console.Write($"🔗 {_currentConnections}/{_peakConnections}".PadRight(15));
        Console.ForegroundColor = _totalErrors > 0 ? ConsoleColor.Red : ConsoleColor.Green;
        Console.Write($"❌ {_totalErrors}");
        Console.ResetColor();
        Console.WriteLine("      ║");

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════════╣");
        Console.ResetColor();

        // Проекты
        foreach (var project in Projects)
        {
            var stats = _projectStats[project.Name];
            var avgLatency = stats.TotalOperations > 0
                ? stats.TotalLatencyMs / stats.TotalOperations
                : 0;

            // Progress bar
            var progress = stats.ActiveConnections > 0 ? Math.Min(stats.ActiveConnections, 20) : 0;
            var bar = new string('█', progress) + new string('░', 20 - progress);

            Console.Write("║ ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write($"{project.Name,-22}");
            Console.ResetColor();

            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write($"[{bar}]");
            Console.ResetColor();

            Console.Write($" {stats.TotalOperations,8:N0} ops");
            Console.Write($" {avgLatency,4}ms");

            if (stats.Errors > 0)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write($" ⚠{stats.Errors}");
                Console.ResetColor();
            }

            Console.WriteLine("  ║");
        }

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine("║                        Press Ctrl+C to stop                                  ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════╝");
        Console.ResetColor();
    }

    static void PrintFinalResults()
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine(@"
╔═══════════════════════════════════════════════════════════════════════════════╗
║                           📊 FINAL RESULTS 📊                                 ║
╚═══════════════════════════════════════════════════════════════════════════════╝
");
        Console.ResetColor();

        Console.WriteLine($"  📈 Total Operations: {_totalOperations:N0}");
        Console.WriteLine($"  ❌ Total Errors: {_totalErrors:N0}");
        Console.WriteLine($"  🔗 Peak Connections: {_peakConnections}");
        Console.WriteLine($"  ✅ Success Rate: {100.0 * _totalOperations / (_totalOperations + _totalErrors + 1):F1}%");

        Console.WriteLine("\n  📦 Per Project:");
        Console.WriteLine("  ─────────────────────────────────────────────────────────");

        foreach (var project in Projects)
        {
            var stats = _projectStats[project.Name];
            var avgLatency = stats.TotalOperations > 0
                ? stats.TotalLatencyMs / stats.TotalOperations
                : 0;

            Console.WriteLine($"  {project.Name}");
            Console.WriteLine($"     Operations: {stats.TotalOperations:N0}, Errors: {stats.Errors}, Avg Latency: {avgLatency}ms");
        }

        Console.WriteLine();
    }
}

// === Модели ===

record ProjectConfig(
    string Name,
    string Database,
    int BaseWorkers,
    int MaxWorkers,
    OperationType Type);

enum OperationType
{
    Mixed,
    ReadHeavy,
    WriteHeavy,
    BurstRead,
    LongQueries,
    QuickRead
}

class ProjectStats
{
    public long TotalOperations;
    public long TotalLatencyMs;
    public long Errors;
    public int ActiveConnections;
}
