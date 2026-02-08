using System.Collections.Concurrent;
using System.Diagnostics;
using Npgsql;

namespace PgBouncer.LoadTester;

/// <summary>
/// Запускает нагрузочное тестирование
/// </summary>
public class LoadTestRunner
{
    private readonly Options _options;
    private readonly ConcurrentBag<TestMetrics> _metrics = new();
    private readonly Stopwatch _stopwatch = new();

    public LoadTestRunner(Options options)
    {
        _options = options;
    }

    public async Task RunAsync()
    {
        Console.WriteLine("Запуск теста...");
        _stopwatch.Start();

        var tasks = new List<Task>();
        
        // Запускаем экземпляры
        for (int i = 0; i < _options.Instances; i++)
        {
            var instanceId = i;
            tasks.Add(Task.Run(async () => await RunInstanceAsync(instanceId)));
        }

        // Запускаем мониторинг статистики
        var monitorTask = Task.Run(async () => await MonitorStatsAsync());

        await Task.WhenAll(tasks);
        _stopwatch.Stop();

        // Выводим итоговую статистику
        PrintFinalStats();
    }

    /// <summary>
    /// Запустить один экземпляр тестера
    /// </summary>
    private async Task RunInstanceAsync(int instanceId)
    {
        var tasks = new List<Task>();

        for (int i = 0; i < _options.Connections; i++)
        {
            var connId = i;
            tasks.Add(Task.Run(async () => await RunConnectionAsync(instanceId, connId)));
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Запустить одно соединение
    /// </summary>
    private async Task RunConnectionAsync(int instanceId, int connectionId)
    {
        var connString = $"Host={_options.Host};Port={_options.Port};Database={_options.Database};" +
                        $"Username={_options.User};Password={_options.Password};Pooling=false";

        var pattern = GetPattern(_options.Pattern);
        var metrics = new TestMetrics();

        try
        {
            await using var conn = new NpgsqlConnection(connString);
            await conn.OpenAsync();

            var endTime = DateTime.UtcNow.AddSeconds(_options.Duration);

            while (DateTime.UtcNow < endTime)
            {
                var sw = Stopwatch.StartNew();
                
                try
                {
                    await pattern.ExecuteAsync(conn);
                    sw.Stop();
                    
                    metrics.TotalQueries++;
                    metrics.Latencies.Add(sw.Elapsed.TotalMilliseconds);
                }
                catch (Exception ex)
                {
                    metrics.Errors++;
                    Console.WriteLine($"[Instance {instanceId}, Conn {connectionId}] Ошибка: {ex.Message}");
                }

                await Task.Delay(pattern.GetDelayMs());
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Instance {instanceId}, Conn {connectionId}] Фатальная ошибка: {ex.Message}");
            metrics.Errors++;
        }

        _metrics.Add(metrics);
    }

    /// <summary>
    /// Мониторинг статистики в реальном времени
    /// </summary>
    private async Task MonitorStatsAsync()
    {
        while (_stopwatch.Elapsed.TotalSeconds < _options.Duration)
        {
            await Task.Delay(5000); // каждые 5 секунд

            var elapsed = _stopwatch.Elapsed.TotalSeconds;
            var totalQueries = _metrics.Sum(m => m.TotalQueries);
            var totalErrors = _metrics.Sum(m => m.Errors);
            var qps = totalQueries / elapsed;

            Console.WriteLine($"[{elapsed:F1}s] Запросов: {totalQueries}, QPS: {qps:F1}, Ошибок: {totalErrors}");
        }
    }

    /// <summary>
    /// Вывести итоговую статистику
    /// </summary>
    private void PrintFinalStats()
    {
        var totalQueries = _metrics.Sum(m => m.TotalQueries);
        var totalErrors = _metrics.Sum(m => m.Errors);
        var allLatencies = _metrics.SelectMany(m => m.Latencies).OrderBy(l => l).ToList();

        var avgLatency = allLatencies.Any() ? allLatencies.Average() : 0;
        var p50 = allLatencies.Any() ? allLatencies[(int)(allLatencies.Count * 0.50)] : 0;
        var p95 = allLatencies.Any() ? allLatencies[(int)(allLatencies.Count * 0.95)] : 0;
        var p99 = allLatencies.Any() ? allLatencies[(int)(allLatencies.Count * 0.99)] : 0;

        var duration = _stopwatch.Elapsed.TotalSeconds;
        var qps = totalQueries / duration;

        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                  ИТОГОВАЯ СТАТИСТИКА                     ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"Длительность:        {duration:F2}с");
        Console.WriteLine($"Всего запросов:      {totalQueries}");
        Console.WriteLine($"Запросов/сек:        {qps:F2}");
        Console.WriteLine($"Ошибок:              {totalErrors} ({(totalErrors * 100.0 / Math.Max(totalQueries, 1)):F2}%)");
        Console.WriteLine();
        Console.WriteLine("Latency:");
        Console.WriteLine($"  Средняя:           {avgLatency:F2} мс");
        Console.WriteLine($"  p50:               {p50:F2} мс");
        Console.WriteLine($"  p95:               {p95:F2} мс");
        Console.WriteLine($"  p99:               {p99:F2} мс");
        Console.WriteLine();
    }

    /// <summary>
    /// Получить паттерн нагрузки
    /// </summary>
    private ILoadPattern GetPattern(string patternName)
    {
        return patternName.ToLower() switch
        {
            "rapid" => new RapidPattern(),
            "idle" => new IdlePattern(),
            "mixed" => new MixedPattern(),
            "burst" => new BurstPattern(),
            "transaction" => new TransactionPattern(),
            _ => new MixedPattern()
        };
    }
}

/// <summary>
/// Метрики одного соединения
/// </summary>
public class TestMetrics
{
    public long TotalQueries { get; set; }
    public long Errors { get; set; }
    public List<double> Latencies { get; } = new();
}

/// <summary>
/// Паттерн нагрузки
/// </summary>
public interface ILoadPattern
{
    Task ExecuteAsync(NpgsqlConnection connection);
    int GetDelayMs();
}

/// <summary>
/// Rapid - быстрые короткие запросы
/// </summary>
public class RapidPattern : ILoadPattern
{
    public async Task ExecuteAsync(NpgsqlConnection connection)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1", connection);
        await cmd.ExecuteScalarAsync();
    }

    public int GetDelayMs() => 10; // 10ms между запросами
}

/// <summary>
/// Idle - редкие запросы, много idle времени
/// </summary>
public class IdlePattern : ILoadPattern
{
    public async Task ExecuteAsync(NpgsqlConnection connection)
    {
        await using var cmd = new NpgsqlCommand("SELECT pg_sleep(0.1)", connection);
        await cmd.ExecuteNonQueryAsync();
    }

    public int GetDelayMs() => 5000; // 5 секунд между запросами
}

/// <summary>
/// Mixed - комбинация active/idle
/// </summary>
public class MixedPattern : ILoadPattern
{
    private readonly Random _random = new();

    public async Task ExecuteAsync(NpgsqlConnection connection)
    {
        var query = _random.Next(3) switch
        {
            0 => "SELECT 1",
            1 => "SELECT COUNT(*) FROM pg_database",
            _ => "SELECT pg_sleep(0.05)"
        };

        await using var cmd = new NpgsqlCommand(query, connection);
        await cmd.ExecuteScalarAsync();
    }

    public int GetDelayMs() => Random.Shared.Next(100, 1000); // 100-1000ms
}

/// <summary>
/// Burst - периодические всплески
/// </summary>
public class BurstPattern : ILoadPattern
{
    private int _counter;

    public async Task ExecuteAsync(NpgsqlConnection connection)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1", connection);
        await cmd.ExecuteScalarAsync();
    }

    public int GetDelayMs()
    {
        _counter++;
        // Каждые 10 запросов - всплеск (быстро), иначе - пауза
        return (_counter % 10 == 0) ? 10 : 2000;
    }
}

/// <summary>
/// Transaction - длинные транзакции
/// </summary>
public class TransactionPattern : ILoadPattern
{
    public async Task ExecuteAsync(NpgsqlConnection connection)
    {
        await using var tx = await connection.BeginTransactionAsync();
        
        for (int i = 0; i < 5; i++)
        {
            await using var cmd = new NpgsqlCommand("SELECT 1", connection, tx);
            await cmd.ExecuteScalarAsync();
        }

        await tx.CommitAsync();
    }

    public int GetDelayMs() => 500; // 500ms между транзакциями
}
