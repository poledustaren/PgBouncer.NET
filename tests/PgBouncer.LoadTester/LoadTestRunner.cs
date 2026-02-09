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
        Console.WriteLine("═══════════════════════════════════════════════════════════");
        Console.WriteLine("                 ЗАПУСК НАГРУЗОЧНОГО ТЕСТА                  ");
        Console.WriteLine("═══════════════════════════════════════════════════════════");
        Console.WriteLine();
        Console.WriteLine($"[CONFIG] Host:        {_options.Host}");
        Console.WriteLine($"[CONFIG] Port:        {_options.Port}");
        Console.WriteLine($"[CONFIG] Database:    {_options.Database}");
        Console.WriteLine($"[CONFIG] User:        {_options.User}");
        Console.WriteLine($"[CONFIG] Password:    {_options.Password}");
        Console.WriteLine($"[CONFIG] Pattern:     {_options.Pattern}");
        Console.WriteLine($"[CONFIG] Duration:    {_options.Duration} сек");
        Console.WriteLine($"[CONFIG] Connections: {_options.Connections}");
        Console.WriteLine($"[CONFIG] Instances:   {_options.Instances}");
        Console.WriteLine();

        _stopwatch.Start();

        var tasks = new List<Task>();

        // Запускаем экземпляры
        for (int i = 0; i < _options.Instances; i++)
        {
            var instanceId = i;
            Console.WriteLine($"[START] Запуск экземпляра {instanceId}...");
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
        Console.WriteLine($"[DONE] Экземпляр {instanceId} завершён");
    }

    /// <summary>
    /// Запустить одно соединение
    /// </summary>
    private async Task RunConnectionAsync(int instanceId, int connectionId)
    {
        var connString = $"Host={_options.Host};Port={_options.Port};Database={_options.Database};" +
                        $"Username={_options.User};Password={_options.Password};Pooling=false;Timeout=30";

        Console.WriteLine($"[CONN {instanceId}.{connectionId}] Подключение: {_options.Host}:{_options.Port}/{_options.Database} как {_options.User}");

        var pattern = GetPattern(_options.Pattern);
        var metrics = new TestMetrics();

        try
        {
            await using var conn = new NpgsqlConnection(connString);

            Console.WriteLine($"[CONN {instanceId}.{connectionId}] Открытие соединения...");
            await conn.OpenAsync();
            Console.WriteLine($"[CONN {instanceId}.{connectionId}] Соединение открыто!");

            // Верификация подключения - проверяем что мы реально в нужной БД
            await using var checkCmd = new NpgsqlCommand("SELECT current_database(), current_user", conn);
            await using var checkReader = await checkCmd.ExecuteReaderAsync();
            if (await checkReader.ReadAsync())
            {
                var actualDb = checkReader.GetString(0);
                var actualUser = checkReader.GetString(1);
                metrics.VerifiedConnection = (actualDb == _options.Database);

                Console.WriteLine($"[VERIFY {instanceId}.{connectionId}] БД: {actualDb}, User: {actualUser} (ожидали: {_options.Database}/{_options.User})");

                if (!metrics.VerifiedConnection)
                {
                    Console.WriteLine($"[ERROR!!!] ВЕРИФИКАЦИЯ ПРОВАЛЕНА: Ожидали {_options.Database}, получили {actualDb}");
                }
                else
                {
                    Console.WriteLine($"[OK {instanceId}.{connectionId}] Верификация пройдена!");
                }
            }
            await checkReader.CloseAsync();

            var endTime = DateTime.UtcNow.AddSeconds(_options.Duration);

            while (DateTime.UtcNow < endTime)
            {
                var sw = Stopwatch.StartNew();

                try
                {
                    var result = await pattern.ExecuteWithVerificationAsync(conn);
                    sw.Stop();

                    metrics.TotalQueries++;
                    metrics.Latencies.Add(sw.Elapsed.TotalMilliseconds);

                    if (result.IsValid)
                    {
                        metrics.VerifiedQueries++;
                    }
                    else
                    {
                        metrics.VerificationFailed++;
                        Console.WriteLine($"[Conn {connectionId}] Верификация провалена: {result.Error}");
                    }
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

        // Статистика верификации
        var verifiedConnections = _metrics.Count(m => m.VerifiedConnection);
        var totalVerified = _metrics.Sum(m => m.VerifiedQueries);
        var totalVerifyFailed = _metrics.Sum(m => m.VerificationFailed);

        Console.WriteLine("Верификация:");
        Console.WriteLine($"  Соединений проверено: {verifiedConnections}/{_metrics.Count}");
        Console.WriteLine($"  Запросов верифицировано: {totalVerified}");
        Console.WriteLine($"  Верификаций провалено: {totalVerifyFailed}");
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

    // Верификация
    public bool VerifiedConnection { get; set; }
    public long VerifiedQueries { get; set; }
    public long VerificationFailed { get; set; }
}

/// <summary>
/// Результат верификации
/// </summary>
public class VerificationResult
{
    public bool IsValid { get; set; }
    public string? Error { get; set; }

    public static VerificationResult Success() => new() { IsValid = true };
    public static VerificationResult Fail(string error) => new() { IsValid = false, Error = error };
}

/// <summary>
/// Паттерн нагрузки
/// </summary>
public interface ILoadPattern
{
    Task<VerificationResult> ExecuteWithVerificationAsync(NpgsqlConnection connection);
    int GetDelayMs();
}

/// <summary>
/// Rapid - быстрые короткие запросы с верификацией
/// </summary>
public class RapidPattern : ILoadPattern
{
    public async Task<VerificationResult> ExecuteWithVerificationAsync(NpgsqlConnection connection)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1 as result", connection);
        var result = await cmd.ExecuteScalarAsync();

        if (result is int intResult && intResult == 1)
            return VerificationResult.Success();

        return VerificationResult.Fail($"Ожидали 1, получили {result}");
    }

    public int GetDelayMs() => 10;
}

/// <summary>
/// Idle - редкие запросы с верификацией
/// </summary>
public class IdlePattern : ILoadPattern
{
    public async Task<VerificationResult> ExecuteWithVerificationAsync(NpgsqlConnection connection)
    {
        await using var cmd = new NpgsqlCommand("SELECT pg_sleep(0.01), 42 as marker", connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        if (await reader.ReadAsync())
            return VerificationResult.Success();

        return VerificationResult.Fail("Нет результата от pg_sleep");
    }

    public int GetDelayMs() => 2000;
}

/// <summary>
/// Mixed - комбинация запросов с верификацией
/// </summary>
public class MixedPattern : ILoadPattern
{
    private readonly Random _random = new();

    public async Task<VerificationResult> ExecuteWithVerificationAsync(NpgsqlConnection connection)
    {
        var queryType = _random.Next(3);

        if (queryType == 0)
        {
            // Простой SELECT
            await using var cmd = new NpgsqlCommand("SELECT 1", connection);
            var result = await cmd.ExecuteScalarAsync();
            return (result is int i && i == 1)
                ? VerificationResult.Success()
                : VerificationResult.Fail($"SELECT 1 вернул {result}");
        }
        else if (queryType == 1)
        {
            // Счётчик баз данных
            await using var cmd = new NpgsqlCommand("SELECT COUNT(*) FROM pg_database", connection);
            var result = await cmd.ExecuteScalarAsync();
            return (result is long count && count > 0)
                ? VerificationResult.Success()
                : VerificationResult.Fail($"pg_database count = {result}");
        }
        else
        {
            // Sleep
            await using var cmd = new NpgsqlCommand("SELECT pg_sleep(0.01)", connection);
            await cmd.ExecuteScalarAsync();
            return VerificationResult.Success();
        }
    }

    public int GetDelayMs() => Random.Shared.Next(50, 500);
}

/// <summary>
/// Burst - всплески с верификацией
/// </summary>
public class BurstPattern : ILoadPattern
{
    private int _counter;

    public async Task<VerificationResult> ExecuteWithVerificationAsync(NpgsqlConnection connection)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1", connection);
        var result = await cmd.ExecuteScalarAsync();

        return (result is int i && i == 1)
            ? VerificationResult.Success()
            : VerificationResult.Fail($"Burst: ожидали 1, получили {result}");
    }

    public int GetDelayMs()
    {
        _counter++;
        return (_counter % 10 == 0) ? 10 : 1000;
    }
}

/// <summary>
/// Transaction - транзакции с верификацией
/// </summary>
public class TransactionPattern : ILoadPattern
{
    public async Task<VerificationResult> ExecuteWithVerificationAsync(NpgsqlConnection connection)
    {
        await using var tx = await connection.BeginTransactionAsync();

        int sum = 0;
        for (int i = 1; i <= 5; i++)
        {
            await using var cmd = new NpgsqlCommand($"SELECT {i}", connection, tx);
            var result = await cmd.ExecuteScalarAsync();
            if (result is int val) sum += val;
        }

        await tx.CommitAsync();

        // Сумма 1+2+3+4+5 = 15
        return (sum == 15)
            ? VerificationResult.Success()
            : VerificationResult.Fail($"Transaction sum ожидали 15, получили {sum}");
    }

    public int GetDelayMs() => 300;
}
