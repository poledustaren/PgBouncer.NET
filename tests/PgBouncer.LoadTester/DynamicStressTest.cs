using Npgsql;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace PgBouncer.LoadTester;

/// <summary>
/// Dynamic stress test that gradually increases load every 30 seconds
/// until failures occur, measuring performance metrics
/// </summary>
public class DynamicStressTest
{
    private readonly string _connectionString;
    private readonly List<TestPhase> _phases = new();
    private readonly ConcurrentBag<MetricSnapshot> _metrics = new();
    
    public DynamicStressTest(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<StressTestReport> RunAsync(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        Console.WriteLine("║    PgBouncer Dynamic Stress Test                         ║");
        Console.WriteLine("║    Increasing load every 30 seconds...                   ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        int phaseNumber = 1;
        int concurrentConnections = 10;
        int totalConnections = 100;
        bool continueTesting = true;

        while (continueTesting && !cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine($"\n{'='.Repeat(60)}");
            Console.WriteLine($"PHASE {phaseNumber}: Testing with {concurrentConnections} concurrent, {totalConnections} total");
            Console.WriteLine($"{'='.Repeat(60)}");

            var phaseResult = await RunPhaseAsync(
                phaseNumber,
                concurrentConnections,
                totalConnections,
                TimeSpan.FromSeconds(30),
                cancellationToken);

            _phases.Add(phaseResult);
            
            // Check if we should continue
            if (phaseResult.SuccessRate < 80 || phaseResult.AvgLatencyMs > 1000)
            {
                Console.WriteLine($"\n⚠️  PERFORMANCE DEGRADATION DETECTED!");
                Console.WriteLine($"   Success rate: {phaseResult.SuccessRate:F1}%");
                Console.WriteLine($"   Average latency: {phaseResult.AvgLatencyMs:F0}ms");
                continueTesting = false;
            }
            else if (phaseNumber >= 10)
            {
                Console.WriteLine($"\n✓ Maximum phases reached (10)");
                continueTesting = false;
            }
            else
            {
                // Increase load for next phase
                concurrentConnections = (int)(concurrentConnections * 1.5);
                totalConnections = (int)(totalConnections * 1.5);
                phaseNumber++;
                
                Console.WriteLine($"\n✓ Phase {phaseNumber - 1} completed successfully");
                Console.WriteLine($"  Increasing load to {concurrentConnections} concurrent...");
                await Task.Delay(2000, cancellationToken); // Brief pause between phases
            }
        }

        return GenerateReport();
    }

    private async Task<TestPhase> RunPhaseAsync(
        int phaseNumber,
        int concurrentConnections,
        int totalConnections,
        TimeSpan duration,
        CancellationToken cancellationToken)
    {
        var results = new ConcurrentBag<DynamicConnectionResult>();
        var semaphore = new SemaphoreSlim(concurrentConnections);
        var phaseStopwatch = Stopwatch.StartNew();
        var metricsTimer = new Timer(_ => CaptureMetrics(phaseNumber, results), null, TimeSpan.Zero, TimeSpan.FromSeconds(5));

        var tasks = Enumerable.Range(0, totalConnections).Select(async i =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                var result = await ExecuteConnectionAsync(i, cancellationToken);
                results.Add(result);
                if (!result.Success)
                {
                    Console.WriteLine($"   [Error #{i}] {result.ErrorType}: {result.ErrorMessage}");
                }
            }
            finally
            {
                semaphore.Release();
            }
        });

        // Run for specified duration or until all complete
        var timeoutTask = Task.Delay(duration, cancellationToken);
        var completionTask = Task.WhenAll(tasks);
        
        await Task.WhenAny(completionTask, timeoutTask);
        
        phaseStopwatch.Stop();
        await metricsTimer.DisposeAsync();

        // Calculate phase metrics
        var resultList = results.ToList();
        var successful = resultList.Where(r => r.Success).ToList();
        
        return new TestPhase
        {
            PhaseNumber = phaseNumber,
            ConcurrentConnections = concurrentConnections,
            TotalConnections = totalConnections,
            SuccessfulConnections = successful.Count,
            FailedConnections = resultList.Count - successful.Count,
            SuccessRate = resultList.Count > 0 ? (double)successful.Count / resultList.Count * 100 : 0,
            AvgLatencyMs = successful.Any() ? successful.Average(r => r.Duration.TotalMilliseconds) : 0,
            MinLatencyMs = successful.Any() ? successful.Min(r => r.Duration.TotalMilliseconds) : 0,
            MaxLatencyMs = successful.Any() ? successful.Max(r => r.Duration.TotalMilliseconds) : 0,
            ConnectionsPerSecond = phaseStopwatch.Elapsed.TotalSeconds > 0 
                ? resultList.Count / phaseStopwatch.Elapsed.TotalSeconds 
                : 0,
            TotalQueries = resultList.Sum(r => r.QueriesExecuted),
            QueriesPerSecond = phaseStopwatch.Elapsed.TotalSeconds > 0
                ? resultList.Sum(r => r.QueriesExecuted) / phaseStopwatch.Elapsed.TotalSeconds
                : 0,
            Duration = phaseStopwatch.Elapsed,
            ErrorTypes = resultList.Where(r => !r.Success)
                .GroupBy(r => r.ErrorType)
                .ToDictionary(g => g.Key, g => g.Count())
        };
    }

    private async Task<DynamicConnectionResult> ExecuteConnectionAsync(int id, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        var success = false;
        var errorType = "None";
        var errorMessage = "";
        int queriesExecuted = 0;

        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            // Execute multiple queries
            for (int i = 0; i < 5; i++)
            {
                await using var cmd = new NpgsqlCommand($"SELECT {id}, {i}, pg_sleep(0.001)", conn);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                queriesExecuted++;
            }

            success = true;
        }
        catch (NpgsqlException ex) when (ex.Message.Contains("timeout"))
        {
            errorType = "Timeout";
            errorMessage = ex.Message;
        }
        catch (NpgsqlException ex) when (ex.Message.Contains("portal"))
        {
            errorType = "PortalError";
            errorMessage = ex.Message;
        }
        catch (NpgsqlException ex)
        {
            errorType = "NpgsqlError";
            errorMessage = ex.Message;
        }
        catch (Exception ex)
        {
            errorType = ex.GetType().Name;
            errorMessage = ex.Message;
        }

        sw.Stop();

        return new DynamicConnectionResult
        {
            Id = id,
            Success = success,
            Duration = sw.Elapsed,
            ErrorType = errorType,
            ErrorMessage = errorMessage,
            QueriesExecuted = queriesExecuted
        };
    }

    private void CaptureMetrics(int phaseNumber, ConcurrentBag<DynamicConnectionResult> results)
    {
        var snapshot = results.ToList();
        var successful = snapshot.Where(r => r.Success).ToList();
        
        _metrics.Add(new MetricSnapshot
        {
            PhaseNumber = phaseNumber,
            Timestamp = DateTime.UtcNow,
            TotalConnections = snapshot.Count,
            SuccessfulConnections = successful.Count,
            AvgLatencyMs = successful.Any() ? successful.Average(r => r.Duration.TotalMilliseconds) : 0,
            ActiveConnections = snapshot.Count - successful.Count
        });
    }

    private StressTestReport GenerateReport()
    {
        return new StressTestReport
        {
            Phases = _phases,
            TotalPhases = _phases.Count,
            MaxConcurrentTest = _phases.Any() ? _phases.Max(p => p.ConcurrentConnections) : 0,
            MaxSuccessRate = _phases.Any() ? _phases.Max(p => p.SuccessRate) : 0,
            MinSuccessRate = _phases.Any() ? _phases.Min(p => p.SuccessRate) : 0,
            BreakingPoint = _phases.FirstOrDefault(p => p.SuccessRate < 80)?.ConcurrentConnections ?? 0,
            MetricsOverTime = _metrics.ToList()
        };
    }
}

public class TestPhase
{
    public int PhaseNumber { get; set; }
    public int ConcurrentConnections { get; set; }
    public int TotalConnections { get; set; }
    public int SuccessfulConnections { get; set; }
    public int FailedConnections { get; set; }
    public double SuccessRate { get; set; }
    public double AvgLatencyMs { get; set; }
    public double MinLatencyMs { get; set; }
    public double MaxLatencyMs { get; set; }
    public double ConnectionsPerSecond { get; set; }
    public int TotalQueries { get; set; }
    public double QueriesPerSecond { get; set; }
    public TimeSpan Duration { get; set; }
    public Dictionary<string, int> ErrorTypes { get; set; } = new();
}

public class DynamicConnectionResult
{
    public int Id { get; set; }
    public bool Success { get; set; }
    public TimeSpan Duration { get; set; }
    public string ErrorType { get; set; } = "None";
    public string ErrorMessage { get; set; } = "";
    public int QueriesExecuted { get; set; }
}

public class MetricSnapshot
{
    public int PhaseNumber { get; set; }
    public DateTime Timestamp { get; set; }
    public int TotalConnections { get; set; }
    public int SuccessfulConnections { get; set; }
    public double AvgLatencyMs { get; set; }
    public int ActiveConnections { get; set; }
}

public class StressTestReport
{
    public List<TestPhase> Phases { get; set; } = new();
    public int TotalPhases { get; set; }
    public int MaxConcurrentTest { get; set; }
    public double MaxSuccessRate { get; set; }
    public double MinSuccessRate { get; set; }
    public int BreakingPoint { get; set; }
    public List<MetricSnapshot> MetricsOverTime { get; set; } = new();

    public void PrintReport()
    {
        Console.WriteLine("\n" + new string('=', 70));
        Console.WriteLine("                    STRESS TEST FINAL REPORT");
        Console.WriteLine(new string('=', 70));
        
        Console.WriteLine($"\n📊 SUMMARY:");
        Console.WriteLine($"   Total Phases Completed: {TotalPhases}");
        Console.WriteLine($"   Maximum Concurrent Connections Tested: {MaxConcurrentTest}");
        Console.WriteLine($"   Best Success Rate: {MaxSuccessRate:F1}%");
        Console.WriteLine($"   Worst Success Rate: {MinSuccessRate:F1}%");
        if (BreakingPoint > 0)
        {
            Console.WriteLine($"   ⚠️  Breaking Point: {BreakingPoint} concurrent connections");
        }
        else
        {
            Console.WriteLine($"   ✓ No breaking point reached within test limits");
        }

        Console.WriteLine("\n📈 PHASE DETAILS:");
        Console.WriteLine(new string('─', 70));
        Console.WriteLine(string.Format("{0,-6} {1,-12} {2,-10} {3,-10} {4,-10} {5,-12}", "Phase", "Concurrent", "Success %", "Avg Ms", "Conn/s", "Queries/s"));
        Console.WriteLine(new string('─', 70));
        
        foreach (var phase in Phases)
        {
            var marker = phase.SuccessRate < 80 ? "⚠️" : phase.SuccessRate == Phases.Max(p => p.SuccessRate) ? "⭐" : " ";
            Console.WriteLine($"{marker}{phase.PhaseNumber,-5} {phase.ConcurrentConnections,-12} {phase.SuccessRate,-10:F1} {phase.AvgLatencyMs,-10:F0} {phase.ConnectionsPerSecond,-10:F1} {phase.QueriesPerSecond,-12:F1}");
        }

        Console.WriteLine($"{'─'.Repeat(70)}");

        // Error analysis
        Console.WriteLine($"\n🔍 ERROR ANALYSIS:");
        var allErrors = Phases.SelectMany(p => p.ErrorTypes)
            .GroupBy(x => x.Key)
            .ToDictionary(g => g.Key, g => g.Sum(x => x.Value));
        
        if (allErrors.Any())
        {
            foreach (var error in allErrors.OrderByDescending(e => e.Value))
            {
                Console.WriteLine($"   {error.Key}: {error.Value} occurrences");
            }
        }
        else
        {
            Console.WriteLine($"   ✓ No errors recorded");
        }

        Console.WriteLine($"\n📉 PERFORMANCE TREND:");
        if (Phases.Count >= 2)
        {
            var first = Phases.First();
            var last = Phases.Last();
            var latencyIncrease = ((last.AvgLatencyMs - first.AvgLatencyMs) / first.AvgLatencyMs) * 100;
            var successChange = last.SuccessRate - first.SuccessRate;
            
            Console.WriteLine($"   Latency increased by: {latencyIncrease:F0}% (from {first.AvgLatencyMs:F0}ms to {last.AvgLatencyMs:F0}ms)");
            Console.WriteLine($"   Success rate change: {successChange:F1}% (from {first.SuccessRate:F1}% to {last.SuccessRate:F1}%)");
        }

        Console.WriteLine($"\n{'='.Repeat(70)}");
        Console.WriteLine("                         END OF REPORT");
        Console.WriteLine($"{'='.Repeat(70)}\n");
    }
}

public static class StringExtensions
{
    public static string Repeat(this char character, int count)
    {
        return new string(character, count);
    }
}
