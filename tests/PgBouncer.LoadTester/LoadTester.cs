using Npgsql;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace PgBouncer.LoadTester;

/// <summary>
/// High-performance load tester for PgBouncer - tests 1000+ concurrent connections
/// </summary>
public class LoadTester
{
    private readonly string _connectionString;
    private readonly int _totalConnections;
    private readonly int _concurrentConnections;
    private readonly int _queriesPerConnection;

    public LoadTester(
        string connectionString,
        int totalConnections = 1000,
        int concurrentConnections = 100,
        int queriesPerConnection = 10)
    {
        _connectionString = connectionString;
        _totalConnections = totalConnections;
        _concurrentConnections = concurrentConnections;
        _queriesPerConnection = queriesPerConnection;
    }

    public async Task<LoadTestResult> RunAsync(CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"=== PgBouncer Load Test ===");
        Console.WriteLine($"Total connections: {_totalConnections}");
        Console.WriteLine($"Concurrent connections: {_concurrentConnections}");
        Console.WriteLine($"Queries per connection: {_queriesPerConnection}");
        Console.WriteLine();

        var results = new ConcurrentBag<ConnectionResult>();
        var semaphore = new SemaphoreSlim(_concurrentConnections);
        var stopwatch = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, _totalConnections).Select(async i =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                var result = await ExecuteConnectionAsync(i, cancellationToken);
                results.Add(result);

                if (i % 100 == 0)
                {
                    Console.WriteLine($"Progress: {i}/{_totalConnections} connections completed");
                }
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks);
        stopwatch.Stop();

        return AnalyzeResults(results, stopwatch.Elapsed);
    }

    private async Task<ConnectionResult> ExecuteConnectionAsync(int id, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        var success = false;
        var error = string.Empty;
        int queriesExecuted = 0;

        try
        {
            if (id % 10 == 0) Console.WriteLine($"[Connection {id}] Starting...");
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            for (int i = 0; i < _queriesPerConnection; i++)
            {
                await using var cmd = new NpgsqlCommand($"SELECT {id}, {i}", conn);
                await cmd.ExecuteScalarAsync(cancellationToken);
                queriesExecuted++;
            }

            success = true;
        }
        catch (Exception ex)
        {
            error = $"{ex.GetType().Name}: {ex.Message}";
            Console.WriteLine($"[Connection {id}] FAILED: {error}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"[Connection {id}] Inner: {ex.InnerException.Message}");
            }
        }

        sw.Stop();

        return new ConnectionResult
        {
            Id = id,
            Success = success,
            Duration = sw.Elapsed,
            Error = error,
            QueriesExecuted = queriesExecuted
        };
    }

    private LoadTestResult AnalyzeResults(ConcurrentBag<ConnectionResult> results, TimeSpan totalDuration)
    {
        var resultList = results.ToList();
        var successful = resultList.Where(r => r.Success).ToList();
        var failed = resultList.Where(r => !r.Success).ToList();

        var avgConnectionTime = successful.Any()
            ? TimeSpan.FromTicks((long)successful.Average(r => r.Duration.Ticks))
            : TimeSpan.Zero;

        var maxConnectionTime = successful.Any()
            ? successful.Max(r => r.Duration)
            : TimeSpan.Zero;

        var minConnectionTime = successful.Any()
            ? successful.Min(r => r.Duration)
            : TimeSpan.Zero;

        var totalQueries = resultList.Sum(r => r.QueriesExecuted);
        var queriesPerSecond = totalDuration.TotalSeconds > 0
            ? totalQueries / totalDuration.TotalSeconds
            : 0;

        var connectionsPerSecond = totalDuration.TotalSeconds > 0
            ? resultList.Count / totalDuration.TotalSeconds
            : 0;

        return new LoadTestResult
        {
            TotalConnections = resultList.Count,
            SuccessfulConnections = successful.Count,
            FailedConnections = failed.Count,
            TotalQueries = totalQueries,
            TotalDuration = totalDuration,
            AvgConnectionTime = avgConnectionTime,
            MinConnectionTime = minConnectionTime,
            MaxConnectionTime = maxConnectionTime,
            QueriesPerSecond = queriesPerSecond,
            ConnectionsPerSecond = connectionsPerSecond,
            ErrorDetails = failed.Take(10).Select(r => $"Connection {r.Id}: {r.Error}").ToList()
        };
    }
}

public class ConnectionResult
{
    public int Id { get; set; }
    public bool Success { get; set; }
    public TimeSpan Duration { get; set; }
    public string Error { get; set; } = string.Empty;
    public int QueriesExecuted { get; set; }
}

public class LoadTestResult
{
    public int TotalConnections { get; set; }
    public int SuccessfulConnections { get; set; }
    public int FailedConnections { get; set; }
    public int TotalQueries { get; set; }
    public TimeSpan TotalDuration { get; set; }
    public TimeSpan AvgConnectionTime { get; set; }
    public TimeSpan MinConnectionTime { get; set; }
    public TimeSpan MaxConnectionTime { get; set; }
    public double QueriesPerSecond { get; set; }
    public double ConnectionsPerSecond { get; set; }
    public List<string> ErrorDetails { get; set; } = new();

    public double SuccessRate => TotalConnections > 0
        ? (double)SuccessfulConnections / TotalConnections * 100
        : 0;

    public void PrintReport()
    {
        Console.WriteLine();
        Console.WriteLine("=== Load Test Results ===");
        Console.WriteLine($"Total connections: {TotalConnections}");
        Console.WriteLine($"Successful: {SuccessfulConnections} ({SuccessRate:F2}%)");
        Console.WriteLine($"Failed: {FailedConnections}");
        Console.WriteLine($"Total queries: {TotalQueries}");
        Console.WriteLine($"Total duration: {TotalDuration.TotalSeconds:F2}s");
        Console.WriteLine($"Connections/sec: {ConnectionsPerSecond:F2}");
        Console.WriteLine($"Queries/sec: {QueriesPerSecond:F2}");
        Console.WriteLine($"Avg connection time: {AvgConnectionTime.TotalMilliseconds:F2}ms");
        Console.WriteLine($"Min connection time: {MinConnectionTime.TotalMilliseconds:F2}ms");
        Console.WriteLine($"Max connection time: {MaxConnectionTime.TotalMilliseconds:F2}ms");

        if (ErrorDetails.Any())
        {
            Console.WriteLine();
            Console.WriteLine("=== Sample Errors ===");
            foreach (var error in ErrorDetails)
            {
                Console.WriteLine(error);
            }
        }
    }
}
