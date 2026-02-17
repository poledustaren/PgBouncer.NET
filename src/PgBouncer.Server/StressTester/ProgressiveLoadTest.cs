using Npgsql;
using System.Diagnostics;
using System.Data;
using System.Collections.Concurrent;

Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║     PgBouncer.NET Progressive Load Stress Tester         ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝");

string connString = "Host=127.0.0.1;Port=6432;Database=postgres;Username=postgres;Pooling=false";

// Configuration
int maxClients = 20;
int connectionsPerClient = 200;
int warmupRequestsPerClient = 10;

Console.WriteLine($"\nConfiguration:");
Console.WriteLine($"  Max Clients: {maxClients}");
Console.WriteLine($"  Connections per client: {connectionsPerClient}");
Console.WriteLine($"  Total connections: {maxClients * connectionsPerClient}");
Console.WriteLine($"\nStarting progressive load test...\n");

var results = new ConcurrentDictionary<int, ClientResult>();
var allStats = new ConcurrentDictionary<string, ScenarioStats>();

// Initialize scenario stats
var scenarioNames = new[] { "Simple Query", "Transaction", "Extended Query" };
foreach (var name in scenarioNames)
{
    allStats[name] = new ScenarioStats();
}

// Progressive load test
for (int clientCount = 1; clientCount <= maxClients; clientCount++)
{
    Console.WriteLine($"\n--- Phase {clientCount}/{maxClients}: {clientCount} clients x {connectionsPerClient} connections = {clientCount * connectionsPerClient} total ---");
    
    var phaseResults = await RunLoadPhase(clientCount, connectionsPerClient, connString, allStats);
    results[clientCount] = phaseResults;
    
    // Print phase results
    Console.WriteLine($"  Duration: {phaseResults.DurationMs}ms");
    Console.WriteLine($"  Success: {phaseResults.SuccessCount}/{phaseResults.TotalCount} ({100.0 * phaseResults.SuccessCount / phaseResults.TotalCount:F1}%)");
    Console.WriteLine($"  Errors: {phaseResults.ErrorCount}");
    Console.WriteLine($"  Avg Latency: {phaseResults.AvgLatencyMs:F1}ms");
    Console.WriteLine($"  Throughput: {phaseResults.Throughput:F0} req/sec");
    
    // Brief pause between phases to let system stabilize
    if (clientCount < maxClients)
    {
        await Task.Delay(500);
    }
}

// Final summary
Console.WriteLine("\n" + new string('═', 60));
Console.WriteLine("FINAL RESULTS SUMMARY");
Console.WriteLine(new string('═', 60));

Console.WriteLine("\nScenario Breakdown:");
foreach (var scenario in allStats)
{
    var stats = scenario.Value;
    var successRate = stats.Total > 0 ? 100.0 * stats.Success / stats.Total : 0;
    Console.WriteLine($"  {scenario.Key,-20} : {stats.Success,5}/{stats.Total,-5} ({successRate,5:F1}%) Avg: {stats.AvgLatencyMs,6:F1}ms");
}

Console.WriteLine("\nLoad Progression:");
Console.WriteLine($"{"Clients",-10} {"Connections",-12} {"Success %",-10} {"Latency(ms)",-12} {"Throughput",-12}");
Console.WriteLine(new string('-', 60));

foreach (var result in results.OrderBy(r => r.Key))
{
    var r = result.Value;
    var successRate = 100.0 * r.SuccessCount / r.TotalCount;
    Console.WriteLine($"{result.Key,-10} {result.Key * connectionsPerClient,-12} {successRate,-10:F1} {r.AvgLatencyMs,-12:F1} {r.Throughput,-12:F0}");
}

Console.WriteLine("\n" + new string('═', 60));

// Test functions
async Task<ClientResult> RunLoadPhase(int clientCount, int connectionsPerClient, string connString, ConcurrentDictionary<string, ScenarioStats> allStats)
{
    var stopwatch = Stopwatch.StartNew();
    var latencies = new ConcurrentBag<double>();
    int successCount = 0;
    int errorCount = 0;
    int totalCount = clientCount * connectionsPerClient;
    
    var options = new ParallelOptions { MaxDegreeOfParallelism = clientCount };
    
    await Parallel.ForEachAsync(
        Enumerable.Range(0, totalCount), 
        options, 
        async (i, ct) =>
    {
        var clientId = i / connectionsPerClient;
        var connectionId = i % connectionsPerClient;
        
        // Determine scenario based on index
        string scenarioName;
        Func<string, int, int, CancellationToken, Task<(bool Success, double LatencyMs)>> testFunc;
        
        int mod = i % 3;
        if (mod == 0)
        {
            scenarioName = "Simple Query";
            testFunc = RunSimpleQueryTest;
        }
        else if (mod == 1)
        {
            scenarioName = "Transaction";
            testFunc = RunTransactionTest;
        }
        else
        {
            scenarioName = "Extended Query";
            testFunc = RunExtendedQueryTest;
        }
        
        var (success, latency) = await testFunc(connString, clientId, connectionId, ct);
        
        latencies.Add(latency);
        
        if (success)
        {
            Interlocked.Increment(ref successCount);
            allStats[scenarioName].Success++;
        }
        else
        {
            Interlocked.Increment(ref errorCount);
            allStats[scenarioName].Errors++;
        }
        allStats[scenarioName].Total++;
        allStats[scenarioName].LatencySum += latency;
    });
    
    stopwatch.Stop();
    
    return new ClientResult
    {
        ClientCount = clientCount,
        TotalCount = totalCount,
        SuccessCount = successCount,
        ErrorCount = errorCount,
        DurationMs = stopwatch.ElapsedMilliseconds,
        AvgLatencyMs = latencies.Any() ? latencies.Average() : 0,
        Throughput = totalCount / (stopwatch.ElapsedMilliseconds / 1000.0)
    };
}

async Task<(bool Success, double LatencyMs)> RunSimpleQueryTest(string connString, int clientId, int connectionId, CancellationToken ct)
{
    var sw = Stopwatch.StartNew();
    try
    {
        using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync(ct);
        using var cmd = new NpgsqlCommand("SELECT 1 as test", conn);
        await cmd.ExecuteScalarAsync(ct);
        sw.Stop();
        return (true, sw.ElapsedMilliseconds);
    }
    catch (Exception ex)
    {
        sw.Stop();
        return (false, sw.ElapsedMilliseconds);
    }
}

async Task<(bool Success, double LatencyMs)> RunTransactionTest(string connString, int clientId, int connectionId, CancellationToken ct)
{
    var sw = Stopwatch.StartNew();
    try
    {
        using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync(ct);
        using var tx = await conn.BeginTransactionAsync(ct);
        using var cmd = new NpgsqlCommand("SELECT pg_sleep(0.001)", conn, tx);
        await cmd.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
        sw.Stop();
        return (true, sw.ElapsedMilliseconds);
    }
    catch (Exception ex)
    {
        sw.Stop();
        return (false, sw.ElapsedMilliseconds);
    }
}

async Task<(bool Success, double LatencyMs)> RunExtendedQueryTest(string connString, int clientId, int connectionId, CancellationToken ct)
{
    var sw = Stopwatch.StartNew();
    try
    {
        using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync(ct);
        
        // Extended query protocol (prepared statement)
        using var cmd = new NpgsqlCommand("SELECT @p1 as client_id, @p2 as conn_id", conn);
        cmd.Parameters.AddWithValue("p1", clientId);
        cmd.Parameters.AddWithValue("p2", connectionId);
        await cmd.PrepareAsync(ct);
        await cmd.ExecuteScalarAsync(ct);
        
        sw.Stop();
        return (true, sw.ElapsedMilliseconds);
    }
    catch (Exception ex)
    {
        sw.Stop();
        return (false, sw.ElapsedMilliseconds);
    }
}

class ClientResult
{
    public int ClientCount { get; set; }
    public int TotalCount { get; set; }
    public int SuccessCount { get; set; }
    public int ErrorCount { get; set; }
    public long DurationMs { get; set; }
    public double AvgLatencyMs { get; set; }
    public double Throughput { get; set; }
}

class ScenarioStats
{
    public int Success { get; set; }
    public int Errors { get; set; }
    public int Total { get; set; }
    public double LatencySum { get; set; }
    public double AvgLatencyMs => Total > 0 ? LatencySum / Total : 0;
}
