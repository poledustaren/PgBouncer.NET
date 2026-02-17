using Npgsql;
using System.Diagnostics;
using System.Data;

Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║                PgBouncer.NET Stress Tester               ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝");

string connString = "Host=127.0.0.1;Port=6432;Database=postgres;Username=postgres;Pooling=false";

var scenarios = new List<(string Name, Func<int, CancellationToken, Task> Action, int Weight)>
{
    ("Notification Worker", RunNotificationWorker, 30),
    ("Log Collector", RunLogCollector, 15),
    ("Report Generator", RunReportGenerator, 10),
    ("Session Manager", RunSessionManager, 25),
    ("EF Core Transaction Test", RunTransactionTest, 10),
    ("Extended Query Protocol", RunExtendedQueryTest, 10)
};

int totalRequests = 1000;
int concurrentConnections = 10;

var stats = scenarios.ToDictionary(s => s.Name, _ => (Success: 0, Failure: 0, Errors: new List<string>()));

var sw = Stopwatch.StartNew();

Console.WriteLine($"Starting {totalRequests} requests across {scenarios.Count} scenarios...");

await Parallel.ForEachAsync(Enumerable.Range(0, totalRequests), new ParallelOptions { MaxDegreeOfParallelism = concurrentConnections }, async (i, ct) =>
{
    // Pick a scenario based on weights
    int roll = Random.Shared.Next(100);
    int currentWeight = 0;
    var scenario = scenarios.Last();
    foreach (var s in scenarios)
    {
        currentWeight += s.Weight;
        if (roll < currentWeight)
        {
            scenario = s;
            break;
        }
    }

    try
    {
        await scenario.Action(i, ct);
        lock (stats)
        {
            var s = stats[scenario.Name];
            stats[scenario.Name] = (s.Success + 1, s.Failure, s.Errors);
        }
    }
    catch (Exception ex)
    {
        lock (stats)
        {
            var s = stats[scenario.Name];
            s.Errors.Add(ex.Message);
            stats[scenario.Name] = (s.Success, s.Failure + 1, s.Errors);
        }
    }
    
    if (i % 50 == 0) Console.Write(".");
});

sw.Stop();

Console.WriteLine("\n\nResults:");
Console.WriteLine("┌──────────────────────────┬────────┬────────┬────────┬──────────────────────────────┐");
Console.WriteLine("│ Scenario                 │ Success│ Failure│ Total  │ Last Error                   │");
Console.WriteLine("├──────────────────────────┼────────┼────────┼────────┼──────────────────────────────┤");

foreach (var scenario in scenarios)
{
    var s = stats[scenario.Name];
string lastErr = s.Errors.Count > 0 ? s.Errors.Last() : "-";
    if (lastErr.Length > 80) lastErr = lastErr[..77] + "...";
    
    Console.WriteLine($"│ {scenario.Name,-25} │ {s.Success,6} │ {s.Failure,6} │ {s.Success + s.Failure,6} │ {lastErr,-28} │");
}
Console.WriteLine("└──────────────────────────┴────────┴────────┴────────┴──────────────────────────────┘");
Console.WriteLine($"Finished in {sw.ElapsedMilliseconds}ms");

// Scenarios implementation

async Task RunNotificationWorker(int id, CancellationToken ct)
{
    // Быстрые коннекты, один запрос и выход
    using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync(ct);
    using var cmd = new NpgsqlCommand("SELECT 1", conn);
    await cmd.ExecuteScalarAsync(ct);
}

async Task RunLogCollector(int id, CancellationToken ct)
{
    // Вставка данных
    using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync(ct);
    using var cmd = new NpgsqlCommand("SELECT current_timestamp", conn);
    await cmd.ExecuteScalarAsync(ct);
}

async Task RunReportGenerator(int id, CancellationToken ct)
{
    // Имитация долгого запроса
    using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync(ct);
    using var cmd = new NpgsqlCommand("SELECT pg_sleep(0.1)", conn);
    await cmd.ExecuteScalarAsync(ct);
}

async Task RunSessionManager(int id, CancellationToken ct)
{
    // Простое чтение
    using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync(ct);
    using var cmd = new NpgsqlCommand("SHOW client_encoding", conn);
    await cmd.ExecuteScalarAsync(ct);
}

async Task RunTransactionTest(int id, CancellationToken ct)
{
    // Тест транзакций (Begin/Commit)
    using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync(ct);
    using var tx = await conn.BeginTransactionAsync(ct);
    using var cmd = new NpgsqlCommand("SELECT 1", conn, tx);
    await cmd.ExecuteScalarAsync(ct);
    await tx.CommitAsync(ct);
}

async Task RunExtendedQueryTest(int id, CancellationToken ct)
{
    // Тест подготовленных выражений (Extended Query Protocol)
    using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync(ct);
    
    // Npgsql по умолчанию использует подготовленные выражения для повторяющихся запросов
    // Или мы можем явно их подготовить.
    using var cmd = new NpgsqlCommand("SELECT @val", conn);
    cmd.Parameters.AddWithValue("val", id);
    await cmd.PrepareAsync(ct); // Это заставляет использовать Named Prepared Statement
    await cmd.ExecuteScalarAsync(ct);
}
