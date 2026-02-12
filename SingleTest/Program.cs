using Npgsql;
using System.Diagnostics;

Console.WriteLine("=== Single Connection Test to PgBouncer V2 ===");

var connString = "Host=127.0.0.1;Port=6434;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";

var sw = Stopwatch.StartNew();
try
{
    Console.WriteLine("Creating connection...");
    await using var conn = new NpgsqlConnection(connString);
    
    Console.WriteLine("Opening connection...");
    await conn.OpenAsync();
    Console.WriteLine($"✓ Connected in {sw.ElapsedMilliseconds}ms");
    
    Console.WriteLine("Executing query...");
    await using var cmd = new NpgsqlCommand("SELECT 42 as answer", conn);
    var result = await cmd.ExecuteScalarAsync();
    Console.WriteLine($"✓ Result: {result}");
    
    Console.WriteLine("Closing connection...");
}
catch (Exception ex)
{
    Console.WriteLine($"✗ Error after {sw.ElapsedMilliseconds}ms:");
    Console.WriteLine($"  Type: {ex.GetType().Name}");
    Console.WriteLine($"  Message: {ex.Message}");
    if (ex.InnerException != null)
    {
        Console.WriteLine($"  Inner: {ex.InnerException.Message}");
    }
    Console.WriteLine($"  Stack: {ex.StackTrace?.Split('\n').FirstOrDefault()}");
}
