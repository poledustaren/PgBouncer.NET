using Npgsql;
using System.Diagnostics;

Console.WriteLine("=== Npgsql Test via PgBouncer.NET ===");
Console.WriteLine();

var connectionString = "Host=127.0.0.1;Port=6432;Database=postgres;Username=postgres;Password=123;";

var successCount = 0;
var errorCount = 0;
var errors = new List<string>();

for (int i = 0; i < 10; i++)
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        Console.WriteLine($"[{i}] Connected!");

        // Simple query
        await using (var cmd = new NpgsqlCommand("SELECT @val", conn))
        {
            cmd.Parameters.AddWithValue("val", i);
            var result = await cmd.ExecuteScalarAsync();
            Console.WriteLine($"[{i}] Simple query result: {result}");
        }

        // Prepared statement (this was causing the portal error)
        await using (var cmd = new NpgsqlCommand("SELECT @val * 2", conn))
        {
            cmd.Parameters.AddWithValue("val", i);
            await cmd.PrepareAsync(); // This creates a named statement/portal
            var result = await cmd.ExecuteScalarAsync();
            Console.WriteLine($"[{i}] Prepared statement result: {result}");
        }

        successCount++;
    }
    catch (Exception ex)
    {
        errorCount++;
        errors.Add($"[{i}] ERROR: {ex.Message}");
        Console.WriteLine($"[{i}] ERROR: {ex.Message}");
    }
}

Console.WriteLine();
Console.WriteLine($"=== Results ===");
Console.WriteLine($"Success: {successCount}/10");
Console.WriteLine($"Errors: {errorCount}/10");

if (errors.Count > 0)
{
    Console.WriteLine();
    Console.WriteLine("Sample errors:");
    foreach (var e in errors.Take(3))
    {
        Console.WriteLine(e);
    }
}
