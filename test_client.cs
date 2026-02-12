using Npgsql;
using System.Diagnostics;

Console.WriteLine("=== PgBouncer Transaction Pooling Test ===");
Console.WriteLine();

var connString = "Host=localhost;Port=6434;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";

// Test 1: Simple query
Console.WriteLine("Test 1: Simple query through PgBouncer");
try
{
    await using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync();
    Console.WriteLine("✓ Connected successfully");
    
    await using var cmd = new NpgsqlCommand("SELECT 42 as answer", conn);
    var result = await cmd.ExecuteScalarAsync();
    Console.WriteLine($"✓ Query result: {result}");
}
catch (Exception ex)
{
    Console.WriteLine($"✗ Error: {ex.Message}");
}

Console.WriteLine();

// Test 2: Multiple queries on same connection
Console.WriteLine("Test 2: Multiple queries (Transaction Pooling should reuse backend)");
try
{
    await using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync();
    
    for (int i = 1; i <= 5; i++)
    {
        await using var cmd = new NpgsqlCommand($"SELECT {i} as num", conn);
        var result = await cmd.ExecuteScalarAsync();
        Console.WriteLine($"  Query {i}: {result}");
    }
    Console.WriteLine("✓ All queries executed");
}
catch (Exception ex)
{
    Console.WriteLine($"✗ Error: {ex.Message}");
}

Console.WriteLine();

// Test 3: Check current database
Console.WriteLine("Test 3: Verify database routing");
try
{
    await using var conn = new NpgsqlConnection(connString);
    await conn.OpenAsync();
    
    await using var cmd = new NpgsqlCommand("SELECT current_database()", conn);
    var result = await cmd.ExecuteScalarAsync();
    Console.WriteLine($"✓ Current database: {result}");
}
catch (Exception ex)
{
    Console.WriteLine($"✗ Error: {ex.Message}");
}

Console.WriteLine();
Console.WriteLine("=== Test Complete ===");
