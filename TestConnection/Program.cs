using System;
using Npgsql;

class TestConnection
{
    static void Main()
    {
        Console.WriteLine("=== PgBouncer Connection Test ===");
        var connString = "Host=127.0.0.1;Port=6434;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";
        
        try
        {
            Console.WriteLine("Connecting...");
            using var conn = new NpgsqlConnection(connString);
            conn.Open();
            Console.WriteLine("✓ CONNECTED!");
            
            using var cmd = new NpgsqlCommand("SELECT 42", conn);
            var result = cmd.ExecuteScalar();
            Console.WriteLine($"✓ Result: {result}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ ERROR: {ex.Message}");
            Console.WriteLine($"Stack: {ex.StackTrace}");
        }
    }
}
