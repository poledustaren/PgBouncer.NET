using System;
using Npgsql;

class Test {
    static void Main() {
        try {
            var conn = new NpgsqlConnection("Host=127.0.0.1;Port=6434;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=10");
            conn.Open();
            Console.WriteLine("Connected!");
            var cmd = new NpgsqlCommand("SELECT 1", conn);
            var result = cmd.ExecuteScalar();
            Console.WriteLine($"Result: {result}");
            conn.Close();
        } catch (Exception ex) {
            Console.WriteLine($"Error: {ex.Message}");
            Console.WriteLine($"Stack: {ex.StackTrace}");
        }
    }
}
