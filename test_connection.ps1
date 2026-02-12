Add-Type -Path "$npgsqlPath"
$connString = "Host=127.0.0.1;Port=6434;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30"
Write-Host "Connecting to PgBouncer..."
try {
    $conn = New-Object Npgsql.NpgsqlConnection($connString)
    $conn.Open()
    Write-Host "CONNECTED!"
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = "SELECT 42"
    $result = $cmd.ExecuteScalar()
    Write-Host "Result: $result"
    $conn.Close()
} catch {
    Write-Host "ERROR: $_"
    Write-Host "Stack: $($_.ScriptStackTrace)"
}
