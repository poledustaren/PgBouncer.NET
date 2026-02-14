# Quick comparison test - 10 connections, 5 queries each
$ErrorActionPreference = "SilentlyContinue"

Write-Host "=== QUICK COMPARISON TEST ===" -ForegroundColor Cyan
Write-Host ""

function Test-Pooler {
    param($Port, $Name)
    
    $success = 0
    $fail = 0
    $total = 10
    
    1..$total | ForEach-Object -ThrottleLimit 10 -Parallel {
        $connStr = "Host=localhost;Port=$using:Port;Database=postgres;Username=postgres;Password=123;Timeout=5;Pooling=false"
        try {
            $conn = [Npgsql.NpgsqlConnection]::new($connStr)
            $conn.Open()
            1..5 | ForEach-Object {
                $cmd = $conn.CreateCommand()
                $cmd.CommandText = "SELECT 1"
                $cmd.ExecuteNonQuery() | Out-Null
            }
            $conn.Close()
            1
        } catch {
            0
        }
    } | ForEach-Object {
        if ($_ -eq 1) { $script:success++ } else { $script:fail++ }
    }
    
    $pct = [math]::Round(($success / $total) * 100, 1)
    Write-Host "$Name : $success/$total ($pct%)" -ForegroundColor $(if($pct -ge 95){"Green"}elseif($pct -ge 80){"Yellow"}else{"Red"})
    return $pct
}

# Load Npgsql
Add-Type -Path "C:\Projects\pgbouncer.net\tests\PgBouncer.LoadTester\bin\Debug\net8.0\Npgsql.dll"

Write-Host "Testing PgBouncer.NET (port 6432)..."
$net = Test-Pooler -Port 6432 -Name "PgBouncer.NET"

Write-Host ""
Write-Host "Testing Original PgBouncer (port 6433)..."
$orig = Test-Pooler -Port 6433 -Name "Original PgBouncer"

Write-Host ""
Write-Host "=== RESULT ===" -ForegroundColor Cyan
Write-Host "PgBouncer.NET:  $net%"
Write-Host "Original:       $orig%"
Write-Host "Gap:            $([math]::Round($orig - $net, 1))%"
