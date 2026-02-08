# PgBouncer.NET - Запуск полного нагрузочного теста (20 экземпляров × 100 соединений)
# Запускать из корневой директории проекта

param(
    [int]$Instances = 20,
    [int]$ConnectionsPerInstance = 100,
    [string]$Pattern = "mixed",
    [int]$Duration = 60,
    [string]$Host = "localhost",
    [int]$Port = 6432,
    [string]$Database = "fuel"
)

Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║    PgBouncer.NET - Полный нагрузочный тест               ║" -ForegroundColor Cyan
Write-Host "║    $Instances экземпляров × $ConnectionsPerInstance соединений = $($Instances * $ConnectionsPerInstance) коннектов        ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$exePath = ".\publish\LoadTester\PgBouncer.LoadTester.exe"
if (-not (Test-Path $exePath)) {
    $exePath = $null
}

$jobs = @()

Write-Host "Запуск $Instances экземпляров тестера..." -ForegroundColor Yellow
Write-Host ""

for ($i = 1; $i -le $Instances; $i++) {
    Write-Host "  Запуск экземпляра $i/$Instances..." -ForegroundColor Gray
    
    $scriptBlock = {
        param($ExePath, $Connections, $Pattern, $Duration, $Host, $Port, $Database, $InstanceId)
        
        $args = @(
            "--connections", $Connections,
            "--pattern", $Pattern,
            "--duration", $Duration,
            "--host", $Host,
            "--port", $Port,
            "--database", $Database
        )
        
        if ($ExePath) {
            & $ExePath @args 2>&1 | Out-Null
        }
        else {
            Push-Location tests\PgBouncer.LoadTester
            dotnet run -c Release -- @args 2>&1 | Out-Null
            Pop-Location
        }
    }
    
    $job = Start-Job -ScriptBlock $scriptBlock -ArgumentList @($exePath, $ConnectionsPerInstance, $Pattern, $Duration, $Host, $Port, $Database, $i)
    $jobs += $job
    
    # Небольшая задержка между запусками чтобы не перегрузить систему
    Start-Sleep -Milliseconds 200
}

Write-Host ""
Write-Host "Все экземпляры запущены. Ожидание завершения ($Duration сек)..." -ForegroundColor Yellow
Write-Host ""

# Показываем прогресс
$startTime = Get-Date
while ((Get-Job -State Running).Count -gt 0) {
    $elapsed = ((Get-Date) - $startTime).TotalSeconds
    $running = (Get-Job -State Running).Count
    Write-Host "`r  Прошло: $([int]$elapsed)с / ${Duration}с | Активных экземпляров: $running   " -NoNewline -ForegroundColor Cyan
    Start-Sleep -Seconds 2
}

Write-Host ""
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║              ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!                     ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

# Очистка jobs
$jobs | Remove-Job -Force

Write-Host "Для детальной статистики запустите один экземпляр:" -ForegroundColor Yellow
Write-Host "  .\run-loadtest.ps1 -Connections 100 -Pattern mixed -Duration 60" -ForegroundColor White
Write-Host ""
