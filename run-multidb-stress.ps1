# Мульти-базовый стресс-тест через PgBouncer.NET
# Запускает параллельные нагрузочные тесты на все 10 баз данных

param(
    [string]$PgBouncerHost = "localhost",
    [int]$PgBouncerPort = 6432,
    [int]$DatabaseCount = 10,
    [string]$Pattern = "mixed",
    [int]$DurationSeconds = 60,
    [int]$ConnectionsPerDb = 10
)

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║   PgBouncer.NET - Мульти-базовый стресс-тест             ║" -ForegroundColor Magenta
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Magenta
Write-Host ""

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$loadTesterPath = Join-Path $scriptDir "publish\LoadTester\PgBouncer.LoadTester.exe"

if (-not (Test-Path $loadTesterPath)) {
    Write-Host "ОШИБКА: LoadTester не найден. Запустите .\install.ps1" -ForegroundColor Red
    exit 1
}

Write-Host "Конфигурация:" -ForegroundColor Cyan
Write-Host "  PgBouncer:        $PgBouncerHost`:$PgBouncerPort"
Write-Host "  Баз данных:       $DatabaseCount"
Write-Host "  Паттерн:          $Pattern"
Write-Host "  Длительность:     $DurationSeconds сек"
Write-Host "  Соединений/БД:    $ConnectionsPerDb"
Write-Host "  Всего соединений: $($DatabaseCount * $ConnectionsPerDb)"
Write-Host ""

# Запускаем тесты для каждой базы
$jobs = @()
$startTime = Get-Date

Write-Host "Запуск тестов на $DatabaseCount баз..." -ForegroundColor Yellow
Write-Host ""

for ($i = 1; $i -le $DatabaseCount; $i++) {
    $db = "testdb$i"
    $user = "testuser$i"
    $pass = "testpass$i"
    
    Write-Host "  [DB $i] Запуск теста для $db..." -ForegroundColor Gray
    
    $job = Start-Job -ScriptBlock {
        param($exe, $host_, $port, $db, $user, $pass, $pattern, $duration, $connections)
        
        & $exe `
            --host $host_ `
            --port $port `
            --database $db `
            --user $user `
            --password $pass `
            -p $pattern `
            -d $duration `
            -c $connections 2>&1
            
    } -ArgumentList $loadTesterPath, $PgBouncerHost, $PgBouncerPort, $db, $user, $pass, $Pattern, $DurationSeconds, $ConnectionsPerDb
    
    $jobs += @{
        Job      = $job
        Database = $db
        Index    = $i
    }
}

Write-Host ""
Write-Host "Все тесты запущены. Ожидание завершения..." -ForegroundColor Yellow
Write-Host ""

# Прогресс-бар
$totalTime = $DurationSeconds + 10 # небольшой запас
$progressInterval = 2

for ($elapsed = 0; $elapsed -lt $totalTime; $elapsed += $progressInterval) {
    $completed = ($jobs | Where-Object { $_.Job.State -eq 'Completed' }).Count
    $running = $jobs.Count - $completed
    
    $percent = [math]::Min(100, ($elapsed / $DurationSeconds) * 100)
    $bar = "#" * [int]($percent / 2)
    $empty = " " * (50 - [int]($percent / 2))
    
    Write-Host "`r  [$bar$empty] $([int]$percent)% | Завершено: $completed/$($jobs.Count)" -NoNewline
    
    if ($running -eq 0) {
        break
    }
    
    Start-Sleep -Seconds $progressInterval
}

Write-Host ""
Write-Host ""

# Собираем результаты
$results = @()
$totalQueries = 0
$totalErrors = 0

Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "                     РЕЗУЛЬТАТЫ ТЕСТОВ                     " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

foreach ($item in $jobs) {
    $output = Receive-Job -Job $item.Job -Wait
    Remove-Job -Job $item.Job
    
    # Парсим результаты (упрощённо)
    $qps = 0
    $errors = 0
    
    if ($output -match "QPS:\s*([\d.]+)") {
        $qps = [double]$Matches[1]
    }
    if ($output -match "Errors:\s*(\d+)") {
        $errors = [int]$Matches[1]
    }
    
    $totalQueries += $qps * $DurationSeconds
    $totalErrors += $errors
    
    $status = if ($errors -eq 0) { "✓" } else { "!" }
    $color = if ($errors -eq 0) { "Green" } else { "Yellow" }
    
    Write-Host "  $status $($item.Database): " -NoNewline -ForegroundColor $color
    Write-Host "$([int]$qps) QPS" -NoNewline
    if ($errors -gt 0) {
        Write-Host " ($errors ошибок)" -ForegroundColor Yellow -NoNewline
    }
    Write-Host ""
}

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host "                      ОБЩАЯ СТАТИСТИКА                     " -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Время выполнения:  $([int]$duration.TotalSeconds) сек"
Write-Host "  Всего запросов:    ~$([int]$totalQueries)"
Write-Host "  Всего ошибок:      $totalErrors"
Write-Host "  Суммарный QPS:     ~$([int]($totalQueries / $DurationSeconds))"
Write-Host ""

if ($totalErrors -eq 0) {
    Write-Host "  ✓ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!" -ForegroundColor Green
}
else {
    Write-Host "  ! Некоторые тесты имели ошибки" -ForegroundColor Yellow
}
Write-Host ""
