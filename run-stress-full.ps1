# PgBouncer.NET - Полный стресс-тест (10 минут) со сбором метрик
# Собирает метрики системы до, во время и после теста

param(
    [string]$PgBouncerHost = "localhost",
    [int]$PgBouncerPort = 6432,
    [int]$DashboardPort = 5080,
    [int]$DatabaseCount = 10,
    [int]$DurationMinutes = 10,
    [int]$ConnectionsPerDb = 20,
    [string]$OutputDir = ".\stress-results"
)

$ErrorActionPreference = "Continue"

# Создаём директорию для результатов
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$resultDir = Join-Path $OutputDir $timestamp
New-Item -ItemType Directory -Path $resultDir -Force | Out-Null

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║   PgBouncer.NET - ПОЛНЫЙ СТРЕСС-ТЕСТ ($DurationMinutes мин)              ║" -ForegroundColor Magenta
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Magenta
Write-Host ""
Write-Host "Конфигурация:" -ForegroundColor Cyan
Write-Host "  PgBouncer:        $PgBouncerHost`:$PgBouncerPort"
Write-Host "  Баз данных:       $DatabaseCount"
Write-Host "  Соединений/БД:    $ConnectionsPerDb"
Write-Host "  Всего соединений: $($DatabaseCount * $ConnectionsPerDb)"
Write-Host "  Длительность:     $DurationMinutes минут"
Write-Host "  Результаты:       $resultDir"
Write-Host ""

# Функция сбора системных метрик
function Get-SystemMetrics {
    # CPU (с обработкой ошибок)
    $cpu = 0
    try {
        $counter = Get-Counter '\Processor(_Total)\% Processor Time' -ErrorAction Stop
        if ($counter -and $counter.CounterSamples -and $counter.CounterSamples.Count -gt 0) {
            $cpu = $counter.CounterSamples[0].CookedValue
        }
    } catch {
        # Альтернативный метод через WMI
        try {
            $cpu = (Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue | Measure-Object -Property LoadPercentage -Average).Average
        } catch { $cpu = 0 }
    }
    
    $mem = Get-Process | Where-Object { $_.ProcessName -like "*PgBouncer*" } | Measure-Object WorkingSet64 -Sum
    $memMB = [math]::Round($mem.Sum / 1MB, 2)
    
    # Получаем метрики из API
    try {
        $stats = Invoke-RestMethod -Uri "http://${PgBouncerHost}:${DashboardPort}/api/stats/summary" -TimeoutSec 5
    } catch {
        $stats = @{ TotalPools = 0; TotalConnections = 0; ActiveConnections = 0; IdleConnections = 0 }
    }
    
    return @{
        Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        CpuPercent = [math]::Round($cpu, 2)
        MemoryMB = $memMB
        TotalPools = $stats.TotalPools
        TotalConnections = $stats.TotalConnections
        ActiveConnections = $stats.ActiveConnections
        IdleConnections = $stats.IdleConnections
    }
}

# ============================================
# ФАЗА 1: Сбор метрик ДО теста
# ============================================
Write-Host "[ФАЗА 1] Сбор baseline метрик (до нагрузки)..." -ForegroundColor Yellow

$baselineMetrics = @()
for ($i = 0; $i -lt 5; $i++) {
    $metrics = Get-SystemMetrics
    $baselineMetrics += $metrics
    Write-Host "  [$i/5] CPU: $($metrics.CpuPercent)%, Memory: $($metrics.MemoryMB) MB, Conn: $($metrics.TotalConnections)"
    Start-Sleep -Seconds 2
}

$baselineMetrics | Export-Csv -Path (Join-Path $resultDir "baseline_metrics.csv") -NoTypeInformation
Write-Host "  Baseline сохранён" -ForegroundColor Green
Write-Host ""

# ============================================
# ФАЗА 2: Запуск стресс-теста
# ============================================
Write-Host "[ФАЗА 2] Запуск стресс-теста на $DurationMinutes минут..." -ForegroundColor Yellow

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$loadTesterPath = Join-Path $scriptDir "publish\LoadTester\PgBouncer.LoadTester.exe"

if (-not (Test-Path $loadTesterPath)) {
    Write-Host "ОШИБКА: LoadTester не найден!" -ForegroundColor Red
    exit 1
}

# Запускаем тесты для каждой базы
$testJobs = @()
$durationSeconds = $DurationMinutes * 60

for ($i = 1; $i -le $DatabaseCount; $i++) {
    $db = "testdb$i"
    $user = "testuser$i"
    $pass = "testpass$i"
    $pattern = @("rapid", "mixed", "transaction", "burst")[$i % 4]
    
    Write-Host "  Запуск теста для $db (паттерн: $pattern)..." -ForegroundColor Gray
    
    $job = Start-Job -ScriptBlock {
        param($exe, $host_, $port, $db, $user, $pass, $pattern, $duration, $connections)
        & $exe --host $host_ --port $port --database $db --user $user --password $pass -p $pattern -d $duration -c $connections 2>&1
    } -ArgumentList $loadTesterPath, $PgBouncerHost, $PgBouncerPort, $db, $user, $pass, $pattern, $durationSeconds, $ConnectionsPerDb
    
    $testJobs += @{ Job = $job; Database = $db; Pattern = $pattern }
}

Write-Host ""
Write-Host "[ФАЗА 2.1] Мониторинг во время теста..." -ForegroundColor Yellow

$runtimeMetrics = @()
$startTime = Get-Date
$endTime = $startTime.AddSeconds($durationSeconds)
$sampleInterval = 10 # секунд

while ((Get-Date) -lt $endTime) {
    $elapsed = ((Get-Date) - $startTime).TotalSeconds
    $remaining = $durationSeconds - $elapsed
    $percent = [math]::Min(100, ($elapsed / $durationSeconds) * 100)
    
    $metrics = Get-SystemMetrics
    $runtimeMetrics += $metrics
    
    $bar = "#" * [int]($percent / 2)
    $empty = " " * (50 - [int]($percent / 2))
    
    Write-Host "`r  [$bar$empty] $([int]$percent)% | CPU: $($metrics.CpuPercent)% | Mem: $($metrics.MemoryMB)MB | Conn: $($metrics.TotalConnections) | Осталось: $([int]$remaining)s" -NoNewline
    
    Start-Sleep -Seconds $sampleInterval
}

Write-Host ""
Write-Host ""

$runtimeMetrics | Export-Csv -Path (Join-Path $resultDir "runtime_metrics.csv") -NoTypeInformation
Write-Host "  Runtime метрики сохранены" -ForegroundColor Green

# ============================================
# ФАЗА 3: Сбор результатов тестов
# ============================================
Write-Host ""
Write-Host "[ФАЗА 3] Сбор результатов тестов..." -ForegroundColor Yellow

$testResults = @()
foreach ($item in $testJobs) {
    $output = Receive-Job -Job $item.Job -Wait
    Remove-Job -Job $item.Job
    
    # Парсим результаты
    $totalQueries = 0
    $qps = 0
    $errors = 0
    $avgLatency = 0
    $p95 = 0
    $p99 = 0
    
    $outputStr = $output -join "`n"
    
    if ($outputStr -match "Всего запросов:\s*(\d+)") { $totalQueries = [int]$Matches[1] }
    if ($outputStr -match "Запросов/сек:\s*([\d.]+)") { $qps = [double]$Matches[1] }
    if ($outputStr -match "Ошибок:\s*(\d+)") { $errors = [int]$Matches[1] }
    if ($outputStr -match "Средняя:\s*([\d.]+)") { $avgLatency = [double]$Matches[1] }
    if ($outputStr -match "p95:\s*([\d.]+)") { $p95 = [double]$Matches[1] }
    if ($outputStr -match "p99:\s*([\d.]+)") { $p99 = [double]$Matches[1] }
    
    $testResults += @{
        Database = $item.Database
        Pattern = $item.Pattern
        TotalQueries = $totalQueries
        QPS = $qps
        Errors = $errors
        AvgLatencyMs = $avgLatency
        P95LatencyMs = $p95
        P99LatencyMs = $p99
    }
    
    Write-Host "  $($item.Database): $totalQueries запросов, $([int]$qps) QPS, $errors ошибок"
}

$testResults | ForEach-Object { [PSCustomObject]$_ } | Export-Csv -Path (Join-Path $resultDir "test_results.csv") -NoTypeInformation

# ============================================
# ФАЗА 4: Сбор метрик ПОСЛЕ теста
# ============================================
Write-Host ""
Write-Host "[ФАЗА 4] Сбор метрик после нагрузки (cooldown)..." -ForegroundColor Yellow

$cooldownMetrics = @()
for ($i = 0; $i -lt 10; $i++) {
    $metrics = Get-SystemMetrics
    $cooldownMetrics += $metrics
    Write-Host "  [$i/10] CPU: $($metrics.CpuPercent)%, Memory: $($metrics.MemoryMB) MB, Conn: $($metrics.TotalConnections)"
    Start-Sleep -Seconds 3
}

$cooldownMetrics | Export-Csv -Path (Join-Path $resultDir "cooldown_metrics.csv") -NoTypeInformation

# ============================================
# ФАЗА 5: Генерация отчёта
# ============================================
Write-Host ""
Write-Host "[ФАЗА 5] Генерация итогового отчёта..." -ForegroundColor Yellow

$totalQueries = ($testResults | Measure-Object -Property TotalQueries -Sum).Sum
$totalErrors = ($testResults | Measure-Object -Property Errors -Sum).Sum
$avgQPS = ($testResults | Measure-Object -Property QPS -Average).Average
$maxCPU = ($runtimeMetrics | Measure-Object -Property CpuPercent -Maximum).Maximum
$avgCPU = ($runtimeMetrics | Measure-Object -Property CpuPercent -Average).Average
$maxMem = ($runtimeMetrics | Measure-Object -Property MemoryMB -Maximum).Maximum
$avgMem = ($runtimeMetrics | Measure-Object -Property MemoryMB -Average).Average
$maxConn = ($runtimeMetrics | Measure-Object -Property TotalConnections -Maximum).Maximum

$report = @"
╔══════════════════════════════════════════════════════════════════════════╗
║                    ОТЧЁТ СТРЕСС-ТЕСТИРОВАНИЯ                             ║
╚══════════════════════════════════════════════════════════════════════════╝

Дата:                   $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Длительность теста:     $DurationMinutes минут
Баз данных:             $DatabaseCount
Соединений на БД:       $ConnectionsPerDb
Всего соединений:       $($DatabaseCount * $ConnectionsPerDb)

═══════════════════════════════════════════════════════════════════════════
                           РЕЗУЛЬТАТЫ НАГРУЗКИ
═══════════════════════════════════════════════════════════════════════════

Всего запросов:         $totalQueries
Всего ошибок:           $totalErrors ($([math]::Round($totalErrors * 100 / [math]::Max($totalQueries, 1), 3))%)
Средний QPS:            $([int]$avgQPS)
Суммарный QPS:          $([int]($avgQPS * $DatabaseCount))

═══════════════════════════════════════════════════════════════════════════
                         ПОТРЕБЛЕНИЕ РЕСУРСОВ
═══════════════════════════════════════════════════════════════════════════

CPU (максимум):         $([int]$maxCPU)%
CPU (среднее):          $([int]$avgCPU)%
Память (максимум):      $maxMem MB
Память (среднее):       $([int]$avgMem) MB
Соединений (максимум):  $maxConn

═══════════════════════════════════════════════════════════════════════════
                         РЕЗУЛЬТАТЫ ПО БАЗАМ
═══════════════════════════════════════════════════════════════════════════

"@

foreach ($r in $testResults) {
    $report += "  $($r.Database) [$($r.Pattern)]: $($r.TotalQueries) запросов, $([int]$r.QPS) QPS, p95=$($r.P95LatencyMs)ms`n"
}

$report += @"

═══════════════════════════════════════════════════════════════════════════
                              ФАЙЛЫ ОТЧЁТА
═══════════════════════════════════════════════════════════════════════════

  baseline_metrics.csv  - метрики до нагрузки
  runtime_metrics.csv   - метрики во время теста
  cooldown_metrics.csv  - метрики после нагрузки
  test_results.csv      - результаты по каждой БД
  report.txt            - этот отчёт

═══════════════════════════════════════════════════════════════════════════
"@

$report | Out-File -FilePath (Join-Path $resultDir "report.txt") -Encoding UTF8
Write-Host $report

Write-Host ""
Write-Host "Результаты сохранены в: $resultDir" -ForegroundColor Green
Write-Host ""
