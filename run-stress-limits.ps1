# PgBouncer.NET - Инкрементальный нагрузочный тест
# Постепенно увеличивает нагрузку для определения пределов системы

param(
    [string]$PgBouncerHost = "localhost",
    [int]$PgBouncerPort = 6432,
    [int]$DashboardPort = 5080,
    [int]$StartConnections = 10,
    [int]$MaxConnections = 500,
    [int]$ConnectionStep = 20,
    [int]$StepDurationSeconds = 30,
    [string]$OutputDir = ".\stress-results"
)

$ErrorActionPreference = "Continue"

# Создаём директорию для результатов
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$resultDir = Join-Path $OutputDir "incremental_$timestamp"
New-Item -ItemType Directory -Path $resultDir -Force | Out-Null

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Red
Write-Host "║   PgBouncer.NET - ИНКРЕМЕНТАЛЬНЫЙ ТЕСТ ПРЕДЕЛОВ          ║" -ForegroundColor Red
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Red
Write-Host ""
Write-Host "Конфигурация:" -ForegroundColor Cyan
Write-Host "  Начальных соединений: $StartConnections"
Write-Host "  Максимум соединений:  $MaxConnections"
Write-Host "  Шаг увеличения:       +$ConnectionStep"
Write-Host "  Время на шаг:         $StepDurationSeconds сек"
Write-Host "  Результаты:           $resultDir"
Write-Host ""

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$loadTesterPath = Join-Path $scriptDir "publish\LoadTester\PgBouncer.LoadTester.exe"

if (-not (Test-Path $loadTesterPath)) {
    Write-Host "ОШИБКА: LoadTester не найден!" -ForegroundColor Red
    exit 1
}

# Функция сбора метрик
function Get-SystemMetrics {
    $cpu = 0
    try {
        $counter = Get-Counter '\Processor(_Total)\% Processor Time' -ErrorAction Stop
        if ($counter -and $counter.CounterSamples -and $counter.CounterSamples.Count -gt 0) {
            $cpu = $counter.CounterSamples[0].CookedValue
        }
    } catch {
        try {
            $cpu = (Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue | Measure-Object -Property LoadPercentage -Average).Average
        } catch { $cpu = 0 }
    }
    
    $mem = Get-Process | Where-Object { $_.ProcessName -like "*PgBouncer*" } | Measure-Object WorkingSet64 -Sum
    $memMB = [math]::Round($mem.Sum / 1MB, 2)
    
    try {
        $stats = Invoke-RestMethod -Uri "http://${PgBouncerHost}:${DashboardPort}/api/stats/summary" -TimeoutSec 3
    } catch {
        $stats = @{ TotalPools = 0; TotalConnections = 0; ActiveConnections = 0; IdleConnections = 0 }
    }
    
    return @{
        CpuPercent = [math]::Round($cpu, 2)
        MemoryMB = $memMB
        TotalConnections = $stats.TotalConnections
        ActiveConnections = $stats.ActiveConnections
    }
}

# Тест для одного уровня нагрузки
function Test-LoadLevel {
    param(
        [int]$Connections,
        [int]$Duration
    )
    
    $job = Start-Job -ScriptBlock {
        param($exe, $host_, $port, $connections, $duration)
        & $exe --host $host_ --port $port --database testdb1 --user testuser1 --password testpass1 -p rapid -d $duration -c $connections 2>&1
    } -ArgumentList $loadTesterPath, $PgBouncerHost, $PgBouncerPort, $Connections, $Duration
    
    # Мониторим во время теста
    $samples = @()
    $startTime = Get-Date
    
    while ($job.State -eq 'Running') {
        $samples += Get-SystemMetrics
        Start-Sleep -Seconds 2
    }
    
    $output = Receive-Job -Job $job -Wait
    Remove-Job -Job $job
    
    # Парсим результаты
    $outputStr = $output -join "`n"
    $totalQueries = 0
    $qps = 0
    $errors = 0
    
    if ($outputStr -match "Всего запросов:\s*(\d+)") { $totalQueries = [int]$Matches[1] }
    if ($outputStr -match "Запросов/сек:\s*([\d.]+)") { $qps = [double]$Matches[1] }
    if ($outputStr -match "Ошибок:\s*(\d+)") { $errors = [int]$Matches[1] }
    
    $avgCpu = ($samples | Measure-Object -Property CpuPercent -Average).Average
    $maxCpu = ($samples | Measure-Object -Property CpuPercent -Maximum).Maximum
    $avgMem = ($samples | Measure-Object -Property MemoryMB -Average).Average
    $maxMem = ($samples | Measure-Object -Property MemoryMB -Maximum).Maximum
    
    return @{
        Connections = $Connections
        TotalQueries = $totalQueries
        QPS = $qps
        Errors = $errors
        AvgCpuPercent = [math]::Round($avgCpu, 1)
        MaxCpuPercent = [math]::Round($maxCpu, 1)
        AvgMemoryMB = [math]::Round($avgMem, 1)
        MaxMemoryMB = [math]::Round($maxMem, 1)
        ErrorRate = [math]::Round($errors * 100 / [math]::Max($totalQueries, 1), 2)
    }
}

# Основной цикл тестирования
$results = @()
$currentConnections = $StartConnections
$stepNumber = 1
$totalSteps = [math]::Ceiling(($MaxConnections - $StartConnections) / $ConnectionStep) + 1

Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Yellow
Write-Host "  Conn  │  QPS   │ Errors │ CPU%  │ Mem MB │  Status"
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Yellow

while ($currentConnections -le $MaxConnections) {
    Write-Host -NoNewline "  " -ForegroundColor White
    Write-Host -NoNewline "$currentConnections".PadLeft(4) -ForegroundColor White
    Write-Host -NoNewline "  │ " -ForegroundColor Gray
    Write-Host -NoNewline "testing..." -ForegroundColor Yellow
    
    $result = Test-LoadLevel -Connections $currentConnections -Duration $StepDurationSeconds
    $results += $result
    
    # Определяем статус
    $status = "OK"
    $statusColor = "Green"
    
    if ($result.ErrorRate -gt 5) {
        $status = "ERRORS!"
        $statusColor = "Red"
    } elseif ($result.MaxCpuPercent -gt 90) {
        $status = "HIGH CPU"
        $statusColor = "Yellow"
    } elseif ($result.ErrorRate -gt 1) {
        $status = "WARN"
        $statusColor = "Yellow"
    }
    
    # Перезаписываем строку с результатами
    Write-Host "`r  $($currentConnections.ToString().PadLeft(4))  │ $($result.QPS.ToString("F0").PadLeft(6)) │ $($result.Errors.ToString().PadLeft(6)) │ $($result.MaxCpuPercent.ToString().PadLeft(5)) │ $($result.MaxMemoryMB.ToString().PadLeft(6)) │ " -NoNewline
    Write-Host $status -ForegroundColor $statusColor
    
    # Проверяем предел
    if ($result.ErrorRate -gt 20 -or $result.MaxCpuPercent -gt 99) {
        Write-Host ""
        Write-Host "!!! ОБНАРУЖЕН ПРЕДЕЛ СИСТЕМЫ !!!" -ForegroundColor Red
        Write-Host "Максимальные соединения без деградации: $($currentConnections - $ConnectionStep)" -ForegroundColor Yellow
        break
    }
    
    $currentConnections += $ConnectionStep
    $stepNumber++
    
    # Небольшая пауза между шагами для стабилизации
    Start-Sleep -Seconds 5
}

Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Yellow

# Сохраняем результаты
$results | ForEach-Object { [PSCustomObject]$_ } | Export-Csv -Path (Join-Path $resultDir "incremental_results.csv") -NoTypeInformation

# Генерируем отчёт
$peakQPS = ($results | Measure-Object -Property QPS -Maximum)
$peakQPSResult = $results | Where-Object { $_.QPS -eq $peakQPS.Maximum } | Select-Object -First 1

$stableResults = $results | Where-Object { $_.ErrorRate -lt 1 }
$maxStableConn = if ($stableResults) { ($stableResults | Measure-Object -Property Connections -Maximum).Maximum } else { 0 }

$report = @"

╔══════════════════════════════════════════════════════════════════════════╗
║              ОТЧЁТ ИНКРЕМЕНТАЛЬНОГО НАГРУЗОЧНОГО ТЕСТА                   ║
╚══════════════════════════════════════════════════════════════════════════╝

Дата:                     $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Протестировано шагов:     $($results.Count)
Диапазон соединений:      $StartConnections - $($results[-1].Connections)

═══════════════════════════════════════════════════════════════════════════
                              КЛЮЧЕВЫЕ НАХОДКИ
═══════════════════════════════════════════════════════════════════════════

Пиковый QPS:              $([int]$peakQPS.Maximum) (при $($peakQPSResult.Connections) соединениях)
Макс. стабильных коннов:  $maxStableConn (без ошибок)
Макс. CPU:                $(($results | Measure-Object -Property MaxCpuPercent -Maximum).Maximum)%
Макс. память:             $(($results | Measure-Object -Property MaxMemoryMB -Maximum).Maximum) MB

═══════════════════════════════════════════════════════════════════════════
                           РЕЗУЛЬТАТЫ ПО УРОВНЯМ
═══════════════════════════════════════════════════════════════════════════

"@

foreach ($r in $results) {
    $status = if ($r.ErrorRate -gt 5) { "[FAIL]" } elseif ($r.ErrorRate -gt 1) { "[WARN]" } else { "[ OK ]" }
    $report += "$status $($r.Connections) conn: $([int]$r.QPS) QPS, $($r.Errors) errors, CPU $($r.MaxCpuPercent)%, Mem $($r.MaxMemoryMB)MB`n"
}

$report += @"

═══════════════════════════════════════════════════════════════════════════
                              РЕКОМЕНДАЦИИ
═══════════════════════════════════════════════════════════════════════════

Рекомендуемый лимит соединений:  $([int]($maxStableConn * 0.8)) (80% от стабильного)
Планируемый пиковый QPS:         $([int]($peakQPS.Maximum * 0.7)) (70% от пика)

Файлы:
  incremental_results.csv - детальные результаты
  report.txt              - этот отчёт

═══════════════════════════════════════════════════════════════════════════
"@

$report | Out-File -FilePath (Join-Path $resultDir "report.txt") -Encoding UTF8
Write-Host $report

Write-Host ""
Write-Host "Результаты сохранены в: $resultDir" -ForegroundColor Green
Write-Host ""
