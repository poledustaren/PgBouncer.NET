# PgBouncer.NET Stress Test Launcher
# Запускает PgBouncer.Server и PgBouncer.StressTester параллельно

param(
    [string]$PgBouncerPath = "src\PgBouncer.Server\bin\Release\net8.0\win-x64\PgBouncer.Server.dll",
    [string]$StressTestPath = "tests\PgBouncer.StressTester\bin\Release\net8.0\PgBouncer.StressTester.dll",
    [switch]$Build = $false,
    [switch]$Debug = $false
)

$ErrorActionPreference = "Continue"

Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║      PgBouncer.NET Stress Test - Parallel Launcher            ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Шаг 1: Проверка PostgreSQL
Write-Host "[1/4] Проверка PostgreSQL..." -ForegroundColor Yellow
$pgPort = 5432
$pgHost = "127.0.0.1"

try {
    $testConn = New-Object System.Net.Sockets.TcpClient
    $testConn.Connect($pgHost, $pgPort)
    $testConn.Close()
    Write-Host "  ✅ PostgreSQL доступен на порту $pgPort" -ForegroundColor Green
} catch {
    Write-Host "  ❌ PostgreSQL недоступен на порту $pgPort" -ForegroundColor Red
    Write-Host "  Запустите PostgreSQL и повторите попытку" -ForegroundColor Red
    exit 1
}

# Шаг 2: Сборка (если требуется)
if ($Build) {
    Write-Host "[2/4] Сборка проектов..." -ForegroundColor Yellow
    dotnet build src\PgBouncer.Server\PgBouncer.Server.csproj -c Release
    dotnet build tests\PgBouncer.StressTester\PgBouncer.StressTester.csproj -c Release
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ❌ Ошибка сборки" -ForegroundColor Red
        exit 1
    }
    Write-Host "  ✅ Сборка завершена" -ForegroundColor Green
} else {
    Write-Host "[2/4] Пропуск сборки (используются существующие бинарники)" -ForegroundColor Gray
}

# Шаг 3: Запуск PgBouncer.Server
Write-Host "[3/4] Запуск PgBouncer.Server..." -ForegroundColor Yellow

# Проверяем, не запущен ли уже
$existing = Get-NetTCPConnection -LocalPort 6432 -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "  ℹ️  PgBouncer уже запущен на порту 6432" -ForegroundColor Cyan
} else {
    # Запускаем в фоне
    $pgBouncerJob = Start-Job -ScriptBlock {
        param($path)
        Set-Location "C:\Projects\pgbouncer.net"
        dotnet run --project $path --configuration Release
    } -ArgumentList $PgBouncerPath
    
    Write-Host "  🚀 PgBouncer.Server запущен (Job ID: $($pgBouncerJob.Id))" -ForegroundColor Green
    
    # Ждём запуска
    Start-Sleep -Seconds 3
    
    # Проверяем, что запустился
    $started = $false
    for ($i = 0; $i -lt 10; $i++) {
        try {
            $testConn = New-Object System.Net.Sockets.TcpClient
            $testConn.Connect("127.0.0.1", 6432)
            $testConn.Close()
            $started = $true
            break
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    
    if ($started) {
        Write-Host "  ✅ PgBouncer.Server готов на порту 6432" -ForegroundColor Green
    } else {
        Write-Host "  ⚠️  PgBouncer.Server может быть не готов, продолжаем..." -ForegroundColor Yellow
    }
}

# Шаг 4: Запуск StressTester
Write-Host "[4/4] Запуск StressTester..." -ForegroundColor Yellow
Write-Host ""

# Запускаем стресс-тест
$stressArgs = if ($Debug) { "--debug" } else { "" }
dotnet run --project $StressTestPath --configuration Release $stressArgs

# Очистка
Write-Host ""
Write-Host "═══ Тест завершен ═══" -ForegroundColor Cyan

# Предлагаем остановить PgBouncer
$response = Read-Host "Остановить PgBouncer.Server? (Y/N)"
if ($response -eq "Y" -or $response -eq "y") {
    Stop-Job -Id $pgBouncerJob.Id -ErrorAction SilentlyContinue
    Remove-Job -Id $pgBouncerJob.Id -ErrorAction SilentlyContinue
    Write-Host "PgBouncer.Server остановлен" -ForegroundColor Green
}
