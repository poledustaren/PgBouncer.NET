# PgBouncer.NET - Скрипт установки и сборки
# Запускать из корневой директории проекта

Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         PgBouncer.NET - Установка и сборка               ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Проверяем наличие .NET SDK
Write-Host "[1/4] Проверка .NET SDK..." -ForegroundColor Yellow
$dotnetVersion = dotnet --version 2>$null
if (-not $dotnetVersion) {
    Write-Host "ОШИБКА: .NET SDK не установлен!" -ForegroundColor Red
    Write-Host "Скачайте с https://dotnet.microsoft.com/download/dotnet/8.0" -ForegroundColor Yellow
    exit 1
}
Write-Host "  .NET SDK версия: $dotnetVersion" -ForegroundColor Green

# Восстановление пакетов
Write-Host ""
Write-Host "[2/4] Восстановление NuGet пакетов..." -ForegroundColor Yellow
dotnet restore
if ($LASTEXITCODE -ne 0) {
    Write-Host "ОШИБКА: Не удалось восстановить пакеты!" -ForegroundColor Red
    exit 1
}
Write-Host "  Пакеты восстановлены" -ForegroundColor Green

# Сборка проектов
Write-Host ""
Write-Host "[3/4] Сборка проектов..." -ForegroundColor Yellow

Write-Host "  Сборка PgBouncer.Core..." -ForegroundColor Gray
dotnet build src/PgBouncer.Core/PgBouncer.Core.csproj -c Release --no-restore
if ($LASTEXITCODE -ne 0) { Write-Host "ОШИБКА при сборке PgBouncer.Core" -ForegroundColor Red; exit 1 }

Write-Host "  Сборка PgBouncer.Server..." -ForegroundColor Gray
dotnet build src/PgBouncer.Server/PgBouncer.Server.csproj -c Release --no-restore
if ($LASTEXITCODE -ne 0) { Write-Host "ОШИБКА при сборке PgBouncer.Server" -ForegroundColor Red; exit 1 }

Write-Host "  Сборка PgBouncer.Dashboard.Api..." -ForegroundColor Gray
dotnet build src/PgBouncer.Dashboard.Api/PgBouncer.Dashboard.Api.csproj -c Release --no-restore
if ($LASTEXITCODE -ne 0) { Write-Host "ОШИБКА при сборке PgBouncer.Dashboard.Api" -ForegroundColor Red; exit 1 }

Write-Host "  Сборка PgBouncer.LoadTester..." -ForegroundColor Gray
dotnet build tests/PgBouncer.LoadTester/PgBouncer.LoadTester.csproj -c Release --no-restore
if ($LASTEXITCODE -ne 0) { Write-Host "ОШИБКА при сборке PgBouncer.LoadTester" -ForegroundColor Red; exit 1 }

Write-Host "  Все проекты собраны успешно!" -ForegroundColor Green

# Публикация для продакшена (опционально)
Write-Host ""
Write-Host "[4/4] Публикация приложений..." -ForegroundColor Yellow

$publishDir = ".\publish"
if (-not (Test-Path $publishDir)) {
    New-Item -ItemType Directory -Path $publishDir | Out-Null
}

dotnet publish src/PgBouncer.Server/PgBouncer.Server.csproj -c Release -o "$publishDir\Server" --no-build
dotnet publish src/PgBouncer.Dashboard.Api/PgBouncer.Dashboard.Api.csproj -c Release -o "$publishDir\DashboardApi" --no-build
dotnet publish tests/PgBouncer.LoadTester/PgBouncer.LoadTester.csproj -c Release -o "$publishDir\LoadTester" --no-build

Write-Host "  Приложения опубликованы в $publishDir" -ForegroundColor Green

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                  УСТАНОВКА ЗАВЕРШЕНА!                    ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Запуск:" -ForegroundColor Yellow
Write-Host "  .\run-server.ps1      - Запуск прокси-сервера" -ForegroundColor White
Write-Host "  .\run-api.ps1         - Запуск Dashboard API" -ForegroundColor White
Write-Host "  .\run-loadtest.ps1    - Запуск нагрузочного теста" -ForegroundColor White
Write-Host ""
