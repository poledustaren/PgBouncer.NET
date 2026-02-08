# PgBouncer.NET - Запуск нагрузочного тестирования
# Запускать из корневой директории проекта

param(
    [int]$Connections = 100,
    [int]$Instances = 1,
    [string]$Pattern = "mixed",
    [int]$Duration = 60,
    [string]$Host = "localhost",
    [int]$Port = 6432,
    [string]$Database = "fuel",
    [string]$User = "postgres",
    [string]$Password = "password"
)

Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         PgBouncer.NET - Нагрузочное тестирование         ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Конфигурация:" -ForegroundColor Yellow
Write-Host "  Соединений на экземпляр:  $Connections" -ForegroundColor White
Write-Host "  Экземпляров:              $Instances" -ForegroundColor White
Write-Host "  Паттерн:                  $Pattern" -ForegroundColor White
Write-Host "  Длительность:             ${Duration}с" -ForegroundColor White
Write-Host "  Цель:                     ${Host}:${Port}/${Database}" -ForegroundColor White
Write-Host ""
Write-Host "Паттерны: rapid, idle, mixed, burst, transaction" -ForegroundColor Gray
Write-Host ""

$args = @(
    "--connections", $Connections,
    "--instances", $Instances,
    "--pattern", $Pattern,
    "--duration", $Duration,
    "--host", $Host,
    "--port", $Port,
    "--database", $Database,
    "--user", $User,
    "--password", $Password
)

# Проверяем наличие опубликованной версии
$exePath = ".\publish\LoadTester\PgBouncer.LoadTester.exe"
if (Test-Path $exePath) {
    Write-Host "Запуск из опубликованной версии..." -ForegroundColor Green
    & $exePath @args
} else {
    Write-Host "Запуск через dotnet run..." -ForegroundColor Green
    Push-Location tests\PgBouncer.LoadTester
    dotnet run -c Release -- @args
    Pop-Location
}
