# PgBouncer.NET - Запуск прокси-сервера
# Запускать из корневой директории проекта

param(
    [int]$Port = 6432,
    [string]$BackendHost = "localhost",
    [int]$BackendPort = 5437
)

Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         PgBouncer.NET - Прокси-сервер                    ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Конфигурация:" -ForegroundColor Yellow
Write-Host "  Порт прослушивания:  $Port" -ForegroundColor White
Write-Host "  Backend PostgreSQL:  ${BackendHost}:$BackendPort" -ForegroundColor White
Write-Host ""
Write-Host "Нажмите Ctrl+C для остановки" -ForegroundColor Gray
Write-Host ""

# Проверяем наличие опубликованной версии
$exePath = ".\publish\Server\PgBouncer.Server.exe"
if (Test-Path $exePath) {
    Write-Host "Запуск из опубликованной версии..." -ForegroundColor Green
    & $exePath
} else {
    Write-Host "Запуск через dotnet run..." -ForegroundColor Green
    Push-Location src\PgBouncer.Server
    dotnet run -c Release
    Pop-Location
}
