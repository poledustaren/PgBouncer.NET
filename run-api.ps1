# PgBouncer.NET - Запуск Dashboard API
# Запускать из корневой директории проекта

param(
    [int]$Port = 5080
)

Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         PgBouncer.NET - Dashboard API                    ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Swagger UI: http://localhost:$Port/swagger" -ForegroundColor Yellow
Write-Host ""
Write-Host "Нажмите Ctrl+C для остановки" -ForegroundColor Gray
Write-Host ""

# Проверяем наличие опубликованной версии
$exePath = ".\publish\DashboardApi\PgBouncer.Dashboard.Api.exe"
if (Test-Path $exePath) {
    Write-Host "Запуск из опубликованной версии..." -ForegroundColor Green
    & $exePath
} else {
    Write-Host "Запуск через dotnet run..." -ForegroundColor Green
    Push-Location src\PgBouncer.Dashboard.Api
    dotnet run -c Release
    Pop-Location
}
