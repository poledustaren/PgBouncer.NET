# PgBouncer.NET - Запуск всего стека (сервер + API)
# Запускать из корневой директории проекта

Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         PgBouncer.NET - Запуск полного стека             ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Проверяем сборку
if (-not (Test-Path ".\publish\Server\PgBouncer.Server.exe")) {
    Write-Host "Проекты не собраны. Запускаю install.ps1..." -ForegroundColor Yellow
    & .\install.ps1
    if ($LASTEXITCODE -ne 0) { exit 1 }
}

Write-Host ""
Write-Host "Запуск прокси-сервера (порт 6432)..." -ForegroundColor Yellow
$serverJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    & .\publish\Server\PgBouncer.Server.exe
}

Write-Host "Запуск Dashboard API (порт 5080)..." -ForegroundColor Yellow
$apiJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    & .\publish\DashboardApi\PgBouncer.Dashboard.Api.exe
}

Start-Sleep -Seconds 3

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║                    ВСЁ ЗАПУЩЕНО!                         ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "Сервисы:" -ForegroundColor Yellow
Write-Host "  Прокси-сервер:    localhost:6432" -ForegroundColor White
Write-Host "  Dashboard API:    http://localhost:5080/swagger" -ForegroundColor White
Write-Host ""
Write-Host "Подключение клиентов:" -ForegroundColor Yellow
Write-Host "  Host=localhost;Port=6432;Database=testdb1;Username=testuser1;Password=testpass1" -ForegroundColor Gray
Write-Host ""
Write-Host "Нажмите любую клавишу для остановки..." -ForegroundColor Red
Write-Host ""

$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

Write-Host ""
Write-Host "Остановка сервисов..." -ForegroundColor Yellow

$serverJob | Stop-Job -PassThru | Remove-Job
$apiJob | Stop-Job -PassThru | Remove-Job

Write-Host "Готово!" -ForegroundColor Green
