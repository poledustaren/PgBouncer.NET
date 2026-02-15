@echo off
setlocal EnableDelayedExpansion

echo.
echo ================================================================
echo   PgBouncer.NET Release Build Script
echo ================================================================
echo.

set VERSION=1.0.0
set OUTPUT_DIR=release\PgBouncer.NET-v%VERSION%-win-x64

echo [1/4] Cleaning previous builds...
if exist release rmdir /s /q release
mkdir "%OUTPUT_DIR%"
mkdir "%OUTPUT_DIR%\logs"

echo [2/4] Building self-contained binary...
dotnet publish src\PgBouncer.Server\PgBouncer.Server.csproj ^
    --configuration Release ^
    --runtime win-x64 ^
    --self-contained true ^
    --output "%OUTPUT_DIR%" ^
    -p:PublishSingleFile=true ^
    -p:PublishTrimmed=true ^
    -p:EnableCompressionInSingleFile=true ^
    -p:IncludeNativeLibrariesForSelfExtract=true ^
    -p:DebugType=none ^
    -p:DebugSymbols=false

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Build failed!
    exit /b 1
)

echo [3/4] Copying configuration files...
copy src\PgBouncer.Server\appsettings.Production.json "%OUTPUT_DIR%\appsettings.json" >nul

echo [4/4] Creating README...
(
echo # PgBouncer.NET v%VERSION%
echo.
echo High-performance PostgreSQL connection pooler for Windows.
echo.
echo ## Quick Start
echo.
echo 1. Edit `appsettings.json` to configure your PostgreSQL server
echo 2. Run `PgBouncer.Net.exe`
echo 3. Connect your applications to `localhost:6432`
echo.
echo ## Configuration
echo.
echo Edit `appsettings.json`:
echo.
echo ```json
echo {
echo   "ListenPort": 6432,
echo   "Backend": {
echo     "Host": "your-postgres-host",
echo     "Port": 5432,
echo     "AdminUser": "postgres",
echo     "AdminPassword": "your-password"
echo   },
echo   "Pool": {
echo     "MaxSize": 100
echo   }
echo }
echo ```
echo.
echo ## Performance
echo.
echo - 0%% errors at 50 concurrent connections
echo - ~1.6%% errors at 100 connections
echo - ~420 QPS sustained throughput
echo.
echo ## Dashboard
echo.
echo Open http://localhost:5081/ for real-time statistics.
echo.
echo ## Support
echo.
echo GitHub: https://github.com/poledustaren/PgBouncer.NET
) > "%OUTPUT_DIR%\README.md"

echo.
echo ================================================================
echo   Build Complete!
echo ================================================================
echo.
echo Output: %OUTPUT_DIR%
echo Binary: %OUTPUT_DIR%\PgBouncer.Net.exe
echo.

dir "%OUTPUT_DIR%\PgBouncer.Net.exe" | findstr "PgBouncer"

echo.
echo Creating ZIP archive...
powershell -Command "Compress-Archive -Path '%OUTPUT_DIR%' -DestinationPath 'release\PgBouncer.NET-v%VERSION%-win-x64.zip' -Force"

echo.
echo Done! Release package ready:
echo   %CD%\release\PgBouncer.NET-v%VERSION%-win-x64.zip
echo.
