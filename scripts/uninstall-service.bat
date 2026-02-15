@echo off
setlocal EnableDelayedExpansion

echo.
echo ================================================================
echo   PgBouncer.NET Service Uninstaller
echo ================================================================
echo.

:: Check admin rights
net session >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: This script requires Administrator privileges.
    echo Please right-click and select "Run as administrator".
    pause
    exit /b 1
)

set "SERVICE_NAME=PgBouncerNet"

:: Check if service exists
sc query "%SERVICE_NAME%" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo.
    echo Service '%SERVICE_NAME%' is not installed.
    pause
    exit /b 0
)

echo.
echo Service found: %SERVICE_NAME%
echo.
choice /C YN /M "Are you sure you want to remove this service"
if errorlevel 2 goto :eof

echo.
echo Stopping service...
net stop "%SERVICE_NAME%" >nul 2>&1
timeout /t 2 /nobreak >nul

echo Removing service...
sc delete "%SERVICE_NAME%"

if %ERRORLEVEL% equ 0 (
    echo.
    echo ================================================================
    echo   Service Removed Successfully
    echo ================================================================
    echo.
    echo The service has been uninstalled.
    echo.
    echo To completely remove PgBouncer.NET, you can delete:
    echo   %~dp0
    echo.
) else (
    echo.
    echo ERROR: Failed to remove service.
    echo Try running this script as Administrator.
)

echo.
pause
