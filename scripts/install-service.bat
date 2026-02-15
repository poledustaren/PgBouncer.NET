@echo off
setlocal EnableDelayedExpansion

echo.
echo ================================================================
echo   PgBouncer.NET Service Installer
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

:: Get the directory where this script is located
set "INSTALL_DIR=%~dp0"
set "INSTALL_DIR=%INSTALL_DIR:~0,-1%"

:: Service configuration
set "SERVICE_NAME=PgBouncerNet"
set "SERVICE_DISPLAYNAME=PgBouncer.NET Connection Pooler"
set "SERVICE_DESCRIPTION=High-performance PostgreSQL connection pooler for .NET applications"
set "EXE_PATH=%INSTALL_DIR%\PgBouncer.Net.exe"

:: Check if executable exists
if not exist "%EXE_PATH%" (
    echo ERROR: PgBouncer.Net.exe not found in current directory.
    echo Expected: %EXE_PATH%
    pause
    exit /b 1
)

:: Check if service already exists
sc query "%SERVICE_NAME%" >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo.
    echo WARNING: Service '%SERVICE_NAME%' already exists.
    echo.
    choice /C YN /M "Do you want to reinstall it"
    if errorlevel 2 goto :eof
    if errorlevel 1 (
        echo Stopping existing service...
        net stop "%SERVICE_NAME%" >nul 2>&1
        timeout /t 2 /nobreak >nul
        sc delete "%SERVICE_NAME%" >nul 2>&1
        timeout /t 2 /nobreak >nul
    )
)

echo.
echo Creating service...
echo   Name: %SERVICE_NAME%
echo   Path: %EXE_PATH%
echo.

sc create "%SERVICE_NAME%" ^
    binPath= "%EXE_PATH%" ^
    DisplayName= "%SERVICE_DISPLAYNAME%" ^
    start= auto ^
    obj= LocalSystem

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Failed to create service.
    pause
    exit /b 1
)

:: Set service description
sc description "%SERVICE_NAME%" "%SERVICE_DESCRIPTION%"

echo.
echo Service created successfully!
echo.
echo ================================================================
echo   Configuration
echo ================================================================
echo.
echo Before starting the service, please edit:
echo   %INSTALL_DIR%\appsettings.json
echo.
echo Set your PostgreSQL connection details:
echo   - Backend.Host
echo   - Backend.Port  
echo   - Backend.AdminPassword
echo.
echo ================================================================
echo   Service Commands
echo ================================================================
echo.
echo   Start:   net start %SERVICE_NAME%
echo   Stop:    net stop %SERVICE_NAME%
echo   Status:  sc query %SERVICE_NAME%
echo   Remove:  uninstall-service.bat
echo.
echo ================================================================

choice /C YN /M "Do you want to start the service now"
if errorlevel 2 goto :eof
if errorlevel 1 (
    echo.
    echo Starting service...
    net start "%SERVICE_NAME%"
    
    if %ERRORLEVEL% equ 0 (
        echo.
        echo Service started successfully!
        echo.
        echo Dashboard: http://localhost:5081/
        echo Proxy:     localhost:6432
    ) else (
        echo.
        echo WARNING: Service failed to start. Check logs at:
        echo   %INSTALL_DIR%\logs\
    )
)

echo.
pause
