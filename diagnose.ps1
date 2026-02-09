# Диагностический скрипт для PgBouncer.NET
# Проверяет всю цепочку подключений

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║          ДИАГНОСТИКА PGBOUNCER.NET                       ║" -ForegroundColor Cyan  
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Конфигурация
$PG_HOST = "localhost"
$PG_PORT = 5437
$PG_USER = "postgres"
$PG_PASS = "123"
$PG_DB = "postgres"

$BOUNCER_HOST = "localhost"
$BOUNCER_PORT = 6432

$API_URL = "http://localhost:5080"

Write-Host "=== ШАГ 1: Проверка PostgreSQL напрямую ===" -ForegroundColor Yellow
Write-Host ""

$env:PGPASSWORD = $PG_PASS
try {
    $result = psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -t -c "SELECT 'PostgreSQL OK: ' || version();" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] PostgreSQL доступен: $result" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] PostgreSQL недоступен: $result" -ForegroundColor Red
        Write-Host "ПРОВЕРЬ: Запущен ли PostgreSQL на порту $PG_PORT?" -ForegroundColor Red
    }
} catch {
    Write-Host "[FAIL] Ошибка: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== ШАГ 2: Проверка тестовых баз ===" -ForegroundColor Yellow
Write-Host ""

for ($i = 1; $i -le 3; $i++) {
    $testDb = "testdb$i"
    $testUser = "testuser$i"
    $testPass = "testpass$i"
    
    $env:PGPASSWORD = $testPass
    try {
        $result = psql -h $PG_HOST -p $PG_PORT -U $testUser -d $testDb -t -c "SELECT 'OK: ' || current_database() || '/' || current_user;" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[OK] $testDb/$testUser -> $result" -ForegroundColor Green
        } else {
            Write-Host "[FAIL] $testDb/$testUser -> $result" -ForegroundColor Red
        }
    } catch {
        Write-Host "[FAIL] $testDb/$testUser -> $_" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "=== ШАГ 3: Проверка порта PgBouncer (6432) ===" -ForegroundColor Yellow
Write-Host ""

try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $tcp.Connect($BOUNCER_HOST, $BOUNCER_PORT)
    if ($tcp.Connected) {
        Write-Host "[OK] Порт $BOUNCER_PORT открыт и слушает!" -ForegroundColor Green
        $tcp.Close()
    }
} catch {
    Write-Host "[FAIL] Порт $BOUNCER_PORT НЕ ДОСТУПЕН!" -ForegroundColor Red
    Write-Host "ПРОВЕРЬ: Запущен ли .\run-server.ps1 ?" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== ШАГ 4: Проверка Dashboard API ===" -ForegroundColor Yellow
Write-Host ""

try {
    $stats = Invoke-RestMethod "$API_URL/api/stats/summary" -TimeoutSec 5
    Write-Host "[OK] API доступно:" -ForegroundColor Green
    Write-Host "     Pools: $($stats.totalPools)"
    Write-Host "     Connections: $($stats.totalConnections)"
    Write-Host "     Active: $($stats.activeConnections)"
} catch {
    Write-Host "[FAIL] API недоступно: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== ШАГ 5: Тест подключения через PgBouncer ===" -ForegroundColor Yellow
Write-Host ""

# Попробуем подключиться напрямую через сокет и посмотреть что возвращается
try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $tcp.Connect($BOUNCER_HOST, $BOUNCER_PORT)
    $stream = $tcp.GetStream()
    $stream.ReadTimeout = 5000
    $stream.WriteTimeout = 5000
    
    # Шлём SSLRequest (как делает Npgsql)
    $sslRequest = [byte[]]@(0, 0, 0, 8, 4, 210, 22, 47)  # 80877103 в big-endian
    $stream.Write($sslRequest, 0, 8)
    $stream.Flush()
    
    # Читаем ответ
    $buffer = New-Object byte[] 1
    $bytesRead = $stream.Read($buffer, 0, 1)
    
    if ($bytesRead -gt 0) {
        $response = [char]$buffer[0]
        if ($response -eq 'N') {
            Write-Host "[OK] SSLRequest -> ответ '$response' (SSL не поддерживается, это нормально)" -ForegroundColor Green
        } elseif ($response -eq 'S') {
            Write-Host "[OK] SSLRequest -> ответ '$response' (SSL поддерживается)" -ForegroundColor Green
        } else {
            Write-Host "[WARN] SSLRequest -> неожиданный ответ: $response (код: $($buffer[0]))" -ForegroundColor Yellow
        }
    } else {
        Write-Host "[FAIL] Нет ответа от сервера на SSLRequest!" -ForegroundColor Red
    }
    
    # Шлём StartupMessage
    $database = "testdb1"
    $user = "testuser1"
    
    # Формируем StartupMessage
    $params = "user`0$user`0database`0$database`0`0"
    $paramsBytes = [System.Text.Encoding]::UTF8.GetBytes($params)
    $msgLen = 4 + 4 + $paramsBytes.Length  # length + protocol + params
    
    $startupMsg = New-Object byte[] $msgLen
    # Length (big-endian)
    $startupMsg[0] = ($msgLen -shr 24) -band 0xFF
    $startupMsg[1] = ($msgLen -shr 16) -band 0xFF
    $startupMsg[2] = ($msgLen -shr 8) -band 0xFF
    $startupMsg[3] = $msgLen -band 0xFF
    # Protocol version 3.0 = 196608 = 0x00030000
    $startupMsg[4] = 0
    $startupMsg[5] = 3
    $startupMsg[6] = 0
    $startupMsg[7] = 0
    # Params
    [Array]::Copy($paramsBytes, 0, $startupMsg, 8, $paramsBytes.Length)
    
    $stream.Write($startupMsg, 0, $msgLen)
    $stream.Flush()
    
    # Читаем ответ
    Start-Sleep -Milliseconds 500
    $responseBuffer = New-Object byte[] 256
    $stream.ReadTimeout = 3000
    try {
        $bytesRead = $stream.Read($responseBuffer, 0, $responseBuffer.Length)
        if ($bytesRead -gt 0) {
            $firstByte = [char]$responseBuffer[0]
            Write-Host "[RECV] Получено $bytesRead байт, первый символ: '$firstByte' (код: $($responseBuffer[0]))" -ForegroundColor Cyan
            
            if ($firstByte -eq 'R') {
                Write-Host "[OK] Сервер ответил Authentication!" -ForegroundColor Green
            } elseif ($firstByte -eq 'E') {
                # Парсим ошибку
                $errorMsg = [System.Text.Encoding]::UTF8.GetString($responseBuffer, 5, [Math]::Min($bytesRead - 5, 100))
                Write-Host "[ERROR] Сервер вернул ошибку: $errorMsg" -ForegroundColor Red
            } else {
                $hexDump = ($responseBuffer[0..([Math]::Min($bytesRead, 20) - 1)] | ForEach-Object { $_.ToString("X2") }) -join " "
                Write-Host "[DATA] Hex: $hexDump" -ForegroundColor Gray
            }
        } else {
            Write-Host "[FAIL] Нет ответа на StartupMessage!" -ForegroundColor Red
        }
    } catch {
        Write-Host "[TIMEOUT] Таймаут ожидания ответа от сервера" -ForegroundColor Yellow
    }
    
    $tcp.Close()
    
} catch {
    Write-Host "[FAIL] Ошибка подключения: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== ШАГ 6: Проверка процессов ===" -ForegroundColor Yellow
Write-Host ""

$pgBouncer = Get-Process -Name "PgBouncer.Server" -ErrorAction SilentlyContinue
if ($pgBouncer) {
    Write-Host "[OK] PgBouncer.Server запущен (PID: $($pgBouncer.Id))" -ForegroundColor Green
} else {
    Write-Host "[FAIL] PgBouncer.Server НЕ ЗАПУЩЕН!" -ForegroundColor Red
    Write-Host "Запусти: .\run-server.ps1" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "                    ДИАГНОСТИКА ЗАВЕРШЕНА                   " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
