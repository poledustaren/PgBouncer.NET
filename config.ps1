# Единая конфигурация PgBouncer.NET
# Все скрипты и компоненты читают реквизиты отсюда

# PostgreSQL Backend (реальная база данных)
$global:PG_HOST = "localhost"
$global:PG_PORT = 5437
$global:PG_ADMIN_USER = "postgres"
$global:PG_ADMIN_PASSWORD = "123"

# PgBouncer.NET (прокси)
$global:PGBOUNCER_HOST = "localhost"
$global:PGBOUNCER_PORT = 6432
$global:DASHBOARD_PORT = 5080

# Тестовые базы данных
$global:TEST_DB_COUNT = 10
$global:TEST_DB_PREFIX = "testdb"
$global:TEST_USER_PREFIX = "testuser"
$global:TEST_PASS_PREFIX = "testpass"

# Функция получения connection string для PostgreSQL
function Get-PgConnectionString {
    param(
        [string]$Database = "postgres",
        [string]$User = $global:PG_ADMIN_USER,
        [string]$Password = $global:PG_ADMIN_PASSWORD,
        [switch]$ThroughPgBouncer
    )
    
    if ($ThroughPgBouncer) {
        return "Host=$global:PGBOUNCER_HOST;Port=$global:PGBOUNCER_PORT;Database=$Database;Username=$User;Password=$Password"
    }
    return "Host=$global:PG_HOST;Port=$global:PG_PORT;Database=$Database;Username=$User;Password=$Password"
}

# Функция получения реквизитов тестовой базы
function Get-TestDbCredentials {
    param([int]$Number)
    
    return @{
        Database = "$global:TEST_DB_PREFIX$Number"
        User = "$global:TEST_USER_PREFIX$Number"
        Password = "$global:TEST_PASS_PREFIX$Number"
    }
}

Write-Host "Конфигурация загружена:" -ForegroundColor Green
Write-Host "  PostgreSQL: $global:PG_HOST`:$global:PG_PORT (user: $global:PG_ADMIN_USER)"
Write-Host "  PgBouncer:  $global:PGBOUNCER_HOST`:$global:PGBOUNCER_PORT"
Write-Host "  Dashboard:  http://$global:PGBOUNCER_HOST`:$global:DASHBOARD_PORT/"
