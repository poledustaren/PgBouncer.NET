# Скрипт создания 10 тестовых баз данных для стресс-тестирования PgBouncer.NET

param(
    [string]$DbHost = "localhost",
    [int]$DbPort = 5437,
    [string]$AdminUser = "postgres",
    [string]$AdminPassword = "123",
    [int]$DatabaseCount = 10,
    [int]$RowsPerTable = 100000
)

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   PgBouncer.NET - Создание тестовых баз данных           ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$env:PGPASSWORD = $AdminPassword

# Проверка psql
$psql = Get-Command psql -ErrorAction SilentlyContinue
if (-not $psql) {
    Write-Host "ОШИБКА: psql не найден. Установите PostgreSQL и добавьте в PATH" -ForegroundColor Red
    exit 1
}

Write-Host "Конфигурация:" -ForegroundColor Yellow
Write-Host "  PostgreSQL: $DbHost`:$DbPort"
Write-Host "  Admin: $AdminUser"
Write-Host "  Баз данных: $DatabaseCount"
Write-Host ""

Write-Host "[1/3] Создание баз данных и пользователей..." -ForegroundColor Yellow

# Создаём базы по одной
for ($i = 1; $i -le $DatabaseCount; $i++) {
    Write-Host "  Создание testdb$i и testuser$i..." -NoNewline
    
    # Создаём пользователя
    $userSql = "DO `$`$ BEGIN CREATE USER testuser$i WITH PASSWORD 'testpass$i'; EXCEPTION WHEN duplicate_object THEN NULL; END `$`$;"
    psql -h $DbHost -p $DbPort -U $AdminUser -d postgres -c $userSql 2>$null | Out-Null
    
    # Создаём базу
    $dbExists = psql -h $DbHost -p $DbPort -U $AdminUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='testdb$i'" 2>$null
    if ($dbExists -ne "1") {
        psql -h $DbHost -p $DbPort -U $AdminUser -d postgres -c "CREATE DATABASE testdb$i OWNER testuser$i" 2>$null | Out-Null
    }
    
    Write-Host " OK" -ForegroundColor Green
}

Write-Host ""
Write-Host "[2/3] Создание таблиц и заполнение данными..." -ForegroundColor Yellow

# SQL для создания таблиц и данных
$createTablesSql = @"
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    balance DECIMAL(15,2) DEFAULT 0
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock INT DEFAULT 0,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    total_price DECIMAL(15,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO customers (name, email, phone, address, balance)
SELECT 
    'Customer ' || i,
    'customer' || i || '@example.com',
    '+7' || (9000000000 + i)::text,
    'Address #' || i,
    (random() * 10000)::decimal(15,2)
FROM generate_series(1, $RowsPerTable) i;

INSERT INTO products (name, description, price, stock, category)
SELECT 
    'Product ' || i,
    'Description ' || i,
    (random() * 1000 + 10)::decimal(10,2),
    (random() * 1000)::int,
    CASE (i % 5) WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing' WHEN 2 THEN 'Food' WHEN 3 THEN 'Books' ELSE 'Other' END
FROM generate_series(1, $RowsPerTable) i;

INSERT INTO orders (customer_id, product_id, quantity, total_price, status)
SELECT 
    (random() * $RowsPerTable + 1)::int,
    (random() * $RowsPerTable + 1)::int,
    (random() * 10 + 1)::int,
    (random() * 5000)::decimal(15,2),
    CASE (i % 4) WHEN 0 THEN 'pending' WHEN 1 THEN 'processing' WHEN 2 THEN 'shipped' ELSE 'delivered' END
FROM generate_series(1, $RowsPerTable * 3) i;

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);

ANALYZE customers; ANALYZE products; ANALYZE orders;
"@

for ($i = 1; $i -le $DatabaseCount; $i++) {
    Write-Host "  Заполнение testdb$i..." -NoNewline
    $env:PGPASSWORD = "testpass$i"
    echo $createTablesSql | psql -h $DbHost -p $DbPort -U "testuser$i" -d "testdb$i" 2>$null | Out-Null
    Write-Host " OK" -ForegroundColor Green
}

$env:PGPASSWORD = $AdminPassword

Write-Host ""
Write-Host "[3/3] Проверка размеров баз..." -ForegroundColor Yellow

$sizeSql = "SELECT datname, pg_size_pretty(pg_database_size(datname)) as size FROM pg_database WHERE datname LIKE 'testdb%' ORDER BY datname;"
psql -h $DbHost -p $DbPort -U $AdminUser -d postgres -c $sizeSql

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║              БАЗА ДАННЫХ СОЗДАНЫ УСПЕШНО!                ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "Созданные базы:" -ForegroundColor Cyan
for ($i = 1; $i -le $DatabaseCount; $i++) {
    Write-Host "  testdb$i (testuser$i / testpass$i)"
}
Write-Host ""
