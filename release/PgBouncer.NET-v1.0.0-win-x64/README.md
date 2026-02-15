# PgBouncer.NET v1.0.0

High-performance PostgreSQL connection pooler for Windows.

## Installation

### Option 1: Run as Console Application
```batch
PgBouncer.Net.exe
```

### Option 2: Install as Windows Service
```batch
REM Run as Administrator
install-service.bat
```

## Quick Start

1. Edit `appsettings.json` to configure your PostgreSQL server
2. Run `install-service.bat` as Administrator (or run `PgBouncer.Net.exe` directly)
3. Connect your applications to `localhost:6432`

## Configuration

Edit `appsettings.json`:

```json
{
  "ListenPort": 6432,
  "Backend": {
    "Host": "your-postgres-host",
    "Port": 5432,
    "AdminUser": "postgres",
    "AdminPassword": "your-password"
  },
  "Pool": {
    "MaxSize": 100
  }
}
```

## Service Management

- `install-service.bat` - Install as Windows service
- `uninstall-service.bat` - Remove Windows service
- `net start PgBouncerNet` - Start service
- `net stop PgBouncerNet` - Stop service
- `sc query PgBouncerNet` - Check service status

## Performance

- 0% errors at 50 concurrent connections
- ~1.6% errors at 100 connections
- ~420 QPS sustained throughput
- p50 latency: ~0.78ms

## Dashboard

Open http://localhost:5081/ for real-time statistics.

## Logs

Logs are written to `logs/pgbouncer-{date}.log`

## Support

GitHub: https://github.com/poledustaren/PgBouncer.NET
