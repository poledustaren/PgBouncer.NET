# PgBouncer.NET v1.0.0

High-performance PostgreSQL connection pooler for Windows.

## Quick Start

1. Edit `appsettings.json` to configure your PostgreSQL server
2. Run `PgBouncer.Net.exe`
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

## Performance

- 0% errors at 50 concurrent connections
- ~1.6% errors at 100 connections
- ~420 QPS sustained throughput

## Dashboard

Open http://localhost:5081/ for real-time statistics.

## Support

GitHub: https://github.com/poledustaren/PgBouncer.NET
