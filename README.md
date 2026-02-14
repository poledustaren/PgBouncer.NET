# PgBouncer.NET

**Connection pooler for PostgreSQL on .NET** — lightweight proxy server for managing PostgreSQL connections.

[![.NET](https://img.shields.io/badge/.NET-8.0-512BD4?logo=dotnet)](https://dotnet.microsoft.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-336791?logo=postgresql)](https://postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Problem Solved

Typical scenario:
- **10 microservices** × **200 connections** = **2000 connections to PostgreSQL**
- PostgreSQL supports only **100-500 connections**
- Result: `FATAL: too many connections`

**PgBouncer.NET** accepts all 2000 clients but maintains max 100 connections to the database.

## How It Works

```
┌─────────────┐     ┌──────────────────┐     ┌────────────┐
│  Client 1   │────▶│                  │     │            │
├─────────────┤     │                  │     │            │
│  Client 2   │────▶│  PgBouncer.NET   │────▶│ PostgreSQL │
├─────────────┤     │   (up to 2000)   │     │  (to 100)  │
│  ...        │────▶│                  │     │            │
├─────────────┤     │                  │     │            │
│  Client N   │────▶│                  │     │            │
└─────────────┘     └──────────────────┘     └────────────┘
```

### Pooling Mode: Transaction Pooling

- **Queue Type:** FIFO (First In, First Out)
- **Algorithm:** Semaphore limits backend connections
- **On Limit Exceeded:** Clients wait in queue (timeout 30-60 sec)
- **Backend Reset:** `DISCARD ALL` clears session state between clients

## Quick Start

### Requirements

- [.NET 8.0 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- PostgreSQL 13+

### Installation and Running

```bash
# 1. Clone repository
git clone https://github.com/your-username/pgbouncer.net.git
cd pgbouncer.net

# 2. Configure
# Edit src/PgBouncer.Server/appsettings.json

# 3. Build and run
dotnet run --project src/PgBouncer.Server
```

### Connection

```
Proxy:     localhost:6432
Dashboard: http://localhost:5081
```

Connect to port **6432** instead of the default **5432**.

## Configuration

`src/PgBouncer.Server/appsettings.json`:

```json
{
  "ListenPort": 6432,
  "DashboardPort": 5081,
  "Backend": {
    "Host": "127.0.0.1",
    "Port": 5432,
    "AdminUser": "postgres",
    "AdminPassword": "your_password"
  },
  "Pool": {
    "DefaultSize": 100,
    "MinSize": 50,
    "MaxSize": 200,
    "MaxTotalConnections": 2000,
    "Mode": "Transaction",
    "IdleTimeout": 300,
    "ConnectionTimeout": 60,
    "ServerResetQuery": "DISCARD ALL",
    "UsePipelinesArchitecture": false
  }
}
```

| Parameter                    | Description                                           | Default     |
| ---------------------------- | ----------------------------------------------------- | ----------- |
| `ListenPort`                 | Port for clients                                      | 6432        |
| `DashboardPort`              | Web dashboard port                                    | 5081        |
| `Pool.MaxSize`               | Max connections to PostgreSQL                         | 200         |
| `Pool.ConnectionTimeout`     | Slot wait timeout (sec)                               | 60          |
| `Pool.ServerResetQuery`      | Query to reset backend session                        | DISCARD ALL |
| `Pool.UsePipelinesArchitecture` | Enable zero-allocation I/O with System.IO.Pipelines | false       |

### Pipelines Architecture

PgBouncer.NET supports two I/O architectures:

**Stream (Legacy):** Traditional Stream-based I/O with buffered reads/writes. Stable and well-tested.

**Pipelines:** Zero-allocation I/O using `System.IO.Pipelines` for reduced memory pressure and improved throughput under high load.

```json
{
  "Pool": {
    "UsePipelinesArchitecture": true
  }
}
```

See [PIPELINES_ARCHITECTURE.md](docs/PIPELINES_ARCHITECTURE.md) for detailed performance characteristics and implementation details.

## Dashboard

Web interface for monitoring at `http://localhost:5081`:

- **Active backend connections** — PostgreSQL load
- **Clients in queue** — waiting for slot
- **Wait time** — average and maximum
- **Timeouts** — clients that didn't get a slot

## Project Structure

```
pgbouncer.net/
├── src/
│   ├── PgBouncer.Core/        # Core: pooling, protocol
│   │   ├── Pooling/           # ConnectionPool, PoolManager
│   │   └── Protocol/          # PostgreSQL protocol
│   └── PgBouncer.Server/      # Server and Dashboard
│       ├── ClientSession.cs   # Legacy Stream-based client handling
│       ├── PipelinesClientSession.cs # Pipelines-based client handling
│       ├── ProxyServer.cs     # TCP server
│       └── wwwroot/           # Web interface
├── tests/
│   ├── PgBouncer.Tests/       # Unit tests
│   ├── PgBouncer.Benchmarks/  # Performance benchmarks
│   ├── PgBouncer.BattleTest/  # Production readiness tests
│   ├── PgBouncer.LoadTester/  # Load testing
│   └── PgBouncer.StressTester/# Stress testing
├── docs/
│   ├── PIPELINES_ARCHITECTURE.md  # System.IO.Pipelines documentation
│   ├── OPTIMIZATION_ROADMAP.md
│   └── archive/               # Historical reports
└── pgbouncer-bin/             # Original pgbouncer for benchmarking
```

## Benchmarking Against Original PgBouncer

To compare performance with the original C pgbouncer:

```bash
# 1. Start original pgbouncer (port 6433)
cd pgbouncer-bin/pgbouncer && ./pgbouncer.exe pgbouncer.ini

# 2. Start PgBouncer.NET (port 6432)
dotnet run --project src/PgBouncer.Server

# 3. Run load test
dotnet run --project tests/PgBouncer.LoadTester -- \
  --host localhost --port 6432 \
  --dynamic-stress --duration 60
```

## Status

**Current:** Alpha - Under active development

**Architecture:**
- Stream-based I/O (default, stable)
- System.IO.Pipelines I/O (experimental, opt-in via `UsePipelinesArchitecture=true`)
  - Zero-allocation message parsing
  - Reduced GC pressure under high load
  - See [PIPELINES_ARCHITECTURE.md](docs/PIPELINES_ARCHITECTURE.md) for details

**Known Issues:**
- Extended Query Protocol (prepared statements) support is basic
- COPY protocol not supported
- Some edge cases in transaction handling
- Pipelines architecture needs production validation

## Roadmap

1. Full Extended Query Protocol support
2. COPY protocol support
3. Performance optimization
4. Docker image
5. Kubernetes Helm chart

## License

[MIT License](LICENSE)
