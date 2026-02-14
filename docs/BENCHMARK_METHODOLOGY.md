# Benchmark Methodology

This document describes the testing methodology for comparing PgBouncer.NET with the original PgBouncer.

## Test Environment

### Hardware Requirements
- CPU: 4+ cores
- RAM: 8GB+
- Network: localhost (minimal latency)

### Software Requirements
- .NET 8.0 SDK
- PostgreSQL 13+
- Original PgBouncer (for comparison)

## Configuration

### PgBouncer.NET (Port 6432)
```json
{
  "Pool": {
    "MaxSize": 100,
    "Mode": "Transaction",
    "ServerResetQuery": "DISCARD ALL"
  }
}
```

### Original PgBouncer (Port 6433)
```ini
pool_mode = transaction
default_pool_size = 100
server_reset_query = DISCARD ALL
```

## Test Scenarios

### Tier 1: Baseline (Required)
- **Clients:** 100 concurrent
- **Queries:** 10,000 total
- **Query:** `SELECT 1`
- **Metrics:** Throughput, Latency P50/P95/P99

### Tier 2: Stress
- **Clients:** 1000 concurrent (ramp-up)
- **Duration:** 60 seconds
- **Query:** Mixed (SELECT, INSERT, UPDATE)
- **Metrics:** Success rate, Breaking point

### Tier 3: Long-running
- **Clients:** 500 concurrent
- **Duration:** 1 hour
- **Metrics:** Memory leaks, Stability

## Metrics

| Metric            | Target vs Original |
|-------------------|-------------------|
| Throughput        | ≥90%              |
| Latency P50       | ≤110%             |
| Latency P99       | ≤120%             |
| Connection Time   | ≤105%             |
| Memory            | ≤200%             |
| CPU               | ≤150%             |

## Running Benchmarks

### Start Both Poolers
```bash
# Original PgBouncer
cd pgbouncer-bin/pgbouncer && ./pgbouncer.exe pgbouncer.ini

# PgBouncer.NET
dotnet run --project src/PgBouncer.Server
```

### Run Load Test
```bash
# Against PgBouncer.NET
dotnet run --project tests/PgBouncer.LoadTester -- \
  --host localhost --port 6432 \
  --database fuel --user postgres --password 123 \
  --dynamic-stress --duration 60

# Against Original PgBouncer
dotnet run --project tests/PgBouncer.LoadTester -- \
  --host localhost --port 6433 \
  --database fuel --user postgres --password 123 \
  --dynamic-stress --duration 60
```

## Success Criteria

- **Production Ready:** Grade A or B
- **Acceptable:** Grade C (needs improvement)
- **Not Ready:** Grade D or F

### Grade Scale

| Grade | Criteria                                    |
|-------|---------------------------------------------|
| A+    | 99%+ success, <50ms avg latency             |
| A     | 98-99% success, <100ms avg latency          |
| B     | 95-98% success, <250ms avg latency          |
| C     | 90-95% success, <500ms avg latency          |
| D     | 85-90% success, <1000ms avg latency         |
| F     | <85% success OR >1000ms latency             |

## Known Issues

1. Extended Query Protocol support is basic
2. COPY protocol not implemented
3. Some prepared statement edge cases

## Improvement Process

1. Run benchmark → Analyze errors
2. Make minimal code changes
3. Verify with unit tests
4. Re-run benchmark
5. Commit only if all tests pass

## Self-Check Before Release

- [ ] `dotnet build` — no errors
- [ ] `dotnet test` — all tests pass
- [ ] Success rate ≥95% at 500 clients
- [ ] Latency P99 ≤200ms under load
- [ ] 1-hour stress test without crash
- [ ] Memory stable (no leaks)
