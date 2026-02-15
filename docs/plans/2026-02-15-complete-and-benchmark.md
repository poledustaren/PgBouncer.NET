# Complete Implementation and Benchmark Against Original PgBouncer

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete the PgBouncer.NET implementation, fix bugs, and benchmark against original pgbouncer (C)

**Architecture:** 
- Current implementation uses System.IO.Pipelines for zero-allocation I/O
- Transaction pooling mode with lazy backend acquisition
- Channel-based connection pool with FIFO semantics
- Original pgbouncer runs on port 6433, .NET on 6432

**Tech Stack:** .NET 8.0, System.IO.Pipelines, Npgsql for testing, PostgreSQL 13+

---

## Current State Analysis

**What exists:**
- `BackendConnection.cs` - Pipelines-based backend connection with read/write loops
- `PipelinesClientSession.cs` - Client session handling startup, session/transaction pooling
- `ConnectionPool.cs` - Channel-based pool with circuit breaker
- `ProxyServer.cs` - TCP listener with connection management
- Original pgbouncer in `pgbouncer-bin/pgbouncer/` for comparison

**Known issues to fix:**
1. Pipelines architecture may have race conditions in backend release
2. Transaction state tracking incomplete
3. Error handling needs improvement
4. No proper benchmark comparison script

---

## Task 1: Fix Transaction Pooling Backend Release Race Condition

**Files:**
- Modify: `src/PgBouncer.Server/PipelinesClientSession.cs:718-738`

**Problem:** Backend release happens in Task.Run with delay, causing potential race conditions

**Step 1: Review current implementation**

Current code at line 718-738:
```csharp
if (_config.Pool.Mode == PoolingMode.Transaction && msgTypeChar == PgMessageTypes.ReadyForQuery)
{
    byte txState = (byte)'I';
    if (message.Length >= 6)
    {
        var dataSpan = new ReadOnlySpan<byte>(data);
        txState = dataSpan[5];
    }
    if (txState == 'I')
    {
        _ = Task.Run(async () =>
        {
            await Task.Delay(100);
            await ReleaseBackendAsync();
        });
    }
}
```

**Step 2: Fix the race condition - release synchronously after ensuring message is sent**

Replace the problematic code with:
```csharp
if (_config.Pool.Mode == PoolingMode.Transaction && msgTypeChar == PgMessageTypes.ReadyForQuery)
{
    byte txState = (byte)'I';
    if (message.Length >= 6)
    {
        txState = data[5];
    }
    
    if (txState == 'I')
    {
        _pendingBackendRelease = true;
    }
}

_backendToClientChannel.Writer.TryWrite(data);

if (_pendingBackendRelease)
{
    _pendingBackendRelease = false;
    _ = Task.Run(async () =>
    {
        try
        {
            await _clientWritePipe.Writer.FlushAsync();
            await ReleaseBackendAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Session {Id}] Failed to release backend", _sessionInfo.Id);
        }
    });
}

return ValueTask.CompletedTask;
```

**Step 3: Add field for pending release**

Add field to class:
```csharp
private bool _pendingBackendRelease;
```

**Step 4: Build and verify**

Run: `dotnet build src/PgBouncer.Server`
Expected: Build succeeded

---

## Task 2: Add Proper IBackendPool Interface

**Files:**
- Create: `src/PgBouncer.Core/Pooling/IBackendPool.cs`
- Modify: `src/PgBouncer.Core/PgBouncer.Core.csproj`

**Step 1: Create IBackendPool interface**

```csharp
namespace PgBouncer.Core.Pooling;

public interface IBackendPool
{
    Task<BackendConnection> AcquireAsync(CancellationToken cancellationToken = default);
    void Return(BackendConnection connection);
    PoolStats GetStats();
}
```

**Step 2: Build to verify**

Run: `dotnet build src/PgBouncer.Core`
Expected: Build succeeded

---

## Task 3: Create Benchmark Comparison Script

**Files:**
- Create: `tests/PgBouncer.Benchmarks/BenchmarkComparison.cs`

**Step 1: Create benchmark script**

```csharp
using System.Diagnostics;
using Npgsql;

namespace PgBouncer.Benchmarks;

public class BenchmarkComparison
{
    private const string NetPgbouncer = "Host=localhost;Port=6432;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";
    private const string OriginalPgbouncer = "Host=localhost;Port=6433;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";
    private const string DirectPg = "Host=localhost;Port=5437;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=30";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     PgBouncer.NET vs Original PgBouncer Benchmark        ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝\n");

        var iterations = args.Length > 0 ? int.Parse(args[0]) : 1000;
        var concurrency = args.Length > 1 ? int.Parse(args[1]) : 50;

        Console.WriteLine($"Iterations: {iterations}, Concurrency: {concurrency}\n");

        // Test direct PostgreSQL
        Console.WriteLine("Testing direct PostgreSQL connection (baseline)...");
        var directResult = await RunBenchmark("Direct PG", DirectPg, iterations, concurrency);
        
        // Test original pgbouncer
        Console.WriteLine("\nTesting original pgbouncer (port 6433)...");
        var originalResult = await RunBenchmark("Original", OriginalPgbouncer, iterations, concurrency);
        
        // Test .NET pgbouncer
        Console.WriteLine("\nTesting PgBouncer.NET (port 6432)...");
        var netResult = await RunBenchmark(".NET", NetPgbouncer, iterations, concurrency);

        // Print comparison
        Console.WriteLine("\n" + new string('=', 70));
        Console.WriteLine("RESULTS COMPARISON");
        Console.WriteLine(new string('=', 70));
        Console.WriteLine($"{"Target",-20} {"Avg (ms)",-12} {"P95 (ms)",-12} {"P99 (ms)",-12} {"QPS",-12}");
        Console.WriteLine(new string('-', 70));
        Console.WriteLine($"{"Direct PG",-20} {directResult.AvgMs,-12:F2} {directResult.P95Ms,-12:F2} {directResult.P99Ms,-12:F2} {directResult.Qps,-12:F0}");
        Console.WriteLine($"{"Original PgBouncer",-20} {originalResult.AvgMs,-12:F2} {originalResult.P95Ms,-12:F2} {originalResult.P99Ms,-12:F2} {originalResult.Qps,-12:F0}");
        Console.WriteLine($"{"PgBouncer.NET",-20} {netResult.AvgMs,-12:F2} {netResult.P95Ms,-12:F2} {netResult.P99Ms,-12:F2} {netResult.Qps,-12:F0}");
        
        Console.WriteLine(new string('-', 70));
        var overheadVsOriginal = ((netResult.AvgMs - originalResult.AvgMs) / originalResult.AvgMs * 100);
        var overheadVsDirect = ((netResult.AvgMs - directResult.AvgMs) / directResult.AvgMs * 100);
        Console.WriteLine($"\nPgBouncer.NET overhead vs Original: {overheadVsOriginal:+0.##;-0.##;0}%");
        Console.WriteLine($"PgBouncer.NET overhead vs Direct: {overheadVsDirect:+0.##;-0.##;0}%");
    }

    private static async Task<BenchmarkResult> RunBenchmark(string name, string connStr, int iterations, int concurrency)
    {
        var latencies = new List<double>();
        var errors = 0;
        var sw = Stopwatch.StartNew();
        
        var semaphore = new SemaphoreSlim(concurrency);
        var tasks = Enumerable.Range(0, iterations).Select(async _ =>
        {
            await semaphore.WaitAsync();
            var querySw = Stopwatch.StartNew();
            try
            {
                using var conn = new NpgsqlConnection(connStr);
                await conn.OpenAsync();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1";
                await cmd.ExecuteScalarAsync();
                querySw.Stop();
                lock (latencies) latencies.Add(querySw.ElapsedTotalMilliseconds);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                Console.WriteLine($"  Error: {ex.Message}");
            }
            finally
            {
                semaphore.Release();
            }
        }).ToArray();

        await Task.WhenAll(tasks);
        sw.Stop();

        latencies.Sort();
        
        return new BenchmarkResult
        {
            AvgMs = latencies.Count > 0 ? latencies.Average() : 0,
            P95Ms = latencies.Count > 0 ? latencies[(int)(latencies.Count * 0.95)] : 0,
            P99Ms = latencies.Count > 0 ? latencies[(int)(latencies.Count * 0.99)] : 0,
            Qps = iterations / sw.Elapsed.TotalSeconds,
            Errors = errors
        };
    }

    private record BenchmarkResult
    {
        public double AvgMs { get; init; }
        public double P95Ms { get; init; }
        public double P99Ms { get; init; }
        public double Qps { get; init; }
        public int Errors { get; init; }
    }
}
```

**Step 2: Add Benchmark target to csproj**

Add to `tests/PgBouncer.Benchmarks/PgBouncer.Benchmarks.csproj`:
```xml
<ItemGroup>
  <PackageReference Include="Npgsql" Version="8.0.0" />
</ItemGroup>
```

**Step 3: Build benchmark**

Run: `dotnet build tests/PgBouncer.Benchmarks`
Expected: Build succeeded

---

## Task 4: Create Simple Integration Test

**Files:**
- Create: `tests/PgBouncer.Tests/SimpleComparisonTest.cs`

**Step 1: Create simple test that verifies basic functionality**

```csharp
using Npgsql;
using Xunit;

namespace PgBouncer.Tests;

public class SimpleComparisonTest : IAsyncLifetime
{
    private const string NetPgbouncerConnStr = "Host=localhost;Port=6432;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=10";
    private const string OriginalPgbouncerConnStr = "Host=localhost;Port=6433;Database=postgres;Username=postgres;Password=123;Pooling=false;Timeout=10";

    public Task InitializeAsync() => Task.CompletedTask;
    
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task CanConnectToPgBouncerNet()
    {
        using var conn = new NpgsqlConnection(NetPgbouncerConnStr);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task CanConnectToOriginalPgBouncer()
    {
        using var conn = new NpgsqlConnection(OriginalPgbouncerConnStr);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task CanExecuteMultipleQueries()
    {
        using var conn = new NpgsqlConnection(NetPgbouncerConnStr);
        await conn.OpenAsync();
        
        for (int i = 0; i < 5; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT {i}";
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(i, result);
        }
    }

    [Fact]
    public async Task CanHandleConcurrentConnections()
    {
        var tasks = Enumerable.Range(0, 10).Select(async _ =>
        {
            using var conn = new NpgsqlConnection(NetPgbouncerConnStr);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT pg_backend_pid()";
            var pid = await cmd.ExecuteScalarAsync();
            Assert.NotNull(pid);
        }).ToArray();

        await Task.WhenAll(tasks);
    }
}
```

**Step 2: Run tests**

Run: `dotnet test tests/PgBouncer.Tests --filter "FullyQualifiedName~SimpleComparisonTest"`
Expected: All tests pass (requires running servers)

---

## Task 5: Create PowerShell Benchmark Runner

**Files:**
- Create: `run-benchmark.ps1`

**Step 1: Create comprehensive benchmark runner**

```powershell
#!/usr/bin/env pwsh
#Requires -Version 7

param(
    [int]$Iterations = 1000,
    [int]$Concurrency = 50,
    [int]$Duration = 60,
    [switch]$SkipOriginal,
    [switch]$SkipNet,
    [switch]$Verbose
)

$ErrorActionPreference = "Stop"

Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     PgBouncer.NET Benchmark Runner                       ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Check PostgreSQL is running
Write-Host "Checking PostgreSQL..." -ForegroundColor Yellow
try {
    $pgCheck = & psql -h localhost -p 5437 -U postgres -c "SELECT 1" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "PostgreSQL not responding on port 5437. Please start it first." -ForegroundColor Red
        exit 1
    }
    Write-Host "✓ PostgreSQL is running" -ForegroundColor Green
} catch {
    Write-Host "PostgreSQL check failed: $_" -ForegroundColor Red
    exit 1
}

# Start original pgbouncer if needed
if (-not $SkipOriginal) {
    Write-Host "Starting original pgbouncer on port 6433..." -ForegroundColor Yellow
    $originalProcess = Start-Process -FilePath ".\pgbouncer-bin\pgbouncer\pgbouncer.exe" -ArgumentList ".\pgbouncer-bin\pgbouncer\pgbouncer.ini" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 2
    
    # Verify it started
    try {
        $test = & psql -h localhost -p 6433 -U postgres -c "SELECT 1" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Original pgbouncer started (PID: $($originalProcess.Id))" -ForegroundColor Green
        } else {
            Write-Host "⚠ Original pgbouncer may not have started properly" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "⚠ Could not verify original pgbouncer" -ForegroundColor Yellow
    }
}

# Build .NET version
if (-not $SkipNet) {
    Write-Host "Building PgBouncer.NET..." -ForegroundColor Yellow
    $build = dotnet build src/PgBouncer.Server -c Release 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed!" -ForegroundColor Red
        Write-Host $build
        exit 1
    }
    Write-Host "✓ Build succeeded" -ForegroundColor Green

    # Start .NET pgbouncer
    Write-Host "Starting PgBouncer.NET on port 6432..." -ForegroundColor Yellow
    $netProcess = Start-Process -FilePath "dotnet" -ArgumentList "run --project src/PgBouncer.Server -c Release --no-build" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 3
    
    # Verify it started
    try {
        $test = & psql -h localhost -p 6432 -U postgres -c "SELECT 1" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ PgBouncer.NET started (PID: $($netProcess.Id))" -ForegroundColor Green
        } else {
            Write-Host "⚠ PgBouncer.NET may not have started properly" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "⚠ Could not verify PgBouncer.NET" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "Running benchmarks..." -ForegroundColor Cyan
Write-Host "Iterations: $Iterations, Concurrency: $Concurrency" -ForegroundColor Gray
Write-Host ""

# Run load test against both
$env:PGPASSWORD = "123"

Write-Host "Testing PgBouncer.NET (port 6432)..." -ForegroundColor Yellow
dotnet run --project tests/PgBouncer.LoadTester -c Release --no-build -- `
    --host localhost --port 6432 `
    --connections $Concurrency --total $Iterations `
    --queries 1 --duration $Duration

if (-not $SkipOriginal) {
    Write-Host ""
    Write-Host "Testing Original PgBouncer (port 6433)..." -ForegroundColor Yellow
    dotnet run --project tests/PgBouncer.LoadTester -c Release --no-build -- `
        --host localhost --port 6433 `
        --connections $Concurrency --total $Iterations `
        --queries 1 --duration $Duration
}

# Cleanup
Write-Host ""
Write-Host "Cleaning up..." -ForegroundColor Yellow

if ($originalProcess) {
    Stop-Process -Id $originalProcess.Id -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Stopped original pgbouncer" -ForegroundColor Green
}

if ($netProcess) {
    Stop-Process -Id $netProcess.Id -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Stopped PgBouncer.NET" -ForegroundColor Green
}

Write-Host ""
Write-Host "Benchmark complete!" -ForegroundColor Green
```

**Step 2: Test the script**

Run: `pwsh -File run-benchmark.ps1 -Iterations 100 -Concurrency 10 -Duration 10`
Expected: Script runs and shows comparison

---

## Task 6: Document Known Issues and Limitations

**Files:**
- Modify: `docs/PIPELINES_ARCHITECTURE.md`

**Step 1: Add known issues section**

Add to end of file:
```markdown
## Known Issues and Limitations

### Transaction Pooling
- Backend release after ReadyForQuery has small race window
- Extended Query Protocol (prepared statements) support is basic
- COPY protocol not supported

### Performance
- Initial connection has higher latency than original pgbouncer
- Memory usage under extreme load needs monitoring

### Compatibility
- SSL connections not yet supported
- Only trust authentication tested
- cancel_request not implemented
```

---

## Task 7: Run Full Benchmark Suite

**Step 1: Ensure PostgreSQL is running**

Run: `psql -h localhost -p 5437 -U postgres -c "SELECT version()"`

**Step 2: Start original pgbouncer**

Run: `cd pgbouncer-bin/pgbouncer && ./pgbouncer.exe pgbouncer.ini &`

**Step 3: Build and start .NET pgbouncer**

Run: 
```bash
dotnet build src/PgBouncer.Server -c Release
dotnet run --project src/PgBouncer.Server -c Release --no-build &
```

**Step 4: Run benchmark**

Run: 
```bash
dotnet run --project tests/PgBouncer.LoadTester -c Release -- \
  --dynamic-stress --duration 60 --host localhost --port 6432
```

**Step 5: Compare with original**

Run:
```bash
dotnet run --project tests/PgBouncer.LoadTester -c Release -- \
  --dynamic-stress --duration 60 --host localhost --port 6433
```

---

## Verification Checklist

Before marking complete:

- [ ] `dotnet build` succeeds for all projects
- [ ] `dotnet test tests/PgBouncer.Tests` passes (with running servers)
- [ ] Can connect through .NET pgbouncer with psql
- [ ] Can connect through .NET pgbouncer with Npgsql
- [ ] Load test runs without errors
- [ ] Memory usage stays stable under load
- [ ] Benchmark results documented
