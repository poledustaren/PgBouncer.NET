using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PgBouncer.BattleTest.Config;
using PgBouncer.BattleTest.Models;
using PgBouncer.BattleTest.Validators;

namespace PgBouncer.BattleTest.Executors;

public class WaveController
{
    private readonly TestConfiguration _config;
    private readonly List<ClientSimulator> _clients;
    private readonly List<WaveMetrics> _waveMetrics;
    private readonly ConsistencyValidator _consistencyValidator;
    private int _currentWave;
    private bool _shouldStop;
    private string _terminationReason = "";

    public IReadOnlyList<WaveMetrics> WaveMetrics => _waveMetrics;
    public int CurrentWave => _currentWave;
    public bool ShouldStop => _shouldStop;
    public string TerminationReason => _terminationReason;
    public int ActiveClientCount => _clients.Count;

    public WaveController(TestConfiguration config)
    {
        _config = config;
        _clients = new List<ClientSimulator>();
        _waveMetrics = new List<WaveMetrics>();
        _consistencyValidator = new ConsistencyValidator(config);
    }

    public async Task<BattleTestReport> RunBattleTestAsync(CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        
        Console.WriteLine($"=== PgBouncer Battle Test Started ===");
        Console.WriteLine($"Session ID: {_config.SessionId}");
        Console.WriteLine($"Max Duration: {_config.MaxTotalDuration.TotalMinutes:F0} minutes");
        Console.WriteLine($"Max Clients: {_config.MaxClients}");
        Console.WriteLine();

        try
        {
            // Start with initial clients
            await AddClientsAsync(_config.InitialClients, cancellationToken);

            while (!_shouldStop && stopwatch.Elapsed < _config.MaxTotalDuration)
            {
                _currentWave++;
                
                Console.WriteLine($"\n--- Wave {_currentWave} ---");
                Console.WriteLine($"Active Clients: {_clients.Count}");
                Console.WriteLine($"Target Ops/Sec/Client: {_config.TargetOperationsPerSecondPerClient}");
                
                // Run the wave
                var waveMetrics = await RunWaveAsync(cancellationToken);
                _waveMetrics.Add(waveMetrics);
                
                // Display wave results
                DisplayWaveResults(waveMetrics);

                // Check termination conditions
                if (ShouldTerminate(waveMetrics))
                {
                    break;
                }

                // Add more clients for next wave if we haven't reached max
                if (_clients.Count < _config.MaxClients)
                {
                    var clientsToAdd = Math.Min(
                        _config.ClientsPerWaveIncrement, 
                        _config.MaxClients - _clients.Count
                    );
                    
                    await AddClientsAsync(clientsToAdd, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _terminationReason = "Cancelled by user";
            Console.WriteLine("\nTest cancelled by user.");
        }
        catch (Exception ex)
        {
            _terminationReason = $"Error: {ex.Message}";
            Console.WriteLine($"\nTest terminated due to error: {ex.Message}");
        }
        finally
        {
            stopwatch.Stop();
        }

        // Generate final report
        var endTime = DateTime.UtcNow;
        var finalConsistencyReport = await _consistencyValidator.ValidateAllDatabasesAsync(cancellationToken);
        
        return GenerateFinalReport(startTime, endTime, finalConsistencyReport);
    }

    private async Task AddClientsAsync(int count, CancellationToken cancellationToken)
    {
        var startId = _clients.Count;
        
        for (int i = 0; i < count; i++)
        {
            var clientId = startId + i;
            var client = new ClientSimulator(clientId, _config);
            _clients.Add(client);
        }
        
        Console.WriteLine($"Added {count} clients. Total: {_clients.Count}");
    }

    private async Task<WaveMetrics> RunWaveAsync(CancellationToken cancellationToken)
    {
        var waveStartTime = DateTime.UtcNow;
        var waveStopwatch = Stopwatch.StartNew();
        var waveCancellationToken = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, 
            new CancellationTokenSource(TimeSpan.FromSeconds(_config.WaveDurationSeconds)).Token
        ).Token;

        // Reset client metrics for this wave
        foreach (var client in _clients)
        {
            client.ResetMetrics();
        }

        // Calculate delay between operations to achieve target rate
        var operationDelayMs = 1000.0 / _config.TargetOperationsPerSecondPerClient;

        // Run all clients concurrently
        var clientTasks = _clients.Select(client => 
            RunClientAsync(client, waveCancellationToken, operationDelayMs)
        ).ToList();

        try
        {
            await Task.WhenAll(clientTasks);
        }
        catch (OperationCanceledException)
        {
            // Expected when wave duration expires
        }

        waveStopwatch.Stop();
        var waveEndTime = DateTime.UtcNow;

        // Aggregate metrics from all clients
        var aggregatedMetrics = AggregateClientMetrics(waveStartTime, waveStopwatch.Elapsed);
        
        // Perform consistency check
        var consistencyReport = await _consistencyValidator.ValidateAllDatabasesAsync(cancellationToken);

        return new WaveMetrics
        {
            WaveNumber = _currentWave,
            Timestamp = waveStartTime,
            Duration = waveStopwatch.Elapsed,
            TotalOperations = aggregatedMetrics.TotalOperations,
            SuccessfulOperations = aggregatedMetrics.SuccessfulOperations,
            AverageLatencyMs = aggregatedMetrics.AverageLatencyMs,
            P50LatencyMs = aggregatedMetrics.P50LatencyMs,
            P95LatencyMs = aggregatedMetrics.P95LatencyMs,
            P99LatencyMs = aggregatedMetrics.P99LatencyMs,
            ActiveClients = _clients.Count,
            NewClientsAdded = _currentWave == 1 ? _config.InitialClients : 
                Math.Min(_config.ClientsPerWaveIncrement, _config.MaxClients - _clients.Count + _config.ClientsPerWaveIncrement),
            TargetOperationsPerSecond = _config.TargetOperationsPerSecondPerClient * _clients.Count,
            ConsistencyCheck = consistencyReport
        };
    }

    private async Task RunClientAsync(ClientSimulator client, CancellationToken cancellationToken, double operationDelayMs)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await client.RunCrudCycleAsync(_config.SessionId, _currentWave, cancellationToken);
            
            // Throttle to achieve target rate
            await Task.Delay(TimeSpan.FromMilliseconds(operationDelayMs), cancellationToken);
        }
    }

    private TestMetrics AggregateClientMetrics(DateTime startTime, TimeSpan duration)
    {
        var allLatencies = new List<TimeSpan>();
        var totalOperations = 0;
        var successfulOperations = 0;

        foreach (var client in _clients)
        {
            allLatencies.AddRange(client.LatencyMeasurements);
            totalOperations += client.TotalOperations;
            successfulOperations += client.SuccessfulOperations;
        }

        allLatencies.Sort((a, b) => a.CompareTo(b));

        return new TestMetrics
        {
            Timestamp = startTime,
            Duration = duration,
            TotalOperations = totalOperations,
            SuccessfulOperations = successfulOperations,
            AverageLatencyMs = allLatencies.Count > 0 ? allLatencies.Average(l => l.TotalMilliseconds) : 0,
            P50LatencyMs = CalculatePercentile(allLatencies, 0.5),
            P95LatencyMs = CalculatePercentile(allLatencies, 0.95),
            P99LatencyMs = CalculatePercentile(allLatencies, 0.99),
            ActiveClients = _clients.Count,
            TargetOperationsPerSecond = _config.TargetOperationsPerSecondPerClient * _clients.Count
        };
    }

    private static double CalculatePercentile(List<TimeSpan> latencies, double percentile)
    {
        if (latencies.Count == 0) return 0;
        
        var index = (int)Math.Ceiling(latencies.Count * percentile) - 1;
        index = Math.Max(0, Math.Min(index, latencies.Count - 1));
        
        return latencies[index].TotalMilliseconds;
    }

    private bool ShouldTerminate(WaveMetrics waveMetrics)
    {
        // Check success rate
        if (waveMetrics.SuccessRate < _config.MinimumSuccessRate * 100)
        {
            _shouldStop = true;
            _terminationReason = $"Success rate dropped to {waveMetrics.SuccessRate:F2}% (minimum: {_config.MinimumSuccessRate * 100:F0}%)";
            return true;
        }

        // Check P95 latency
        if (waveMetrics.P95LatencyMs > _config.MaxAcceptableP95LatencyMs)
        {
            _shouldStop = true;
            _terminationReason = $"P95 latency exceeded {waveMetrics.P95LatencyMs:F0}ms (maximum: {_config.MaxAcceptableP95LatencyMs:F0}ms)";
            return true;
        }

        // Check consistency
        if (waveMetrics.ConsistencyCheck != null && !waveMetrics.ConsistencyCheck.IsConsistent)
        {
            _shouldStop = true;
            _terminationReason = $"Consistency check failed with {waveMetrics.ConsistencyCheck.Violations.Count} violations";
            return true;
        }

        return false;
    }

    private void DisplayWaveResults(WaveMetrics waveMetrics)
    {
        Console.WriteLine($"  Duration: {waveMetrics.Duration.TotalSeconds:F1}s");
        Console.WriteLine($"  Operations: {waveMetrics.SuccessfulOperations:N0} / {waveMetrics.TotalOperations:N0} ({waveMetrics.SuccessRate:F2}% success)");
        Console.WriteLine($"  Throughput: {waveMetrics.ActualOperationsPerSecond:F1} ops/sec");
        Console.WriteLine($"  Latency: Avg={waveMetrics.AverageLatencyMs:F1}ms, P50={waveMetrics.P50LatencyMs:F1}ms, P95={waveMetrics.P95LatencyMs:F1}ms, P99={waveMetrics.P99LatencyMs:F1}ms");
        
        if (waveMetrics.ConsistencyCheck != null)
        {
            var status = waveMetrics.ConsistencyCheck.IsConsistent ? "PASS" : "FAIL";
            Console.WriteLine($"  Consistency: {status} ({waveMetrics.ConsistencyCheck.ValidRecords:N0}/{waveMetrics.ConsistencyCheck.TotalRecords:N0} records valid)");
        }

        // Show sample errors if any
        var sampleErrors = _clients.SelectMany(c => c.Errors).Take(3).ToList();
        if (sampleErrors.Count > 0)
        {
            Console.WriteLine($"  Sample errors:");
            foreach (var error in sampleErrors)
            {
                Console.WriteLine($"    - {error.GetType().Name}: {error.Message}");
            }
        }

        if (_shouldStop)
        {
            Console.WriteLine($"  *** STOPPING: {_terminationReason} ***");
        }
    }

    private BattleTestReport GenerateFinalReport(DateTime startTime, DateTime endTime, ConsistencyReport finalConsistency)
    {
        var totalOps = _waveMetrics.Sum(w => w.TotalOperations);
        var totalSuccess = _waveMetrics.Sum(w => w.SuccessfulOperations);
        var allLatencies = _waveMetrics.SelectMany(w => 
        {
            // Create dummy TimeSpan list from metrics
            var latencies = new List<TimeSpan>();
            for (int i = 0; i < w.TotalOperations; i++)
            {
                latencies.Add(TimeSpan.FromMilliseconds(w.AverageLatencyMs));
            }
            return latencies;
        }).ToList();

        var avgLatency = _waveMetrics.Count > 0 ? _waveMetrics.Average(w => w.AverageLatencyMs) : 0;
        var p95Latency = _waveMetrics.Count > 0 ? _waveMetrics.Max(w => w.P95LatencyMs) : 0;
        var p99Latency = _waveMetrics.Count > 0 ? _waveMetrics.Max(w => w.P99LatencyMs) : 0;

        var grade = CalculateGrade(totalSuccess, totalOps, avgLatency, finalConsistency);
        var recommendations = GenerateRecommendations(grade, _waveMetrics, finalConsistency);

        if (string.IsNullOrEmpty(_terminationReason))
        {
            _terminationReason = "Completed all waves";
        }

        return new BattleTestReport
        {
            SessionId = _config.SessionId,
            StartedAt = startTime,
            EndedAt = endTime,
            TerminationReason = _terminationReason,
            TotalWaves = _currentWave,
            PeakClients = _waveMetrics.Count > 0 ? _waveMetrics.Max(w => w.ActiveClients) : 0,
            TotalOperations = totalOps,
            TotalSuccessfulOperations = totalSuccess,
            MinimumSuccessRate = _config.MinimumSuccessRate * 100,
            AverageLatencyMs = avgLatency,
            P95LatencyMs = p95Latency,
            P99LatencyMs = p99Latency,
            FinalConsistencyCheckPassed = finalConsistency?.IsConsistent ?? false,
            ConsistencyViolations = finalConsistency?.Violations.Count ?? 0,
            Waves = new List<WaveMetrics>(_waveMetrics),
            FinalConsistencyReport = finalConsistency,
            Grade = grade,
            Recommendations = recommendations
        };
    }

    private static PerformanceGrade CalculateGrade(int success, int total, double avgLatency, ConsistencyReport consistency)
    {
        var successRate = total > 0 ? (double)success / total : 0;
        
        if (!consistency.IsConsistent || successRate < 0.85 || avgLatency > 1000)
            return PerformanceGrade.F;
        
        if (successRate >= 0.99 && avgLatency < 50 && consistency.Violations.Count == 0)
            return PerformanceGrade.APlus;
        
        if (successRate >= 0.98 && avgLatency < 100)
            return PerformanceGrade.A;
        
        if (successRate >= 0.95 && avgLatency < 250)
            return PerformanceGrade.B;
        
        if (successRate >= 0.90 && avgLatency < 500)
            return PerformanceGrade.C;
        
        if (successRate >= 0.85 && avgLatency < 1000)
            return PerformanceGrade.D;
        
        return PerformanceGrade.F;
    }

    private static List<string> GenerateRecommendations(PerformanceGrade grade, List<WaveMetrics> waves, ConsistencyReport consistency)
    {
        var recommendations = new List<string>();

        if (grade == PerformanceGrade.F)
        {
            recommendations.Add("CRITICAL: PgBouncer is not production-ready under these conditions.");
        }

        if (!consistency.IsConsistent)
        {
            recommendations.Add($"Fix {consistency.Violations.Count} consistency violations immediately.");
        }

        if (waves.Count > 0)
        {
            var lastWave = waves.Last();
            if (lastWave.P95LatencyMs > 500)
            {
                recommendations.Add("High latency detected. Consider connection pool tuning.");
            }

            if (lastWave.SuccessRate < 95)
            {
                recommendations.Add("Low success rate. Check for connection errors or timeouts.");
            }

            var peakWave = waves.OrderByDescending(w => w.ActiveClients).FirstOrDefault();
            if (peakWave != null)
            {
                recommendations.Add($"Peak performance achieved at {peakWave.ActiveClients} clients.");
            }
        }

        if (grade >= PerformanceGrade.A)
        {
            recommendations.Add("Excellent performance! PgBouncer is production-ready.");
        }

        return recommendations;
    }
}
