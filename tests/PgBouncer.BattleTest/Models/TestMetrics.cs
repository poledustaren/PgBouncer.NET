using System;
using System.Collections.Generic;

namespace PgBouncer.BattleTest.Models;

public record TestMetrics
{
    public DateTime Timestamp { get; init; }
    public TimeSpan Duration { get; init; }
    public int TotalOperations { get; init; }
    public int SuccessfulOperations { get; init; }
    public int FailedOperations => TotalOperations - SuccessfulOperations;
    public double SuccessRate => TotalOperations > 0 ? (double)SuccessfulOperations / TotalOperations * 100 : 0;
    public double AverageLatencyMs { get; init; }
    public double P50LatencyMs { get; init; }
    public double P95LatencyMs { get; init; }
    public double P99LatencyMs { get; init; }
    public int ActiveClients { get; init; }
    public int TargetOperationsPerSecond { get; init; }
    public int ActualOperationsPerSecond => TotalOperations > 0 && Duration.TotalSeconds > 0 
        ? (int)(TotalOperations / Duration.TotalSeconds) 
        : 0;
}

public record WaveMetrics : TestMetrics
{
    public int WaveNumber { get; init; }
    public int NewClientsAdded { get; init; }
    public ConsistencyReport? ConsistencyCheck { get; init; }
}

public record ConsistencyReport
{
    public DateTime CheckedAt { get; set; }
    public int TotalRecords { get; set; }
    public int ValidRecords { get; set; }
    public int InvalidChecksums { get; set; }
    public int OrphanedInserts { get; set; }
    public int GhostDeletes { get; set; }
    public int CrossDatabaseInconsistencies { get; set; }
    public bool IsConsistent => InvalidChecksums == 0 && OrphanedInserts == 0 && GhostDeletes == 0 && CrossDatabaseInconsistencies == 0;
    public List<ConsistencyViolation> Violations { get; set; } = new();
}

public record ConsistencyViolation
{
    public string Type { get; init; } = string.Empty;
    public Guid RecordId { get; init; }
    public string DatabaseName { get; init; } = string.Empty;
    public string Details { get; init; } = string.Empty;
}

public record BattleTestReport
{
    public Guid SessionId { get; init; } = Guid.NewGuid();
    public DateTime StartedAt { get; init; }
    public DateTime? EndedAt { get; init; }
    public TimeSpan Duration => EndedAt.HasValue ? EndedAt.Value - StartedAt : TimeSpan.Zero;
    public string TerminationReason { get; init; } = string.Empty;
    public int TotalWaves { get; init; }
    public int PeakClients { get; init; }
    public int TotalOperations { get; init; }
    public int TotalSuccessfulOperations { get; init; }
    public int TotalFailedOperations => TotalOperations - TotalSuccessfulOperations;
    public double OverallSuccessRate => TotalOperations > 0 ? (double)TotalSuccessfulOperations / TotalOperations * 100 : 0;
    public double MinimumSuccessRate { get; init; }
    public double AverageLatencyMs { get; init; }
    public double P95LatencyMs { get; init; }
    public double P99LatencyMs { get; init; }
    public bool FinalConsistencyCheckPassed { get; init; }
    public int ConsistencyViolations { get; init; }
    public List<WaveMetrics> Waves { get; init; } = new();
    public ConsistencyReport? FinalConsistencyReport { get; init; }
    public PerformanceGrade Grade { get; init; }
    public List<string> Recommendations { get; init; } = new();
}

public enum PerformanceGrade
{
    APlus,  // 99%+ success, <50ms avg latency, no consistency issues
    A,      // 98-99% success, <100ms avg latency
    B,      // 95-98% success, <250ms avg latency
    C,      // 90-95% success, <500ms avg latency
    D,      // 85-90% success, <1000ms avg latency
    F       // <85% success or >1000ms avg latency or consistency failures
}
