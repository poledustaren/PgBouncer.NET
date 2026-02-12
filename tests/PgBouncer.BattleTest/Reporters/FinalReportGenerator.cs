using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PgBouncer.BattleTest.Models;

namespace PgBouncer.BattleTest.Reporters;

public class FinalReportGenerator
{
    public async Task GenerateReportAsync(BattleTestReport report, string outputPath)
    {
        var sb = new StringBuilder();
        
        // Header
        sb.AppendLine("# PgBouncer Battle Test Report");
        sb.AppendLine();
        sb.AppendLine($"**Session ID:** {report.SessionId}");
        sb.AppendLine($"**Date:** {report.StartedAt:yyyy-MM-dd HH:mm:ss UTC}");
        sb.AppendLine($"**Duration:** {FormatDuration(report.Duration)}");
        sb.AppendLine($"**Termination Reason:** {report.TerminationReason}");
        sb.AppendLine();

        // Grade Section
        sb.AppendLine("## Performance Grade");
        sb.AppendLine();
        var gradeEmoji = GetGradeEmoji(report.Grade);
        sb.AppendLine($"# {gradeEmoji} Grade: {FormatGrade(report.Grade)}");
        sb.AppendLine();
        sb.AppendLine(GetGradeDescription(report.Grade));
        sb.AppendLine();

        // Executive Summary
        sb.AppendLine("## Executive Summary");
        sb.AppendLine();
        sb.AppendLine("| Metric | Value |");
        sb.AppendLine("|--------|-------|");
        sb.AppendLine($"| Total Waves | {report.TotalWaves} |");
        sb.AppendLine($"| Peak Concurrent Clients | {report.PeakClients} |");
        sb.AppendLine($"| Total Operations | {report.TotalOperations:N0} |");
        sb.AppendLine($"| Successful Operations | {report.TotalSuccessfulOperations:N0} |");
        sb.AppendLine($"| Failed Operations | {report.TotalFailedOperations:N0} |");
        sb.AppendLine($"| Overall Success Rate | {report.OverallSuccessRate:F2}% |");
        sb.AppendLine($"| Minimum Success Rate | {report.MinimumSuccessRate:F0}% |");
        sb.AppendLine($"| Average Latency | {report.AverageLatencyMs:F1} ms |");
        sb.AppendLine($"| P95 Latency | {report.P95LatencyMs:F1} ms |");
        sb.AppendLine($"| P99 Latency | {report.P99LatencyMs:F1} ms |");
        sb.AppendLine($"| Final Consistency Check | {(report.FinalConsistencyCheckPassed ? "PASS" : "FAIL")} |");
        sb.AppendLine($"| Consistency Violations | {report.ConsistencyViolations} |");
        sb.AppendLine();

        // Wave Details
        if (report.Waves.Count > 0)
        {
            sb.AppendLine("## Wave-by-Wave Breakdown");
            sb.AppendLine();
            sb.AppendLine("| Wave | Clients | Duration | Operations | Success % | P50 (ms) | P95 (ms) | P99 (ms) | Throughput | Consistency |");
            sb.AppendLine("|------|---------|----------|------------|-----------|----------|----------|----------|------------|-------------|");

            foreach (var wave in report.Waves)
            {
                var consistency = wave.ConsistencyCheck?.IsConsistent ?? false ? "PASS" : "FAIL";
                sb.AppendLine($"| {wave.WaveNumber} | {wave.ActiveClients} | {wave.Duration.TotalSeconds:F0}s | {wave.SuccessfulOperations:N0}/{wave.TotalOperations:N0} | {wave.SuccessRate:F1}% | {wave.P50LatencyMs:F0} | {wave.P95LatencyMs:F0} | {wave.P99LatencyMs:F0} | {wave.ActualOperationsPerSecond:F1}/s | {consistency} |");
            }
            sb.AppendLine();
        }

        // Consistency Report
        if (report.FinalConsistencyReport != null)
        {
            sb.AppendLine("## Consistency Validation");
            sb.AppendLine();
            sb.AppendLine($"**Status:** {(report.FinalConsistencyReport.IsConsistent ? "CONSISTENT" : "INCONSISTENT")}");
            sb.AppendLine();
            sb.AppendLine("| Check | Count |");
            sb.AppendLine("|-------|-------|");
            sb.AppendLine($"| Total Records | {report.FinalConsistencyReport.TotalRecords:N0} |");
            sb.AppendLine($"| Valid Records | {report.FinalConsistencyReport.ValidRecords:N0} |");
            sb.AppendLine($"| Invalid Checksums | {report.FinalConsistencyReport.InvalidChecksums:N0} |");
            sb.AppendLine($"| Orphaned Inserts | {report.FinalConsistencyReport.OrphanedInserts:N0} |");
            sb.AppendLine($"| Ghost Deletes | {report.FinalConsistencyReport.GhostDeletes:N0} |");
            sb.AppendLine($"| Cross-DB Inconsistencies | {report.FinalConsistencyReport.CrossDatabaseInconsistencies:N0} |");
            sb.AppendLine();

            if (report.FinalConsistencyReport.Violations.Count > 0)
            {
                sb.AppendLine("### Consistency Violations");
                sb.AppendLine();
                sb.AppendLine("| Type | Database | Record ID | Details |");
                sb.AppendLine("|------|----------|-----------|---------|");

                foreach (var violation in report.FinalConsistencyReport.Violations.Take(100))
                {
                    sb.AppendLine($"| {violation.Type} | {violation.DatabaseName} | {violation.RecordId} | {violation.Details} |");
                }

                if (report.FinalConsistencyReport.Violations.Count > 100)
                {
                    sb.AppendLine($"| ... | ... | ... | *{report.FinalConsistencyReport.Violations.Count - 100} more violations* |");
                }
                sb.AppendLine();
            }
        }

        // Recommendations
        if (report.Recommendations.Count > 0)
        {
            sb.AppendLine("## Recommendations");
            sb.AppendLine();
            foreach (var recommendation in report.Recommendations)
            {
                sb.AppendLine($"- {recommendation}");
            }
            sb.AppendLine();
        }

        // Grade Scale
        sb.AppendLine("## Grade Scale");
        sb.AppendLine();
        sb.AppendLine("| Grade | Criteria |");
        sb.AppendLine("|-------|----------|");
        sb.AppendLine("| **A+** | 99%+ success rate, <50ms avg latency, 0 consistency issues |");
        sb.AppendLine("| **A** | 98-99% success rate, <100ms avg latency |");
        sb.AppendLine("| **B** | 95-98% success rate, <250ms avg latency |");
        sb.AppendLine("| **C** | 90-95% success rate, <500ms avg latency |");
        sb.AppendLine("| **D** | 85-90% success rate, <1000ms avg latency |");
        sb.AppendLine("| **F** | <85% success rate OR >1000ms avg latency OR consistency failures |");
        sb.AppendLine();

        // Footer
        sb.AppendLine("---");
        sb.AppendLine($"*Report generated by PgBouncer Battle Test on {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss UTC}*");

        // Write to file
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(outputPath, sb.ToString());
    }

    private static string FormatDuration(TimeSpan duration)
    {
        if (duration.TotalHours >= 1)
            return $"{duration.TotalHours:F1} hours";
        if (duration.TotalMinutes >= 1)
            return $"{duration.TotalMinutes:F1} minutes";
        return $"{duration.TotalSeconds:F1} seconds";
    }

    private static string FormatGrade(PerformanceGrade grade)
    {
        return grade switch
        {
            PerformanceGrade.APlus => "A+",
            PerformanceGrade.A => "A",
            PerformanceGrade.B => "B",
            PerformanceGrade.C => "C",
            PerformanceGrade.D => "D",
            PerformanceGrade.F => "F",
            _ => grade.ToString()
        };
    }

    private static string GetGradeEmoji(PerformanceGrade grade)
    {
        return grade switch
        {
            PerformanceGrade.APlus => "🌟",
            PerformanceGrade.A => "✅",
            PerformanceGrade.B => "👍",
            PerformanceGrade.C => "⚠️",
            PerformanceGrade.D => "🔶",
            PerformanceGrade.F => "❌",
            _ => "❓"
        };
    }

    private static string GetGradeDescription(PerformanceGrade grade)
    {
        return grade switch
        {
            PerformanceGrade.APlus => "**Excellent!** PgBouncer performed flawlessly with exceptional reliability and speed.",
            PerformanceGrade.A => "**Great!** PgBouncer performed very well with high reliability.",
            PerformanceGrade.B => "**Good.** PgBouncer performed well but there's room for optimization.",
            PerformanceGrade.C => "**Fair.** PgBouncer is functional but may need tuning for production use.",
            PerformanceGrade.D => "**Poor.** PgBouncer struggled under load. Significant improvements needed.",
            PerformanceGrade.F => "**Failed.** PgBouncer is NOT production-ready. Critical issues must be resolved.",
            _ => "Unknown grade."
        };
    }
}
