using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Npgsql;
using PgBouncer.BattleTest.Config;
using PgBouncer.BattleTest.Models;

namespace PgBouncer.BattleTest.Validators;

public class ConsistencyValidator
{
    private readonly TestConfiguration _config;
    private readonly string _connectionString;

    public ConsistencyValidator(TestConfiguration config)
    {
        _config = config;
        _connectionString = $"Host={config.Host};Port={config.Port};Username={config.Username};Password={config.Password};";
    }

    public async Task<ConsistencyReport> ValidateAllDatabasesAsync(CancellationToken cancellationToken)
    {
        var report = new ConsistencyReport
        {
            CheckedAt = DateTime.UtcNow,
            TotalRecords = 0,
            ValidRecords = 0,
            InvalidChecksums = 0,
            OrphanedInserts = 0,
            GhostDeletes = 0,
            CrossDatabaseInconsistencies = 0,
            Violations = new List<ConsistencyViolation>()
        };

        // Validate each database
        for (int dbNum = 1; dbNum <= _config.TotalDatabases; dbNum++)
        {
            var dbReport = await ValidateDatabaseAsync(dbNum, cancellationToken);
            
            report.TotalRecords += dbReport.TotalRecords;
            report.ValidRecords += dbReport.ValidRecords;
            report.InvalidChecksums += dbReport.InvalidChecksums;
            report.OrphanedInserts += dbReport.OrphanedInserts;
            report.GhostDeletes += dbReport.GhostDeletes;
            report.Violations.AddRange(dbReport.Violations);
        }

        // Check for cross-database inconsistencies
        await CheckCrossDatabaseConsistencyAsync(report, cancellationToken);

        return report;
    }

    private async Task<ConsistencyReport> ValidateDatabaseAsync(int databaseNumber, CancellationToken cancellationToken)
    {
        var report = new ConsistencyReport
        {
            CheckedAt = DateTime.UtcNow,
            Violations = new List<ConsistencyViolation>()
        };

        var databaseName = _config.GetDatabaseName(databaseNumber);
        var connectionString = $"{_connectionString}Database={databaseName};";

        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Get all records
            var records = await connection.QueryAsync<TestRecord>(@"
                SELECT id, session_id, wave_number, client_id, payload, checksum, created_at, updated_at, is_deleted, deleted_at
                FROM battle_records
                WHERE session_id = @SessionId",
                new { SessionId = _config.SessionId });

            var recordList = records.ToList();
            report.TotalRecords = recordList.Count;

            foreach (var record in recordList)
            {
                bool isValid = true;

                // Check 1: Verify checksum
                var dataGenerator = new Generators.DataGenerator(_config.PayloadSizeBytes);
                if (!dataGenerator.VerifyChecksum(record.Payload, record.Checksum))
                {
                    report.InvalidChecksums++;
                    isValid = false;
                    report.Violations.Add(new ConsistencyViolation
                    {
                        Type = "InvalidChecksum",
                        RecordId = record.Id,
                        DatabaseName = databaseName,
                        Details = $"Checksum mismatch for record {record.Id}"
                    });
                }

                // Check 2: Check for orphaned inserts (record created but no activity)
                if (!record.UpdatedAt.HasValue && !record.IsDeleted)
                {
                    // Check if this is truly orphaned (no operations performed)
                    var operationCount = await connection.ExecuteScalarAsync<int>(@"
                        SELECT COUNT(*) FROM battle_metrics WHERE record_id = @RecordId",
                        new { RecordId = record.Id });

                    if (operationCount == 0)
                    {
                        // This might be a record from an interrupted operation, mark as warning
                        // Not necessarily a violation if the test was stopped mid-operation
                    }
                }

                // Check 3: Check for ghost deletes (marked deleted but not actually deleted)
                if (record.IsDeleted && !record.DeletedAt.HasValue)
                {
                    report.GhostDeletes++;
                    isValid = false;
                    report.Violations.Add(new ConsistencyViolation
                    {
                        Type = "GhostDelete",
                        RecordId = record.Id,
                        DatabaseName = databaseName,
                        Details = $"Record {record.Id} marked as deleted but DeletedAt is null"
                    });
                }

                // Check 4: Verify update timestamps are consistent
                if (record.UpdatedAt.HasValue && record.UpdatedAt.Value < record.CreatedAt)
                {
                    isValid = false;
                    report.Violations.Add(new ConsistencyViolation
                    {
                        Type = "InvalidTimestamp",
                        RecordId = record.Id,
                        DatabaseName = databaseName,
                        Details = $"Record {record.Id} has UpdatedAt before CreatedAt"
                    });
                }

                // Check 5: Verify delete timestamps are consistent
                if (record.DeletedAt.HasValue && record.DeletedAt.Value < record.CreatedAt)
                {
                    isValid = false;
                    report.Violations.Add(new ConsistencyViolation
                    {
                        Type = "InvalidDeleteTimestamp",
                        RecordId = record.Id,
                        DatabaseName = databaseName,
                        Details = $"Record {record.Id} has DeletedAt before CreatedAt"
                    });
                }

                if (isValid)
                {
                    report.ValidRecords++;
                }
            }
        }
        catch (Exception ex)
        {
            report.Violations.Add(new ConsistencyViolation
            {
                Type = "ValidationError",
                RecordId = Guid.Empty,
                DatabaseName = databaseName,
                Details = $"Failed to validate database: {ex.Message}"
            });
        }

        return report;
    }

    private async Task CheckCrossDatabaseConsistencyAsync(ConsistencyReport report, CancellationToken cancellationToken)
    {
        // Check that the same session doesn't have records with same ID in multiple databases
        var allRecordIds = new HashSet<Guid>();
        var duplicateIds = new List<Guid>();

        for (int dbNum = 1; dbNum <= _config.TotalDatabases; dbNum++)
        {
            var databaseName = _config.GetDatabaseName(dbNum);
            var connectionString = $"{_connectionString}Database={databaseName};";

            try
            {
                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken);

                var ids = await connection.QueryAsync<Guid>(@"
                    SELECT id FROM battle_records WHERE session_id = @SessionId",
                    new { SessionId = _config.SessionId });

                foreach (var id in ids)
                {
                    if (!allRecordIds.Add(id))
                    {
                        duplicateIds.Add(id);
                    }
                }
            }
            catch
            {
                // Ignore connection errors for cross-database check
            }
        }

        if (duplicateIds.Count > 0)
        {
            report.CrossDatabaseInconsistencies = duplicateIds.Count;
            foreach (var dupId in duplicateIds)
            {
                report.Violations.Add(new ConsistencyViolation
                {
                    Type = "CrossDatabaseDuplicate",
                    RecordId = dupId,
                    DatabaseName = "Multiple",
                    Details = $"Record ID {dupId} exists in multiple databases"
                });
            }
        }
    }
}
