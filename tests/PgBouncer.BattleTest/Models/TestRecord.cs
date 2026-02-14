using System;

namespace PgBouncer.BattleTest.Models;

public record TestRecord
{
    public Guid Id { get; init; }
    public Guid SessionId { get; init; }
    public int WaveNumber { get; init; }
    public int OperationNumber { get; init; }
    public int ClientId { get; init; }
    public int DatabaseId { get; init; }
    public string Payload { get; init; } = string.Empty;
    public string Checksum { get; init; } = string.Empty;
    public string OperationType { get; init; } = string.Empty;
    public DateTime CreatedAt { get; init; }
    public DateTime? UpdatedAt { get; init; }
    public bool IsDeleted { get; init; }
    public DateTime? DeletedAt { get; init; }

    public TestRecord() { }

    public TestRecord(Guid id, Guid sessionId, int waveNumber, int operationNumber, int clientId, int databaseId, string payload, string checksum, string operationType)
    {
        Id = id;
        SessionId = sessionId;
        WaveNumber = waveNumber;
        OperationNumber = operationNumber;
        ClientId = clientId;
        DatabaseId = databaseId;
        Payload = payload;
        Checksum = checksum;
        OperationType = operationType;
        CreatedAt = DateTime.UtcNow;
    }
}
