using System;

namespace PgBouncer.BattleTest.Models;

public record TestRecord
{
    public Guid Id { get; init; }
    public string SessionId { get; init; } = string.Empty;
    public int WaveNumber { get; init; }
    public int ClientId { get; init; }
    public string Payload { get; init; } = string.Empty;
    public string Checksum { get; init; } = string.Empty;
    public DateTime CreatedAt { get; init; }
    public DateTime? UpdatedAt { get; init; }
    public bool IsDeleted { get; init; }
    public DateTime? DeletedAt { get; init; }

    public TestRecord() { }

    public TestRecord(Guid id, string sessionId, int waveNumber, int clientId, string payload, string checksum)
    {
        Id = id;
        SessionId = sessionId;
        WaveNumber = waveNumber;
        ClientId = clientId;
        Payload = payload;
        Checksum = checksum;
        CreatedAt = DateTime.UtcNow;
    }
}
