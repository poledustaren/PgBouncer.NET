using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using PgBouncer.BattleTest.Models;

namespace PgBouncer.BattleTest.Generators;

public class DataGenerator
{
    private readonly int _payloadSizeBytes;
    private readonly Random _random;

    public DataGenerator(int payloadSizeBytes = 1024)
    {
        _payloadSizeBytes = payloadSizeBytes;
        _random = new Random();
    }

    public TestRecord GenerateRecord(Guid sessionId, int waveNumber, int clientId, int databaseId, int operationNumber, string operationType)
    {
        var id = Guid.NewGuid();
        var payload = GeneratePayload();
        var checksum = CalculateChecksum(payload);

        return new TestRecord
        {
            Id = id,
            SessionId = sessionId,
            WaveNumber = waveNumber,
            OperationNumber = operationNumber,
            ClientId = clientId,
            DatabaseId = databaseId,
            Payload = payload,
            Checksum = checksum,
            OperationType = operationType
        };
    }

    public TestRecord UpdateRecord(TestRecord existingRecord, string newPayload)
    {
        return existingRecord with
        {
            Payload = newPayload,
            Checksum = CalculateChecksum(newPayload),
            UpdatedAt = DateTime.UtcNow
        };
    }

    public TestRecord MarkAsDeleted(TestRecord existingRecord)
    {
        return existingRecord with
        {
            IsDeleted = true,
            DeletedAt = DateTime.UtcNow
        };
    }

    public string GeneratePayload()
    {
        // Generate realistic synthetic data
        var sb = new StringBuilder(_payloadSizeBytes);
        
        // Add some structure to make it realistic
        sb.Append("{");
        sb.Append($"\"timestamp\":\"{DateTime.UtcNow:O}\",");
        sb.Append($"\"random_id\":\"{Guid.NewGuid()}\",");
        sb.Append($"\"sequence\":{_random.Next(1, int.MaxValue)},");
        sb.Append("\"data\":\"");
        
        // Fill remaining space with base64-like random characters
        var remainingBytes = _payloadSizeBytes - sb.Length - 2; // -2 for closing braces
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        
        for (int i = 0; i < remainingBytes; i++)
        {
            sb.Append(chars[_random.Next(chars.Length)]);
        }
        
        sb.Append("}");
        
        return sb.ToString();
    }

    public string CalculateChecksum(string payload)
    {
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(payload));
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }

    public bool VerifyChecksum(string payload, string expectedChecksum)
    {
        var actualChecksum = CalculateChecksum(payload);
        return actualChecksum.Equals(expectedChecksum, StringComparison.OrdinalIgnoreCase);
    }

    public int CalculateDatabaseNumber(int clientId, int totalDatabases)
    {
        return (clientId % totalDatabases) + 1;
    }
}
