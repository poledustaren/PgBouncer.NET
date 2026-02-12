using System;

namespace PgBouncer.BattleTest.Config;

public class TestConfiguration
{
    // Test identification
    public string SessionId { get; set; } = Guid.NewGuid().ToString("N")[..8];
    
    // Database configuration
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 6434;
    public string Username { get; set; } = "postgres";
    public string Password { get; set; } = "postgres";
    public int TotalDatabases { get; set; } = 20;
    public string DatabaseNamePrefix { get; set; } = "battle_test_";
    
    // Client configuration
    public int InitialClients { get; set; } = 5;
    public int MaxClients { get; set; } = 20;
    public int ClientsPerWaveIncrement { get; set; } = 2;
    public int TargetOperationsPerSecondPerClient { get; set; } = 10;
    
    // Wave configuration
    public int WaveDurationSeconds { get; set; } = 30;
    public TimeSpan MaxTotalDuration { get; set; } = TimeSpan.FromMinutes(10);
    
    // Success criteria
    public double MinimumSuccessRate { get; set; } = 0.95;
    public double MaxAcceptableP95LatencyMs { get; set; } = 1000;
    
    // Payload configuration
    public int PayloadSizeBytes { get; set; } = 1024;
    
    // Reporting
    public string ReportOutputPath { get; set; } = "./battle-test-report.md";
    public bool GenerateDetailedLogs { get; set; } = true;

    public string GetConnectionString(int databaseNumber)
    {
        var databaseName = $"{DatabaseNamePrefix}{databaseNumber:D3}";
        return $"Host={Host};Port={Port};Database={databaseName};Username={Username};Password={Password};Pooling=true;Minimum Pool Size=1;Maximum Pool Size=5;";
    }

    public string GetDatabaseName(int databaseNumber) =>
        $"{DatabaseNamePrefix}{databaseNumber:D3}";
}
