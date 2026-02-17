namespace PgBouncer.StressTester.Scenarios;

/// <summary>
/// Интерфейс для сценариев стресс-тестирования
/// </summary>
public interface IScenario
{
    string Name { get; }
    string Description { get; }
    Task RunAsync(string connectionString, int iteration, Random random, CancellationToken ct);
    ScenarioResult GetResult();
    void Reset();
}

/// <summary>
/// Результат выполнения сценария
/// </summary>
public class ScenarioResult
{
    public long SuccessCount { get; set; }
    public long ErrorCount { get; set; }
    public List<ErrorInfo> Errors { get; } = new();
    
    public double SuccessRate => ErrorCount + SuccessCount > 0 
        ? (double)SuccessCount / (SuccessCount + ErrorCount) * 100 
        : 0;
}

/// <summary>
/// Информация об ошибке
/// </summary>
public class ErrorInfo
{
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public string Message { get; set; } = "";
    public string Scenario { get; set; } = "";
    public int Iteration { get; set; }
    public ErrorType Type { get; set; }
}

/// <summary>
/// Типы ошибок
/// </summary>
public enum ErrorType
{
    None,
    Timeout,
    ConnectionLost,
    ProtocolError,
    QueryError,
    TransactionError,
    PreparedStatementError,
    BatchError,
    Unknown
}
