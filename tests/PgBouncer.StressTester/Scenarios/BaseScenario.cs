using Npgsql;

namespace PgBouncer.StressTester.Scenarios;

/// <summary>
/// Базовый класс для всех сценариев стресс-тестирования
/// </summary>
public abstract class BaseScenario : IScenario
{
    protected ScenarioResult _result = new();
    protected string _connectionString = "";
    
    public abstract string Name { get; }
    public abstract string Description { get; }
    
    public virtual ScenarioResult GetResult() => _result;
    
    public virtual void Reset() => _result = new ScenarioResult();
    
    public async Task RunAsync(string connectionString, int iteration, Random random, CancellationToken ct)
    {
        _connectionString = connectionString;
        try
        {
            await ExecuteAsync(iteration, random, ct);
            _result.SuccessCount++;
        }
        catch (Exception ex)
        {
            _result.ErrorCount++;
            _result.Errors.Add(new ErrorInfo
            {
                Message = ex.Message,
                Scenario = Name,
                Iteration = iteration,
                Type = ClassifyError(ex)
            });
        }
    }
    
    protected abstract Task ExecuteAsync(int iteration, Random random, CancellationToken ct);
    
    protected virtual ErrorType ClassifyError(Exception ex)
    {
        var msg = ex.Message.ToLowerInvariant();
        
        if (msg.Contains("timeout") || msg.Contains("timed out"))
            return ErrorType.Timeout;
        if (msg.Contains("connection") && (msg.Contains("lost") || msg.Contains("closed") || msg.Contains("refused")))
            return ErrorType.ConnectionLost;
        if (msg.Contains("protocol") || msg.Contains("parse") || msg.Contains("bind"))
            return ErrorType.ProtocolError;
        if (msg.Contains("transaction") || msg.Contains("commit") || msg.Contains("rollback"))
            return ErrorType.TransactionError;
        if (msg.Contains("prepared") || msg.Contains("statement"))
            return ErrorType.PreparedStatementError;
        if (msg.Contains("batch"))
            return ErrorType.BatchError;
        if (msg.Contains("sql") || msg.Contains("syntax") || msg.Contains("error"))
            return ErrorType.QueryError;
            
        return ErrorType.Unknown;
    }
    
    /// <summary>
    /// Создать новое соединение с отключенным пулингом (как в проде)
    /// </summary>
    protected NpgsqlConnection CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        return conn;
    }
}
