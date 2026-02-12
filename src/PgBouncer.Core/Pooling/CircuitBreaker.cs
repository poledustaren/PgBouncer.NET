using System.Collections.Concurrent;

namespace PgBouncer.Core.Pooling;

/// <summary>
/// Circuit breaker pattern for backend connections
/// Prevents cascading failures by temporarily blocking requests to failed backends
/// </summary>
public class CircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _resetTimeout;
    private readonly ConcurrentDictionary<Guid, CircuitState> _circuits = new();
    
    public CircuitBreaker(int failureThreshold = 5, int resetTimeoutSeconds = 30)
    {
        _failureThreshold = failureThreshold;
        _resetTimeout = TimeSpan.FromSeconds(resetTimeoutSeconds);
    }

    public bool IsAllowed(Guid connectionId)
    {
        if (!_circuits.TryGetValue(connectionId, out var state))
        {
            return true;
        }

        switch (state.Status)
        {
            case CircuitStatus.Closed:
                return true;
                
            case CircuitStatus.Open:
                if (DateTime.UtcNow - state.LastFailure > _resetTimeout)
                {
                    // Try half-open
                    state.Status = CircuitStatus.HalfOpen;
                    _circuits[connectionId] = state;
                    return true;
                }
                return false;
                
            case CircuitStatus.HalfOpen:
                return true;
                
            default:
                return true;
        }
    }

    public void RecordSuccess(Guid connectionId)
    {
        if (_circuits.TryGetValue(connectionId, out var state))
        {
            if (state.Status == CircuitStatus.HalfOpen)
            {
                // Recovery successful
                _circuits.TryRemove(connectionId, out _);
            }
            else
            {
                state.FailureCount = 0;
                _circuits[connectionId] = state;
            }
        }
    }

    public void RecordFailure(Guid connectionId)
    {
        var state = _circuits.GetOrAdd(connectionId, _ => new CircuitState 
        { 
            ConnectionId = connectionId,
            Status = CircuitStatus.Closed 
        });

        state.FailureCount++;
        state.LastFailure = DateTime.UtcNow;

        if (state.FailureCount >= _failureThreshold && state.Status == CircuitStatus.Closed)
        {
            state.Status = CircuitStatus.Open;
        }

        _circuits[connectionId] = state;
    }

    public void Remove(Guid connectionId)
    {
        _circuits.TryRemove(connectionId, out _);
    }

    public CircuitStats GetStats()
    {
        var states = _circuits.Values.ToList();
        return new CircuitStats
        {
            TotalCircuits = states.Count,
            OpenCircuits = states.Count(s => s.Status == CircuitStatus.Open),
            HalfOpenCircuits = states.Count(s => s.Status == CircuitStatus.HalfOpen),
            ClosedCircuits = states.Count(s => s.Status == CircuitStatus.Closed)
        };
    }
}

public class CircuitState
{
    public Guid ConnectionId { get; set; }
    public CircuitStatus Status { get; set; }
    public int FailureCount { get; set; }
    public DateTime LastFailure { get; set; }
}

public enum CircuitStatus
{
    Closed,    // Normal operation
    Open,      // Circuit is open, requests blocked
    HalfOpen   // Testing if service recovered
}

public class CircuitStats
{
    public int TotalCircuits { get; set; }
    public int OpenCircuits { get; set; }
    public int HalfOpenCircuits { get; set; }
    public int ClosedCircuits { get; set; }
}
