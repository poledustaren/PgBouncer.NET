using System.Buffers.Binary;
using System.IO.Pipelines;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server;

/// <summary>
/// Enhanced Transaction Pooling Session with proper Extended Query Protocol support
/// </summary>
public class EnhancedTransactionPoolingSession : IDisposable
{
    private readonly Stream _clientStream;
    private readonly IConnectionPool _pool;
    private readonly PgBouncerConfig _config;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;
    
    // Extended Query Protocol state tracking
    private enum QueryProtocolState
    {
        Idle,           // Waiting for query
        ParseReceived,  // Parse received, waiting for Bind
        BindReceived,   // Bind received, waiting for Execute
        ExecuteReceived,// Execute received, waiting for Sync
        InTransaction   // In transaction block
    }
    
    private QueryProtocolState _queryState = QueryProtocolState.Idle;
    private readonly List<byte[]> _pendingMessages = new();
    private IServerConnection? _backend;
    private bool _disposed;
    
    // Callbacks for metrics
    private readonly Action<long> _recordWaitTime;
    private readonly Action _recordTimeout;
    private readonly Action _onBackendAcquired;
    private readonly Action _onBackendReleased;

    private readonly byte[] _clientBuffer = new byte[65536];
    private readonly byte[] _backendBuffer = new byte[65536];

    public EnhancedTransactionPoolingSession(
        Stream clientStream,
        IConnectionPool pool,
        PgBouncerConfig config,
        ILogger logger,
        SessionInfo sessionInfo,
        Action<long> recordWaitTime,
        Action recordTimeout,
        Action onBackendAcquired,
        Action onBackendReleased)
    {
        _clientStream = clientStream;
        _pool = pool;
        _config = config;
        _logger = logger;
        _sessionInfo = sessionInfo;
        _recordWaitTime = recordWaitTime;
        _recordTimeout = recordTimeout;
        _onBackendAcquired = onBackendAcquired;
        _onBackendReleased = onBackendReleased;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] Starting Enhanced Transaction Pooling Session", _sessionInfo.Id);

        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                // Read message from client
                var message = await ReadClientMessageAsync(cancellationToken);
                if (message == null) break;

                // Process message based on type and current state
                await ProcessClientMessageAsync(message, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("[Session {Id}] Session cancelled", _sessionInfo.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Session error", _sessionInfo.Id);
        }
        finally
        {
            await CleanupAsync();
        }
    }

    private async Task<byte[]?> ReadClientMessageAsync(CancellationToken cancellationToken)
    {
        // Read header (5 bytes)
        if (!await ReadExactAsync(_clientStream, _clientBuffer.AsMemory(0, 5), cancellationToken))
        {
            return null;
        }

        if (!PgMessageScanner.TryReadMessageInfo(_clientBuffer.AsSpan(0, 5), out var msgInfo))
        {
            _logger.LogError("[Session {Id}] Invalid message header", _sessionInfo.Id);
            return null;
        }

        var bodyLength = msgInfo.Length - 4;
        var totalLength = 5 + bodyLength;

        if (bodyLength > 0)
        {
            if (totalLength > _clientBuffer.Length)
            {
                _logger.LogError("[Session {Id}] Message too large: {Length}", _sessionInfo.Id, msgInfo.Length);
                return null;
            }

            if (!await ReadExactAsync(_clientStream, _clientBuffer.AsMemory(5, bodyLength), cancellationToken))
            {
                return null;
            }
        }

        // Copy to new buffer
        var message = new byte[totalLength];
        Buffer.BlockCopy(_clientBuffer, 0, message, 0, totalLength);
        
        return message;
    }

    private async Task ProcessClientMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        var msgType = (char)message[0];
        
        _logger.LogDebug("[Session {Id}] Processing message type '{Type}' in state {State}", 
            _sessionInfo.Id, msgType, _queryState);

        switch (msgType)
        {
            case 'P': // Parse
                await HandleParseMessageAsync(message, cancellationToken);
                break;
                
            case 'B': // Bind
                await HandleBindMessageAsync(message, cancellationToken);
                break;
                
            case 'E': // Execute
                await HandleExecuteMessageAsync(message, cancellationToken);
                break;
                
            case 'S': // Sync
                await HandleSyncMessageAsync(message, cancellationToken);
                break;
                
            case 'Q': // Simple Query
                await HandleSimpleQueryAsync(message, cancellationToken);
                break;
                
            case 'X': // Terminate
                _disposed = true;
                break;
                
            default:
                // Forward other messages immediately
                await ForwardToBackendAsync(message, cancellationToken);
                break;
        }
    }

    private async Task HandleParseMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_queryState != QueryProtocolState.Idle)
        {
            _logger.LogWarning("[Session {Id}] Parse received in non-Idle state: {State}", 
                _sessionInfo.Id, _queryState);
        }

        await EnsureBackendAsync(cancellationToken);
        
        _pendingMessages.Clear();
        _pendingMessages.Add(message);
        _queryState = QueryProtocolState.ParseReceived;
        
        _logger.LogDebug("[Session {Id}] Parse received, buffered. State: ParseReceived", _sessionInfo.Id);
    }

    private async Task HandleBindMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_queryState != QueryProtocolState.ParseReceived)
        {
            _logger.LogWarning("[Session {Id}] Bind received in wrong state: {State}", 
                _sessionInfo.Id, _queryState);
        }

        _pendingMessages.Add(message);
        _queryState = QueryProtocolState.BindReceived;
        
        _logger.LogDebug("[Session {Id}] Bind received, buffered. State: BindReceived", _sessionInfo.Id);
    }

    private async Task HandleExecuteMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_queryState != QueryProtocolState.BindReceived)
        {
            _logger.LogWarning("[Session {Id}] Execute received in wrong state: {State}", 
                _sessionInfo.Id, _queryState);
        }

        _pendingMessages.Add(message);
        _queryState = QueryProtocolState.ExecuteReceived;
        
        _logger.LogDebug("[Session {Id}] Execute received, buffered. State: ExecuteReceived", _sessionInfo.Id);
    }

    private async Task HandleSyncMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        _pendingMessages.Add(message);
        
        _logger.LogDebug("[Session {Id}] Sync received. Flushing {Count} buffered messages", 
            _sessionInfo.Id, _pendingMessages.Count);

        // Flush all buffered messages to backend in correct order
        await FlushBufferedMessagesAsync(cancellationToken);
        
        // Process backend responses until ReadyForQuery
        await ProcessBackendResponsesAsync(cancellationToken);
        
        _queryState = QueryProtocolState.Idle;
        _pendingMessages.Clear();
    }

    private async Task FlushBufferedMessagesAsync(CancellationToken cancellationToken)
    {
        if (_backend == null)
        {
            _logger.LogError("[Session {Id}] No backend available for flush", _sessionInfo.Id);
            return;
        }

        foreach (var msg in _pendingMessages)
        {
            await _backend.Stream.WriteAsync(msg, cancellationToken);
            _logger.LogDebug("[Session {Id}] Flushed message type '{Type}'", 
                _sessionInfo.Id, (char)msg[0]);
        }

        await _backend.Stream.FlushAsync(cancellationToken);
    }

    private async Task HandleSimpleQueryAsync(byte[] message, CancellationToken cancellationToken)
    {
        await EnsureBackendAsync(cancellationToken);
        
        // For simple queries, forward immediately and wait for completion
        await _backend!.Stream.WriteAsync(message, cancellationToken);
        await _backend.Stream.FlushAsync(cancellationToken);
        
        await ProcessBackendResponsesAsync(cancellationToken);
    }

    private async Task ForwardToBackendAsync(byte[] message, CancellationToken cancellationToken)
    {
        await EnsureBackendAsync(cancellationToken);
        await _backend!.Stream.WriteAsync(message, cancellationToken);
        await _backend.Stream.FlushAsync(cancellationToken);
    }

    private async Task ProcessBackendResponsesAsync(CancellationToken cancellationToken)
    {
        if (_backend == null) return;

        var buffer = new byte[8192];
        bool released = false;

        try
        {
            while (!released && !cancellationToken.IsCancellationRequested)
            {
                // Read message header from backend
                if (!await ReadExactAsync(_backend.Stream, buffer.AsMemory(0, 5), cancellationToken))
                {
                    _logger.LogWarning("[Session {Id}] Backend disconnected", _sessionInfo.Id);
                    break;
                }

                if (!PgMessageScanner.TryReadMessageInfo(buffer.AsSpan(0, 5), out var msgInfo))
                {
                    _logger.LogError("[Session {Id}] Invalid backend message", _sessionInfo.Id);
                    break;
                }

                // Read rest of message
                if (msgInfo.Length - 4 > 0)
                {
                    if (!await ReadExactAsync(_backend.Stream, buffer.AsMemory(5, msgInfo.Length - 4), cancellationToken))
                    {
                        break;
                    }
                }

                var fullLength = 1 + msgInfo.Length;

                // Check for ReadyForQuery
                if (msgInfo.IsReadyForQuery)
                {
                    var status = buffer[5];
                    released = await HandleReadyForQueryAsync(status, cancellationToken);
                }

                // Forward to client
                await _clientStream.WriteAsync(buffer.AsMemory(0, fullLength), cancellationToken);
                await _clientStream.FlushAsync(cancellationToken);

                if (released) break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Error processing backend responses", _sessionInfo.Id);
            throw;
        }
    }

    private async Task<bool> HandleReadyForQueryAsync(byte status, CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] ReadyForQuery received with status '{Status}'", 
            _sessionInfo.Id, (char)status);

        switch (status)
        {
            case (byte)'I': // Idle
                _logger.LogDebug("[Session {Id}] Transaction idle - releasing backend", _sessionInfo.Id);
                await ReleaseBackendAsync();
                _onBackendReleased();
                return true;

            case (byte)'T': // In transaction block
                _logger.LogDebug("[Session {Id}] In transaction block - keeping backend", _sessionInfo.Id);
                _queryState = QueryProtocolState.InTransaction;
                return false;

            case (byte)'E': // Failed transaction block
                _logger.LogDebug("[Session {Id}] Failed transaction - keeping backend for rollback", _sessionInfo.Id);
                _queryState = QueryProtocolState.InTransaction;
                return false;

            default:
                _logger.LogWarning("[Session {Id}] Unknown transaction status: {Status}", 
                    _sessionInfo.Id, status);
                return false;
        }
    }

    private async Task EnsureBackendAsync(CancellationToken cancellationToken)
    {
        if (_backend != null) return;

        using var acquireCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        acquireCts.CancelAfter(TimeSpan.FromSeconds(_config.Pool.ConnectionTimeout));

        try
        {
            _logger.LogDebug("[Session {Id}] Acquiring backend from pool", _sessionInfo.Id);
            _backend = await _pool.AcquireAsync(acquireCts.Token);
            _logger.LogDebug("[Session {Id}] Backend acquired: {BackendId}", 
                _sessionInfo.Id, _backend.Id);
        }
        catch (OperationCanceledException)
        {
            _recordTimeout();
            _logger.LogWarning("[Session {Id}] Pool timeout - no available backends", _sessionInfo.Id);
            await SendErrorToClientAsync("connection pool timeout", "57014", cancellationToken);
            throw;
        }
    }

    private async Task ReleaseBackendAsync()
    {
        if (_backend == null) return;

        _logger.LogDebug("[Session {Id}] Releasing backend {BackendId}", 
            _sessionInfo.Id, _backend.Id);

        _pool.Release(_backend);
        _backend = null;
    }

    private async Task CleanupAsync()
    {
        if (_backend != null)
        {
            _pool.Release(_backend);
            _backend = null;
        }

        try
        {
            await _clientStream.DisposeAsync();
        }
        catch { }

        _disposed = true;
        _logger.LogDebug("[Session {Id}] Session cleaned up", _sessionInfo.Id);
    }

    private async Task SendErrorToClientAsync(string message, string sqlState, CancellationToken cancellationToken)
    {
        try
        {
            var msgBytes = System.Text.Encoding.UTF8.GetBytes(message);
            var codeBytes = System.Text.Encoding.UTF8.GetBytes(sqlState);
            var severity = "FATAL"u8.ToArray();

            var len = 4 + 1 + severity.Length + 1 + 1 + codeBytes.Length + 1 + 1 + msgBytes.Length + 1 + 1;
            var response = new byte[1 + len];
            var pos = 0;

            response[pos++] = (byte)'E';
            response[pos++] = (byte)(len >> 24);
            response[pos++] = (byte)(len >> 16);
            response[pos++] = (byte)(len >> 8);
            response[pos++] = (byte)len;

            response[pos++] = (byte)'S';
            Buffer.BlockCopy(severity, 0, response, pos, severity.Length);
            pos += severity.Length;
            response[pos++] = 0;

            response[pos++] = (byte)'C';
            Buffer.BlockCopy(codeBytes, 0, response, pos, codeBytes.Length);
            pos += codeBytes.Length;
            response[pos++] = 0;

            response[pos++] = (byte)'M';
            Buffer.BlockCopy(msgBytes, 0, response, pos, msgBytes.Length);
            pos += msgBytes.Length;
            response[pos++] = 0;

            response[pos] = 0;

            await _clientStream.WriteAsync(response, cancellationToken);
            await _clientStream.FlushAsync(cancellationToken);
        }
        catch { }
    }

    private static async Task<bool> ReadExactAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.Slice(totalRead), cancellationToken);
            if (read == 0) return false;
            totalRead += read;
        }
        return true;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            CleanupAsync().GetAwaiter().GetResult();
        }
    }
}
