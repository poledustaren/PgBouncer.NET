using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server;

/// <summary>
/// Production-ready Transaction Pooling Session with event-driven architecture
/// Similar to original pgbouncer: event loop + channels + non-blocking I/O
/// </summary>
public class EnhancedTransactionPoolingSessionV2 : IDisposable
{
    private readonly Stream _clientStream;
    private readonly IConnectionPool _pool;
    private readonly PgBouncerConfig _config;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;
    private readonly Action<long> _recordWaitTime;
    private readonly Action _recordTimeout;
    private readonly Action _onBackendAcquired;
    private readonly Action _onBackendReleased;

    // Extended Query Protocol state
    private enum QueryState { Idle, ParseReceived, BindReceived, ExecuteReceived, InTransaction }
    private QueryState _queryState = QueryState.Idle;
    private readonly List<byte[]> _pendingMessages = new();
    
    // Backend connection
    private IServerConnection? _backend;
    private readonly object _backendLock = new();
    private volatile bool _backendAcquired;
    
    // Channels for message passing (lock-free queues)
    private readonly Channel<byte[]> _clientToBackendChannel;
    private readonly Channel<byte[]> _backendToClientChannel;
    private readonly Channel<bool> _readyForQueryChannel;
    
    // Cancellation
    private readonly CancellationTokenSource _sessionCts = new();
    private Task? _clientReaderTask;
    private Task? _clientWriterTask;
    private Task? _backendReaderTask;
    private Task? _backendWriterTask;

    public EnhancedTransactionPoolingSessionV2(
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

        // Unbounded channels for message passing
        _clientToBackendChannel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        _backendToClientChannel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        _readyForQueryChannel = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = true
        });
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[Session {Id}] Starting EnhancedTransactionPoolingSessionV2 (Event-driven)", _sessionInfo.Id);

        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _sessionCts.Token);
            var linkedToken = linkedCts.Token;

            // Start four concurrent tasks:
            // 1. Client reader - reads from client, processes protocol, sends to backend channel
            // 2. Client writer - reads from backend channel, sends to client
            // 3. Backend reader - reads from backend, sends to client channel
            // 4. Backend writer - writes from client channel to actual backend

            _clientReaderTask = ClientReaderLoopAsync(linkedToken);
            _clientWriterTask = ClientWriterLoopAsync(linkedToken);
            _backendReaderTask = BackendReaderLoopAsync(linkedToken);
            _backendWriterTask = BackendWriterLoopAsync(linkedToken);

            // Wait for any task to complete or fail
            var completedTask = await Task.WhenAny(_clientReaderTask, _clientWriterTask, _backendReaderTask, _backendWriterTask);
            
            // If any task failed, propagate exception
            await completedTask;
            
            // Wait for others to complete gracefully
            await Task.WhenAll(
                AwaitWithTimeout(_clientReaderTask, TimeSpan.FromSeconds(5)),
                AwaitWithTimeout(_clientWriterTask, TimeSpan.FromSeconds(5)),
                AwaitWithTimeout(_backendReaderTask, TimeSpan.FromSeconds(5)),
                AwaitWithTimeout(_backendWriterTask, TimeSpan.FromSeconds(5))
            );
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

    /// <summary>
    /// Client Reader Loop: Reads messages from client, handles Extended Query Protocol, sends to backend channel
    /// </summary>
    private async Task ClientReaderLoopAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[65536];
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Read message from client
                var message = await ReadMessageAsync(_clientStream, buffer, cancellationToken);
                if (message == null) break;

                var msgType = (char)message[0];
                _logger.LogDebug("[Session {Id}] Client->Backend: Type='{Type}', State={State}, Size={Size}",
                    _sessionInfo.Id, msgType, _queryState, message.Length);

                // Handle Extended Query Protocol state machine
                switch (msgType)
                {
                    case 'P': // Parse
                        await HandleParseMessageAsync(message, cancellationToken);
                        break;

                    case 'B': // Bind
                        HandleBindMessage(message);
                        break;

                    case 'E': // Execute
                        HandleExecuteMessage(message);
                        break;

                    case 'S': // Sync
                        await HandleSyncMessageAsync(message, cancellationToken);
                        break;

                    case 'Q': // Simple Query
                        await HandleSimpleQueryAsync(message, cancellationToken);
                        break;

                    case 'X': // Terminate
                        _logger.LogDebug("[Session {Id}] Client sent Terminate", _sessionInfo.Id);
                        return;

                    default:
                        // Other messages - forward directly
                        await EnsureBackendAsync(cancellationToken);
                        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
                        break;
                }
            }
        }
        catch (ChannelClosedException)
        {
            _logger.LogDebug("[Session {Id}] Client reader channel closed", _sessionInfo.Id);
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Client reader error", _sessionInfo.Id);
            throw;
        }
        finally
        {
            _clientToBackendChannel.Writer.Complete();
        }
    }

    /// <summary>
    /// Backend Reader Loop: Reads responses from backend, forwards to client channel
    /// </summary>
    private async Task BackendReaderLoopAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[65536];
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Wait for backend to be available
                IServerConnection? backend;
                lock (_backendLock)
                {
                    backend = _backend;
                }

                if (backend == null)
                {
                    await Task.Delay(10, cancellationToken);
                    continue;
                }

                // Read message from backend
                var message = await ReadMessageAsync(backend.Stream, buffer, cancellationToken);
                if (message == null)
                {
                    _logger.LogWarning("[Session {Id}] Backend disconnected", _sessionInfo.Id);
                    _pool.RecordFailure(backend.Id);
                    await ReleaseBackendAsync();
                    continue;
                }

                var msgType = (char)message[0];
                _logger.LogDebug("[Session {Id}] Backend->Client: Type='{Type}', Size={Size}",
                    _sessionInfo.Id, msgType, message.Length);

                // Check for ReadyForQuery
                if (msgType == 'Z')
                {
                    var status = message.Length > 5 ? message[5] : (byte)'I';
                    await HandleReadyForQueryAsync(status, cancellationToken);
                }

                // Forward to client
                await _backendToClientChannel.Writer.WriteAsync(message, cancellationToken);
            }
        }
        catch (ChannelClosedException)
        {
            _logger.LogDebug("[Session {Id}] Backend reader channel closed", _sessionInfo.Id);
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Backend reader error", _sessionInfo.Id);
            throw;
        }
        finally
        {
            _backendToClientChannel.Writer.Complete();
        }
    }

    /// <summary>
    /// Backend Writer Loop: Writes messages from client channel to actual backend
    /// </summary>
    private async Task BackendWriterLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _clientToBackendChannel.Reader.ReadAllAsync(cancellationToken))
            {
                IServerConnection? backend;
                lock (_backendLock)
                {
                    backend = _backend;
                }

                if (backend == null)
                {
                    _logger.LogError("[Session {Id}] No backend available for writing", _sessionInfo.Id);
                    continue;
                }

                try
                {
                    await backend.Stream.WriteAsync(message, cancellationToken);
                    await backend.Stream.FlushAsync(cancellationToken);
                    
                    var msgType = (char)message[0];
                    _logger.LogTrace("[Session {Id}] Written to backend: Type='{Type}'", 
                        _sessionInfo.Id, msgType);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[Session {Id}] Failed to write to backend", _sessionInfo.Id);
                    _pool.RecordFailure(backend.Id);
                    throw;
                }
            }
        }
        catch (ChannelClosedException)
        {
            _logger.LogDebug("[Session {Id}] Backend writer channel closed", _sessionInfo.Id);
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Backend writer error", _sessionInfo.Id);
            throw;
        }
    }

    /// <summary>
    /// Client Writer Loop: Reads messages from backend channel and writes to client
    /// </summary>
    private async Task ClientWriterLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[Session {Id}] ClientWriterLoop starting", _sessionInfo.Id);
        
        try
        {
            await foreach (var message in _backendToClientChannel.Reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await _clientStream.WriteAsync(message, cancellationToken);
                    await _clientStream.FlushAsync(cancellationToken);
                    
                    var msgType = (char)message[0];
                    _logger.LogDebug("[Session {Id}] Written to client: Type='{Type}'", 
                        _sessionInfo.Id, msgType);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[Session {Id}] Failed to write to client", _sessionInfo.Id);
                    throw;
                }
            }
        }
        catch (ChannelClosedException)
        {
            _logger.LogDebug("[Session {Id}] Client writer channel closed", _sessionInfo.Id);
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Client writer error", _sessionInfo.Id);
            throw;
        }
    }

    // Extended Query Protocol Handlers

    private async Task HandleParseMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_queryState != QueryState.Idle)
        {
            _logger.LogWarning("[Session {Id}] Parse in wrong state: {State}", _sessionInfo.Id, _queryState);
        }

        await EnsureBackendAsync(cancellationToken);
        _pendingMessages.Clear();
        _pendingMessages.Add(message);
        _queryState = QueryState.ParseReceived;
    }

    private void HandleBindMessage(byte[] message)
    {
        if (_queryState != QueryState.ParseReceived)
        {
            _logger.LogWarning("[Session {Id}] Bind in wrong state: {State}", _sessionInfo.Id, _queryState);
        }
        _pendingMessages.Add(message);
        _queryState = QueryState.BindReceived;
    }

    private void HandleExecuteMessage(byte[] message)
    {
        if (_queryState != QueryState.BindReceived)
        {
            _logger.LogWarning("[Session {Id}] Execute in wrong state: {State}", _sessionInfo.Id, _queryState);
        }
        _pendingMessages.Add(message);
        _queryState = QueryState.ExecuteReceived;
    }

    private async Task HandleSyncMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        _pendingMessages.Add(message);
        
        _logger.LogDebug("[Session {Id}] Flushing {Count} messages to backend", 
            _sessionInfo.Id, _pendingMessages.Count);

        // Send all buffered messages
        foreach (var msg in _pendingMessages)
        {
            await _clientToBackendChannel.Writer.WriteAsync(msg, cancellationToken);
        }
        _pendingMessages.Clear();

        // Wait for ReadyForQuery
        await _readyForQueryChannel.Reader.ReadAsync(cancellationToken);
        
        _queryState = QueryState.Idle;
    }

    private async Task HandleSimpleQueryAsync(byte[] message, CancellationToken cancellationToken)
    {
        await EnsureBackendAsync(cancellationToken);
        
        // For simple queries, send immediately and wait
        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
        await _readyForQueryChannel.Reader.ReadAsync(cancellationToken);
    }

    private async Task HandleReadyForQueryAsync(byte status, CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] ReadyForQuery: Status='{Status}'", 
            _sessionInfo.Id, (char)status);

        // Signal that ReadyForQuery received
        await _readyForQueryChannel.Writer.WriteAsync(true, cancellationToken);

        switch (status)
        {
            case (byte)'I': // Idle - can release backend
                _onBackendReleased();
                await ReleaseBackendAsync();
                _queryState = QueryState.Idle;
                break;

            case (byte)'T': // In transaction
            case (byte)'E': // Failed transaction
                _queryState = QueryState.InTransaction;
                break;
        }
    }

    private async Task EnsureBackendAsync(CancellationToken cancellationToken)
    {
        if (_backendAcquired) return;

        var sw = Stopwatch.StartNew();
        
        try
        {
            var timeout = TimeSpan.FromSeconds(_config.Pool.ConnectionTimeout > 0 
                ? _config.Pool.ConnectionTimeout 
                : 60);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeout);

            var backend = await _pool.AcquireAsync(cts.Token);
            
            lock (_backendLock)
            {
                _backend = backend;
                _backendAcquired = true;
            }
            
            sw.Stop();
            _recordWaitTime(sw.ElapsedMilliseconds);
            _onBackendAcquired();
            
            _logger.LogDebug("[Session {Id}] Backend acquired: {BackendId} in {Ms}ms", 
                _sessionInfo.Id, backend.Id, sw.ElapsedMilliseconds);
        }
        catch (OperationCanceledException)
        {
            _recordTimeout();
            _logger.LogWarning("[Session {Id}] Pool timeout", _sessionInfo.Id);
            throw;
        }
    }

    private async Task ReleaseBackendAsync()
    {
        IServerConnection? backend;
        lock (_backendLock)
        {
            backend = _backend;
            _backend = null;
            _backendAcquired = false;
        }

        if (backend != null)
        {
            _pool.Release(backend);
            _logger.LogDebug("[Session {Id}] Backend released: {BackendId}", 
                _sessionInfo.Id, backend.Id);
        }
    }

    private async Task<byte[]?> ReadMessageAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        // Read header (5 bytes)
        if (!await ReadExactAsync(stream, buffer.AsMemory(0, 5), cancellationToken))
            return null;

        if (!PgMessageScanner.TryReadMessageInfo(buffer.AsSpan(0, 5), out var msgInfo))
            return null;

        var bodyLength = msgInfo.Length - 4;
        var totalLength = 5 + bodyLength;

        if (bodyLength > 0)
        {
            if (totalLength > buffer.Length)
                throw new InvalidOperationException($"Message too large: {totalLength}");

            if (!await ReadExactAsync(stream, buffer.AsMemory(5, bodyLength), cancellationToken))
                return null;
        }

        var message = new byte[totalLength];
        Buffer.BlockCopy(buffer, 0, message, 0, totalLength);
        return message;
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

    private static bool IsExpectedException(Exception ex)
    {
        return ex is OperationCanceledException 
            || (ex is IOException ioEx && ioEx.InnerException is SocketException);
    }

    private static async Task AwaitWithTimeout(Task task, TimeSpan timeout)
    {
        if (await Task.WhenAny(task, Task.Delay(timeout)) == task)
        {
            await task;
        }
    }

    private async Task CleanupAsync()
    {
        _sessionCts.Cancel();
        
        _clientToBackendChannel.Writer.TryComplete();
        _backendToClientChannel.Writer.TryComplete();
        _readyForQueryChannel.Writer.TryComplete();

        await ReleaseBackendAsync();

        try { await _clientStream.DisposeAsync(); } catch { }
        
        _logger.LogInformation("[Session {Id}] Session cleaned up", _sessionInfo.Id);
    }

    public void Dispose()
    {
        _sessionCts.Cancel();
        CleanupAsync().GetAwaiter().GetResult();
        _sessionCts.Dispose();
    }
}
