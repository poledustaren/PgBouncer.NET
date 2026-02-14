using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Server;

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
    private readonly byte[]? _initialData;

    private enum SessionState
    {
        Idle,
        InExtendedQuery,
        WaitingForNextMessage
    };
    
    private SessionState _state = SessionState.Idle;
    private readonly List<byte[]> _pendingMessages = new();
    
    private IServerConnection? _backend;
    private readonly object _backendLock = new();
    private volatile bool _backendAcquired;
    private volatile bool _pendingBackendRelease;
    private int _backendGeneration;
    
    private readonly Channel<byte[]> _clientToBackendChannel;
    private readonly Channel<byte[]> _backendToClientChannel;
    private readonly Channel<bool> _readyForQueryChannel;
    
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
        Action onBackendReleased,
        byte[]? initialData = null)
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
        _initialData = initialData;

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
        _logger.LogDebug("[Session {Id}] Starting Transaction Pooling Session V2", _sessionInfo.Id);

        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _sessionCts.Token);
            var linkedToken = linkedCts.Token;

            Stream effectiveStream = _clientStream;
            if (_initialData != null && _initialData.Length > 0)
            {
                effectiveStream = new PrefixedStream(_clientStream, _initialData);
                _logger.LogDebug("[Session {Id}] Using PrefixedStream with {Bytes} initial bytes", _sessionInfo.Id, _initialData.Length);
            }

            _clientReaderTask = ClientReaderLoopAsync(effectiveStream, linkedToken);
            _clientWriterTask = ClientWriterLoopAsync(linkedToken);
            _backendReaderTask = BackendReaderLoopAsync(linkedToken);
            _backendWriterTask = BackendWriterLoopAsync(linkedToken);

            var completedTask = await Task.WhenAny(_clientReaderTask, _clientWriterTask, _backendReaderTask, _backendWriterTask);
            
            try { await completedTask; } catch { }
            
            await Task.WhenAll(
                AwaitWithTimeout(_clientReaderTask, TimeSpan.FromSeconds(3)),
                AwaitWithTimeout(_clientWriterTask, TimeSpan.FromSeconds(3)),
                AwaitWithTimeout(_backendReaderTask, TimeSpan.FromSeconds(3)),
                AwaitWithTimeout(_backendWriterTask, TimeSpan.FromSeconds(3))
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

    private async Task ClientReaderLoopAsync(Stream clientStream, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(65536);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await ReadMessageAsync(clientStream, buffer, cancellationToken);
                if (message == null)
                {
                    _logger.LogDebug("[Session {Id}] Client disconnected", _sessionInfo.Id);
                    break;
                }

                var msgType = (char)message[0];
                _logger.LogDebug("[Session {Id}] Client->: Type='{Type}', State={State}",
                    _sessionInfo.Id, msgType, _state);

                switch (msgType)
                {
                    case PgMessageTypes.Parse:
                        await HandleParseAsync(message, cancellationToken);
                        break;

                    case PgMessageTypes.Bind:
                        HandleBind(message);
                        break;

                    case PgMessageTypes.Execute:
                        HandleExecute(message);
                        break;

                    case PgMessageTypes.Describe:
                        HandleDescribe(message);
                        break;

                    case PgMessageTypes.Close:
                        await HandleCloseAsync(message, cancellationToken);
                        break;

                    case PgMessageTypes.Sync:
                        await HandleSyncAsync(message, cancellationToken);
                        break;

                    case PgMessageTypes.Flush:
                        await HandleFlushAsync(message, cancellationToken);
                        break;

                    case PgMessageTypes.Query:
                        await HandleSimpleQueryAsync(message, cancellationToken);
                        break;

                    case PgMessageTypes.Terminate:
                        _logger.LogDebug("[Session {Id}] Client sent Terminate", _sessionInfo.Id);
                        return;

                    default:
                        await HandleOtherMessageAsync(message, cancellationToken);
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
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            _clientToBackendChannel.Writer.TryComplete();
        }
    }

    private async Task BackendReaderLoopAsync(CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(65536);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                IServerConnection? backend;
                int readGeneration;
                lock (_backendLock)
                {
                    backend = _backend;
                    readGeneration = _backendGeneration;
                }

                if (backend == null)
                {
                    await Task.Delay(5, cancellationToken);
                    continue;
                }

                var message = await ReadMessageAsync(backend.Stream, buffer, cancellationToken);
                if (message == null)
                {
                    _logger.LogWarning("[Session {Id}] Backend disconnected", _sessionInfo.Id);
                    _pool.RecordFailure(backend.Id);
                    await DestroyBackendAsync();
                    continue;
                }

                lock (_backendLock)
                {
                    if (_backendGeneration != readGeneration || _backend != backend)
                    {
                        _logger.LogDebug("[Session {Id}] Backend changed (gen {Old}->{New}), discarding message",
                            _sessionInfo.Id, readGeneration, _backendGeneration);
                        continue;
                    }
                }

                var msgType = (char)message[0];
                _logger.LogDebug("[Session {Id}] Backend->: Type='{Type}'",
                    _sessionInfo.Id, msgType);

                if (msgType == PgMessageTypes.ReadyForQuery)
                {
                    var status = message.Length > 5 ? message[5] : (byte)'I';
                    await HandleReadyForQueryAsync(status, cancellationToken);
                }

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
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

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
                    _logger.LogWarning("[Session {Id}] No backend for writing, message dropped", _sessionInfo.Id);
                    continue;
                }

                try
                {
                    await backend.Stream.WriteAsync(message, cancellationToken);
                    await backend.Stream.FlushAsync(cancellationToken);
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
        }
    }

    private async Task ClientWriterLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _backendToClientChannel.Reader.ReadAllAsync(cancellationToken))
            {
                await _clientStream.WriteAsync(message, cancellationToken);
                await _clientStream.FlushAsync(cancellationToken);
            }
        }
        catch (ChannelClosedException)
        {
            _logger.LogDebug("[Session {Id}] Client writer channel closed", _sessionInfo.Id);
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Client writer error", _sessionInfo.Id);
        }
    }

    private async Task HandleParseAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_pendingBackendRelease)
        {
            _logger.LogTrace("[Session {Id}] Reusing backend for next Extended Query batch", _sessionInfo.Id);
            _pendingBackendRelease = false;
        }
        
        await EnsureBackendAsync(cancellationToken);
        _pendingMessages.Clear();
        _pendingMessages.Add(message);
        _state = SessionState.InExtendedQuery;
    }

    private void HandleBind(byte[] message)
    {
        _pendingMessages.Add(message);
    }

    private void HandleExecute(byte[] message)
    {
        _pendingMessages.Add(message);
    }

    private void HandleDescribe(byte[] message)
    {
        if (_state == SessionState.Idle)
        {
            _pendingMessages.Clear();
            _state = SessionState.InExtendedQuery;
        }
        _pendingMessages.Add(message);
    }

    private async Task HandleCloseAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_pendingBackendRelease && _backendAcquired)
        {
            _logger.LogTrace("[Session {Id}] Sending Close to backend before release", _sessionInfo.Id);
            await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
            return;
        }
        
        _pendingMessages.Add(message);
    }

    private async Task HandleSyncAsync(byte[] message, CancellationToken cancellationToken)
    {
        _pendingMessages.Add(message);
        
        _logger.LogTrace("[Session {Id}] Sync: flushing {Count} messages", _sessionInfo.Id, _pendingMessages.Count);

        foreach (var msg in _pendingMessages)
        {
            await _clientToBackendChannel.Writer.WriteAsync(msg, cancellationToken);
        }
        _pendingMessages.Clear();

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            await _readyForQueryChannel.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("[Session {Id}] Timeout waiting for ReadyForQuery", _sessionInfo.Id);
        }
        
        _state = SessionState.WaitingForNextMessage;
    }

    private async Task HandleFlushAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_pendingMessages.Count > 0)
        {
            foreach (var msg in _pendingMessages)
            {
                await _clientToBackendChannel.Writer.WriteAsync(msg, cancellationToken);
            }
            _pendingMessages.Clear();
        }
        
        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
    }

    private async Task HandleSimpleQueryAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_pendingBackendRelease)
        {
            _logger.LogTrace("[Session {Id}] Simple Query after ReadyForQuery, releasing backend", _sessionInfo.Id);
            await ReleaseBackendAsync();
        }
        
        await EnsureBackendAsync(cancellationToken);
        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
        
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            await _readyForQueryChannel.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("[Session {Id}] Timeout waiting for ReadyForQuery after Query", _sessionInfo.Id);
        }
        
        _state = SessionState.WaitingForNextMessage;
    }

    private async Task HandleOtherMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (_pendingBackendRelease)
        {
            _logger.LogTrace("[Session {Id}] Non-Extended Query message after ReadyForQuery, releasing backend", _sessionInfo.Id);
            await ReleaseBackendAsync();
            _state = SessionState.Idle;
        }
        
        if (PgMessageScanner.RequiresBackend((char)message[0]))
        {
            await EnsureBackendAsync(cancellationToken);
        }
        
        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
    }

    private async Task HandleReadyForQueryAsync(byte status, CancellationToken cancellationToken)
    {
        _logger.LogTrace("[Session {Id}] ReadyForQuery: Status='{Status}'", _sessionInfo.Id, (char)status);

        await _readyForQueryChannel.Writer.WriteAsync(true, cancellationToken);

        if (status == 'I')
        {
            _logger.LogDebug("[Session {Id}] Transaction idle - scheduling backend release", _sessionInfo.Id);
            _pendingBackendRelease = true;
        }
        else if (status == 'T')
        {
            _logger.LogDebug("[Session {Id}] In transaction block - keeping backend", _sessionInfo.Id);
        }
        else if (status == 'E')
        {
            _logger.LogDebug("[Session {Id}] Failed transaction - keeping backend for rollback", _sessionInfo.Id);
        }
    }

    private async Task EnsureBackendAsync(CancellationToken cancellationToken)
    {
        if (_backendAcquired)
        {
            if (_pendingBackendRelease)
            {
                _logger.LogDebug("[Session {Id}] Releasing backend before acquiring new one", _sessionInfo.Id);
                await ReleaseBackendAsync();
            }
            else
            {
                return;
            }
        }

        var sw = Stopwatch.StartNew();
        
        try
        {
            var timeout = TimeSpan.FromSeconds(_config.Pool.ConnectionTimeout > 0 
                ? _config.Pool.ConnectionTimeout 
                : 60);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeout);

            var backend = await _pool.AcquireAsync(cts.Token);
            
            // Clear any stale messages from previous backend usage
            while (_backendToClientChannel.Reader.TryRead(out _))
            {
                _logger.LogTrace("[Session {Id}] Cleared stale message from backend channel", _sessionInfo.Id);
            }
            
            lock (_backendLock)
            {
                _backend = backend;
                _backendAcquired = true;
                _pendingBackendRelease = false;
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
            _pendingBackendRelease = false;
            _backendGeneration++;
        }

        while (_backendToClientChannel.Reader.TryRead(out var staleMsg))
        {
            _logger.LogTrace("[Session {Id}] Cleared stale message from backend channel during release", _sessionInfo.Id);
        }
        
        while (_readyForQueryChannel.Reader.TryRead(out _))
        {
            _logger.LogTrace("[Session {Id}] Cleared stale ReadyForQuery from channel during release", _sessionInfo.Id);
        }

        if (backend != null)
        {
            try
            {
                await BackendResetHelper.SendResetQueryAsync(
                    backend,
                    _config.Pool.ServerResetQuery,
                    _logger,
                    default);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Session {Id}] Failed to reset backend {BackendId}, releasing anyway", _sessionInfo.Id, backend.Id);
            }
            
            _pool.Release(backend);
            _onBackendReleased();
            _logger.LogDebug("[Session {Id}] Backend released: {BackendId} (gen {Gen})", _sessionInfo.Id, backend.Id, _backendGeneration);
        }
    }

    private async Task DestroyBackendAsync()
    {
        IServerConnection? backend;
        lock (_backendLock)
        {
            backend = _backend;
            _backend = null;
            _backendAcquired = false;
            _pendingBackendRelease = false;
        }

        if (backend != null)
        {
            await backend.DisposeAsync();
            _pool.Release(backend);
            _onBackendReleased();
            _logger.LogDebug("[Session {Id}] Backend destroyed: {BackendId}", _sessionInfo.Id, backend.Id);
        }
    }

    private async Task<byte[]?> ReadMessageAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
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
            try { await task; } catch { }
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
        
        _logger.LogDebug("[Session {Id}] Session cleaned up", _sessionInfo.Id);
    }

    public void Dispose()
    {
        _sessionCts.Cancel();
        CleanupAsync().GetAwaiter().GetResult();
        _sessionCts.Dispose();
    }
    
    private sealed class PrefixedStream : Stream
    {
        private readonly Stream _inner;
        private ReadOnlyMemory<byte> _prefix;

        public PrefixedStream(Stream inner, byte[] prefix)
        {
            _inner = inner;
            _prefix = prefix;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _inner.Length + _prefix.Length;
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
        public override int Read(Span<byte> buffer)
        {
            if (!_prefix.IsEmpty)
            {
                var toCopy = Math.Min(buffer.Length, _prefix.Length);
                _prefix.Span.Slice(0, toCopy).CopyTo(buffer);
                _prefix = _prefix.Slice(toCopy);
                return toCopy;
            }
            return _inner.Read(buffer);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (!_prefix.IsEmpty)
            {
                var toCopy = Math.Min(buffer.Length, _prefix.Length);
                _prefix.Slice(0, toCopy).CopyTo(buffer);
                _prefix = _prefix.Slice(toCopy);
                return toCopy;
            }
            return await _inner.ReadAsync(buffer, cancellationToken);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.WriteAsync(buffer, offset, count, cancellationToken);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Close() { _inner.Close(); base.Close(); }
    }
}
