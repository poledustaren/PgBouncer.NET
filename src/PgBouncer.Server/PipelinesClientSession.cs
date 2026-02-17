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

/// <summary>
/// Client session using System.IO.Pipelines for zero-allocation I/O.
/// Uses BackendConnection with Pipelines for backend communication.
/// Handles both Session and Transaction pooling modes.
/// </summary>
public sealed class PipelinesClientSession : IBackendHandler, IDisposable
{
    private readonly Socket _clientSocket;
    private readonly NetworkStream _clientStream;
    private readonly PipeReader _clientReader;
    private readonly Pipe _clientWritePipe;
    private readonly PgBouncerConfig _config;
    private readonly PoolManager? _poolManager;
    private readonly ILogger _logger;
    private readonly SessionInfo _sessionInfo;

    // Statistics tracking
    private int _waitingForBackend;
    private bool _disposed;

    // Backend connection management
    private IConnectionPool? _pool;
    private BackendConnection? _backend;
    private string? _database;
    private string? _username;
    private string? _password;

    // Channels for message passing
    private readonly Channel<byte[]> _clientToBackendChannel;
    private readonly Channel<byte[]> _backendToClientChannel;

    // Tasks
    private Task? _clientReaderTask;
    private Task? _clientWriterTask;
    private Task? _backendWriterTask;
    private Task? _backendToClientTask;

    private readonly CancellationTokenSource _cts = new();

    private bool _pendingBackendRelease;
    private readonly SemaphoreSlim _backendLock = new(1, 1);

    /// <summary>
    /// Creates a new PipelinesClientSession
    /// </summary>
    public PipelinesClientSession(
        Socket clientSocket,
        PgBouncerConfig config,
        PoolManager poolManager,
        ILogger logger,
        SessionInfo sessionInfo)
    {
        _clientSocket = clientSocket ?? throw new ArgumentNullException(nameof(clientSocket));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _poolManager = poolManager ?? throw new ArgumentNullException(nameof(poolManager));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _sessionInfo = sessionInfo ?? throw new ArgumentNullException(nameof(sessionInfo));

        // Create network stream for initial handshake
        _clientStream = new NetworkStream(clientSocket, ownsSocket: false);

        // Create pipe reader from client socket for message reading after handshake
        _clientReader = PipeReader.Create(_clientStream);

        // Create pipe for writing to client
        _clientWritePipe = new Pipe(new PipeOptions(
            minimumSegmentSize: 512,
            pauseWriterThreshold: 64 * 1024));

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
    }

    /// <summary>
    /// Creates a new PipelinesClientSession with direct IConnectionPool (for new Pipelines architecture)
    /// </summary>
    public PipelinesClientSession(
        Socket clientSocket,
        PgBouncerConfig config,
        IConnectionPool pool,
        ILogger logger,
        SessionInfo sessionInfo)
    {
        _clientSocket = clientSocket ?? throw new ArgumentNullException(nameof(clientSocket));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _sessionInfo = sessionInfo ?? throw new ArgumentNullException(nameof(sessionInfo));

        // Create network stream for initial handshake
        _clientStream = new NetworkStream(clientSocket, ownsSocket: false);

        // Create pipe reader from client socket for message reading after handshake
        _clientReader = PipeReader.Create(_clientStream);

        // Create pipe for writing to client
        _clientWritePipe = new Pipe(new PipeOptions(
            minimumSegmentSize: 512,
            pauseWriterThreshold: 64 * 1024));

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
    }

    /// <summary>
    /// Indicates whether the client has completed authentication
    /// </summary>
    public bool IsAuthenticated { get; private set; }

    /// <summary>
    /// Indicates whether the session has been disposed
    /// </summary>
    public bool IsDisposed => _disposed;

    /// <summary>
    /// Gets the PipeWriter for writing to the client
    /// </summary>
    public PipeWriter ClientWriter => _clientWritePipe.Writer;

    /// <summary>
    /// Runs the client session
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("[Session {Id}] Starting PipelinesClientSession", _sessionInfo.Id);

        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            var token = linkedCts.Token;

            // Phase 1: Read StartupMessage from client
            var initialData = await ReadStartupMessageAsync(token);
            if (initialData == null)
            {
                return; // Client disconnected during handshake
            }

            // Parse parameters from startup message
            ParseStartupParameters(initialData, out _database, out _username);
            _password = _config.Backend.AdminPassword;

            // Update session info
            _sessionInfo.Database = _database;
            _sessionInfo.Username = _username;

            // Get the pool for this database/user (only if using PoolManager constructor)
            if (_pool == null && _poolManager != null)
            {
                _pool = await _poolManager.GetPoolAsync(_database!, _username!, _password!);
                _logger.LogDebug("[Session {Id}] Pool acquired for {Database}/{User}", _sessionInfo.Id, _database, _username);
            }

            // Phase 2: Based on pooling mode, handle accordingly
            if (_config.Pool.Mode == PoolingMode.Transaction)
            {
                await RunTransactionPoolingAsync(initialData, token);
            }
            else
            {
                await RunSessionPoolingAsync(initialData, token);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("[Session {Id}] Session cancelled", _sessionInfo.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Session {Id}] Session error", _sessionInfo.Id);
            _sessionInfo.State = SessionState.Error;
        }
        finally
        {
            await CleanupAsync();
        }
    }

    /// <summary>
    /// Reads and parses the PostgreSQL StartupMessage
    /// Returns the initial data including any excess data, or null if client disconnected
    /// </summary>
    private async Task<byte[]?> ReadStartupMessageAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];

        // Read first packet
        var bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
        if (bytesRead < 8)
            return null;

        var position = 0;

        // Read length and protocol code
        var length = ReadInt32BigEndian(buffer, position);
        position += 4;
        var protocolCode = ReadInt32BigEndian(buffer, position);
        position += 4;

        // Check for SSLRequest
        if (protocolCode == 0x04D2162F) // SSLRequest
        {
            _logger.LogDebug("[Session {Id}] SSLRequest received", _sessionInfo.Id);

            // Send 'N' to deny SSL
            await _clientStream.WriteAsync(new byte[] { (byte)'N' }, 0, 1, cancellationToken);
            await _clientStream.FlushAsync(cancellationToken);

            // Read the actual StartupMessage
            bytesRead = await _clientStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
            if (bytesRead < 8)
                return null;

            length = ReadInt32BigEndian(buffer, 0);
            position = 4;
        }

        // Validate protocol code (should be 196608 for StartupMessage)
        if (protocolCode != 196608 && protocolCode != 0x04D2162F)
        {
            _logger.LogWarning("[Session {Id}] Invalid protocol code: {Code}", _sessionInfo.Id, protocolCode);
            return null;
        }

        // Re-read length if we had SSLRequest
        if (protocolCode == 196608)
        {
            length = ReadInt32BigEndian(buffer, 0);
            position = 4;
        }

        // Ensure we have the full startup message
        while (bytesRead < length)
        {
            var additional = await _clientStream.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead, cancellationToken);
            if (additional == 0)
                return null;
            bytesRead += additional;
        }

        // Check for excess data (pipelining/packet coalescing)
        byte[]? excessData = null;
        if (bytesRead > length)
        {
            var excessLen = bytesRead - length;
            excessData = new byte[excessLen];
            Array.Copy(buffer, length, excessData, 0, excessLen);
            _logger.LogDebug("[Session {Id}] {Bytes} excess bytes after startup", _sessionInfo.Id, excessLen);
        }

        // Return the startup message without excess
        var result = new byte[length];
        Array.Copy(buffer, 0, result, 0, length);

        return result;
    }

    /// <summary>
    /// Parses parameters from startup message
    /// </summary>
    private void ParseStartupParameters(byte[] startupData, out string? database, out string? username)
    {
        database = "postgres";
        username = "postgres";

        var position = 8; // Skip length and protocol
        while (position < startupData.Length - 1)
        {
            var keyStart = position;
            while (position < startupData.Length && startupData[position] != 0)
                position++;

            if (position >= startupData.Length) break;

            var key = System.Text.Encoding.UTF8.GetString(startupData, keyStart, position - keyStart);
            position++;

            if (string.IsNullOrEmpty(key)) break;

            var valueStart = position;
            while (position < startupData.Length && startupData[position] != 0)
                position++;

            var value = System.Text.Encoding.UTF8.GetString(startupData, valueStart, position - valueStart);
            position++;

            if (key == "database") database = value;
            else if (key == "user") username = value;
        }

        _logger.LogInformation("[Session {Id}] Startup: {Database}/{User}", _sessionInfo.Id, database, username);
    }

    /// <summary>
    /// Runs in Session pooling mode - one backend connection per client session
    /// </summary>
    private async Task RunSessionPoolingAsync(byte[] initialData, CancellationToken cancellationToken)
    {
        _sessionInfo.State = SessionState.WaitingForSlot;
        var waitStopwatch = Stopwatch.StartNew();

        try
        {
            // Acquire backend from pool
            var timeout = TimeSpan.FromSeconds(_config.Pool.ConnectionTimeout);
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(timeout);

            Interlocked.Increment(ref _waitingForBackend);
            var backend = await _pool!.AcquireAsync(timeoutCts.Token);
            Interlocked.Decrement(ref _waitingForBackend);

            waitStopwatch.Stop();
            _sessionInfo.WaitTimeMs = waitStopwatch.ElapsedMilliseconds;
            _sessionInfo.State = SessionState.Active;

            _backend = (BackendConnection)backend;
            _backend.AttachHandler(this);

// Start the loops
            _clientWriterTask = ClientWriterLoopAsync(cancellationToken);
            _backendWriterTask = BackendWriterLoopAsync(cancellationToken);
            _backendToClientTask = BackendToClientForwarderLoopAsync(cancellationToken);
            _clientReaderTask = ClientReaderLoopAsync(initialData, cancellationToken);

            // Wait for any to complete
            await Task.WhenAny(_clientReaderTask, _clientWriterTask, _backendWriterTask, _backendToClientTask);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            waitStopwatch.Stop();
            _sessionInfo.WaitTimeMs = waitStopwatch.ElapsedMilliseconds;
            _sessionInfo.State = SessionState.Error;
            _logger.LogWarning("[Session {Id}] Backend acquisition timeout", _sessionInfo.Id);
            await SendErrorAsync("too many connections - please try again later", "53300", cancellationToken);
        }
        finally
        {
            Interlocked.Exchange(ref _waitingForBackend, 0);
        }
    }

    /// <summary>
    /// Runs in Transaction pooling mode - sends AuthOk+ReadyForQuery then handles transaction
    /// </summary>
    private async Task RunTransactionPoolingAsync(byte[] initialData, CancellationToken cancellationToken)
    {
        _sessionInfo.State = SessionState.Idle;

        // Send AuthOk and ReadyForQuery to client
        await SendAuthOkAndReadyAsync(cancellationToken);
        IsAuthenticated = true;

// Start the loops
        _clientWriterTask = ClientWriterLoopAsync(cancellationToken);
        _backendWriterTask = BackendWriterLoopAsync(cancellationToken);
        _backendToClientTask = BackendToClientForwarderLoopAsync(cancellationToken);
        _clientReaderTask = TransactionPoolingReaderLoopAsync(initialData, cancellationToken);

        // Wait for any to complete
        await Task.WhenAny(_clientReaderTask, _clientWriterTask, _backendWriterTask, _backendToClientTask);
    }

    /// <summary>
    /// Client reader loop for session pooling - forwards all messages to backend
    /// </summary>
    private async Task ClientReaderLoopAsync(byte[] initialData, CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] Client reader loop started", _sessionInfo.Id);

        try
        {
            // Write initial data (StartupMessage) first
            if (initialData != null && initialData.Length > 0)
            {
                await _clientToBackendChannel.Writer.WriteAsync(initialData, cancellationToken);
            }

            // Then read from client
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _clientReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCompleted)
                {
                    _logger.LogDebug("[Session {Id}] Client disconnected", _sessionInfo.Id);
                    break;
                }

                if (buffer.Length > 0)
                {
                    var data = new byte[buffer.Length];
                    buffer.CopyTo(data);
                    await _clientToBackendChannel.Writer.WriteAsync(data, cancellationToken);
                }

                _clientReader.AdvanceTo(buffer.End);
            }
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Client reader error", _sessionInfo.Id);
        }
        finally
        {
            _clientReader.Complete();
            _clientToBackendChannel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Transaction pooling reader loop - handles client messages and manages backend acquisition/release
    /// </summary>
    private async Task TransactionPoolingReaderLoopAsync(byte[] initialData, CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] Transaction pooling reader loop started", _sessionInfo.Id);

        var state = TxState.Idle;
        var pendingMessages = new List<byte[]>();
        var buffer = ArrayPool<byte>.Shared.Rent(65536);

        try
        {
            // Create a stream with initial data
            Stream effectiveStream = _clientStream;

            if (initialData != null && initialData.Length > 0)
            {
                // Check if there's excess data after the startup message
                var startupLength = ReadInt32BigEndian(initialData, 0);
                if (initialData.Length > startupLength)
                {
                    // We already have startup + some messages
                    effectiveStream = new PrefixedStream(_clientStream, initialData);
                }
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await ReadMessageAsync(effectiveStream, buffer, cancellationToken);
                if (message == null)
                {
                    _logger.LogDebug("[Session {Id}] Client disconnected", _sessionInfo.Id);
                    break;
                }

                var msgTypeChar = (char)message[0];
                _logger.LogDebug("[Session {Id}] ->: Type='{Type}', State={State}",
                    _sessionInfo.Id, msgTypeChar, state);

                switch (msgTypeChar)
                {
                    case PgMessageTypes.Query:
                        await HandleSimpleQueryAsync(message, cancellationToken);
                        state = TxState.AwaitingReady;
                        break;

                    case PgMessageTypes.Parse:
                        await HandleParseAsync(message, cancellationToken);
                        state = TxState.InExtendedQuery;
                        break;

                    case PgMessageTypes.Bind:
                    case PgMessageTypes.Execute:
                    case PgMessageTypes.Describe:
                        pendingMessages.Add(message);
                        break;

                    case PgMessageTypes.Close:
                        pendingMessages.Add(message);
                        break;

                    case PgMessageTypes.Sync:
                        pendingMessages.Add(message);
                        await FlushPendingMessagesAsync(pendingMessages, cancellationToken);
                        pendingMessages.Clear();
                        state = TxState.AwaitingReady;
                        break;

                    case PgMessageTypes.Flush:
                        await FlushPendingMessagesAsync(pendingMessages, cancellationToken);
                        pendingMessages.Clear();
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
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Transaction pooling error", _sessionInfo.Id);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            _clientToBackendChannel.Writer.TryComplete();
        }
    }

    private enum TxState { Idle, InExtendedQuery, AwaitingReady, ReadyPendingRelease }

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

    private async Task HandleSimpleQueryAsync(byte[] message, CancellationToken cancellationToken)
    {
        await EnsureBackendAsync(cancellationToken);
        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
    }

    private async Task HandleParseAsync(byte[] message, CancellationToken cancellationToken)
    {
        await EnsureBackendAsync(cancellationToken);
        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
    }

    private async Task HandleOtherMessageAsync(byte[] message, CancellationToken cancellationToken)
    {
        if (PgMessageScanner.RequiresBackend((char)message[0]))
        {
            await EnsureBackendAsync(cancellationToken);
        }
        await _clientToBackendChannel.Writer.WriteAsync(message, cancellationToken);
    }

    private async Task FlushPendingMessagesAsync(List<byte[]> messages, CancellationToken cancellationToken)
    {
        if (messages.Count == 0) return;
        await EnsureBackendAsync(cancellationToken);
        foreach (var msg in messages)
        {
            await _clientToBackendChannel.Writer.WriteAsync(msg, cancellationToken);
        }
    }

    private async Task EnsureBackendAsync(CancellationToken cancellationToken)
    {
        if (_backend != null) return;

        await _backendLock.WaitAsync(cancellationToken);
        try
        {
            if (_backend != null) return;

            var timeout = TimeSpan.FromSeconds(_config.Pool.ConnectionTimeout);
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(timeout);

            Interlocked.Increment(ref _waitingForBackend);
            try
            {
                var backend = await _pool!.AcquireAsync(timeoutCts.Token);
                _backend = (BackendConnection)backend;
                _backend.AttachHandler(this);
                _logger.LogDebug("[Session {Id}] Backend acquired: {BackendId}", _sessionInfo.Id, _backend.Id);
            }
            finally
            {
                Interlocked.Decrement(ref _waitingForBackend);
            }
        }
        finally
        {
            _backendLock.Release();
        }
    }

    private async Task ReleaseBackendAsync()
    {
        await _backendLock.WaitAsync();
        try
        {
            if (_backend == null) return;

            var backendToRelease = _backend;
            _backend = null;

            backendToRelease.DetachHandler();
            
            if (backendToRelease.IsHealthy)
            {
                try
                {
                    // Посылаем сброс монопольно, никто больше не пишет в этот поток
                    await backendToRelease.ExecuteResetQueryAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[Session {Id}] Failed to reset backend {BackendId}", _sessionInfo.Id, backendToRelease.Id);
                    backendToRelease.MarkAsBroken();
                }
            }

            _pool!.Release(backendToRelease);
            _logger.LogDebug("[Session {Id}] Backend released: {BackendId}", _sessionInfo.Id, backendToRelease.Id);
        }
        finally
        {
            _backendLock.Release();
        }
    }

    /// <summary>
    /// Backend writer loop - writes to backend
    /// </summary>
    private async Task BackendWriterLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] Backend writer loop started", _sessionInfo.Id);

        try
        {
            await foreach (var message in _clientToBackendChannel.Reader.ReadAllAsync(cancellationToken))
            {
                await _backendLock.WaitAsync(cancellationToken);
                try
                {
                    // Если бэкенда нет (отцепили предыдущий), запрашиваем новый
                    if (_backend == null)
                    {
                        var timeout = TimeSpan.FromSeconds(_config.Pool.ConnectionTimeout);
                        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        timeoutCts.CancelAfter(timeout);

                        Interlocked.Increment(ref _waitingForBackend);
                        try
                        {
                            var backend = await _pool!.AcquireAsync(timeoutCts.Token);
                            _backend = (BackendConnection)backend;
                            _backend.AttachHandler(this);
                            _logger.LogDebug("[Session {Id}] Backend acquired (writer): {BackendId}", _sessionInfo.Id, _backend.Id);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _waitingForBackend);
                        }
                    }

                    // Пишем безопасно, так как ReleaseBackendAsync не сможет вклиниться
                    var writer = _backend.Writer;
                    writer.Write(message);
                    await writer.FlushAsync(cancellationToken);
                }
                finally
                {
                    _backendLock.Release();
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

    /// <summary>
    /// Backend to client forwarder loop - reads from backend channel and writes to client pipe
    /// </summary>
    private async Task BackendToClientForwarderLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] Backend->Client forwarder loop started", _sessionInfo.Id);

        try
        {
            await foreach (var message in _backendToClientChannel.Reader.ReadAllAsync(cancellationToken))
            {
                var writer = _clientWritePipe.Writer;
                writer.Write(message);
                await writer.FlushAsync(cancellationToken);
            }
        }
        catch (ChannelClosedException)
        {
            _logger.LogDebug("[Session {Id}] Backend->Client channel closed", _sessionInfo.Id);
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Backend->Client forwarder error", _sessionInfo.Id);
        }
        finally
        {
            _clientWritePipe.Writer.Complete();
        }
    }

    /// <summary>
    /// Client writer loop - writes to client socket
    /// </summary>
    private async Task ClientWriterLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("[Session {Id}] Client writer loop started", _sessionInfo.Id);

        try
        {
            var reader = _clientWritePipe.Reader;

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCompleted && buffer.IsEmpty)
                    break;

                // Write to socket
                foreach (var segment in buffer)
                {
                    if (segment.Length > 0)
                    {
                        var data = segment.ToArray();
                        await _clientSocket.SendAsync(new ArraySegment<byte>(data), SocketFlags.None, cancellationToken);
                    }
                }

                reader.AdvanceTo(buffer.End);
            }
        }
        catch (Exception ex) when (!IsExpectedException(ex))
        {
            _logger.LogError(ex, "[Session {Id}] Client writer error", _sessionInfo.Id);
        }
        finally
        {
            _clientWritePipe.Reader.Complete();
        }
    }

    /// <summary>
    /// IBackendHandler implementation - receives messages from backend
    /// </summary>
    public ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)
    {
        var msgTypeChar = (char)messageType;
        _logger.LogDebug("[Session {Id}] <-: Type='{Type}'", _sessionInfo.Id, msgTypeChar);

        // Forward to client via channel
        var data = new byte[message.Length];
        message.CopyTo(data);

        if (_config.Pool.Mode == PoolingMode.Transaction && msgTypeChar == PgMessageTypes.ReadyForQuery)
        {
            byte txState = (byte)'I';
            if (message.Length >= 6)
            {
                txState = data[5];
            }

            if (txState == 'I')
            {
                _pendingBackendRelease = true;
            }
        }

        _backendToClientChannel.Writer.TryWrite(data);

        if (_pendingBackendRelease)
        {
            _pendingBackendRelease = false;
            _ = Task.Run(async () =>
            {
                try
                {
                    await _clientWritePipe.Writer.FlushAsync();
                    await ReleaseBackendAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[Session {Id}] Failed to release backend", _sessionInfo.Id);
                }
            });
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// IBackendHandler implementation - backend disconnected
    /// </summary>
    public void OnBackendDisconnected(Exception? exception)
    {
        _logger.LogWarning(exception, "[Session {Id}] Backend disconnected", _sessionInfo.Id);
        _backend = null;
        _cts.Cancel();
    }

    /// <summary>
    /// Sends AuthOk and ReadyForQuery to client
    /// </summary>
    private async Task SendAuthOkAndReadyAsync(CancellationToken cancellationToken)
    {
        using var ms = new MemoryStream(512);

        // AuthenticationOk
        ms.WriteByte((byte)'R');
        WriteInt32BigEndian(ms, 8);
        WriteInt32BigEndian(ms, 0);

        // ParameterStatus messages
        WriteParameterStatus(ms, "server_version", "16.0");
        WriteParameterStatus(ms, "server_encoding", "UTF8");
        WriteParameterStatus(ms, "client_encoding", "UTF8");
        WriteParameterStatus(ms, "DateStyle", "ISO, MDY");
        WriteParameterStatus(ms, "TimeZone", "UTC");

        // BackendKeyData
        ms.WriteByte((byte)'K');
        WriteInt32BigEndian(ms, 12);
        WriteInt32BigEndian(ms, Environment.ProcessId);
        WriteInt32BigEndian(ms, Random.Shared.Next());

        // ReadyForQuery
        ms.WriteByte((byte)'Z');
        WriteInt32BigEndian(ms, 5);
        ms.WriteByte((byte)'I');

        var data = ms.ToArray();
        var writer = _clientWritePipe.Writer;
        writer.Write(data);
        await writer.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Sends an error message to the client
    /// </summary>
    private async Task SendErrorAsync(string message, string sqlState, CancellationToken cancellationToken)
    {
        var msgBytes = System.Text.Encoding.UTF8.GetBytes(message);
        var stateBytes = System.Text.Encoding.UTF8.GetBytes(sqlState);

        using var ms = new MemoryStream();

        ms.WriteByte((byte)'E');

        // Length (not including type byte)
        var length = 4 + 1 + 1 + 1 + 1 + stateBytes.Length + 1 + 1 + msgBytes.Length + 1 + 1;
        WriteInt32BigEndian(ms, length);

        // Severity
        ms.WriteByte((byte)'S');
        ms.WriteByte((byte)'F'); // FATAL
        ms.WriteByte(0);

        // SQLSTATE
        ms.WriteByte((byte)'C');
        ms.Write(stateBytes);
        ms.WriteByte(0);

        // Message
        ms.WriteByte((byte)'M');
        ms.Write(msgBytes);
        ms.WriteByte(0);

        // Terminator
        ms.WriteByte(0);

        var data = ms.ToArray();
        var writer = _clientWritePipe.Writer;
        writer.Write(data);
        await writer.FlushAsync(cancellationToken);
    }

    private static void WriteParameterStatus(MemoryStream ms, string name, string value)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);
        var length = 4 + nameBytes.Length + 1 + valueBytes.Length + 1;

        ms.WriteByte((byte)'S');
        WriteInt32BigEndian(ms, length);
        ms.Write(nameBytes);
        ms.WriteByte(0);
        ms.Write(valueBytes);
        ms.WriteByte(0);
    }

    private static void WriteInt32BigEndian(MemoryStream ms, int value)
    {
        var buf = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buf, value);
        ms.Write(buf);
    }

    private static int ReadInt32BigEndian(byte[] buffer, int offset)
    {
        return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(new ReadOnlySpan<byte>(buffer, offset, 4));
    }

    private static bool IsExpectedException(Exception ex)
    {
        return ex is OperationCanceledException
            || (ex is IOException ioEx && ioEx.InnerException is SocketException);
    }

    private async Task CleanupAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _logger.LogDebug("[Session {Id}] Cleaning up", _sessionInfo.Id);

        _cts.Cancel();

        await ReleaseBackendAsync();

        try { await _clientReader.CompleteAsync(); } catch { }
        try { _clientWritePipe.Reader.Complete(); } catch { }
        try { _clientWritePipe.Writer.Complete(); } catch { }

        try { _clientStream.Dispose(); } catch { }
        try { _clientSocket.Shutdown(SocketShutdown.Both); } catch { }
        try { _clientSocket.Close(); } catch { }

        _cts.Dispose();

        _sessionInfo.State = SessionState.Completed;
        _logger.LogDebug("[Session {Id}] Cleanup complete", _sessionInfo.Id);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        await CleanupAsync();
        _cts.Dispose();
    }

    public void Dispose()
    {
        if (_disposed) return;
        CleanupAsync().GetAwaiter().GetResult();
        _cts.Dispose();
        _backendLock.Dispose();
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
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
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
                _prefix.Span.Slice(0, toCopy).CopyTo(buffer.Span);
                _prefix = _prefix.Slice(toCopy);
                return toCopy;
            }
            return await _inner.ReadAsync(buffer, cancellationToken);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Write(ReadOnlySpan<byte> buffer) => throw new NotSupportedException();
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => throw new NotSupportedException();
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Close() { _inner.Close(); base.Close(); }
    }
}
