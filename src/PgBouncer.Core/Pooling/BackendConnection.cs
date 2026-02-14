using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace PgBouncer.Core.Pooling;

/// <summary>
/// PostgreSQL backend connection using System.IO.Pipelines for zero-allocation I/O.
/// Maintains a read loop that parses PostgreSQL messages and forwards them to an attached handler.
/// When no handler is attached, processes idle messages (NoticeResponse, ParameterStatus, etc.)
/// and marks connection as broken if unexpected messages arrive.
/// </summary>
public sealed class BackendConnection : IServerConnection
{
    private readonly Socket _socket;
    private readonly PipeReader _reader;
    private readonly Pipe _writerPipe;
    private readonly ILogger? _logger;
    private readonly CancellationTokenSource _readLoopCts = new();
    private DateTime _lastActivity;
    private volatile bool _isBroken;
    private int _generation;
    private volatile IBackendHandler? _newHandler;
    private volatile IBackendPacketHandler? _legacyHandler;
    private Task? _readLoopTask;

    /// <summary>
    /// Creates a new BackendConnection
    /// </summary>
    public BackendConnection(Socket socket, string database, string username, ILogger? logger = null)
    {
        _socket = socket ?? throw new ArgumentNullException(nameof(socket));
        Database = database ?? throw new ArgumentNullException(nameof(database));
        Username = username ?? throw new ArgumentNullException(nameof(username));
        _logger = logger;

        Id = Guid.NewGuid();
        _lastActivity = DateTime.UtcNow;

        // Create pipe reader from socket
        var networkStream = new NetworkStream(socket, ownsSocket: false);
        _reader = PipeReader.Create(networkStream);

        // Create pipe for writing
        _writerPipe = new Pipe(new PipeOptions(
            minimumSegmentSize: 512,
            pauseWriterThreshold: 64 * 1024));

        Writer = _writerPipe.Writer;

        // Start the read loop immediately
        StartReaderLoop();
    }

    // IServerConnection properties
    public Guid Id { get; }
    public string Database { get; }
    public string Username { get; }

    /// <summary>
    /// Legacy Stream property - throws NotSupportedException for Pipelines-based connection
    /// </summary>
    public Stream Stream => throw new NotSupportedException("Use Writer property instead for Pipelines-based I/O");

    public bool IsBroken => _isBroken;
    public bool IsHealthy => !_isBroken && _socket.Connected;
    public DateTime LastActivity => _lastActivity;

    public int Generation
    {
        get => _generation;
        set => _generation = value;
    }

    /// <summary>
    /// Gets the PipeWriter for writing to the backend
    /// </summary>
    public PipeWriter Writer { get; }

    /// <summary>
    /// Attaches a new-style IBackendHandler to receive backend messages
    /// </summary>
    public void AttachHandler(IBackendHandler handler)
    {
        _newHandler = handler ?? throw new ArgumentNullException(nameof(handler));
        _legacyHandler = null; // Clear legacy handler
        _generation++;
        UpdateActivity();
    }

    /// <summary>
    /// Attaches a legacy IBackendPacketHandler for backward compatibility
    /// </summary>
    void IServerConnection.AttachHandler(IBackendPacketHandler handler)
    {
        _legacyHandler = handler ?? throw new ArgumentNullException(nameof(handler));
        _newHandler = null; // Clear new handler
        _generation++;
        UpdateActivity();
    }

    /// <summary>
    /// Detaches the current handler
    /// </summary>
    public void DetachHandler()
    {
        _newHandler = null;
        _legacyHandler = null;
        UpdateActivity();
    }

    /// <summary>
    /// Starts the reader loop (already started in constructor)
    /// </summary>
    public void StartReaderLoop()
    {
        if (_readLoopTask != null) return; // Already started

        _readLoopTask = Task.Run(async () => await ReadLoopAsync(_readLoopCts.Token));

        // Also start the writer loop to copy from pipe to socket
        _ = Task.Run(async () => await WriterLoopAsync(_readLoopCts.Token));
    }

    /// <summary>
    /// Updates the last activity timestamp
    /// </summary>
    public void UpdateActivity()
    {
        _lastActivity = DateTime.UtcNow;
    }

    /// <summary>
    /// Marks connection as dirty - no-op for Pipelines-based connection
    /// </summary>
    public void MarkDirty()
    {
        // No-op for Pipelines-based connection
        // In the old Stream-based implementation, this was used to track
        // whether the connection state was potentially inconsistent
    }

    /// <summary>
    /// Checks if connection has been idle longer than the specified timeout
    /// </summary>
    public bool IsIdle(int idleTimeoutSeconds)
    {
        if (idleTimeoutSeconds <= 0) return idleTimeoutSeconds < 0; // 0 timeout means never idle, negative means always idle
        return (DateTime.UtcNow - _lastActivity).TotalSeconds > idleTimeoutSeconds;
    }

    /// <summary>
    /// Main read loop using System.IO.Pipelines
    /// </summary>
    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        _logger?.LogDebug("[BackendConnection {Id}] Read loop started", Id);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    break;

                if (result.IsCompleted)
                {
                    // Socket closed
                    _logger?.LogDebug("[BackendConnection {Id}] Socket closed", Id);
                    _isBroken = true;
                    NotifyHandlerDisconnected(new SocketException((int)SocketError.ConnectionReset));
                    break;
                }

                // Process all complete messages in the buffer
                while (TryParsePostgresMessage(ref buffer, out var message, out var messageType))
                {
                    await ProcessMessageAsync(message, messageType);
                }

                // Tell the reader how much we consumed
                _reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during disposal
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "[BackendConnection {Id}] Read loop error", Id);
            _isBroken = true;
            NotifyHandlerDisconnected(ex);
        }
        finally
        {
            _logger?.LogDebug("[BackendConnection {Id}] Read loop ended", Id);
        }
    }

    /// <summary>
    /// Writer loop - copies data from the internal pipe to the socket
    /// </summary>
    private async Task WriterLoopAsync(CancellationToken cancellationToken)
    {
        _logger?.LogDebug("[BackendConnection {Id}] Writer loop started", Id);

        try
        {
            var reader = _writerPipe.Reader;

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    break;

                if (result.IsCompleted && buffer.IsEmpty)
                    break;

                // Write to socket
                foreach (var segment in buffer)
                {
                    if (segment.Length > 0)
                    {
                        var data = segment.ToArray();
                        await _socket.SendAsync(new ArraySegment<byte>(data), SocketFlags.None, cancellationToken);
                        UpdateActivity();
                    }
                }

                reader.AdvanceTo(buffer.End);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during disposal
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "[BackendConnection {Id}] Writer loop error", Id);
            _isBroken = true;
        }
        finally
        {
            _logger?.LogDebug("[BackendConnection {Id}] Writer loop ended", Id);
        }
    }

    /// <summary>
    /// Parses a PostgreSQL message from the buffer
    /// PostgreSQL message format: 1 byte type + 4 bytes length (Big Endian) + payload
    /// </summary>
    private bool TryParsePostgresMessage(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> message, out byte messageType)
    {
        message = default;
        messageType = 0;

        // Need at least 5 bytes for message header (type + length)
        if (buffer.Length < 5)
            return false;

        // Peek at the message type and length
        var reader = new SequenceReader<byte>(buffer);

        if (!reader.TryRead(out messageType))
            return false;

        // Read length as Big Endian
        if (!reader.TryReadBigEndian(out int messageLength))
            return false;

        // Validate message length
        if (messageLength < 4 || messageLength > 1024 * 1024) // Max 1MB per message
        {
            _logger?.LogWarning("[BackendConnection {Id}] Invalid message length: {Length}", Id, messageLength);
            _isBroken = true;
            return false;
        }

        var totalMessageLength = 1 + 4 + (messageLength - 4); // type + length field + payload

        // Check if we have the complete message
        if (buffer.Length < totalMessageLength)
            return false;

        // Extract the complete message
        message = buffer.Slice(0, totalMessageLength);
        buffer = buffer.Slice(totalMessageLength);

        return true;
    }

    /// <summary>
    /// Processes a parsed PostgreSQL message
    /// </summary>
    private async ValueTask ProcessMessageAsync(ReadOnlySequence<byte> message, byte messageType)
    {
        UpdateActivity();

        var newHandler = _newHandler;
        var legacyHandler = _legacyHandler;

        if (newHandler != null)
        {
            // Forward to new-style handler
            await newHandler.HandleBackendMessageAsync(message, messageType);
        }
        else if (legacyHandler != null)
        {
            // Forward to legacy handler - convert message to byte array
            var packet = new byte[message.Length];
            message.CopyTo(packet);
            await legacyHandler.HandlePacketAsync(packet, _generation);
        }
        else
        {
            // No handler - process as idle message
            ProcessIdleMessage(messageType);
        }
    }

    /// <summary>
    /// Processes a message when no handler is attached
    /// Allowed idle messages: NoticeResponse (N), ParameterStatus (S), Authentication (A), ReadyForQuery (Z)
    /// Error messages (E) mark connection as broken
    /// Any other message marks connection as broken
    /// </summary>
    private void ProcessIdleMessage(byte messageType)
    {
        switch ((char)messageType)
        {
            case 'N': // NoticeResponse - safe to ignore
                _logger?.LogDebug("[BackendConnection {Id}] Received NoticeResponse while idle", Id);
                break;

            case 'S': // ParameterStatus - safe to ignore
                _logger?.LogDebug("[BackendConnection {Id}] Received ParameterStatus while idle", Id);
                break;

            case 'A': // Authentication - handle (may need to reset connection)
                _logger?.LogDebug("[BackendConnection {Id}] Received Authentication message while idle", Id);
                break;

            case 'Z': // ReadyForQuery - connection is idle
                _logger?.LogDebug("[BackendConnection {Id}] Received ReadyForQuery while idle", Id);
                break;

            case 'E': // ErrorResponse - connection is broken
                _logger?.LogWarning("[BackendConnection {Id}] Received ErrorResponse while idle", Id);
                _isBroken = true;
                break;

            default:
                // Unexpected message - connection is broken
                _logger?.LogWarning("[BackendConnection {Id}] Received unexpected message {MessageType} while idle", Id, (char)messageType);
                _isBroken = true;
                break;
        }
    }

    /// <summary>
    /// Notifies attached handlers of disconnection
    /// </summary>
    private void NotifyHandlerDisconnected(Exception? exception)
    {
        _newHandler?.OnBackendDisconnected(exception);
        _legacyHandler?.OnBackendDisconnected();
    }

    /// <summary>
    /// Disposes the connection and cleans up resources
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        _logger?.LogDebug("[BackendConnection {Id}] Disposing", Id);

        _readLoopCts.Cancel();
        _isBroken = true;

        // Wait for read loop to complete
        if (_readLoopTask != null)
        {
            try
            {
                await Task.WhenAny(_readLoopTask, Task.Delay(1000));
            }
            catch { }
        }

        // Complete the writer pipe
        try
        {
            _writerPipe.Writer.Complete();
        }
        catch { }

        try
        {
            _writerPipe.Reader.Complete();
        }
        catch { }

        // Complete the reader
        try
        {
            await _reader.CompleteAsync();
        }
        catch { }

        // Close the socket
        try
        {
            if (_socket.Connected)
            {
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Close();
            }
        }
        catch { }

        // Cleanup
        _readLoopCts.Dispose();
    }
}
