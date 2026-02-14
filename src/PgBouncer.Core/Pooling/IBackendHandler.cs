using System.Buffers;

namespace PgBouncer.Core.Pooling;

/// <summary>
/// Handler for backend PostgreSQL messages forwarded to clients.
/// Part of System.IO.Pipelines architecture for zero-allocation message handling.
/// </summary>
public interface IBackendHandler
{
    /// <summary>
    /// Handle a message received from PostgreSQL backend
    /// </summary>
    /// <param name="message">The complete message including type byte and payload</param>
    /// <param name="messageType">The message type byte (first byte of message)</param>
    ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType);

    /// <summary>
    /// Called when backend connection is lost or encounters an error
    /// </summary>
    /// <param name="exception">The exception that caused disconnection, or null if graceful</param>
    void OnBackendDisconnected(Exception? exception);
}
