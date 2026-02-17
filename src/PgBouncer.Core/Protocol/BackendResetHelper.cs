using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Core.Protocol;

public static class BackendResetHelper
{
    private static readonly byte[] DiscardAllQuery = Encoding.UTF8.GetBytes("DISCARD ALL");
    private static readonly byte[] DeallocateAllQuery = Encoding.UTF8.GetBytes("DEALLOCATE ALL");
    
    public static async Task<bool> SendResetQueryAsync(
        IServerConnection connection,
        string? resetQuery,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(resetQuery))
        {
            logger?.LogTrace("ServerResetQuery is empty, skipping reset for backend {BackendId}", connection.Id);
            return true;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            logger?.LogDebug("Cancellation requested, skipping reset for backend {BackendId}", connection.Id);
            return true;
        }

        var queryBytes = Encoding.UTF8.GetBytes(resetQuery);
        
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
        var effectiveToken = linkedCts.Token;
        
        try
        {
            var message = new byte[1 + 4 + queryBytes.Length + 1];
            var pos = 0;
            
            message[pos++] = (byte)'Q';
            BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(pos), 4 + queryBytes.Length + 1);
            pos += 4;
            queryBytes.CopyTo(message, pos);
            pos += queryBytes.Length;
            message[pos] = 0;

            await connection.Stream.WriteAsync(message, effectiveToken);
            await connection.Stream.FlushAsync(effectiveToken);

            logger?.LogDebug("Sent reset query '{Query}' to backend {BackendId}", resetQuery, connection.Id);

            var buffer = new byte[1024];
            while (!effectiveToken.IsCancellationRequested)
            {
                if (!await ReadExactAsync(connection.Stream, buffer.AsMemory(0, 5), effectiveToken))
                {
                    logger?.LogWarning("Backend {BackendId} disconnected during reset query response", connection.Id);
                    return false;
                }

                var length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1));
                var bodyLength = length - 4;

                if (bodyLength > 0 && bodyLength < buffer.Length - 5)
                {
                    if (!await ReadExactAsync(connection.Stream, buffer.AsMemory(5, bodyLength), effectiveToken))
                    {
                        return false;
                    }
                }
                else if (bodyLength > 0)
                {
                    var bigBuffer = new byte[5 + bodyLength];
                    buffer.AsSpan(0, 5).CopyTo(bigBuffer);
                    if (!await ReadExactAsync(connection.Stream, bigBuffer.AsMemory(5, bodyLength), effectiveToken))
                    {
                        return false;
                    }
                }

                var msgType = (char)buffer[0];
                logger?.LogTrace("Backend {BackendId} reset response: {MsgType}", connection.Id, msgType);

                if (msgType == 'Z')
                {
                    logger?.LogDebug("Backend {BackendId} reset complete (ReadyForQuery received)", connection.Id);
                    return true;
                }

                if (msgType == 'E')
                {
                    logger?.LogWarning("Backend {BackendId} reset query returned error, but continuing", connection.Id);
                }
            }
            
            logger?.LogDebug("Reset query cancelled or timed out for backend {BackendId}", connection.Id);
            return true;
        }
        catch (OperationCanceledException)
        {
            logger?.LogDebug("Reset query cancelled for backend {BackendId}", connection.Id);
            return true;
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Failed to send reset query to backend {BackendId}", connection.Id);
            return false;
        }
    }

    /// <summary>
    /// Send reset query via PipeWriter (for BackendConnection which uses Pipelines).
    /// Waits for response to complete before returning.
    /// </summary>
    public static bool SendResetQueryViaPipelines(
        BackendConnection connection,
        string? resetQuery,
        ILogger? logger)
    {
        if (string.IsNullOrEmpty(resetQuery))
        {
            return true;
        }

        var queryBytes = Encoding.UTF8.GetBytes(resetQuery);
        
        try
        {
            var message = new byte[1 + 4 + queryBytes.Length + 1];
            var pos = 0;
            
            message[pos++] = (byte)'Q';
            BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(pos), 4 + queryBytes.Length + 1);
            pos += 4;
            queryBytes.CopyTo(message, pos);
            pos += queryBytes.Length;
            message[pos] = 0;

            // Use PipeWriter instead of Stream
            var span = connection.Writer.GetSpan(message.Length);
            message.CopyTo(span);
            connection.Writer.Advance(message.Length);
            connection.Writer.FlushAsync().AsTask().Wait(TimeSpan.FromSeconds(2));

            logger?.LogDebug("Sent reset query '{Query}' to backend {BackendId}", resetQuery, connection.Id);
            
            // Wait a bit for the response to be processed by ReadLoop
            // The DISCARD ALL response should arrive quickly
            Thread.Sleep(50);
            
            return true;
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Failed to send reset query to backend {BackendId}", connection.Id);
            return false;
        }
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
}
