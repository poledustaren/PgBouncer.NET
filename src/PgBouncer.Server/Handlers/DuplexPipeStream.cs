using System;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using System.Buffers;

namespace PgBouncer.Server.Handlers;

/// <summary>
/// Wraps PipeReader and PipeWriter into a Stream for SslStream compatibility
/// </summary>
public class DuplexPipeStream : Stream
{
    private readonly PipeReader _input;
    private readonly PipeWriter _output;

    public DuplexPipeStream(PipeReader input, PipeWriter output)
    {
        _input = input;
        _output = output;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override void Flush()
    {
        // No-op sync flush
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _output.FlushAsync(cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadAsync(buffer, offset, count).Result;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await _input.ReadAsync(cancellationToken);
            var bufferSeq = result.Buffer;

            if (bufferSeq.IsEmpty && result.IsCompleted)
            {
                return 0;
            }

            if (!bufferSeq.IsEmpty)
            {
                var length = Math.Min((int)bufferSeq.Length, count);
                var slice = bufferSeq.Slice(0, length);
                slice.CopyTo(buffer.AsSpan(offset, length));

                _input.AdvanceTo(slice.End);
                return length;
            }

            _input.AdvanceTo(bufferSeq.Start, bufferSeq.End);
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        WriteAsync(buffer, offset, count).Wait();
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _output.WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
