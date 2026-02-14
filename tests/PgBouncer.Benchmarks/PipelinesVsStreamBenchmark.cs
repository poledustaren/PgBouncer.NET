using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace PgBouncer.Benchmarks;

/// <summary>
/// Performance benchmarks comparing Stream vs Pipelines I/O architectures.
/// Tests throughput, latency, and memory allocations.
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class PipelinesVsStreamBenchmark
{
    private const int MessageSize = 512;
    private const int MessageCount = 1000;
    private static readonly byte[] TestMessage = Encoding.ASCII.GetBytes(new string('A', MessageSize));

    private Pipe _pipeReader = null!;
    private Pipe _pipeWriter = null!;
    private MemoryStream _memoryStream = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Setup for Pipe benchmarks
        _pipeReader = new Pipe(new PipeOptions(
            minimumSegmentSize: 512,
            pauseWriterThreshold: 64 * 1024,
            useSynchronizationContext: false));

        _pipeWriter = new Pipe(new PipeOptions(
            minimumSegmentSize: 512,
            pauseWriterThreshold: 64 * 1024,
            useSynchronizationContext: false));

        // Setup for Stream benchmarks
        _memoryStream = new MemoryStream(64 * 1024);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _pipeReader?.Reader.Complete();
        _pipeReader?.Writer.Complete();
        _pipeWriter?.Reader.Complete();
        _pipeWriter?.Writer.Complete();
        _memoryStream?.Dispose();
    }

    #region Write Benchmarks

    [Benchmark]
    public async Task Stream_WriteAsync_SingleMessage()
    {
        _memoryStream.SetLength(0);
        _memoryStream.Position = 0;
        await _memoryStream.WriteAsync(TestMessage, 0, TestMessage.Length);
    }

    [Benchmark]
    public async Task Pipe_WriteAsync_SingleMessage()
    {
        _pipeWriter.Writer.Reset();
        await _pipeWriter.Writer.WriteAsync(TestMessage);
        await _pipeWriter.Writer.FlushAsync();
    }

    [Benchmark]
    public async Task Stream_WriteAsync_MultipleMessages()
    {
        _memoryStream.SetLength(0);
        _memoryStream.Position = 0;
        for (int i = 0; i < 10; i++)
        {
            await _memoryStream.WriteAsync(TestMessage, 0, TestMessage.Length);
        }
    }

    [Benchmark]
    public async Task Pipe_WriteAsync_MultipleMessages()
    {
        _pipeWriter.Writer.Reset();
        for (int i = 0; i < 10; i++)
        {
            await _pipeWriter.Writer.WriteAsync(TestMessage);
        }
        await _pipeWriter.Writer.FlushAsync();
    }

    #endregion

    #region Read Benchmarks

    [Benchmark(Baseline = true)]
    public async Task<int> Stream_ReadAsync_SingleMessage()
    {
        _memoryStream.SetLength(0);
        _memoryStream.Position = 0;
        await _memoryStream.WriteAsync(TestMessage, 0, TestMessage.Length);
        _memoryStream.Position = 0;

        var buffer = new byte[MessageSize];
        return await _memoryStream.ReadAsync(buffer, 0, buffer.Length);
    }

    [Benchmark]
    public async ValueTask<ReadResult> Pipe_ReadAsync_SingleMessage()
    {
        _pipeReader.Writer.Reset();
        await _pipeReader.Writer.WriteAsync(TestMessage);
        await _pipeReader.Writer.FlushAsync();

        var result = await _pipeReader.Reader.ReadAsync();
        _pipeReader.Reader.AdvanceTo(result.Buffer.End);
        return result;
    }

    [Benchmark]
    public async Task<int> Stream_ReadAsync_MultipleReads()
    {
        _memoryStream.SetLength(0);
        _memoryStream.Position = 0;

        // Write 10 messages
        for (int i = 0; i < 10; i++)
        {
            await _memoryStream.WriteAsync(TestMessage, 0, TestMessage.Length);
        }
        _memoryStream.Position = 0;

        // Read them back
        var buffer = new byte[MessageSize];
        int totalRead = 0;
        for (int i = 0; i < 10; i++)
        {
            totalRead += await _memoryStream.ReadAsync(buffer, 0, buffer.Length);
        }
        return totalRead;
    }

    [Benchmark]
    public async Task<int> Pipe_ReadAsync_MultipleReads()
    {
        _pipeReader.Writer.Reset();

        // Write 10 messages
        for (int i = 0; i < 10; i++)
        {
            await _pipeReader.Writer.WriteAsync(TestMessage);
        }
        await _pipeReader.Writer.FlushAsync();

        // Read them back
        int totalRead = 0;
        for (int i = 0; i < 10; i++)
        {
            var result = await _pipeReader.Reader.ReadAsync();
            totalRead += (int)result.Buffer.Length;
            _pipeReader.Reader.AdvanceTo(result.Buffer.End);
        }
        return totalRead;
    }

    #endregion

    #region Parse Benchmarks (PostgreSQL Protocol Message Parsing)

    private static readonly byte[] PgStartupMessage = CreatePgStartupMessage();

    private static byte[] CreatePgStartupMessage()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Length: 4 bytes for length field + protocol version + parameters
        int length = 4 + 4 + 5 + 1 + 8 + 1 + 5 + 1 + 8 + 1; // user + database + application_name

        writer.Write(IPAddress.HostToNetworkOrder(length));
        writer.Write(IPAddress.HostToNetworkOrder(0x00030000)); // Protocol version 3.0

        // user
        WritePgString(writer, "user");
        WritePgString(writer, "testuser");

        // database
        WritePgString(writer, "database");
        WritePgString(writer, "testdb");

        // application_name
        WritePgString(writer, "application_name");
        WritePgString(writer, "pgbouncer_bench");

        writer.Write((byte)0); // Terminator

        return ms.ToArray();
    }

    private static void WritePgString(BinaryWriter writer, string value)
    {
        foreach (char c in value)
        {
            writer.Write((byte)c);
        }
        writer.Write((byte)0);
    }

    [Benchmark]
    public void Stream_ParseStartupMessage()
    {
        var buffer = PgStartupMessage;
        int offset = 0;

        // Read length
        int length = ReadInt32_BE(buffer, ref offset);

        // Read protocol version
        int protocolVersion = ReadInt32_BE(buffer, ref offset);

        // Read parameters
        var parameters = new Dictionary<string, string>();
        while (offset < buffer.Length - 1)
        {
            string key = ReadPgString(buffer, ref offset);
            if (string.IsNullOrEmpty(key)) break;
            string value = ReadPgString(buffer, ref offset);
            parameters[key] = value;
        }
    }

    [Benchmark]
    public void Pipe_ParseStartupMessage()
    {
        var buffer = PgStartupMessage;
        var sequence = new ReadOnlySequence<byte>(buffer);

        var reader = new SequenceReader<byte>(sequence);

        // Read length
        reader.TryReadBigEndian(out int length);

        // Read protocol version
        reader.TryReadBigEndian(out int protocolVersion);

        // Read parameters
        var parameters = new Dictionary<string, string>();
        while (reader.Remaining > 1)
        {
            string key = ReadPgString(ref reader);
            if (string.IsNullOrEmpty(key)) break;
            string value = ReadPgString(ref reader);
            parameters[key] = value;
        }
    }

    private static int ReadInt32_BE(byte[] buffer, ref int offset)
    {
        int value = (buffer[offset] << 24) | (buffer[offset + 1] << 16) |
                    (buffer[offset + 2] << 8) | buffer[offset + 3];
        offset += 4;
        return value;
    }

    private static string ReadPgString(byte[] buffer, ref int offset)
    {
        int start = offset;
        while (offset < buffer.Length && buffer[offset] != 0)
        {
            offset++;
        }
        string result = Encoding.ASCII.GetString(buffer, start, offset - start);
        offset++; // Skip null terminator
        return result;
    }

    private static string ReadPgString(ref SequenceReader<byte> reader)
    {
        reader.TryReadTo(out ReadOnlySpan<byte> bytes, (byte)0);
        return Encoding.ASCII.GetString(bytes);
    }

    #endregion

    #region Throughput Benchmarks

    [Benchmark]
    public async Task<long> Stream_Throughput_SmallMessages()
    {
        _memoryStream.SetLength(0);
        _memoryStream.Position = 0;

        const int smallMessageSize = 64;
        var smallMessage = new byte[smallMessageSize];

        for (int i = 0; i < MessageCount; i++)
        {
            await _memoryStream.WriteAsync(smallMessage, 0, smallMessageSize);
        }

        return _memoryStream.Length;
    }

    [Benchmark]
    public async Task<long> Pipe_Throughput_SmallMessages()
    {
        _pipeWriter.Writer.Reset();

        const int smallMessageSize = 64;
        var smallMessage = new byte[smallMessageSize];

        for (int i = 0; i < MessageCount; i++)
        {
            await _pipeWriter.Writer.WriteAsync(smallMessage);
        }
        await _pipeWriter.Writer.FlushAsync();

        return smallMessageSize * MessageCount;
    }

    #endregion

    #region Memory Pool Benchmarks

    [Benchmark]
    public void ArrayPool_RentAndReturn()
    {
        var array = ArrayPool<byte>.Shared.Rent(MessageSize);
        ArrayPool<byte>.Shared.Return(array);
    }

    [Benchmark]
    public void IMemoryOwner_Allocate()
    {
        using var owner = new TestMemoryOwner(MessageSize);
    }

    private sealed class TestMemoryOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _array;

        public TestMemoryOwner(int length)
        {
            _array = ArrayPool<byte>.Shared.Rent(length);
            Memory = new Memory<byte>(_array, 0, length);
        }

        public Memory<byte> Memory { get; }

        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_array);
        }
    }

    #endregion
}

/// <summary>
/// Extension methods for PipeWriter to support Reset.
/// </summary>
public static class PipeWriterExtensions
{
    public static void Reset(this PipeWriter writer)
    {
        // Complete and reset the pipe
        try
        {
            writer.Complete();
        }
        catch { }
    }
}

/// <summary>
/// Simple program to run benchmarks.
/// </summary>
public class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Running PgBouncer Pipelines vs Stream Benchmarks");
        Console.WriteLine("=================================================");
        Console.WriteLine();

        BenchmarkRunner.Run<PipelinesVsStreamBenchmark>();
    }
}
