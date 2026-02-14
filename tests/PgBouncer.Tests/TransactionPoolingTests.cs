using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Server;
using Xunit;

namespace PgBouncer.Tests;

public class TransactionPoolingTests
{
    private readonly Mock<IConnectionPool> _poolMock;
    private readonly Mock<IServerConnection> _backendMock;
    private readonly PgBouncerConfig _config;
    private readonly SessionInfo _sessionInfo;

    // Streams
    private readonly DuplexMemoryStream _clientStream; // Fake Client <-> Session
    private readonly DuplexMemoryStream _backendStream; // Fake Session <-> Backend

    public TransactionPoolingTests()
    {
        _poolMock = new Mock<IConnectionPool>();
        _backendMock = new Mock<IServerConnection>();
        _config = new PgBouncerConfig();
        _sessionInfo = new SessionInfo { Id = Guid.NewGuid() };

        _clientStream = new DuplexMemoryStream();
        _backendStream = new DuplexMemoryStream();

        _backendMock.Setup(x => x.Stream).Returns(_backendStream);
        _backendMock.Setup(x => x.Id).Returns(Guid.NewGuid());
        _backendMock.Setup(x => x.IsHealthy).Returns(true);

        _poolMock.Setup(x => x.AcquireAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_backendMock.Object);
    }

    [Fact]
    public async Task RunAsync_ShouldProcessQueryAndReleaseBackend()
    {
        // Arrange
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        // Disable ServerResetQuery for this test (we test the reset separately)
        _config.Pool.ServerResetQuery = "";

        // 1. Prepare Client Query: 'Q' "SELECT 1"
        var queryMsg = CreateQueryMessage("SELECT 1");
        _clientStream.WriteToInput(queryMsg);

        // 2. Prepare Backend Response: 'Z' (ReadyForQuery) - Idle
        // We write this to Backend's InputBuffer so Session can read it
        var backendResponse = CreateReadyForQueryMessage(true);
        _backendStream.WriteToInput(backendResponse);

        // Capture released connection
        IServerConnection? releasedConnection = null;
        _poolMock.Setup(x => x.Release(It.IsAny<IServerConnection>()))
            .Callback<IServerConnection>(c => releasedConnection = c);

        // Act
        var session = new TransactionPoolingSession(
            _clientStream,
            _poolMock.Object,
            _config,
            NullLogger.Instance,
            _sessionInfo,
            _ => { },
            () => { },
            () => { },
            () => { }
        );

        // Run session in background
        var runTask = session.RunAsync(cts.Token);

        // Allow some time for processing
        await Task.Delay(500);
        cts.Cancel();

        // Assert
        // 1. Check if backend was acquired
        _poolMock.Verify(x => x.AcquireAsync(It.IsAny<CancellationToken>()), Times.Once);

        // 2. Check if Session wrote Query to Backend (OutputBuffer of BackendStream)
        var backendReceived = _backendStream.OutputBuffer.ToArray();
        backendReceived.Should().Equal(queryMsg);

        // 3. Check if Session wrote Response to Client (OutputBuffer of ClientStream)
        var clientReceived = _clientStream.OutputBuffer.ToArray();
        clientReceived.Should().Equal(backendResponse);

        // 4. Check if Backend was Released
        _poolMock.Verify(x => x.Release(It.IsAny<IServerConnection>()), Times.Once);
        releasedConnection.Should().Be(_backendMock.Object);
    }

    [Fact]
    public async Task RunAsync_ShouldSendErrorWhenPoolTimeout()
    {
        // Arrange
        _poolMock.Setup(x => x.AcquireAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var timeoutRecorded = false;
        var session = new TransactionPoolingSession(
            _clientStream,
            _poolMock.Object,
            _config,
            NullLogger.Instance,
            _sessionInfo,
            _ => { },
            () => timeoutRecorded = true,
            () => { },
            () => { }
        );

        // Client sends a query that will trigger backend acquisition
        var queryMsg = CreateQueryMessage("SELECT 1");
        _clientStream.WriteToInput(queryMsg);

        // Act
        await session.RunAsync(CancellationToken.None);

        // Assert
        // 1. Verify timeout was recorded
        timeoutRecorded.Should().BeTrue("timeout callback should be invoked");

        // 2. Verify error was sent to client
        var clientOutput = _clientStream.OutputBuffer.ToArray();
        clientOutput.Length.Should().BeGreaterThan(0, "error response should be sent to client");
        clientOutput[0].Should().Be((byte)'E', "first byte should be ErrorResponse message type");

        // 3. Verify error message contains "timeout"
        var errorMsg = Encoding.UTF8.GetString(clientOutput);
        errorMsg.Should().Contain("timeout", "error message should mention timeout");

        // 4. Verify AcquireAsync was called
        _poolMock.Verify(x => x.AcquireAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RunAsync_ShouldSendResetQueryBeforeRelease()
    {
        // Arrange
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        _config.Pool.ServerResetQuery = "DISCARD ALL";

        var queryMsg = CreateQueryMessage("SELECT 1");
        _clientStream.WriteToInput(queryMsg);

        // First ReadyForQuery for the query
        var backendResponse = CreateReadyForQueryMessage(true);
        _backendStream.WriteToInput(backendResponse);
        // Second ReadyForQuery for the DISCARD ALL
        _backendStream.WriteToInput(backendResponse);

        IServerConnection? releasedConnection = null;
        _poolMock.Setup(x => x.Release(It.IsAny<IServerConnection>()))
            .Callback<IServerConnection>(c => releasedConnection = c);

        var session = new TransactionPoolingSession(
            _clientStream,
            _poolMock.Object,
            _config,
            NullLogger.Instance,
            _sessionInfo,
            _ => { },
            () => { },
            () => { },
            () => { }
        );

        var runTask = session.RunAsync(cts.Token);
        await Task.Delay(500);
        cts.Cancel();

        // Assert
        var backendReceived = _backendStream.OutputBuffer.ToArray();
        
        // Should contain: Query + DISCARD ALL
        backendReceived.Length.Should().BeGreaterThan(queryMsg.Length, "should contain query + reset query");
        
        // Check that DISCARD ALL was sent (starts after first query)
        var resetQueryStart = queryMsg.Length;
        var resetQueryType = (char)backendReceived[resetQueryStart];
        resetQueryType.Should().Be('Q', "reset query should be a Query message");
        
        // Verify backend was released
        _poolMock.Verify(x => x.Release(It.IsAny<IServerConnection>()), Times.Once);
    }

    private byte[] CreateQueryMessage(string query)
    {
        var queryBytes = Encoding.UTF8.GetBytes(query);
        // 'Q' + Int32(len) + query + null
        var len = 4 + queryBytes.Length + 1;
        var msg = new byte[1 + len];
        msg[0] = (byte)'Q';
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), len);
        queryBytes.CopyTo(msg.AsSpan(5));
        msg[^1] = 0;
        return msg;
    }

    private byte[] CreateReadyForQueryMessage(bool isIdle)
    {
        // 'Z' + Int32(5) + 'I'/'T'
        var msg = new byte[1 + 4 + 1];
        msg[0] = (byte)'Z';
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), 5);
        msg[5] = isIdle ? (byte)'I' : (byte)'T';
        return msg;
    }

    // Helper class to simulate full-duplex stream
    // Uses BlockingCollection for input to simulate blocking network reads
    // OutputBuffer: What we Write to, and "other side" reads from.
    private class DuplexMemoryStream : Stream
    {
        private readonly BlockingCollection<byte> _inputQueue = new BlockingCollection<byte>();
        private readonly MemoryStream _outputBuffer = new MemoryStream();
        private volatile bool _disconnected = false;

        public MemoryStream OutputBuffer => _outputBuffer;
        public bool IsDisconnected => _disconnected;

        // Add data to input (simulates network receive)
        public void WriteToInput(byte[] data)
        {
            if (_disconnected) throw new InvalidOperationException("Stream is disconnected");
            foreach (var b in data) _inputQueue.Add(b);
        }

        // Simulate disconnect
        public void SimulateDisconnect()
        {
            _disconnected = true;
            _inputQueue.CompleteAdding();
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() { _outputBuffer.Flush(); }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadAsync(buffer, offset, count).GetAwaiter().GetResult();
        }

        // Blocking read like real NetworkStream
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_disconnected || _inputQueue.IsCompleted) return 0;

            int read = 0;
            while (read < buffer.Length && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var b = _inputQueue.Take(cancellationToken);
                    buffer.Span[read++] = b;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (InvalidOperationException)
                {
                    // Queue is completed
                    break;
                }
            }
            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _outputBuffer.Write(buffer, offset, count);
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await _outputBuffer.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await _outputBuffer.WriteAsync(buffer, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
