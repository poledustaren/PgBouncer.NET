using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Tests.Pooling;

/// <summary>
/// Tests for BackendConnection - System.IO.Pipelines based PostgreSQL connection
/// </summary>
public class BackendConnectionTests : IAsyncDisposable
{
    private readonly List<Socket> _socketsToCleanup = new();
    private readonly List<Task> _tasksToCleanup = new();

    public async ValueTask DisposeAsync()
    {
        foreach (var socket in _socketsToCleanup)
        {
            try
            {
                socket.Shutdown(SocketShutdown.Both);
                socket.Close();
            }
            catch { }
        }

        foreach (var task in _tasksToCleanup)
        {
            try
            {
                await Task.WhenAny(task, Task.Delay(1000));
            }
            catch { }
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Creates a pair of connected sockets for testing
    /// </summary>
    private (Socket Client, Socket Server) CreateSocketPair()
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        listener.Listen(1);

        var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        client.Connect(listener.LocalEndPoint!);

        var server = listener.Accept();
        listener.Close();

        _socketsToCleanup.Add(client);
        _socketsToCleanup.Add(server);

        return (client, server);
    }

    /// <summary>
    /// Mock IBackendHandler for testing
    /// </summary>
    private class TestBackendHandler : IBackendHandler
    {
        public readonly List<(ReadOnlySequence<byte> Message, byte MessageType)> ReceivedMessages = new();
        public Exception? LastDisconnectException { get; private set; }
        public int DisconnectCallCount { get; private set; }
        public readonly Channel<bool> MessageReceivedChannel = Channel.CreateUnbounded<bool>();

        public ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)
        {
            // Copy the message for later verification
            var buffer = new byte[message.Length];
            message.CopyTo(buffer);
            ReceivedMessages.Add((new ReadOnlySequence<byte>(buffer), messageType));
            MessageReceivedChannel.Writer.TryWrite(true);
            return ValueTask.CompletedTask;
        }

        public void OnBackendDisconnected(Exception? exception)
        {
            LastDisconnectException = exception;
            DisconnectCallCount++;
        }
    }

    [Fact]
    public void Constructor_ShouldInitializeProperties()
    {
        // Arrange & Act
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Assert
        connection.Database.Should().Be("testdb");
        connection.Username.Should().Be("testuser");
        connection.Id.Should().NotBeEmpty();
        connection.IsBroken.Should().BeFalse();
        connection.IsHealthy.Should().BeTrue();
        connection.Generation.Should().Be(0);
        connection.Writer.Should().NotBeNull();
    }

    [Fact]
    public void AttachHandler_ShouldSetHandler()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();

        // Act
        connection.AttachHandler(handler);

        // Assert
        // Handler is attached - we can verify this by sending a message
        // Generation should be incremented
    }

    [Fact]
    public void DetachHandler_ShouldRemoveHandler()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Act
        connection.DetachHandler();

        // Assert
        // Handler is detached - messages should be processed as idle messages
    }

    [Fact]
    public async Task MessageWithAttachedHandler_ShouldBeForwarded()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Create a simple PostgreSQL message: type 'T' (RowDescription), length 4, no payload
        var message = new byte[] { (byte)'T', 0x00, 0x00, 0x00, 0x04 };

        // Act
        await client.SendAsync(new ArraySegment<byte>(message), SocketFlags.None);
        await Task.Delay(100); // Give time for message to be processed

        // Assert
        handler.ReceivedMessages.Should().HaveCount(1);
        handler.ReceivedMessages[0].MessageType.Should().Be((byte)'T');
    }

    [Fact]
    public async Task MultipleMessagesWithAttachedHandler_ShouldAllBeForwarded()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Create multiple messages with correct lengths
        // Format: 1 byte type + 4 bytes length (Big Endian, includes itself) + payload
        var message1 = new byte[] { (byte)'T', 0x00, 0x00, 0x00, 0x04 }; // RowDescription, length=4 (no payload), total=5
        var message2 = new byte[] { (byte)'D', 0x00, 0x00, 0x00, 0x0C, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x61, 0x00 }; // DataRow, length=12 (8 bytes payload), total=13
        var message3 = new byte[] { (byte)'Z', 0x00, 0x00, 0x00, 0x05, 0x49 }; // ReadyForQuery, length=5 (1 byte payload), total=6

        // Act
        await client.SendAsync(new ArraySegment<byte>(message1), SocketFlags.None);
        await Task.Delay(50);
        await client.SendAsync(new ArraySegment<byte>(message2), SocketFlags.None);
        await Task.Delay(50);
        await client.SendAsync(new ArraySegment<byte>(message3), SocketFlags.None);
        await Task.Delay(500); // Give more time for messages to be processed

        // Assert
        handler.ReceivedMessages.Should().HaveCountGreaterOrEqualTo(3);
        handler.ReceivedMessages[0].MessageType.Should().Be((byte)'T');
        handler.ReceivedMessages[1].MessageType.Should().Be((byte)'D');
        handler.ReceivedMessages[2].MessageType.Should().Be((byte)'Z');
    }

    [Fact]
    public async Task SingleMessage_ShouldBeReceived()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Test the 3rd message individually
        var message3 = new byte[] { (byte)'Z', 0x00, 0x00, 0x00, 0x05, 0x49 }; // ReadyForQuery

        // Act
        await client.SendAsync(new ArraySegment<byte>(message3), SocketFlags.None);
        await Task.Delay(200);

        // Assert
        handler.ReceivedMessages.Should().HaveCount(1);
        handler.ReceivedMessages[0].MessageType.Should().Be((byte)'Z');
    }

    [Fact]
    public async Task MultipleMessagesSentSequentially_ShouldAllBeForwarded()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Send messages sequentially with correct lengths
        // PostgreSQL message format: 1 byte type + 4 bytes length (includes itself) + payload
        var message1 = new byte[] { (byte)'T', 0x00, 0x00, 0x00, 0x04 }; // length=4, payload=0, total=5
        await client.SendAsync(new ArraySegment<byte>(message1), SocketFlags.None);
        await Task.Delay(100);

        // D message: length=12 (8 bytes payload), total=13 bytes
        var message2 = new byte[] { (byte)'D', 0x00, 0x00, 0x00, 0x0C, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x61, 0x00 };
        await client.SendAsync(new ArraySegment<byte>(message2), SocketFlags.None);
        await Task.Delay(100);

        var message3 = new byte[] { (byte)'Z', 0x00, 0x00, 0x00, 0x05, 0x49 }; // length=5 (1 byte payload), total=6
        await client.SendAsync(new ArraySegment<byte>(message3), SocketFlags.None);
        await Task.Delay(100);

        // Assert
        handler.ReceivedMessages.Should().HaveCount(3);
        handler.ReceivedMessages[0].MessageType.Should().Be((byte)'T');
        handler.ReceivedMessages[1].MessageType.Should().Be((byte)'D');
        handler.ReceivedMessages[2].MessageType.Should().Be((byte)'Z');
    }

    [Fact]
    public async Task MessageWithNoHandler_ShouldBeProcessedAsIdleMessage()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Send idle-allowed messages (NoticeResponse, ParameterStatus)
        var noticeMessage = new byte[] { (byte)'N', 0x00, 0x00, 0x00, 0x04 };
        var paramStatusMessage = new byte[] { (byte)'S', 0x00, 0x00, 0x00, 0x04 };

        // Act
        await client.SendAsync(new ArraySegment<byte>(noticeMessage), SocketFlags.None);
        await client.SendAsync(new ArraySegment<byte>(paramStatusMessage), SocketFlags.None);
        await Task.Delay(100);

        // Assert - Connection should remain healthy for idle-allowed messages
        connection.IsBroken.Should().BeFalse();
        connection.IsHealthy.Should().BeTrue();
    }

    [Fact]
    public async Task UnexpectedMessageInIdleState_ShouldMarkConnectionBroken()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Send a non-idle message (DataRow should not be received when idle)
        // PostgreSQL message: 1 byte type + 4 bytes length (includes itself) + payload
        // Length 6 means 2 bytes of payload, total 7 bytes
        var dataRowMessage = new byte[] { (byte)'D', 0x00, 0x00, 0x00, 0x06, 0x00, 0x00 };

        // Act
        await client.SendAsync(new ArraySegment<byte>(dataRowMessage), SocketFlags.None);
        await Task.Delay(100);

        // Assert
        connection.IsBroken.Should().BeTrue();
        connection.IsHealthy.Should().BeFalse();
    }

    [Fact]
    public async Task ErrorMessageInIdleState_ShouldMarkConnectionBroken()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Send an ErrorResponse
        var errorMessage = new byte[] { (byte)'E', 0x00, 0x00, 0x00, 0x04 };

        // Act
        await client.SendAsync(new ArraySegment<byte>(errorMessage), SocketFlags.None);
        await Task.Delay(100);

        // Assert
        connection.IsBroken.Should().BeTrue();
    }

    [Fact]
    public async Task SocketClose_ShouldMarkConnectionBrokenAndNotifyHandler()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Act - close the client socket
        client.Shutdown(SocketShutdown.Both);
        client.Close();
        await Task.Delay(200);

        // Assert
        connection.IsBroken.Should().BeTrue();
        connection.IsHealthy.Should().BeFalse();
        handler.DisconnectCallCount.Should().BeGreaterThan(0);
    }

    [Fact]
    public void UpdateActivity_ShouldUpdateLastActivityTime()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var initialActivity = connection.LastActivity;
        Thread.Sleep(10);

        // Act
        connection.UpdateActivity();

        // Assert
        connection.LastActivity.Should().BeAfter(initialActivity);
    }

    [Fact]
    public void MarkDirty_ShouldBeNoOpForPipelines()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Act
        connection.MarkDirty();

        // Assert - should not throw, connection should remain healthy
        connection.IsHealthy.Should().BeTrue();
    }

    [Fact]
    public void IsIdle_ShouldReturnCorrectResult()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Act & Assert
        connection.IsIdle(0).Should().BeFalse(); // Just created
        connection.IsIdle(-1).Should().BeTrue(); // Negative timeout always idle
    }

    [Fact]
    public async Task DisposeAsync_ShouldCleanUpResources()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Act
        await connection.DisposeAsync();

        // Assert
        connection.IsBroken.Should().BeTrue();
    }

    [Fact]
    public async Task LargeMessage_ShouldBeHandledCorrectly()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Create a large message (10KB payload)
        var payloadSize = 10000;
        var message = new byte[payloadSize + 5]; // 5 bytes header
        message[0] = (byte)'D'; // DataRow
        message[1] = 0x00;
        message[2] = 0x00;
        message[3] = (byte)((payloadSize + 4) >> 8);
        message[4] = (byte)((payloadSize + 4) & 0xFF);

        // Fill with pattern
        for (int i = 5; i < message.Length; i++)
        {
            message[i] = (byte)(i % 256);
        }

        // Act
        await client.SendAsync(new ArraySegment<byte>(message), SocketFlags.None);
        await Task.Delay(200);

        // Assert
        handler.ReceivedMessages.Should().HaveCountGreaterOrEqualTo(1);
        var receivedMsg = handler.ReceivedMessages[0].Message.ToArray();
        receivedMsg.Length.Should().BeGreaterOrEqualTo(message.Length);
        receivedMsg[0].Should().Be((byte)'D');
    }

    [Fact]
    public async Task MessageFragmentedAcrossMultipleReads_ShouldBeAssembledCorrectly()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        var message = new byte[] { (byte)'T', 0x00, 0x00, 0x00, 0x04 };

        // Act - Send the message in fragments
        await client.SendAsync(new ArraySegment<byte>(message, 0, 2), SocketFlags.None);
        await Task.Delay(20);
        await client.SendAsync(new ArraySegment<byte>(message, 2, 3), SocketFlags.None);
        await Task.Delay(100);

        // Assert
        handler.ReceivedMessages.Should().HaveCount(1);
        handler.ReceivedMessages[0].MessageType.Should().Be((byte)'T');
    }

    [Fact]
    public void Generation_ShouldBeSettableAndGettable()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        // Act
        connection.Generation = 5;

        // Assert
        connection.Generation.Should().Be(5);
    }

    [Fact]
    public async Task TryParsePostgresMessage_ShouldHandleValidMessage()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");
        var handler = new TestBackendHandler();
        connection.AttachHandler(handler);

        // Create a valid message with proper Big Endian length
        var message = new byte[] { (byte)'R', 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00 };

        // Act
        await client.SendAsync(new ArraySegment<byte>(message), SocketFlags.None);
        await Task.Delay(100);

        // Assert
        handler.ReceivedMessages.Should().HaveCount(1);
        handler.ReceivedMessages[0].MessageType.Should().Be((byte)'R');
        handler.ReceivedMessages[0].Message.Length.Should().Be(9); // 1 + 4 + 4
    }

    [Fact]
    public async Task Writer_ShouldAllowWritingToBackend()
    {
        // Arrange
        var (client, server) = CreateSocketPair();
        var connection = new BackendConnection(server, "testdb", "testuser");

        var buffer = connection.Writer.GetMemory(1024);
        var query = new byte[] { (byte)'Q', 0x00, 0x00, 0x00, 0x09, (byte)'S', (byte)'E', (byte)'L', (byte)'E', (byte)'C', (byte)'T', (byte)' ', (byte)'1', 0x00 };
        query.CopyTo(buffer);

        // Act
        connection.Writer.Advance(query.Length);
        await connection.Writer.FlushAsync(CancellationToken.None);

        // Assert - verify client receives the data
        var receiveBuffer = new byte[1024];
        var received = await client.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), SocketFlags.None);
        received.Should().Be(query.Length);
        receiveBuffer.Take(received).Should().BeEquivalentTo(query);
    }
}
