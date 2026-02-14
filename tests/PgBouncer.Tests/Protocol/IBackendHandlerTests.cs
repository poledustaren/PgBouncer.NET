using System.Buffers;
using System.Net.Sockets;
using PgBouncer.Core.Pooling;

namespace PgBouncer.Tests.Protocol;

/// <summary>
/// Tests for IBackendHandler interface - System.IO.Pipelines backend message handling
/// </summary>
public class IBackendHandlerTests
{
    /// <summary>
    /// Mock implementation of IBackendHandler for testing interface contracts
    /// </summary>
    private class MockBackendHandler : IBackendHandler
    {
        public List<(ReadOnlySequence<byte> Message, byte MessageType)> ReceivedMessages { get; } = new();
        public Exception? LastDisconnectedException { get; private set; }
        public int OnBackendDisconnectedCallCount { get; private set; }

        public ValueTask HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)
        {
            // Capture the message data for verification
            var buffer = new byte[message.Length];
            message.CopyTo(buffer);
            ReceivedMessages.Add((new ReadOnlySequence<byte>(buffer), messageType));

            return ValueTask.CompletedTask;
        }

        public void OnBackendDisconnected(Exception? exception)
        {
            LastDisconnectedException = exception;
            OnBackendDisconnectedCallCount++;
        }
    }

    [Fact]
    public void HandleBackendMessageAsync_ShouldBeCalledWithCorrectMessageAndType()
    {
        // Arrange
        var handler = new MockBackendHandler();
        var testMessage = new byte[] { 0x52, 0x00, 0x00, 0x00, 0x05, 0x74, 0x65, 0x73, 0x74 }; // 'R' message with "test" payload
        var messageType = (byte)'R'; // Authentication message type

        // Act
        var messageSequence = new ReadOnlySequence<byte>(testMessage);
        handler.HandleBackendMessageAsync(messageSequence, messageType);

        // Assert
        handler.ReceivedMessages.Should().HaveCount(1);
        handler.ReceivedMessages[0].MessageType.Should().Be(messageType);
        handler.ReceivedMessages[0].Message.Length.Should().Be(testMessage.Length);
        handler.ReceivedMessages[0].Message.ToArray().Should().BeEquivalentTo(testMessage);
    }

    [Fact]
    public async Task HandleBackendMessageAsync_ShouldHandleMultipleMessages()
    {
        // Arrange
        var handler = new MockBackendHandler();
        var message1 = new byte[] { 0x54, 0x00, 0x00, 0x00, 0x04 }; // 'T' row description
        var message2 = new byte[] { 0x44, 0x00, 0x00, 0x00, 0x06 }; // 'D' data row
        var message3 = new byte[] { 0x5A, 0x00, 0x00, 0x00, 0x05 }; // 'Z' ready for query

        // Act
        await handler.HandleBackendMessageAsync(new ReadOnlySequence<byte>(message1), (byte)'T');
        await handler.HandleBackendMessageAsync(new ReadOnlySequence<byte>(message2), (byte)'D');
        await handler.HandleBackendMessageAsync(new ReadOnlySequence<byte>(message3), (byte)'Z');

        // Assert
        handler.ReceivedMessages.Should().HaveCount(3);
        handler.ReceivedMessages[0].MessageType.Should().Be((byte)'T');
        handler.ReceivedMessages[1].MessageType.Should().Be((byte)'D');
        handler.ReceivedMessages[2].MessageType.Should().Be((byte)'Z');
    }

    [Fact]
    public void OnBackendDisconnected_ShouldRecordException()
    {
        // Arrange
        var handler = new MockBackendHandler();
        var testException = new InvalidOperationException("Backend connection lost");

        // Act
        handler.OnBackendDisconnected(testException);

        // Assert
        handler.LastDisconnectedException.Should().Be(testException);
        handler.OnBackendDisconnectedCallCount.Should().Be(1);
    }

    [Fact]
    public void OnBackendDisconnected_ShouldAllowNullException()
    {
        // Arrange
        var handler = new MockBackendHandler();

        // Act
        handler.OnBackendDisconnected(null);

        // Assert
        handler.LastDisconnectedException.Should().BeNull();
        handler.OnBackendDisconnectedCallCount.Should().Be(1);
    }

    [Fact]
    public void OnBackendDisconnected_ShouldRecordMultipleCalls()
    {
        // Arrange
        var handler = new MockBackendHandler();
        var exception1 = new IOException("First disconnect");
        var exception2 = new IOException("Second disconnect");

        // Act
        handler.OnBackendDisconnected(exception1);
        handler.OnBackendDisconnected(exception2);

        // Assert
        handler.OnBackendDisconnectedCallCount.Should().Be(2);
        handler.LastDisconnectedException.Should().Be(exception2);
    }

    [Fact]
    public async Task HandleBackendMessageAsync_ShouldHandleEmptyMessage()
    {
        // Arrange
        var handler = new MockBackendHandler();
        var emptyMessage = Array.Empty<byte>();
        var messageType = (byte)'N'; // Notice response

        // Act
        await handler.HandleBackendMessageAsync(new ReadOnlySequence<byte>(emptyMessage), messageType);

        // Assert
        handler.ReceivedMessages.Should().HaveCount(1);
        handler.ReceivedMessages[0].MessageType.Should().Be(messageType);
        handler.ReceivedMessages[0].Message.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public async Task HandleBackendMessageAsync_ShouldHandleLargeMessage()
    {
        // Arrange
        var handler = new MockBackendHandler();
        var largePayload = new byte[10000]; // Simulate a large query result
        for (int i = 0; i < largePayload.Length; i++)
        {
            largePayload[i] = (byte)(i % 256);
        }
        var message = new byte[largePayload.Length + 5];
        message[0] = (byte)'D'; // Data row
        Buffer.BlockCopy(largePayload, 0, message, 1, largePayload.Length);

        // Act
        await handler.HandleBackendMessageAsync(new ReadOnlySequence<byte>(message), (byte)'D');

        // Assert
        handler.ReceivedMessages.Should().HaveCount(1);
        handler.ReceivedMessages[0].Message.Length.Should().Be(message.Length);
    }
}
