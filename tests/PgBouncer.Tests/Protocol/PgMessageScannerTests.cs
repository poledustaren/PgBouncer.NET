using FluentAssertions;
using PgBouncer.Core.Protocol;
using Xunit;

namespace PgBouncer.Tests.Protocol;

public class PgMessageScannerTests
{
    [Fact]
    public void TryReadMessageInfo_WithValidQueryMessage_ShouldReturnTrue()
    {
        // Arrange
        var message = PgProtocolHelpers.CreateQueryMessage("SELECT 1");

        // Act
        var result = PgMessageScanner.TryReadMessageInfo(message, out var info);

        // Assert
        result.Should().BeTrue();
        info.Type.Should().Be(PgMessageTypes.Query);
        info.Length.Should().Be(message.Length - 1);
        info.TotalLength.Should().Be(message.Length);
        info.IsReadyForQuery.Should().BeFalse();
    }

    [Theory]
    [InlineData('I', PgTransactionState.Idle)]
    [InlineData('T', PgTransactionState.InTransaction)]
    [InlineData('E', PgTransactionState.Failed)]
    public void TryReadMessageInfo_WithReadyForQuery_ShouldReturnCorrectState(char statusChar, PgTransactionState expectedState)
    {
        // Arrange
        var message = PgProtocolHelpers.CreateReadyForQuery(statusChar);

        // Act
        var result = PgMessageScanner.TryReadMessageInfo(message, out var info);

        // Assert
        result.Should().BeTrue();
        info.Type.Should().Be(PgMessageTypes.ReadyForQuery);
        info.Length.Should().Be(message.Length - 1);
        info.TotalLength.Should().Be(message.Length);
        info.PgTransactionState.Should().Be(expectedState);
        info.IsReadyForQuery.Should().BeTrue();
        if (expectedState == PgTransactionState.Idle)
            info.CanReleaseBackend.Should().BeTrue();
        else
            info.CanReleaseBackend.Should().BeFalse();
    }

    [Fact]
    public void TryReadMessageInfo_WithBufferTooShort_ShouldReturnFalse()
    {
        // Arrange
        var buffer = new byte[] { (byte)'Q', 0, 0, 0 }; // Only 4 bytes, need 5

        // Act
        var result = PgMessageScanner.TryReadMessageInfo(buffer, out _);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void TryReadMessageInfo_WithReadyForQueryMissingStateByte_ShouldReturnDefaultState()
    {
        // Arrange
        var message = new byte[] { (byte)'Z', 0, 0, 0, 5 }; // ReadyForQuery header but no payload

        // Act
        var result = PgMessageScanner.TryReadMessageInfo(message, out var info);

        // Assert
        result.Should().BeTrue();
        info.PgTransactionState.Should().Be(PgTransactionState.Idle); // Default state
    }

    [Fact]
    public void TryReadMessageInfo_WithInvalidLengthTooSmall_ShouldReturnFalse()
    {
        // Arrange
        var buffer = new byte[] { (byte)'Q', 0, 0, 0, 3 }; // Length 3 is invalid (min 4)

        // Act
        var result = PgMessageScanner.TryReadMessageInfo(buffer, out _);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void TryReadMessageInfo_WithInvalidLengthTooLarge_ShouldReturnFalse()
    {
        // Arrange
        var buffer = new byte[] { (byte)'Q', 0x7F, 0xFF, 0xFF, 0xFF }; // Very large length

        // Act
        var result = PgMessageScanner.TryReadMessageInfo(buffer, out _);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void HasCompleteMessage_WithCompleteMessage_ShouldReturnTrue()
    {
        // Arrange
        var message = PgProtocolHelpers.CreateQueryMessage("SELECT 1");

        // Act
        var result = PgMessageScanner.HasCompleteMessage(message);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void HasCompleteMessage_WithIncompleteMessage_ShouldReturnFalse()
    {
        // Arrange
        var message = PgProtocolHelpers.CreateQueryMessage("SELECT 1");
        var incomplete = message.AsSpan(0, message.Length - 1);

        // Act
        var result = PgMessageScanner.HasCompleteMessage(incomplete);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void HasCompleteMessage_WithOnlyHeader_ShouldReturnFalse()
    {
        // Arrange
        var message = PgProtocolHelpers.CreateQueryMessage("SELECT 1");
        var headerOnly = message.AsSpan(0, 5);

        // Act
        var result = PgMessageScanner.HasCompleteMessage(headerOnly);

        // Assert
        result.Should().BeFalse();
    }

    [Theory]
    [InlineData(PgMessageTypes.Query, true)]
    [InlineData(PgMessageTypes.Parse, true)]
    [InlineData(PgMessageTypes.Bind, true)]
    [InlineData(PgMessageTypes.Execute, true)]
    [InlineData(PgMessageTypes.Describe, true)]
    [InlineData(PgMessageTypes.Close, true)]
    [InlineData(PgMessageTypes.Sync, true)]
    [InlineData(PgMessageTypes.Flush, true)]
    [InlineData(PgMessageTypes.CopyData, true)]
    [InlineData(PgMessageTypes.CopyDone, true)]
    [InlineData(PgMessageTypes.CopyFail, true)]
    [InlineData(PgMessageTypes.Terminate, false)]
    [InlineData(PgMessageTypes.ReadyForQuery, false)]
    public void RequiresBackend_ShouldReturnCorrectValue(char type, bool expected)
    {
        // Act
        var result = PgMessageScanner.RequiresBackend(type);

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData(PgMessageTypes.Parse, true)]
    [InlineData(PgMessageTypes.Bind, true)]
    [InlineData(PgMessageTypes.Execute, true)]
    [InlineData(PgMessageTypes.Describe, true)]
    [InlineData(PgMessageTypes.Close, true)]
    [InlineData(PgMessageTypes.Sync, true)]
    [InlineData(PgMessageTypes.Flush, true)]
    [InlineData(PgMessageTypes.Query, false)]
    [InlineData(PgMessageTypes.ReadyForQuery, false)]
    public void IsExtendedQueryMessage_ShouldReturnCorrectValue(char type, bool expected)
    {
        // Act
        var result = PgMessageScanner.IsExtendedQueryMessage(type);

        // Assert
        result.Should().Be(expected);
    }
}
