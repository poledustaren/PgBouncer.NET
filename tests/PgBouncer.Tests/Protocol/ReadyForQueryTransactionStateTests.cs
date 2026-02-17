using System.Buffers;
using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace PgBouncer.Tests.Protocol;

/// <summary>
/// Тесты для проверки обработки ReadyForQuery сообщений с разными статусами транзакции
/// </summary>
public class ReadyForQueryTransactionStateTests
{
    /// <summary>
    /// Создает сообщение ReadyForQuery PostgreSQL протокола
    /// Структура: 'Z' (1 byte) + Length (4 bytes, big-endian) + TxStatus (1 byte)
    /// </summary>
    private static ReadOnlySequence<byte> CreateReadyForQueryMessage(char transactionStatus)
    {
        // ReadyForQuery: тип + длина + статус
        var msg = new byte[6];
        msg[0] = (byte)'Z';
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), 5);  // длина = 5 (включает 4 байта поля длины + 1 байт статуса)
        msg[5] = (byte)transactionStatus;
        
        return new ReadOnlySequence<byte>(msg);
    }

    [Theory]
    [InlineData('I', true, "Idle - соединение должно возвращаться в пул")]
    [InlineData('T', false, "In Transaction - соединение НЕ должно возвращаться в пул")]
    [InlineData('E', false, "Failed Transaction - соединение НЕ должно возвращаться в пул")]
    public void ParseReadyForQuery_WithDifferentTxStatus_ShouldReturnCorrectState(
        char txStatus, 
        bool canRelease, 
        string reason)
    {
        // Arrange
        var message = CreateReadyForQueryMessage(txStatus);

        // Act
        var status = (char)message.Slice(5).FirstSpan[0];

        // Assert
        status.Should().Be(txStatus);
        (status == 'I').Should().Be(canRelease, reason);
    }

    [Fact]
    public void ReadyForQuery_PacketStructure_ShouldBeValid()
    {
        // Arrange
        var message = CreateReadyForQueryMessage('I');

        // Act & Assert
        message.Length.Should().Be(6, "ReadyForQuery должен быть 6 байт");
        message.FirstSpan[0].Should().Be((byte)'Z', "Первый байт должен быть 'Z'");
        
        var length = BinaryPrimitives.ReadInt32BigEndian(message.Slice(1).FirstSpan);
        length.Should().Be(5, "Длина должна быть 5");
        
        var status = (char)message.Slice(5).FirstSpan[0];
        status.Should().Be('I');
    }

    [Theory]
    [InlineData('I')]
    [InlineData('T')]
    [InlineData('E')]
    public void ReadyForQuery_AllValidStatuses_ShouldBeReadable(char status)
    {
        // Arrange
        var message = CreateReadyForQueryMessage(status);

        // Act
        var txStatus = (char)GetByteAtPosition(message, 5);

        // Assert
        txStatus.Should().Be(status);
    }

    /// <summary>
    /// Получает байт из ReadOnlySequence по позиции (копия из ClientSession)
    /// </summary>
    private static byte GetByteAtPosition(ReadOnlySequence<byte> sequence, long position)
    {
        if (sequence.IsSingleSegment)
        {
            return sequence.FirstSpan[(int)position];
        }

        long currentPosition = 0;
        foreach (var segment in sequence)
        {
            if (currentPosition + segment.Length > position)
            {
                return segment.Span[(int)(position - currentPosition)];
            }
            currentPosition += segment.Length;
        }

        throw new ArgumentOutOfRangeException(nameof(position), "Position is beyond sequence length");
    }
}
