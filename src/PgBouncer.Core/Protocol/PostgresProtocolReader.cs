using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;

namespace PgBouncer.Core.Protocol;

/// <summary>
/// Читает сообщения PostgreSQL из потока с использованием Pipelines для эффективности
/// </summary>
public class PostgresProtocolReader
{
    private readonly PipeReader _reader;
    private readonly ILogger? _logger;

    public PostgresProtocolReader(PipeReader reader, ILogger? logger = null)
    {
        _reader = reader;
        _logger = logger;
    }

    /// <summary>
    /// Читает следующее сообщение из потока
    /// </summary>
    public async ValueTask<PostgresMessage?> ReadMessageAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var result = await _reader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (TryReadMessage(ref buffer, out var message))
            {
                _reader.AdvanceTo(buffer.Start);
                return message;
            }

            if (result.IsCompleted)
            {
                return null; // Соединение закрыто
            }

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    /// <summary>
    /// Пытается прочитать одно сообщение из буфера
    /// </summary>
    private bool TryReadMessage(ref ReadOnlySequence<byte> buffer, out PostgresMessage? message)
    {
        message = null;

        // Минимум нужно: 1 байт (тип) + 4 байта (длина)
        if (buffer.Length < 5)
            return false;

        // Читаем тип сообщения
        var reader = new SequenceReader<byte>(buffer);
        reader.TryRead(out byte typeByte);
        var messageType = (MessageType)typeByte;

        // Читаем длину (включает сами 4 байта длины, но не тип)
        Span<byte> lengthBytes = stackalloc byte[4];
        reader.TryCopyTo(lengthBytes);
        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);

        // Проверяем, достаточно ли данных
        var totalLength = 1 + length; // тип + длина + payload
        if (buffer.Length < totalLength)
            return false;

        // Извлекаем payload (без типа и длины)
        var payloadLength = length - 4;
        var payloadBuffer = buffer.Slice(5, payloadLength);
        
        // Копируем payload в массив для парсинга
        byte[] payloadArray = ArrayPool<byte>.Shared.Rent(payloadLength);
        try
        {
            payloadBuffer.CopyTo(payloadArray);
            var payloadSpan = new ReadOnlySpan<byte>(payloadArray, 0, payloadLength);
            
            message = PostgresMessage.ReadFrom(messageType, payloadSpan);
            
            _logger?.LogTrace("Прочитано сообщение: {MessageType}, длина: {Length}", 
                messageType, length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadArray);
        }

        // Продвигаем буфер
        buffer = buffer.Slice(totalLength);
        return true;
    }

    /// <summary>
    /// Читает startup message (особый формат без типа)
    /// </summary>
    public async ValueTask<StartupMessage?> ReadStartupMessageAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var result = await _reader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (TryReadStartupMessage(ref buffer, out var message))
            {
                _reader.AdvanceTo(buffer.Start);
                return message;
            }

            if (result.IsCompleted)
                return null;

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private bool TryReadStartupMessage(ref ReadOnlySequence<byte> buffer, out StartupMessage? message)
    {
        message = null;

        if (buffer.Length < 8) // минимум: length(4) + protocol(4)
            return false;

        var reader = new SequenceReader<byte>(buffer);
        
        // Читаем длину
        Span<byte> lengthBytes = stackalloc byte[4];
        reader.TryCopyTo(lengthBytes);
        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);

        if (buffer.Length < length)
            return false;

        // Читаем protocol version
        reader.Advance(4);
        Span<byte> protocolBytes = stackalloc byte[4];
        reader.TryCopyTo(protocolBytes);
        var protocolVersion = BinaryPrimitives.ReadInt32BigEndian(protocolBytes);

        // Парсим параметры (key-value пары, null-terminated)
        var parameters = new Dictionary<string, string>();
        reader.Advance(4);
        
        var remaining = length - 8;
        byte[] paramBuffer = ArrayPool<byte>.Shared.Rent(remaining);
        try
        {
            var paramSlice = buffer.Slice(8, remaining);
            paramSlice.CopyTo(paramBuffer);
            
            int offset = 0;
            while (offset < remaining - 1) // -1 для финального null terminator
            {
                var key = ReadNullTerminatedString(paramBuffer, ref offset);
                if (string.IsNullOrEmpty(key))
                    break;
                    
                var value = ReadNullTerminatedString(paramBuffer, ref offset);
                parameters[key] = value;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(paramBuffer);
        }

        message = new StartupMessage
        {
            ProtocolVersion = protocolVersion,
            Parameters = parameters
        };

        buffer = buffer.Slice(length);
        return true;
    }

    private string ReadNullTerminatedString(byte[] buffer, ref int offset)
    {
        var start = offset;
        while (offset < buffer.Length && buffer[offset] != 0)
            offset++;
        
        var str = System.Text.Encoding.UTF8.GetString(buffer, start, offset - start);
        offset++; // skip null terminator
        return str;
    }
}

/// <summary>
/// Startup message (первое сообщение от клиента)
/// </summary>
public class StartupMessage
{
    public int ProtocolVersion { get; set; }
    public Dictionary<string, string> Parameters { get; set; } = new();
    
    public string? Database => Parameters.GetValueOrDefault("database");
    public string? User => Parameters.GetValueOrDefault("user");
}

// Добавляем заглушку для ILogger если нужно
public interface ILogger
{
    void LogTrace(string message, params object[] args);
}
