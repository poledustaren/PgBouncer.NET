namespace PgBouncer.Core.Protocol;

/// <summary>
/// Типы сообщений PostgreSQL Wire Protocol 3.0
/// </summary>
public enum MessageType : byte
{
    // Frontend messages (от клиента)
    Query = (byte)'Q',
    Parse = (byte)'P',
    Bind = (byte)'B',
    Execute = (byte)'E',
    Describe = (byte)'D',
    Close = (byte)'C',
    Sync = (byte)'S',
    Flush = (byte)'H',
    Terminate = (byte)'X',
    PasswordMessage = (byte)'p',

    // Backend messages (от сервера)
    Authentication = (byte)'R',
    BackendKeyData = (byte)'K',
    BindComplete = (byte)'2',
    CloseComplete = (byte)'3',
    CommandComplete = (byte)'C',
    DataRow = (byte)'D',
    EmptyQueryResponse = (byte)'I',
    ErrorResponse = (byte)'E',
    NoData = (byte)'n',
    NoticeResponse = (byte)'N',
    ParameterDescription = (byte)'t',
    ParameterStatus = (byte)'S',
    ParseComplete = (byte)'1',
    ReadyForQuery = (byte)'Z',
    RowDescription = (byte)'T'
}

/// <summary>
/// Статус транзакции (из ReadyForQuery)
/// </summary>
public enum TransactionStatus : byte
{
    Idle = (byte)'I',           // Не в транзакции
    InTransaction = (byte)'T',   // В транзакции
    Failed = (byte)'E'           // Транзакция провалена
}

/// <summary>
/// Базовое сообщение PostgreSQL протокола
/// </summary>
public abstract class PostgresMessage
{
    public abstract MessageType Type { get; }

    /// <summary>
    /// Сериализация сообщения в байты
    /// </summary>
    public abstract void WriteTo(Stream stream);

    /// <summary>
    /// Десериализация из потока
    /// </summary>
    public static PostgresMessage ReadFrom(MessageType type, ReadOnlySpan<byte> payload)
    {
        // Примечание: некоторые MessageType имеют одинаковые байтовые значения
        // (например 'C' = CommandComplete и Close, 'E' = Execute и ErrorResponse)
        // Здесь мы парсим только по первому совпадению
        return type switch
        {
            MessageType.Query => QueryMessage.Parse(payload),
            MessageType.Parse => ParseMessage.Parse(payload),
            MessageType.Bind => BindMessage.Parse(payload),
            MessageType.Terminate => new TerminateMessage(),
            MessageType.ReadyForQuery => ReadyForQueryMessage.Parse(payload),
            _ => new UnknownMessage(type, payload.ToArray())
        };
    }
}

/// <summary>
/// Query (простой запрос)
/// </summary>
public class QueryMessage : PostgresMessage
{
    public override MessageType Type => MessageType.Query;
    public string Query { get; set; } = string.Empty;

    public override void WriteTo(Stream stream)
    {
        var queryBytes = System.Text.Encoding.UTF8.GetBytes(Query);
        var length = 4 + queryBytes.Length + 1; // length + query + null terminator

        stream.WriteByte((byte)Type);
        WriteInt32(stream, length);
        stream.Write(queryBytes);
        stream.WriteByte(0); // null terminator
    }

    public static QueryMessage Parse(ReadOnlySpan<byte> payload)
    {
        var query = System.Text.Encoding.UTF8.GetString(payload[..^1]); // убираем null terminator
        return new QueryMessage { Query = query };
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }
}

/// <summary>
/// Parse (prepared statement)
/// </summary>
public class ParseMessage : PostgresMessage
{
    public override MessageType Type => MessageType.Parse;
    public string StatementName { get; set; } = string.Empty;
    public string Query { get; set; } = string.Empty;
    public short[] ParameterTypes { get; set; } = Array.Empty<short>();

    public override void WriteTo(Stream stream)
    {
        // TODO: реализация
        throw new NotImplementedException();
    }

    public static ParseMessage Parse(ReadOnlySpan<byte> payload)
    {
        // TODO: полный парсинг
        return new ParseMessage();
    }
}

/// <summary>
/// Bind (привязка параметров)
/// </summary>
public class BindMessage : PostgresMessage
{
    public override MessageType Type => MessageType.Bind;

    public override void WriteTo(Stream stream)
    {
        throw new NotImplementedException();
    }

    public static BindMessage Parse(ReadOnlySpan<byte> payload)
    {
        return new BindMessage();
    }
}

/// <summary>
/// Execute (выполнение prepared statement)
/// </summary>
public class ExecuteMessage : PostgresMessage
{
    public override MessageType Type => MessageType.Execute;
    public string PortalName { get; set; } = string.Empty;
    public int MaxRows { get; set; }

    public override void WriteTo(Stream stream)
    {
        throw new NotImplementedException();
    }

    public static ExecuteMessage Parse(ReadOnlySpan<byte> payload)
    {
        return new ExecuteMessage();
    }
}

/// <summary>
/// Terminate (закрытие соединения)
/// </summary>
public class TerminateMessage : PostgresMessage
{
    public override MessageType Type => MessageType.Terminate;

    public override void WriteTo(Stream stream)
    {
        stream.WriteByte((byte)Type);
        WriteInt32(stream, 4); // только длина
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }
}

/// <summary>
/// ReadyForQuery (сервер готов к новому запросу)
/// </summary>
public class ReadyForQueryMessage : PostgresMessage
{
    public override MessageType Type => MessageType.ReadyForQuery;
    public TransactionStatus Status { get; set; }

    public override void WriteTo(Stream stream)
    {
        stream.WriteByte((byte)Type);
        WriteInt32(stream, 5); // length (4) + status (1)
        stream.WriteByte((byte)Status);
    }

    public static ReadyForQueryMessage Parse(ReadOnlySpan<byte> payload)
    {
        return new ReadyForQueryMessage
        {
            Status = (TransactionStatus)payload[0]
        };
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }
}

/// <summary>
/// CommandComplete (завершение команды)
/// </summary>
public class CommandCompleteMessage : PostgresMessage
{
    public override MessageType Type => MessageType.CommandComplete;
    public string CommandTag { get; set; } = string.Empty;

    public override void WriteTo(Stream stream)
    {
        var tagBytes = System.Text.Encoding.UTF8.GetBytes(CommandTag);
        var length = 4 + tagBytes.Length + 1;

        stream.WriteByte((byte)Type);
        WriteInt32(stream, length);
        stream.Write(tagBytes);
        stream.WriteByte(0);
    }

    public static CommandCompleteMessage Parse(ReadOnlySpan<byte> payload)
    {
        var tag = System.Text.Encoding.UTF8.GetString(payload[..^1]);
        return new CommandCompleteMessage { CommandTag = tag };
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }
}

/// <summary>
/// ErrorResponse (ошибка)
/// </summary>
public class ErrorResponseMessage : PostgresMessage
{
    public override MessageType Type => MessageType.ErrorResponse;
    public Dictionary<char, string> Fields { get; set; } = new();

    public override void WriteTo(Stream stream)
    {
        using var ms = new MemoryStream();
        foreach (var (code, value) in Fields)
        {
            ms.WriteByte((byte)code);
            var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);
            ms.Write(valueBytes);
            ms.WriteByte(0);
        }
        ms.WriteByte(0); // terminator

        stream.WriteByte((byte)Type);
        WriteInt32(stream, (int)(4 + ms.Length));
        ms.Position = 0;
        ms.CopyTo(stream);
    }

    public static ErrorResponseMessage Parse(ReadOnlySpan<byte> payload)
    {
        var msg = new ErrorResponseMessage();
        // TODO: полный парсинг полей
        return msg;
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }
}

/// <summary>
/// Неизвестное сообщение (для отладки)
/// </summary>
public class UnknownMessage : PostgresMessage
{
    private readonly MessageType _type;
    private readonly byte[] _payload;

    public UnknownMessage(MessageType type, byte[] payload)
    {
        _type = type;
        _payload = payload;
    }

    public override MessageType Type => _type;

    public override void WriteTo(Stream stream)
    {
        stream.WriteByte((byte)Type);
        WriteInt32(stream, 4 + _payload.Length);
        stream.Write(_payload);
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }
}
