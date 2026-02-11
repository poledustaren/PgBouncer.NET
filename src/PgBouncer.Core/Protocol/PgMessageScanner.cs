using System.Buffers.Binary;

namespace PgBouncer.Core.Protocol;

/// <summary>
/// Состояние транзакции из ReadyForQuery
/// </summary>
public enum PgTransactionState : byte
{
    /// <summary>Вне транзакции - можно вернуть backend в пул</summary>
    Idle = (byte)'I',

    /// <summary>В активной транзакции - backend занят</summary>
    InTransaction = (byte)'T',

    /// <summary>Транзакция в состоянии ошибки - нужен ROLLBACK</summary>
    Failed = (byte)'E'
}

/// <summary>
/// Типы PostgreSQL сообщений
/// </summary>
public static class PgMessageTypes
{
    // === Frontend (Client → Server) ===
    public const char Query = 'Q';           // Simple Query
    public const char Parse = 'P';           // Extended Query: Parse
    public const char Bind = 'B';            // Extended Query: Bind
    public const char Execute = 'E';         // Extended Query: Execute
    public const char Describe = 'D';        // Describe statement/portal
    public const char Sync = 'S';            // Sync (end of extended query)
    public const char Terminate = 'X';       // Terminate connection
    public const char CopyData = 'd';        // COPY data
    public const char CopyDone = 'c';        // COPY done
    public const char CopyFail = 'f';        // COPY failed

    // === Backend (Server → Client) ===
    public const char Authentication = 'R';  // Auth request/response
    public const char ParameterStatus = 'S'; // Server parameter
    public const char BackendKeyData = 'K';  // Cancellation key
    public const char ReadyForQuery = 'Z';   // Ready for next query
    public const char RowDescription = 'T';  // Column metadata
    public const char DataRow = 'D';         // Row data (конфликт с Describe, но направление разное)
    public const char CommandComplete = 'C'; // Command completed
    public const char EmptyQueryResponse = 'I'; // Empty query
    public const char ErrorResponse = 'E';   // Error
    public const char NoticeResponse = 'N';  // Warning/notice
    public const char ParseComplete = '1';   // Parse completed
    public const char BindComplete = '2';    // Bind completed
    public const char CloseComplete = '3';   // Close completed
    public const char NoData = 'n';          // No data for Describe
    public const char PortalSuspended = 's'; // Portal suspended
}

/// <summary>
/// Результат сканирования PostgreSQL сообщения
/// </summary>
public readonly struct PgMessageInfo
{
    /// <summary>Тип сообщения (первый байт)</summary>
    public char Type { get; init; }

    /// <summary>Длина payload (не включая тип)</summary>
    public int Length { get; init; }

    /// <summary>Полная длина сообщения (1 + Length)</summary>
    public int TotalLength => 1 + Length;

    /// <summary>Состояние транзакции (только для ReadyForQuery)</summary>
    public PgTransactionState PgTransactionState { get; init; }

    /// <summary>Это ReadyForQuery?</summary>
    public bool IsReadyForQuery => Type == PgMessageTypes.ReadyForQuery;

    /// <summary>Транзакция завершена и backend можно вернуть в пул?</summary>
    public bool CanReleaseBackend => IsReadyForQuery && PgTransactionState == PgTransactionState.Idle;
}

/// <summary>
/// Zero-allocation сканер PostgreSQL сообщений
/// Позволяет парсить поток сообщений без создания объектов в heap
/// </summary>
public static class PgMessageScanner
{
    /// <summary>
    /// Минимальный размер заголовка PostgreSQL сообщения (тип + длина)
    /// </summary>
    public const int MinHeaderSize = 5;

    /// <summary>
    /// Попытаться прочитать информацию о сообщении из буфера
    /// </summary>
    /// <param name="buffer">Буфер с данными</param>
    /// <param name="info">Информация о сообщении</param>
    /// <returns>true если удалось прочитать полный заголовок</returns>
    public static bool TryReadMessageInfo(ReadOnlySpan<byte> buffer, out PgMessageInfo info)
    {
        info = default;

        if (buffer.Length < MinHeaderSize)
            return false;

        var type = (char)buffer[0];
        var length = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(1));

        // Sanity check
        if (length < 4 || length > 1_000_000_000)
            return false;

        var txState = PgTransactionState.Idle;

        // Для ReadyForQuery парсим состояние транзакции
        if (type == PgMessageTypes.ReadyForQuery && buffer.Length >= 6)
        {
            txState = (PgTransactionState)buffer[5];
        }

        info = new PgMessageInfo
        {
            Type = type,
            Length = length,
            PgTransactionState = txState
        };

        return true;
    }

    /// <summary>
    /// Проверить есть ли в буфере полное сообщение
    /// </summary>
    public static bool HasCompleteMessage(ReadOnlySpan<byte> buffer)
    {
        if (!TryReadMessageInfo(buffer, out var info))
            return false;

        return buffer.Length >= info.TotalLength;
    }

    /// <summary>
    /// Проверить требует ли сообщение от клиента backend connection
    /// </summary>
    public static bool RequiresBackend(char messageType)
    {
        return messageType switch
        {
            PgMessageTypes.Query => true,      // Simple Query
            PgMessageTypes.Parse => true,      // Extended Query
            PgMessageTypes.Bind => true,
            PgMessageTypes.Execute => true,
            PgMessageTypes.Describe => true,
            PgMessageTypes.Sync => true,
            PgMessageTypes.CopyData => true,
            PgMessageTypes.CopyDone => true,
            _ => false
        };
    }
}
