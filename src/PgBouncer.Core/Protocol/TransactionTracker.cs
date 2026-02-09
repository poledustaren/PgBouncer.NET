namespace PgBouncer.Core.Protocol;

/// <summary>
/// Состояние транзакции PostgreSQL
/// </summary>
public enum TransactionState
{
    /// <summary>Вне транзакции (idle)</summary>
    Idle,

    /// <summary>В активной транзакции</summary>
    InTransaction,

    /// <summary>Транзакция завершилась с ошибкой (нужен ROLLBACK)</summary>
    Failed
}

/// <summary>
/// Отслеживание состояния транзакций для Transaction Pooling
/// </summary>
public class TransactionTracker
{
    private TransactionState _state = TransactionState.Idle;
    private int _transactionDepth; // Для SAVEPOINT

    /// <summary>Текущее состояние транзакции</summary>
    public TransactionState State => _state;

    /// <summary>Находимся ли в транзакции</summary>
    public bool InTransaction => _state != TransactionState.Idle;

    /// <summary>Можно ли вернуть соединение в пул</summary>
    public bool CanReleaseToPool => _state == TransactionState.Idle && _transactionDepth == 0;

    /// <summary>
    /// Обработать ReadyForQuery ('Z') сообщение от PostgreSQL
    /// Формат: 'Z' + int32(length) + byte(status)
    /// Status: 'I' = idle, 'T' = in transaction, 'E' = failed transaction
    /// </summary>
    public void ProcessReadyForQuery(byte statusByte)
    {
        _state = (char)statusByte switch
        {
            'I' => TransactionState.Idle,
            'T' => TransactionState.InTransaction,
            'E' => TransactionState.Failed,
            _ => _state
        };
    }

    /// <summary>
    /// Парсит байты и ищет ReadyForQuery сообщения
    /// Возвращает true если нашли ReadyForQuery
    /// </summary>
    public bool ProcessBackendData(ReadOnlySpan<byte> data)
    {
        bool foundReadyForQuery = false;
        int pos = 0;

        while (pos + 5 <= data.Length)
        {
            byte messageType = data[pos];

            // Читаем длину (big-endian)
            int length = (data[pos + 1] << 24) | (data[pos + 2] << 16) |
                         (data[pos + 3] << 8) | data[pos + 4];

            int totalLength = 1 + length; // type byte + length bytes + payload

            if (pos + totalLength > data.Length)
                break;

            // ReadyForQuery = 'Z' (0x5A)
            if (messageType == 0x5A && length == 5)
            {
                byte statusByte = data[pos + 5];
                ProcessReadyForQuery(statusByte);
                foundReadyForQuery = true;
            }

            pos += totalLength;
        }

        return foundReadyForQuery;
    }

    /// <summary>Сбросить состояние</summary>
    public void Reset()
    {
        _state = TransactionState.Idle;
        _transactionDepth = 0;
    }
}
