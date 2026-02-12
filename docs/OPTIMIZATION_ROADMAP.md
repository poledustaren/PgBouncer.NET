# 🚀 PgBouncer.NET Optimization Roadmap

> Глубокий анализ и план превращения PoC в высокопроизводительное решение, способное конкурировать с нативным C-кодом.

## Часть 1: Сравнение архитектур

| Характеристика        | PgBouncer (C/MinGW)                      | PgBouncer.NET (Текущий) | **PgBouncer.NET (Целевой)**      |
| --------------------- | ---------------------------------------- | ----------------------- | -------------------------------- |
| **Механизм**          | Single-threaded Event Loop (select/IOCP) | Task-based Async/Await  | SocketAsyncEventArgs / Pipelines |
| **Context Switching** | Почти нет (один поток)                   | Высокий                 | Умеренный (Thread Affinity)      |
| **Масштабирование**   | Плохое на Windows                        | Хорошее                 | Отличное                         |

### Управление памятью

- **PgBouncer (C):** Slab allocator, нет GC
- **PgBouncer.NET (сейчас):** GC Pressure от Task/byte[] аллокаций
- **Решение:** `ArrayPool<T>`, `Span<T>`, `Memory<T>`, `struct` вместо `class`

---

## Часть 2: Расчёт ресурсов

### Стоимость соединения (текущая реализация)

| Объект                | Размер                   |
| --------------------- | ------------------------ |
| Socket (Managed)      | ~64 байт + Native Handle |
| NetworkStream         | ~64 байт                 |
| Task (State Machine)  | ~72 байт                 |
| SemaphoreSlim waiter  | ~32 байт                 |
| PipeReader/PipeWriter | ~сотни байт              |
| **Буферы**            | ~4KB минимум             |

**Проблема:** 10k RPS = массовые аллокации → GC Gen0 → Latency spikes

---

## Часть 3: План оптимизации

### Шаг 1: Channels вместо SemaphoreSlim

```csharp
public class OptimizedPool
{
    private readonly Channel<ServerConnection> _idleConnections;
    
    public async ValueTask<ServerConnection> AcquireAsync(CancellationToken ct)
    {
        if (_idleConnections.Reader.TryRead(out var conn))
        {
            if (conn.IsHealthy()) return conn;
            conn.Dispose();
        }
        return await CreateNewConnectionAsync(ct);
    }

    public void Release(ServerConnection conn)
    {
        if (!_idleConnections.Writer.TryWrite(conn))
            conn.Dispose();
    }
}
```

### Шаг 2: Transaction Pooling (Главное!)

Алгоритм:
1. Читаем байты из `PipeReader`
2. Ищем `Q` (Query) или `P` (Parse)
3. При транзакции:
   - Запрашиваем Backend из пула
   - Прокидываем данные
   - Ждём `ReadyForQuery` (`Z`) со статусом `I` (Idle)
   - **Возвращаем Backend в пул**
   - Клиент остаётся подключен (без ресурса базы)

### Шаг 3: Zero-Allocation Parsing

```csharp
// Было (аллокация строки!)
public class QueryMessage { public string Query { get; set; } }

// Надо (zero alloc)
public ref struct PgHeaderParser(ReadOnlySpan<byte> data) {
    public char Type => (char)data[0];
    public int Length => BinaryPrimitives.ReadInt32BigEndian(data.Slice(1));
}
```

### Шаг 4: Kestrel ConnectionHandler

Использовать ASP.NET Core ConnectionHandler:
- Готовый оптимизированный `PipeReader`/`PipeWriter`
- Нативный IOCP
- TLS "из коробки"

---

## Часть 4: Прогноз производительности

| Метрика          | C PgBouncer | .NET Optimized                    | Причина победы .NET                  |
| ---------------- | ----------- | --------------------------------- | ------------------------------------ |
| **Throughput**   | ~40-50k RPS | ~45-60k RPS                       | Нативный IOCP > эмуляция select      |
| **Memory (10k)** | ~250 MB     | ~600 MB                           | Runtime overhead (плата за удобство) |
| **Latency p99**  | Стабильная  | Редкие GC всплески                | Gen0 GC быстр                        |
| **DevOps**       | DLL hell    | `dotnet publish --self-contained` | Единый EXE                           |

---

## Приоритеты реализации

1. ✅ **SASL/SCRAM-SHA-256** — блокер
2. 🔄 **Channels вместо SemaphoreSlim** — быстрая победа
3. 🔲 **Transaction Pooling** — максимальный эффект
4. 🔲 **System.IO.Pipelines** — zero-copy
5. 🔲 **Kestrel ConnectionHandler** — готовый IOCP
