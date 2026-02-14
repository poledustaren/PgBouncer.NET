# Context: System.IO.Pipelines Migration Plan

## Current Architecture
The existing PgBouncer.NET uses:
- `NetworkStream` wrapped in `Stream` for I/O
- `byte[]` buffers with ArrayPool.Rent for pooling
- Simple read/write loops with `ReadAsync`/`WriteAsync`
- `TransactionPoolingSession` and `EnhancedTransactionPoolingSessionV2` for transaction pooling

## Target Architecture
The proposed design uses System.IO.Pipelines:
- `PipeReader` for reading from network streams
- `PipeWriter` for writing to network streams
- Zero-allocation message parsing with `SequenceReader<byte>`
- Better backpressure handling
- Automatic buffer management

## Key Components to Implement

### 1. BackendConnection (Pipelines-based)
Replaces `ServerConnection` with Pipeline-based reader:
- Reads PostgreSQL messages via PipeReader
- Parses protocol without allocations
- Handles idle state when no client attached
- Clean message boundary handling

### 2. ClientSession (Pipelines-based)
Replaces current session handling:
- Handles StartupMessage with PipelineReader
- Proxies frontend messages to backend
- Uses backend's PipeWriter for writing

### 3. IBackendHandler Interface
Callback interface for backend message handling:
- `HandleBackendMessageAsync(ReadOnlySequence<byte> message, byte messageType)`
- `OnBackendDisconnected(Exception)`

## Integration Points
- `PoolManager` - remains mostly unchanged
- `ConnectionPool` - needs to work with new connection type
- `ProxyServer` - needs to create new session type
- `PgMessageScanner` - can be reused/adapted
