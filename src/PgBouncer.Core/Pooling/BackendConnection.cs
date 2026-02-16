using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PgBouncer.Core.Protocol;

namespace PgBouncer.Core.Pooling;

public sealed class BackendConnection : IServerConnection
{
    private readonly Socket _socket;
    private readonly PipeReader _reader;
    private readonly ILogger? _logger;
    private readonly CancellationTokenSource _readLoopCts = new();
    private DateTime _lastActivity;
    private volatile int _isBroken;
    private volatile IBackendHandler? _handler;
    private Task? _readLoopTask;

    // Для SASL аутентификации
    private ScramSha256Auth? _scramAuth;

    public Guid Id { get; }
    public string Database { get; }
    public string Username { get; }
    public PipeWriter Writer { get; }

    Stream IServerConnection.Stream => throw new NotSupportedException("Use Writer for Pipelines I/O");
    public bool IsBroken => _isBroken == 1;
    public bool IsHealthy => !IsBroken; // НЕ проверяем _socket.Connected - он не надежен
    public DateTime LastActivity => _lastActivity;
    public int Generation { get; set; }

    public BackendConnection(Socket socket, string database, string username, ILogger? logger = null)
    {
        _socket = socket ?? throw new ArgumentNullException(nameof(socket));
        Database = database ?? throw new ArgumentNullException(nameof(database));
        Username = username ?? throw new ArgumentNullException(nameof(username));
        _logger = logger;

        Id = Guid.NewGuid();
        _lastActivity = DateTime.UtcNow;

        var stream = new NetworkStream(_socket, ownsSocket: false);
        _reader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
        Writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
    }

    public async Task ConnectAndAuthenticateAsync(string password, CancellationToken cancellationToken = default)
    {
        WriteStartupMessage(Database, Username);
        await Writer.FlushAsync(cancellationToken);

        while (true)
        {
            var result = await _reader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            while (TryParseMessage(ref buffer, out var message, out var msgType))
            {
                char c = (char)msgType;

                if (c == 'R')
                {
                    await HandleAuthAsync(message, password, Username, cancellationToken);
                }
                else if (c == 'E')
                {
                    throw new Exception("PostgreSQL authentication failed");
                }
                else if (c == 'Z')
                {
                    _reader.AdvanceTo(buffer.Start);
                    _ = ReadLoopAsync();
                    return;
                }
                // Пропускаем ParameterStatus ('S'), BackendKeyData ('K'), NotificationResponse ('A')
            }

            _reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
                throw new Exception("Connection closed during startup");
        }
    }

    private void WriteStartupMessage(string database, string user)
    {
        Span<byte> buf = stackalloc byte[512];
        int pos = 4;

        BinaryPrimitives.WriteInt32BigEndian(buf.Slice(pos), 196608); // Protocol 3.0
        pos += 4;

        pos += WriteCString(buf.Slice(pos), "user");
        pos += WriteCString(buf.Slice(pos), user);
        pos += WriteCString(buf.Slice(pos), "database");
        pos += WriteCString(buf.Slice(pos), database);
        buf[pos++] = 0;

        BinaryPrimitives.WriteInt32BigEndian(buf, pos);
        Writer.Write(buf.Slice(0, pos));
    }

    private int WriteCString(Span<byte> buf, string value)
    {
        int len = Encoding.UTF8.GetBytes(value, buf);
        buf[len] = 0;
        return len + 1;
    }

    private async Task HandleAuthAsync(
        ReadOnlySequence<byte> message,
        string password,
        string username,
        CancellationToken ct)
    {
        var data = message.ToArray();
        int authCode = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(5));
        var authType = (AuthenticationType)authCode;

        _logger?.LogDebug("Authentication request: {AuthType}", authType);
        Console.WriteLine($"DEBUG: Authenticating user '{username}' with password length: {password?.Length ?? 0}");

        switch (authType)
        {
            case AuthenticationType.Ok:
                // Аутентификация успешна, продолжаем читать сообщения до ReadyForQuery
                return;

            case AuthenticationType.CleartextPassword:
                var pwdMsg = PostgresAuth.CreatePasswordMessage(password);
                Writer.Write(pwdMsg);
                await Writer.FlushAsync(ct);
                return;

            case AuthenticationType.MD5Password:
                // Извлекаем соль (4 байта начиная с позиции 9)
                var salt = new byte[4];
                Array.Copy(data, 9, salt, 0, 4);
                var md5Password = PostgresAuth.GenerateMd5Password(username, password, salt);
                var md5Msg = PostgresAuth.CreatePasswordMessage(md5Password);
                Writer.Write(md5Msg);
                await Writer.FlushAsync(ct);
                return;

            case AuthenticationType.SASL:
                // Парсим поддерживаемые механизмы
                var mechanisms = ParseSASLMechanisms(data);
                _logger?.LogDebug("SASL mechanisms: {Mechanisms}", string.Join(", ", mechanisms));
                Console.WriteLine($"DEBUG: Server SASL mechanisms: {string.Join(", ", mechanisms)}");

                // PostgreSQL 17+ поддерживает SCRAM-SHA-256-PLUS.
                // ВАЖНО: Мы НЕ используем SSL/TLS для backend-соединения, поэтому мы НЕ МОЖЕМ использовать PLUS.
                // PLUS требует channel binding (tls-server-end-point).
                // Мы должны явно выбрать SCRAM-SHA-256, даже если сервер предлагает PLUS.

                string selectedMechanism;
                if (mechanisms.Contains("SCRAM-SHA-256"))
                {
                    selectedMechanism = "SCRAM-SHA-256";
                }
                else if (mechanisms.Contains("SCRAM-SHA-256-PLUS"))
                {
                    // Сервер предлагает ТОЛЬКО PLUS, а мы без SSL.
                    // Обычно сервер предлагает оба, но если нет - придется пытаться PLUS (скорее всего, не сработает без SSL)
                    selectedMechanism = "SCRAM-SHA-256-PLUS";
                    _logger?.LogWarning("Server offers only SCRAM-SHA-256-PLUS but we don't handle SSL channel binding yet.");
                }
                else
                {
                    throw new NotSupportedException($"Only SCRAM-SHA-256 is supported. Server offers: {string.Join(", ", mechanisms)}");
                }

                Console.WriteLine($"DEBUG: Selected mechanism: {selectedMechanism}");
                _scramAuth = new ScramSha256Auth(password);
                
                // Если мы выбрали PLUS (что маловероятно без SSL), то надо сказать об этом ScramAuth
                if (selectedMechanism == "SCRAM-SHA-256-PLUS")
                {
                    _scramAuth.SetPlusMode();
                }

                var clientFirst = _scramAuth.CreateClientFirstMessage(username);
                Console.WriteLine($"DEBUG: ClientFirst: {clientFirst}");
                var saslInitial = PostgresAuth.CreateSASLInitialResponse(selectedMechanism, clientFirst);
                Writer.Write(saslInitial);
                await Writer.FlushAsync(ct);
                return;

            case AuthenticationType.SASLContinue:
                if (_scramAuth == null)
                    throw new InvalidOperationException("SASLContinue without SASL request");

                // Извлекаем данные из сообщения
                var saslData = Encoding.UTF8.GetString(data, 9, data.Length - 9);
                _logger?.LogDebug("SASLContinue: {Data}", saslData);
                Console.WriteLine($"DEBUG: SASLContinue Data: {saslData}");

                try 
                {
                    var clientFinal = _scramAuth.ProcessServerFirstAndCreateClientFinal(saslData, username);
                    Console.WriteLine($"DEBUG: ClientFinal: {clientFinal}");
                    var saslResponse = PostgresAuth.CreateSASLResponse(clientFinal);
                    Writer.Write(saslResponse);
                    await Writer.FlushAsync(ct);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"DEBUG: SCRAM Error: {ex}");
                    throw;
                }
                return;

            case AuthenticationType.SASLFinal:
                if (_scramAuth == null)
                    throw new InvalidOperationException("SASLFinal without SASL request");

                // Извлекаем ServerSignature
                var finalData = Encoding.UTF8.GetString(data, 9, data.Length - 9);
                _logger?.LogDebug("SASLFinal: {Data}", finalData);

                // Парсим v=server_signature
                var signature = ParseServerSignature(finalData);
                if (!_scramAuth.VerifyServerSignature(signature))
                    throw new InvalidOperationException("Invalid server signature");

                _logger?.LogInformation("SCRAM-SHA-256 authentication successful");
                _scramAuth = null; // Очищаем для GC
                return;

            default:
                throw new NotSupportedException($"Auth type {authType} ({authCode}) not supported");
        }
    }

    private static List<string> ParseSASLMechanisms(byte[] data)
    {
        var mechanisms = new List<string>();
        int pos = 9; // Пропускаем тип (4) + длину (4) + authCode (4), начало данных

        while (pos < data.Length)
        {
            int start = pos;
            while (pos < data.Length && data[pos] != 0)
                pos++;

            if (pos > start)
            {
                mechanisms.Add(Encoding.UTF8.GetString(data, start, pos - start));
            }
            pos++; // Пропускаем null terminator
        }

        return mechanisms;
    }

    private static string ParseServerSignature(string saslFinalData)
    {
        // Формат: r=nonce,p=proof,v=server_signature
        foreach (var part in saslFinalData.Split(','))
        {
            if (part.StartsWith("v="))
                return part[2..];
        }
        throw new InvalidOperationException("No server signature in SASLFinal");
    }

    public void AttachHandler(IBackendHandler handler)
    {
        if (_handler != null)
        {
            throw new InvalidOperationException("Cannot attach handler: backend is already in use by another session. Missing Detach?");
        }
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        Generation++;
        UpdateActivity();
    }

    void IServerConnection.AttachHandler(IBackendPacketHandler handler)
    {
        throw new NotSupportedException("Use AttachHandler(IBackendHandler)");
    }

    public void DetachHandler()
    {
        _handler = null;
        UpdateActivity();
    }

    public void StartReaderLoop()
    {
        if (_readLoopTask != null) return;
        _readLoopTask = Task.Run(() => ReadLoopAsync());
    }

    public void UpdateActivity() => _lastActivity = DateTime.UtcNow;
    public void MarkDirty() { }
    public bool IsIdle(int timeout) => timeout > 0 && (DateTime.UtcNow - _lastActivity).TotalSeconds > timeout;

    private async Task ReadLoopAsync()
    {
        try
        {
            while (true)
            {
                var result = await _reader.ReadAsync(_readLoopCts.Token);
                var buffer = result.Buffer;

                while (TryParseMessage(ref buffer, out var message, out var msgType))
                {
                    var handler = _handler;
                    if (handler != null)
                    {
                        await handler.HandleBackendMessageAsync(message, msgType);
                    }
                    else
                    {
                        ProcessIdleMessage(msgType);
                    }
                }

                _reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown - expected
        }
        catch (Exception ex)
        {
            MarkAsBroken();
            _handler?.OnBackendDisconnected(ex);
        }
        finally
        {
            MarkAsBroken();
        }
    }

    private bool TryParseMessage(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> message, out byte msgType)
    {
        message = default;
        msgType = 0;

        if (buffer.Length < 5) return false;

        var first5 = buffer.Slice(0, 5);
        Span<byte> header = stackalloc byte[5];
        first5.CopyTo(header);

        msgType = header[0];
        int len = BinaryPrimitives.ReadInt32BigEndian(header.Slice(1));

        if (len < 4 || len > 1024 * 1024)
            return false;

        int total = len + 1;

        if (buffer.Length < total) return false;

        message = buffer.Slice(0, total);
        buffer = buffer.Slice(total);
        return true;
    }

    private void ProcessIdleMessage(byte msgType)
    {
        char typeChar = (char)msgType;
        switch (typeChar)
        {
            case 'N': // NoticeResponse
            case 'S': // ParameterStatus
            case 'A': // NotificationResponse
            case 'Z': // ReadyForQuery
            case '1': // ParseComplete
            case '2': // BindComplete
            case 'T': // RowDescription
            case 'D': // DataRow
            case 'C': // CommandComplete
                break;
            case 'E':
                Console.WriteLine($"[Backend {_socket.Handle}] Broken while idle: received ErrorResponse (E)");
                MarkAsBroken();
                break;
            default:
                Console.WriteLine($"[Backend {_socket.Handle}] Protocol desync! Received unexpected packet '{typeChar}' (0x{msgType:X2}) in Idle state.");
                MarkAsBroken();
                break;
        }
    }

    public void MarkAsBroken() => Interlocked.Exchange(ref _isBroken, 1);

    public async ValueTask DisposeAsync()
    {
        _readLoopCts.Cancel();
        if (_readLoopTask != null)
            await Task.WhenAny(_readLoopTask, Task.Delay(1000));

        try { await _reader.CompleteAsync(); } catch { }
        try { await Writer.CompleteAsync(); } catch { }
        try { _socket.Shutdown(SocketShutdown.Both); } catch { }
        try { _socket.Close(); } catch { }
        _readLoopCts.Dispose();
    }
}
