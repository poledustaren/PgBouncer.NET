using System.Security.Cryptography;
using System.Text;
using System.Buffers.Binary;

namespace PgBouncer.Core.Protocol;

/// <summary>
/// Обработка аутентификации PostgreSQL
/// Поддерживает: CleartextPassword, MD5Password, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-256-PLUS
/// </summary>
public static class PostgresAuth
{
    /// <summary>
    /// Генерация MD5 хеша пароля для PostgreSQL
    /// </summary>
    public static string GenerateMd5Password(string username, string password, byte[] salt)
    {
        // Шаг 1: md5(password + username)
        var inner = MD5.HashData(Encoding.UTF8.GetBytes(password + username));
        var innerHex = Convert.ToHexString(inner).ToLowerInvariant();

        // Шаг 2: md5(hex(step1) + salt)
        var combined = new byte[innerHex.Length + salt.Length];
        Encoding.UTF8.GetBytes(innerHex, 0, innerHex.Length, combined, 0);
        Array.Copy(salt, 0, combined, innerHex.Length, salt.Length);

        var outer = MD5.HashData(combined);
        var outerHex = Convert.ToHexString(outer).ToLowerInvariant();

        return "md5" + outerHex;
    }

    /// <summary>
    /// Создать PasswordMessage для отправки на сервер
    /// </summary>
    public static byte[] CreatePasswordMessage(string password)
    {
        var passwordBytes = Encoding.UTF8.GetBytes(password);
        var length = 4 + passwordBytes.Length + 1;

        var message = new byte[1 + length];
        message[0] = (byte)'p';

        BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(1), length);
        Array.Copy(passwordBytes, 0, message, 5, passwordBytes.Length);
        // null terminator already 0

        return message;
    }

    /// <summary>
    /// Создать SASLInitialResponse для SCRAM-SHA-256
    /// </summary>
    public static byte[] CreateSASLInitialResponse(string mechanism, string clientFirstMessage)
    {
        var mechanismBytes = Encoding.UTF8.GetBytes(mechanism);
        var responseBytes = Encoding.UTF8.GetBytes(clientFirstMessage);

        // Формат: механизм + 0 + data_length (big-endian) + data
        var length = 4 + mechanismBytes.Length + 1 + 4 + responseBytes.Length;

        var message = new byte[1 + length];
        message[0] = (byte)'p';

        var pos = 1;
        BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(pos), length);
        pos += 4;

        Array.Copy(mechanismBytes, 0, message, pos, mechanismBytes.Length);
        pos += mechanismBytes.Length;
        message[pos++] = 0;

        BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(pos), responseBytes.Length);
        pos += 4;
        Array.Copy(responseBytes, 0, message, pos, responseBytes.Length);

        return message;
    }

    /// <summary>
    /// Создать SASLResponse сообщение
    /// </summary>
    public static byte[] CreateSASLResponse(string data)
    {
        var dataBytes = Encoding.UTF8.GetBytes(data);
        var length = 4 + dataBytes.Length;

        var message = new byte[1 + length];
        message[0] = (byte)'p';

        BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(1), length);
        Array.Copy(dataBytes, 0, message, 5, dataBytes.Length);

        return message;
    }
}

/// <summary>
/// Типы аутентификации PostgreSQL
/// </summary>
public enum AuthenticationType : int
{
    Ok = 0,
    KerberosV5 = 2,
    CleartextPassword = 3,
    MD5Password = 5,
    SCMCredential = 6,
    GSS = 7,
    GSSContinue = 8,
    SSPI = 9,
    SASL = 10,
    SASLContinue = 11,
    SASLFinal = 12
}

/// <summary>
/// SCRAM-SHA-256 клиент для PostgreSQL аутентификации
/// Поддерживает SCRAM-SHA-256 (RFC 7677) и SCRAM-SHA-256-PLUS (RFC 8446, PostgreSQL 17+)
/// </summary>
public sealed class ScramSha256Auth
{
    private readonly string _password;
    private readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();
    private string _clientNonce = null!;
    private byte[] _saltedPassword = null!;
    private string? _authMessage; // Сохраняем как строку для удобства
    private bool _usePlus = false; // Использовать SCRAM-SHA-256-PLUS (PostgreSQL 17+)

    public ScramSha256Auth(string password)
    {
        _password = password ?? throw new ArgumentNullException(nameof(password));
    }

    /// <summary>
    /// Установить режим PLUS для PostgreSQL 17+
    /// </summary>
    public void SetPlusMode() => _usePlus = true;

    /// <summary>
    /// Создать клиентское первое сообщение (client-first-message)
    /// Формат: n,,n=user,r=nonce
    /// </summary>
    public string CreateClientFirstMessage(string username)
    {
        _clientNonce = GenerateNonce();
        return $"n,,n={username},r={_clientNonce}";
    }

    /// <summary>
    /// Обработать серверное первое сообщение и создать клиентское последнее сообщение
    /// Server-first: r=nonce+salt, i=iterations
    /// </summary>
    public string ProcessServerFirstAndCreateClientFinal(string serverFirstMessage, string username)
    {
        Console.WriteLine($"DEBUG: Processing server first message: {serverFirstMessage}");
        var parts = serverFirstMessage.Split(',');
        string? combinedNonce = null;
        string? saltBase64 = null;
        int iterations = 4096;

        foreach (var part in parts)
        {
            if (part.StartsWith("r="))
                combinedNonce = part[2..];
            else if (part.StartsWith("s="))
                saltBase64 = part[2..];
            else if (part.StartsWith("i="))
                int.TryParse(part[2..], out iterations);
        }

        if (combinedNonce == null || saltBase64 == null)
            throw new InvalidOperationException("Invalid SCRAM server-first message");

        if (!combinedNonce.StartsWith(_clientNonce))
            throw new InvalidOperationException("SCRAM nonce mismatch");

        var salt = Convert.FromBase64String(saltBase64);
        var normalizedPassword = _password.Normalize(NormalizationForm.FormKC); // SASLprep-ish

        _saltedPassword = Hi(normalizedPassword, salt, iterations);

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        var clientKey = Hmac(_saltedPassword, Encoding.UTF8.GetBytes("Client Key"));
        Console.WriteLine($"DEBUG: ClientKey: {Convert.ToBase64String(clientKey)}");

        // StoredKey = H(ClientKey)
        var storedKey = SHA256.HashData(clientKey);
        Console.WriteLine($"DEBUG: StoredKey: {Convert.ToBase64String(storedKey)}");

        // AuthMessage
        var cbFlag = _usePlus ? "p=padding" : "c=biws";
        var clientFinalMessageWithoutProof = $"{cbFlag},r={combinedNonce}";
        var clientFirstMessage = $"n={username},r={_clientNonce}";
        _authMessage = $"{clientFirstMessage},{serverFirstMessage},{clientFinalMessageWithoutProof}";
        
        Console.WriteLine($"DEBUG: AuthMessage: {_authMessage}");

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        var clientSignature = Hmac(storedKey, Encoding.UTF8.GetBytes(_authMessage));
        Console.WriteLine($"DEBUG: ClientSignature: {Convert.ToBase64String(clientSignature)}");

        // ClientProof = ClientKey XOR ClientSignature
        var clientProof = new byte[clientKey.Length];
        for (int i = 0; i < clientKey.Length; i++)
        {
            clientProof[i] = (byte)(clientKey[i] ^ clientSignature[i]);
        }

        var proofBase64 = Convert.ToBase64String(clientProof);
        Console.WriteLine($"DEBUG: ClientProof: {proofBase64}");
        
        return $"{clientFinalMessageWithoutProof},p={proofBase64}";
    }

    /// <summary>
    /// Проверить серверную подпись (ServerSignature)
    /// </summary>
    public bool VerifyServerSignature(string serverSignatureBase64)
    {
        // ServerKey = HMAC(SaltedPassword, "Server Key")
        var serverKey = Hmac(_saltedPassword, Encoding.UTF8.GetBytes("Server Key"));

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        var expectedServerSignature = Hmac(serverKey, Encoding.UTF8.GetBytes(_authMessage!));
        var actualServerSignature = Convert.FromBase64String(serverSignatureBase64);

        return CryptographicOperations.FixedTimeEquals(expectedServerSignature, actualServerSignature);
    }

    /// <summary>
    /// HMAC-SHA256
    /// </summary>
    private static byte[] Hmac(byte[] key, byte[] data)
    {
        using var hmac = new HMACSHA256(key);
        return hmac.ComputeHash(data);
    }

    /// <summary>
    /// PBKDF2-HMAC-SHA256 (Hi function из RFC 7677)
    /// </summary>
    private static byte[] Hi(string password, byte[] salt, int iterations)
    {
        var passwordBytes = Encoding.UTF8.GetBytes(password);
        using var hmac = new HMACSHA256(passwordBytes);

        // U1 = HMAC(password, salt || 0x01)
        var saltPlusOne = new byte[salt.Length + 4];
        Array.Copy(salt, saltPlusOne, salt.Length);
        saltPlusOne[salt.Length] = 0;
        saltPlusOne[salt.Length + 1] = 0;
        saltPlusOne[salt.Length + 2] = 0;
        saltPlusOne[salt.Length + 3] = 1;

        var u = hmac.ComputeHash(saltPlusOne);
        var result = (byte[])u.Clone();

        // U2...Ui
        for (int i = 1; i < iterations; i++)
        {
            u = hmac.ComputeHash(u);
            for (int j = 0; j < 32; j++)
            {
                result[j] ^= u[j];
            }
        }

        Console.WriteLine($"DEBUG: SaltedPassword (Hi): {Convert.ToBase64String(result)}");
        return result;
    }

    /// <summary>
    /// Сгенерировать случайный nonce для SCRAM (base64 без паддингов)
    /// </summary>
    private string GenerateNonce()
    {
        var bytes = new byte[18];
        _rng.GetBytes(bytes);
        return Convert.ToBase64String(bytes)
            .TrimEnd('=');
    }
}
