using System.Security.Cryptography;
using System.Text;

namespace PgBouncer.Core.Protocol;

/// <summary>
/// Обработка аутентификации PostgreSQL
/// </summary>
public static class PostgresAuth
{
    /// <summary>
    /// Генерация MD5 хеша пароля для PostgreSQL
    /// </summary>
    /// <param name="username">Имя пользователя</param>
    /// <param name="password">Пароль</param>
    /// <param name="salt">4-байтовая соль от сервера</param>
    /// <returns>MD5 хеш в формате "md5" + hex</returns>
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
        var length = 4 + passwordBytes.Length + 1; // length + password + null terminator

        var message = new byte[1 + length];
        message[0] = (byte)'p'; // PasswordMessage type

        // Length (big-endian)
        message[1] = (byte)(length >> 24);
        message[2] = (byte)(length >> 16);
        message[3] = (byte)(length >> 8);
        message[4] = (byte)length;

        // Password
        Array.Copy(passwordBytes, 0, message, 5, passwordBytes.Length);
        // Null terminator уже 0 по умолчанию

        return message;
    }

    /// <summary>
    /// Создать StartupMessage для подключения к PostgreSQL
    /// </summary>
    public static byte[] CreateStartupMessage(string database, string username)
    {
        var parameters = new Dictionary<string, string>
        {
            { "user", username },
            { "database", database },
            { "client_encoding", "UTF8" },
            { "application_name", "PgBouncer.NET" }
        };

        using var ms = new MemoryStream();

        // Placeholder для длины
        ms.Write(new byte[4], 0, 4);

        // Protocol version 3.0 (196608 = 3 << 16)
        var version = new byte[] { 0, 3, 0, 0 };
        ms.Write(version, 0, 4);

        // Параметры (key=value с null terminators)
        foreach (var (key, value) in parameters)
        {
            var keyBytes = Encoding.UTF8.GetBytes(key);
            ms.Write(keyBytes, 0, keyBytes.Length);
            ms.WriteByte(0);

            var valueBytes = Encoding.UTF8.GetBytes(value);
            ms.Write(valueBytes, 0, valueBytes.Length);
            ms.WriteByte(0);
        }

        // Финальный null terminator
        ms.WriteByte(0);

        var result = ms.ToArray();

        // Записываем длину в начало (big-endian)
        var length = result.Length;
        result[0] = (byte)(length >> 24);
        result[1] = (byte)(length >> 16);
        result[2] = (byte)(length >> 8);
        result[3] = (byte)length;

        return result;
    }
}

/// <summary>
/// Типы аутентификации PostgreSQL
/// </summary>
public enum AuthenticationType
{
    Ok = 0,              // Auth successful
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
/// Сообщение аутентификации от сервера
/// </summary>
public class AuthenticationMessage : PostgresMessage
{
    public override MessageType Type => MessageType.Authentication;

    public AuthenticationType AuthType { get; set; }

    /// <summary>
    /// Соль для MD5 (4 байта)
    /// </summary>
    public byte[]? Md5Salt { get; set; }

    /// <summary>
    /// SASL mechanisms (для SCRAM-SHA-256)
    /// </summary>
    public List<string>? SaslMechanisms { get; set; }

    public override void WriteTo(Stream stream)
    {
        throw new NotSupportedException("Authentication messages are sent by server");
    }

    public static AuthenticationMessage Parse(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 4)
            throw new InvalidOperationException("Invalid authentication message");

        var authType = (AuthenticationType)System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(payload);

        var message = new AuthenticationMessage { AuthType = authType };

        switch (authType)
        {
            case AuthenticationType.MD5Password:
                if (payload.Length >= 8)
                {
                    message.Md5Salt = payload.Slice(4, 4).ToArray();
                }
                break;

            case AuthenticationType.SASL:
                // Парсим список SASL механизмов
                message.SaslMechanisms = new List<string>();
                var offset = 4;
                while (offset < payload.Length)
                {
                    var end = payload.Slice(offset).IndexOf((byte)0);
                    if (end <= 0) break;

                    var mechanism = Encoding.UTF8.GetString(payload.Slice(offset, end));
                    message.SaslMechanisms.Add(mechanism);
                    offset += end + 1;
                }
                break;
        }

        return message;
    }
}
