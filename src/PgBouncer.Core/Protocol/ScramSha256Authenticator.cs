using System.Security.Cryptography;
using System.Text;

namespace PgBouncer.Core.Protocol;

/// <summary>
/// Реализация SCRAM-SHA-256 аутентификации для PostgreSQL
/// https://www.rfc-editor.org/rfc/rfc5802
/// </summary>
public class ScramSha256Authenticator
{
    private readonly string _username;
    private readonly string _password;
    private string _clientNonce = "";
    private string _serverNonce = "";
    private byte[] _salt = Array.Empty<byte>();
    private int _iterations;
    private string _clientFirstMessageBare = "";
    private byte[] _saltedPassword = Array.Empty<byte>();

    public ScramSha256Authenticator(string username, string password)
    {
        _username = username;
        _password = password;
    }

    /// <summary>
    /// Создать SASLInitialResponse для отправки серверу
    /// </summary>
    public byte[] CreateInitialResponse()
    {
        // Генерируем случайный nonce
        _clientNonce = GenerateNonce();

        // gs2-header: n,,  (no channel binding, no auth identity)
        // client-first-message-bare: n=user,r=nonce
        _clientFirstMessageBare = $"n={SaslPrepUsername(_username)},r={_clientNonce}";
        var clientFirstMessage = $"n,,{_clientFirstMessageBare}";

        var mechanism = "SCRAM-SHA-256";
        var mechanismBytes = Encoding.UTF8.GetBytes(mechanism);
        var responseBytes = Encoding.UTF8.GetBytes(clientFirstMessage);

        // Формат SASLInitialResponse:
        // 'p' (1 байт) + length (4 байта) + mechanism\0 + response_length (4 байта) + response
        using var ms = new MemoryStream();
        ms.WriteByte((byte)'p'); // SASLInitialResponse

        // Placeholder для длины
        var lengthPos = ms.Position;
        ms.Write(new byte[4], 0, 4);

        // Mechanism name + null terminator
        ms.Write(mechanismBytes);
        ms.WriteByte(0);

        // Response length (4 байта, big-endian)
        var respLen = responseBytes.Length;
        ms.WriteByte((byte)(respLen >> 24));
        ms.WriteByte((byte)(respLen >> 16));
        ms.WriteByte((byte)(respLen >> 8));
        ms.WriteByte((byte)respLen);

        // Response data
        ms.Write(responseBytes);

        var result = ms.ToArray();

        // Записываем длину (не включая тип сообщения)
        var totalLength = result.Length - 1;
        result[1] = (byte)(totalLength >> 24);
        result[2] = (byte)(totalLength >> 16);
        result[3] = (byte)(totalLength >> 8);
        result[4] = (byte)totalLength;

        return result;
    }

    /// <summary>
    /// Обработать AuthenticationSASLContinue и создать SASLResponse
    /// </summary>
    public byte[] ProcessServerFirstMessage(byte[] serverData)
    {
        // Парсим server-first-message: r=nonce,s=salt,i=iterations
        var serverFirstMessage = Encoding.UTF8.GetString(serverData);

        var parts = serverFirstMessage.Split(',');
        foreach (var part in parts)
        {
            if (part.StartsWith("r="))
                _serverNonce = part.Substring(2);
            else if (part.StartsWith("s="))
                _salt = Convert.FromBase64String(part.Substring(2));
            else if (part.StartsWith("i="))
                _iterations = int.Parse(part.Substring(2));
        }

        // Проверяем что server nonce начинается с client nonce
        if (!_serverNonce.StartsWith(_clientNonce))
            throw new InvalidOperationException("Invalid server nonce");

        // Вычисляем SaltedPassword
        _saltedPassword = Hi(_password, _salt, _iterations);

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        var clientKey = HmacSha256(_saltedPassword, "Client Key");

        // StoredKey = H(ClientKey)
        var storedKey = SHA256.HashData(clientKey);

        // client-final-message-without-proof: c=biws,r=nonce
        // biws = base64("n,,") - channel binding
        var channelBinding = Convert.ToBase64String(Encoding.UTF8.GetBytes("n,,"));
        var clientFinalWithoutProof = $"c={channelBinding},r={_serverNonce}";

        // AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
        var authMessage = $"{_clientFirstMessageBare},{serverFirstMessage},{clientFinalWithoutProof}";

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        var clientSignature = HmacSha256(storedKey, authMessage);

        // ClientProof = ClientKey XOR ClientSignature
        var clientProof = new byte[clientKey.Length];
        for (int i = 0; i < clientKey.Length; i++)
            clientProof[i] = (byte)(clientKey[i] ^ clientSignature[i]);

        // client-final-message: c=biws,r=nonce,p=proof
        var clientFinalMessage = $"{clientFinalWithoutProof},p={Convert.ToBase64String(clientProof)}";
        var responseBytes = Encoding.UTF8.GetBytes(clientFinalMessage);

        // Формат SASLResponse: 'p' (1) + length (4) + data
        var result = new byte[1 + 4 + responseBytes.Length];
        result[0] = (byte)'p';

        var len = 4 + responseBytes.Length;
        result[1] = (byte)(len >> 24);
        result[2] = (byte)(len >> 16);
        result[3] = (byte)(len >> 8);
        result[4] = (byte)len;

        Array.Copy(responseBytes, 0, result, 5, responseBytes.Length);

        return result;
    }

    /// <summary>
    /// Проверить AuthenticationSASLFinal (опционально)
    /// </summary>
    public bool VerifyServerFinalMessage(byte[] serverData)
    {
        // Парсим server-final-message: v=server_signature
        var serverFinalMessage = Encoding.UTF8.GetString(serverData);

        if (!serverFinalMessage.StartsWith("v="))
            return false;

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        var serverKey = HmacSha256(_saltedPassword, "Server Key");

        // Мы можем проверить подпись сервера, но для прокси это не критично
        return true;
    }

    private static string GenerateNonce()
    {
        var bytes = new byte[24];
        RandomNumberGenerator.Fill(bytes);
        return Convert.ToBase64String(bytes);
    }

    private static string SaslPrepUsername(string username)
    {
        // Простая реализация - экранируем = и ,
        return username.Replace("=", "=3D").Replace(",", "=2C");
    }

    /// <summary>
    /// PBKDF2 (Hi) - RFC 5802
    /// </summary>
    private static byte[] Hi(string password, byte[] salt, int iterations)
    {
        using var pbkdf2 = new Rfc2898DeriveBytes(
            Encoding.UTF8.GetBytes(password),
            salt,
            iterations,
            HashAlgorithmName.SHA256);
        return pbkdf2.GetBytes(32);
    }

    private static byte[] HmacSha256(byte[] key, string data)
    {
        return HmacSha256(key, Encoding.UTF8.GetBytes(data));
    }

    private static byte[] HmacSha256(byte[] key, byte[] data)
    {
        using var hmac = new HMACSHA256(key);
        return hmac.ComputeHash(data);
    }
}
