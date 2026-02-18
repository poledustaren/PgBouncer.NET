using System.Security.Cryptography;
using System.Text;

namespace PgBouncer.Core.Authentication;

public class ScramServerAuthenticator
{
    private readonly string _username;
    private readonly string _password; // In a real scenario, this should be the stored key/salt/etc.
                                     // But assuming we have the plain password or can derive, we'll simulate.
                                     // If we have MD5 shadow, we can't do SCRAM.
                                     // If we have plain text password in userlist, we can generate SCRAM secrets on the fly.

    private string _clientNonce = "";
    private string _serverNonce = "";
    private byte[] _salt = Array.Empty<byte>();
    private int _iterations = 4096;
    private string _clientFirstMessageBare = "";
    private string _serverFirstMessage = "";
    private byte[] _saltedPassword = Array.Empty<byte>();
    private byte[] _authMessage = Array.Empty<byte>();

    public ScramServerAuthenticator(string username, string password)
    {
        _username = username;
        _password = password;
    }

    public byte[] ProcessClientFirstMessage(ReadOnlySpan<byte> message, out string error)
    {
        error = "";
        // Message format: n,,n=user,r=nonce
        // Or with channel binding: y,,... or p=...
        // We only support n,, (no binding) for now as typical for PgBouncer

        var msgString = Encoding.UTF8.GetString(message);

        // Split by comma
        // gs2-header = gs2-cbind-flag "," [ authzid ] ","
        // We expect "n,," or "n,a=authzid,"

        // Find the first part (client-first-message-bare) starts after the first two commas usually
        // But let's parse properly.

        // For simplicity, we look for "r=" and "n=" in the bare message.
        // The bare message starts after the GS2 header.

        // Example: n,,n=user,r=fyko...

        var parts = msgString.Split(',');
        if (parts.Length < 3)
        {
            error = "Invalid SCRAM message format";
            return Array.Empty<byte>();
        }

        var gs2BindFlag = parts[0];
        // parts[1] is authzid (optional)

        // Join the rest as bare message (in case nonce has commas? unlikely for base64)
        // client-first-message-bare starts at parts[2]
        var bareStart = msgString.IndexOf(',', msgString.IndexOf(',') + 1) + 1;
        _clientFirstMessageBare = msgString.Substring(bareStart);

        var bareParts = _clientFirstMessageBare.Split(',');
        foreach (var p in bareParts)
        {
            if (p.StartsWith("r="))
            {
                _clientNonce = p.Substring(2);
            }
            // n=user is also there
        }

        if (string.IsNullOrEmpty(_clientNonce))
        {
            error = "Client nonce missing";
            return Array.Empty<byte>();
        }

        // Generate Server Nonce
        var serverNonceBytes = new byte[24];
        RandomNumberGenerator.Fill(serverNonceBytes);
        var serverNoncePart = Convert.ToBase64String(serverNonceBytes);
        _serverNonce = _clientNonce + serverNoncePart;

        // Generate Salt
        _salt = new byte[16];
        RandomNumberGenerator.Fill(_salt);
        var saltString = Convert.ToBase64String(_salt);

        // Construct ServerFirstMessage
        // r=server_nonce,s=salt,i=iterations
        _serverFirstMessage = $"r={_serverNonce},s={saltString},i={_iterations}";

        var responseBytes = Encoding.UTF8.GetBytes(_serverFirstMessage);
        return responseBytes;
    }

    public byte[] ProcessClientFinalMessage(ReadOnlySpan<byte> message, out string error)
    {
        error = "";
        // client-final-message = channel-binding "," nonce "," proof
        // c=biws,r=...,p=...

        var msgString = Encoding.UTF8.GetString(message);

        // Parse parts
        var parts = msgString.Split(',');
        string clientFinalMessageWithoutProof = "";
        string proof = "";

        int pIndex = msgString.IndexOf(",p=");
        if (pIndex == -1)
        {
             error = "Proof missing";
             return Array.Empty<byte>();
        }

        clientFinalMessageWithoutProof = msgString.Substring(0, pIndex);
        proof = msgString.Substring(pIndex + 3); // value of p=

        // Validate nonce
        // The message should contain r=_serverNonce
        if (!clientFinalMessageWithoutProof.Contains($"r={_serverNonce}"))
        {
            error = "Nonce mismatch";
            return Array.Empty<byte>();
        }

        // Verify proof
        // AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
        var authMessageStr = $"{_clientFirstMessageBare},{_serverFirstMessage},{clientFinalMessageWithoutProof}";
        _authMessage = Encoding.UTF8.GetBytes(authMessageStr);

        // Calculate expected proof
        _saltedPassword = Hi(_password, _salt, _iterations);
        var clientKey = HmacSha256(_saltedPassword, "Client Key");
        var storedKey = SHA256.HashData(clientKey);
        var clientSignature = HmacSha256(storedKey, _authMessage);

        var clientProofBytes = Convert.FromBase64String(proof);
        var recoveredClientKey = new byte[clientKey.Length];

        for (int i = 0; i < clientKey.Length; i++)
        {
            recoveredClientKey[i] = (byte)(clientProofBytes[i] ^ clientSignature[i]);
        }

        var hashedRecoveredClientKey = SHA256.HashData(recoveredClientKey);

        if (!hashedRecoveredClientKey.SequenceEqual(storedKey))
        {
            error = "Invalid proof (password mismatch)";
            return Array.Empty<byte>();
        }

        // Generate ServerFinalMessage
        // v=ServerSignature
        var serverKey = HmacSha256(_saltedPassword, "Server Key");
        var serverSignature = HmacSha256(serverKey, _authMessage);
        var serverSignatureBase64 = Convert.ToBase64String(serverSignature);

        return Encoding.UTF8.GetBytes($"v={serverSignatureBase64}");
    }

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
