using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

namespace PgBouncer.Core.Authentication;

public class UserRegistry
{
    private readonly ILogger<UserRegistry> _logger;
    private readonly ConcurrentDictionary<string, string> _users = new(StringComparer.Ordinal);

    public UserRegistry(ILogger<UserRegistry> logger)
    {
        _logger = logger;
    }

    public void Load(string filePath)
    {
        if (!File.Exists(filePath))
        {
            _logger.LogWarning("Auth file {FilePath} not found. Authentication may fail unless using trust/passthrough.", filePath);
            return;
        }

        try
        {
            var lines = File.ReadAllLines(filePath);
            int count = 0;

            var regex = new Regex("^\\s*\"([^\"]+)\"\\s+\"([^\"]+)\"", RegexOptions.Compiled);

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line) || line.TrimStart().StartsWith('#')) continue;

                var match = regex.Match(line);
                if (match.Success)
                {
                    var user = match.Groups[1].Value;
                    var pass = match.Groups[2].Value;

                    if (!pass.StartsWith("md5"))
                    {
                        pass = "md5" + CreateMD5(pass + user);
                    }

                    _users[user] = pass;
                    count++;
                }
            }

            _logger.LogInformation("Loaded {Count} users from {FilePath}", count, filePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load auth file {FilePath}", filePath);
        }
    }

    public string? GetShadowPassword(string username)
    {
        if (_users.TryGetValue(username, out var pass))
            return pass;
        return null;
    }

    private static string CreateMD5(string input)
    {
        using var md5 = System.Security.Cryptography.MD5.Create();
        var bytes = Encoding.UTF8.GetBytes(input);
        var hash = md5.ComputeHash(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
