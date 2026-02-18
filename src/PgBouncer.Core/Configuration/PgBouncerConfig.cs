namespace PgBouncer.Core.Configuration;

public class PgBouncerConfig
{
    public int ListenPort { get; set; } = 6432;
    public int DashboardPort { get; set; } = 5081;
    public BackendConfig Backend { get; set; } = new();
    public PoolConfig Pool { get; set; } = new();
    public AuthConfig Auth { get; set; } = new();
    public SslConfig Ssl { get; set; } = new();
}

public class BackendConfig
{
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 5437;
    public string AdminUser { get; set; } = "postgres";
    public string AdminPassword { get; set; } = string.Empty;
}

public class PoolConfig
{
    public int DefaultSize { get; set; } = 20;
    public int MinSize { get; set; } = 2;
    public int MaxSize { get; set; } = 1000;
    public int MaxTotalConnections { get; set; } = 8000;
    public PoolingMode Mode { get; set; } = PoolingMode.Transaction;
    public int IdleTimeout { get; set; } = 600;
    public int ConnectionTimeout { get; set; } = 60;
    public string ServerResetQuery { get; set; } = "DISCARD ALL";
    public bool UsePipelinesArchitecture { get; set; } = false;
}

public class AuthConfig
{
    public string Type { get; set; } = "md5";
    public string AuthFile { get; set; } = "userlist.txt";
}

public class SslConfig
{
    public bool Enabled { get; set; } = false;
    public string CertificatePath { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public bool RequireSsl { get; set; } = false;
}

public enum PoolingMode
{
    Session,
    Transaction,
    Statement
}
