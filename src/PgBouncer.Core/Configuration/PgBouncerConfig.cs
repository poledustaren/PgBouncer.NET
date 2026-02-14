namespace PgBouncer.Core.Configuration;

/// <summary>
/// Конфигурация PgBouncer
/// </summary>
public class PgBouncerConfig
{
    /// <summary>
    /// Порт для прослушивания клиентских соединений
    /// </summary>
    public int ListenPort { get; set; } = 6442;

    /// <summary>
    /// Порт для Dashboard API
    /// </summary>
    public int DashboardPort { get; set; } = 5083;

    /// <summary>
    /// Настройки бэкенда PostgreSQL
    /// </summary>
    public BackendConfig Backend { get; set; } = new();

    /// <summary>
    /// Настройки пулов соединений
    /// </summary>
    public PoolConfig Pool { get; set; } = new();

    /// <summary>
    /// Настройки аутентификации
    /// </summary>
    public AuthConfig Auth { get; set; } = new();
}

/// <summary>
/// Конфигурация бэкенда PostgreSQL
/// </summary>
public class BackendConfig
{
    /// <summary>
    /// Хост PostgreSQL сервера
    /// </summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>
    /// Порт PostgreSQL сервера
    /// </summary>
    public int Port { get; set; } = 5437;

    /// <summary>
    /// Админский пользователь для служебных операций
    /// </summary>
    public string AdminUser { get; set; } = "postgres";

    /// <summary>
    /// Пароль админа
    /// </summary>
    public string AdminPassword { get; set; } = string.Empty;
}

/// <summary>
/// Конфигурация пулов
/// </summary>
public class PoolConfig
{
    /// <summary>
    /// Размер пула по умолчанию для каждой БД/пользователя
    /// </summary>
    public int DefaultSize { get; set; } = 20;

    /// <summary>
    /// Минимальный размер пула
    /// </summary>
    public int MinSize { get; set; } = 2;

    /// <summary>
    /// Максимальный размер пула для одной БД/пользователя
    /// </summary>
    public int MaxSize { get; set; } = 1000;

    /// <summary>
    /// Максимальное общее количество серверных соединений
    /// </summary>
    public int MaxTotalConnections { get; set; } = 8000;

    /// <summary>
    /// Режим пулинга
    /// </summary>
    public PoolingMode Mode { get; set; } = PoolingMode.Transaction;

    /// <summary>
    /// Таймаут idle соединения (секунды)
    /// </summary>
    public int IdleTimeout { get; set; } = 600;

    /// <summary>
    /// Таймаут подключения к бэкенду (секунды)
    /// </summary>
    public int ConnectionTimeout { get; set; } = 60;

    /// <summary>
    /// Query to execute on backend before returning it to pool.
    /// Used to clean up server-side state (prepared statements, portals, temp tables).
    /// Default: "DISCARD ALL" which resets session state.
    /// Set to empty string to disable.
    /// </summary>
    public string ServerResetQuery { get; set; } = "DISCARD ALL";

    /// <summary>
    /// Use System.IO.Pipelines architecture for client sessions.
    /// When true, uses PipelinesClientSession (zero-allocation I/O).
    /// When false, uses legacy ClientSession (Stream-based I/O).
    /// Default: false (legacy Stream-based).
    /// </summary>
    public bool UsePipelinesArchitecture { get; set; } = false;
}

/// <summary>
/// Конфигурация аутентификации
/// </summary>
public class AuthConfig
{
    /// <summary>
    /// Тип аутентификации (passthrough, md5, trust)
    /// </summary>
    public string Type { get; set; } = "passthrough";
}

/// <summary>
/// Режимы пулинга
/// </summary>
public enum PoolingMode
{
    /// <summary>
    /// Соединение привязано на всю сессию клиента
    /// </summary>
    Session,

    /// <summary>
    /// Соединение возвращается после каждой транзакции
    /// </summary>
    Transaction,

    /// <summary>
    /// Соединение возвращается после каждого statement
    /// </summary>
    Statement
}
