namespace PgBouncer.Core.Configuration;

/// <summary>
/// Конфигурация PgBouncer
/// </summary>
public class PgBouncerConfig
{
    /// <summary>
    /// Порт для прослушивания клиентских соединений
    /// </summary>
    public int ListenPort { get; set; } = 6432;

    /// <summary>
    /// Порт для Dashboard API
    /// </summary>
    public int DashboardPort { get; set; } = 5080;

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
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Порт PostgreSQL сервера
    /// </summary>
    public int Port { get; set; } = 5432;

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
    public int MaxSize { get; set; } = 100;

    /// <summary>
    /// Максимальное общее количество серверных соединений
    /// </summary>
    public int MaxTotalConnections { get; set; } = 2000;

    /// <summary>
    /// Режим пулинга
    /// </summary>
    public PoolingMode Mode { get; set; } = PoolingMode.Transaction;

    /// <summary>
    /// Таймаут idle соединения (секунды)
    /// </summary>
    public int IdleTimeout { get; set; } = 300;

    /// <summary>
    /// Таймаут подключения к бэкенду (секунды)
    /// </summary>
    public int ConnectionTimeout { get; set; } = 30;
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
