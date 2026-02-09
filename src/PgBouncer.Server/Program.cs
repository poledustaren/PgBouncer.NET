using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Serilog;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Server;
using System.Text.Json;

// Настройка Serilog
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("logs/pgbouncer-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

try
{
    Log.Information("Запуск PgBouncer.NET...");

    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog();

    // Конфигурация
    var config = builder.Configuration.Get<PgBouncerConfig>() ?? new PgBouncerConfig();
    builder.Services.AddSingleton(config);

    // Пул-менеджер (один на всех!) - с логгером!
    builder.Services.AddSingleton(sp =>
    {
        var cfg = sp.GetRequiredService<PgBouncerConfig>();
        var logger = sp.GetRequiredService<ILogger<PoolManager>>();
        return new PoolManager(cfg, logger);
    });

    // Прокси-сервер
    builder.Services.AddSingleton<ProxyServer>();
    builder.Services.AddHostedService<ProxyServerHostedService>();

    // Controllers и Swagger
    builder.Services.AddControllers();
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen();

    // CORS для дашборда
    builder.Services.AddCors(options =>
    {
        options.AddDefaultPolicy(policy =>
        {
            policy.AllowAnyOrigin()
                  .AllowAnyMethod()
                  .AllowAnyHeader();
        });
    });

    var app = builder.Build();

    // Swagger
    app.UseSwagger();
    app.UseSwaggerUI();

    app.UseCors();

    // Статические файлы дашборда
    app.UseDefaultFiles();
    app.UseStaticFiles();

    // API Controllers
    app.MapControllers();

    // API endpoint для статистики сессий (ProxyServer)
    app.MapGet("/api/sessions", (ProxyServer proxyServer) => Results.Ok(new
    {
        // Основные метрики
        ActiveSessions = proxyServer.ActiveSessions,
        ActiveBackendConnections = proxyServer.ActiveBackendConnections,
        MaxBackendConnections = proxyServer.MaxBackendConnections,
        WaitingClients = proxyServer.WaitingClients,
        TotalConnections = proxyServer.TotalConnections,

        // Метрики времени ожидания
        AvgWaitTimeMs = proxyServer.AvgWaitTimeMs,
        MaxWaitTimeMs = proxyServer.MaxWaitTimeMs,
        TimeoutCount = proxyServer.TimeoutCount,
        ConnectionTimeout = proxyServer.Config.Pool.ConnectionTimeout,

        // Сессии с детализацией
        Sessions = proxyServer.Sessions.Values.Select(s => new
        {
            s.Id,
            s.RemoteEndPoint,
            s.Database,
            s.Username,
            State = s.State.ToString(),
            s.WaitTimeMs,
            s.StartedAt,
            DurationMs = (long)s.Duration.TotalMilliseconds
        })
    }));

    // Баннер
    Console.WriteLine();
    Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
    Console.WriteLine("║                   PgBouncer.NET                          ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
    Console.WriteLine();
    Console.WriteLine($"  Прокси:     localhost:{config.ListenPort}");
    Console.WriteLine($"  Dashboard:  http://localhost:{config.DashboardPort}/");
    Console.WriteLine($"  API:        http://localhost:{config.DashboardPort}/api/stats");
    Console.WriteLine($"  Backend:    {config.Backend.Host}:{config.Backend.Port}");
    Console.WriteLine();

    await app.RunAsync($"http://0.0.0.0:{config.DashboardPort}");
}
catch (Exception ex)
{
    Log.Fatal(ex, "Приложение упало с ошибкой");
}
finally
{
    Log.CloseAndFlush();
}

/// <summary>
/// Hosted service для запуска ProxyServer
/// </summary>
class ProxyServerHostedService : IHostedService
{
    private readonly ProxyServer _proxyServer;

    public ProxyServerHostedService(ProxyServer proxyServer)
    {
        _proxyServer = proxyServer;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _proxyServer.StartAsync(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _proxyServer.StopAsync();
    }
}
