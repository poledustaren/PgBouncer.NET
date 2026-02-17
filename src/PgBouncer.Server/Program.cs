using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Serilog;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Core.Authentication;
using PgBouncer.Server;
using System.Text.Json;

var runId = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .WriteTo.File($"logs/log_{runId}.txt", rollingInterval: RollingInterval.Infinite)
    .CreateLogger();

Log.Information($"=== ЗАПУСК PgBouncer.NET. RunID: {runId} ===");

try
{
    Log.Information("Запуск PgBouncer.NET...");

    ThreadPool.SetMinThreads(1000, 1000);

    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog();
    builder.Host.UseWindowsService();

    var config = builder.Configuration.Get<PgBouncerConfig>() ?? new PgBouncerConfig();
    builder.Services.AddSingleton(config);

    builder.Services.AddSingleton<UserRegistry>();

    builder.Services.AddSingleton(sp =>
    {
        var cfg = sp.GetRequiredService<PgBouncerConfig>();
        var logger = sp.GetRequiredService<ILogger<PoolManager>>();
        return new PoolManager(cfg, logger);
    });

    builder.Services.AddSingleton<ProxyServer>();
    builder.Services.AddHostedService<ProxyServerHostedService>();
    builder.Services.AddHostedService<MetricsMonitorService>();

    builder.Services.AddControllers();
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen();

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

    app.UseSwagger();
    app.UseSwaggerUI();

    app.UseCors();

    app.UseDefaultFiles();
    app.UseStaticFiles();

    app.MapControllers();

    app.MapGet("/api/sessions", (ProxyServer proxyServer) => Results.Ok(new
    {
        ActiveSessions = proxyServer.ActiveSessions,
        ActiveBackendConnections = proxyServer.ActiveBackendConnections,
        MaxBackendConnections = proxyServer.MaxBackendConnections,
        WaitingClients = proxyServer.WaitingClients,
        TotalConnections = proxyServer.TotalConnections,
        AvgWaitTimeMs = proxyServer.AvgWaitTimeMs,
        MaxWaitTimeMs = proxyServer.MaxWaitTimeMs,
        TimeoutCount = proxyServer.TimeoutCount,
        ConnectionTimeout = proxyServer.Config.Pool.ConnectionTimeout,
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

public class MetricsMonitorService : BackgroundService
{
    private readonly ILogger<MetricsMonitorService> _logger;

    public MetricsMonitorService(ILogger<MetricsMonitorService> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("MONITOR STARTED");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                ThreadPool.GetAvailableThreads(out var workerThreads, out var completionPortThreads);
                ThreadPool.GetMaxThreads(out var maxWorkerThreads, out var maxCompletionPortThreads);
                ThreadPool.GetMinThreads(out var minWorkerThreads, out var minCompletionPortThreads);

                var process = System.Diagnostics.Process.GetCurrentProcess();

                _logger.LogInformation(
                    "[MONITOR] Threads: W={Worker}/{MinWorker}/{MaxWorker}, IO={IO}/{MinIO}/{MaxIO}. RAM: {RAM}MB. Handles: {Handles}",
                    maxWorkerThreads - workerThreads, minWorkerThreads, maxWorkerThreads,
                    maxCompletionPortThreads - completionPortThreads, minCompletionPortThreads, maxCompletionPortThreads,
                    process.WorkingSet64 / 1024 / 1024,
                    process.HandleCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Monitor failed");
            }

            await Task.Delay(1000, stoppingToken);
        }
    }
}
