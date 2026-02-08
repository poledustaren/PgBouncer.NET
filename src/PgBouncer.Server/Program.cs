using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Serilog;
using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;
using PgBouncer.Server;

// Настройка Serilog
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("logs/pgbouncer-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

try
{
    Log.Information("Запуск PgBouncer.NET...");

    var host = Host.CreateDefaultBuilder(args)
        .UseSerilog()
        .ConfigureServices((context, services) =>
        {
            // Конфигурация
            var config = context.Configuration.Get<PgBouncerConfig>() 
                ?? new PgBouncerConfig();
            services.AddSingleton(config);

            // Пул-менеджер
            services.AddSingleton<PoolManager>();

            // Прокси-сервер
            services.AddSingleton<ProxyServer>();
            services.AddHostedService<ProxyServerHostedService>();
        })
        .Build();

    await host.RunAsync();
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
