using PgBouncer.Core.Configuration;
using PgBouncer.Core.Pooling;

var builder = WebApplication.CreateBuilder(args);

// Конфигурация
var config = builder.Configuration.Get<PgBouncerConfig>() ?? new PgBouncerConfig();
builder.Services.AddSingleton(config);

// Пул-менеджер (shared с ProxyServer)
builder.Services.AddSingleton<PoolManager>();

// Controllers
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// CORS для веб-дашборда
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

// Swagger всегда доступен
app.UseSwagger();
app.UseSwaggerUI();

app.UseCors();

// Статические файлы (веб-дашборд)
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapControllers();

Console.WriteLine($"╔══════════════════════════════════════════════════════════╗");
Console.WriteLine($"║         PgBouncer.NET Dashboard API                      ║");
Console.WriteLine($"╚══════════════════════════════════════════════════════════╝");
Console.WriteLine();
Console.WriteLine($"  Dashboard:  http://localhost:{config.DashboardPort}/");
Console.WriteLine($"  Swagger:    http://localhost:{config.DashboardPort}/swagger");
Console.WriteLine($"  API:        http://localhost:{config.DashboardPort}/api/stats");
Console.WriteLine();

app.Run($"http://0.0.0.0:{config.DashboardPort}");

