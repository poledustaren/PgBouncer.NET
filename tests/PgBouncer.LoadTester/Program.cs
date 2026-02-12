using CommandLine;

namespace PgBouncer.LoadTester;

/// <summary>
/// Опции командной строки
/// </summary>
public class Options
{
    [Option('c', "connections", Required = false, Default = 100, HelpText = "Количество соединений на экземпляр")]
    public int Connections { get; set; }

    [Option('t', "total", Required = false, Default = 1000, HelpText = "Общее количество соединений для нагрузочного теста")]
    public int TotalConnections { get; set; }

    [Option('m', "max-concurrent", Required = false, Default = 100, HelpText = "Максимальное количество одновременных соединений")]
    public int MaxConcurrent { get; set; }

    [Option('q', "queries", Required = false, Default = 10, HelpText = "Количество запросов на соединение")]
    public int QueriesPerConnection { get; set; }

    [Option("stress-test", Required = false, Default = false, HelpText = "Запустить стресс-тест 1000+ соединений")]
    public bool StressTest { get; set; }

    [Option("dynamic-stress", Required = false, Default = false, HelpText = "Запустить динамический стресс-тест с увеличением нагрузки")]
    public bool DynamicStress { get; set; }

    [Option('i', "instances", Required = false, Default = 1, HelpText = "Количество экземпляров тестера")]
    public int Instances { get; set; }

    [Option('p', "pattern", Required = false, Default = "mixed", HelpText = "Паттерн нагрузки: rapid, idle, mixed, burst, transaction")]
    public string Pattern { get; set; } = "mixed";

    [Option('d', "duration", Required = false, Default = 60, HelpText = "Длительность теста (секунды)")]
    public int Duration { get; set; }

    [Option('h', "host", Required = false, Default = "localhost", HelpText = "Хост PgBouncer")]
    public string Host { get; set; } = "localhost";

    [Option("port", Required = false, Default = 6432, HelpText = "Порт PgBouncer")]
    public int Port { get; set; }

    [Option("database", Required = false, Default = "fuel", HelpText = "База данных")]
    public string Database { get; set; } = "fuel";

    [Option("user", Required = false, Default = "postgres", HelpText = "Пользователь")]
    public string User { get; set; } = "postgres";

    [Option("password", Required = false, Default = "password", HelpText = "Пароль")]
    public string Password { get; set; } = "password";
}

class Program
{
    static async Task Main(string[] args)
    {
        await Parser.Default.ParseArguments<Options>(args)
            .WithParsedAsync(async options =>
            {
                Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
                Console.WriteLine("║         PgBouncer.NET Load Tester                        ║");
                Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
                Console.WriteLine();
                Console.WriteLine($"Конфигурация:");
                Console.WriteLine($"  Соединений на экземпляр: {options.Connections}");
                Console.WriteLine($"  Экземпляров: {options.Instances}");
                Console.WriteLine($"  Паттерн: {options.Pattern}");
                Console.WriteLine($"  Длительность: {options.Duration}с");
                Console.WriteLine($"  Цель: {options.Host}:{options.Port}/{options.Database}");
                Console.WriteLine();

                if (options.DynamicStress)
                {
                    // Run dynamic stress test with increasing load
                    var connString = $"Host={options.Host};Port={options.Port};Database={options.Database};Username={options.User};Password={options.Password};Pooling=false;Timeout=30";
                    var tester = new DynamicStressTest(connString);
                    
                    using var cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (sender, e) =>
                    {
                        e.Cancel = true;
                        cts.Cancel();
                        Console.WriteLine("\n\n⚠️  Test interrupted by user");
                    };
                    
                    var report = await tester.RunAsync(cts.Token);
                    report.PrintReport();
                }
                else if (options.StressTest)
                {
                    // Run high-performance stress test
                    var connString = $"Host={options.Host};Port={options.Port};Database={options.Database};Username={options.User};Password={options.Password};Pooling=false;Timeout=30";
                    var tester = new LoadTester(
                        connString,
                        options.TotalConnections,
                        options.MaxConcurrent,
                        options.QueriesPerConnection);
                    
                    using var cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (sender, e) =>
                    {
                        e.Cancel = true;
                        cts.Cancel();
                    };
                    
                    var result = await tester.RunAsync(cts.Token);
                    result.PrintReport();
                }
                else
                {
                    // Run old load test
                    var runner = new LoadTestRunner(options);
                    await runner.RunAsync();
                }
            });
    }
}
