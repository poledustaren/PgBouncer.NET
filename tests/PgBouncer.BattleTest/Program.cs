using System;
using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Threading;
using System.Threading.Tasks;
using PgBouncer.BattleTest.Config;
using PgBouncer.BattleTest.Executors;
using PgBouncer.BattleTest.Reporters;

namespace PgBouncer.BattleTest;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var rootCommand = new RootCommand("PgBouncer Battle Test - Production readiness testing tool");

        // Options
        var durationOption = new Option<int>(
            aliases: new[] { "--duration", "-d" },
            description: "Maximum test duration in minutes",
            getDefaultValue: () => 10);

        var maxClientsOption = new Option<int>(
            aliases: new[] { "--max-clients", "-c" },
            description: "Maximum number of concurrent clients",
            getDefaultValue: () => 20);

        var hostOption = new Option<string>(
            aliases: new[] { "--host", "-h" },
            description: "PgBouncer host",
            getDefaultValue: () => "localhost");

        var portOption = new Option<int>(
            aliases: new[] { "--port", "-p" },
            description: "PgBouncer port",
            getDefaultValue: () => 6434);

        var usernameOption = new Option<string>(
            aliases: new[] { "--username", "-u" },
            description: "Database username",
            getDefaultValue: () => "postgres");

        var passwordOption = new Option<string>(
            aliases: new[] { "--password", "-P" },
            description: "Database password",
            getDefaultValue: () => "postgres");

        var outputOption = new Option<string>(
            aliases: new[] { "--output", "-o" },
            description: "Output report path",
            getDefaultValue: () => "./battle-test-report.md");

        var opsPerSecOption = new Option<int>(
            aliases: new[] { "--ops-per-sec", "-r" },
            description: "Target operations per second per client",
            getDefaultValue: () => 10);

        var waveDurationOption = new Option<int>(
            aliases: new[] { "--wave-duration", "-w" },
            description: "Wave duration in seconds",
            getDefaultValue: () => 30);

        // Add options to command
        rootCommand.AddOption(durationOption);
        rootCommand.AddOption(maxClientsOption);
        rootCommand.AddOption(hostOption);
        rootCommand.AddOption(portOption);
        rootCommand.AddOption(usernameOption);
        rootCommand.AddOption(passwordOption);
        rootCommand.AddOption(outputOption);
        rootCommand.AddOption(opsPerSecOption);
        rootCommand.AddOption(waveDurationOption);

        // Handler
        rootCommand.Handler = CommandHandler.Create(
            async (int duration, int maxClients, string host, int port, string username, string password, string output, int opsPerSec, int waveDuration) =>
            {
                var config = new TestConfiguration
                {
                    Host = host,
                    Port = port,
                    Username = username,
                    Password = password,
                    MaxClients = maxClients,
                    MaxTotalDuration = TimeSpan.FromMinutes(duration),
                    TargetOperationsPerSecondPerClient = opsPerSec,
                    WaveDurationSeconds = waveDuration,
                    ReportOutputPath = output
                };

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (sender, e) =>
                {
                    e.Cancel = true;
                    Console.WriteLine("\nStopping test...");
                    cts.Cancel();
                };

                try
                {
                    var controller = new WaveController(config);
                    var report = await controller.RunBattleTestAsync(cts.Token);

                    // Generate report
                    var reportGenerator = new FinalReportGenerator();
                    await reportGenerator.GenerateReportAsync(report, config.ReportOutputPath);

                    Console.WriteLine($"\n=== Test Complete ===");
                    Console.WriteLine($"Report saved to: {config.ReportOutputPath}");
                    Console.WriteLine($"Grade: {report.Grade}");
                    Console.WriteLine($"Success Rate: {report.OverallSuccessRate:F2}%");

                    return report.Grade == Models.PerformanceGrade.F ? 1 : 0;
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("\nTest cancelled.");
                    return 1;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\nTest failed with error: {ex.Message}");
                    Console.WriteLine(ex.StackTrace);
                    return 1;
                }
            });

        return await rootCommand.InvokeAsync(args);
    }
}
