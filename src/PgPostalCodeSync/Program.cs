using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using PgPostalCodeSync.Models;
using PgPostalCodeSync.Services;

namespace PgPostalCodeSync;

class Program
{
    static async Task<int> Main(string[] args)
    {
        try
        {
            // CLIオプションの解析
            var cliOptions = CliOptions.Parse(args);

            // 設定の読み込み
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddCommandLine(args)
                .Build();

            // Serilogの設定
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .WriteTo.File("logs/postal-code-sync-.log", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            // ホストの構築
            var host = Host.CreateDefaultBuilder(args)
                .UseSerilog()
                .ConfigureServices((context, services) =>
                {
                    // 設定の登録
                    services.Configure<PostalCodeSyncOptions>(
                        configuration.GetSection(PostalCodeSyncOptions.SectionName));

                    // サービスの登録
                    services.AddScoped<Downloader>();
                    services.AddScoped<CopyImporter>();
                    services.AddScoped<Differ>();
                    services.AddScoped<FullSwitch>();
                    services.AddScoped<MetaRecorder>();
                    services.AddScoped<PostalCodeSyncService>();
                })
                .Build();

            // サービスの実行
            using var scope = host.Services.CreateScope();
            var postalCodeSyncService = scope.ServiceProvider.GetRequiredService<PostalCodeSyncService>();

            var connectionString = configuration.GetConnectionString("PostalDb");
            if (string.IsNullOrEmpty(connectionString))
            {
                Log.Error("データベース接続文字列が設定されていません");
                return 1;
            }

            Log.Information("Postal Code Sync for PostgreSQL を開始します");
            Log.Information("CLIオプション: Full={Full}, YYMM={YyMm}, WorkDir={WorkDir}",
                cliOptions.Full, cliOptions.YyMm, cliOptions.WorkDir);

            var success = await postalCodeSyncService.ExecuteAsync(cliOptions, connectionString);

            if (success)
            {
                Log.Information("郵便番号同期処理が正常に完了しました");
                return 0;
            }
            else
            {
                Log.Error("郵便番号同期処理が失敗しました");
                return 1;
            }
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "アプリケーションで予期しないエラーが発生しました");
            return 1;
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}
