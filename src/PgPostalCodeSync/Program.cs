using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Extensions.Hosting;
using System.Text;
using PgPostalCodeSync.Configuration;
using PgPostalCodeSync.Services;

namespace PgPostalCodeSync;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        
        var host = CreateHostBuilder(args).Build();
        
        try
        {
            var logger = host.Services.GetRequiredService<ILogger<Program>>();
            logger.LogInformation("PgPostalCodeSync アプリケーションを開始します");
            
            var syncService = host.Services.GetRequiredService<PostalCodeSyncService>();
            await syncService.ExecuteAsync(args);
            
            logger.LogInformation("PgPostalCodeSync アプリケーションが正常終了しました");
            return 0;
        }
        catch (Exception ex)
        {
            var logger = host.Services.GetService<ILogger<Program>>();
            logger?.LogError(ex, "PgPostalCodeSync アプリケーションでエラーが発生しました");
            return 1;
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }
    
    private static IHostBuilder CreateHostBuilder(string[] args)
    {
        return Host.CreateDefaultBuilder(args)
            .UseSerilog((context, services, configuration) => configuration
                .ReadFrom.Configuration(context.Configuration)
                .ReadFrom.Services(services)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .WriteTo.File("logs/postal-sync-.txt", rollingInterval: RollingInterval.Day))
            .ConfigureServices((context, services) =>
            {
                services.Configure<AppSettings>(context.Configuration.GetSection("PostalCodeSync"));
                
                services.AddHttpClient();
                services.AddSingleton<DownloadService>();
                services.AddSingleton<DatabaseService>();
                services.AddSingleton<PostalCodeSyncService>();
            });
    }
}
