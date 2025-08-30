using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using PgPostalCodeSync.Configuration;
using PgPostalCodeSync.Services;

namespace PgPostalCodeSync;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var configuration = BuildConfiguration();

        Log.Logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger();

        try
        {
            Log.Information("Starting PgPostalCodeSync application");

            var options = CliOptions.Parse(args);
            
            var host = CreateHost(configuration);
            using var scope = host.Services.CreateScope();
            
            var syncService = scope.ServiceProvider.GetRequiredService<IPostalCodeSyncService>();
            
            var success = await syncService.ExecuteAsync(options);
            
            Log.Information("PgPostalCodeSync execution completed - Success: {Success}", success);
            return success ? 0 : 1;
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Application failed with fatal error");
            return 1;
        }
        finally
        {
            await Log.CloseAndFlushAsync();
        }
    }

    private static IConfiguration BuildConfiguration()
    {
        return new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
            .Build();
    }

    private static IHost CreateHost(IConfiguration configuration)
    {
        return Host.CreateDefaultBuilder()
            .UseSerilog()
            .ConfigureServices((context, services) =>
            {
                var postalConfig = configuration.GetSection(PostalCodeSyncConfig.SectionName).Get<PostalCodeSyncConfig>()
                    ?? throw new InvalidOperationException($"Missing configuration section: {PostalCodeSyncConfig.SectionName}");
                
                var connectionString = configuration.GetConnectionString("PostalDb")
                    ?? throw new InvalidOperationException("Missing connection string: PostalDb");

                services.AddSingleton(postalConfig);
                services.AddSingleton(postalConfig.Download);
                services.AddSingleton(connectionString);

                services.AddHttpClient<IDownloadService, DownloadService>();
                services.AddSingleton<IZipExtractorService, ZipExtractorService>();
                services.AddSingleton<ICopyImportService, CopyImportService>();
                services.AddSingleton<IDifferentialProcessingService, DifferentialProcessingService>();
                services.AddSingleton<IFullSwitchService, FullSwitchService>();
                services.AddSingleton<IMetadataService, MetadataService>();
                services.AddSingleton<IPostalCodeSyncService, PostalCodeSyncService>();

                services.AddLogging();
            })
            .Build();
    }
}
