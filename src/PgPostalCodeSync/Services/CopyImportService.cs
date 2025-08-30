using System.Text;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public interface ICopyImportService
{
    Task<long> ImportCsvToLandedTableAsync(string[] csvFilePaths, string connectionString, CancellationToken cancellationToken = default);
    Task TruncateLandedTableAsync(string connectionString, CancellationToken cancellationToken = default);
}

public class CopyImportService : ICopyImportService
{
    private readonly ILogger<CopyImportService> _logger;

    public CopyImportService(ILogger<CopyImportService> logger)
    {
        _logger = logger;
    }

    public async Task<long> ImportCsvToLandedTableAsync(string[] csvFilePaths, string connectionString, CancellationToken cancellationToken = default)
    {
        long totalRows = 0;
        var startTime = DateTime.UtcNow;

        _logger.LogInformation("Starting COPY import for {FileCount} CSV files", csvFilePaths.Length);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (var csvFilePath in csvFilePaths)
        {
            if (!File.Exists(csvFilePath))
            {
                _logger.LogWarning("CSV file not found, skipping: {CsvFile}", csvFilePath);
                continue;
            }

            var fileRows = await ImportSingleCsvFileAsync(connection, csvFilePath, cancellationToken);
            totalRows += fileRows;
            
            _logger.LogInformation("Imported {Rows} rows from {CsvFile}", fileRows, Path.GetFileName(csvFilePath));
        }

        var duration = DateTime.UtcNow - startTime;
        _logger.LogInformation("COPY import completed: {TotalRows} rows imported in {Duration}ms", 
            totalRows, duration.TotalMilliseconds);

        return totalRows;
    }

    private async Task<long> ImportSingleCsvFileAsync(NpgsqlConnection connection, string csvFilePath, CancellationToken cancellationToken)
    {
        const string copyCommand = """
            COPY ext.postal_codes_landed (
                local_government_code, old_zip_code5, zip_code7, 
                prefecture_katakana, city_katakana, town_katakana,
                prefecture, city, town,
                is_multi_zip, is_koaza, is_chome, is_multi_town,
                update_status, update_reason
            ) FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER ',',
                QUOTE '"',
                ESCAPE '"',
                ENCODING 'UTF8'
            )
            """;

        await using var writer = await connection.BeginTextImportAsync(copyCommand, cancellationToken);
        long rowCount = 0;

        try
        {
            using var reader = new StreamReader(csvFilePath, Encoding.UTF8);
            string? line;
            
            while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                cancellationToken.ThrowIfCancellationRequested();

                await writer.WriteLineAsync(line.AsMemory(), cancellationToken);
                rowCount++;
            }

            await writer.DisposeAsync();
            return rowCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during COPY import from file: {CsvFile}", csvFilePath);
            throw;
        }
    }

    public async Task TruncateLandedTableAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Truncating landed table");

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand("TRUNCATE TABLE ext.postal_codes_landed", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("Landed table truncated successfully");
    }

}