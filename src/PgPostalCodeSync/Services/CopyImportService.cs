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

        await using var writer = await connection.BeginBinaryImportAsync(copyCommand, cancellationToken);
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

                var record = ParseCsvLine(line);
                if (record == null)
                {
                    _logger.LogWarning("Failed to parse CSV line, skipping: {Line}", line);
                    continue;
                }

                await writer.StartRowAsync(cancellationToken);
                
                await writer.WriteAsync(record.LocalGovernmentCode, cancellationToken);
                await writer.WriteAsync(record.OldZipCode5, cancellationToken);
                await writer.WriteAsync(record.ZipCode7, cancellationToken);
                await writer.WriteAsync(record.PrefectureKatakana, cancellationToken);
                await writer.WriteAsync(record.CityKatakana, cancellationToken);
                await writer.WriteAsync(record.TownKatakana, cancellationToken);
                await writer.WriteAsync(record.Prefecture, cancellationToken);
                await writer.WriteAsync(record.City, cancellationToken);
                await writer.WriteAsync(record.Town, cancellationToken);
                await writer.WriteAsync(record.IsMultiZip, cancellationToken);
                await writer.WriteAsync(record.IsKoaza, cancellationToken);
                await writer.WriteAsync(record.IsChome, cancellationToken);
                await writer.WriteAsync(record.IsMultiTown, cancellationToken);
                await writer.WriteAsync(record.UpdateStatus, cancellationToken);
                await writer.WriteAsync(record.UpdateReason, cancellationToken);
                
                rowCount++;
            }

            await writer.CompleteAsync(cancellationToken);
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

    private static PostalCodeRecord? ParseCsvLine(string line)
    {
        try
        {
            var fields = ParseCsvFields(line);
            
            if (fields.Length < 15)
                return null;

            return new PostalCodeRecord
            {
                LocalGovernmentCode = fields[0],
                OldZipCode5 = fields[1],
                ZipCode7 = fields[2],
                PrefectureKatakana = fields[3],
                CityKatakana = fields[4],
                TownKatakana = fields[5],
                Prefecture = fields[6],
                City = fields[7],
                Town = fields[8],
                IsMultiZip = fields[9] == "1",
                IsKoaza = fields[10] == "1",
                IsChome = fields[11] == "1",
                IsMultiTown = fields[12] == "1",
                UpdateStatus = fields[13],
                UpdateReason = fields[14]
            };
        }
        catch
        {
            return null;
        }
    }

    private static string[] ParseCsvFields(string line)
    {
        var fields = new List<string>();
        var current = new StringBuilder();
        var inQuotes = false;
        var i = 0;

        while (i < line.Length)
        {
            var c = line[i];

            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i += 2;
                    continue;
                }
                inQuotes = !inQuotes;
            }
            else if (c == ',' && !inQuotes)
            {
                fields.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(c);
            }

            i++;
        }

        fields.Add(current.ToString());
        return fields.ToArray();
    }
}