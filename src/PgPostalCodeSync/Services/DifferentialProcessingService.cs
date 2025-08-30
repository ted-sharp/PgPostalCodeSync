using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgPostalCodeSync.Services;

public interface IDifferentialProcessingService
{
    Task<DifferentialResult> ApplyDifferentialChangesAsync(string connectionString, bool hasAddFile, bool hasDelFile, CancellationToken cancellationToken = default);
}

public class DifferentialProcessingService : IDifferentialProcessingService
{
    private readonly ILogger<DifferentialProcessingService> _logger;

    public DifferentialProcessingService(ILogger<DifferentialProcessingService> logger)
    {
        _logger = logger;
    }

    public async Task<DifferentialResult> ApplyDifferentialChangesAsync(string connectionString, bool hasAddFile, bool hasDelFile, CancellationToken cancellationToken = default)
    {
        var result = new DifferentialResult();
        var startTime = DateTime.UtcNow;

        _logger.LogInformation("Starting differential processing - Add: {HasAdd}, Del: {HasDel}", hasAddFile, hasDelFile);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        if (hasAddFile)
        {
            result.AddedCount = await ApplyAddChangesAsync(connection, cancellationToken);
        }

        if (hasDelFile)
        {
            result.DeletedCount = await ApplyDeleteChangesAsync(connection, cancellationToken);
        }

        result.Duration = DateTime.UtcNow - startTime;
        _logger.LogInformation("Differential processing completed - Added: {Added}, Deleted: {Deleted}, Duration: {Duration}ms",
            result.AddedCount, result.DeletedCount, result.Duration.TotalMilliseconds);

        return result;
    }

    private async Task<long> ApplyAddChangesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Applying ADD changes using MERGE");

        const string mergeCommand = """
            MERGE INTO ext.postal_codes AS t
            USING (
                SELECT
                    l.zip_code7 AS postal_code,
                    l.prefecture_katakana,
                    l.city_katakana,
                    l.town_katakana,
                    l.prefecture,
                    l.city,
                    l.town
                FROM ext.postal_codes_landed l
            ) AS s
            ON (
                t.postal_code = s.postal_code
                AND t.prefecture = s.prefecture
                AND t.city = s.city
                AND t.town = s.town
            )
            WHEN MATCHED THEN
                UPDATE SET
                    prefecture_katakana = s.prefecture_katakana,
                    city_katakana = s.city_katakana,
                    town_katakana = s.town_katakana
            WHEN NOT MATCHED THEN
                INSERT (postal_code, prefecture_katakana, city_katakana, town_katakana, prefecture, city, town)
                VALUES (s.postal_code, s.prefecture_katakana, s.city_katakana, s.town_katakana, s.prefecture, s.city, s.town)
            """;

        await using var command = new NpgsqlCommand(mergeCommand, connection);
        command.CommandTimeout = 300;
        
        var affectedRows = await command.ExecuteNonQueryAsync(cancellationToken);
        
        _logger.LogInformation("ADD changes applied: {AffectedRows} rows", affectedRows);
        return affectedRows;
    }

    private async Task<long> ApplyDeleteChangesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Applying DELETE changes");

        const string deleteCommand = """
            DELETE FROM ext.postal_codes t
            USING (
                SELECT
                    l.zip_code7 AS postal_code,
                    l.prefecture,
                    l.city,
                    l.town
                FROM ext.postal_codes_landed l
            ) AS d
            WHERE t.postal_code = d.postal_code
                AND t.prefecture = d.prefecture
                AND t.city = d.city
                AND t.town = d.town
            """;

        await using var command = new NpgsqlCommand(deleteCommand, connection);
        command.CommandTimeout = 300;
        
        var deletedRows = await command.ExecuteNonQueryAsync(cancellationToken);
        
        _logger.LogInformation("DELETE changes applied: {DeletedRows} rows", deletedRows);
        return deletedRows;
    }
}

public class DifferentialResult
{
    public long AddedCount { get; set; }
    public long DeletedCount { get; set; }
    public TimeSpan Duration { get; set; }
    public bool Success { get; set; } = true;
    public string? Error { get; set; }
}