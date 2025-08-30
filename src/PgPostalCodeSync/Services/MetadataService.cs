using System.Text.Json;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public interface IMetadataService
{
    Task<long> CreateIngestionRunAsync(string connectionString, string sourceSystem, DateTime versionDate, string mode, CancellationToken cancellationToken = default);
    Task UpdateIngestionRunAsync(string connectionString, long runId, string status, long landedRows, string? notes = null, object? errors = null, CancellationToken cancellationToken = default);
    Task CreateIngestionFilesAsync(string connectionString, long runId, DownloadResult[] downloadResults, CancellationToken cancellationToken = default);
    Task<IngestionRun?> GetLatestSuccessfulRunAsync(string connectionString, CancellationToken cancellationToken = default);
}

public class MetadataService : IMetadataService
{
    private readonly ILogger<MetadataService> _logger;

    public MetadataService(ILogger<MetadataService> logger)
    {
        _logger = logger;
    }

    public async Task<long> CreateIngestionRunAsync(string connectionString, string sourceSystem, DateTime versionDate, string mode, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating ingestion run - System: {SourceSystem}, Version: {VersionDate}, Mode: {Mode}", 
            sourceSystem, versionDate, mode);

        const string insertCommand = """
            INSERT INTO ext.ingestion_runs (source_system, version_date, mode, started_at, status)
            VALUES (@sourceSystem, @versionDate, @mode, @startedAt, @status)
            RETURNING run_id
            """;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(insertCommand, connection);
        command.Parameters.AddWithValue("@sourceSystem", sourceSystem);
        command.Parameters.AddWithValue("@versionDate", versionDate);
        command.Parameters.AddWithValue("@mode", mode);
        command.Parameters.AddWithValue("@startedAt", DateTime.UtcNow);
        command.Parameters.AddWithValue("@status", "Running");

        var runId = (long)(await command.ExecuteScalarAsync(cancellationToken))!;

        _logger.LogInformation("Created ingestion run with ID: {RunId}", runId);
        return runId;
    }

    public async Task UpdateIngestionRunAsync(string connectionString, long runId, string status, long landedRows, string? notes = null, object? errors = null, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Updating ingestion run {RunId} - Status: {Status}, Rows: {LandedRows}", 
            runId, status, landedRows);

        const string updateCommand = """
            UPDATE ext.ingestion_runs 
            SET finished_at = @finishedAt,
                status = @status,
                landed_rows = @landedRows,
                notes = @notes,
                errors = @errors
            WHERE run_id = @runId
            """;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(updateCommand, connection);
        command.Parameters.AddWithValue("@runId", runId);
        command.Parameters.AddWithValue("@finishedAt", DateTime.UtcNow);
        command.Parameters.AddWithValue("@status", status);
        command.Parameters.AddWithValue("@landedRows", landedRows);
        command.Parameters.AddWithValue("@notes", notes ?? string.Empty);
        var errorsJson = errors != null ? JsonSerializer.Serialize(errors) : "{}";
        command.Parameters.Add(new NpgsqlParameter("@errors", NpgsqlTypes.NpgsqlDbType.Jsonb) { Value = errorsJson });

        await command.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("Updated ingestion run {RunId}", runId);
    }

    public async Task CreateIngestionFilesAsync(string connectionString, long runId, DownloadResult[] downloadResults, CancellationToken cancellationToken = default)
    {
        if (downloadResults.Length == 0)
        {
            _logger.LogInformation("No download results to record for run {RunId}", runId);
            return;
        }

        _logger.LogInformation("Creating ingestion file records for run {RunId} - {FileCount} files", 
            runId, downloadResults.Length);

        const string insertCommand = """
            INSERT INTO ext.ingestion_files (run_id, source_uri, file_name, size_bytes, hash_sha256)
            VALUES (@runId, @sourceUri, @fileName, @sizeBytes, @hashSha256)
            """;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (var result in downloadResults.Where(r => r.Success))
        {
            await using var command = new NpgsqlCommand(insertCommand, connection);
            command.Parameters.AddWithValue("@runId", runId);
            command.Parameters.AddWithValue("@sourceUri", result.SourceUri);
            command.Parameters.AddWithValue("@fileName", result.FileName);
            command.Parameters.AddWithValue("@sizeBytes", result.SizeBytes);
            command.Parameters.AddWithValue("@hashSha256", result.HashSha256);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        var successfulFiles = downloadResults.Count(r => r.Success);
        _logger.LogInformation("Created {SuccessfulFiles} ingestion file records for run {RunId}", 
            successfulFiles, runId);
    }

    public async Task<IngestionRun?> GetLatestSuccessfulRunAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        const string selectCommand = """
            SELECT run_id, source_system, version_date, mode, started_at, finished_at, status, landed_rows, notes
            FROM ext.ingestion_runs
            WHERE status = 'Succeeded'
            ORDER BY finished_at DESC
            LIMIT 1
            """;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(selectCommand, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new IngestionRun
        {
            RunId = reader.GetInt64(0),
            SourceSystem = reader.GetString(1),
            VersionDate = reader.GetDateTime(2),
            Mode = reader.GetString(3),
            StartedAt = reader.GetDateTime(4),
            FinishedAt = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            Status = reader.GetString(6),
            LandedRows = reader.GetInt64(7),
            Notes = reader.GetString(8)
        };
    }
}

public class IngestionRun
{
    public long RunId { get; set; }
    public string SourceSystem { get; set; } = string.Empty;
    public DateTime VersionDate { get; set; }
    public string Mode { get; set; } = string.Empty;
    public DateTime StartedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
    public string Status { get; set; } = string.Empty;
    public long LandedRows { get; set; }
    public string Notes { get; set; } = string.Empty;
}