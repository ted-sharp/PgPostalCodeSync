using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using PgPostalCodeSync.Models;
using System.Text.Json;

namespace PgPostalCodeSync.Services;

public class MetaRecorder
{
    private readonly ILogger<MetaRecorder> _logger;
    private readonly PostalCodeSyncOptions _options;

    public MetaRecorder(ILogger<MetaRecorder> logger, IOptions<PostalCodeSyncOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    public async Task<IngestionRun> StartIngestionRunAsync(string connectionString, string mode, string? yymm, DateTime versionDate)
    {
        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var insertSql = @"
                INSERT INTO ext.ingestion_runs (
                    started_at, status, mode, yymm, version_date
                ) VALUES (
                    @startedAt, @status, @mode, @yymm, @versionDate
                ) RETURNING id;";

            using var command = new NpgsqlCommand(insertSql, connection);
            command.Parameters.AddWithValue("@startedAt", DateTime.UtcNow);
            command.Parameters.AddWithValue("@status", "InProgress");
            command.Parameters.AddWithValue("@mode", mode);
            command.Parameters.AddWithValue("@yymm", yymm ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@versionDate", versionDate);

            var id = await command.ExecuteScalarAsync();

            var ingestionRun = new IngestionRun
            {
                Id = Convert.ToInt32(id),
                StartedAt = DateTime.UtcNow,
                Status = "InProgress",
                Mode = mode,
                YyMm = yymm,
                VersionDate = versionDate
            };

            _logger.LogInformation("実行履歴を開始しました: ID={Id}, Mode={Mode}, YYMM={YyMm}", id, mode, yymm);

            return ingestionRun;
        }
        catch (Exception ex)
        {
            var error = $"実行履歴の開始中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            throw;
        }
    }

    public async Task CompleteIngestionRunAsync(string connectionString, int runId, string status,
        int? totalRecords = null, int? addedRecords = null, int? updatedRecords = null,
        int? deletedRecords = null, string? errors = null, string? summary = null)
    {
        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var updateSql = @"
                UPDATE ext.ingestion_runs SET
                    completed_at = @completedAt,
                    status = @status,
                    total_records = @totalRecords,
                    added_records = @addedRecords,
                    updated_records = @updatedRecords,
                    deleted_records = @deletedRecords,
                    errors = @errors,
                    summary = @summary
                WHERE id = @runId;";

            using var command = new NpgsqlCommand(updateSql, connection);
            command.Parameters.AddWithValue("@completedAt", DateTime.UtcNow);
            command.Parameters.AddWithValue("@status", status);
            command.Parameters.AddWithValue("@totalRecords", totalRecords ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@addedRecords", addedRecords ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@updatedRecords", updatedRecords ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@deletedRecords", deletedRecords ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@errors", errors != null ? (object)JsonSerializer.Deserialize<JsonElement>(errors) : DBNull.Value);
            command.Parameters.AddWithValue("@summary", summary ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@runId", runId);

            await command.ExecuteNonQueryAsync();

            _logger.LogInformation("実行履歴を完了しました: ID={Id}, Status={Status}", runId, status);
        }
        catch (Exception ex)
        {
            var error = $"実行履歴の完了中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            throw;
        }
    }

    public async Task<int> RecordIngestionFileAsync(string connectionString, int runId, string fileName,
        string fileType, string filePath, long fileSize, string? sha256Hash, string? errors = null)
    {
        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var insertSql = @"
                INSERT INTO ext.ingestion_files (
                    ingestion_run_id, file_name, file_type, file_path, file_size,
                    sha256_hash, downloaded_at, processed_at, errors
                ) VALUES (
                    @runId, @fileName, @fileType, @filePath, @fileSize,
                    @sha256Hash, @downloadedAt, @processedAt, @errors
                ) RETURNING id;";

            using var command = new NpgsqlCommand(insertSql, connection);
            command.Parameters.AddWithValue("@runId", runId);
            command.Parameters.AddWithValue("@fileName", fileName);
            command.Parameters.AddWithValue("@fileType", fileType);
            command.Parameters.AddWithValue("@filePath", filePath);
            command.Parameters.AddWithValue("@fileSize", fileSize);
            command.Parameters.AddWithValue("@sha256Hash", sha256Hash ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@downloadedAt", DateTime.UtcNow);
            command.Parameters.AddWithValue("@processedAt", (object)DBNull.Value);
            command.Parameters.AddWithValue("@errors", errors ?? (object)DBNull.Value);

            var id = await command.ExecuteScalarAsync();

            _logger.LogInformation("ファイルメタ情報を記録しました: ID={Id}, FileName={FileName}, Type={Type}",
                id, fileName, fileType);

            return Convert.ToInt32(id);
        }
        catch (Exception ex)
        {
            var error = $"ファイルメタ情報の記録中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            throw;
        }
    }

    public async Task UpdateFileProcessedAsync(string connectionString, int fileId, DateTime processedAt)
    {
        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var updateSql = @"
                UPDATE ext.ingestion_files SET
                    processed_at = @processedAt
                WHERE id = @fileId;";

            using var command = new NpgsqlCommand(updateSql, connection);
            command.Parameters.AddWithValue("@processedAt", processedAt);
            command.Parameters.AddWithValue("@fileId", fileId);

            await command.ExecuteNonQueryAsync();

            _logger.LogDebug("ファイル処理完了時刻を更新しました: ID={Id}, ProcessedAt={ProcessedAt}", fileId, processedAt);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "ファイル処理完了時刻の更新中にエラーが発生しました: ID={Id}", fileId);
        }
    }

    public async Task RecordErrorAsync(string connectionString, int runId, string errorMessage, string? details = null)
    {
        try
        {
            var errors = new
            {
                timestamp = DateTime.UtcNow,
                message = errorMessage,
                details = details
            };

            var errorsJson = JsonSerializer.Serialize(errors);

            await CompleteIngestionRunAsync(connectionString, runId, "Failed", errors: errorsJson);

            _logger.LogError("エラーを記録しました: RunId={RunId}, Error={Error}", runId, errorMessage);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "エラーの記録中にエラーが発生しました: RunId={RunId}", runId);
        }
    }

    public Task<string> GenerateSummaryAsync(string mode, string? yymm,
        int? totalRecords = null, int? addedRecords = null, int? updatedRecords = null, int? deletedRecords = null)
    {
        var summary = new
        {
            mode = mode,
            yymm = yymm,
            timestamp = DateTime.UtcNow,
            records = new
            {
                total = totalRecords,
                added = addedRecords,
                updated = updatedRecords,
                deleted = deletedRecords
            }
        };

        return Task.FromResult(JsonSerializer.Serialize(summary, new JsonSerializerOptions { WriteIndented = true }));
    }

    public async Task<bool> EnsureTablesExistAsync(string connectionString)
    {
        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            // テーブルの存在確認のみ（作成・削除は行わない）
            var checkTablesSql = @"
                SELECT
                    EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ext' AND table_name = 'ingestion_runs') as runs_exists,
                    EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ext' AND table_name = 'ingestion_files') as files_exists,
                    EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ext' AND table_name = 'postal_codes_landed') as landed_exists,
                    EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ext' AND table_name = 'postal_codes') as postal_exists;";

            using var checkCommand = new NpgsqlCommand(checkTablesSql, connection);
            using var reader = await checkCommand.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                var runsExists = reader.GetBoolean(0);
                var filesExists = reader.GetBoolean(1);
                var landedExists = reader.GetBoolean(2);
                var postalExists = reader.GetBoolean(3);

                if (!runsExists || !filesExists || !landedExists || !postalExists)
                {
                    _logger.LogError("必要なテーブルが存在しません。以下のSQLスクリプトを実行してください: create_tables.sql");
                    _logger.LogError("存在しないテーブル: ingestion_runs={RunsExists}, ingestion_files={FilesExists}, postal_codes_landed={LandedExists}, postal_codes={PostalExists}",
                        runsExists, filesExists, landedExists, postalExists);
                    return false;
                }
            }

            _logger.LogInformation("メタ情報テーブルの存在確認が完了しました");
            return true;
        }
        catch (Exception ex)
        {
            var error = $"メタ情報テーブルの確認中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return false;
        }
    }
}
