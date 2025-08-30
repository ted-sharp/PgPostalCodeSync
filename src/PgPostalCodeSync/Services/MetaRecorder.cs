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

            // 既存のテーブルを削除（存在する場合）
            var dropTablesSql = @"
                DROP TABLE IF EXISTS ext.ingestion_files CASCADE;
                DROP TABLE IF EXISTS ext.ingestion_runs CASCADE;
                DROP TABLE IF EXISTS ext.postal_codes_landed CASCADE;
                DROP TABLE IF EXISTS ext.postal_codes CASCADE;";

            using var dropCommand = new NpgsqlCommand(dropTablesSql, connection);
            await dropCommand.ExecuteNonQueryAsync();

            // ext.ingestion_runsテーブルの作成
            var createRunsTableSql = @"
                CREATE TABLE ext.ingestion_runs (
                    id SERIAL PRIMARY KEY,
                    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    status VARCHAR(20) NOT NULL CHECK (status IN ('InProgress', 'Succeeded', 'Failed')),
                    mode VARCHAR(20) NOT NULL CHECK (mode IN ('Full', 'Diff')),
                    yymm VARCHAR(4),
                    version_date DATE NOT NULL,
                    total_records INTEGER,
                    added_records INTEGER,
                    updated_records INTEGER,
                    deleted_records INTEGER,
                    errors JSONB,
                    summary TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );";

            using var runsCommand = new NpgsqlCommand(createRunsTableSql, connection);
            await runsCommand.ExecuteNonQueryAsync();

            // ext.ingestion_filesテーブルの作成
            var createFilesTableSql = @"
                CREATE TABLE ext.ingestion_files (
                    id SERIAL PRIMARY KEY,
                    ingestion_run_id INTEGER NOT NULL REFERENCES ext.ingestion_runs(id) ON DELETE CASCADE,
                    file_name VARCHAR(255) NOT NULL,
                    file_type VARCHAR(20) NOT NULL CHECK (file_type IN ('Full', 'Add', 'Del')),
                    file_path TEXT NOT NULL,
                    file_size BIGINT NOT NULL,
                    sha256_hash VARCHAR(64),
                    downloaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    processed_at TIMESTAMP WITH TIME ZONE,
                    errors TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );";

            using var filesCommand = new NpgsqlCommand(createFilesTableSql, connection);
            await filesCommand.ExecuteNonQueryAsync();

            // ext.postal_codes_landedテーブルの作成
            var createLandedTableSql = @"
                CREATE TABLE ext.postal_codes_landed (
                    id SERIAL PRIMARY KEY,
                    postal_code VARCHAR(7) NOT NULL,
                    local_government_code VARCHAR(6) NOT NULL,
                    old_zip_code5 VARCHAR(5) NOT NULL,
                    prefecture_katakana VARCHAR(50) NOT NULL,
                    city_katakana VARCHAR(50) NOT NULL,
                    town_katakana VARCHAR(50) NOT NULL,
                    prefecture VARCHAR(50) NOT NULL,
                    city VARCHAR(50) NOT NULL,
                    town VARCHAR(50) NOT NULL,
                    is_multi_zip BOOLEAN NOT NULL,
                    is_koaza BOOLEAN NOT NULL,
                    is_chome BOOLEAN NOT NULL,
                    is_multi_town BOOLEAN NOT NULL,
                    update_status INTEGER NOT NULL,
                    update_reason INTEGER NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );";

            using var landedCommand = new NpgsqlCommand(createLandedTableSql, connection);
            await landedCommand.ExecuteNonQueryAsync();

            // ext.postal_codesテーブルの作成
            var createPostalCodesTableSql = @"
                CREATE TABLE ext.postal_codes (
                    id SERIAL PRIMARY KEY,
                    postal_code VARCHAR(7) NOT NULL,
                    local_government_code VARCHAR(6) NOT NULL,
                    old_zip_code5 VARCHAR(5) NOT NULL,
                    prefecture_katakana VARCHAR(50) NOT NULL,
                    city_katakana VARCHAR(50) NOT NULL,
                    town_katakana VARCHAR(50) NOT NULL,
                    prefecture VARCHAR(50) NOT NULL,
                    city VARCHAR(50) NOT NULL,
                    town VARCHAR(50) NOT NULL,
                    is_multi_zip BOOLEAN NOT NULL,
                    is_koaza BOOLEAN NOT NULL,
                    is_chome BOOLEAN NOT NULL,
                    is_multi_town BOOLEAN NOT NULL,
                    update_status INTEGER NOT NULL,
                    update_reason INTEGER NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );";

            using var postalCodesCommand = new NpgsqlCommand(createPostalCodesTableSql, connection);
            await postalCodesCommand.ExecuteNonQueryAsync();

            // インデックスの作成
            var createIndexesSql = @"
                CREATE INDEX IF NOT EXISTS ix_postal_codes_comp
                    ON ext.postal_codes (postal_code, prefecture, city, town);

                CREATE INDEX IF NOT EXISTS ix_postal_codes_prefecture
                    ON ext.postal_codes (prefecture);

                CREATE INDEX IF NOT EXISTS ix_postal_codes_city
                    ON ext.postal_codes (city);

                CREATE INDEX IF NOT EXISTS ix_postal_codes_zip
                    ON ext.postal_codes (postal_code);

                CREATE INDEX IF NOT EXISTS ix_ingestion_runs_status
                    ON ext.ingestion_runs (status);

                CREATE INDEX IF NOT EXISTS ix_ingestion_runs_mode
                    ON ext.ingestion_runs (mode);";

            using var indexesCommand = new NpgsqlCommand(createIndexesSql, connection);
            await indexesCommand.ExecuteNonQueryAsync();

            _logger.LogInformation("メタ情報テーブルの存在確認・作成が完了しました");

            return true;
        }
        catch (Exception ex)
        {
            var error = $"メタ情報テーブルの確認・作成中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return false;
        }
    }
}
