using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public class DatabaseService
{
    private readonly ILogger<DatabaseService> _logger;
    private readonly string _connectionString;

    public DatabaseService(ILogger<DatabaseService> logger, IConfiguration configuration)
    {
        this._logger = logger;
        this._connectionString = configuration.GetConnectionString("PostalDb")
            ?? throw new InvalidOperationException("PostalDb connection string is not configured");
    }

    public async Task<NpgsqlConnection> CreateConnectionAsync()
    {
        this._logger.LogDebug("データベース接続を作成します");

        var connection = new NpgsqlConnection(this._connectionString);
        await connection.OpenAsync();

        this._logger.LogDebug("データベース接続を確立しました");
        return connection;
    }

    public async Task<bool> HasExistingDataAsync()
    {
        this._logger.LogDebug("既存データの存在を確認します");

        try
        {
            using var connection = await this.CreateConnectionAsync();

            var sql = @"
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ext'
                    AND table_name = 'postal_codes'
                )";

            using var command = new NpgsqlCommand(sql, connection);
            var exists = await command.ExecuteScalarAsync() as bool? ?? false;

            if (exists)
            {
                var countSql = "SELECT COUNT(*) FROM ext.postal_codes LIMIT 1";
                using var countCommand = new NpgsqlCommand(countSql, connection);
                var count = await countCommand.ExecuteScalarAsync() as long? ?? 0;
                exists = count > 0;
            }

            this._logger.LogDebug("既存データ確認結果: {HasData}", exists);
            return exists;
        }
        catch (Exception ex)
        {
            this._logger.LogError(ex, "既存データ確認でエラーが発生しました");
            return false;
        }
    }

    public async Task<long> CreateIngestionRunAsync(string runType, string status)
    {
        this._logger.LogInformation("取り込み実行レコードを作成します: Type={RunType}, Status={Status}", runType, status);

        using var connection = await this.CreateConnectionAsync();

        await this.EnsureSchemaExistsAsync(connection);

        var sql = @"
            INSERT INTO ext.ingestion_runs (mode, status, started_at)
            VALUES (@mode, @status, @startedAt)
            RETURNING run_id";

        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("@mode", runType);
        command.Parameters.AddWithValue("@status", status);
        command.Parameters.AddWithValue("@startedAt", DateTime.UtcNow);

        var id = await command.ExecuteScalarAsync() as long? ?? 0;

        this._logger.LogInformation("取り込み実行レコードを作成しました: ID={Id}", id);
        return id;
    }

    public async Task<long> RecordFileMetadataAsync(long runId, string filename, string url, long fileSize, string sha256Hash, string[] extractedFiles)
    {
        this._logger.LogInformation("ファイルメタデータを記録します: RunId={RunId}, File={Filename}", runId, filename);

        using var connection = await this.CreateConnectionAsync();

        var sql = @"
            INSERT INTO ext.ingestion_files (run_id, file_name, source_uri, size_bytes, hash_sha256)
            VALUES (@runId, @fileName, @sourceUri, @sizeBytes, @hashSha256)
            RETURNING file_id";

        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("@runId", runId);
        command.Parameters.AddWithValue("@fileName", filename);
        command.Parameters.AddWithValue("@sourceUri", url);
        command.Parameters.AddWithValue("@sizeBytes", fileSize);
        command.Parameters.AddWithValue("@hashSha256", sha256Hash);

        var fileId = await command.ExecuteScalarAsync() as long? ?? 0;

        this._logger.LogInformation("ファイルメタデータを記録しました: FileId={FileId}", fileId);
        return fileId;
    }

    public async Task CompleteIngestionRunAsync(long runId, string status, long landedRows, string notes = "", object? errors = null)
    {
        this._logger.LogInformation("取り込み実行を完了します: RunId={RunId}, Status={Status}", runId, status);

        using var connection = await this.CreateConnectionAsync();

        // version_date を前月の1日として設定
        var versionDate = DateTime.Now.AddMonths(-1);
        versionDate = new DateTime(versionDate.Year, versionDate.Month, 1);

        var sql = @"
            UPDATE ext.ingestion_runs
            SET finished_at = @finishedAt,
                status = @status,
                landed_rows = @landedRows,
                notes = @notes,
                errors = @errors,
                version_date = @versionDate
            WHERE run_id = @runId";

        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("@runId", runId);
        command.Parameters.AddWithValue("@finishedAt", DateTime.UtcNow);
        command.Parameters.AddWithValue("@status", status);
        command.Parameters.AddWithValue("@landedRows", landedRows);
        command.Parameters.AddWithValue("@notes", notes);
        var errorsJson = errors != null ? System.Text.Json.JsonSerializer.Serialize(errors) : "{}";
        command.Parameters.Add("@errors", NpgsqlTypes.NpgsqlDbType.Jsonb).Value = errorsJson;
        command.Parameters.AddWithValue("@versionDate", versionDate);

        await command.ExecuteNonQueryAsync();

        this._logger.LogInformation("取り込み実行完了: RunId={RunId}", runId);
    }

    public async Task BulkInsertPostalCodesAsync(IEnumerable<PostalCodeRecord> records)
    {
        this._logger.LogInformation("郵便番号データのバルク投入を開始します");

        using var connection = await this.CreateConnectionAsync();

        await this.EnsureSchemaExistsAsync(connection);
        await this.EnsurePostalCodeTableExistsAsync(connection, "postal_codes_landed");

        var copyCommand = @"
            COPY ext.postal_codes_landed (
                local_government_code, old_zip_code5, zip_code7,
                prefecture_katakana, city_katakana, town_katakana,
                prefecture, city, town,
                is_multi_zip, is_koaza, is_chome, is_multi_town,
                update_status, update_reason
            ) FROM STDIN WITH (FORMAT CSV)";

        using var writer = connection.BeginTextImport(copyCommand);

        var recordCount = 0;
        foreach (var record in records)
        {
            var csvLine = $"{record.LocalGovernmentCode},{record.OldZipCode5},{record.ZipCode7}," +
                         $"{record.PrefectureKatakana},{record.CityKatakana},{record.TownKatakana}," +
                         $"{record.Prefecture},{record.City},{record.Town}," +
                         $"{(record.IsMultiZip ? 1 : 0)},{(record.IsKoaza ? 1 : 0)},{(record.IsChome ? 1 : 0)},{(record.IsMultiTown ? 1 : 0)}," +
                         $"{record.UpdateStatus},{record.UpdateReason}";

            await writer.WriteLineAsync(csvLine);
            recordCount++;
        }

        writer.Close();

        this._logger.LogInformation("郵便番号データのバルク投入を完了しました: {RecordCount}件", recordCount);
    }

    public async Task RenameTableAsync(string fromTableName, string toTableName)
    {
        this._logger.LogInformation("テーブルをリネームします: {FromTable} -> {ToTable}", fromTableName, toTableName);

        using var connection = await this.CreateConnectionAsync();

        var sql = $"ALTER TABLE ext.{fromTableName} RENAME TO {toTableName}";
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();

        this._logger.LogInformation("テーブルリネームを完了しました");
    }

    public async Task BackupTableAsync(string tableName, string backupSuffix)
    {
        this._logger.LogInformation("テーブルをバックアップします: {TableName} -> {TableName}_{BackupSuffix}",
            tableName, tableName, backupSuffix);

        using var connection = await this.CreateConnectionAsync();

        var backupTableName = $"{tableName}_{backupSuffix}";
        var sql = $"ALTER TABLE ext.{tableName} RENAME TO {backupTableName}";

        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();

        this._logger.LogInformation("テーブルバックアップを完了しました: {BackupTableName}", backupTableName);
    }

    private async Task EnsureSchemaExistsAsync(NpgsqlConnection connection)
    {
        var sql = "CREATE SCHEMA IF NOT EXISTS ext";
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task EnsurePostalCodeTableExistsAsync(NpgsqlConnection connection, string tableName)
    {
        var sql = $@"
            CREATE TABLE IF NOT EXISTS ext.{tableName} (
                local_government_code TEXT NOT NULL,
                old_zip_code5 TEXT DEFAULT '' NOT NULL,
                zip_code7 TEXT DEFAULT '' NOT NULL,
                prefecture_katakana TEXT DEFAULT '' NOT NULL,
                city_katakana TEXT DEFAULT '' NOT NULL,
                town_katakana TEXT DEFAULT '' NOT NULL,
                prefecture TEXT DEFAULT '' NOT NULL,
                city TEXT DEFAULT '' NOT NULL,
                town TEXT DEFAULT '' NOT NULL,
                is_multi_zip BOOLEAN DEFAULT FALSE NOT NULL,
                is_koaza BOOLEAN DEFAULT FALSE NOT NULL,
                is_chome BOOLEAN DEFAULT FALSE NOT NULL,
                is_multi_town BOOLEAN DEFAULT FALSE NOT NULL,
                update_status TEXT DEFAULT '' NOT NULL,
                update_reason TEXT DEFAULT '' NOT NULL
            )";

        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();

        await this.EnsureIngestionRunsTableExistsAsync(connection);

        // postal_codes テーブルも確保
        if (tableName == "postal_codes_landed")
        {
            await this.EnsurePostalCodesMainTableExistsAsync(connection);
        }
    }

    private async Task EnsureIngestionRunsTableExistsAsync(NpgsqlConnection connection)
    {
        var sql = @"
            CREATE TABLE IF NOT EXISTS ext.ingestion_runs (
                run_id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                source_system TEXT DEFAULT '' NOT NULL,
                version_date date,
                mode TEXT DEFAULT '' NOT NULL,
                started_at timestamp DEFAULT CURRENT_TIMESTAMP,
                finished_at timestamp,
                status TEXT DEFAULT '' NOT NULL,
                landed_rows BIGINT DEFAULT 0 NOT NULL,
                notes TEXT DEFAULT '' NOT NULL,
                errors JSONB DEFAULT '{}' NOT NULL,
                CONSTRAINT ingestion_runs_PKC PRIMARY KEY (run_id)
            )";

        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();

        // ingestion_files テーブルも作成
        await this.EnsureIngestionFilesTableExistsAsync(connection);
    }

    private async Task EnsureIngestionFilesTableExistsAsync(NpgsqlConnection connection)
    {
        var sql = @"
            CREATE TABLE IF NOT EXISTS ext.ingestion_files (
                file_id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                run_id BIGINT NOT NULL,
                filename TEXT DEFAULT '' NOT NULL,
                url TEXT DEFAULT '' NOT NULL,
                file_size BIGINT DEFAULT 0 NOT NULL,
                sha256_hash TEXT DEFAULT '' NOT NULL,
                downloaded_at timestamp DEFAULT CURRENT_TIMESTAMP,
                extracted_files TEXT[] DEFAULT '{}' NOT NULL,
                CONSTRAINT ingestion_files_PKC PRIMARY KEY (file_id),
                CONSTRAINT ingestion_files_run_id_FK FOREIGN KEY (run_id) REFERENCES ext.ingestion_runs(run_id)
            )";

        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task EnsurePostalCodesMainTableExistsAsync(NpgsqlConnection connection)
    {
        var sql = @"
            CREATE TABLE IF NOT EXISTS ext.postal_codes (
                id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                postal_code character(7) DEFAULT '' NOT NULL,
                prefecture_katakana TEXT DEFAULT '' NOT NULL,
                city_katakana TEXT DEFAULT '' NOT NULL,
                town_katakana TEXT DEFAULT '' NOT NULL,
                prefecture TEXT DEFAULT '' NOT NULL,
                city TEXT DEFAULT '' NOT NULL,
                town TEXT DEFAULT '' NOT NULL,
                run_id BIGINT,
                CONSTRAINT postal_codes_PKC PRIMARY KEY (id)
            )";

        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();

        // 仕様書通りの複合インデックスを作成
        var indexSql = @"
            CREATE INDEX IF NOT EXISTS ix_postal_codes_comp
            ON ext.postal_codes (postal_code, prefecture, city, town)";

        using var indexCommand = new NpgsqlCommand(indexSql, connection);
        await indexCommand.ExecuteNonQueryAsync();

        // 論理キー用のユニーク制約を追加（MERGEで必要）
        var uniqueConstraintSql = @"
            ALTER TABLE ext.postal_codes
            ADD CONSTRAINT IF NOT EXISTS uq_postal_codes_logical_key
            UNIQUE (postal_code, prefecture, city, town)";

        try
        {
            using var uniqueCommand = new NpgsqlCommand(uniqueConstraintSql, connection);
            await uniqueCommand.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            // 制約が既に存在する場合は無視
            this._logger.LogDebug(ex, "ユニーク制約の追加をスキップしました（既に存在するか、IF NOT EXISTSがサポートされていません）");
        }
    }

    public async Task UpsertFromLandedAsync()
    {
        this._logger.LogInformation("郵便番号データのupsert処理を開始します（MERGE文使用）");

        using var connection = await this.CreateConnectionAsync();

        var sql = @"
            MERGE INTO ext.postal_codes AS t
            USING (
                SELECT
                    l.zip_code7 AS postal_code,
                    l.prefecture_katakana, l.city_katakana, l.town_katakana,
                    l.prefecture, l.city, l.town
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
                VALUES (s.postal_code, s.prefecture_katakana, s.city_katakana, s.town_katakana, s.prefecture, s.city, s.town)";

        using var command = new NpgsqlCommand(sql, connection);
        var affected = await command.ExecuteNonQueryAsync();

        this._logger.LogInformation("MERGE処理完了: {AffectedRows}件", affected);
    }

    public async Task DeleteFromLandedAsync()
    {
        this._logger.LogInformation("郵便番号データの削除処理を開始します");

        using var connection = await this.CreateConnectionAsync();

        var sql = @"
            DELETE FROM ext.postal_codes t
            USING (
                SELECT
                    l.zip_code7 AS postal_code,
                    l.prefecture, l.city, l.town
                FROM ext.postal_codes_landed l
            ) AS d
            WHERE t.postal_code = d.postal_code
              AND t.prefecture = d.prefecture
              AND t.city = d.city
              AND t.town = d.town";

        using var command = new NpgsqlCommand(sql, connection);
        var affected = await command.ExecuteNonQueryAsync();

        this._logger.LogInformation("削除処理完了: {AffectedRows}件", affected);
    }

    public async Task TruncateLandedTableAsync()
    {
        this._logger.LogInformation("postal_codes_landed テーブルをクリアします");

        using var connection = await this.CreateConnectionAsync();

        var sql = "TRUNCATE TABLE ext.postal_codes_landed";
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task CreateNewTableWithDataAsync()
    {
        this._logger.LogInformation("新しいテーブルを作成してデータを投入します");

        using var connection = await this.CreateConnectionAsync();

        // クリーンアップは別のトランザクションで実行
        // 古いバックアップテーブル（過去のものも含む）とnewテーブルを削除
        var cleanupSqls = new[]
        {
            "DROP TABLE IF EXISTS ext.postal_codes_new CASCADE",
            @"DO $$
            DECLARE
                table_name TEXT;
            BEGIN
                FOR table_name IN
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'ext'
                    AND tablename LIKE 'postal_codes_old_%'
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ext.' || table_name || ' CASCADE';
                END LOOP;
            END $$;"
        };

        foreach (var cleanupSql in cleanupSqls)
        {
            try
            {
                using var cleanupCommand = new NpgsqlCommand(cleanupSql, connection);
                await cleanupCommand.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                this._logger.LogDebug(ex, "クリーンアップコマンド実行時のエラー（無視して続行）: {Sql}", cleanupSql);
            }
        }

        // メインの処理は新しいトランザクションで実行
        using var transaction = connection.BeginTransaction();

        try
        {

            var createSql = @"
                CREATE TABLE ext.postal_codes_new (
                    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                    postal_code character(7) DEFAULT '' NOT NULL,
                    prefecture_katakana TEXT DEFAULT '' NOT NULL,
                    city_katakana TEXT DEFAULT '' NOT NULL,
                    town_katakana TEXT DEFAULT '' NOT NULL,
                    prefecture TEXT DEFAULT '' NOT NULL,
                    city TEXT DEFAULT '' NOT NULL,
                    town TEXT DEFAULT '' NOT NULL,
                    run_id BIGINT,
                    CONSTRAINT postal_codes_new_PKC PRIMARY KEY (id)
                )";

            using var createCommand = new NpgsqlCommand(createSql, connection, transaction);
            await createCommand.ExecuteNonQueryAsync();

            // 2. データコピー
            var insertSql = @"
                INSERT INTO ext.postal_codes_new (postal_code, prefecture_katakana, city_katakana, town_katakana, prefecture, city, town)
                SELECT
                    l.zip_code7 AS postal_code,
                    l.prefecture_katakana, l.city_katakana, l.town_katakana,
                    l.prefecture, l.city, l.town
                FROM ext.postal_codes_landed l";

            using var insertCommand = new NpgsqlCommand(insertSql, connection, transaction);
            var inserted = await insertCommand.ExecuteNonQueryAsync();

            // 3. インデックス作成
            var indexSql = @"
                CREATE INDEX IF NOT EXISTS ix_postal_codes_new_comp
                ON ext.postal_codes_new (postal_code, prefecture, city, town)";

            using var indexCommand = new NpgsqlCommand(indexSql, connection, transaction);
            await indexCommand.ExecuteNonQueryAsync();

            // 4. ANALYZE実行
            var analyzeSql = "ANALYZE ext.postal_codes_new";
            using var analyzeCommand = new NpgsqlCommand(analyzeSql, connection, transaction);
            await analyzeCommand.ExecuteNonQueryAsync();

            transaction.Commit();
            this._logger.LogInformation("新テーブル作成完了: {Rows}件のデータを投入", inserted);
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    public async Task AtomicTableSwitchAsync()
    {
        this._logger.LogInformation("テーブルの瞬時切り替えを開始します");

        using var connection = await this.CreateConnectionAsync();
        using var transaction = connection.BeginTransaction();

        try
        {
            // lock_timeout設定で長時間ブロック回避
            var timeoutSql = "SET LOCAL lock_timeout = '5s'";
            using var timeoutCommand = new NpgsqlCommand(timeoutSql, connection, transaction);
            await timeoutCommand.ExecuteNonQueryAsync();

            // 既存テーブルをバックアップ（存在する場合）
            var backupSuffix = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            try
            {
                var backupSql = $"ALTER TABLE ext.postal_codes RENAME TO postal_codes_old_{backupSuffix}";
                using var backupCommand = new NpgsqlCommand(backupSql, connection, transaction);
                await backupCommand.ExecuteNonQueryAsync();

                // 古いテーブルの制約名も変更して競合を回避
                var renameConstraintSql = $"ALTER TABLE ext.postal_codes_old_{backupSuffix} RENAME CONSTRAINT postal_codes_PKC TO postal_codes_old_{backupSuffix}_PKC";
                using var renameConstraintCommand = new NpgsqlCommand(renameConstraintSql, connection, transaction);
                await renameConstraintCommand.ExecuteNonQueryAsync();

                this._logger.LogInformation("既存テーブルをバックアップ: postal_codes_old_{BackupSuffix}", backupSuffix);
            }
            catch (Exception ex)
            {
                this._logger.LogInformation("既存テーブルが存在しないため、バックアップをスキップします: {Message}", ex.Message);
            }

            // 新テーブルを本番テーブルにリネーム
            var renameSql = "ALTER TABLE ext.postal_codes_new RENAME TO postal_codes";
            using var renameCommand = new NpgsqlCommand(renameSql, connection, transaction);
            await renameCommand.ExecuteNonQueryAsync();

            // 制約名も本番用に変更
            var renameNewConstraintSql = "ALTER TABLE ext.postal_codes RENAME CONSTRAINT postal_codes_new_PKC TO postal_codes_PKC";
            using var renameNewConstraintCommand = new NpgsqlCommand(renameNewConstraintSql, connection, transaction);
            await renameNewConstraintCommand.ExecuteNonQueryAsync();

            transaction.Commit();
            this._logger.LogInformation("テーブル切り替え完了");
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            this._logger.LogError(ex, "テーブル切り替えに失敗しました");
            throw;
        }
    }
}
