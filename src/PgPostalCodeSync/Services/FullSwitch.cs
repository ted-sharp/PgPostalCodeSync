using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public class FullSwitch
{
    private readonly ILogger<FullSwitch> _logger;
    private readonly PostalCodeSyncOptions _options;

    public FullSwitch(ILogger<FullSwitch> logger, IOptions<PostalCodeSyncOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    public async Task<FullSwitchResult> ExecuteFullSwitchAsync(string connectionString, string csvFilePath)
    {
        _logger.LogInformation("フル切替処理を開始します");

        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            // 1. 新規テーブル作成
            var createResult = await CreateNewTableAsync(connection);
            if (!createResult.Success)
            {
                return new FullSwitchResult { Success = false, Error = createResult.Error };
            }

            // 2. データ投入
            var copyResult = await CopyDataToNewTableAsync(connection, csvFilePath);
            if (!copyResult.Success)
            {
                return new FullSwitchResult { Success = false, Error = copyResult.Error };
            }

            // 3. インデックス作成
            var indexResult = await CreateIndexesAsync(connection);
            if (!indexResult.Success)
            {
                return new FullSwitchResult { Success = false, Error = indexResult.Error };
            }

            // 4. ANALYZE実行
            var analyzeResult = await AnalyzeTableAsync(connection);
            if (!analyzeResult.Success)
            {
                return new FullSwitchResult { Success = false, Error = analyzeResult.Error };
            }

            // 5. 瞬時切替（RENAME）
            var switchResult = await SwitchTablesAsync(connection);
            if (!switchResult.Success)
            {
                return new FullSwitchResult { Success = false, Error = switchResult.Error };
            }

            // 6. 古いバックアップテーブルのクリーンアップ
            await CleanupOldBackupTablesAsync(connection);

            _logger.LogInformation("フル切替処理が完了しました: {RecordCount} 件", copyResult.RecordCount);

            return new FullSwitchResult
            {
                Success = true,
                RecordCount = copyResult.RecordCount,
                NewTableName = "ext.postal_codes_new",
                BackupTableName = switchResult.BackupTableName
            };
        }
        catch (Exception ex)
        {
            var error = $"フル切替処理中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new FullSwitchResult { Success = false, Error = error };
        }
    }

    private async Task<CreateTableResult> CreateNewTableAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogInformation("新規テーブルの作成を開始します");

            var createTableSql = @"
                CREATE TABLE ext.postal_codes_new (
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

            using var command = new NpgsqlCommand(createTableSql, connection);
            await command.ExecuteNonQueryAsync();

            _logger.LogInformation("新規テーブルの作成が完了しました");

            return new CreateTableResult { Success = true };
        }
        catch (Exception ex)
        {
            var error = $"新規テーブルの作成中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new CreateTableResult { Success = false, Error = error };
        }
    }

    private async Task<CopyResult> CopyDataToNewTableAsync(NpgsqlConnection connection, string csvFilePath)
    {
        try
        {
            _logger.LogInformation("新規テーブルへのデータ投入を開始します");

            var copySql = @"COPY ext.postal_codes_new (
                postal_code, local_government_code, old_zip_code5,
                prefecture_katakana, city_katakana, town_katakana,
                prefecture, city, town,
                is_multi_zip, is_koaza, is_chome, is_multi_town,
                update_status, update_reason
            ) FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER ',',
                QUOTE " + "\"" + @",
                ESCAPE " + "\"" + @",
                ENCODING 'UTF8'
            )";

            using var copyWriter = connection.BeginTextImport(copySql);
            using var reader = new StreamReader(csvFilePath, System.Text.Encoding.UTF8);

            var recordCount = 0;
            string? line;

            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                try
                {
                    var postalCode = PostalCode.FromCsvLine(line);

                    var copyLine = $"{postalCode.ZipCode7}," +
                                  $"{postalCode.LocalGovernmentCode}," +
                                  $"{postalCode.OldZipCode5}," +
                                  $"\"{postalCode.PrefectureKatakana}\"," +
                                  $"\"{postalCode.CityKatakana}\"," +
                                  $"\"{postalCode.TownKatakana}\"," +
                                  $"\"{postalCode.Prefecture}\"," +
                                  $"\"{postalCode.City}\"," +
                                  $"\"{postalCode.Town}\"," +
                                  $"{(postalCode.IsMultiZip ? "1" : "0")}," +
                                  $"{(postalCode.IsKoaza ? "1" : "0")}," +
                                  $"{(postalCode.IsChome ? "1" : "0")}," +
                                  $"{(postalCode.IsMultiTown ? "1" : "0")}," +
                                  $"{postalCode.UpdateStatus}," +
                                  $"{postalCode.UpdateReason}";

                    copyWriter.WriteLine(copyLine);
                    recordCount++;

                    if (recordCount % 10000 == 0)
                    {
                        _logger.LogDebug("処理済みレコード数: {RecordCount}", recordCount);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("行の処理に失敗しました: {Error}", ex.Message);
                }
            }



            _logger.LogInformation("新規テーブルへのデータ投入が完了しました: {RecordCount} 件", recordCount);

            return new CopyResult
            {
                Success = true,
                RecordCount = recordCount
            };
        }
        catch (Exception ex)
        {
            var error = $"新規テーブルへのデータ投入中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new CopyResult { Success = false, Error = error };
        }
    }

    private async Task<IndexResult> CreateIndexesAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogInformation("インデックスの作成を開始します");

            var indexSql = @"
                CREATE INDEX IF NOT EXISTS ix_postal_codes_new_comp
                    ON ext.postal_codes_new (postal_code, prefecture, city, town);

                CREATE INDEX IF NOT EXISTS ix_postal_codes_new_prefecture
                    ON ext.postal_codes_new (prefecture);

                CREATE INDEX IF NOT EXISTS ix_postal_codes_new_city
                    ON ext.postal_codes_new (city);";

            using var command = new NpgsqlCommand(indexSql, connection);
            await command.ExecuteNonQueryAsync();

            _logger.LogInformation("インデックスの作成が完了しました");

            return new IndexResult { Success = true };
        }
        catch (Exception ex)
        {
            var error = $"インデックスの作成中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new IndexResult { Success = false, Error = error };
        }
    }

    private async Task<AnalyzeResult> AnalyzeTableAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogInformation("テーブルのANALYZEを開始します");

            var analyzeSql = "ANALYZE ext.postal_codes_new;";

            using var command = new NpgsqlCommand(analyzeSql, connection);
            await command.ExecuteNonQueryAsync();

            _logger.LogInformation("テーブルのANALYZEが完了しました");

            return new AnalyzeResult { Success = true };
        }
        catch (Exception ex)
        {
            var error = $"テーブルのANALYZE中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new AnalyzeResult { Success = false, Error = error };
        }
    }

    private async Task<SwitchResult> SwitchTablesAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogInformation("テーブルの瞬時切替を開始します");

            var backupTableName = $"ext.postal_codes_old_{DateTime.Now:yyyyMMdd_HHmmss}";

            // 単一トランザクションで瞬時切替
            using var transaction = await connection.BeginTransactionAsync();

            try
            {
                // ロックタイムアウト設定
                using var lockCommand = new NpgsqlCommand("SET LOCAL lock_timeout = '5s'", connection, transaction);
                await lockCommand.ExecuteNonQueryAsync();

                // 既存テーブルをバックアップ名に変更
                var renameOldSql = $"ALTER TABLE ext.postal_codes RENAME TO postal_codes_old_{DateTime.Now:yyyyMMdd_HHmmss}";
                using var renameOldCommand = new NpgsqlCommand(renameOldSql, connection, transaction);
                await renameOldCommand.ExecuteNonQueryAsync();

                // 新規テーブルを本番名に変更
                var renameNewSql = "ALTER TABLE ext.postal_codes_new RENAME TO postal_codes";
                using var renameNewCommand = new NpgsqlCommand(renameNewSql, connection, transaction);
                await renameNewCommand.ExecuteNonQueryAsync();

                await transaction.CommitAsync();

                _logger.LogInformation("テーブルの瞬時切替が完了しました");

                return new SwitchResult
                {
                    Success = true,
                    BackupTableName = backupTableName
                };
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
        catch (Exception ex)
        {
            var error = $"テーブルの瞬時切替中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new SwitchResult { Success = false, Error = error };
        }
    }

    private async Task CleanupOldBackupTablesAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogInformation("古いバックアップテーブルのクリーンアップを開始します");

            var cleanupSql = @"
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'ext'
                  AND tablename LIKE 'postal_codes_old_%'
                ORDER BY tablename DESC
                OFFSET 3;";

            using var command = new NpgsqlCommand(cleanupSql, connection);
            using var reader = await command.ExecuteReaderAsync();

            var tablesToDrop = new List<string>();
            while (await reader.ReadAsync())
            {
                tablesToDrop.Add(reader.GetString(0));
            }

            foreach (var tableName in tablesToDrop)
            {
                try
                {
                    var dropSql = $"DROP TABLE ext.{tableName}";
                    using var dropCommand = new NpgsqlCommand(dropSql, connection);
                    await dropCommand.ExecuteNonQueryAsync();
                    _logger.LogInformation("古いバックアップテーブルを削除しました: {TableName}", tableName);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "バックアップテーブルの削除に失敗しました: {TableName}", tableName);
                }
            }

            _logger.LogInformation("古いバックアップテーブルのクリーンアップが完了しました");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "古いバックアップテーブルのクリーンアップ中にエラーが発生しました");
        }
    }
}

public class FullSwitchResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public int RecordCount { get; set; }
    public string? NewTableName { get; set; }
    public string? BackupTableName { get; set; }
}

public class CreateTableResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
}

public class IndexResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
}

public class AnalyzeResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
}

public class SwitchResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public string? BackupTableName { get; set; }
}


