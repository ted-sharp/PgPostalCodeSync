using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public class Differ
{
    private readonly ILogger<Differ> _logger;
    private readonly PostalCodeSyncOptions _options;

    public Differ(ILogger<Differ> logger, IOptions<PostalCodeSyncOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    public async Task<DiffResult> ApplyDiffAsync(string connectionString)
    {
        _logger.LogInformation("差分データの適用を開始します");

        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            // 着地テーブルの件数を確認
            var landedCount = await GetLandedRecordCountAsync(connection);
            if (landedCount == 0)
            {
                _logger.LogWarning("着地テーブルにデータがありません");
                return new DiffResult
                {
                    Success = true,
                    AddedRecords = 0,
                    UpdatedRecords = 0,
                    DeletedRecords = 0
                };
            }

            _logger.LogInformation("着地テーブルのレコード数: {Count}", landedCount);

            // 追加・更新処理（upsert）
            var upsertResult = await ApplyUpsertAsync(connection);
            if (!upsertResult.Success)
            {
                return new DiffResult { Success = false, Error = upsertResult.Error };
            }

            // 削除処理
            var deleteResult = await ApplyDeleteAsync(connection);
            if (!deleteResult.Success)
            {
                return new DiffResult { Success = false, Error = deleteResult.Error };
            }

            _logger.LogInformation("差分データの適用が完了しました: 追加/更新={Added}, 削除={Deleted}",
                upsertResult.AddedRecords + upsertResult.UpdatedRecords, deleteResult.DeletedRecords);

            return new DiffResult
            {
                Success = true,
                AddedRecords = upsertResult.AddedRecords,
                UpdatedRecords = upsertResult.UpdatedRecords,
                DeletedRecords = deleteResult.DeletedRecords
            };
        }
        catch (Exception ex)
        {
            var error = $"差分データの適用中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new DiffResult { Success = false, Error = error };
        }
    }

    private async Task<int> GetLandedRecordCountAsync(NpgsqlConnection connection)
    {
        using var command = new NpgsqlCommand("SELECT COUNT(*) FROM ext.postal_codes_landed", connection);
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private async Task<UpsertResult> ApplyUpsertAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogInformation("追加・更新処理を開始します");

            // PostgreSQL 15+のMERGE文を使用
            var mergeSql = @"
                MERGE INTO ext.postal_codes AS t
                USING (
                    SELECT
                        l.zip_code7 AS postal_code,
                        l.local_government_code,
                        l.old_zip_code5,
                        l.prefecture_katakana, l.city_katakana, l.town_katakana,
                        l.prefecture, l.city, l.town,
                        l.is_multi_zip, l.is_koaza, l.is_chome, l.is_multi_town,
                        l.update_status, l.update_reason
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
                        local_government_code = s.local_government_code,
                        old_zip_code5 = s.old_zip_code5,
                        prefecture_katakana = s.prefecture_katakana,
                        city_katakana = s.city_katakana,
                        town_katakana = s.town_katakana,
                        is_multi_zip = s.is_multi_zip,
                        is_koaza = s.is_koaza,
                        is_chome = s.is_chome,
                        is_multi_town = s.is_multi_town,
                        update_status = s.update_status,
                        update_reason = s.update_reason
                WHEN NOT MATCHED THEN
                    INSERT (
                        postal_code, local_government_code, old_zip_code5,
                        prefecture_katakana, city_katakana, town_katakana,
                        prefecture, city, town,
                        is_multi_zip, is_koaza, is_chome, is_multi_town,
                        update_status, update_reason
                    )
                    VALUES (
                        s.postal_code, s.local_government_code, s.old_zip_code5,
                        s.prefecture_katakana, s.city_katakana, s.town_katakana,
                        s.prefecture, s.city, s.town,
                        s.is_multi_zip, s.is_koaza, s.is_chome, s.is_multi_town,
                        s.update_status, s.update_reason
                    );";

            using var command = new NpgsqlCommand(mergeSql, connection);
            var affectedRows = await command.ExecuteNonQueryAsync();

            // 影響を受けた行数を取得（正確な内訳は別途取得が必要）
            var addedRecords = await GetAddedRecordCountAsync(connection);
            var updatedRecords = await GetUpdatedRecordCountAsync(connection);

            _logger.LogInformation("追加・更新処理が完了しました: 追加={Added}, 更新={Updated}", addedRecords, updatedRecords);

            return new UpsertResult
            {
                Success = true,
                AddedRecords = addedRecords,
                UpdatedRecords = updatedRecords
            };
        }
        catch (Exception ex)
        {
            var error = $"追加・更新処理中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new UpsertResult { Success = false, Error = error };
        }
    }

    private async Task<DeleteResult> ApplyDeleteAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogInformation("削除処理を開始します");

            // 削除対象の件数を事前確認
            var deleteCount = await GetDeleteTargetCountAsync(connection);
            if (deleteCount == 0)
            {
                _logger.LogInformation("削除対象のレコードがありません");
                return new DeleteResult { Success = true, DeletedRecords = 0 };
            }

            _logger.LogInformation("削除対象レコード数: {Count}", deleteCount);

            // 削除処理
            var deleteSql = @"
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
                  AND t.town = d.town;";

            using var command = new NpgsqlCommand(deleteSql, connection);
            var affectedRows = await command.ExecuteNonQueryAsync();

            _logger.LogInformation("削除処理が完了しました: {Deleted} 件", affectedRows);

            return new DeleteResult
            {
                Success = true,
                DeletedRecords = affectedRows
            };
        }
        catch (Exception ex)
        {
            var error = $"削除処理中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new DeleteResult { Success = false, Error = error };
        }
    }

    private async Task<int> GetAddedRecordCountAsync(NpgsqlConnection connection)
    {
        var sql = @"
            SELECT COUNT(*) FROM ext.postal_codes_landed l
            WHERE NOT EXISTS (
                SELECT 1 FROM ext.postal_codes t
                WHERE t.postal_code = l.zip_code7
                  AND t.prefecture = l.prefecture
                  AND t.city = l.city
                  AND t.town = l.town
            )";

        using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private async Task<int> GetUpdatedRecordCountAsync(NpgsqlConnection connection)
    {
        var sql = @"
            SELECT COUNT(*) FROM ext.postal_codes_landed l
            WHERE EXISTS (
                SELECT 1 FROM ext.postal_codes t
                WHERE t.postal_code = l.zip_code7
                  AND t.prefecture = l.prefecture
                  AND t.city = l.city
                  AND t.town = l.town
            )";

        using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private async Task<int> GetDeleteTargetCountAsync(NpgsqlConnection connection)
    {
        var sql = @"
            SELECT COUNT(*) FROM ext.postal_codes t
            WHERE EXISTS (
                SELECT 1 FROM ext.postal_codes_landed l
                WHERE l.zip_code7 = t.postal_code
                  AND l.prefecture = t.prefecture
                  AND l.city = t.city
                  AND l.town = t.town
            )";

        using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }
}

public class DiffResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public int AddedRecords { get; set; }
    public int UpdatedRecords { get; set; }
    public int DeletedRecords { get; set; }
}

public class UpsertResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public int AddedRecords { get; set; }
    public int UpdatedRecords { get; set; }
}

public class DeleteResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public int DeletedRecords { get; set; }
}
