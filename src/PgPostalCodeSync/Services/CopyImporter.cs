using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public class CopyImporter
{
    private readonly ILogger<CopyImporter> _logger;
    private readonly PostalCodeSyncOptions _options;

    public CopyImporter(ILogger<CopyImporter> logger, IOptions<PostalCodeSyncOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    public async Task<CopyResult> ImportToLandedTableAsync(string connectionString, string csvFilePath)
    {
        _logger.LogInformation("landedテーブルへのデータ投入を開始します");

        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            // landedテーブルのクリア
            await TruncateLandedTableAsync(connection);

            // CSVデータの投入
            var result = await CopyCsvToLandedTableAsync(connection, csvFilePath);

            _logger.LogInformation("landedテーブルへのデータ投入が完了しました: {RecordCount} 件", result.RecordCount);

            return result;
        }
        catch (Exception ex)
        {
            var error = $"landedテーブルへのデータ投入中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new CopyResult { Success = false, Error = error };
        }
    }

    private async Task TruncateLandedTableAsync(NpgsqlConnection connection)
    {
        try
        {
            _logger.LogDebug("landedテーブルのクリアを開始します");

            var truncateSql = "TRUNCATE TABLE ext.postal_codes_landed;";

            using var command = new NpgsqlCommand(truncateSql, connection);
            await command.ExecuteNonQueryAsync();

            _logger.LogDebug("landedテーブルのクリアが完了しました");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "landedテーブルのクリア中にエラーが発生しました");
        }
    }

    private async Task<CopyResult> CopyCsvToLandedTableAsync(NpgsqlConnection connection, string csvFilePath)
    {
        try
        {
            var copySql = @"COPY ext.postal_codes_landed (
                postal_code, local_government_code, old_zip_code5,
                prefecture_katakana, city_katakana, town_katakana,
                prefecture, city, town,
                is_multi_zip, is_koaza, is_chome, is_multi_town,
                update_status, update_reason
            ) FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER ',',
                ENCODING 'UTF8'
            )";

            _logger.LogDebug("実行するCOPY SQL: {CopySql}", copySql);

            using var copyWriter = connection.BeginTextImport(copySql);
            using var reader = new StreamReader(csvFilePath, System.Text.Encoding.UTF8);

            var recordCount = 0;
            var errorCount = 0;
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
                    errorCount++;
                    _logger.LogWarning("行の処理に失敗しました (行 {LineNumber}): {Error}", recordCount + errorCount, ex.Message);

                    if (errorCount > 100)
                    {
                        _logger.LogError("エラーが100件を超えたため、処理を中断します");
                        break;
                    }
                }
            }



            if (errorCount > 0)
            {
                _logger.LogWarning("データ投入が完了しましたが、{ErrorCount} 件のエラーが発生しました", errorCount);
            }

            return new CopyResult
            {
                Success = true,
                RecordCount = recordCount,
                ErrorCount = errorCount
            };
        }
        catch (Exception ex)
        {
            var error = $"CSVデータのlandedテーブルへの投入中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new CopyResult { Success = false, Error = error };
        }
    }
}

public class CopyResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public int RecordCount { get; set; }
    public int ErrorCount { get; set; }
}
