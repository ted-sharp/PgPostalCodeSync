using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgPostalCodeSync.Models;
using Npgsql;

namespace PgPostalCodeSync.Services;

public class PostalCodeSyncService
{
    private readonly ILogger<PostalCodeSyncService> _logger;
    private readonly PostalCodeSyncOptions _options;
    private readonly Downloader _downloader;
    private readonly CopyImporter _copyImporter;
    private readonly Differ _differ;
    private readonly FullSwitch _fullSwitch;
    private readonly MetaRecorder _metaRecorder;

    public PostalCodeSyncService(
        ILogger<PostalCodeSyncService> logger,
        IOptions<PostalCodeSyncOptions> options,
        Downloader downloader,
        CopyImporter copyImporter,
        Differ differ,
        FullSwitch fullSwitch,
        MetaRecorder metaRecorder)
    {
        _logger = logger;
        _options = options.Value;
        _downloader = downloader;
        _copyImporter = copyImporter;
        _differ = differ;
        _fullSwitch = fullSwitch;
        _metaRecorder = metaRecorder;
    }

    public async Task<bool> ExecuteAsync(CliOptions cliOptions, string connectionString)
    {
        var workDir = cliOptions.WorkDir ?? _options.WorkDir;
        var yymm = cliOptions.GetYyMm();
        var versionDate = cliOptions.GetVersionDate();
        var isFullMode = cliOptions.Full;

        _logger.LogInformation("郵便番号同期処理を開始します: Mode={Mode}, YYMM={YyMm}, WorkDir={WorkDir}",
            isFullMode ? "Full" : "Diff", yymm, workDir);

        // 作業ディレクトリの作成
        Directory.CreateDirectory(workDir);
        Directory.CreateDirectory(Path.Combine(workDir, "downloads"));
        Directory.CreateDirectory(Path.Combine(workDir, "extracted"));
        Directory.CreateDirectory(Path.Combine(workDir, "logs"));

        // メタ情報テーブルの存在確認・作成
        if (!await _metaRecorder.EnsureTablesExistAsync(connectionString))
        {
            _logger.LogError("メタ情報テーブルの準備に失敗しました");
            return false;
        }

        // 実行履歴の開始
        var mode = isFullMode ? "Full" : "Diff";
        var ingestionRun = await _metaRecorder.StartIngestionRunAsync(connectionString, mode, yymm, versionDate);

        try
        {
            if (isFullMode)
            {
                return await ExecuteFullModeAsync(ingestionRun, workDir, connectionString);
            }
            else
            {
                // 差分モードの場合、まずデータベースにデータが存在するかチェック
                if (!await HasDataInDatabaseAsync(connectionString))
                {
                    _logger.LogInformation("データベースに郵便番号データが存在しません。フル取り込みモードに切り替えます。");
                    return await ExecuteFullModeAsync(ingestionRun, workDir, connectionString);
                }

                return await ExecuteDiffModeAsync(ingestionRun, yymm, workDir, connectionString);
            }
        }
        catch (Exception ex)
        {
            var error = $"郵便番号同期処理中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);

            await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id, error, ex.ToString());
            return false;
        }
    }

    private async Task<bool> ExecuteFullModeAsync(IngestionRun ingestionRun, string workDir, string connectionString)
    {
        _logger.LogInformation("フルモードでの処理を開始します");

        try
        {
            // 1. フルデータのダウンロード・解凍
            var downloadResult = await _downloader.DownloadFullDataAsync(workDir);
            if (!downloadResult.Success)
            {
                await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id,
                    "フルデータのダウンロードに失敗しました", downloadResult.Error);
                return false;
            }

            // ファイルメタ情報の記録
            var fileName = Path.GetFileName(downloadResult.DownloadedFilePath!);
            await _metaRecorder.RecordIngestionFileAsync(connectionString, ingestionRun.Id,
                fileName, "Full", downloadResult.DownloadedFilePath!, downloadResult.FileSize, downloadResult.Sha256Hash);

            // 2. CSVファイルの検索
            var csvFiles = Directory.GetFiles(downloadResult.ExtractedDirectory!, "*.csv", SearchOption.AllDirectories);
            if (csvFiles.Length == 0)
            {
                var error = "解凍されたCSVファイルが見つかりません";
                await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id, error);
                return false;
            }

            var csvFilePath = csvFiles[0]; // 最初のCSVファイルを使用

            // 3. フル切替処理
            var fullSwitchResult = await _fullSwitch.ExecuteFullSwitchAsync(connectionString, csvFilePath);
            if (!fullSwitchResult.Success)
            {
                await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id,
                    "フル切替処理に失敗しました", fullSwitchResult.Error);
                return false;
            }

            // 4. 実行履歴の完了
            var summary = await _metaRecorder.GenerateSummaryAsync("Full", null, fullSwitchResult.RecordCount);
            await _metaRecorder.CompleteIngestionRunAsync(connectionString, ingestionRun.Id, "Succeeded",
                totalRecords: fullSwitchResult.RecordCount, summary: summary);

            // 5. クリーンアップ
            await CleanupAfterProcessingAsync(workDir, true);

            _logger.LogInformation("フルモードでの処理が完了しました: {RecordCount} 件", fullSwitchResult.RecordCount);
            return true;
        }
        catch (Exception ex)
        {
            var error = $"フルモードでの処理中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);

            await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id, error, ex.ToString());
            return false;
        }
    }

    private async Task<bool> ExecuteDiffModeAsync(IngestionRun ingestionRun, string yymm, string workDir, string connectionString)
    {
        _logger.LogInformation("差分モードでの処理を開始します: YYMM={YyMm}", yymm);

        try
        {
            // 1. 差分データのダウンロード・解凍
            var downloadResult = await _downloader.DownloadDiffDataAsync(yymm, workDir);
            if (!downloadResult.Success)
            {
                await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id,
                    "差分データのダウンロードに失敗しました", downloadResult.Error);
                return false;
            }

            // ファイルメタ情報の記録
            var addFileName = $"utf_add_{yymm}.zip";
            var delFileName = $"utf_del_{yymm}.zip";

            await _metaRecorder.RecordIngestionFileAsync(connectionString, ingestionRun.Id,
                addFileName, "Add", Path.Combine(workDir, "downloads", addFileName), 0, null);
            await _metaRecorder.RecordIngestionFileAsync(connectionString, ingestionRun.Id,
                delFileName, "Del", Path.Combine(workDir, "downloads", delFileName), 0, null);

            // 2. CSVファイルの検索
            var csvFiles = Directory.GetFiles(downloadResult.ExtractedDirectory!, "*.csv", SearchOption.AllDirectories);
            if (csvFiles.Length == 0)
            {
                var error = "解凍されたCSVファイルが見つかりません";
                await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id, error);
                return false;
            }

            var csvFilePath = csvFiles[0]; // 最初のCSVファイルを使用

            // 3. 着地テーブルへのデータ投入
            var copyResult = await _copyImporter.ImportToLandedTableAsync(connectionString, csvFilePath);
            if (!copyResult.Success)
            {
                await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id,
                    "着地テーブルへのデータ投入に失敗しました", copyResult.Error);
                return false;
            }

            // 4. 差分データの適用
            var diffResult = await _differ.ApplyDiffAsync(connectionString);
            if (!diffResult.Success)
            {
                await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id,
                    "差分データの適用に失敗しました", diffResult.Error);
                return false;
            }

            // 5. 実行履歴の完了
            var summary = await _metaRecorder.GenerateSummaryAsync("Diff", yymm,
                copyResult.RecordCount, diffResult.AddedRecords, diffResult.UpdatedRecords, diffResult.DeletedRecords);
            await _metaRecorder.CompleteIngestionRunAsync(connectionString, ingestionRun.Id, "Succeeded",
                totalRecords: copyResult.RecordCount,
                addedRecords: diffResult.AddedRecords,
                updatedRecords: diffResult.UpdatedRecords,
                deletedRecords: diffResult.DeletedRecords,
                summary: summary);

            // 6. クリーンアップ
            await CleanupAfterProcessingAsync(workDir, false);

            _logger.LogInformation("差分モードでの処理が完了しました: 追加={Added}, 更新={Updated}, 削除={Deleted}",
                diffResult.AddedRecords, diffResult.UpdatedRecords, diffResult.DeletedRecords);
            return true;
        }
        catch (Exception ex)
        {
            var error = $"差分モードでの処理中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);

            await _metaRecorder.RecordErrorAsync(connectionString, ingestionRun.Id, error, ex.ToString());
            return false;
        }
    }

    private async Task<bool> HasDataInDatabaseAsync(string connectionString)
    {
        try
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "SELECT COUNT(*) FROM ext.postal_codes";
            using var command = new NpgsqlCommand(sql, connection);
            var result = await command.ExecuteScalarAsync();
            var count = Convert.ToInt32(result);

            _logger.LogDebug("データベース内の郵便番号レコード数: {Count}", count);
            return count > 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "データベースの状態確認中にエラーが発生しました。フル取り込みを実行します。");
            return false;
        }
    }

    private Task CleanupAfterProcessingAsync(string workDir, bool isFullMode)
    {
        try
        {
            if (_options.CleanupPolicy.TruncateLandedAfterProcessing)
            {
                // 着地テーブルのクリアは各サービスで実行済み
                _logger.LogDebug("着地テーブルのクリアは完了済みです");
            }

            if (_options.CleanupPolicy.DeleteExtractedFiles)
            {
                var extractedDir = Path.Combine(workDir, "extracted");
                if (Directory.Exists(extractedDir))
                {
                    Directory.Delete(extractedDir, true);
                    _logger.LogInformation("解凍されたファイルを削除しました");
                }
            }

            if (_options.CleanupPolicy.DeleteDownloadedFiles)
            {
                var downloadsDir = Path.Combine(workDir, "downloads");
                if (Directory.Exists(downloadsDir))
                {
                    Directory.Delete(downloadsDir, true);
                    _logger.LogInformation("ダウンロードされたファイルを削除しました");
                }
            }

            _logger.LogInformation("クリーンアップが完了しました");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "クリーンアップ中にエラーが発生しました");
        }

        return Task.CompletedTask;
    }
}
