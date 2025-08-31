using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgPostalCodeSync.Configuration;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public class PostalCodeSyncService
{
    private readonly ILogger<PostalCodeSyncService> _logger;
    private readonly AppSettings _settings;
    private readonly DownloadService _downloadService;
    private readonly DatabaseService _databaseService;

    public PostalCodeSyncService(
        ILogger<PostalCodeSyncService> logger,
        IOptions<AppSettings> settings,
        DownloadService downloadService,
        DatabaseService databaseService)
    {
        this._logger = logger;
        this._settings = settings.Value;
        this._downloadService = downloadService;
        this._databaseService = databaseService;
    }

    public async Task ExecuteAsync(string[] args)
    {
        this._logger.LogInformation("PostalCodeSyncService.ExecuteAsync() を開始します");

        try
        {
            // 設定値検証
            this._settings.Validate();
            this._logger.LogInformation("設定値検証完了");

            // コマンドライン引数解析
            var options = this.ParseCommandLineArguments(args);
            if (options == null) return; // ヘルプ表示の場合は終了

            // 作業ディレクトリ設定（CLIオプションで上書き可能）
            var workDirectory = options.WorkDirectory ?? this._settings.WorkDirectory;
            this._logger.LogInformation("作業ディレクトリ: {WorkDirectory}", workDirectory);

            // 取り込みモード判定
            var isFullMode = await this.DetermineProcessingModeAsync(options);
            this._logger.LogInformation("処理モード: {Mode}", isFullMode ? "フル取り込み" : "差分取り込み");

            // メイン処理実行
            if (isFullMode)
            {
                this._logger.LogInformation("フル取り込み処理を実行します");
                await this.ExecuteFullImportAsync(workDirectory);
            }
            else
            {
                var targetMonth = options.ParseYearMonth() ?? CalculatePreviousMonth();
                this._logger.LogInformation("差分取り込み処理を実行します: {TargetMonth:yyyy-MM}", targetMonth);
                await this.ExecuteDifferentialImportAsync(workDirectory, targetMonth);
            }
        }
        catch (Exception ex)
        {
            this._logger.LogError(ex, "PostalCodeSyncService.ExecuteAsync() でエラーが発生しました");
            throw;
        }

        this._logger.LogInformation("PostalCodeSyncService.ExecuteAsync() を完了しました");
    }

    private CommandLineOptions? ParseCommandLineArguments(string[] args)
    {
        var options = CommandLineOptions.Parse(args);

        if (options.Help)
        {
            CommandLineOptions.ShowHelp(args);
            return null;
        }

        try
        {
            options.Validate();
        }
        catch (ArgumentException ex)
        {
            this._logger.LogError(ex, "コマンドライン引数の検証に失敗しました");
            CommandLineOptions.ShowHelp(args);
            throw;
        }

        this._logger.LogInformation("コマンドライン引数解析完了: Full={Full}, YearMonth={YearMonth}, WorkDir={WorkDir}",
            options.FullMode, options.YearMonth, options.WorkDirectory);

        return options;
    }

    private async Task<bool> DetermineProcessingModeAsync(CommandLineOptions options)
    {
        // データベース接続確認（全てのモードで必須）
        this._logger.LogInformation("データベース接続を確認します");
        try
        {
            using var connection = await this._databaseService.CreateConnectionAsync();
            this._logger.LogInformation("データベース接続確認完了");
            connection.Close();
        }
        catch (Exception ex)
        {
            this._logger.LogError(ex, "データベース接続に失敗しました");
            throw new InvalidOperationException("データベース接続に失敗しました。接続文字列やデータベースの起動状態を確認してください。", ex);
        }

        if (options.FullMode)
        {
            this._logger.LogInformation("--full オプションが指定されたため、フル取り込みを実行します");
            return true;
        }

        // 既存データ確認によるモード判定
        this._logger.LogInformation("既存データを確認します");
        var hasExistingData = await this._databaseService.HasExistingDataAsync();

        if (!hasExistingData)
        {
            this._logger.LogInformation("既存データが存在しないため、フル取り込みを実行します");
            return true;
        }

        return false;
    }

    private async Task ExecuteFullImportAsync(string workDirectory)
    {
        this._logger.LogInformation("フル取り込みフローを開始します");

        var ingestionRunId = await this._databaseService.CreateIngestionRunAsync("FULL", "RUNNING");

        try
        {
            // 1. utf_ken_all.zip をダウンロードして解凍
            var utfKenAllUrl = this._settings.Download.FullUrl;
            var downloadResult = await this._downloadService.DownloadZipAndExtractAsync(utfKenAllUrl, workDirectory);

            // ダウンロードファイルのメタデータを記録
            await this._databaseService.RecordFileMetadataAsync(
                ingestionRunId,
                Path.GetFileName(downloadResult.FilePath),
                downloadResult.Url,
                downloadResult.FileSize,
                downloadResult.Sha256Hash,
                downloadResult.ExtractedFiles
            );

            // 2. CSVファイルを読み込み
            var csvFiles = downloadResult.ExtractedFiles.Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)).ToArray();
            if (csvFiles.Length == 0)
            {
                throw new InvalidOperationException("CSVファイルが見つかりません");
            }

            var records = new List<PostalCodeRecord>();
            foreach (var csvFile in csvFiles)
            {
                var fileRecords = await this.ReadCsvFileAsync(csvFile);
                records.AddRange(fileRecords);
            }

            this._logger.LogInformation("読み込み完了: {RecordCount}件のレコード", records.Count);

            // 3. postal_codes_landed テーブルにバルク投入
            await this._databaseService.BulkInsertPostalCodesAsync(records);

            // 4. 新テーブル作成→データ投入→インデックス→ANALYZE
            await this._databaseService.CreateNewTableWithDataAsync();

            // 5. 瞬時切り替え（単一トランザクションでリネーム）
            await this._databaseService.AtomicTableSwitchAsync();

            // 6. 一時ファイルクリーンアップ
            if (this._settings.CleanupPolicy.DeleteExtractedFiles)
            {
                this.CleanupTempFiles(workDirectory);
            }

            // 7. landedテーブルのクリーンアップ
            if (this._settings.CleanupPolicy.TruncateLandedAfterProcessing)
            {
                await this._databaseService.TruncateLandedTableAsync();
            }

            // 実行完了を記録
            await this._databaseService.CompleteIngestionRunAsync(ingestionRunId, "SUCCEEDED", records.Count, "フル取り込み正常完了");

            this._logger.LogInformation("フル取り込みフローを完了しました");
        }
        catch (Exception ex)
        {
            // 実行失敗を記録
            await this._databaseService.CompleteIngestionRunAsync(ingestionRunId, "FAILED", 0, "フル取り込みエラー", new { error = ex.Message, stackTrace = ex.StackTrace });

            this._logger.LogError(ex, "フル取り込みフローでエラーが発生しました");
            throw;
        }
    }

    private async Task ExecuteDifferentialImportAsync(string workDirectory, DateTime targetMonth)
    {
        this._logger.LogInformation("差分取り込みフローを開始します: {TargetMonth:yyyy-MM}", targetMonth);

        var ingestionRunId = await this._databaseService.CreateIngestionRunAsync("DIFFERENTIAL", "RUNNING");
        var yymm = targetMonth.ToString("yyMM");

        try
        {
            // 1. 追加・削除データをダウンロード
            var addUrl = this._settings.Download.AddUrlPattern.Replace("{YYMM}", yymm);
            var delUrl = this._settings.Download.DelUrlPattern.Replace("{YYMM}", yymm);

            var tasks = new[]
            {
                this._downloadService.DownloadZipAndExtractAsync(addUrl, Path.Combine(workDirectory, "add")),
                this._downloadService.DownloadZipAndExtractAsync(delUrl, Path.Combine(workDirectory, "del"))
            };

            var results = await Task.WhenAll(tasks);
            var addResult = results[0];
            var delResult = results[1];

            // ダウンロードファイルのメタデータを記録
            await this._databaseService.RecordFileMetadataAsync(
                ingestionRunId,
                Path.GetFileName(addResult.FilePath),
                addResult.Url,
                addResult.FileSize,
                addResult.Sha256Hash,
                addResult.ExtractedFiles
            );

            await this._databaseService.RecordFileMetadataAsync(
                ingestionRunId,
                Path.GetFileName(delResult.FilePath),
                delResult.Url,
                delResult.FileSize,
                delResult.Sha256Hash,
                delResult.ExtractedFiles
            );

            // 2. 追加データ処理
            var addRecords = new List<PostalCodeRecord>();
            foreach (var csvFile in addResult.ExtractedFiles.Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)))
            {
                var fileRecords = await this.ReadCsvFileAsync(csvFile);
                addRecords.AddRange(fileRecords);
            }

            if (addRecords.Any())
            {
                this._logger.LogInformation("追加データを投入します: {Count}件", addRecords.Count);
                await this._databaseService.BulkInsertPostalCodesAsync(addRecords);
                await this._databaseService.UpsertFromLandedAsync();
                await this._databaseService.TruncateLandedTableAsync();
            }

            // 3. 削除データ処理
            var delRecords = new List<PostalCodeRecord>();
            foreach (var csvFile in delResult.ExtractedFiles.Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)))
            {
                var fileRecords = await this.ReadCsvFileAsync(csvFile);
                delRecords.AddRange(fileRecords);
            }

            if (delRecords.Any())
            {
                this._logger.LogInformation("削除データを処理します: {Count}件", delRecords.Count);
                await this._databaseService.BulkInsertPostalCodesAsync(delRecords);
                await this._databaseService.DeleteFromLandedAsync();
                await this._databaseService.TruncateLandedTableAsync();
            }

            // 4. 一時ファイルクリーンアップ
            if (this._settings.CleanupPolicy.DeleteExtractedFiles)
            {
                this.CleanupTempFiles(workDirectory);
            }

            // 5. landedテーブルのクリーンアップ
            if (this._settings.CleanupPolicy.TruncateLandedAfterProcessing)
            {
                await this._databaseService.TruncateLandedTableAsync();
            }

            var totalRecords = addRecords.Count + delRecords.Count;

            // 実行完了を記録
            await this._databaseService.CompleteIngestionRunAsync(
                ingestionRunId,
                "SUCCEEDED",
                totalRecords,
                $"差分取り込み正常完了 (追加:{addRecords.Count}件, 削除:{delRecords.Count}件)"
            );

            this._logger.LogInformation("差分取り込みフローを完了しました");
        }
        catch (Exception ex)
        {
            // 実行失敗を記録
            await this._databaseService.CompleteIngestionRunAsync(
                ingestionRunId,
                "FAILED",
                0,
                "差分取り込みエラー",
                new { error = ex.Message, stackTrace = ex.StackTrace }
            );

            this._logger.LogError(ex, "差分取り込みフローでエラーが発生しました");
            throw;
        }
    }

    private async Task<List<PostalCodeRecord>> ReadCsvFileAsync(string csvFilePath)
    {
        this._logger.LogDebug("CSVファイルを読み込みます: {FilePath}", csvFilePath);

        var records = new List<PostalCodeRecord>();
        var lines = await File.ReadAllLinesAsync(csvFilePath, System.Text.Encoding.UTF8);

        foreach (var line in lines)
        {
            try
            {
                var fields = line.Split(',');
                if (fields.Length >= 15)
                {
                    var record = PostalCodeRecord.FromCsvFields(fields);
                    records.Add(record);
                }
            }
            catch (Exception ex)
            {
                this._logger.LogWarning(ex, "CSVレコードの解析に失敗しました: {Line}", line);
            }
        }

        this._logger.LogDebug("CSVファイル読み込み完了: {FilePath} ({RecordCount}件)", csvFilePath, records.Count);
        return records;
    }

    private void CleanupTempFiles(string workDirectory)
    {
        try
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, true);
                this._logger.LogInformation("一時ファイルをクリーンアップしました: {WorkDirectory}", workDirectory);
            }
        }
        catch (Exception ex)
        {
            this._logger.LogWarning(ex, "一時ファイルクリーンアップに失敗しました: {WorkDirectory}", workDirectory);
        }
    }

    private static DateTime CalculatePreviousMonth()
    {
        var now = DateTime.Now;
        return new DateTime(now.Year, now.Month, 1).AddMonths(-1);
    }
}
