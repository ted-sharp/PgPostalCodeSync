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
        _logger = logger;
        _settings = settings.Value;
        _downloadService = downloadService;
        _databaseService = databaseService;
    }

    public async Task ExecuteAsync(string[] args)
    {
        _logger.LogInformation("PostalCodeSyncService.ExecuteAsync() を開始します");
        
        try
        {
            // 設定値検証
            _settings.Validate();
            _logger.LogInformation("設定値検証完了");
            
            // コマンドライン引数解析
            var options = ParseCommandLineArguments(args);
            if (options == null) return; // ヘルプ表示の場合は終了
            
            // 作業ディレクトリ設定（CLIオプションで上書き可能）
            var workDirectory = options.WorkDirectory ?? _settings.WorkDirectory;
            _logger.LogInformation("作業ディレクトリ: {WorkDirectory}", workDirectory);
            
            // 取り込みモード判定
            var isFullMode = await DetermineProcessingModeAsync(options);
            _logger.LogInformation("処理モード: {Mode}", isFullMode ? "フル取り込み" : "差分取り込み");
            
            // メイン処理実行
            if (isFullMode)
            {
                _logger.LogInformation("フル取り込み処理を実行します");
                await ExecuteFullImportAsync(workDirectory);
            }
            else
            {
                var targetMonth = options.ParseYearMonth() ?? CalculatePreviousMonth();
                _logger.LogInformation("差分取り込み処理を実行します: {TargetMonth:yyyy-MM}", targetMonth);
                await ExecuteDifferentialImportAsync(workDirectory, targetMonth);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PostalCodeSyncService.ExecuteAsync() でエラーが発生しました");
            throw;
        }
        
        _logger.LogInformation("PostalCodeSyncService.ExecuteAsync() を完了しました");
    }
    
    private CommandLineOptions? ParseCommandLineArguments(string[] args)
    {
        var options = CommandLineOptions.Parse(args);
        
        if (options.Help)
        {
            CommandLineOptions.ShowHelp();
            return null;
        }
        
        try
        {
            options.Validate();
        }
        catch (ArgumentException ex)
        {
            _logger.LogError(ex, "コマンドライン引数の検証に失敗しました");
            CommandLineOptions.ShowHelp();
            throw;
        }
        
        _logger.LogInformation("コマンドライン引数解析完了: Full={Full}, YearMonth={YearMonth}, WorkDir={WorkDir}",
            options.FullMode, options.YearMonth, options.WorkDirectory);
        
        return options;
    }
    
    private async Task<bool> DetermineProcessingModeAsync(CommandLineOptions options)
    {
        // データベース接続確認（全てのモードで必須）
        _logger.LogInformation("データベース接続を確認します");
        try
        {
            using var connection = await _databaseService.CreateConnectionAsync();
            _logger.LogInformation("データベース接続確認完了");
            connection.Close();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "データベース接続に失敗しました");
            throw new InvalidOperationException("データベース接続に失敗しました。接続文字列やデータベースの起動状態を確認してください。", ex);
        }
        
        if (options.FullMode)
        {
            _logger.LogInformation("--full オプションが指定されたため、フル取り込みを実行します");
            return true;
        }
        
        // 既存データ確認によるモード判定
        _logger.LogInformation("既存データを確認します");
        var hasExistingData = await _databaseService.HasExistingDataAsync();
        
        if (!hasExistingData)
        {
            _logger.LogInformation("既存データが存在しないため、フル取り込みを実行します");
            return true;
        }
        
        return false;
    }
    
    private async Task ExecuteFullImportAsync(string workDirectory)
    {
        _logger.LogInformation("フル取り込みフローを開始します");
        
        var ingestionRunId = await _databaseService.CreateIngestionRunAsync("FULL", "RUNNING");
        
        try
        {
            // 1. utf_ken_all.zip をダウンロードして解凍
            var utfKenAllUrl = $"{_settings.BaseUrl}/utf_ken_all.zip";
            var extractedFiles = await _downloadService.DownloadZipAndExtractAsync(utfKenAllUrl, workDirectory);
            
            // 2. CSVファイルを読み込み
            var csvFiles = extractedFiles.Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)).ToArray();
            if (csvFiles.Length == 0)
            {
                throw new InvalidOperationException("CSVファイルが見つかりません");
            }
            
            var records = new List<PostalCodeRecord>();
            foreach (var csvFile in csvFiles)
            {
                var fileRecords = await ReadCsvFileAsync(csvFile);
                records.AddRange(fileRecords);
            }
            
            _logger.LogInformation("読み込み完了: {RecordCount}件のレコード", records.Count);
            
            // 3. postal_codes_landed テーブルにバルク投入
            await _databaseService.BulkInsertPostalCodesAsync(records);
            
            // 4. 既存テーブルをバックアップ（存在する場合）
            var backupSuffix = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            try
            {
                await _databaseService.BackupTableAsync("postal_codes", backupSuffix);
                _logger.LogInformation("既存テーブルをバックアップしました: postal_codes_{Suffix}", backupSuffix);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "テーブルバックアップに失敗しました（テーブルが存在しない可能性があります）");
            }
            
            // 5. postal_codes_landed を postal_codes にリネーム
            await _databaseService.RenameTableAsync("postal_codes_landed", "postal_codes");
            
            // 6. 一時ファイルクリーンアップ
            if (_settings.CleanupTempFiles)
            {
                CleanupTempFiles(workDirectory);
            }
            
            _logger.LogInformation("フル取り込みフローを完了しました");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "フル取り込みフローでエラーが発生しました");
            throw;
        }
    }

    private async Task ExecuteDifferentialImportAsync(string workDirectory, DateTime targetMonth)
    {
        _logger.LogInformation("差分取り込みフローを開始します: {TargetMonth:yyyy-MM}", targetMonth);
        
        var ingestionRunId = await _databaseService.CreateIngestionRunAsync("DIFFERENTIAL", "RUNNING");
        var yymm = targetMonth.ToString("yyMM");
        
        try
        {
            // 1. 追加・削除データをダウンロード
            var addUrl = $"{_settings.BaseUrl}/utf_add_{yymm}.zip";
            var delUrl = $"{_settings.BaseUrl}/utf_del_{yymm}.zip";
            
            var tasks = new[]
            {
                _downloadService.DownloadZipAndExtractAsync(addUrl, Path.Combine(workDirectory, "add")),
                _downloadService.DownloadZipAndExtractAsync(delUrl, Path.Combine(workDirectory, "del"))
            };
            
            var results = await Task.WhenAll(tasks);
            var addFiles = results[0];
            var delFiles = results[1];
            
            // 2. 追加データ処理
            var addRecords = new List<PostalCodeRecord>();
            foreach (var csvFile in addFiles.Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)))
            {
                var fileRecords = await ReadCsvFileAsync(csvFile);
                addRecords.AddRange(fileRecords);
            }
            
            if (addRecords.Any())
            {
                _logger.LogInformation("追加データを投入します: {Count}件", addRecords.Count);
                await _databaseService.BulkInsertPostalCodesAsync(addRecords);
            }
            
            // 3. 削除データ処理（実装は簡略化）
            var delRecords = new List<PostalCodeRecord>();
            foreach (var csvFile in delFiles.Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)))
            {
                var fileRecords = await ReadCsvFileAsync(csvFile);
                delRecords.AddRange(fileRecords);
            }
            
            if (delRecords.Any())
            {
                _logger.LogInformation("削除対象データ: {Count}件（削除処理は省略）", delRecords.Count);
            }
            
            // 4. 一時ファイルクリーンアップ
            if (_settings.CleanupTempFiles)
            {
                CleanupTempFiles(workDirectory);
            }
            
            _logger.LogInformation("差分取り込みフローを完了しました");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "差分取り込みフローでエラーが発生しました");
            throw;
        }
    }

    private async Task<List<PostalCodeRecord>> ReadCsvFileAsync(string csvFilePath)
    {
        _logger.LogDebug("CSVファイルを読み込みます: {FilePath}", csvFilePath);
        
        var records = new List<PostalCodeRecord>();
        var lines = await File.ReadAllLinesAsync(csvFilePath, System.Text.Encoding.GetEncoding("Shift_JIS"));
        
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
                _logger.LogWarning(ex, "CSVレコードの解析に失敗しました: {Line}", line);
            }
        }
        
        _logger.LogDebug("CSVファイル読み込み完了: {FilePath} ({RecordCount}件)", csvFilePath, records.Count);
        return records;
    }

    private void CleanupTempFiles(string workDirectory)
    {
        try
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, true);
                _logger.LogInformation("一時ファイルをクリーンアップしました: {WorkDirectory}", workDirectory);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "一時ファイルクリーンアップに失敗しました: {WorkDirectory}", workDirectory);
        }
    }

    private static DateTime CalculatePreviousMonth()
    {
        var now = DateTime.Now;
        return new DateTime(now.Year, now.Month, 1).AddMonths(-1);
    }
}