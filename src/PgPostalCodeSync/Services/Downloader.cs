using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public class Downloader
{
    private readonly ILogger<Downloader> _logger;
    private readonly PostalCodeSyncOptions _options;
    private readonly HttpClient _httpClient;

    public Downloader(ILogger<Downloader> logger, IOptions<PostalCodeSyncOptions> options)
    {
        _logger = logger;
        _options = options.Value;
        _httpClient = new HttpClient();
        _httpClient.Timeout = TimeSpan.FromMinutes(30);
    }

    public async Task<DownloadResult> DownloadFullDataAsync(string workDir)
    {
        _logger.LogInformation("フルデータのダウンロードを開始します: {FileName}", _options.Download.FullFileName);

        var downloadPath = Path.Combine(workDir, "downloads", _options.Download.FullFileName);
        var extractPath = Path.Combine(workDir, "extracted");

        Directory.CreateDirectory(Path.GetDirectoryName(downloadPath)!);
        Directory.CreateDirectory(extractPath);

        try
        {
            // ダウンロード
            var downloadResult = await DownloadFileAsync(_options.Download.FullUrl, downloadPath);
            if (!downloadResult.Success)
            {
                return new DownloadResult { Success = false, Error = downloadResult.Error };
            }

            // SHA-256検証
            var expectedHash = await GetExpectedSha256Async(_options.Download.FullFileName);
            if (!string.IsNullOrEmpty(expectedHash))
            {
                var actualHash = await CalculateSha256Async(downloadPath);
                if (actualHash != expectedHash)
                {
                    var error = $"SHA-256ハッシュが一致しません。期待値: {expectedHash}, 実際: {actualHash}";
                    _logger.LogError(error);
                    return new DownloadResult { Success = false, Error = error };
                }
                _logger.LogInformation("SHA-256ハッシュ検証が完了しました");
            }

            // 解凍
            var extractResult = await ExtractZipAsync(downloadPath, extractPath);
            if (!extractResult.Success)
            {
                return new DownloadResult { Success = false, Error = extractResult.Error };
            }

            return new DownloadResult
            {
                Success = true,
                DownloadedFilePath = downloadPath,
                ExtractedDirectory = extractPath,
                FileSize = downloadResult.FileSize,
                Sha256Hash = await CalculateSha256Async(downloadPath)
            };
        }
        catch (Exception ex)
        {
            var error = $"フルデータのダウンロード中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new DownloadResult { Success = false, Error = error };
        }
    }

    public async Task<DownloadResult> DownloadDiffDataAsync(string yymm, string workDir)
    {
        _logger.LogInformation("差分データのダウンロードを開始します: YYMM={YyMm}", yymm);

        var downloadPath = Path.Combine(workDir, "downloads");
        var extractPath = Path.Combine(workDir, "extracted");

        Directory.CreateDirectory(downloadPath);
        Directory.CreateDirectory(extractPath);

        try
        {
            var addFileName = _options.Download.AddPattern.Replace("{YYMM}", yymm);
            var delFileName = _options.Download.DelPattern.Replace("{YYMM}", yymm);

            var addUrl = _options.Download.AddUrlPattern.Replace("{YYMM}", yymm);
            var delUrl = _options.Download.DelUrlPattern.Replace("{YYMM}", yymm);

            var results = new List<FileDownloadResult>();

            // 追加データのダウンロード
            var addResult = await DownloadFileAsync(addUrl, Path.Combine(downloadPath, addFileName));
            results.Add(addResult);

            // 削除データのダウンロード
            var delResult = await DownloadFileAsync(delUrl, Path.Combine(downloadPath, delFileName));
            results.Add(delResult);

            if (results.Any(r => !r.Success))
            {
                var errors = string.Join("; ", results.Where(r => !r.Success).Select(r => r.Error));
                return new DownloadResult { Success = false, Error = errors };
            }

            // 解凍
            foreach (var result in results)
            {
                var extractResult = await ExtractZipAsync(result.FilePath!, extractPath);
                if (!extractResult.Success)
                {
                    return new DownloadResult { Success = false, Error = extractResult.Error };
                }
            }

            return new DownloadResult
            {
                Success = true,
                DownloadedFilePath = downloadPath,
                ExtractedDirectory = extractPath,
                FileSize = results.Sum(r => r.FileSize),
                Sha256Hash = string.Join(";", results.Select(r => r.Sha256Hash))
            };
        }
        catch (Exception ex)
        {
            var error = $"差分データのダウンロード中にエラーが発生しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new DownloadResult { Success = false, Error = error };
        }
    }

    private async Task<FileDownloadResult> DownloadFileAsync(string url, string filePath)
    {
        try
        {
            _logger.LogInformation("ファイルをダウンロード中: {Url}", url);

            var response = await _httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();

            var fileSize = response.Content.Headers.ContentLength ?? 0;
            if (fileSize == 0)
            {
                return new FileDownloadResult { Success = false, Error = "ダウンロードされたファイルのサイズが0です" };
            }

            using (var fileStream = File.Create(filePath))
            {
                await response.Content.CopyToAsync(fileStream);
                await fileStream.FlushAsync();
            }

            // ファイルストリームを確実にクローズしてからハッシュ計算
            await Task.Delay(200);

            var sha256Hash = await CalculateSha256Async(filePath);

            _logger.LogInformation("ファイルのダウンロードが完了しました: {FilePath}, サイズ: {FileSize} bytes", filePath, fileSize);

            return new FileDownloadResult
            {
                Success = true,
                FilePath = filePath,
                FileSize = fileSize,
                Sha256Hash = sha256Hash
            };
        }
        catch (Exception ex)
        {
            var error = $"ファイルのダウンロードに失敗しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new FileDownloadResult { Success = false, Error = error };
        }
    }

    private async Task<string> CalculateSha256Async(string filePath)
    {
        using var sha256 = SHA256.Create();
        using var fileStream = File.OpenRead(filePath);
        var hash = await sha256.ComputeHashAsync(fileStream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private async Task<string?> GetExpectedSha256Async(string fileName)
    {
        try
        {
            // 実際の運用では、郵便局の公式ページからハッシュ値を取得する
            // ここでは簡易実装としてnullを返す
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SHA-256ハッシュ値の取得に失敗しました");
            return null;
        }
    }

    private async Task<ExtractResult> ExtractZipAsync(string zipPath, string extractPath)
    {
        try
        {
            _logger.LogInformation("ZIPファイルを解凍中: {ZipPath}", zipPath);

            System.IO.Compression.ZipFile.ExtractToDirectory(zipPath, extractPath, true);

            _logger.LogInformation("ZIPファイルの解凍が完了しました: {ExtractPath}", extractPath);

            return new ExtractResult { Success = true, ExtractedDirectory = extractPath };
        }
        catch (Exception ex)
        {
            var error = $"ZIPファイルの解凍に失敗しました: {ex.Message}";
            _logger.LogError(ex, error);
            return new ExtractResult { Success = false, Error = error };
        }
    }
}

public class DownloadResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public string? DownloadedFilePath { get; set; }
    public string? ExtractedDirectory { get; set; }
    public long FileSize { get; set; }
    public string? Sha256Hash { get; set; }
}

public class FileDownloadResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public string? FilePath { get; set; }
    public long FileSize { get; set; }
    public string? Sha256Hash { get; set; }
}

public class ExtractResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public string? ExtractedDirectory { get; set; }
}
