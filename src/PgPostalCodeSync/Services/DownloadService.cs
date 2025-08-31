using Microsoft.Extensions.Logging;
using System.IO.Compression;
using System.Security.Cryptography;

namespace PgPostalCodeSync.Services;

public record DownloadResult
{
    public string FilePath { get; init; } = String.Empty;
    public string Url { get; init; } = String.Empty;
    public long FileSize { get; init; }
    public string Sha256Hash { get; init; } = String.Empty;
    public DateTime DownloadStartTime { get; init; }
    public DateTime DownloadEndTime { get; init; }
    public string[] ExtractedFiles { get; init; } = Array.Empty<string>();
}

public class DownloadService
{
    private readonly ILogger<DownloadService> _logger;
    private readonly HttpClient _httpClient;

    public DownloadService(ILogger<DownloadService> logger, HttpClient httpClient)
    {
        this._logger = logger;
        this._httpClient = httpClient;
    }

    public async Task<DownloadResult> DownloadFileAsync(string url, string outputPath)
    {
        this._logger.LogInformation("ファイルのダウンロードを開始します: {Url}", url);

        try
        {
            var response = await this._httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();

            var directory = Path.GetDirectoryName(outputPath);
            if (!String.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var downloadStartTime = DateTime.UtcNow;

            using (var fileStream = File.Create(outputPath))
            {
                using var httpStream = await response.Content.ReadAsStreamAsync();
                await httpStream.CopyToAsync(fileStream);
                await fileStream.FlushAsync();
            }

            var fileInfo = new FileInfo(outputPath);
            var sha256Hash = await this.CalculateSha256Async(outputPath);
            var downloadEndTime = DateTime.UtcNow;

            this._logger.LogInformation("ファイルのダウンロードを完了しました: {OutputPath} ({Size} bytes, SHA256: {Hash})",
                outputPath, fileInfo.Length, sha256Hash);

            return new DownloadResult
            {
                FilePath = outputPath,
                Url = url,
                FileSize = fileInfo.Length,
                Sha256Hash = sha256Hash,
                DownloadStartTime = downloadStartTime,
                DownloadEndTime = downloadEndTime
            };
        }
        catch (Exception ex)
        {
            this._logger.LogError(ex, "ファイルのダウンロードでエラーが発生しました: {Url}", url);
            throw;
        }
    }

    public async Task<string> CalculateSha256Async(string filePath)
    {
        using var sha256 = SHA256.Create();
        using var fileStream = File.OpenRead(filePath);
        var hashBytes = await sha256.ComputeHashAsync(fileStream);
        return Convert.ToHexString(hashBytes).ToLower();
    }

    public async Task<DownloadResult> DownloadZipAndExtractAsync(string url, string workDirectory)
    {
        this._logger.LogInformation("ZIPファイルのダウンロードと解凍を開始します: {Url}", url);

        try
        {
            var zipPath = Path.Combine(workDirectory, Path.GetFileName(new Uri(url).LocalPath));

            var downloadResult = await this.DownloadFileAsync(url, zipPath);

            var extractPath = Path.Combine(workDirectory, "extracted");
            if (Directory.Exists(extractPath))
            {
                Directory.Delete(extractPath, true);
            }
            Directory.CreateDirectory(extractPath);

            ZipFile.ExtractToDirectory(zipPath, extractPath);

            var extractedFiles = Directory.GetFiles(extractPath, "*", SearchOption.AllDirectories);
            this._logger.LogInformation("ZIP解凍完了: {FileCount}個のファイルを解凍しました", extractedFiles.Length);

            return downloadResult with { ExtractedFiles = extractedFiles };
        }
        catch (Exception ex)
        {
            this._logger.LogError(ex, "ZIPファイルのダウンロード・解凍でエラーが発生しました: {Url}", url);
            throw;
        }
    }

    public Task<bool> ValidateFileAsync(string filePath, long? minimumSize = null)
    {
        this._logger.LogDebug("ファイル検証を開始します: {FilePath}", filePath);

        try
        {
            if (!File.Exists(filePath))
            {
                this._logger.LogWarning("ファイルが存在しません: {FilePath}", filePath);
                return Task.FromResult(false);
            }

            var fileInfo = new FileInfo(filePath);
            if (minimumSize.HasValue && fileInfo.Length < minimumSize.Value)
            {
                this._logger.LogWarning("ファイルサイズが最小サイズを下回っています: {ActualSize} < {MinimumSize}",
                    fileInfo.Length, minimumSize.Value);
                return Task.FromResult(false);
            }

            this._logger.LogDebug("ファイル検証成功: {FilePath} ({Size} bytes)", filePath, fileInfo.Length);
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            this._logger.LogError(ex, "ファイル検証でエラーが発生しました: {FilePath}", filePath);
            return Task.FromResult(false);
        }
    }
}
