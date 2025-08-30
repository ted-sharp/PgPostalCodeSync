using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using PgPostalCodeSync.Configuration;

namespace PgPostalCodeSync.Services;

public interface IDownloadService
{
    Task<DownloadResult> DownloadFileAsync(string url, string destinationPath, CancellationToken cancellationToken = default);
    Task<DownloadResult[]> DownloadDifferentialFilesAsync(string yymm, string workDir, CancellationToken cancellationToken = default);
    Task<DownloadResult> DownloadFullFileAsync(string workDir, CancellationToken cancellationToken = default);
}

public class DownloadService : IDownloadService
{
    private readonly HttpClient _httpClient;
    private readonly DownloadConfig _config;
    private readonly ILogger<DownloadService> _logger;

    public DownloadService(HttpClient httpClient, DownloadConfig config, ILogger<DownloadService> logger)
    {
        _httpClient = httpClient;
        _config = config;
        _logger = logger;
    }

    public async Task<DownloadResult> DownloadFileAsync(string url, string destinationPath, CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting download: {Url} -> {Destination}", url, destinationPath);

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);

            using var response = await _httpClient.GetAsync(url, cancellationToken);
            
            if (!response.IsSuccessStatusCode)
            {
                var error = $"HTTP {(int)response.StatusCode} {response.StatusCode}";
                _logger.LogError("Download failed for {Url}: {Error}", url, error);
                return new DownloadResult
                {
                    SourceUri = url,
                    FileName = Path.GetFileName(destinationPath),
                    Success = false,
                    Error = error,
                    StartedAt = startTime,
                    FinishedAt = DateTime.UtcNow
                };
            }

            var totalBytes = response.Content.Headers.ContentLength ?? 0;
            long actualSize;
            string hash;
            
            using (var contentStream = await response.Content.ReadAsStreamAsync(cancellationToken))
            {
                await using var fileStream = new FileStream(destinationPath, FileMode.Create, FileAccess.Write, FileShare.None);
                await contentStream.CopyToAsync(fileStream, cancellationToken);
                await fileStream.FlushAsync(cancellationToken);
            }
            
            // File streams are now properly disposed, safe to access file
            actualSize = new FileInfo(destinationPath).Length;
            
            if (actualSize == 0)
            {
                var error = "Downloaded file is empty";
                _logger.LogError("Download failed for {Url}: {Error}", url, error);
                return new DownloadResult
                {
                    SourceUri = url,
                    FileName = Path.GetFileName(destinationPath),
                    Success = false,
                    Error = error,
                    StartedAt = startTime,
                    FinishedAt = DateTime.UtcNow
                };
            }

            hash = await ComputeFileHashAsync(destinationPath, cancellationToken);
            var finishedAt = DateTime.UtcNow;

            _logger.LogInformation("Download completed: {Url} -> {Size} bytes, SHA256: {Hash}, Duration: {Duration}ms", 
                url, actualSize, hash, (finishedAt - startTime).TotalMilliseconds);

            return new DownloadResult
            {
                SourceUri = url,
                FileName = Path.GetFileName(destinationPath),
                SizeBytes = actualSize,
                HashSha256 = hash,
                Success = true,
                StartedAt = startTime,
                FinishedAt = finishedAt
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Download failed for {Url}: {Error}", url, ex.Message);
            return new DownloadResult
            {
                SourceUri = url,
                FileName = Path.GetFileName(destinationPath),
                Success = false,
                Error = ex.Message,
                StartedAt = startTime,
                FinishedAt = DateTime.UtcNow
            };
        }
    }

    public async Task<DownloadResult[]> DownloadDifferentialFilesAsync(string yymm, string workDir, CancellationToken cancellationToken = default)
    {
        var downloadDir = Path.Combine(workDir, "downloads");
        Directory.CreateDirectory(downloadDir);

        var addUrl = _config.GetAddUrl(yymm);
        var delUrl = _config.GetDelUrl(yymm);
        var addFileName = _config.GetAddFileName(yymm);
        var delFileName = _config.GetDelFileName(yymm);
        var addPath = Path.Combine(downloadDir, addFileName);
        var delPath = Path.Combine(downloadDir, delFileName);

        var tasks = new[]
        {
            DownloadFileAsync(addUrl, addPath, cancellationToken),
            DownloadFileAsync(delUrl, delPath, cancellationToken)
        };

        return await Task.WhenAll(tasks);
    }

    public async Task<DownloadResult> DownloadFullFileAsync(string workDir, CancellationToken cancellationToken = default)
    {
        var downloadDir = Path.Combine(workDir, "downloads");
        Directory.CreateDirectory(downloadDir);

        var fullPath = Path.Combine(downloadDir, _config.FullFileName);
        return await DownloadFileAsync(_config.FullUrl, fullPath, cancellationToken);
    }

    private static async Task<string> ComputeFileHashAsync(string filePath, CancellationToken cancellationToken = default)
    {
        using var sha256 = SHA256.Create();
        using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var hashBytes = await sha256.ComputeHashAsync(stream, cancellationToken);
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }
}

public class DownloadResult
{
    public string SourceUri { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long SizeBytes { get; set; }
    public string HashSha256 { get; set; } = string.Empty;
    public bool Success { get; set; }
    public string? Error { get; set; }
    public DateTime StartedAt { get; set; }
    public DateTime FinishedAt { get; set; }
}