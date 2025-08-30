using Microsoft.Extensions.Logging;
using System.IO.Compression;

namespace PgPostalCodeSync.Services;

public class DownloadService
{
    private readonly ILogger<DownloadService> _logger;
    private readonly HttpClient _httpClient;

    public DownloadService(ILogger<DownloadService> logger, HttpClient httpClient)
    {
        _logger = logger;
        _httpClient = httpClient;
    }

    public async Task<string> DownloadFileAsync(string url, string outputPath)
    {
        _logger.LogInformation("ファイルのダウンロードを開始します: {Url}", url);
        
        try
        {
            var response = await _httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();
            
            var directory = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            
            using var fileStream = File.Create(outputPath);
            using var httpStream = await response.Content.ReadAsStreamAsync();
            await httpStream.CopyToAsync(fileStream);
            
            _logger.LogInformation("ファイルのダウンロードを完了しました: {OutputPath} ({Size} bytes)", 
                outputPath, new FileInfo(outputPath).Length);
            
            return outputPath;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ファイルのダウンロードでエラーが発生しました: {Url}", url);
            throw;
        }
    }

    public async Task<string[]> DownloadZipAndExtractAsync(string url, string workDirectory)
    {
        _logger.LogInformation("ZIPファイルのダウンロードと解凍を開始します: {Url}", url);
        
        try
        {
            var zipPath = Path.Combine(workDirectory, Path.GetFileName(new Uri(url).LocalPath));
            
            await DownloadFileAsync(url, zipPath);
            
            var extractPath = Path.Combine(workDirectory, "extracted");
            if (Directory.Exists(extractPath))
            {
                Directory.Delete(extractPath, true);
            }
            Directory.CreateDirectory(extractPath);
            
            ZipFile.ExtractToDirectory(zipPath, extractPath);
            
            var extractedFiles = Directory.GetFiles(extractPath, "*", SearchOption.AllDirectories);
            _logger.LogInformation("ZIP解凍完了: {FileCount}個のファイルを解凍しました", extractedFiles.Length);
            
            return extractedFiles;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ZIPファイルのダウンロード・解凍でエラーが発生しました: {Url}", url);
            throw;
        }
    }

    public Task<bool> ValidateFileAsync(string filePath, long? minimumSize = null)
    {
        _logger.LogDebug("ファイル検証を開始します: {FilePath}", filePath);
        
        try
        {
            if (!File.Exists(filePath))
            {
                _logger.LogWarning("ファイルが存在しません: {FilePath}", filePath);
                return Task.FromResult(false);
            }
            
            var fileInfo = new FileInfo(filePath);
            if (minimumSize.HasValue && fileInfo.Length < minimumSize.Value)
            {
                _logger.LogWarning("ファイルサイズが最小サイズを下回っています: {ActualSize} < {MinimumSize}", 
                    fileInfo.Length, minimumSize.Value);
                return Task.FromResult(false);
            }
            
            _logger.LogDebug("ファイル検証成功: {FilePath} ({Size} bytes)", filePath, fileInfo.Length);
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ファイル検証でエラーが発生しました: {FilePath}", filePath);
            return Task.FromResult(false);
        }
    }
}