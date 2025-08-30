using System.IO.Compression;
using Microsoft.Extensions.Logging;

namespace PgPostalCodeSync.Services;

public interface IZipExtractorService
{
    Task<string[]> ExtractZipAsync(string zipFilePath, string extractDir, CancellationToken cancellationToken = default);
    Task<string[]> ExtractMultipleZipsAsync(string[] zipFilePaths, string extractDir, CancellationToken cancellationToken = default);
}

public class ZipExtractorService : IZipExtractorService
{
    private readonly ILogger<ZipExtractorService> _logger;

    public ZipExtractorService(ILogger<ZipExtractorService> logger)
    {
        _logger = logger;
    }

    public Task<string[]> ExtractZipAsync(string zipFilePath, string extractDir, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Extracting ZIP file: {ZipFile} -> {ExtractDir}", zipFilePath, extractDir);

        if (!File.Exists(zipFilePath))
        {
            throw new FileNotFoundException($"ZIP file not found: {zipFilePath}");
        }

        Directory.CreateDirectory(extractDir);

        try
        {
            var extractedFiles = new List<string>();

            using var archive = ZipFile.OpenRead(zipFilePath);

            foreach (var entry in archive.Entries)
            {
                if (string.IsNullOrEmpty(entry.Name))
                    continue;

                cancellationToken.ThrowIfCancellationRequested();

                var extractPath = Path.Combine(extractDir, entry.Name);

                Directory.CreateDirectory(Path.GetDirectoryName(extractPath)!);

                _logger.LogDebug("Extracting entry: {EntryName} -> {ExtractPath}", entry.Name, extractPath);

                entry.ExtractToFile(extractPath, overwrite: true);
                extractedFiles.Add(extractPath);
            }

            _logger.LogInformation("ZIP extraction completed: {ZipFile} -> {FileCount} files extracted",
                zipFilePath, extractedFiles.Count);

            return Task.FromResult(extractedFiles.ToArray());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to extract ZIP file: {ZipFile}", zipFilePath);
            throw;
        }
    }

    public async Task<string[]> ExtractMultipleZipsAsync(string[] zipFilePaths, string extractDir, CancellationToken cancellationToken = default)
    {
        var allExtractedFiles = new List<string>();

        foreach (var zipFilePath in zipFilePaths)
        {
            if (!File.Exists(zipFilePath))
            {
                _logger.LogWarning("ZIP file not found, skipping: {ZipFile}", zipFilePath);
                continue;
            }

            var extractedFiles = await ExtractZipAsync(zipFilePath, extractDir, cancellationToken);
            allExtractedFiles.AddRange(extractedFiles);
        }

        return allExtractedFiles.ToArray();
    }
}
