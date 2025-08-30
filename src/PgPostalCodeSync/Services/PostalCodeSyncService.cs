using Microsoft.Extensions.Logging;
using PgPostalCodeSync.Configuration;

namespace PgPostalCodeSync.Services;

public interface IPostalCodeSyncService
{
    Task<bool> ExecuteAsync(CliOptions options, CancellationToken cancellationToken = default);
}

public class PostalCodeSyncService : IPostalCodeSyncService
{
    private readonly IDownloadService _downloadService;
    private readonly IZipExtractorService _zipExtractorService;
    private readonly ICopyImportService _copyImportService;
    private readonly IDifferentialProcessingService _differentialProcessingService;
    private readonly IFullSwitchService _fullSwitchService;
    private readonly IMetadataService _metadataService;
    private readonly PostalCodeSyncConfig _config;
    private readonly string _connectionString;
    private readonly ILogger<PostalCodeSyncService> _logger;

    public PostalCodeSyncService(
        IDownloadService downloadService,
        IZipExtractorService zipExtractorService,
        ICopyImportService copyImportService,
        IDifferentialProcessingService differentialProcessingService,
        IFullSwitchService fullSwitchService,
        IMetadataService metadataService,
        PostalCodeSyncConfig config,
        string connectionString,
        ILogger<PostalCodeSyncService> logger)
    {
        _downloadService = downloadService;
        _zipExtractorService = zipExtractorService;
        _copyImportService = copyImportService;
        _differentialProcessingService = differentialProcessingService;
        _fullSwitchService = fullSwitchService;
        _metadataService = metadataService;
        _config = config;
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task<bool> ExecuteAsync(CliOptions options, CancellationToken cancellationToken = default)
    {
        var workDir = options.WorkDir ?? _config.WorkDir;
        var yymm = options.ResolveYyMm();
        var versionDate = options.ResolveVersionDate();
        var mode = options.Full ? "Full" : "Differential";

        _logger.LogInformation("Starting PostalCodeSync - Mode: {Mode}, YYMM: {YyMm}, WorkDir: {WorkDir}", 
            mode, yymm, workDir);

        Directory.CreateDirectory(workDir);
        Directory.CreateDirectory(Path.Combine(workDir, "downloads"));
        Directory.CreateDirectory(Path.Combine(workDir, "extracted"));
        Directory.CreateDirectory(Path.Combine(workDir, "logs"));

        long runId = 0;
        try
        {
            runId = await _metadataService.CreateIngestionRunAsync(
                _connectionString, "JapanPost", versionDate, mode, cancellationToken);

            if (options.Full)
            {
                return await ExecuteFullModeAsync(workDir, runId, cancellationToken);
            }
            else
            {
                return await ExecuteDifferentialModeAsync(workDir, yymm, runId, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Execution failed: {Error}", ex.Message);

            if (runId > 0)
            {
                try
                {
                    await _metadataService.UpdateIngestionRunAsync(
                        _connectionString, runId, "Failed", 0, ex.Message, new { exception = ex.ToString() }, cancellationToken);
                }
                catch (Exception metaEx)
                {
                    _logger.LogError(metaEx, "Failed to update run metadata after error");
                }
            }

            return false;
        }
    }

    private async Task<bool> ExecuteFullModeAsync(string workDir, long runId, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing full mode");

        var downloadResult = await _downloadService.DownloadFullFileAsync(workDir, cancellationToken);
        if (!downloadResult.Success)
        {
            await _metadataService.UpdateIngestionRunAsync(
                _connectionString, runId, "Failed", 0, "Download failed", 
                new { downloadError = downloadResult.Error }, cancellationToken);
            return false;
        }

        await _metadataService.CreateIngestionFilesAsync(_connectionString, runId, new[] { downloadResult }, cancellationToken);

        var extractedFiles = await _zipExtractorService.ExtractZipAsync(
            Path.Combine(workDir, "downloads", downloadResult.FileName),
            Path.Combine(workDir, "extracted"),
            cancellationToken);

        await _copyImportService.TruncateLandedTableAsync(_connectionString, cancellationToken);

        var importedRows = await _copyImportService.ImportCsvToLandedTableAsync(extractedFiles, _connectionString, cancellationToken);

        var fullSwitchResult = await _fullSwitchService.PerformFullReplacementAsync(_connectionString, runId, cancellationToken);

        if (!fullSwitchResult.Success)
        {
            await _metadataService.UpdateIngestionRunAsync(
                _connectionString, runId, "Failed", importedRows, "Full switch failed", 
                new { switchError = fullSwitchResult.Error }, cancellationToken);
            return false;
        }

        var notes = $"Full replacement completed. Imported: {importedRows} rows. Duration: {fullSwitchResult.Duration.TotalMilliseconds}ms";
        await _metadataService.UpdateIngestionRunAsync(_connectionString, runId, "Succeeded", importedRows, notes, null, cancellationToken);

        await PerformCleanupAsync(workDir, extractedFiles);

        _logger.LogInformation("Full mode execution completed successfully");
        return true;
    }

    private async Task<bool> ExecuteDifferentialModeAsync(string workDir, string yymm, long runId, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing differential mode for YYMM: {YyMm}", yymm);

        var downloadResults = await _downloadService.DownloadDifferentialFilesAsync(yymm, workDir, cancellationToken);
        
        var successfulDownloads = downloadResults.Where(r => r.Success).ToArray();
        if (successfulDownloads.Length == 0)
        {
            await _metadataService.UpdateIngestionRunAsync(
                _connectionString, runId, "Failed", 0, "No files downloaded successfully",
                new { downloadErrors = downloadResults.Where(r => !r.Success).Select(r => new { r.FileName, r.Error }) }, 
                cancellationToken);
            return false;
        }

        await _metadataService.CreateIngestionFilesAsync(_connectionString, runId, successfulDownloads, cancellationToken);

        var downloadedFilePaths = successfulDownloads.Select(r => Path.Combine(workDir, "downloads", r.FileName)).ToArray();
        var extractedFiles = await _zipExtractorService.ExtractMultipleZipsAsync(
            downloadedFilePaths, 
            Path.Combine(workDir, "extracted"), 
            cancellationToken);

        await _copyImportService.TruncateLandedTableAsync(_connectionString, cancellationToken);

        var importedRows = await _copyImportService.ImportCsvToLandedTableAsync(extractedFiles, _connectionString, cancellationToken);

        var hasAddFile = successfulDownloads.Any(r => r.FileName.Contains("_add_"));
        var hasDelFile = successfulDownloads.Any(r => r.FileName.Contains("_del_"));

        var differentialResult = await _differentialProcessingService.ApplyDifferentialChangesAsync(
            _connectionString, hasAddFile, hasDelFile, cancellationToken);

        if (!differentialResult.Success)
        {
            await _metadataService.UpdateIngestionRunAsync(
                _connectionString, runId, "Failed", importedRows, "Differential processing failed",
                new { differentialError = differentialResult.Error }, cancellationToken);
            return false;
        }

        var notes = $"Differential processing completed. Added: {differentialResult.AddedCount}, Deleted: {differentialResult.DeletedCount}. Duration: {differentialResult.Duration.TotalMilliseconds}ms";
        await _metadataService.UpdateIngestionRunAsync(_connectionString, runId, "Succeeded", importedRows, notes, null, cancellationToken);

        await PerformCleanupAsync(workDir, extractedFiles);

        _logger.LogInformation("Differential mode execution completed successfully");
        return true;
    }

    private async Task PerformCleanupAsync(string workDir, string[] extractedFiles)
    {
        try
        {
            if (_config.CleanupPolicy.TruncateLandedAfterProcessing)
            {
                await _copyImportService.TruncateLandedTableAsync(_connectionString);
                _logger.LogInformation("Cleaned up landed table");
            }

            if (_config.CleanupPolicy.DeleteExtractedFiles)
            {
                foreach (var file in extractedFiles)
                {
                    try
                    {
                        if (File.Exists(file))
                        {
                            File.Delete(file);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete extracted file: {File}", file);
                    }
                }
                _logger.LogInformation("Cleaned up {FileCount} extracted files", extractedFiles.Length);
            }

            if (_config.CleanupPolicy.DeleteDownloadedFiles)
            {
                var downloadDir = Path.Combine(workDir, "downloads");
                if (Directory.Exists(downloadDir))
                {
                    foreach (var file in Directory.GetFiles(downloadDir, "*.zip"))
                    {
                        try
                        {
                            File.Delete(file);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete downloaded file: {File}", file);
                        }
                    }
                    _logger.LogInformation("Cleaned up downloaded files");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during cleanup: {Error}", ex.Message);
        }
    }
}