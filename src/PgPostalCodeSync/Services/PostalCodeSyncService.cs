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
        var requestedMode = options.Full ? "Full" : "Differential";
        
        // Auto-full mode: if differential requested but no data exists, switch to full
        var hasAnyData = await _metadataService.HasAnyDataAsync(_connectionString, cancellationToken);
        var actualMode = requestedMode;
        var autoSwitchedToFull = false;
        
        if (requestedMode == "Differential" && !hasAnyData)
        {
            actualMode = "Full";
            autoSwitchedToFull = true;
            _logger.LogInformation("Auto-switching to Full mode: no existing data found in postal_codes table");
        }

        _logger.LogInformation("Starting PostalCodeSync - Mode: {Mode}{AutoSwitch}, YYMM: {YyMm}, WorkDir: {WorkDir}, Force: {Force}", 
            actualMode, autoSwitchedToFull ? " (auto-switched from Differential)" : "", yymm, workDir, options.Force);

        if (!options.Force)
        {
            var existingRun = await _metadataService.GetSuccessfulRunAsync(_connectionString, versionDate, actualMode, cancellationToken);
            
            // For Full mode: check if specific version data exists
            // For Differential mode: just check if the same version+mode was already processed
            bool shouldSkip = false;
            if (existingRun != null)
            {
                if (actualMode == "Full")
                {
                    var hasValidData = await _metadataService.HasValidDataAsync(_connectionString, versionDate, cancellationToken);
                    shouldSkip = hasValidData;
                }
                else // Differential mode
                {
                    // For differential, if we already processed this YYMM successfully, skip it
                    shouldSkip = true;
                }
            }
            
            if (shouldSkip)
            {
                _logger.LogInformation("Skipping execution: same version and mode already successfully processed on {FinishedAt} (Run ID: {RunId}). Use --force to override.",
                    existingRun!.FinishedAt, existingRun.RunId);
                
                // Create a skip record for audit trail
                var skipRunId = await _metadataService.CreateIngestionRunAsync(
                    _connectionString, "JapanPost", versionDate, actualMode, cancellationToken);
                await _metadataService.UpdateIngestionRunAsync(
                    _connectionString, skipRunId, "Skipped", 0, 
                    $"Execution skipped - already processed successfully by Run ID {existingRun.RunId} on {existingRun.FinishedAt}", 
                    null, cancellationToken);
                
                _logger.LogInformation("PgPostalCodeSync execution completed - Success: True (Skipped)");
                return true;
            }
            else if (existingRun != null && actualMode == "Full")
            {
                _logger.LogWarning("Found successful run metadata (Run ID: {RunId}) but no valid data present. Proceeding with execution.", 
                    existingRun.RunId);
            }
        }

        Directory.CreateDirectory(workDir);
        Directory.CreateDirectory(Path.Combine(workDir, "downloads"));
        Directory.CreateDirectory(Path.Combine(workDir, "extracted"));
        Directory.CreateDirectory(Path.Combine(workDir, "logs"));

        long runId = 0;
        try
        {
            runId = await _metadataService.CreateIngestionRunAsync(
                _connectionString, "JapanPost", versionDate, actualMode, cancellationToken);

            if (actualMode == "Full")
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