namespace PgPostalCodeSync.Models;

public class PostalCodeSyncOptions
{
    public const string SectionName = "PostalCodeSync";

    public string WorkDir { get; set; } = string.Empty;
    public DownloadOptions Download { get; set; } = new();
    public string DefaultYyMmRule { get; set; } = "PreviousMonth";
    public bool UseTransactionsOnDiff { get; set; } = false;
    public string VersionDatePolicy { get; set; } = "FirstDayOfPreviousMonth";
    public ProcessingOptions Processing { get; set; } = new();
    public CleanupPolicyOptions CleanupPolicy { get; set; } = new();
}

public class DownloadOptions
{
    public string Utf8Page { get; set; } = string.Empty;
    public string Utf8Readme { get; set; } = string.Empty;
    public string BaseUrl { get; set; } = string.Empty;
    public string FullFileName { get; set; } = string.Empty;
    public string FullUrl { get; set; } = string.Empty;
    public string AddPattern { get; set; } = string.Empty;
    public string DelPattern { get; set; } = string.Empty;
    public string AddUrlPattern { get; set; } = string.Empty;
    public string DelUrlPattern { get; set; } = string.Empty;
}

public class CleanupPolicyOptions
{
    public bool TruncateLandedAfterProcessing { get; set; } = true;
    public bool DeleteDownloadedFiles { get; set; } = false;
    public bool DeleteExtractedFiles { get; set; } = true;
    public int KeepOldBackupTables { get; set; } = 3;
}

public class ProcessingOptions
{
    public int BatchSize { get; set; } = 10000;
    public int MaxErrorCount { get; set; } = 100;
}
