namespace PgPostalCodeSync.Configuration;

public class PostalCodeSyncConfig
{
    public const string SectionName = "PostalCodeSync";
    
    public string WorkDir { get; set; } = string.Empty;
    public DownloadConfig Download { get; set; } = new();
    public string DefaultYyMmRule { get; set; } = "PreviousMonth";
    public bool UseTransactionsOnDiff { get; set; } = false;
    public string VersionDatePolicy { get; set; } = "FirstDayOfPreviousMonth";
    public CleanupPolicyConfig CleanupPolicy { get; set; } = new();
}

public class DownloadConfig
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
    
    public string GetAddUrl(string yymm) => AddUrlPattern.Replace("{YYMM}", yymm);
    public string GetDelUrl(string yymm) => DelUrlPattern.Replace("{YYMM}", yymm);
    public string GetAddFileName(string yymm) => AddPattern.Replace("{YYMM}", yymm);
    public string GetDelFileName(string yymm) => DelPattern.Replace("{YYMM}", yymm);
}

public class CleanupPolicyConfig
{
    public bool TruncateLandedAfterProcessing { get; set; } = true;
    public bool DeleteDownloadedFiles { get; set; } = false;
    public bool DeleteExtractedFiles { get; set; } = true;
    public int KeepOldBackupTables { get; set; } = 3;
}