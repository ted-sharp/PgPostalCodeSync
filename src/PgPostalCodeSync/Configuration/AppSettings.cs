using System.ComponentModel.DataAnnotations;

namespace PgPostalCodeSync.Configuration;

public class AppSettings
{
    [Required]
    public string WorkDirectory { get; set; } = String.Empty;

    public DownloadSettings Download { get; set; } = new();

    public string DefaultYyMmRule { get; set; } = "PreviousMonth";

    public bool UseTransactionsOnDiff { get; set; } = false;

    public string VersionDatePolicy { get; set; } = "FirstDayOfPreviousMonth";

    public CleanupSettings CleanupPolicy { get; set; } = new();

    public void Validate()
    {
        if (String.IsNullOrWhiteSpace(this.WorkDirectory))
            throw new InvalidOperationException("WorkDirectory setting is required");

        this.Download.Validate();
    }
}

public class DownloadSettings
{
    public string Utf8Page { get; set; } = "https://www.post.japanpost.jp/zipcode/dl/utf-zip.html";
    public string Utf8Readme { get; set; } = "https://www.post.japanpost.jp/zipcode/dl/utf-readme.html";
    public string BaseUrl { get; set; } = "https://www.post.japanpost.jp/zipcode/dl/utf/zip/";
    public string FullFileName { get; set; } = "utf_ken_all.zip";
    public string FullUrl { get; set; } = "https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_ken_all.zip";
    public string AddPattern { get; set; } = "utf_add_{YYMM}.zip";
    public string DelPattern { get; set; } = "utf_del_{YYMM}.zip";
    public string AddUrlPattern { get; set; } = "https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_add_{YYMM}.zip";
    public string DelUrlPattern { get; set; } = "https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_del_{YYMM}.zip";

    public void Validate()
    {
        if (String.IsNullOrWhiteSpace(this.BaseUrl))
            throw new InvalidOperationException("BaseUrl setting is required");
    }
}

public class CleanupSettings
{
    public bool TruncateLandedAfterProcessing { get; set; } = true;
    public bool DeleteDownloadedFiles { get; set; } = false;
    public bool DeleteExtractedFiles { get; set; } = true;
    public int KeepOldBackupTables { get; set; } = 3;
}
