namespace PgPostalCodeSync.Models;

public class IngestionRun
{
    public int Id { get; set; }
    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public string Status { get; set; } = string.Empty; // Succeeded, Failed, InProgress
    public string Mode { get; set; } = string.Empty; // Full, Diff
    public string? YyMm { get; set; }
    public DateTime VersionDate { get; set; }
    public int? TotalRecords { get; set; }
    public int? AddedRecords { get; set; }
    public int? UpdatedRecords { get; set; }
    public int? DeletedRecords { get; set; }
    public string? Errors { get; set; } // JSONB形式でエラー詳細
    public string? Summary { get; set; } // 処理サマリー
}

public class IngestionFile
{
    public int Id { get; set; }
    public int IngestionRunId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string FileType { get; set; } = string.Empty; // Full, Add, Del
    public string FilePath { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public string? Sha256Hash { get; set; }
    public DateTime DownloadedAt { get; set; }
    public DateTime? ProcessedAt { get; set; }
    public string? Errors { get; set; }
}
