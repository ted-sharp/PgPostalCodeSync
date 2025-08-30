using System.ComponentModel.DataAnnotations;

namespace PgPostalCodeSync.Configuration;

public class AppSettings
{
    [Required]
    public string WorkDirectory { get; set; } = string.Empty;
    
    [Required]
    public string BaseUrl { get; set; } = string.Empty;
    
    public bool CleanupTempFiles { get; set; } = true;
    
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(WorkDirectory))
            throw new InvalidOperationException("WorkDirectory setting is required");
            
        if (string.IsNullOrWhiteSpace(BaseUrl))
            throw new InvalidOperationException("BaseUrl setting is required");
    }
}