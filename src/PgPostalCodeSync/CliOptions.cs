using System.CommandLine;

namespace PgPostalCodeSync;

public class CliOptions
{
    public bool Full { get; set; }
    public string? YyMm { get; set; }
    public string? WorkDir { get; set; }
    public bool Force { get; set; }
    
    public static RootCommand CreateRootCommand()
    {
        var rootCommand = new RootCommand("PostgreSQL postal code synchronization tool");
        
        var fullOption = new Option<bool>(
            ["--full"],
            description: "Force full import instead of differential update");
            
        var yymmOption = new Option<string?>(
            ["--yymm"],
            description: "Specify target year-month (YYMM format, e.g., 2508). If omitted, previous month is used.");
            
        var workdirOption = new Option<string?>(
            ["--workdir"],
            description: "Specify work directory path. If omitted, uses value from appsettings.json.");
            
        var forceOption = new Option<bool>(
            ["--force"],
            description: "Force execution even if the same version and mode has already been successfully processed");
        
        rootCommand.AddOption(fullOption);
        rootCommand.AddOption(yymmOption);
        rootCommand.AddOption(workdirOption);
        rootCommand.AddOption(forceOption);
        
        return rootCommand;
    }
    
    public static CliOptions Parse(string[] args)
    {
        var options = new CliOptions();
        var rootCommand = CreateRootCommand();
        
        rootCommand.SetHandler((bool full, string? yymm, string? workdir, bool force) =>
        {
            options.Full = full;
            options.YyMm = yymm;
            options.WorkDir = workdir;
            options.Force = force;
        }, 
        rootCommand.Options.OfType<Option<bool>>().First(o => o.Name == "full"),
        rootCommand.Options.OfType<Option<string?>>().First(o => o.Name == "yymm"),
        rootCommand.Options.OfType<Option<string?>>().First(o => o.Name == "workdir"),
        rootCommand.Options.OfType<Option<bool>>().First(o => o.Name == "force"));
        
        rootCommand.Invoke(args);
        return options;
    }
    
    public string ResolveYyMm()
    {
        if (!string.IsNullOrEmpty(YyMm))
        {
            if (!IsValidYyMm(YyMm))
                throw new ArgumentException($"Invalid YYMM format: {YyMm}. Expected format like '2508'");
            return YyMm;
        }
        
        var previousMonth = DateTime.Now.AddMonths(-1);
        var yy = previousMonth.Year % 100;
        var mm = previousMonth.Month;
        return $"{yy:D2}{mm:D2}";
    }
    
    public DateTime ResolveVersionDate()
    {
        var yymmStr = ResolveYyMm();
        var yy = int.Parse(yymmStr.Substring(0, 2));
        var mm = int.Parse(yymmStr.Substring(2, 2));
        var year = yy < 50 ? 2000 + yy : 1900 + yy;
        
        return new DateTime(year, mm, 1);
    }
    
    private static bool IsValidYyMm(string yymm)
    {
        if (yymm.Length != 4)
            return false;
            
        if (!int.TryParse(yymm.Substring(0, 2), out var yy) || yy < 0 || yy > 99)
            return false;
            
        if (!int.TryParse(yymm.Substring(2, 2), out var mm) || mm < 1 || mm > 12)
            return false;
            
        return true;
    }
}