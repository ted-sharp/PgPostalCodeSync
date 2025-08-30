namespace PgPostalCodeSync.Configuration;

/// <summary>
/// コマンドライン引数オプション
/// </summary>
public class CommandLineOptions
{
    /// <summary>
    /// 強制フル取り込みフラグ
    /// </summary>
    public bool FullMode { get; set; }
    
    /// <summary>
    /// 処理対象年月（YYMM形式）
    /// </summary>
    public string? YearMonth { get; set; }
    
    /// <summary>
    /// 作業ディレクトリパス
    /// </summary>
    public string? WorkDirectory { get; set; }
    
    /// <summary>
    /// ヘルプ表示
    /// </summary>
    public bool Help { get; set; }
    
    /// <summary>
    /// コマンドライン引数から CommandLineOptions を作成
    /// </summary>
    /// <param name="args">コマンドライン引数</param>
    /// <returns>解析済み CommandLineOptions</returns>
    public static CommandLineOptions Parse(string[] args)
    {
        var options = new CommandLineOptions();
        
        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i].ToLower();
            
            switch (arg)
            {
                case "--full":
                    options.FullMode = true;
                    break;
                    
                case "--yymm":
                    if (i + 1 < args.Length)
                        options.YearMonth = args[++i];
                    break;
                    
                case "--workdir":
                    if (i + 1 < args.Length)
                        options.WorkDirectory = args[++i];
                    break;
                    
                case "--help":
                case "-h":
                    options.Help = true;
                    break;
                    
                default:
                    if (arg.StartsWith("--yymm="))
                        options.YearMonth = arg.Substring(7);
                    else if (arg.StartsWith("--workdir="))
                        options.WorkDirectory = arg.Substring(10);
                    break;
            }
        }
        
        return options;
    }
    
    /// <summary>
    /// ヘルプメッセージを表示
    /// </summary>
    public static void ShowHelp()
    {
        Console.WriteLine("PgPostalCodeSync - 日本郵便局郵便番号データ PostgreSQL 同期ツール");
        Console.WriteLine();
        Console.WriteLine("使用方法:");
        Console.WriteLine("  PgPostalCodeSync [オプション]");
        Console.WriteLine();
        Console.WriteLine("オプション:");
        Console.WriteLine("  --full           強制的にフル取り込みを実行します");
        Console.WriteLine("  --yymm <YYMM>    処理対象年月を指定します（YYMM形式、例: 2508）");
        Console.WriteLine("  --workdir <PATH> 作業ディレクトリのパスを指定します");
        Console.WriteLine("  --help, -h       このヘルプメッセージを表示します");
        Console.WriteLine();
        Console.WriteLine("例:");
        Console.WriteLine("  PgPostalCodeSync --full");
        Console.WriteLine("  PgPostalCodeSync --yymm=2508");
        Console.WriteLine("  PgPostalCodeSync --workdir=\"C:\\temp\\postal\"");
    }
    
    /// <summary>
    /// 年月文字列を検証してDateTime型に変換
    /// </summary>
    /// <returns>変換されたDateTime、または null</returns>
    public DateTime? ParseYearMonth()
    {
        if (string.IsNullOrWhiteSpace(YearMonth))
            return null;
            
        if (YearMonth.Length != 4 || !int.TryParse(YearMonth, out var yymmValue))
            throw new ArgumentException($"年月の形式が不正です。YYMM形式で指定してください: {YearMonth}");
        
        var yy = yymmValue / 100;
        var mm = yymmValue % 100;
        
        // 2000年代として解釈（例: 25 -> 2025）
        var year = 2000 + yy;
        
        if (mm < 1 || mm > 12)
            throw new ArgumentException($"月の値が不正です（1-12）: {mm}");
            
        try
        {
            return new DateTime(year, mm, 1);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"年月の値が不正です: {YearMonth}", ex);
        }
    }
    
    /// <summary>
    /// 設定値を検証
    /// </summary>
    public void Validate()
    {
        if (!string.IsNullOrWhiteSpace(YearMonth))
        {
            ParseYearMonth(); // 検証のためにパースを実行
        }
    }
}