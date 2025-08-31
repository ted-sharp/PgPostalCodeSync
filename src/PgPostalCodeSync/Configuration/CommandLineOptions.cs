using Aloe.Utils.CommandLine;

namespace PgPostalCodeSync.Configuration;

/// <summary>
/// コマンドライン引数オプション（Aloe.Utils.CommandLine使用）
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
    /// コマンドライン引数から CommandLineOptions を作成（Aloe.Utils.CommandLine使用）
    /// </summary>
    /// <param name="args">コマンドライン引数</param>
    /// <returns>解析済み CommandLineOptions</returns>
    public static CommandLineOptions Parse(string[] args)
    {
        // Aloe.Utils.CommandLineを使って引数を前処理
        var flagOptions = new[] { "--full", "--help" };
        var shortOptions = new[] { "-f", "-h" };
        var preprocessedArgs = ArgsHelper.PreprocessArgs(args, flagOptions, shortOptions);

        // 前処理された引数をパース
        var options = new CommandLineOptions();

        for (int i = 0; i < preprocessedArgs.Length; i++)
        {
            var arg = preprocessedArgs[i].ToLower();

            switch (arg)
            {
                case "--full":
                case "-f":
                    options.FullMode = true;
                    break;

                case "--yymm":
                case "-y":
                    if (i + 1 < preprocessedArgs.Length)
                        options.YearMonth = preprocessedArgs[++i];
                    break;

                case "--workdir":
                case "-w":
                    if (i + 1 < preprocessedArgs.Length)
                        options.WorkDirectory = preprocessedArgs[++i];
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
    public static void ShowHelp(string[] args)
    {
        Console.WriteLine("PgPostalCodeSync - 日本郵便局郵便番号データ PostgreSQL 同期ツール");
        Console.WriteLine();
        Console.WriteLine("使用方法:");
        Console.WriteLine("  PgPostalCodeSync [オプション]");
        Console.WriteLine();
        Console.WriteLine("オプション:");
        Console.WriteLine("  --full, -f       強制的にフル取り込みを実行します");
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
        if (String.IsNullOrWhiteSpace(this.YearMonth))
            return null;

        if (this.YearMonth.Length != 4 || !Int32.TryParse(this.YearMonth, out var yymmValue))
            throw new ArgumentException($"年月の形式が不正です。YYMM形式で指定してください: {this.YearMonth}");

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
            throw new ArgumentException($"年月の値が不正です: {this.YearMonth}", ex);
        }
    }

    /// <summary>
    /// 設定値を検証
    /// </summary>
    public void Validate()
    {
        if (!String.IsNullOrWhiteSpace(this.YearMonth))
        {
            this.ParseYearMonth(); // 検証のためにパースを実行
        }
    }
}
