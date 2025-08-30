namespace PgPostalCodeSync.Models;

public class CliOptions
{
    public bool Full { get; set; } = false;
    public string? YyMm { get; set; }
    public string? WorkDir { get; set; }

    public static CliOptions Parse(string[] args)
    {
        var options = new CliOptions();

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (arg == "--full" || arg == "-f")
            {
                options.Full = true;
            }
            else if (arg.StartsWith("--yymm="))
            {
                options.YyMm = arg.Substring("--yymm=".Length);
            }
            else if (arg.StartsWith("--workdir="))
            {
                options.WorkDir = arg.Substring("--workdir=".Length);
            }
            else if (arg == "--help" || arg == "-h")
            {
                ShowHelp();
                Environment.Exit(0);
            }
        }

        return options;
    }

    private static void ShowHelp()
    {
        Console.WriteLine("Postal Code Sync for PostgreSQL");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  PostalCodeSync                    # 前月差分を自動取得");
        Console.WriteLine("  PostalCodeSync --full            # フル取り込み");
        Console.WriteLine("  PostalCodeSync --yymm=2508      # 明示的な年月指定");
        Console.WriteLine("  PostalCodeSync --workdir=path   # 作業ディレクトリ指定");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --full, -f          フル取り込みを実行");
        Console.WriteLine("  --yymm=YYMM        対象年月を指定（YY=西暦下2桁, MM=2桁月）");
        Console.WriteLine("  --workdir=path     作業ディレクトリを指定");
        Console.WriteLine("  --help, -h         このヘルプを表示");
    }

    public string GetYyMm()
    {
        if (!string.IsNullOrEmpty(YyMm))
        {
            return YyMm;
        }

        // 前月のYYMMを自動算出
        var now = DateTime.Now;
        var previousMonth = now.AddMonths(-1);
        return $"{previousMonth.Year % 100:D2}{previousMonth.Month:D2}";
    }

    public DateTime GetVersionDate()
    {
        var yymm = GetYyMm();
        var year = 2000 + int.Parse(yymm.Substring(0, 2));
        var month = int.Parse(yymm.Substring(2, 2));

        // 前月の1日を返す
        var date = new DateTime(year, month, 1);
        return date;
    }
}
