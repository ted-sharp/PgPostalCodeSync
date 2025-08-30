namespace PgPostalCodeSync.Models;

public class PostalCode
{
    public string LocalGovernmentCode { get; set; } = string.Empty;
    public string OldZipCode5 { get; set; } = string.Empty;
    public string ZipCode7 { get; set; } = string.Empty;
    public string PrefectureKatakana { get; set; } = string.Empty;
    public string CityKatakana { get; set; } = string.Empty;
    public string TownKatakana { get; set; } = string.Empty;
    public string Prefecture { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string Town { get; set; } = string.Empty;
    public bool IsMultiZip { get; set; }
    public bool IsKoaza { get; set; }
    public bool IsChome { get; set; }
    public bool IsMultiTown { get; set; }
    public int UpdateStatus { get; set; }
    public int UpdateReason { get; set; }

    public static PostalCode FromCsvLine(string csvLine)
    {
        var fields = ParseCsvLine(csvLine);

        if (fields.Length < 15)
        {
            throw new ArgumentException($"CSV行のフィールド数が不足しています: {csvLine}");
        }

        return new PostalCode
        {
            LocalGovernmentCode = fields[0],
            OldZipCode5 = fields[1],
            ZipCode7 = fields[2],
            PrefectureKatakana = fields[3],
            CityKatakana = fields[4],
            TownKatakana = fields[5],
            Prefecture = fields[6],
            City = fields[7],
            Town = fields[8],
            IsMultiZip = fields[9] == "1",
            IsKoaza = fields[10] == "1",
            IsChome = fields[11] == "1",
            IsMultiTown = fields[12] == "1",
            UpdateStatus = int.Parse(fields[13]),
            UpdateReason = int.Parse(fields[14])
        };
    }

    private static string[] ParseCsvLine(string csvLine)
    {
        var fields = new List<string>();
        var currentField = new System.Text.StringBuilder();
        bool inQuotes = false;

        for (int i = 0; i < csvLine.Length; i++)
        {
            char c = csvLine[i];

            if (c == '"')
            {
                if (inQuotes && i + 1 < csvLine.Length && csvLine[i + 1] == '"')
                {
                    // エスケープされた引用符
                    currentField.Append('"');
                    i++; // 次の引用符をスキップ
                }
                else
                {
                    // 引用符の開始/終了
                    inQuotes = !inQuotes;
                }
            }
            else if (c == ',' && !inQuotes)
            {
                // フィールド区切り
                fields.Add(currentField.ToString());
                currentField.Clear();
            }
            else
            {
                currentField.Append(c);
            }
        }

        // 最後のフィールド
        fields.Add(currentField.ToString());

        return fields.ToArray();
    }
}
