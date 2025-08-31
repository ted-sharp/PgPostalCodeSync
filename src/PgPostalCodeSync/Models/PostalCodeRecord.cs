namespace PgPostalCodeSync.Models;

/// <summary>
/// 郵便番号データレコード（日本郵便CSV形式）
/// </summary>
public record PostalCodeRecord
{
    /// <summary>
    /// 全国地方公共団体コード（JIS X0401、X0402）
    /// </summary>
    public string LocalGovernmentCode { get; init; } = String.Empty;

    /// <summary>
    /// 旧郵便番号（5桁）
    /// </summary>
    public string OldZipCode5 { get; init; } = String.Empty;

    /// <summary>
    /// 郵便番号（7桁）
    /// </summary>
    public string ZipCode7 { get; init; } = String.Empty;

    /// <summary>
    /// 都道府県名（カタカナ）
    /// </summary>
    public string PrefectureKatakana { get; init; } = String.Empty;

    /// <summary>
    /// 市区町村名（カタカナ）
    /// </summary>
    public string CityKatakana { get; init; } = String.Empty;

    /// <summary>
    /// 町域名（カタカナ）
    /// </summary>
    public string TownKatakana { get; init; } = String.Empty;

    /// <summary>
    /// 都道府県名
    /// </summary>
    public string Prefecture { get; init; } = String.Empty;

    /// <summary>
    /// 市区町村名
    /// </summary>
    public string City { get; init; } = String.Empty;

    /// <summary>
    /// 町域名
    /// </summary>
    public string Town { get; init; } = String.Empty;

    /// <summary>
    /// 一町域が二以上の郵便番号で表される場合の表示（"1": 該当、"0": 該当せず）
    /// </summary>
    public bool IsMultiZip { get; init; }

    /// <summary>
    /// 小字毎に番地が起番されている町域の表示（"1": 該当、"0": 該当せず）
    /// </summary>
    public bool IsKoaza { get; init; }

    /// <summary>
    /// 丁目を有する町域の場合の表示（"1": 該当、"0": 該当せず）
    /// </summary>
    public bool IsChome { get; init; }

    /// <summary>
    /// 一つの郵便番号で二以上の町域を表す場合の表示（"1": 該当、"0": 該当せず）
    /// </summary>
    public bool IsMultiTown { get; init; }

    /// <summary>
    /// 更新の表示（"0": 変更なし、"1": 変更あり、"2": 廃止）
    /// </summary>
    public string UpdateStatus { get; init; } = String.Empty;

    /// <summary>
    /// 変更理由（"0": 変更なし、"1": 市政・区政・町政・分区・政令指定都市施行、"2": 住居表示の実施、"3": 区画整理、"4": 郵便区調整等、"5": 訂正、"6": 廃止）
    /// </summary>
    public string UpdateReason { get; init; } = String.Empty;

    /// <summary>
    /// CSVの行からPostalCodeRecordを作成
    /// </summary>
    /// <param name="csvFields">CSVフィールド配列（15項目）</param>
    /// <returns>PostalCodeRecord インスタンス</returns>
    public static PostalCodeRecord FromCsvFields(string[] csvFields)
    {
        if (csvFields.Length != 15)
            throw new ArgumentException($"CSVフィールド数が不正です。期待値: 15, 実際: {csvFields.Length}");

        return new PostalCodeRecord
        {
            LocalGovernmentCode = csvFields[0].Trim('"'),
            OldZipCode5 = csvFields[1].Trim('"'),
            ZipCode7 = csvFields[2].Trim('"'),
            PrefectureKatakana = csvFields[3].Trim('"'),
            CityKatakana = csvFields[4].Trim('"'),
            TownKatakana = csvFields[5].Trim('"'),
            Prefecture = csvFields[6].Trim('"'),
            City = csvFields[7].Trim('"'),
            Town = csvFields[8].Trim('"'),
            IsMultiZip = csvFields[9].Trim('"') == "1",
            IsKoaza = csvFields[10].Trim('"') == "1",
            IsChome = csvFields[11].Trim('"') == "1",
            IsMultiTown = csvFields[12].Trim('"') == "1",
            UpdateStatus = csvFields[13].Trim('"'),
            UpdateReason = csvFields[14].Trim('"')
        };
    }
}
