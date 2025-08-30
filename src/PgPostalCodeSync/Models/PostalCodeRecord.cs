namespace PgPostalCodeSync.Models;

public class PostalCodeRecord
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
    public string UpdateStatus { get; set; } = string.Empty;
    public string UpdateReason { get; set; } = string.Empty;
}

public class PostalCodeLanded
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
    public string UpdateStatus { get; set; } = string.Empty;
    public string UpdateReason { get; set; } = string.Empty;
}

public class PostalCode
{
    public long Id { get; set; }
    public string PostalCodeValue { get; set; } = string.Empty;
    public string PrefectureKatakana { get; set; } = string.Empty;
    public string CityKatakana { get; set; } = string.Empty;
    public string TownKatakana { get; set; } = string.Empty;
    public string Prefecture { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string Town { get; set; } = string.Empty;
    public long? RunId { get; set; }
}