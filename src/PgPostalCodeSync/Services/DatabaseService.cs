using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgPostalCodeSync.Models;

namespace PgPostalCodeSync.Services;

public class DatabaseService
{
    private readonly ILogger<DatabaseService> _logger;
    private readonly string _connectionString;

    public DatabaseService(ILogger<DatabaseService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _connectionString = configuration.GetConnectionString("DefaultConnection") 
            ?? throw new InvalidOperationException("DefaultConnection is not configured");
    }

    public async Task<NpgsqlConnection> CreateConnectionAsync()
    {
        _logger.LogDebug("データベース接続を作成します");
        
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        
        _logger.LogDebug("データベース接続を確立しました");
        return connection;
    }

    public async Task<bool> HasExistingDataAsync()
    {
        _logger.LogDebug("既存データの存在を確認します");
        
        try
        {
            using var connection = await CreateConnectionAsync();
            
            var sql = @"
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'ext' 
                    AND table_name = 'postal_codes'
                )";
            
            using var command = new NpgsqlCommand(sql, connection);
            var exists = await command.ExecuteScalarAsync() as bool? ?? false;
            
            if (exists)
            {
                var countSql = "SELECT COUNT(*) FROM ext.postal_codes LIMIT 1";
                using var countCommand = new NpgsqlCommand(countSql, connection);
                var count = await countCommand.ExecuteScalarAsync() as long? ?? 0;
                exists = count > 0;
            }
            
            _logger.LogDebug("既存データ確認結果: {HasData}", exists);
            return exists;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "既存データ確認でエラーが発生しました");
            throw new InvalidOperationException("データベース接続に失敗しました。接続文字列やデータベースの起動状態を確認してください。", ex);
        }
    }

    public async Task<long> CreateIngestionRunAsync(string runType, string status)
    {
        _logger.LogInformation("取り込み実行レコードを作成します: Type={RunType}, Status={Status}", runType, status);
        
        using var connection = await CreateConnectionAsync();
        
        await EnsureSchemaExistsAsync(connection);
        
        var sql = @"
            INSERT INTO ext.ingestion_runs (mode, status, started_at) 
            VALUES (@mode, @status, @startedAt) 
            RETURNING run_id";
        
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("@mode", runType);
        command.Parameters.AddWithValue("@status", status);
        command.Parameters.AddWithValue("@startedAt", DateTime.UtcNow);
        
        var id = await command.ExecuteScalarAsync() as long? ?? 0;
        
        _logger.LogInformation("取り込み実行レコードを作成しました: ID={Id}", id);
        return id;
    }

    public async Task BulkInsertPostalCodesAsync(IEnumerable<PostalCodeRecord> records)
    {
        _logger.LogInformation("郵便番号データのバルク投入を開始します");
        
        using var connection = await CreateConnectionAsync();
        
        await EnsureSchemaExistsAsync(connection);
        await EnsurePostalCodeTableExistsAsync(connection, "postal_codes_landed");
        
        var copyCommand = @"
            COPY ext.postal_codes_landed (
                local_government_code, old_zip_code_5, zip_code_7,
                prefecture_katakana, city_katakana, town_katakana,
                prefecture, city, town,
                is_multi_zip, is_koaza, is_chome, is_multi_town,
                update_status, update_reason
            ) FROM STDIN WITH (FORMAT CSV)";
        
        using var writer = connection.BeginTextImport(copyCommand);
        
        var recordCount = 0;
        foreach (var record in records)
        {
            var csvLine = $"{record.LocalGovernmentCode},{record.OldZipCode5},{record.ZipCode7}," +
                         $"{record.PrefectureKatakana},{record.CityKatakana},{record.TownKatakana}," +
                         $"{record.Prefecture},{record.City},{record.Town}," +
                         $"{(record.IsMultiZip ? 1 : 0)},{(record.IsKoaza ? 1 : 0)},{(record.IsChome ? 1 : 0)},{(record.IsMultiTown ? 1 : 0)}," +
                         $"{record.UpdateStatus},{record.UpdateReason}";
            
            await writer.WriteLineAsync(csvLine);
            recordCount++;
        }
        
        writer.Close();
        
        _logger.LogInformation("郵便番号データのバルク投入を完了しました: {RecordCount}件", recordCount);
    }

    public async Task RenameTableAsync(string fromTableName, string toTableName)
    {
        _logger.LogInformation("テーブルをリネームします: {FromTable} -> {ToTable}", fromTableName, toTableName);
        
        using var connection = await CreateConnectionAsync();
        
        var sql = $"ALTER TABLE ext.{fromTableName} RENAME TO {toTableName}";
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
        
        _logger.LogInformation("テーブルリネームを完了しました");
    }

    public async Task BackupTableAsync(string tableName, string backupSuffix)
    {
        _logger.LogInformation("テーブルをバックアップします: {TableName} -> {TableName}_{BackupSuffix}", 
            tableName, tableName, backupSuffix);
        
        using var connection = await CreateConnectionAsync();
        
        var backupTableName = $"{tableName}_{backupSuffix}";
        var sql = $"ALTER TABLE ext.{tableName} RENAME TO {backupTableName}";
        
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
        
        _logger.LogInformation("テーブルバックアップを完了しました: {BackupTableName}", backupTableName);
    }

    private async Task EnsureSchemaExistsAsync(NpgsqlConnection connection)
    {
        var sql = "CREATE SCHEMA IF NOT EXISTS ext";
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task EnsurePostalCodeTableExistsAsync(NpgsqlConnection connection, string tableName)
    {
        var sql = $@"
            CREATE TABLE IF NOT EXISTS ext.{tableName} (
                local_government_code TEXT NOT NULL,
                old_zip_code_5 TEXT NOT NULL,
                zip_code_7 TEXT NOT NULL,
                prefecture_katakana TEXT NOT NULL,
                city_katakana TEXT NOT NULL,
                town_katakana TEXT NOT NULL,
                prefecture TEXT NOT NULL,
                city TEXT NOT NULL,
                town TEXT NOT NULL,
                is_multi_zip BOOLEAN NOT NULL,
                is_koaza BOOLEAN NOT NULL,
                is_chome BOOLEAN NOT NULL,
                is_multi_town BOOLEAN NOT NULL,
                update_status TEXT NOT NULL,
                update_reason TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )";
        
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
        
        await EnsureIngestionRunsTableExistsAsync(connection);
    }

    private async Task EnsureIngestionRunsTableExistsAsync(NpgsqlConnection connection)
    {
        var sql = @"
            CREATE TABLE IF NOT EXISTS ext.ingestion_runs (
                run_id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                source_system TEXT DEFAULT '' NOT NULL,
                version_date date,
                mode TEXT DEFAULT '' NOT NULL,
                started_at timestamp DEFAULT CURRENT_TIMESTAMP,
                finished_at timestamp,
                status TEXT DEFAULT '' NOT NULL,
                landed_rows BIGINT DEFAULT 0 NOT NULL,
                notes TEXT DEFAULT '' NOT NULL,
                errors JSONB DEFAULT '{}' NOT NULL,
                CONSTRAINT ingestion_runs_PKC PRIMARY KEY (run_id)
            )";
        
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}