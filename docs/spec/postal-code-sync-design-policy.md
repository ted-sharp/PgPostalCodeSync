# PgPostalCodeSync 詳細設計方針

## 変更対象ファイル・関数

### 新規作成ファイル
- **src/PgPostalCodeSync/Services/PostalCodeSyncService.cs**
  - メイン処理クラス
  - 差分・フル取り込み制御
- **src/PgPostalCodeSync/Services/DownloadService.cs**
  - ZIP ファイルダウンロード
  - ファイル検証
- **src/PgPostalCodeSync/Services/DatabaseService.cs**
  - PostgreSQL 操作
  - COPY FROM STDIN 実行
- **src/PgPostalCodeSync/Models/PostalCodeRecord.cs**
  - 郵便番号データモデル
- **src/PgPostalCodeSync/Configuration/AppSettings.cs**
  - 設定値管理
- **appsettings.json**
  - データベース接続文字列
  - ワークディレクトリパス

### 変更ファイル
- **src/PgPostalCodeSync/Program.cs**
  - DI コンテナ設定
  - コマンドライン引数解析
  - メイン処理呼び出し

## データ設計方針

### 既存テーブル活用
既存の SQL スキーマ（ext.* テーブル）をそのまま使用:

- **ext.postal_codes_landed**: CSV データ着地用（一時テーブル）
- **ext.postal_codes**: 本番データテーブル
- **ext.ingestion_runs**: 実行メタ管理
- **ext.ingestion_files**: ファイルメタ管理

### データフロー
1. **CSV → ext.postal_codes_landed** (COPY FROM STDIN)
2. **差分モード**: landed → postal_codes (UPSERT/DELETE)
3. **フルモード**: landed → postal_codes_new → RENAME

### 論理キー
複合キー: `(postal_code, prefecture, city, town)`

## アーキテクチャ方針

### コンソールアプリ構成
- **.NET 9 汎用ホスト** (`Host.CreateDefaultBuilder()`)
- **依存性注入** (Microsoft.Extensions.DependencyInjection)
- **設定管理** (Microsoft.Extensions.Configuration)
- **構造化ログ** (Serilog)

### サービス層設計
```
Program.cs (エントリポイント)
├── PostalCodeSyncService (メイン制御)
│   ├── DownloadService (ファイル取得)
│   └── DatabaseService (DB操作)
└── AppSettings (設定管理)
```

## 処理フロー設計

### 差分取り込みフロー
1. 前月YYMM算出 (CLI省略時)
2. utf_add_YYMM.zip / utf_del_YYMM.zip ダウンロード
3. ZIP解凍 → CSV読み込み
4. ext.postal_codes_landed へ COPY FROM STDIN
5. UPSERT (追加) / DELETE (削除) 実行
6. メタ情報記録

### フル取り込みフロー
1. utf_ken_all.zip ダウンロード
2. ZIP解凍 → CSV読み込み
3. ext.postal_codes_new 作成・投入
4. インデックス作成・ANALYZE
5. RENAME: postal_codes → postal_codes_20250830_194436, postal_codes_new → postal_codes
6. メタ情報記録

## 技術詳細方針

### HTTP ダウンロード
- **HttpClient** (標準ライブラリ)
- **URL**: https://www.post.japanpost.jp/zipcode/dl/utf-zip.html のファイル
- **リトライなし** (外部スケジューラで対応)

### PostgreSQL 最適化
- **COPY FROM STDIN** でバルク投入
- **準備文 (PreparedStatement)** 使用
- **トランザクション** 単位: ファイル毎

### エラーハンドリング
- **失敗時即座に終了** (ExitCode != 0)
- **ログ出力**: Serilog (Console + File)
- **クリーンアップ**: using/finally で確実実行

## 設定管理方針

### appsettings.json 構造
```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Database=postal;Username=postgres;Password=***"
  },
  "PostalCodeSync": {
    "WorkDirectory": "C:\\temp\\postal-sync",
    "BaseUrl": "https://www.post.japanpost.jp/zipcode/dl/utf-zip.html",
    "CleanupTempFiles": true
  },
  "Serilog": {
    "MinimumLevel": "Information",
    "WriteTo": [
      { "Name": "Console" },
      { "Name": "File", "Args": { "path": "logs/postal-sync-.txt", "rollingInterval": "Day" }}
    ]
  }
}
```

### コマンドライン引数
- `--full`: 強制フル取り込み
- `--yymm=2508`: 指定年月処理
- `--workdir="C:\\temp"`: 作業ディレクトリ指定

## パッケージ利用方針

### 既存パッケージ継続使用
- ✅ **Aloe.Utils.CommandLine** (1.0.4) - 引数解析
- ✅ **Npgsql** (9.0.3) - PostgreSQL接続
- ✅ **Microsoft.Extensions.Hosting** (9.0.8) - 汎用ホスト
- ✅ **Serilog.Extensions.Hosting** (9.0.0) - ログ
