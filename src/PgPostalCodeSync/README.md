# Postal Code Sync for PostgreSQL

日本郵便の郵便番号データ定期取り込みコンソールアプリケーション

## 概要

このアプリケーションは、日本郵便が提供するUTF-8版郵便番号データを定期的にPostgreSQLデータベースに取り込むためのコンソールアプリケーションです。

- **差分取り込み**: 毎月の差分データ（追加・削除）を自動取得・適用
- **フル取り込み**: 全件データの一括取り込み（初期化時や強制実行時）
- **瞬時切替**: フル取り込み時は新テーブル作成→瞬時リネームでダウンタイム最小化
- **実行履歴管理**: 取り込み履歴とファイルメタ情報の詳細記録

## 機能

### 差分取り込みモード
- 前月の差分データ（`utf_add_YYMM.zip`, `utf_del_YYMM.zip`）を自動取得
- 着地テーブル経由での安全なデータ投入
- PostgreSQL 15+のMERGE文による効率的なupsert処理
- 非トランザクション処理による高速化

### フル取り込みモード
- 全件データ（`utf_ken_all.zip`）の一括取得
- 新規テーブル作成→データ投入→インデックス作成→ANALYZE
- 瞬時テーブル切替によるダウンタイム最小化
- 古いバックアップテーブルの自動クリーンアップ

### データ管理
- 複合キー（郵便番号、都道府県、市区町村、町域）による一意性管理
- 全角カナ・漢字データの完全保持
- 更新フラグ・変更理由の詳細記録
- バージョン日付によるデータ履歴管理

## セットアップ

### 1. 前提条件

- .NET 9.0 SDK
- PostgreSQL 15+（MERGE文対応）
- Windows / Linux クロスプラットフォーム対応

### 2. データベース準備

```sql
-- extスキーマの作成
CREATE SCHEMA ext;

-- テーブルの作成（create_tables.sqlを実行）
\i create_tables.sql
```

### 3. 設定ファイル

`appsettings.json`を編集して、データベース接続情報と作業ディレクトリを設定：

```json
{
  "PostalCodeSync": {
    "WorkDir": "C:\\data\\postal-sync"
  },
  "ConnectionStrings": {
    "PostalDb": "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postal_code_db"
  }
}
```

### 4. ビルド・実行

```bash
# ビルド
dotnet build

# 実行（前月差分を自動取得）
dotnet run

# フル取り込み
dotnet run -- --full

# 明示的な年月指定
dotnet run -- --yymm=2508

# 作業ディレクトリ指定
dotnet run -- --workdir="/var/tmp/postal-sync"
```

## 使用方法

### 基本実行

```bash
# 前月差分を自動取得（推奨）
PostalCodeSync

# ヘルプ表示
PostalCodeSync --help
```

### オプション

| オプション | 説明 | 例 |
|------------|------|-----|
| `--full`, `-f` | フル取り込みを実行 | `--full` |
| `--yymm=YYMM` | 対象年月を指定 | `--yymm=2508` |
| `--workdir=path` | 作業ディレクトリを指定 | `--workdir=C:\data\postal-sync` |
| `--help`, `-h` | ヘルプを表示 | `--help` |

### 実行例

```bash
# 前月差分を自動取得
PostalCodeSync

# 2025年8月の差分を取得
PostalCodeSync --yymm=2508

# フル取り込みを実行
PostalCodeSync --full

# 作業ディレクトリを指定してフル取り込み
PostalCodeSync --full --workdir="/var/tmp/postal-sync"
```

## アーキテクチャ

### ディレクトリ構造

```
WorkDir/
├── downloads/     # ZIPファイル保存先
├── extracted/     # 解凍CSV保存先
└── logs/         # ログファイル（Serilog設定による）
```

### データフロー

1. **ダウンロード**: 日本郵便公式サイトからZIPファイル取得
2. **解凍**: 作業ディレクトリにCSVファイル展開
3. **着地**: `COPY FROM STDIN`で着地テーブルに投入
4. **処理**: 差分適用またはフル切替
5. **記録**: 実行履歴とファイルメタ情報を記録
6. **クリーンアップ**: 一時ファイルの削除

### テーブル構成

- `ext.ingestion_runs`: 実行履歴管理
- `ext.ingestion_files`: ファイルメタ情報
- `ext.postal_codes_landed`: 着地テーブル（一時データ）
- `ext.postal_codes`: 本番テーブル（郵便番号データ）

## ログ・監視

### Serilog設定

- **Console**: 標準出力へのログ出力
- **File**: 日次ローテーション、14日間保持
- **Debug**: デバッグ出力
- **Async**: 全出力の非同期化

### メトリクス

- ダウンロード時間・ファイルサイズ・SHA-256
- 処理レコード数・所要時間
- 差分処理件数（追加・更新・削除）
- フル切替のロック待ち発生状況

## 運用

### スケジューリング

外部スケジューラ（cron、Task Scheduler等）で月初に実行：

```bash
# Linux (cron)
0 2 1 * * /path/to/PostalCodeSync

# Windows (Task Scheduler)
# 毎月1日 午前2時に実行
```

### 監視ポイント

- 実行ログの確認（`ext.ingestion_runs.status`）
- エラー詳細の確認（`ext.ingestion_runs.errors`）
- 処理件数の確認（`ext.ingestion_runs.total_records`）
- ディスク容量の監視（作業ディレクトリ）

### トラブルシューティング

#### よくある問題

1. **ダウンロード失敗**
   - ネットワーク接続確認
   - 日本郵便サイトの稼働状況確認

2. **データベース接続エラー**
   - 接続文字列の確認
   - PostgreSQLの稼働状況確認
   - 権限設定の確認

3. **COPY処理エラー**
   - CSVファイルの文字エンコーディング確認
   - テーブルスキーマの確認

#### 再実行

- 同一YYMMの再実行は冪等設計のため安全
- 失敗時は`ext.ingestion_runs.errors`で詳細確認
- 必要に応じて手動クリーンアップ後再実行

## 開発

### ビルド

```bash
# リリースビルド
dotnet build -c Release

# 実行可能ファイルの作成
dotnet publish -c Release -r win-x64 --self-contained
dotnet publish -c Release -r linux-x64 --self-contained
```

### テスト

```bash
# 単体テスト実行
dotnet test

# カバレッジ測定
dotnet test --collect:"XPlat Code Coverage"
```

### 依存関係

- **Npgsql**: PostgreSQL接続
- **Microsoft.Extensions**: 設定・DI・ログ
- **Serilog**: 構造化ログ
- **System.CommandLine**: CLI引数解析

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 参考資料

- [日本郵便 郵便番号ダウンロード](https://www.post.japanpost.jp/zipcode/download.html)
- [UTF-8版郵便番号データ](https://www.post.japanpost.jp/zipcode/dl/utf-zip.html)
- [PostgreSQL MERGE文](https://www.postgresql.org/docs/current/sql-merge.html)
