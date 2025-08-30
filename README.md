# PgPostalCodeSync

PostgreSQL用の郵便番号データ同期コンソールアプリケーション

## 概要

日本郵便の住所の郵便番号（UTF-8形式）を毎月定期的に取り込み、PostgreSQLデータベースに保持するコンソールアプリケーションです。

## 主な機能

- **定期実行**: 月初に前月分の差分データを自動取り込み
- **初期化・フル取り込み**: データが存在しない場合は全件取り込み
- **強制フル**: `--full`オプションで強制的に全件取り込み可能
- **安全切替**: フル取り込み時は新テーブル作成→リネームで瞬時切替
- **効率重視**: PostgreSQL専用の最適化手法（COPY FROM STDIN等）を採用

## 技術スタック

- .NET 9 / C# 13
- PostgreSQL / Npgsql
- Windows / Linux クロスプラットフォーム対応

## 使用方法

```bash
# 基本実行（前月差分を自動取得）
PgPostalCodeSync

# 明示的な年月指定
PgPostalCodeSync --yymm=2508 --workdir "/var/tmp/postal-sync"

# フル取り込み
PgPostalCodeSync --full --workdir "C:\data\postal-sync"
```

## データソース

- **ダウンロード総合ページ**: https://www.post.japanpost.jp/zipcode/download.html
- **UTF-8版ダウンロードページ**: https://www.post.japanpost.jp/zipcode/dl/utf-zip.html
- **UTF-8版説明ページ**: https://www.post.japanpost.jp/zipcode/dl/utf-readme.html

## ファイル命名規則

- **全件（全国一括）**: `utf_ken_all.zip`
- **差分（追加）**: `utf_add_YYMM.zip`（YY=西暦下2桁, MM=2桁月）
- **差分（削除）**: `utf_del_YYMM.zip`

## データベース設計

### スキーマとテーブル

- **実行メタ管理**:
  - `ext.ingestion_runs` - 実行履歴
  - `ext.ingestion_files` - ファイルメタ情報
- **データ管理**:
  - `ext.postal_codes_landed` - 着地用（一時テーブル）
  - `ext.postal_codes` - 本番テーブル

### 論理キー

複合キー: `(postal_code, prefecture, city, town)`

## 処理フロー

### 差分モード
1. YYMM決定（CLI省略時は前月を自動算出）
2. 差分ZIP取得（add/delファイル）
3. 解凍・着地テーブルへの投入
4. 差分適用（upsert/delete）
5. メタ情報記録

### フルモード
1. 全件ZIP取得
2. 新テーブル作成・データ投入
3. インデックス作成・ANALYZE実行
4. 瞬時切替（リネーム）
5. メタ情報記録

## 開発・ビルド

```bash
# ビルド
dotnet build

# 実行
dotnet run

# 公開
dotnet publish -c Release -r win-x64 --self-contained
dotnet publish -c Release -r linux-x64 --self-contained
```

## ワークディレクトリ構造

```
WorkDir/
├── downloads/     # ZIPファイル保存先
├── extracted/     # 解凍CSV保存先
└── logs/         # ログファイル
```

## 設定

appsettings.jsonで以下を設定可能：
- データベース接続文字列
- ワークディレクトリパス
- ダウンロードURL設定
- ログ設定（Serilog）
- クリーンアップポリシー

## ライセンス

MIT License

## 参考資料

詳細な仕様については以下のドキュメントを参照してください：
- `doc/POSTAL_CODE_SYNC_SPECIFICATION.md` - 完全仕様書
- `doc/DESIGN_QA.md` - 設計Q&A
- `sql/` - データベーススキーマ・セットアップスクリプト
