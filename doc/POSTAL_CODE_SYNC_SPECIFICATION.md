# Postal Code Sync for PostgreSQL - 完全仕様書

**作成日:** 2025-08-30 (JST)
**対象バージョン:** .NET 9 / C# 13 / PostgreSQL 17 / EF Core 9 / Npgsql
**プロジェクト:** 郵便番号データ定期取り込みコンソールアプリケーション

---

## 0. プロジェクト概要とスコープ

### 0.1 目的
日本郵便の**住所の郵便番号（1レコード1行、UTF-8形式）**を毎月定期的に取り込み、PostgreSQLデータベースに保持するコンソールアプリケーションを作成する。

### 0.2 基本仕様
- **実行タイミング:** 月初に前月分の差分を取り込む（外部スケジューラで実行）
- **初期化・フル取り込み:** データが存在しない場合は全件取り込み
- **強制フル:** `--full`オプションで強制的に全件取り込み可能
- **安全切替:** フル取り込み時は新テーブル作成→リネームで瞬時切替
- **効率重視:** PostgreSQL専用の最適化手法を積極採用

### 0.3 技術スタック
- **.NET 9 / C# 13**
- **PostgreSQL / Npgsql**
- **依存ライブラリ:**
  - Aloe.Utils.CommandLine (コマンドライン引数解析)
  - Serilog (ログ出力)
- **開発支援:** A5:SQL Mk-2
- **プラットフォーム:** Windows / Linux クロスプラットフォーム対応

### 0.4 非スコープ
- **事業所の個別番号:** 対象外（別系統のため）
- **旧来形式:** SJIS / KEN_ALL / 都道府県別は不使用
- **アプリ内スケジューラ:** 搭載しない（外部ツールで実行）

---

## 1. データソースと取得方式

### 1.1 公式ダウンロード元
- **ダウンロード総合ページ:** https://www.post.japanpost.jp/zipcode/download.html
- **UTF-8版ダウンロードページ:** https://www.post.japanpost.jp/zipcode/dl/utf-zip.html
- **UTF-8版説明ページ:** https://www.post.japanpost.jp/zipcode/dl/utf-readme.html

### 1.2 ファイル命名規則
- **全件（全国一括）:** `utf_ken_all.zip`
- **差分（追加）:** `utf_add_YYMM.zip`（YY=西暦下2桁, MM=2桁月）
- **差分（削除）:** `utf_del_YYMM.zip`

### 1.3 ファイル仕様
- **文字コード:** UTF-8（BOMなし）
- **区切り文字:** カンマ（`,`）
- **行区切り:** CRLF
- **引用符:** 2～9列は`"`で囲み
- **レコード単位:** 1郵便番号=1行（旧来の分割なし）

---

## 2. CSVレイアウトとスキーマ対応

### 2.1 UTF-8版CSV列定義

| 列# | 項目名 | データ型/形式 | 着地テーブル列名 | 備考 |
|-----|--------|---------------|------------------|------|
| 1 | 全国地方公共団体コード | 半角数字 | `local_government_code` | JIS X0401/0402 |
| 2 | 旧郵便番号（5桁） | 半角数字 | `old_zip_code5` | |
| 3 | 郵便番号（7桁） | 半角数字 | `zip_code7` | 先頭0保持 |
| 4 | 都道府県名（カナ） | 全角カナ | `prefecture_katakana` | |
| 5 | 市区町村名（カナ） | 全角カナ | `city_katakana` | |
| 6 | 町域名（カナ） | 全角カナ | `town_katakana` | |
| 7 | 都道府県名（漢字） | 文字列 | `prefecture` | |
| 8 | 市区町村名（漢字） | 文字列 | `city` | |
| 9 | 町域名（漢字） | 文字列 | `town` | |
| 10 | 一町域が複数郵便番号 | 0/1 | `is_multi_zip` | |
| 11 | 小字毎に番地が起番 | 0/1 | `is_koaza` | |
| 12 | 丁目を有する | 0/1 | `is_chome` | |
| 13 | 1郵便番号で複数町域 | 0/1 | `is_multi_town` | |
| 14 | 更新フラグ | 0/1/2 | `update_status` | 0:変更なし/1:変更あり/2:廃止 |
| 15 | 変更理由 | 0～6 | `update_reason` | |

### 2.2 論理キー（差分判定用）
**複合キー:** `(postal_code, prefecture, city, town)`
- CSVに一意キー列がないため、上記4項目の組み合わせで同定
- UNIQUE制約は付与しない（運用上の柔軟性を優先）

---

## 3. データベース設計

### 3.1 採用スキーマとテーブル
- **実行メタ管理（オプショナル）:**
  - `ext.ingestion_runs` - 実行履歴（存在しなくても動作）
  - `ext.ingestion_files` - ファイルメタ情報（存在しなくても動作）
- **データ管理（必須）:**
  - `ext.postal_codes_landed` - 着地用（一時テーブル）
  - `ext.postal_codes` - 本番テーブル

### 3.2 インデックス設計
```sql
-- 本番テーブル用（差分判定・検索高速化）
CREATE INDEX IF NOT EXISTS ix_postal_codes_comp
  ON ext.postal_codes (postal_code, prefecture, city, town);
```

**着地テーブルにはインデックスを作成しない**
- 理由：短命用途でI/O優先、作成コストがリターンを上回る

### 3.3 バージョン管理
- `version_date`は常に**前月の1日**を記録
- 例：2025年9月実行時 → `version_date = '2025-08-01'`

---

## 4. CLI設計と設定

### 4.1 コマンドライン引数
```bash
# 基本実行（実データ存在チェック → 自動判定）
# - データなし: フル取り込み（初期化モード）
# - データあり: 前月差分取り込み
PostalCodeSync

# 明示的な年月指定（差分モード）
PostalCodeSync --yymm=2508 --workdir "/var/tmp/postal-sync"

# 強制フル取り込み
PostalCodeSync --full --workdir "C:\data\postal-sync"
```

### 4.2 appsettings.json設定例
```json
{
  "PostalCodeSync": {
    "WorkDir": "C:\\data\\postal-sync",
    "Download": {
      "Utf8Page": "https://www.post.japanpost.jp/zipcode/dl/utf-zip.html",
      "Utf8Readme": "https://www.post.japanpost.jp/zipcode/dl/utf-readme.html",
      "BaseUrl": "https://www.post.japanpost.jp/zipcode/dl/utf/zip/",
      "FullFileName": "utf_ken_all.zip",
      "FullUrl": "https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_ken_all.zip",
      "AddPattern": "utf_add_{YYMM}.zip",
      "DelPattern": "utf_del_{YYMM}.zip",
      "AddUrlPattern": "https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_add_{YYMM}.zip",
      "DelUrlPattern": "https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_del_{YYMM}.zip"
    },
    "DefaultYyMmRule": "PreviousMonth",
    "UseTransactionsOnDiff": false,
    "VersionDatePolicy": "FirstDayOfPreviousMonth",
    "CleanupPolicy": {
      "TruncateLandedAfterProcessing": true,
      "DeleteDownloadedFiles": false,
      "DeleteExtractedFiles": true,
      "KeepOldBackupTables": 3
    }
  },
  "ConnectionStrings": {
    "PostalDb": "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postal_code_db"
  },
  "Serilog": {
    "Using": ["Serilog.Sinks.Console", "Serilog.Sinks.File", "Serilog.Sinks.Debug", "Serilog.Sinks.Async"],
    "MinimumLevel": "Information",
    "WriteTo": [
      { "Name": "Async", "Args": { "configure": [{ "Name": "Console" }] } },
      { "Name": "Async", "Args": { "configure": [{ "Name": "Debug" }] } },
      { "Name": "Async", "Args": { "configure": [{ "Name": "File", "Args": {
        "path": "logs/log-.txt",
        "rollingInterval": "Day",
        "retainedFileCountLimit": 14,
        "rollOnFileSizeLimit": true,
        "fileSizeLimitBytes": 10485760
      }}] } }
    ],
    "Enrich": ["FromLogContext"]
  }
}
```

---

## 5. 処理フロー詳細

### 5.1 処理フロー図（Mermaid）
```mermaid
flowchart TB
  A[外部スケジューラ起動<br/>console --yymm=YYMM / --full] --> B{YYMM 指定?}
  B -- あり --> C[指定YYMM]
  B -- なし --> D{実データ存在チェック<br/>ext.postal_codes}
  D -- データなし --> D1[フルモードへ]
  D -- データあり --> D2[前月YYMMを算出]
  C --> E[ダウンロード対象決定]
  D1 --> E
  D2 --> E

  subgraph S1[ファイル取得～着地]
    E --> F{--full ?}
    F -- はい --> G[utf_ken_all.zip 取得<br/>ZIP解凍→CSV]
    F -- いいえ --> H[utf_add_YYMM.zip / utf_del_YYMM.zip 取得<br/>ZIP解凍→CSV]
    G --> I[ext.postal_codes_landed に COPY]
    H --> I
  end

  subgraph S2[差分モード]
    I --> J{--full ?}
    J -- いいえ(差分) --> K[論理キー=(postal_code,prefecture,city,town)]
    K --> L[ADD: upsert into ext.postal_codes<br/>※明示TXなし]
    K --> M[DEL: 複合キー一致 delete<br/>※明示TXなし]
    L --> N[ingestion_runs/files 記録<br/>version_date=前月1日（テーブルが存在する場合）]
    M --> N
  end

  subgraph S3[フルモード]
    J -- はい(フル) --> P[新規テーブル作成→COPY→index→ANALYZE]
    P --> Q[単一トランザクションでリネーム切替]
    Q --> R[ingestion_runs/files 記録<br/>version_date=前月1日（テーブルが存在する場合）]
  end
```

### 5.2 差分モード詳細
1. **YYMM決定**: CLI省略時は前月を自動算出
2. **差分ZIP取得**: add/delファイルをWorkDirに保存、メタ情報記録
3. **解凍**: WorkDir配下に展開
4. **着地**: `COPY FROM STDIN`で`postal_codes_landed`に投入
5. **差分適用**: 非トランザクションでupsert/delete実行
6. **メタ記録**: 実行結果を`ingestion_runs`に記録（テーブルが存在する場合）

### 5.3 フルモード詳細
1. **全件ZIP取得**: `utf_ken_all.zip`を取得・解凍
2. **新テーブル作成**: `postal_codes_new`を作成してデータ投入
3. **インデックス作成**: 複合インデックス作成→ANALYZE実行
4. **瞬時切替**: 単一トランザクションでリネーム
5. **メタ記録**: 実行結果記録（テーブルが存在する場合）

### 5.4 ワークディレクトリ構造
```
WorkDir/
├── downloads/     # ZIPファイル保存先
├── extracted/     # 解凍CSV保存先
└── logs/         # ログファイル（Serilog設定による）
```

### 5.5 COPY文の詳細設定
```sql
COPY ext.postal_codes_landed FROM STDIN
WITH (
  FORMAT CSV,
  DELIMITER ',',
  QUOTE '"',
  ESCAPE '"',
  ENCODING 'UTF8'
);
```
- **FORMAT CSV**: 日本郵便CSVに対応
- **DELIMITER ','**: カンマ区切り
- **QUOTE '"'**: ダブルクォート囲み（2～9列）
- **ESCAPE '"'**: ダブルクォートエスケープ（""で"を表現）
- **ENCODING 'UTF8'**: UTF-8エンコーディング
- **ヘッダー行なし**: 日本郵便CSVにはヘッダー行がないため

### 5.6 クリーンアップポリシー設定
```json
"CleanupPolicy": {
  "TruncateLandedAfterProcessing": true,    // 処理後にlandedテーブルをTRUNCATE
  "DeleteDownloadedFiles": false,           // ダウンロードZIPファイルを削除しない
  "DeleteExtractedFiles": true,             // 解凍CSVファイルを削除
  "KeepOldBackupTables": 3                  // 古いバックアップテーブルを3個まで保持
}
```
- **TruncateLandedAfterProcessing**: 差分・フル処理完了後の`postal_codes_landed`テーブルクリア
- **DeleteDownloadedFiles**: ZIPファイルの保持・削除設定（監査用に保持推奨）
- **DeleteExtractedFiles**: 解凍CSVの削除設定（ディスク容量節約）
- **KeepOldBackupTables**: `postal_codes_old_yyyymmdd`の保持世代数

---

## 6. 差分適用SQL実装

### 6.1 ADD処理（upsert）
PostgreSQL 15+のMERGE文を使用：
```sql
MERGE INTO ext.postal_codes AS t
USING (
  SELECT
    l.zip_code7 AS postal_code,
    l.prefecture_katakana, l.city_katakana, l.town_katakana,
    l.prefecture, l.city, l.town
  FROM ext.postal_codes_landed l
) AS s
ON (
  t.postal_code = s.postal_code
  AND t.prefecture = s.prefecture
  AND t.city = s.city
  AND t.town = s.town
)
WHEN MATCHED THEN
  UPDATE SET
    prefecture_katakana = s.prefecture_katakana,
    city_katakana = s.city_katakana,
    town_katakana = s.town_katakana
WHEN NOT MATCHED THEN
  INSERT (postal_code, prefecture_katakana, city_katakana, town_katakana, prefecture, city, town)
  VALUES (s.postal_code, s.prefecture_katakana, s.city_katakana, s.town_katakana, s.prefecture, s.city, s.town);
```

### 6.2 DEL処理（削除）
```sql
DELETE FROM ext.postal_codes t
USING (
  SELECT
    l.zip_code7 AS postal_code,
    l.prefecture, l.city, l.town
  FROM ext.postal_codes_landed l
) AS d
WHERE t.postal_code = d.postal_code
  AND t.prefecture = d.prefecture
  AND t.city = d.city
  AND t.town = d.town;
```

---

## 7. フル切替（最小ダウンタイム実装）

### 7.1 基本方針
- RENAMEは基本的に**メタデータ更新のみ**で瞬時
- 時間がかかるのは**ACCESS EXCLUSIVEロック取得待ち**
- 事前作業を完了させ、切替は**リネームのみ**に限定

### 7.2 推奨手順
```sql
-- 1. 事前準備（本番影響なし）
-- postal_codes_new作成 → COPY → インデックス作成 → ANALYZE

-- 2. 瞬時切替（単一トランザクション）
BEGIN;
SET LOCAL lock_timeout = '5s';  -- 長時間ブロック回避

ALTER TABLE ext.postal_codes RENAME TO postal_codes_old_yyyymmdd;
ALTER TABLE ext.postal_codes_new RENAME TO postal_codes;

-- 必要に応じてCOMMENT/GRANT再設定
COMMIT;
```

### 7.3 ロック待ち対策
- `lock_timeout`設定により長時間待機を回避
- 取得できない場合は中断→後刻再試行
- 失敗時は旧テーブルがそのまま残るため復旧不要

### 7.4 FK参照がある場合の注意
- RENAMEによる表OID変化がFK制約に影響
- その場合は`TRUNCATE + INSERT`方式を検討
- 本プロジェクトではextスキーマで独立運用想定

---

## 8. エラー処理と再実行設計

### 8.1 主要エラーパターンと対処
- **ダウンロード失敗**: HTTP非200/サイズ0 → ログ出力、status=Failed
- **ZIP破損/解凍失敗**: 詳細を`errors(JSONB)`に記録
- **COPY失敗**: エラー行・オフセット記録、landed TRUNCATE後再試行可
- **差分SQL失敗**: 実行SQL要約・件数・エラーをJSONB保存
- **RENAMEロック待ち**: lock_timeout到達で中断、旧本番継続

### 8.2 冪等性と再実行
- 同一YYMMの再実行は最終状態不変（MERGE upsert + delete）
- 差分とフル混在は非推奨だが、フル完了で整合確保
- メタ情報で実行履歴追跡可能

---

## 9. 性能・可観測性・メトリクス

### 9.1 性能要素
- **I/O最適化**: `COPY FROM STDIN`による高速ロード
- **判定性能**: 本番側複合インデックスでupsert/delete高速化
- **ロック最小化**: 差分は非トランザクション、フルはRENAMEのみロック

### 9.2 収集メトリクス
- ダウンロード開始・終了時間、ファイルサイズ、SHA-256
- COPY処理の行数・所要時間
- 差分処理件数（add/del別）
- フル切替のRENAME試行回数・所要時間・ロック待ち有無
- `ingestion_runs`のstatus（Succeeded/Failed）とerrors詳細

### 9.3 Serilogログ設計
- **Console/Debug/File**の3系統出力
- 全て**Async**で非同期化
- Fileは日次ローテーション、保持期間設定可能

---

## 10. テスト方針とチェックリスト

### 10.1 機能テスト
- **YYMM省略時**: 前月自動解決、version_date=前月1日
- **差分処理**:
  - addのみ → 新規挿入・既存更新確認
  - delのみ → 指定複合キー削除確認
  - add+del同月 → 期待最終状態確認
- **フル処理**:
  - `utf_ken_all.zip`全件取り込み
  - RENAME切替瞬時完了確認

### 10.2 回帰・冪等テスト
- 同一YYMM連続実行で差分なし確認
- 途中失敗→再実行で整合復旧確認

### 10.3 性能テスト
- COPY所要時間・秒間行数計測（Windows/Linux）
- 差分バッチ所要時間（add/del各ケース）
- フル切替RENAMEロック待ち発生確認

### 10.4 ログ・メタテスト
- Serilog出力（Console/Debug/File）非同期ローテーション確認
- `ingestion_runs/files`必要情報格納確認（テーブルが存在する場合）
- ingestion系テーブルなし環境での動作確認

---

## 11. 開発工数見積

| 作業項目 | 工数(h) | 備考 |
|----------|---------|------|
| 設計微修正 | 2 | |
| CLI/引数処理 (Aloe.Utils.CommandLine使用) | 2 | ライブラリ使用で簡略化 |
| YYMM既定/URLビルダ | 2 | |
| ダウンロード & SHA-256 検証 | 4 | |
| ZIP解凍 | 1 | |
| COPY実装（landed投入） | 6 | |
| 差分（upsert/delete・非Tx） | 8 | |
| 複合Index作成（本番） | 1 | |
| フル切替（新規→索引→ANALYZE→リネームTx） | 5 | |
| 実行メタ記録 | 3 | |
| Serilog設定（Async・Rolling・Console/Debug/File） | 2 | |
| 設定/接続（機密情報外出し） | 2 | |
| Win/Linux検証 | 6 | |
| 性能検証・チューニング | 4 | |
| 運用ドキュメント | 3 | |
| **合計** | **52h** | **約6.5人日（8h換算）** |

---

## 12. 運用時注意点

### 12.1 UTF-8版データ固有の特徴
- **1レコード1行**・**全角カナ**・**都道府県別分割なし**
- CSV 2～9列は引用符含む、COPY列順設定ミスに注意
- Excel閲覧時の先頭ゼロ落ち・日付化に注意
- 事業所個別番号は別系のため本CSV未含有

### 12.2 運用上の確認事項
- 直近更新日は月末～月末営業日掲載が多い
- 月初未公開は今回要件では考慮外
- 著作権：郵便番号データは自由利用・再配布可能

### 12.3 未確定事項とTODO
- ZIP/CSV保持期間（例：直近3か月）と自動削除の要否
- 本番テーブルへの外部参照（FK）有無確認
- 接続情報最終確定と権限確認（COPY/CREATE INDEX/ALTER TABLE RENAME）
- 運用ログ保管ポリシー設定

---

## 13. よくある質問（Q&A）

**Q1. RENAMEは本当に一瞬ですか？**
A. 基本はメタデータ更新のみで瞬時。時間がかかるのはロック取得待ち。`lock_timeout`を短くし、取れなければ後刻再試行が安全。

**Q2. 差分にトランザクションを使わないのは問題ありませんか？**
A. 効率優先で非トランザクション。add/upsert・del/deleteは冪等設計のため、再実行で整合回復可能。

**Q3. landedへのインデックスを貼らない理由は？**
A. 着地→即読み捌きの短命用途で、作成コストがリターンを上回る。判定側の本番テーブル複合インデックスで高速化。

**Q4. version_dateは何を入れますか？**
A. 前月の1日（例：2025/09取込 → 2025-08-01）。ダウンロード対象YYMMと整合。

---

## 14. 参考リンク

- **ダウンロード総合**: https://www.post.japanpost.jp/zipcode/download.html
- **UTF-8版ダウンロード**: https://www.post.japanpost.jp/zipcode/dl/utf-zip.html
- **UTF-8版説明**: https://www.post.japanpost.jp/zipcode/dl/utf-readme.html

---

## 15. ファイル・クラス構成

| ファイル名 | 責務 |
|------------|------|
| `Program.cs` | エントリポイント、引数解析（--full, --yymm=YYMM, --workdir）, Aloe.Utils.CommandLine使用 |
| `CliOptions.cs` | 引数検証、YYMM既定処理（未指定時はデータ存在チェックで自動判定）, Aloe.Utils.CommandLineと連動 |
| `Downloader.cs` | 公式UTF-8ページから前月差分（add/del）または最新全件取得 |
| `ZipExtractor.cs` | ZIP解凍（ワークディレクトリ配下） |
| `CopyImporter.cs` | `COPY FROM STDIN`で`ext.postal_codes_landed`投入 |
| `Differ.cs` | 差分処理（add: upsert / del: delete）非トランザクション |
| `FullSwitch.cs` | フル切替（新規表→索引→ANALYZE→リネームTx） |
| `MetaRecorder.cs` | `ext.ingestion_runs/files`記録（version_date=前月1日ほか、テーブル存在チェック付き） |

---

*このドキュメントは、doc/配下の全ファイル内容を統合・整理したものです。実装時はこの仕様書に従って開発を進めてください。*
