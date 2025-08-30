# PgPostalCodeSync 最終要件書

**作成日**: 2025-08-30  
**バージョン**: 1.0.0  
**プロジェクト**: 日本郵便局郵便番号データ PostgreSQL 同期コンソールアプリケーション

## 1. プロジェクト概要

### 1.1 目的
日本郵便局が提供する郵便番号データ（UTF-8 CSV）をPostgreSQLデータベースに同期するコンソールアプリケーションを開発する。

### 1.2 技術仕様
- **言語**: C# 13
- **フレームワーク**: .NET 9 Console Application
- **データベース**: PostgreSQL
- **実行形態**: 月次バッチ処理（外部スケジューラ連携）

## 2. 機能要件（EARS形式）

### 2.1 コア機能
| ID | 要件 |
|----|------|
| FR-001 | システムは、日本郵便局サイトから utf_ken_all.zip ファイルをダウンロードできなければならない |
| FR-002 | システムは、ダウンロードした ZIP ファイルを解凍し CSV データを読み込めなければならない |
| FR-003 | システムは、CSV データを PostgreSQL の ext.postal_codes テーブルに投入できなければならない |
| FR-004 | システムは、月次差分ファイル（add_YYMM.zip, del_YYMM.zip）を処理できなければならない |
| FR-005 | システムは、コマンドライン引数 --full でフル取り込みを強制実行できなければならない |
| FR-006 | システムは、処理状況をログ出力できなければならない |
| FR-007 | システムは、処理失敗時に適切な終了コード（非0）を返さなければならない |

### 2.2 品質要件
| ID | 要件 |
|----|------|
| QR-001 | システムは、COPY FROM STDIN を使用してデータベース投入を最適化しなければならない |
| QR-002 | システムは、フル取り込み時にテーブルリネーム方式でゼロダウンタイム更新を実現しなければならない |
| QR-003 | システムは、処理失敗時に一時ファイルをクリーンアップしなければならない |

## 3. 処理フロー

### 3.1 差分取り込みフロー
```
開始
 ↓
コマンドライン引数解析
 ↓
既存データ確認（データなし→フル取り込みへ）
 ↓
前月YYMM算出
 ↓
差分ZIPダウンロード（add_YYMM.zip, del_YYMM.zip）
 ↓
ZIP解凍・CSV読み込み
 ↓
ext.postal_codes_landed 投入（COPY FROM STDIN）
 ↓
差分適用（UPSERT/DELETE）
 ↓
メタ情報記録
 ↓
クリーンアップ
 ↓
完了
```

### 3.2 フル取り込みフロー
```
開始
 ↓
全件ZIPダウンロード（utf_ken_all.zip）
 ↓
ZIP解凍・CSV読み込み
 ↓
ext.postal_codes_new 作成・投入（COPY FROM STDIN）
 ↓
インデックス作成・ANALYZE
 ↓
テーブルリネーム
  postal_codes → postal_codes_YYYYMMDD_HHMMSS
  postal_codes_new → postal_codes
 ↓
メタ情報記録
 ↓
クリーンアップ
 ↓
完了
```

## 4. アーキテクチャ設計

### 4.1 サービス層構成
```
Program.cs (エントリポイント)
├── PostalCodeSyncService (メイン制御)
│   ├── DownloadService (ファイル取得)
│   └── DatabaseService (DB操作)
└── AppSettings (設定管理)
```

### 4.2 技術スタック
- **.NET 9 汎用ホスト** (Host.CreateDefaultBuilder())
- **依存性注入** (Microsoft.Extensions.DependencyInjection)
- **構造化ログ** (Serilog)
- **PostgreSQL最適化** (COPY FROM STDIN, 準備文使用)

### 4.3 データベース設計
既存SQLスキーマ活用:
- **ext.postal_codes_landed**: CSV着地用（一時テーブル）
- **ext.postal_codes**: 本番データテーブル
- **ext.ingestion_runs**: 実行メタ管理
- **ext.ingestion_files**: ファイルメタ管理

論理キー: `(postal_code, prefecture, city, town)`

## 5. 開発計画

### 5.1 工数見積: **7.5人日**
| フェーズ | 内容 | 工数 |
|----------|------|------|
| Phase1 | 基本機能実装 | 5.0人日 |
| Phase2 | テスト・品質確保 | 1.5人日 |
| Phase3 | ドキュメント・仕上げ | 1.0人日 |

### 5.2 段階的リリース方針
1. **Phase1**: 差分・フル取り込み機能、基本エラーハンドリング
2. **Phase2**: 単体・結合テスト、運用テスト
3. **Phase3**: 監視・運用対応、ドキュメント整備

## 6. 運用要件

### 6.1 実行環境
- **スケジュール**: 月次実行（月初指定日時）
- **実行方法**: 外部スケジューラ（Windows Task Scheduler/cron）
- **作業ディレクトリ**: 設定可能（一時ファイル保存用）

### 6.2 コマンドライン引数
- `--full`: 強制フル取り込み
- `--yymm=2508`: 指定年月処理
- `--workdir="C:\temp"`: 作業ディレクトリ指定

### 6.3 設定管理
```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Database=postal;Username=postgres;Password=***"
  },
  "PostalCodeSync": {
    "WorkDirectory": "C:\\temp\\postal-sync",
    "BaseUrl": "https://www.post.japanpost.jp/zipcode/dl/utf-zip.html",
    "CleanupTempFiles": true
  }
}
```

## 7. 既存パッケージ活用
- **Aloe.Utils.CommandLine** (1.0.4) - 引数解析
- **Npgsql** (9.0.3) - PostgreSQL接続
- **Microsoft.Extensions.Hosting** (9.0.8) - 汎用ホスト
- **Serilog.Extensions.Hosting** (9.0.0) - ログ

## 8. 未確定事項（要確認）

### 8.1 運用・環境関連
1. **本番データベース接続情報**
2. **実行スケジュール詳細**
3. **作業ディレクトリパス・権限**

### 8.2 エラー処理・監視
4. **エラー通知方法**
5. **データ保持ポリシー**

### 8.3 初回データ投入
6. **既存データの扱い**
7. **性能要件**

---

**承認者**: ________________  
**承認日**: ________________

*本要件書は、ステージ1「要件定義」、ステージ2「詳細設計方針」、ステージ3「レビュー＆ブラッシュアップ」の成果物を統合して作成されました。*