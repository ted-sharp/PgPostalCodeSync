-- Postal Code Sync for PostgreSQL - テーブル作成スクリプト
-- 実行前に ext スキーマを作成してください

-- ext スキーマの作成（存在しない場合）
CREATE SCHEMA IF NOT EXISTS ext;

-- メタ情報管理テーブル
CREATE TABLE IF NOT EXISTS ext.ingestion_runs (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL CHECK (status IN ('InProgress', 'Succeeded', 'Failed')),
    mode VARCHAR(20) NOT NULL CHECK (mode IN ('Full', 'Diff')),
    yymm VARCHAR(4),
    version_date DATE NOT NULL,
    total_records INTEGER,
    added_records INTEGER,
    updated_records INTEGER,
    deleted_records INTEGER,
    errors JSONB,
    summary TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ext.ingestion_files (
    id SERIAL PRIMARY KEY,
    ingestion_run_id INTEGER NOT NULL REFERENCES ext.ingestion_runs(id) ON DELETE CASCADE,
    file_name VARCHAR(255) NOT NULL,
    file_type VARCHAR(20) NOT NULL CHECK (file_type IN ('Full', 'Add', 'Del')),
    file_path TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    sha256_hash VARCHAR(64),
    downloaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE,
    errors TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 着地テーブル（一時データ用）
CREATE TABLE IF NOT EXISTS ext.postal_codes_landed (
    id SERIAL PRIMARY KEY,
    local_government_code VARCHAR(6) NOT NULL,
    old_zip_code5 VARCHAR(5) NOT NULL,
    zip_code7 VARCHAR(7) NOT NULL,
    prefecture_katakana VARCHAR(50) NOT NULL,
    city_katakana VARCHAR(50) NOT NULL,
    town_katakana VARCHAR(50) NOT NULL,
    prefecture VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    town VARCHAR(50) NOT NULL,
    is_multi_zip BOOLEAN NOT NULL,
    is_koaza BOOLEAN NOT NULL,
    is_chome BOOLEAN NOT NULL,
    is_multi_town BOOLEAN NOT NULL,
    update_status INTEGER NOT NULL,
    update_reason INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 本番テーブル（郵便番号データ）
CREATE TABLE IF NOT EXISTS ext.postal_codes (
    id SERIAL PRIMARY KEY,
    postal_code VARCHAR(7) NOT NULL,
    local_government_code VARCHAR(6) NOT NULL,
    old_zip_code5 VARCHAR(5) NOT NULL,
    prefecture_katakana VARCHAR(50) NOT NULL,
    city_katakana VARCHAR(50) NOT NULL,
    town_katakana VARCHAR(50) NOT NULL,
    prefecture VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    town VARCHAR(50) NOT NULL,
    is_multi_zip BOOLEAN NOT NULL,
    is_koaza BOOLEAN NOT NULL,
    is_chome BOOLEAN NOT NULL,
    is_multi_town BOOLEAN NOT NULL,
    update_status INTEGER NOT NULL,
    update_reason INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- インデックスの作成
CREATE INDEX IF NOT EXISTS ix_postal_codes_comp
    ON ext.postal_codes (postal_code, prefecture, city, town);

CREATE INDEX IF NOT EXISTS ix_postal_codes_prefecture
    ON ext.postal_codes (prefecture);

CREATE INDEX IF NOT EXISTS ix_postal_codes_city
    ON ext.postal_codes (city);

CREATE INDEX IF NOT EXISTS ix_postal_codes_zip
    ON ext.postal_codes (postal_code);

-- メタ情報テーブルのインデックス
CREATE INDEX IF NOT EXISTS ix_ingestion_runs_status
    ON ext.ingestion_runs (status);

CREATE INDEX IF NOT EXISTS ix_ingestion_runs_mode
    ON ext.ingestion_runs (mode);

CREATE INDEX IF NOT EXISTS ix_ingestion_runs_version_date
    ON ext.ingestion_runs (version_date);

CREATE INDEX IF NOT EXISTS ix_ingestion_files_run_id
    ON ext.ingestion_files (ingestion_run_id);

CREATE INDEX IF NOT EXISTS ix_ingestion_files_type
    ON ext.ingestion_files (file_type);

-- コメントの追加
COMMENT ON TABLE ext.ingestion_runs IS '郵便番号データ取り込み実行履歴';
COMMENT ON TABLE ext.ingestion_files IS '取り込みファイルメタ情報';
COMMENT ON TABLE ext.postal_codes_landed IS '郵便番号データ着地テーブル（一時データ用）';
COMMENT ON TABLE ext.postal_codes IS '郵便番号データ本番テーブル';

COMMENT ON COLUMN ext.ingestion_runs.status IS '実行状態: InProgress=実行中, Succeeded=成功, Failed=失敗';
COMMENT ON COLUMN ext.ingestion_runs.mode IS '実行モード: Full=フル取り込み, Diff=差分取り込み';
COMMENT ON COLUMN ext.ingestion_runs.yymm IS '対象年月（YYMM形式）';
COMMENT ON COLUMN ext.ingestion_runs.version_date IS 'データバージョン日付（前月1日）';

COMMENT ON COLUMN ext.ingestion_files.file_type IS 'ファイル種別: Full=全件, Add=追加, Del=削除';

COMMENT ON COLUMN ext.postal_codes.postal_code IS '郵便番号（7桁）';
COMMENT ON COLUMN ext.postal_codes.local_government_code IS '全国地方公共団体コード';
COMMENT ON COLUMN ext.postal_codes.old_zip_code5 IS '旧郵便番号（5桁）';
COMMENT ON COLUMN ext.postal_codes.prefecture_katakana IS '都道府県名（カナ）';
COMMENT ON COLUMN ext.postal_codes.city_katakana IS '市区町村名（カナ）';
COMMENT ON COLUMN ext.postal_codes.town_katakana IS '町域名（カナ）';
COMMENT ON COLUMN ext.postal_codes.prefecture IS '都道府県名（漢字）';
COMMENT ON COLUMN ext.postal_codes.city IS '市区町村名（漢字）';
COMMENT ON COLUMN ext.postal_codes.town IS '町域名（漢字）';
COMMENT ON COLUMN ext.postal_codes.is_multi_zip IS '一町域が複数郵便番号';
COMMENT ON COLUMN ext.postal_codes.is_koaza IS '小字毎に番地が起番';
COMMENT ON COLUMN ext.postal_codes.is_chome IS '丁目を有する';
COMMENT ON COLUMN ext.postal_codes.is_multi_town IS '1郵便番号で複数町域';
COMMENT ON COLUMN ext.postal_codes.update_status IS '更新フラグ: 0=変更なし, 1=変更あり, 2=廃止';
COMMENT ON COLUMN ext.postal_codes.update_reason IS '変更理由: 0～6';

-- 権限の設定（必要に応じて調整）
-- GRANT ALL PRIVILEGES ON SCHEMA ext TO your_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ext TO your_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ext TO your_user;
