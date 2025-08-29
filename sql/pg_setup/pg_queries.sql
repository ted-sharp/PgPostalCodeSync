--==============================================
-- PostgreSQL 基本情報の確認
--==============================================

-- PostgreSQL バージョンの確認
SELECT version();

-- 設定パラメータ一覧の確認
SELECT name, setting, unit, context
FROM pg_settings
ORDER BY name;

-- 不要データベースのDROP文作成
SELECT 'DROP DATABASE ' || datname || ';' as command FROM pg_database WHERE datname LIKE 'aloe%';

--==============================================
-- データベースおよびテーブルサイズ関連
--==============================================

-- 現在のデータベースサイズを確認
SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size;

-- 各テーブルのサイズを確認
SELECT
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
  pg_size_pretty(pg_relation_size(relid)) AS table_size,
  pg_size_pretty(pg_indexes_size(relid)) AS indexes_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

-- 各インデックスのサイズを確認
SELECT
  relname AS table_name,
  indexrelname AS index_name,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;

--==============================================
-- インデックス使用状況の確認
--==============================================

-- テーブルごとのインデックス使用率を確認
SELECT
  relname AS table_name,
  seq_scan,
  idx_scan,
  CASE (seq_scan + idx_scan)
    WHEN 0 THEN 0
    ELSE ROUND(100.0 * idx_scan / (seq_scan + idx_scan), 2)
  END AS index_usage_percent
FROM pg_stat_user_tables
ORDER BY index_usage_percent;

-- 各インデックスの使用状況を確認
SELECT
  schemaname,
  relname AS table_name,
  indexrelname AS index_name,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY table_name, index_name;

--==============================================
-- VACUUM/ANALYZE 関連の確認
--==============================================

-- VACUUMやANALYZEの最終実行日時
SELECT
  relname,
  last_vacuum,
  last_autovacuum,
  last_analyze,
  last_autoanalyze
FROM pg_stat_user_tables;

-- 不要領域（dead tuple）の割合を確認
SELECT
  relname,
  n_live_tup,
  n_dead_tup,
  CASE
    WHEN (n_live_tup + n_dead_tup) = 0 THEN 0
    ELSE ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
  END AS dead_tuple_ratio
FROM pg_stat_user_tables
ORDER BY dead_tuple_ratio DESC;

--==============================================
-- 接続とトランザクション状況の確認
--==============================================

-- 接続数上限と現在の接続数を確認
SELECT
  current_setting('max_connections') AS max_connections,
  COUNT(*) AS current_connections
FROM pg_stat_activity;

-- 長時間実行中のクエリを確認
SELECT
  pid,
  age(clock_timestamp(), query_start) AS runtime,
  query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY runtime DESC;

-- デッドロックや待機状態にあるクエリを確認
SELECT
  pid,
  wait_event_type,
  wait_event,
  query
FROM pg_stat_activity
WHERE wait_event IS NOT NULL;

-- テーブルロックの状況を確認
SELECT
  locktype,
  relation::regclass,
  mode,
  pid,
  granted
FROM pg_locks
WHERE relation IS NOT NULL;

-- 長期間コミットされていないトランザクションを確認
SELECT
  pid,
  age(xact_start) AS duration,
  query
FROM pg_stat_activity
WHERE state = 'idle in transaction'
ORDER BY duration DESC;
