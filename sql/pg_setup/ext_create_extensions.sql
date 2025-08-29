
-- PostgreSQL拡張機能の有効化スクリプト

-- postgresql.conf に以下を追加して再起動
-- shared_preload_libraries = 'pg_stat_statements'
SHOW shared_preload_libraries;

-- DDLログを記録(CREATE TABLEなどを監視)
ALTER SYSTEM SET log_statement = 'ddl';

-- 遅いクエリを記録
ALTER SYSTEM SET log_min_duration_statement  = '500';

-- 設定を有効化
SELECT pg_reload_conf();

-- 拡張: クエリ統計取得（性能分析）
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- 明示的にリセット(通常はPostgreSQL再起動でリセット)
SELECT pg_stat_statements_reset();

-- 拡張: アクセント記号削除用
CREATE EXTENSION IF NOT EXISTS "unaccent";

-- 拡張: UUID生成
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 拡張: ベクター検索に必要
--CREATE EXTENSION IF NOT EXISTS pgvector;
CREATE EXTENSION IF NOT EXISTS vector;

-- 拡張の一覧
SELECT
 extname
 , extrelocatable
 , extversion
FROM pg_extension
;
