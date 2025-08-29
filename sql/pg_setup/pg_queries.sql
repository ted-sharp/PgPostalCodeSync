--==============================================
-- PostgreSQL ��{���̊m�F
--==============================================

-- PostgreSQL �o�[�W�����̊m�F
SELECT version();

-- �ݒ�p�����[�^�ꗗ�̊m�F
SELECT name, setting, unit, context
FROM pg_settings
ORDER BY name;

-- �s�v�f�[�^�x�[�X��DROP���쐬
SELECT 'DROP DATABASE ' || datname || ';' as command FROM pg_database WHERE datname LIKE 'aloe%';

--==============================================
-- �f�[�^�x�[�X����уe�[�u���T�C�Y�֘A
--==============================================

-- ���݂̃f�[�^�x�[�X�T�C�Y���m�F
SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size;

-- �e�e�[�u���̃T�C�Y���m�F
SELECT
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
  pg_size_pretty(pg_relation_size(relid)) AS table_size,
  pg_size_pretty(pg_indexes_size(relid)) AS indexes_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

-- �e�C���f�b�N�X�̃T�C�Y���m�F
SELECT
  relname AS table_name,
  indexrelname AS index_name,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;

--==============================================
-- �C���f�b�N�X�g�p�󋵂̊m�F
--==============================================

-- �e�[�u�����Ƃ̃C���f�b�N�X�g�p�����m�F
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

-- �e�C���f�b�N�X�̎g�p�󋵂��m�F
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
-- VACUUM/ANALYZE �֘A�̊m�F
--==============================================

-- VACUUM��ANALYZE�̍ŏI���s����
SELECT
  relname,
  last_vacuum,
  last_autovacuum,
  last_analyze,
  last_autoanalyze
FROM pg_stat_user_tables;

-- �s�v�̈�idead tuple�j�̊������m�F
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
-- �ڑ��ƃg�����U�N�V�����󋵂̊m�F
--==============================================

-- �ڑ�������ƌ��݂̐ڑ������m�F
SELECT
  current_setting('max_connections') AS max_connections,
  COUNT(*) AS current_connections
FROM pg_stat_activity;

-- �����Ԏ��s���̃N�G�����m�F
SELECT
  pid,
  age(clock_timestamp(), query_start) AS runtime,
  query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY runtime DESC;

-- �f�b�h���b�N��ҋ@��Ԃɂ���N�G�����m�F
SELECT
  pid,
  wait_event_type,
  wait_event,
  query
FROM pg_stat_activity
WHERE wait_event IS NOT NULL;

-- �e�[�u�����b�N�̏󋵂��m�F
SELECT
  locktype,
  relation::regclass,
  mode,
  pid,
  granted
FROM pg_locks
WHERE relation IS NOT NULL;

-- �����ԃR�~�b�g����Ă��Ȃ��g�����U�N�V�������m�F
SELECT
  pid,
  age(xact_start) AS duration,
  query
FROM pg_stat_activity
WHERE state = 'idle in transaction'
ORDER BY duration DESC;
