-- Postal Code Database Indexes
-- Project: PgPostalCodeSync

-- Drop existing indexes if they exist
DROP INDEX IF EXISTS ext.postal_codes_postal_code_idx;
DROP INDEX IF EXISTS ext.postal_codes_prefecture_city_idx;
DROP INDEX IF EXISTS ext.postal_codes_run_id_idx;
DROP INDEX IF EXISTS ext.ingestion_runs_source_system_idx;
DROP INDEX IF EXISTS ext.ingestion_runs_status_idx;
DROP INDEX IF EXISTS ext.ingestion_files_run_id_idx;

-- Create indexes for postal_codes table
CREATE INDEX postal_codes_postal_code_idx
  ON ext.postal_codes(postal_code);

CREATE INDEX postal_codes_prefecture_city_idx
  ON ext.postal_codes(prefecture, city);

CREATE INDEX postal_codes_run_id_idx
  ON ext.postal_codes(run_id);

-- Note: postal_codes_landed is a temporary staging table for data ingestion
-- No indexes needed as data is frequently truncated and reloaded

-- Create indexes for ingestion_runs table
CREATE INDEX ingestion_runs_source_system_idx
  ON ext.ingestion_runs(source_system);

CREATE INDEX ingestion_runs_status_idx
  ON ext.ingestion_runs(status);

CREATE INDEX ingestion_runs_started_at_idx
  ON ext.ingestion_runs(started_at);

-- Create indexes for ingestion_files table
CREATE INDEX ingestion_files_run_id_idx
  ON ext.ingestion_files(run_id);

CREATE INDEX ingestion_files_file_name_idx
  ON ext.ingestion_files(file_name);

-- Add foreign key indexes for performance
-- These indexes support JOIN operations between related tables
