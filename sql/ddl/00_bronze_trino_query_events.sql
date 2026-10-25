-- Only for the local demo: in production this raw table already exists.
CREATE TABLE IF NOT EXISTS bronze.trino_query_events (
  query_id STRING, user_name STRING, query_state STRING,
  start_time TIMESTAMP, end_time TIMESTAMP,
  cpu_time_ms BIGINT, peak_memory_bytes BIGINT, bytes_read BIGINT,
  tables_read ARRAY<STRING>, ingested_at TIMESTAMP
) USING iceberg;
