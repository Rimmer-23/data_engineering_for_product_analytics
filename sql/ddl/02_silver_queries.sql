CREATE TABLE IF NOT EXISTS silver.queries (
  query_id STRING, query_date DATE, user_name STRING, team STRING, status STRING,
  cpu_time_ms BIGINT, peak_memory_bytes BIGINT, bytes_read BIGINT,
  tables_read ARRAY<STRING>
) USING iceberg PARTITIONED BY (query_date);
