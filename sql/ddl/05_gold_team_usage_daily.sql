CREATE TABLE IF NOT EXISTS gold.team_usage_daily (
  usage_date DATE, team STRING, query_count BIGINT, failed_queries BIGINT,
  cpu_hours DOUBLE, gb_read DOUBLE, memory_gb DOUBLE
) USING iceberg PARTITIONED BY (usage_date);
