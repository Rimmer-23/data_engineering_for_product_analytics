CREATE TABLE IF NOT EXISTS gold.table_usage_daily (
  usage_date DATE, table_name STRING, user_name STRING, team STRING, query_count BIGINT
) USING iceberg PARTITIONED BY (usage_date);
