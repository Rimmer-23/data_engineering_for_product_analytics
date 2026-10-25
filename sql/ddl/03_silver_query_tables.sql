CREATE TABLE IF NOT EXISTS silver.query_tables (
  query_id STRING, query_date DATE, user_name STRING, team STRING, table_name STRING
) USING iceberg PARTITIONED BY (query_date);
