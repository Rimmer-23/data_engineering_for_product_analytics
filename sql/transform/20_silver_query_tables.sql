-- One row per query x table. DISTINCT: a table read twice in one query counts once.
INSERT OVERWRITE silver.query_tables
SELECT DISTINCT query_id, query_date, user_name, team, table_name
FROM (
  SELECT query_id, query_date, user_name, team, explode(tables_read) AS table_name
  FROM silver.queries
  WHERE query_date >= DATE '${start_date}'
);
