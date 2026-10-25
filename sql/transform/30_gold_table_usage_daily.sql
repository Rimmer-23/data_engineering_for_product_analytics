-- Day x table x user. The user is kept so distinct users stay exact for any period.
INSERT OVERWRITE gold.table_usage_daily
SELECT query_date AS usage_date, table_name, user_name, team, COUNT(*) AS query_count
FROM silver.query_tables
WHERE query_date >= DATE '${start_date}'
GROUP BY query_date, table_name, user_name, team;
