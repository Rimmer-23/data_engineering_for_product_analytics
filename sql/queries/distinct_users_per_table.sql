-- How many distinct users queried table X during a period?
SELECT COUNT(DISTINCT user_name) AS distinct_users
FROM gold.table_usage_daily
WHERE table_name = 'iceberg.sales.orders'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-24';
