-- Weekly trend of users and queries for one table (historical perspective).
SELECT date_trunc('week', usage_date) AS week,
       COUNT(DISTINCT user_name)      AS users,
       SUM(query_count)               AS queries
FROM gold.table_usage_daily
WHERE table_name = 'iceberg.sales.orders'
GROUP BY 1
ORDER BY 1;
