-- Day x team. Built from silver.queries, so resources are counted once per query.
INSERT OVERWRITE gold.team_usage_daily
SELECT query_date AS usage_date,
       team,
       COUNT(*)                                           AS query_count,
       SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
       SUM(cpu_time_ms) / 3600000.0                       AS cpu_hours,
       SUM(bytes_read) / POW(1024, 3)                     AS gb_read,
       SUM(peak_memory_bytes) / POW(1024, 3)              AS memory_gb
FROM silver.queries
WHERE query_date >= DATE '${start_date}'
GROUP BY query_date, team;
