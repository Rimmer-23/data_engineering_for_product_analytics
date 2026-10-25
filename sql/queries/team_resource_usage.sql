-- How many resources did team Y consume during a period?
SELECT team,
       SUM(query_count)    AS queries,
       SUM(failed_queries) AS failed_queries,
       SUM(cpu_hours)      AS cpu_hours,
       SUM(gb_read)        AS gb_read,
       SUM(memory_gb)      AS memory_gb
FROM gold.team_usage_daily
WHERE team = 'analytics'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-24'
GROUP BY team;
