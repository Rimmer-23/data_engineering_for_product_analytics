-- One row per query: deduplicate Bronze events (latest ingested wins)
-- and attach the team the user belonged to when the query started.
MERGE INTO silver.queries t
USING (
  SELECT query_id, query_date, user_name, team, status,
         cpu_time_ms, peak_memory_bytes, bytes_read, tables_read
  FROM (
    SELECT e.query_id,
           CAST(e.end_time AS DATE)    AS query_date,
           e.user_name,
           COALESCE(u.team, 'UNKNOWN') AS team,
           e.query_state               AS status,
           e.cpu_time_ms, e.peak_memory_bytes, e.bytes_read, e.tables_read,
           ROW_NUMBER() OVER (PARTITION BY e.query_id ORDER BY e.ingested_at DESC) AS rn
    FROM bronze.trino_query_events e
    LEFT JOIN ref.user_teams u
      ON e.user_name = u.user_name
     AND e.start_time >= u.valid_from AND e.start_time < u.valid_to
    WHERE CAST(e.end_time AS DATE) >= DATE '${start_date}'
  ) WHERE rn = 1
) s
ON t.query_id = s.query_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
