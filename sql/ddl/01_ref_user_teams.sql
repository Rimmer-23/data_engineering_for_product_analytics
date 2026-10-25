CREATE TABLE IF NOT EXISTS ref.user_teams (
  user_name STRING, team STRING,
  valid_from TIMESTAMP, valid_to TIMESTAMP   -- 9999-12-31 for the current row
) USING iceberg;
