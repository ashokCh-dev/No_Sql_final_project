-- Add pipeline_name + execution_time columns to all three result tables.
-- Backfills existing rows from pipeline_runs. Idempotent (IF NOT EXISTS).

ALTER TABLE q1_daily_traffic  ADD COLUMN IF NOT EXISTS pipeline_name VARCHAR(50);
ALTER TABLE q1_daily_traffic  ADD COLUMN IF NOT EXISTS execution_time TIMESTAMPTZ;
ALTER TABLE q2_top_resources  ADD COLUMN IF NOT EXISTS pipeline_name VARCHAR(50);
ALTER TABLE q2_top_resources  ADD COLUMN IF NOT EXISTS execution_time TIMESTAMPTZ;
ALTER TABLE q3_hourly_errors  ADD COLUMN IF NOT EXISTS pipeline_name VARCHAR(50);
ALTER TABLE q3_hourly_errors  ADD COLUMN IF NOT EXISTS execution_time TIMESTAMPTZ;

UPDATE q1_daily_traffic q
   SET pipeline_name = pr.pipeline_name,
       execution_time = pr.started_at
  FROM pipeline_runs pr
 WHERE q.run_id = pr.run_id
   AND q.pipeline_name IS NULL;

UPDATE q2_top_resources q
   SET pipeline_name = pr.pipeline_name,
       execution_time = pr.started_at
  FROM pipeline_runs pr
 WHERE q.run_id = pr.run_id
   AND q.pipeline_name IS NULL;

UPDATE q3_hourly_errors q
   SET pipeline_name = pr.pipeline_name,
       execution_time = pr.started_at
  FROM pipeline_runs pr
 WHERE q.run_id = pr.run_id
   AND q.pipeline_name IS NULL;
