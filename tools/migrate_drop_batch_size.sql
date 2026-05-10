-- File-as-batch refactor: pipeline_runs.batch_size becomes redundant with avg_batch_size.
-- Per-batch sizes live in batch_log.records_in_batch; aggregate is pipeline_runs.avg_batch_size.

ALTER TABLE pipeline_runs DROP COLUMN IF EXISTS batch_size;
