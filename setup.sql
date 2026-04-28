CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(50)   NOT NULL,
    input_file      TEXT          NOT NULL,
    started_at      TIMESTAMPTZ   NOT NULL,
    finished_at     TIMESTAMPTZ,
    runtime_seconds NUMERIC(10,3),
    batch_size      INTEGER       NOT NULL,
    num_batches     INTEGER,
    total_records   BIGINT,
    malformed_count BIGINT,
    avg_batch_size  NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS batch_log (
    id                  SERIAL  PRIMARY KEY,
    run_id              INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    batch_id            INTEGER NOT NULL,
    records_in_batch    INTEGER NOT NULL,
    malformed_in_batch  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id            SERIAL   PRIMARY KEY,
    run_id        INTEGER  NOT NULL REFERENCES pipeline_runs(run_id),
    log_date      DATE     NOT NULL,
    status_code   SMALLINT NOT NULL,
    request_count BIGINT   NOT NULL,
    total_bytes   BIGINT   NOT NULL
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  SERIAL   PRIMARY KEY,
    run_id              INTEGER  NOT NULL REFERENCES pipeline_runs(run_id),
    rank                SMALLINT NOT NULL,
    resource_path       TEXT     NOT NULL,
    request_count       BIGINT   NOT NULL,
    total_bytes         BIGINT   NOT NULL,
    distinct_host_count INTEGER  NOT NULL
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   SERIAL       PRIMARY KEY,
    run_id               INTEGER      NOT NULL REFERENCES pipeline_runs(run_id),
    log_date             DATE         NOT NULL,
    log_hour             SMALLINT     NOT NULL,
    error_request_count  BIGINT       NOT NULL,
    total_request_count  BIGINT       NOT NULL,
    error_rate           NUMERIC(7,4) NOT NULL,
    distinct_error_hosts INTEGER      NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_q1_run ON q1_daily_traffic(run_id);
CREATE INDEX IF NOT EXISTS idx_q2_run ON q2_top_resources(run_id);
CREATE INDEX IF NOT EXISTS idx_q3_run ON q3_hourly_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_log(run_id);
