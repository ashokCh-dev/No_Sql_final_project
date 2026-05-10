-- Q3: Hourly Error Analysis
-- Params: ${input_dir}, ${output_base}

SET hive.exec.mode.local.auto=false;
SET hive.cli.print.header=false;
SET mapreduce.job.reduces=1;

DROP TABLE IF EXISTS staged_logs;
CREATE EXTERNAL TABLE staged_logs (
    host              STRING,
    log_date          STRING,
    log_hour          INT,
    http_method       STRING,
    resource_path     STRING,
    protocol_version  STRING,
    status_code       INT,
    bytes_transferred BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${input_dir}';

INSERT OVERWRITE DIRECTORY '${output_base}/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
    COUNT(*)                                                         AS total_request_count,
    CAST(SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts
FROM staged_logs
GROUP BY log_date, log_hour;

DROP TABLE staged_logs;
