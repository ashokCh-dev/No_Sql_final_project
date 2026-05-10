-- Q2: Top 20 Requested Resources
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

INSERT OVERWRITE DIRECTORY '${output_base}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    resource_path,
    COUNT(*)                 AS request_count,
    SUM(bytes_transferred)   AS total_bytes,
    COUNT(DISTINCT host)     AS distinct_host_count
FROM staged_logs
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;

DROP TABLE staged_logs;
