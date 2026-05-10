-- Q1 Daily Traffic Summary
-- Input:  staged TSV (8 fields per record) on HDFS
-- Output: log_date \t status_code \t request_count \t total_bytes
-- Params: input_dir, output_dir

records = LOAD '$input_dir' USING PigStorage('\t') AS (
    host:chararray,
    log_date:chararray,
    log_hour:int,
    method:chararray,
    path:chararray,
    proto:chararray,
    status_code:int,
    bytes_transferred:long
);

grouped = GROUP records BY (log_date, status_code);

agg = FOREACH grouped GENERATE
    group.log_date     AS log_date,
    group.status_code  AS status_code,
    COUNT(records)     AS request_count,
    SUM(records.bytes_transferred) AS total_bytes;

STORE agg INTO '$output_dir' USING PigStorage('\t');
