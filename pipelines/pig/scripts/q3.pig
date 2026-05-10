-- Q3 Hourly Error Analysis
-- Input:  staged TSV (8 fields per record) on HDFS
-- Output: log_date \t log_hour \t error_request_count \t total_request_count \t error_rate \t distinct_error_hosts
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

tagged = FOREACH records GENERATE
    host,
    log_date,
    log_hour,
    status_code,
    ((status_code >= 400 AND status_code <= 599) ? 1 : 0) AS is_error:int;

grouped = GROUP tagged BY (log_date, log_hour);

agg = FOREACH grouped {
    error_recs = FILTER tagged BY is_error == 1;
    err_hosts  = DISTINCT error_recs.host;
    GENERATE
        group.log_date AS log_date,
        group.log_hour AS log_hour,
        SUM(tagged.is_error) AS error_request_count,
        COUNT(tagged)        AS total_request_count,
        ((double) SUM(tagged.is_error) / (double) COUNT(tagged)) AS error_rate,
        COUNT(err_hosts)     AS distinct_error_hosts;
};

STORE agg INTO '$output_dir' USING PigStorage('\t');
