-- Q2 Top 20 Requested Resources
-- Input:  staged TSV (8 fields per record) on HDFS
-- Output: resource_path \t request_count \t total_bytes \t distinct_host_count
--         (top 20 by request_count, already sorted desc)
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

by_path = GROUP records BY path;

agg = FOREACH by_path {
    hosts = DISTINCT records.host;
    GENERATE
        group                          AS resource_path,
        COUNT(records)                 AS request_count,
        SUM(records.bytes_transferred) AS total_bytes,
        COUNT(hosts)                   AS distinct_host_count;
};

ordered = ORDER agg BY request_count DESC;
top20   = LIMIT ordered 20;

STORE top20 INTO '$output_dir' USING PigStorage('\t');
