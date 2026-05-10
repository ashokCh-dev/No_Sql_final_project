"""
Reads Pig output from HDFS for each query and inserts results into PostgreSQL.
Pig stores output as PigStorage('\t') part-* files under each query's output dir.
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import HDFS_PIG_OUTPUT_DIR, JAVA_HOME
from db.connection import fetch_run_meta


def _read_hdfs_output(run_id: int, query_name: str) -> list[str]:
    path = f'{HDFS_PIG_OUTPUT_DIR}/run_{run_id}/{query_name}/part-*'
    env = {**os.environ, 'JAVA_HOME': JAVA_HOME}
    result = subprocess.run(
        ['hdfs', 'dfs', '-cat', path],
        capture_output=True, text=True, check=True, env=env
    )
    return [l for l in result.stdout.splitlines() if l.strip()]


def load_q1(conn, run_id: int) -> None:
    pipeline_name, started_at = fetch_run_meta(conn, run_id)
    lines = _read_hdfs_output(run_id, 'q1')
    rows = []
    for line in lines:
        parts = line.split('\t')
        if len(parts) != 4:
            continue
        log_date, status_code, req_count, total_bytes = parts
        rows.append((run_id, pipeline_name, started_at, log_date, int(status_code), int(req_count), int(total_bytes)))

    rows.sort(key=lambda r: (r[3], r[4]))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q1_daily_traffic
                (run_id, pipeline_name, execution_time, log_date, status_code, request_count, total_bytes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows
        )
    conn.commit()
    print(f"  Q1: inserted {len(rows)} rows")


def load_q2(conn, run_id: int) -> None:
    pipeline_name, started_at = fetch_run_meta(conn, run_id)
    lines = _read_hdfs_output(run_id, 'q2')
    rows = []
    for rank, line in enumerate(lines, start=1):
        parts = line.split('\t')
        if len(parts) != 4:
            continue
        path, req_count, total_bytes, distinct_hosts = parts
        rows.append((
            run_id, pipeline_name, started_at, rank, path,
            int(req_count), int(total_bytes), int(distinct_hosts),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q2_top_resources
                (run_id, pipeline_name, execution_time, rank, resource_path, request_count, total_bytes, distinct_host_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows
        )
    conn.commit()
    print(f"  Q2: inserted {len(rows)} rows")


def load_q3(conn, run_id: int) -> None:
    pipeline_name, started_at = fetch_run_meta(conn, run_id)
    lines = _read_hdfs_output(run_id, 'q3')
    rows = []
    for line in lines:
        parts = line.split('\t')
        if len(parts) != 6:
            continue
        log_date, log_hour, err_count, total_count, err_rate, dist_hosts = parts
        rows.append((
            run_id,
            pipeline_name,
            started_at,
            log_date,
            int(log_hour),
            int(err_count),
            int(total_count),
            round(float(err_rate), 4),
            int(dist_hosts),
        ))

    rows.sort(key=lambda r: (r[3], r[4]))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q3_hourly_errors
                (run_id, pipeline_name, execution_time, log_date, log_hour, error_request_count,
                 total_request_count, error_rate, distinct_error_hosts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows
        )
    conn.commit()
    print(f"  Q3: inserted {len(rows)} rows")
