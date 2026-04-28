"""
Reads MapReduce HDFS output for each query and inserts results into PostgreSQL.
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import HDFS_OUTPUT_DIR, JAVA_HOME


def _read_hdfs_output(run_id: int, query_name: str) -> list[str]:
    """Return all lines from an HDFS output directory (part-* files)."""
    path = f'{HDFS_OUTPUT_DIR}/run_{run_id}/{query_name}/part-*'
    env = {**os.environ, 'JAVA_HOME': JAVA_HOME}
    result = subprocess.run(
        ['hdfs', 'dfs', '-cat', path],
        capture_output=True, text=True, check=True, env=env
    )
    return [l for l in result.stdout.splitlines() if l.strip()]


def load_q1(conn, run_id: int) -> None:
    lines = _read_hdfs_output(run_id, 'q1')
    rows = []
    for line in lines:
        parts = line.split('\t')
        if len(parts) != 4:
            continue
        log_date, status_code, req_count, total_bytes = parts
        rows.append((run_id, log_date, int(status_code), int(req_count), int(total_bytes)))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q1_daily_traffic
                (run_id, log_date, status_code, request_count, total_bytes)
            VALUES (%s, %s, %s, %s, %s)
            """,
            rows
        )
    conn.commit()
    print(f"  Q1: inserted {len(rows)} rows")


def load_q2(conn, run_id: int) -> None:
    lines = _read_hdfs_output(run_id, 'q2')
    all_rows = []
    for line in lines:
        parts = line.split('\t')
        if len(parts) != 4:
            continue
        path, req_count, total_bytes, distinct_hosts = parts
        all_rows.append((path, int(req_count), int(total_bytes), int(distinct_hosts)))

    # Sort by request_count DESC, take top 20, assign rank
    all_rows.sort(key=lambda r: r[1], reverse=True)
    top20 = all_rows[:20]

    rows = [
        (run_id, rank + 1, row[0], row[1], row[2], row[3])
        for rank, row in enumerate(top20)
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q2_top_resources
                (run_id, rank, resource_path, request_count, total_bytes, distinct_host_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows
        )
    conn.commit()
    print(f"  Q2: inserted {len(rows)} rows (top 20 of {len(all_rows)} resources)")


def load_q3(conn, run_id: int) -> None:
    lines = _read_hdfs_output(run_id, 'q3')
    rows = []
    for line in lines:
        parts = line.split('\t')
        if len(parts) != 6:
            continue
        log_date, log_hour, err_count, total_count, err_rate, dist_hosts = parts
        rows.append((
            run_id,
            log_date,
            int(log_hour),
            int(err_count),
            int(total_count),
            float(err_rate),
            int(dist_hosts),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q3_hourly_errors
                (run_id, log_date, log_hour, error_request_count,
                 total_request_count, error_rate, distinct_error_hosts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows
        )
    conn.commit()
    print(f"  Q3: inserted {len(rows)} rows")
