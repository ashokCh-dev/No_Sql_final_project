"""
Reads completed run results from PostgreSQL and pretty-prints them.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_conn


def _col_widths(headers: list[str], rows: list[tuple]) -> list[int]:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))
    return widths


def _print_table(headers: list[str], rows: list[tuple]) -> None:
    if not rows:
        print("  (no rows)")
        return
    widths = _col_widths(headers, rows)
    fmt = '  ' + '  '.join(f'{{:<{w}}}' for w in widths)
    sep = '  ' + '  '.join('-' * w for w in widths)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))


def generate_report(run_id: int) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # ── Run metadata ──────────────────────────────────────────────────
            cur.execute("SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,))
            run = cur.fetchone()
            if not run is not None:
                pass
            if run is None:
                print(f"No run found with run_id={run_id}")
                return

            (rid, pipeline, input_file, started_at, finished_at,
             runtime_sec, batch_size, num_batches, total_records,
             malformed_count, avg_batch_size) = run

            SEP = '=' * 70
            print(SEP)
            print('  PIPELINE RUN REPORT')
            print(SEP)
            print(f"  Run ID          : {rid}")
            print(f"  Pipeline        : {pipeline}")
            print(f"  Input File      : {input_file}")
            print(f"  Started At      : {started_at}")
            print(f"  Finished At     : {finished_at}")
            print(f"  Runtime         : {runtime_sec} seconds")
            print(f"  Batch Size      : {batch_size:,}")
            print(f"  Num Batches     : {num_batches}")
            print(f"  Total Records   : {total_records:,}")
            print(f"  Malformed Count : {malformed_count:,}")
            print(f"  Avg Batch Size  : {avg_batch_size}")

            # ── Q1 ─────────────────────────────────────────────────────────────
            print()
            print('-' * 70)
            print('  QUERY 1: Daily Traffic Summary')
            print('-' * 70)
            cur.execute(
                """
                SELECT log_date, status_code, request_count, total_bytes
                FROM q1_daily_traffic
                WHERE run_id = %s
                ORDER BY log_date, status_code
                """,
                (run_id,)
            )
            rows = cur.fetchall()
            _print_table(['log_date', 'status_code', 'request_count', 'total_bytes'], rows)
            print(f"  ({len(rows)} rows)")

            # ── Q2 ─────────────────────────────────────────────────────────────
            print()
            print('-' * 70)
            print('  QUERY 2: Top 20 Requested Resources')
            print('-' * 70)
            cur.execute(
                """
                SELECT rank, resource_path, request_count, total_bytes, distinct_host_count
                FROM q2_top_resources
                WHERE run_id = %s
                ORDER BY rank
                """,
                (run_id,)
            )
            rows = cur.fetchall()
            _print_table(
                ['rank', 'resource_path', 'request_count', 'total_bytes', 'distinct_hosts'],
                rows
            )

            # ── Q3 ─────────────────────────────────────────────────────────────
            print()
            print('-' * 70)
            print('  QUERY 3: Hourly Error Analysis')
            print('-' * 70)
            cur.execute(
                """
                SELECT log_date, log_hour,
                       error_request_count, total_request_count,
                       CONCAT(ROUND(error_rate * 100, 2), '%%') AS error_rate_pct,
                       distinct_error_hosts
                FROM q3_hourly_errors
                WHERE run_id = %s
                ORDER BY log_date, log_hour
                """,
                (run_id,)
            )
            rows = cur.fetchall()
            _print_table(
                ['log_date', 'log_hour', 'error_count', 'total_count',
                 'error_rate', 'distinct_error_hosts'],
                rows
            )
            print(f"  ({len(rows)} rows)")

            print()
            print(SEP)

    finally:
        conn.close()
