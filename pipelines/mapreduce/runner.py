"""
MapReduce pipeline orchestrator.

Flow:
  1. Preflight checks (HDFS reachable, input file exists)
  2. INSERT skeleton pipeline_runs row → get run_id
  3. Read input file in batches → parse → stage each batch to HDFS
  4. Run 3 Hadoop Streaming jobs sequentially
  5. Load MR output into PostgreSQL via loader.py
  6. UPDATE pipeline_runs with final stats and runtime
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg2

# Allow running from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import STREAMING_JAR, MR_JOBS_DIR, HDFS_STAGED_DIR, HDFS_OUTPUT_DIR, JAVA_HOME
from db.connection import get_conn
from pipelines.mapreduce.parser import parse_line, stage_batch_to_hdfs
from pipelines.mapreduce import loader


def _hdfs(args: list[str], check: bool = True, capture: bool = True):
    env = {**os.environ, 'JAVA_HOME': JAVA_HOME}
    return subprocess.run(['hdfs'] + args, check=check, capture_output=capture, env=env)


def _preflight(input_path: str) -> None:
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    result = _hdfs(['dfs', '-ls', '/'], check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "HDFS is not reachable. Start the cluster:\n"
            "  /home/ashok_ubun/hadoop/sbin/start-dfs.sh\n"
            "  /home/ashok_ubun/hadoop/sbin/start-yarn.sh"
        )


def _create_run_record(conn, input_path: str, batch_size: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (pipeline_name, input_file, started_at, batch_size)
            VALUES (%s, %s, %s, %s) RETURNING run_id
            """,
            ('mapreduce', input_path, datetime.now(timezone.utc), batch_size)
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def _stage_all_batches(conn, input_path: str, batch_size: int, run_id: int):
    """Read log file in batches, stage to HDFS, track stats."""
    _hdfs(['dfs', '-mkdir', '-p', f'{HDFS_STAGED_DIR}/run_{run_id}'])

    total_records  = 0
    total_malformed = 0
    num_batches    = 0
    batch_id       = 1
    buf: list[dict] = []
    buf_malformed  = 0
    lines_in_buf   = 0

    with open(input_path, 'r', encoding='utf-8', errors='replace') as fh:
        for raw_line in fh:
            record = parse_line(raw_line)
            if record is None:
                buf_malformed += 1
            else:
                buf.append(record)
            lines_in_buf += 1

            if lines_in_buf == batch_size:
                _flush_batch(conn, buf, buf_malformed, run_id, batch_id)
                total_records   += len(buf)
                total_malformed += buf_malformed
                if buf:
                    num_batches += 1
                batch_id    += 1
                buf          = []
                buf_malformed = 0
                lines_in_buf = 0

        # final partial batch
        if lines_in_buf > 0:
            _flush_batch(conn, buf, buf_malformed, run_id, batch_id)
            total_records   += len(buf)
            total_malformed += buf_malformed
            if buf:
                num_batches += 1

    return total_records, total_malformed, num_batches


def _flush_batch(conn, records: list[dict], malformed: int, run_id: int, batch_id: int) -> None:
    if records:
        stage_batch_to_hdfs(records, run_id, batch_id, HDFS_STAGED_DIR)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO batch_log (run_id, batch_id, records_in_batch, malformed_in_batch)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, batch_id, len(records), malformed)
        )
    conn.commit()


def _run_mr_job(run_id: int, query_name: str, key_fields: int) -> None:
    """Run one Hadoop Streaming job for the given query."""
    input_dir  = f'{HDFS_STAGED_DIR}/run_{run_id}'
    output_dir = f'{HDFS_OUTPUT_DIR}/run_{run_id}/{query_name}'

    # Remove stale output dir if present
    _hdfs(['dfs', '-rm', '-r', '-f', output_dir], check=False)

    mapper_path  = os.path.join(MR_JOBS_DIR, f'{query_name}_mapper.py')
    reducer_path = os.path.join(MR_JOBS_DIR, f'{query_name}_reducer.py')

    cmd = [
        'hadoop', 'jar', STREAMING_JAR,
        '-D', 'mapreduce.framework.name=yarn',
        '-D', f'stream.num.map.output.key.fields={key_fields}',
        '-D', 'mapreduce.job.reduces=1',
        '-input',   input_dir,
        '-output',  output_dir,
        '-mapper',  f'python3 {mapper_path}',
        '-reducer', f'python3 {reducer_path}',
    ]

    env = {**os.environ, 'JAVA_HOME': JAVA_HOME}
    print(f"  Running MapReduce job: {query_name} ...", flush=True)
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr[-3000:], file=sys.stderr)
        raise RuntimeError(f"MapReduce job failed: {query_name}")
    print(f"  {query_name} completed.", flush=True)


def _finalize_run(conn, run_id: int, start_time: float,
                  total_records: int, total_malformed: int, num_batches: int,
                  batch_size: int) -> None:
    runtime = time.monotonic() - start_time
    avg = (total_records / num_batches) if num_batches > 0 else 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs SET
                finished_at     = %s,
                runtime_seconds = %s,
                num_batches     = %s,
                total_records   = %s,
                malformed_count = %s,
                avg_batch_size  = %s
            WHERE run_id = %s
            """,
            (datetime.now(timezone.utc), round(runtime, 3),
             num_batches, total_records, total_malformed,
             round(avg, 2), run_id)
        )
    conn.commit()
    return runtime


def run(input_path: str, batch_size: int) -> None:
    _preflight(input_path)

    conn = get_conn()
    try:
        run_id = _create_run_record(conn, input_path, batch_size)
        print(f"Run ID: {run_id}  |  pipeline: mapreduce  |  batch_size: {batch_size}")

        start_time = time.monotonic()

        # ── Batch staging ──────────────────────────────────────────────────────
        print("Staging batches to HDFS...")
        total_records, total_malformed, num_batches = _stage_all_batches(
            conn, input_path, batch_size, run_id
        )
        print(f"  Staged {total_records:,} records in {num_batches} batches "
              f"({total_malformed:,} malformed)")

        # ── MapReduce jobs ─────────────────────────────────────────────────────
        print("Running MapReduce jobs...")
        _run_mr_job(run_id, 'q1', key_fields=2)
        _run_mr_job(run_id, 'q2', key_fields=1)
        _run_mr_job(run_id, 'q3', key_fields=2)

        # ── Load results to PostgreSQL ─────────────────────────────────────────
        print("Loading results into PostgreSQL...")
        loader.load_q1(conn, run_id)
        loader.load_q2(conn, run_id)
        loader.load_q3(conn, run_id)

        # ── Finalize ───────────────────────────────────────────────────────────
        runtime = _finalize_run(conn, run_id, start_time,
                                total_records, total_malformed, num_batches, batch_size)

        print(f"\nDone. Run ID={run_id} | Records={total_records:,} | "
              f"Malformed={total_malformed:,} | Batches={num_batches} | "
              f"Runtime={runtime:.1f}s")
        print(f"View report: python main.py --report --run-id {run_id}")

    finally:
        conn.close()
