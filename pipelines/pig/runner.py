"""
Pig pipeline orchestrator.

Flow:
  1. Preflight checks (HDFS reachable, pig binary present, input file exists)
  2. INSERT skeleton pipeline_runs row → get run_id
  3. Read input file in batches → parse → stage each batch as TSV to HDFS
     (same staging format and batching semantics as the MapReduce pipeline,
      so batch_size / num_batches / malformed counts are directly comparable)
  4. Run 3 Pig scripts sequentially (q1.pig, q2.pig, q3.pig) on the staged dir
  5. Load Pig output into PostgreSQL via loader.py
  6. UPDATE pipeline_runs with final stats and runtime
"""

import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import (
    PIG_BIN, PIG_SCRIPTS_DIR, STREAM_CHUNK_SIZE,
    HDFS_STAGED_DIR, HDFS_PIG_OUTPUT_DIR, JAVA_HOME,
)
from db.connection import get_conn
from pipelines.common.parser import parse_line, stage_chunk_to_hdfs
from pipelines.pig import loader


def _hdfs(args: list[str], check: bool = True, capture: bool = True):
    env = {**os.environ, 'JAVA_HOME': JAVA_HOME}
    return subprocess.run(['hdfs'] + args, check=check, capture_output=capture, env=env)


def _preflight(input_files: list[str]) -> None:
    for p in input_files:
        if not os.path.isfile(p):
            raise FileNotFoundError(f"Input file not found: {p}")

    pig_exec = PIG_BIN if os.path.isfile(PIG_BIN) else shutil.which('pig')
    if pig_exec is None:
        raise RuntimeError(
            "Pig binary not found. Install Apache Pig and either place it at "
            f"{PIG_BIN} or ensure 'pig' is on PATH.\n"
            "Quick install:\n"
            "  cd ~ && wget https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz\n"
            "  tar xzf pig-0.17.0.tar.gz && mv pig-0.17.0 pig\n"
        )

    result = _hdfs(['dfs', '-ls', '/'], check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "HDFS is not reachable. Start the cluster:\n"
            "  /home/ashok_ubun/hadoop/sbin/start-dfs.sh\n"
            "  /home/ashok_ubun/hadoop/sbin/start-yarn.sh"
        )


def _resolve_pig_bin() -> str:
    return PIG_BIN if os.path.isfile(PIG_BIN) else shutil.which('pig')


def _create_run_record(conn, input_files: list[str]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (pipeline_name, input_file, started_at)
            VALUES (%s, %s, %s) RETURNING run_id
            """,
            ('pig', ', '.join(input_files), datetime.now(timezone.utc))
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def _stage_all_batches(conn, input_files: list[str], run_id: int):
    """One batch per input file (Jul95 = batch 1, Aug95 = batch 2, ...).
    Within a file, records are streamed in STREAM_CHUNK_SIZE chunks to HDFS.
    Exactly one batch_log row written per file.
    """
    _hdfs(['dfs', '-mkdir', '-p', f'{HDFS_STAGED_DIR}/run_{run_id}'])

    total_records   = 0
    total_malformed = 0

    for batch_id, input_path in enumerate(input_files, start=1):
        file_records, file_malformed = _stage_one_file(input_path, run_id, batch_id)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batch_log (run_id, batch_id, records_in_batch, malformed_in_batch)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, batch_id, file_records, file_malformed)
            )
        conn.commit()
        total_records   += file_records
        total_malformed += file_malformed
        print(f"  Batch {batch_id} ({os.path.basename(input_path)}): "
              f"{file_records:,} records, {file_malformed:,} malformed", flush=True)

    return total_records, total_malformed, len(input_files)


def _stage_one_file(input_path: str, run_id: int, batch_id: int) -> tuple[int, int]:
    """Stream a single log file to HDFS as one or more chunk files."""
    file_records   = 0
    file_malformed = 0
    chunk_id       = 1
    buf: list[dict] = []

    with open(input_path, 'r', encoding='utf-8', errors='replace') as fh:
        for raw_line in fh:
            record = parse_line(raw_line)
            if record is None:
                file_malformed += 1
            else:
                buf.append(record)

            if len(buf) >= STREAM_CHUNK_SIZE:
                stage_chunk_to_hdfs(buf, run_id, batch_id, chunk_id, HDFS_STAGED_DIR)
                file_records += len(buf)
                buf = []
                chunk_id += 1

        if buf:
            stage_chunk_to_hdfs(buf, run_id, batch_id, chunk_id, HDFS_STAGED_DIR)
            file_records += len(buf)

    return file_records, file_malformed


def _run_pig_script(run_id: int, query_name: str, pig_bin: str) -> None:
    input_dir  = f'{HDFS_STAGED_DIR}/run_{run_id}'
    output_dir = f'{HDFS_PIG_OUTPUT_DIR}/run_{run_id}/{query_name}'

    _hdfs(['dfs', '-rm', '-r', '-f', output_dir], check=False)

    script_path = os.path.join(PIG_SCRIPTS_DIR, f'{query_name}.pig')

    cmd = [
        pig_bin,
        '-x', 'mapreduce',
        '-param', f'input_dir={input_dir}',
        '-param', f'output_dir={output_dir}',
        '-f', script_path,
    ]

    env = {**os.environ, 'JAVA_HOME': JAVA_HOME}
    print(f"  Running Pig script: {query_name} ...", flush=True)
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        sys.stderr.write(result.stdout[-2000:])
        sys.stderr.write(result.stderr[-3000:])
        raise RuntimeError(f"Pig script failed: {query_name}")
    print(f"  {query_name} completed.", flush=True)


def _finalize_run(conn, run_id: int, start_time: float,
                  total_records: int, total_malformed: int,
                  num_batches: int) -> float:
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


def run(inputs: list[str], query: str = 'all') -> None:
    _preflight(inputs)
    pig_bin = _resolve_pig_bin()

    conn = get_conn()
    try:
        run_id = _create_run_record(conn, inputs)
        print(f"Run ID: {run_id}  |  pipeline: pig  |  inputs: {len(inputs)}  |  query: {query}")

        start_time = time.monotonic()

        print("Staging batches to HDFS...")
        total_records, total_malformed, num_batches = _stage_all_batches(
            conn, inputs, run_id
        )
        print(f"  Staged {total_records:,} records in {num_batches} batches "
              f"({total_malformed:,} malformed)")

        print(f"Running Pig scripts (query={query})...")
        if query in ('q1', 'all'):
            _run_pig_script(run_id, 'q1', pig_bin)
        if query in ('q2', 'all'):
            _run_pig_script(run_id, 'q2', pig_bin)
        if query in ('q3', 'all'):
            _run_pig_script(run_id, 'q3', pig_bin)

        print("Loading results into PostgreSQL...")
        if query in ('q1', 'all'):
            loader.load_q1(conn, run_id)
        if query in ('q2', 'all'):
            loader.load_q2(conn, run_id)
        if query in ('q3', 'all'):
            loader.load_q3(conn, run_id)

        runtime = _finalize_run(conn, run_id, start_time,
                                total_records, total_malformed, num_batches)

        print(f"\nDone. Run ID={run_id} | Records={total_records:,} | "
              f"Malformed={total_malformed:,} | Batches={num_batches} | "
              f"Runtime={runtime:.1f}s")
        print(f"View report: python main.py --report --run-id {run_id}")

    finally:
        conn.close()
