"""
MongoDB pipeline orchestrator.

Flow:
  1. Preflight  — verify MongoDB is reachable and input file exists
  2. DB record  — INSERT skeleton row into pipeline_runs, get run_id
  3. Indexes    — create compound indexes on log_records collection
  4. Ingest     — read file in batches, parse, insert_many into MongoDB
  5. Aggregate  — run Q1/Q2/Q3 aggregation pipelines via loader
  6. Finalize   — UPDATE pipeline_runs with runtime and totals
"""

import os
import sys
import time
from datetime import datetime, timezone

import pymongo
from pymongo import MongoClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from db.connection import get_conn
from pipelines.mapreduce.parser import parse_line
from pipelines.mongodb import loader


# ── Preflight ──────────────────────────────────────────────────────────────────

def _preflight(input_path: str) -> None:
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        client.close()
    except Exception as exc:
        raise RuntimeError(
            f"MongoDB is not reachable at {MONGO_URI}.\n"
            "Start it with:\n"
            "  sudo mongod --dbpath /var/lib/mongodb "
            "--logpath /var/log/mongodb/mongod.log --fork\n"
            f"Original error: {exc}"
        ) from exc


# ── PostgreSQL run record ──────────────────────────────────────────────────────

def _create_run_record(conn, input_path: str, batch_size: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (pipeline_name, input_file, started_at, batch_size)
            VALUES (%s, %s, %s, %s) RETURNING run_id
            """,
            ('mongodb', input_path, datetime.now(timezone.utc), batch_size)
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


# ── MongoDB indexes ────────────────────────────────────────────────────────────

def _create_indexes(collection) -> None:
    collection.create_index(
        [('run_id', pymongo.ASCENDING),
         ('log_date', pymongo.ASCENDING),
         ('status_code', pymongo.ASCENDING)],
        name='idx_run_date_status'
    )
    collection.create_index(
        [('run_id', pymongo.ASCENDING),
         ('resource_path', pymongo.ASCENDING)],
        name='idx_run_resource'
    )
    collection.create_index(
        [('run_id', pymongo.ASCENDING),
         ('log_date', pymongo.ASCENDING),
         ('log_hour', pymongo.ASCENDING)],
        name='idx_run_date_hour'
    )


# ── Batch ingest ───────────────────────────────────────────────────────────────

def _ingest_all_batches(pg_conn, collection, input_path: str,
                        batch_size: int, run_id: int):
    """
    Read the log file in chunks of batch_size lines.
    Parse each line; insert valid records into MongoDB with run_id stamped on each
    document. Log every batch (including its malformed count) to batch_log in PG.
    Returns (total_records, total_malformed, num_batches).
    num_batches counts only non-empty batches (per project spec).
    """
    total_records   = 0
    total_malformed = 0
    num_batches     = 0
    batch_id        = 1

    buf: list[dict] = []
    buf_malformed   = 0
    lines_in_buf    = 0

    with open(input_path, 'r', encoding='utf-8', errors='replace') as fh:
        for raw_line in fh:
            record = parse_line(raw_line)
            if record is None:
                buf_malformed += 1
            else:
                buf.append(record)
            lines_in_buf += 1

            if lines_in_buf == batch_size:
                _flush_batch(pg_conn, collection, buf, buf_malformed, run_id, batch_id)
                total_records   += len(buf)
                total_malformed += buf_malformed
                if buf:
                    num_batches += 1
                print(f"  Batch {batch_id:>4}: {len(buf):>7,} records  "
                      f"({buf_malformed} malformed)")
                batch_id     += 1
                buf           = []
                buf_malformed = 0
                lines_in_buf  = 0

        # Final partial batch
        if lines_in_buf > 0:
            _flush_batch(pg_conn, collection, buf, buf_malformed, run_id, batch_id)
            total_records   += len(buf)
            total_malformed += buf_malformed
            if buf:
                num_batches += 1
            print(f"  Batch {batch_id:>4}: {len(buf):>7,} records  "
                  f"({buf_malformed} malformed)  [final]")

    return total_records, total_malformed, num_batches


def _flush_batch(pg_conn, collection, records: list[dict],
                 malformed: int, run_id: int, batch_id: int) -> None:
    if records:
        docs = [{**r, 'run_id': run_id} for r in records]
        collection.insert_many(docs, ordered=False)

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO batch_log (run_id, batch_id, records_in_batch, malformed_in_batch)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, batch_id, len(records), malformed)
        )
    pg_conn.commit()


# ── Finalize ───────────────────────────────────────────────────────────────────

def _finalize_run(pg_conn, run_id: int, start_time: float,
                  total_records: int, total_malformed: int,
                  num_batches: int) -> float:
    runtime = time.monotonic() - start_time
    avg = (total_records / num_batches) if num_batches > 0 else 0.0
    with pg_conn.cursor() as cur:
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
    pg_conn.commit()
    return runtime


# ── Public entry point ─────────────────────────────────────────────────────────

def run(input_path: str, batch_size: int) -> None:
    print(f"[MongoDB] Starting ETL pipeline")
    print(f"  Input:      {input_path}")
    print(f"  Batch size: {batch_size:,}")

    _preflight(input_path)

    mongo_client = MongoClient(MONGO_URI)
    mongo_db     = mongo_client[MONGO_DB]
    collection   = mongo_db[MONGO_COLLECTION]

    pg_conn = get_conn()
    try:
        run_id = _create_run_record(pg_conn, input_path, batch_size)
        print(f"  Run ID:     {run_id}")

        print("[MongoDB] Creating indexes...")
        _create_indexes(collection)

        start_time = time.monotonic()

        print("[MongoDB] Ingesting batches...")
        total_records, total_malformed, num_batches = _ingest_all_batches(
            pg_conn, collection, input_path, batch_size, run_id
        )
        print(f"  Total: {total_records:,} records | "
              f"{total_malformed:,} malformed | {num_batches} batches")

        print("[MongoDB] Running aggregation pipelines...")
        loader.load_q1(pg_conn, mongo_db, run_id)
        loader.load_q2(pg_conn, mongo_db, run_id)
        loader.load_q3(pg_conn, mongo_db, run_id)

        runtime = _finalize_run(pg_conn, run_id, start_time,
                                total_records, total_malformed, num_batches)

        print(f"\n[MongoDB] Done.")
        print(f"  Run ID:  {run_id}")
        print(f"  Records: {total_records:,}  |  Malformed: {total_malformed:,}  "
              f"|  Batches: {num_batches}  |  Runtime: {runtime:.1f}s")
        print(f"  Report:  python3 main.py --report --run-id {run_id}")

    finally:
        pg_conn.close()
        mongo_client.close()
