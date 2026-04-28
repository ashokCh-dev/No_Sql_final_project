"""
MongoDB aggregation pipelines → PostgreSQL result tables.

Each function runs one aggregation, reads the cursor, and bulk-inserts
into the corresponding PostgreSQL table. All aggregations pass
allowDiskUse=True because 1.89M documents exceed the 100MB in-memory limit.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import MONGO_COLLECTION


# ── Q1 — Daily Traffic Summary ─────────────────────────────────────────────────

def load_q1(pg_conn, mongo_db, run_id: int) -> None:
    """
    GROUP BY log_date, status_code
    → request_count = COUNT(*), total_bytes = SUM(bytes_transferred)
    """
    collection = mongo_db[MONGO_COLLECTION]

    pipeline = [
        {'$match': {'run_id': run_id}},
        {'$group': {
            '_id': {
                'log_date':    '$log_date',
                'status_code': '$status_code',
            },
            'request_count': {'$sum': 1},
            'total_bytes':   {'$sum': '$bytes_transferred'},
        }},
        {'$sort': {'_id.log_date': 1, '_id.status_code': 1}},
    ]

    rows = []
    for doc in collection.aggregate(pipeline, allowDiskUse=True):
        rows.append((
            run_id,
            doc['_id']['log_date'],
            doc['_id']['status_code'],
            doc['request_count'],
            doc['total_bytes'],
        ))

    with pg_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q1_daily_traffic
                (run_id, log_date, status_code, request_count, total_bytes)
            VALUES (%s, %s, %s, %s, %s)
            """,
            rows
        )
    pg_conn.commit()
    print(f"  Q1: {len(rows)} rows inserted")


# ── Q2 — Top 20 Requested Resources ───────────────────────────────────────────

def load_q2(pg_conn, mongo_db, run_id: int) -> None:
    """
    GROUP BY resource_path
    → request_count, total_bytes, distinct_host_count
    Sort DESC by request_count, take top 20, assign rank 1–20.
    """
    collection = mongo_db[MONGO_COLLECTION]

    pipeline = [
        {'$match': {'run_id': run_id}},
        {'$group': {
            '_id':           '$resource_path',
            'request_count': {'$sum': 1},
            'total_bytes':   {'$sum': '$bytes_transferred'},
            'host_set':      {'$addToSet': '$host'},
        }},
        {'$addFields': {
            'distinct_host_count': {'$size': '$host_set'},
        }},
        {'$sort': {'request_count': -1}},
        {'$limit': 20},
        {'$project': {'host_set': 0}},
    ]

    rows = []
    for rank, doc in enumerate(
            collection.aggregate(pipeline, allowDiskUse=True), start=1):
        rows.append((
            run_id,
            rank,
            doc['_id'],
            doc['request_count'],
            doc['total_bytes'],
            doc['distinct_host_count'],
        ))

    with pg_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q2_top_resources
                (run_id, rank, resource_path, request_count, total_bytes, distinct_host_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows
        )
    pg_conn.commit()
    print(f"  Q2: {len(rows)} rows inserted (top 20 by request_count)")


# ── Q3 — Hourly Error Analysis ─────────────────────────────────────────────────

def load_q3(pg_conn, mongo_db, run_id: int) -> None:
    """
    GROUP BY log_date, log_hour
    → error_request_count (status 400-599), total_request_count,
      error_rate, distinct_error_hosts

    $$REMOVE trick: $cond returns the host when the record is an error, or
    the $$REMOVE sentinel otherwise. $addToSet silently ignores $$REMOVE,
    so error_host_set accumulates only hosts that triggered errors.
    (Requires MongoDB 3.6+; installed version is 8.0.)
    """
    collection = mongo_db[MONGO_COLLECTION]

    pipeline = [
        {'$match': {'run_id': run_id}},
        {'$group': {
            '_id': {
                'log_date': '$log_date',
                'log_hour': '$log_hour',
            },
            'total_request_count': {'$sum': 1},
            'error_request_count': {
                '$sum': {
                    '$cond': [
                        {'$and': [
                            {'$gte': ['$status_code', 400]},
                            {'$lte': ['$status_code', 599]},
                        ]},
                        1, 0,
                    ]
                }
            },
            'error_host_set': {
                '$addToSet': {
                    '$cond': {
                        'if': {'$and': [
                            {'$gte': ['$status_code', 400]},
                            {'$lte': ['$status_code', 599]},
                        ]},
                        'then': '$host',
                        'else': '$$REMOVE',
                    }
                }
            },
        }},
        {'$addFields': {
            'distinct_error_hosts': {'$size': '$error_host_set'},
            'error_rate': {
                '$cond': [
                    {'$gt': ['$total_request_count', 0]},
                    {'$divide': ['$error_request_count', '$total_request_count']},
                    0.0,
                ]
            },
        }},
        {'$sort': {'_id.log_date': 1, '_id.log_hour': 1}},
        {'$project': {'error_host_set': 0}},
    ]

    rows = []
    for doc in collection.aggregate(pipeline, allowDiskUse=True):
        rows.append((
            run_id,
            doc['_id']['log_date'],
            doc['_id']['log_hour'],
            doc['error_request_count'],
            doc['total_request_count'],
            round(doc['error_rate'], 4),
            doc['distinct_error_hosts'],
        ))

    with pg_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO q3_hourly_errors
                (run_id, log_date, log_hour, error_request_count,
                 total_request_count, error_rate, distinct_error_hosts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows
        )
    pg_conn.commit()
    print(f"  Q3: {len(rows)} rows inserted")
