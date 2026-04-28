# DAS 839 – NoSQL Systems | End Semester Project
## Progress Report — Phase 1 Complete (2 Pipelines)

---

## What Has Been Done

Both **MapReduce** and **MongoDB** pipelines are fully implemented and tested. Both read the same raw NASA log files, apply the same parsing rules, run the same three analytical queries, load results into the same PostgreSQL tables, and produce identical output — verified side by side on the same 1,000-record test slice.

---

## Project Structure

```
final_proj/
├── main.py                               ← CLI entry point (supports mapreduce + mongodb)
├── config.py                             ← Central constants (Hadoop, MongoDB, PostgreSQL)
├── setup.sql                             ← PostgreSQL schema (run once)
├── requirements.txt                      ← Python dependencies
├── data/
│   └── NASA_access_log_Jul95             ← 1.89M records (196 MB)
├── pipelines/
│   ├── mapreduce/
│   │   ├── parser.py                     ← Shared log parser (reused by MongoDB too)
│   │   ├── runner.py                     ← MapReduce orchestrator
│   │   ├── loader.py                     ← Reads HDFS output → PostgreSQL
│   │   └── mr_jobs/
│   │       ├── q1_mapper.py / q1_reducer.py   ← Daily Traffic Summary
│   │       ├── q2_mapper.py / q2_reducer.py   ← Top 20 Requested Resources
│   │       └── q3_mapper.py / q3_reducer.py   ← Hourly Error Analysis
│   └── mongodb/
│       ├── runner.py                     ← MongoDB orchestrator
│       └── loader.py                     ← Runs aggregation pipelines → PostgreSQL
├── reporting/
│   └── reporter.py                       ← Reads PostgreSQL, prints formatted report
└── db/
    └── connection.py                     ← psycopg2 connection factory
```

---

## File-by-File Summary

### `main.py`
Entry point. Routes to the selected pipeline runner or the reporter via CLI flags.
```
python3 main.py --pipeline mapreduce --batch-size 50000 --input data/NASA_access_log_Jul95
python3 main.py --pipeline mongodb   --batch-size 50000 --input data/NASA_access_log_Jul95
python3 main.py --report --run-id 5
```

### `config.py`
All constants in one place: Hadoop Streaming jar, HDFS paths, Java home, PostgreSQL connection, and MongoDB connection. Every module imports from here.

### `setup.sql`
Defines 5 PostgreSQL tables (shared by all pipelines, distinguished by `run_id` and `pipeline_name`):
- `pipeline_runs` — one row per run; stores pipeline name, runtime, batch stats, malformed count
- `batch_log` — one row per batch; records and malformed count per batch
- `q1_daily_traffic` — Query 1 results
- `q2_top_resources` — Query 2 results (top 20 with rank)
- `q3_hourly_errors` — Query 3 results

### `db/connection.py`
Returns a `psycopg2` connection using credentials from `config.py`. Used by all runners, loaders, and the reporter.

---

### `pipelines/mapreduce/parser.py`
Parses each raw log line using a regex into 8 structured fields:
`host`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred`.
- Missing bytes (`-`) → stored as `0`
- Unparseable lines → returns `None` (counted as malformed, never silently dropped)
- `parse_line()` is also imported directly by the MongoDB runner — no duplication

**Log format parsed:**
```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

---

### `pipelines/mapreduce/runner.py`
1. **Preflight** — checks HDFS is reachable and input file exists
2. **Create run record** — inserts into `pipeline_runs`, gets `run_id`
3. **Batch staging** — reads file in chunks of `batch_size`; parses; writes TSV to HDFS; logs each batch to `batch_log`
4. **MapReduce jobs** — runs Q1, Q2, Q3 Hadoop Streaming jobs sequentially on staged HDFS data
5. **Load results** — reads MR output from HDFS, inserts into PostgreSQL
6. **Finalize** — updates `pipeline_runs` with runtime, totals, avg batch size

### `pipelines/mapreduce/loader.py`
Reads Hadoop Streaming output (`hdfs dfs -cat part-*`) and inserts into result tables:
- `load_q1` — daily traffic aggregates
- `load_q2` — all resources sorted DESC; top 20 selected and ranked in Python
- `load_q3` — hourly error stats with error rate

### MapReduce Scripts (Hadoop Streaming)

| Script | What it does |
|---|---|
| `q1_mapper.py` | Emits `log_date \t status_code \t 1 \t bytes` |
| `q1_reducer.py` | Groups by (date, status); sums request count and total bytes |
| `q2_mapper.py` | Emits `resource_path \t host \t 1 \t bytes` |
| `q2_reducer.py` | Groups by path; sums count/bytes; collects distinct hosts in a set |
| `q3_mapper.py` | Emits `log_date \t log_hour \t host \t is_error \t 1` |
| `q3_reducer.py` | Groups by (date, hour); counts errors and total; computes error_rate; distinct error hosts |

Key flag: `-D stream.num.map.output.key.fields=2` groups by 2-field composite key for Q1 and Q3.

---

### `pipelines/mongodb/runner.py`
1. **Preflight** — pings MongoDB (`serverSelectionTimeoutMS=3000`); checks input file exists
2. **Create run record** — same INSERT into `pipeline_runs` as MapReduce
3. **Create indexes** — 3 compound indexes on `log_records` collection before insert
4. **Batch ingest** — reads file in `batch_size` chunks; parses with `parse_line()`; stamps each document with `run_id`; calls `insert_many(ordered=False)` per batch; logs to `batch_log`
5. **Aggregations** — calls `loader.load_q1/q2/q3()`
6. **Finalize** — same UPDATE to `pipeline_runs` as MapReduce

### `pipelines/mongodb/loader.py`
Runs MongoDB aggregation pipelines and inserts results into PostgreSQL:
- `load_q1` — `$match → $group (log_date, status_code) → $sort`
- `load_q2` — `$match → $group (resource_path) → $addToSet(host) → $sort DESC → $limit 20`; rank assigned in Python
- `load_q3` — `$match → $group (log_date, log_hour)` with `$cond` for error counting and `$$REMOVE` trick for distinct error hosts in one pass

All aggregations use `allowDiskUse=True` (required for 1.89M documents).

---

### `reporting/reporter.py`
Reads from PostgreSQL for any `run_id` regardless of which pipeline produced it. Prints:
- Run metadata: pipeline, timestamps, runtime, batch stats, malformed count
- Q1 table: log_date | status_code | request_count | total_bytes
- Q2 table: rank | resource_path | request_count | total_bytes | distinct_hosts
- Q3 table: log_date | log_hour | error_count | total_count | error_rate% | distinct_error_hosts

---

## Infrastructure Setup

| Component | Details |
|---|---|
| Hadoop | 3.3.6 at `/home/ashok_ubun/hadoop/`, single-node pseudo-distributed |
| HDFS | `hdfs://localhost:9000`, replication=1 |
| YARN | Running with `mapreduce_shuffle` aux service |
| MongoDB | 8.0, running on `localhost:27017`, data at `/var/lib/mongodb/` |
| PostgreSQL | 16.13, custom cluster at `/home/ashok_ubun/pgdata_nasa/`, port 5433 |
| Python | 3.12 + psycopg2-binary + pymongo |
| Dataset | `NASA_access_log_Jul95` — 1,891,714 lines, 196 MB |

---

## Test Run Results (1,000 records, batch_size=200)

| Pipeline | Run ID | Records | Malformed | Batches | Runtime | Q1 rows | Q2 rows | Q3 rows |
|---|---|---|---|---|---|---|---|---|
| mapreduce | 3 | 1,000 | 0 | 5 | ~71s | 4 | 20 | 1 |
| mongodb   | 5 | 1,000 | 0 | 5 | 0.04s | 4 | 20 | 1 |

Q1, Q2, and Q3 results are identical across both pipelines — same rows, same values.

---

## How to Run

### Start services (after WSL restart)
```bash
# Hadoop
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
/home/ashok_ubun/hadoop/sbin/start-dfs.sh
/home/ashok_ubun/hadoop/sbin/start-yarn.sh

# PostgreSQL
/usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa \
    -o "-p 5433 -k /home/ashok_ubun/pgdata_nasa/socket" \
    -l /home/ashok_ubun/pgdata_nasa/pg.log start

# MongoDB
sudo mongod --dbpath /var/lib/mongodb --logpath /var/log/mongodb/mongod.log --fork
```

### Run a pipeline
```bash
cd /home/ashok_ubun/studies_ubun/nosql/final_proj

# MapReduce
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline mapreduce --batch-size 50000 --input data/NASA_access_log_Jul95

# MongoDB
python3 main.py --pipeline mongodb --batch-size 50000 --input data/NASA_access_log_Jul95
```

### View report
```bash
python3 main.py --report --run-id <run_id>
```

---

## What Comes Next (Phase 2)

- Add Apache Pig pipeline (`pipelines/pig/`)
- Add Apache Hive pipeline (`pipelines/hive/`)
- Run all 4 pipelines on both Jul and Aug datasets with the same batch size
- Compare: runtime, implementation complexity, batching behaviour, reporting suitability
- Record video demo showing live pipeline selection and report output
- Write compact PDF report with architecture, design decisions, and comparative analysis
