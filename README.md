# DAS 839 – NoSQL Systems | End Semester Project
## Progress Report — Phase 2 Complete (4 Pipelines)

---

## What Has Been Done

**MapReduce**, **MongoDB**, **Pig**, and **Hive** pipelines are fully implemented and verified end-to-end. All four read the same raw NASA log files, apply the same parsing rules, run the same three analytical queries, load results into the same PostgreSQL tables, and produce **byte-identical Q1/Q2/Q3 output on the full Jul95 dataset** (1,889,667 records, 2,048 malformed).

### Compliance with the eval guidelines
- **CLI**: `--pipeline {mapreduce,mongodb,pig,hive}` × `--query {q1,q2,q3,all}` × `--inputs PATH [PATH ...]` — covers all 12 pipeline×query combinations the eval requires
- **Batching = file-as-batch**: each input log file is one batch (Jul95 → batch 1, Aug95 → batch 2). The internal 50k-record streaming chunks are invisible at the batch level
- **Schema**: `pipeline_runs` (run metadata) + `batch_log` (per-batch records/malformed counts) + three result tables, each result row stamped with `pipeline_name` + `execution_time` per the project statement
- **Reporter**: `python3 main.py --report --run-id N` shows pipeline, runtime, per-batch sizes, malformed counts, and Q1/Q2/Q3 tables

### Full Jul95 runtimes
| Pipeline | Run ID | Runtime | Diff vs MapReduce |
|---|---|---|---|
| MapReduce | 9  | 183 s   | (baseline) |
| MongoDB   | 10 | 26 s    | 0 lines |
| Pig       | 20 | 1,076 s | 0 lines |
| Hive      | 22 | 140 s   | 0 lines |

MongoDB is fastest because aggregations are in-process. Hive beats MapReduce by bundling all three queries into one HQL script (avoiding 2 redundant Hadoop submissions). Pig is dominated by per-script JVM cold-start and ORDER BY's sampling MR jobs.

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
│   ├── common/
│   │   └── parser.py                     ← Shared log parser (reused by all pipelines)
│   ├── mapreduce/
│   │   ├── runner.py                     ← MapReduce orchestrator
│   │   ├── loader.py                     ← Reads HDFS output → PostgreSQL
│   │   └── mr_jobs/
│   │       ├── q1_mapper.py / q1_reducer.py   ← Daily Traffic Summary
│   │       ├── q2_mapper.py / q2_reducer.py   ← Top 20 Requested Resources
│   │       └── q3_mapper.py / q3_reducer.py   ← Hourly Error Analysis
│   ├── mongodb/
│   │   ├── runner.py                     ← MongoDB orchestrator
│   │   └── loader.py                     ← Runs aggregation pipelines → PostgreSQL
│   ├── pig/
│   │   ├── runner.py                     ← Pig orchestrator (stages TSV, runs 3 .pig scripts)
│   │   ├── loader.py                     ← Reads Pig output from HDFS → PostgreSQL
│   │   └── scripts/
│   │       ├── q1.pig                    ← Daily Traffic Summary
│   │       ├── q2.pig                    ← Top 20 Requested Resources
│   │       └── q3.pig                    ← Hourly Error Analysis
│   └── hive/
│       ├── runner.py                     ← Hive orchestrator (stages TSV, runs 1 bundled .hql)
│       ├── loader.py                     ← Reads Hive output from HDFS → PostgreSQL
│       └── scripts/
│           └── all_queries.hql           ← Q1 + Q2 + Q3 in one script (single JVM startup)
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

### `pipelines/pig/runner.py`
1. **Preflight** — checks input file exists, `pig` binary exists, HDFS reachable
2. **Create run record** — same INSERT into `pipeline_runs` as the other pipelines
3. **Batch staging** — same as MapReduce: reads file in `batch_size` chunks, parses with `parse_line()`, writes TSV to HDFS under `/user/nasa_etl/staged/run_<id>/`, logs each batch to `batch_log`
4. **Pig scripts** — invokes `pig -x mapreduce` once per query (q1.pig, q2.pig, q3.pig) with `-param input_dir=... -param output_dir=...`
5. **Load results** — `loader.py` reads each query's HDFS output (`part-*` TSV files) and inserts into PostgreSQL
6. **Finalize** — same UPDATE to `pipeline_runs`

### `pipelines/pig/loader.py`
Reads Pig output via `hdfs dfs -cat`, parses TSV, inserts into result tables. Q1 and Q3 are sorted in Python (Pig scripts skip `ORDER BY` for speed). Q2 is already top-20 sorted DESC by Pig; rank assigned in Python.

### Pig Scripts

| Script | What it does |
|---|---|
| `q1.pig` | LOAD → GROUP BY (log_date, status_code) → COUNT, SUM(bytes); STORE (sort in loader) |
| `q2.pig` | LOAD → GROUP BY path → DISTINCT(host) → COUNT/SUM aggregates → ORDER BY count DESC → LIMIT 20 |
| `q3.pig` | LOAD → tag is_error from status_code → GROUP BY (log_date, log_hour) → SUM errors, COUNT total, error_rate, DISTINCT error hosts; STORE (sort in loader) |

All three accept `$input_dir` and `$output_dir` parameters. ORDER BY was deliberately removed from q1 and q3 because it adds a Pig sampling MR job + sort MR job per query — for the small result sets (≤1k rows), Python sort is essentially free and saves ~6-8 min of YARN overhead.

---

### `pipelines/hive/runner.py`
1. **Preflight** — checks input file exists, `hive` binary at `HIVE_BIN`, HDFS reachable
2. **Create run record** — same INSERT into `pipeline_runs` as the other pipelines, `pipeline_name='hive'`
3. **Batch staging** — reuses `pipelines.common.parser.stage_batch_to_hdfs`; identical TSV layout under `/user/nasa_etl/staged/run_<id>/`
4. **Hive script** — invokes `hive -f all_queries.hql --hivevar input_dir=... --hivevar output_base=...` *once*. Sets `JAVA_HOME=/home/ashok_ubun/jdk8` for the subprocess (Hive 3.1.3 requires JDK 8). Pins the Derby metastore via `--hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$HIVE_HOME/metastore_db` so the metastore lives in a stable location regardless of cwd.
5. **Load results** — `loader.py` reads each query's HDFS directory and inserts into PostgreSQL
6. **Finalize** — same UPDATE to `pipeline_runs`

### `pipelines/hive/loader.py`
Reads Hive's `INSERT OVERWRITE DIRECTORY` output via `hdfs dfs -cat .../00*` (Hive emits `000000_0` files, not `part-*`). Q1 and Q3 are sorted in Python (HQL skips ORDER BY for those). Q2 is already top-20 sorted DESC; rank assigned in Python.

### Hive Script

| Script | What it does |
|---|---|
| `all_queries.hql` | One bundled script: CREATE EXTERNAL TABLE on the staged HDFS dir, then three `INSERT OVERWRITE DIRECTORY` statements for Q1, Q2 (with ORDER BY+LIMIT 20), Q3, then DROP TABLE |

Single bundled script means **one** Hive JVM cold-start instead of three — main reason Hive is faster than Pig on this hardware. External-table-on-HDFS avoids any data movement; Hive just reads the staged TSV directly via schema-on-read.

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
| Pig | 0.17.0 at `/home/ashok_ubun/pig/`, executed in `-x mapreduce` mode against YARN |
| Hive | 3.1.3 at `/home/ashok_ubun/hive/`, embedded Derby metastore at `~/hive/metastore_db`, runs on JDK 8 (`/home/ashok_ubun/jdk8`) |
| MongoDB | 8.0, running on `localhost:27017`, data at `/var/lib/mongodb/` |
| PostgreSQL | 16.13, custom cluster at `/home/ashok_ubun/pgdata_nasa/`, port 5433 |
| Python | 3.12 + psycopg2-binary + pymongo |
| Dataset | `NASA_access_log_Jul95` — 1,891,714 lines, 196 MB |

---

## Test Run Results (1,000 records, batch_size=200)

| Pipeline | Run ID | Records | Malformed | Batches | Runtime | Q1 rows | Q2 rows | Q3 rows |
|---|---|---|---|---|---|---|---|---|
| mapreduce | 3  | 1,000 | 0 | 5 | ~71s   | 4 | 20 | 1 |
| mongodb   | 5  | 1,000 | 0 | 5 | 0.04s  | 4 | 20 | 1 |
| pig       | 19 | 1,000 | 0 | 5 | 1029s  | 4 | 20 | 1 |
| hive      | 21 | 1,000 | 0 | 5 | 89.6s  | 4 | 20 | 1 |

Q1 and Q3 are byte-identical across all three pipelines. Q2 differs by exactly one row at the rank-20 boundary because of a `request_count=9` tie — Pig and MapReduce break the tie differently, but both produce a valid top-20.

**Why Pig is so much slower than MapReduce on the same hardware:**
- Pig launches a fresh JVM per `.pig` script (3 invocations from the runner)
- Pig's `ORDER BY` (used by q2) compiles into a *sampling* MR job followed by a sort MR job — extra YARN container overhead per query
- On a single-node WSL pseudo-distributed cluster, every YARN container launch is ~30-60s and they cannot run in parallel
- The runtime is dominated by per-job overhead, not data work — even on the full dataset the wall-clock cost grows only modestly

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

# Pig
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline pig --batch-size 50000 --input data/NASA_access_log_Jul95

# Hive
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline hive --batch-size 50000 --input data/NASA_access_log_Jul95
```

### View report
```bash
python3 main.py --report --run-id <run_id>
```

---

## Batching Strategy

**Each input log file = one batch.** This is the canonical interpretation given by the eval guidelines (verify_its_there.txt § 3): *"the two log files may be treated as two batches: Batch 1: July dataset, Batch 2: August dataset"*. We adopted it directly because:

- **Natural data boundaries**: each NASA log file covers one calendar month, a meaningful unit of analysis
- **Eval-sanctioned**: removes any ambiguity about why a particular batch size was chosen
- **Comparable across pipelines**: all four pipelines see the same N batches because they all consume the same `--inputs` list
- **Trivially extensible**: passing N files yields N batches, with `batch_id` being the 1-based position on the CLI

### Within-file streaming
Multi-million-record files cannot be held in Python memory all at once, so each runner streams records in **50,000-record buffers** (`STREAM_CHUNK_SIZE` in [config.py](config.py)) before flushing to HDFS / inserting into MongoDB. This is **purely a memory-management knob** — it has no batching semantics and is not user-configurable.

### Schema columns
- `batch_log.records_in_batch` = records in that file (one row per file)
- `batch_log.malformed_in_batch` = unparseable lines in that file
- `pipeline_runs.num_batches` = number of input files
- `pipeline_runs.avg_batch_size` = total_records / num_batches

A previous version stored a `batch_size` column on `pipeline_runs` — it has been dropped (and from `setup.sql`) because per-batch sizes are now in `batch_log` and the aggregate is `avg_batch_size`. A migration to drop it from existing databases is at [tools/migrate_drop_batch_size.sql](tools/migrate_drop_batch_size.sql).

### Why not a fixed N-record chunking strategy?
The earlier design used `--batch-size 50000` to chunk *within* a file, producing ~38 logical batches for Jul95. That choice was arbitrary (50k vs 40k vs 60k had no defensible justification), and the resulting "batches" carried no semantic meaning. File-as-batch eliminates the question entirely.

---

## What Comes Next

- Run all 4 pipelines on the Aug dataset (or both Jul + Aug as a 2-batch run)
- Comparative table: runtime, implementation complexity, batching behaviour, reporting suitability
- Record video demo showing live pipeline selection (`--pipeline` × `--query`) and report output
- Write compact PDF report with architecture, design decisions, and comparative analysis
