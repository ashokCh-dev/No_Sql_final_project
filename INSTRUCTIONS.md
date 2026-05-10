# Running Instructions
## DAS 839 – Multi-Pipeline ETL Framework

---

## First-Time Setup (run once ever)

### 1. Install Python dependencies
```bash
pip install psycopg2-binary pymongo
```

### 2. Set up PostgreSQL database
```bash
sudo service postgresql start
sudo -u postgres psql -c "CREATE DATABASE nasa_logs;"
sudo -u postgres psql -c "CREATE USER nasa_user WITH PASSWORD 'nasa_pass';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE nasa_logs TO nasa_user;"

# Initialize custom cluster
/usr/lib/postgresql/16/bin/initdb -D /home/ashok_ubun/pgdata_nasa
/usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa \
    -o "-p 5433 -k /home/ashok_ubun/pgdata_nasa/socket" \
    -l /home/ashok_ubun/pgdata_nasa/pg.log start

psql -U nasa_user -d nasa_logs -p 5433 -h /home/ashok_ubun/pgdata_nasa/socket \
    -f setup.sql
```

### 3. Format HDFS (destroys existing HDFS data — only run once)
```bash
hdfs namenode -format
```

### 4a. Install Apache Hive (3.1.3)
```bash
cd ~
wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar xzf apache-hive-3.1.3-bin.tar.gz && mv apache-hive-3.1.3-bin hive

# If your Hive ships with guava-19.0.jar (older Hadoop conflict), swap it:
# rm ~/hive/lib/guava-19.0.jar
# cp ~/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar ~/hive/lib/

# Init the embedded Derby metastore (uses JDK 8)
cd ~/hive
export JAVA_HOME=/home/ashok_ubun/jdk8
export HADOOP_HOME=/home/ashok_ubun/hadoop
export HIVE_HOME=/home/ashok_ubun/hive
export PATH=$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH
./bin/schematool -dbType derby -initSchema

# HDFS dirs Hive needs (run with HDFS up)
hdfs dfs -mkdir -p /tmp /user/hive/warehouse
hdfs dfs -chmod g+w /tmp /user/hive/warehouse
```

### 4. Install MongoDB
```bash
sudo apt-get install -y gnupg curl
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
  sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] \
  https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt-get update && sudo apt-get install -y mongodb-org
sudo mkdir -p /var/lib/mongodb /var/log/mongodb
sudo chown -R mongodb:mongodb /var/lib/mongodb /var/log/mongodb
```

### 5. Download the dataset
```bash
cd /home/ashok_ubun/studies_ubun/nosql/final_proj/data
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
gunzip NASA_access_log_Jul95.gz
```

---

## Every WSL Session — Start Services First

Run these every time you open WSL before using the project.

### Start Hadoop (for MapReduce pipeline)
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
/home/ashok_ubun/hadoop/sbin/start-dfs.sh
/home/ashok_ubun/hadoop/sbin/start-yarn.sh
```

Verify (NameNode must appear):
```bash
jps
# Expected: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```

If NameNode is missing (happens after WSL restart clears /tmp):
```bash
hdfs namenode -format -force
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
/home/ashok_ubun/hadoop/sbin/start-dfs.sh
/home/ashok_ubun/hadoop/sbin/start-yarn.sh
hdfs dfs -mkdir -p /user/nasa_etl/staged
hdfs dfs -mkdir -p /user/nasa_etl/output
```

### Start PostgreSQL
```bash
/usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa \
    -o "-p 5433 -k /home/ashok_ubun/pgdata_nasa/socket" \
    -l /home/ashok_ubun/pgdata_nasa/pg.log start
```

### Start MongoDB (for MongoDB pipeline)
```bash
sudo mongod --dbpath /var/lib/mongodb --logpath /var/log/mongodb/mongod.log --fork
```

If it fails with lock file error:
```bash
sudo rm -f /var/lib/mongodb/mongod.lock
sudo mongod --dbpath /var/lib/mongodb --logpath /var/log/mongodb/mongod.log --fork
```

---

## End of Session — Stop Services

Reverse order of startup. Run when you're done with the project to free ports and memory.

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# 1. YARN, then HDFS
/home/ashok_ubun/hadoop/sbin/stop-yarn.sh
/home/ashok_ubun/hadoop/sbin/stop-dfs.sh

# 2. PostgreSQL custom cluster
/usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa stop

# 3. MongoDB (started with --fork; admin shutdown command)
mongosh --quiet --eval 'db.getSiblingDB("admin").shutdownServer()' || sudo pkill mongod
```

Verify everything is gone:
```bash
jps                                                                        # only "Jps"
pgrep -a mongod                                                            # no output
/usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa status   # "no server running"
```

If any service refuses to stop:
```bash
pkill -f hadoop
pkill -f mongod
pkill -f postgres
```

---

## Running the Pipelines

Always run from the project directory:
```bash
cd /home/ashok_ubun/studies_ubun/nosql/final_proj
```

### Batching model
**Each input log file = one batch.** Pass one or more files via `--inputs`; batch IDs are assigned in order. So with both NASA datasets:
```
--inputs data/NASA_access_log_Jul95 data/NASA_access_log_Aug95
```
gives `batch_id=1` (July) and `batch_id=2` (August), matching the eval guideline's canonical interpretation.

There is no user-facing chunk size: Python streams records to HDFS / MongoDB in 50,000-record buffers internally (`STREAM_CHUNK_SIZE` in [config.py](config.py)) for memory efficiency, but this is invisible in `batch_log` (one row per input file).

### MapReduce pipeline
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline mapreduce --inputs data/NASA_access_log_Jul95 data/NASA_access_log_Aug95
```

### MongoDB pipeline
```bash
python3 main.py --pipeline mongodb --inputs data/NASA_access_log_Jul95 data/NASA_access_log_Aug95
```

### Pig pipeline
Requires Apache Pig at `/home/ashok_ubun/pig/` (already installed). Same Hadoop services as MapReduce.
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline pig --inputs data/NASA_access_log_Jul95
```
Expect ~25-35 min on full Jul95. Pig is dominated by YARN per-job container overhead on single-node WSL — not by data size.

### Hive pipeline
Requires Apache Hive at `/home/ashok_ubun/hive/` with Derby metastore initialized (see "Install Apache Hive" above). Uses JDK 8 internally; Hadoop services must be running.
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline hive --inputs data/NASA_access_log_Jul95
```
Expect ~6-12 min on full Jul95. Hive runs all three queries from one bundled `.hql` script, so it pays one JVM cold-start instead of three.

### Per-query selection
Add `--query q1` (or `q2`, `q3`) to run only one query; default is `all`.
```bash
python3 main.py --pipeline hive --query q2 --inputs data/test.log
```

### Run all 4 pipelines sequentially (one command)
`--pipeline all` runs MapReduce → MongoDB → Pig → Hive in order, each producing its own `run_id`. If one pipeline fails, the others still continue and the exit code is non-zero.
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline all --inputs data/NASA_access_log_Jul95 data/NASA_access_log_Aug95
```
Expected total: ~35-55 min on full Jul+Aug (Pig dominates).

### Quick test (1,000 lines, fast)
```bash
head -1000 data/NASA_access_log_Jul95 > data/test.log
python3 main.py --pipeline mongodb   --inputs data/test.log
python3 main.py --pipeline mapreduce --inputs data/test.log
python3 main.py --pipeline pig       --inputs data/test.log
python3 main.py --pipeline hive      --inputs data/test.log
```

---

## Viewing Reports

```bash
python3 main.py --report --run-id <run_id>
```

List all past runs:
```bash
psql -U nasa_user -d nasa_logs -p 5433 -h /home/ashok_ubun/pgdata_nasa/socket \
  -c "SELECT run_id, pipeline_name, total_records, runtime_seconds, started_at FROM pipeline_runs ORDER BY run_id;"
```

---

## Reset all run data (keep schema)

Wipes every previous run's data — PostgreSQL rows, MongoDB documents, and HDFS staged/output dirs — but keeps the table schemas intact, so you can immediately run pipelines again. **This does not drop tables; if you need to recreate the schema, re-run `setup.sql` after this.**

```bash
# 1. PostgreSQL: truncate the 5 tables and reset run_id back to 1
psql -U nasa_user -d nasa_logs -p 5433 -h /home/ashok_ubun/pgdata_nasa/socket -c "
TRUNCATE q3_hourly_errors, q2_top_resources, q1_daily_traffic, batch_log, pipeline_runs RESTART IDENTITY;
"

# 2. MongoDB: drop the log_records collection (recreated automatically on next mongo run)
mongosh --quiet --eval 'db.getSiblingDB("nasa_etl").log_records.drop()'

# 3. HDFS: remove all staging + per-engine output dirs
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
hdfs dfs -rm -r -f /user/nasa_etl/staged /user/nasa_etl/output /user/nasa_etl/pig_output /user/nasa_etl/hive_output

# (optional) re-create the empty parent dirs so HDFS-aware pipelines find them
hdfs dfs -mkdir -p /user/nasa_etl/staged /user/nasa_etl/output /user/nasa_etl/pig_output /user/nasa_etl/hive_output
```

Verify:
```bash
psql -U nasa_user -d nasa_logs -p 5433 -h /home/ashok_ubun/pgdata_nasa/socket \
  -c "SELECT COUNT(*) FROM pipeline_runs;"   # should print 0
hdfs dfs -ls /user/nasa_etl/staged             # should be empty
```

---

## Re-running After Errors

### HDFS output directory already exists error
```bash
# MapReduce
hdfs dfs -rm -r -f /user/nasa_etl/output/run_<run_id>
# Pig
hdfs dfs -rm -r -f /user/nasa_etl/pig_output/run_<run_id> /user/nasa_etl/staged/run_<run_id>
# Hive
hdfs dfs -rm -r -f /user/nasa_etl/hive_output/run_<run_id> /user/nasa_etl/staged/run_<run_id>
```

### PostgreSQL not accepting connections
```bash
/usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa status
# If not running:
/usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa \
    -o "-p 5433 -k /home/ashok_ubun/pgdata_nasa/socket" \
    -l /home/ashok_ubun/pgdata_nasa/pg.log start
```

### Delete a bad run from PostgreSQL
```bash
psql -U nasa_user -d nasa_logs -p 5433 -h /home/ashok_ubun/pgdata_nasa/socket -c "
DELETE FROM q3_hourly_errors  WHERE run_id = <run_id>;
DELETE FROM q2_top_resources  WHERE run_id = <run_id>;
DELETE FROM q1_daily_traffic  WHERE run_id = <run_id>;
DELETE FROM batch_log         WHERE run_id = <run_id>;
DELETE FROM pipeline_runs     WHERE run_id = <run_id>;
"
```

---

## CLI Reference

| Command | Description |
|---|---|
| `python3 main.py --pipeline mapreduce --inputs PATH [PATH...]` | Run MapReduce ETL (each input = one batch) |
| `python3 main.py --pipeline mongodb --inputs PATH [PATH...]` | Run MongoDB ETL |
| `python3 main.py --pipeline pig --inputs PATH [PATH...]` | Run Pig ETL |
| `python3 main.py --pipeline hive --inputs PATH [PATH...]` | Run Hive ETL |
| `python3 main.py --pipeline all --inputs PATH [PATH...]` | Run all 4 pipelines sequentially |
| `--query {q1,q2,q3,all}` | (optional) Run only one of the queries; default `all` |
| `--input PATH` | Single-file alias for `--inputs` (backwards-compat) |
| `python3 main.py --report --run-id N` | Display report for run N |

**Per-query example** (run only Q2 via Hive):
```bash
python3 main.py --pipeline hive --query q2 --inputs data/test.log
```
| `jps` | Check which Hadoop processes are running |
| `hdfs dfs -ls /user/nasa_etl/` | Browse HDFS workspace |
| `pgrep mongod` | Check if MongoDB is running |
