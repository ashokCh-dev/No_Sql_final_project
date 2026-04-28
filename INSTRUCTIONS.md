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

## Running the Pipelines

Always run from the project directory:
```bash
cd /home/ashok_ubun/studies_ubun/nosql/final_proj
```

### MapReduce pipeline
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
python3 main.py --pipeline mapreduce --batch-size 50000 --input data/NASA_access_log_Jul95
```

### MongoDB pipeline
```bash
python3 main.py --pipeline mongodb --batch-size 50000 --input data/NASA_access_log_Jul95
```

### Quick test (1,000 lines, fast)
```bash
head -1000 data/NASA_access_log_Jul95 > data/test.log
python3 main.py --pipeline mongodb   --batch-size 200 --input data/test.log
python3 main.py --pipeline mapreduce --batch-size 200 --input data/test.log
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

## Re-running After Errors

### HDFS output directory already exists error
```bash
hdfs dfs -rm -r -f /user/nasa_etl/output/run_<run_id>
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
| `python3 main.py --pipeline mapreduce --input PATH --batch-size N` | Run MapReduce ETL |
| `python3 main.py --pipeline mongodb --input PATH --batch-size N` | Run MongoDB ETL |
| `python3 main.py --report --run-id N` | Display report for run N |
| `jps` | Check which Hadoop processes are running |
| `hdfs dfs -ls /user/nasa_etl/` | Browse HDFS workspace |
| `pgrep mongod` | Check if MongoDB is running |
