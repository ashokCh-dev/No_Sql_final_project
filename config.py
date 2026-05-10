import os

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
MR_JOBS_DIR = os.path.join(BASE_DIR, 'pipelines', 'mapreduce', 'mr_jobs')
PIG_SCRIPTS_DIR = os.path.join(BASE_DIR, 'pipelines', 'pig', 'scripts')

STREAMING_JAR = '/home/ashok_ubun/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar'
JAVA_HOME     = '/usr/lib/jvm/java-21-openjdk-amd64'

PIG_HOME = '/home/ashok_ubun/pig'
PIG_BIN  = os.path.join(PIG_HOME, 'bin', 'pig')

HADOOP_HOME      = '/home/ashok_ubun/hadoop'
HIVE_HOME        = '/home/ashok_ubun/hive'
HIVE_BIN         = os.path.join(HIVE_HOME, 'bin', 'hive')
HIVE_SCRIPTS_DIR = os.path.join(BASE_DIR, 'pipelines', 'hive', 'scripts')
HIVE_JAVA_HOME   = '/home/ashok_ubun/jdk8'

HDFS_BASE            = 'hdfs://localhost:9000'
HDFS_STAGED_DIR      = '/user/nasa_etl/staged'
HDFS_OUTPUT_DIR      = '/user/nasa_etl/output'
HDFS_PIG_OUTPUT_DIR  = '/user/nasa_etl/pig_output'
HDFS_HIVE_OUTPUT_DIR = '/user/nasa_etl/hive_output'

# Memory-buffer flush size used during ingest. NOT the batching unit.
# Batches are input log files (Jul95 = batch 1, Aug95 = batch 2, etc.).
# This constant controls how many records we hold in Python before
# flushing one TSV chunk to HDFS / one insert_many to MongoDB so RSS
# stays bounded on multi-million-line input files.
STREAM_CHUNK_SIZE = 50_000

DB_CONFIG = {
    'dbname':   'nasa_logs',
    'user':     'nasa_user',
    'host':     '/home/ashok_ubun/pgdata_nasa/socket',
    'port':     5433,
}

# ── MongoDB ────────────────────────────────────────────────────────────────────
MONGO_URI        = 'mongodb://localhost:27017/'
MONGO_DB         = 'nasa_etl'
MONGO_COLLECTION = 'log_records'
