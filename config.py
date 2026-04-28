import os

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
MR_JOBS_DIR = os.path.join(BASE_DIR, 'pipelines', 'mapreduce', 'mr_jobs')

STREAMING_JAR = '/home/ashok_ubun/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar'
JAVA_HOME     = '/usr/lib/jvm/java-21-openjdk-amd64'

HDFS_BASE       = 'hdfs://localhost:9000'
HDFS_STAGED_DIR = '/user/nasa_etl/staged'
HDFS_OUTPUT_DIR = '/user/nasa_etl/output'

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
