import psycopg2
from config import DB_CONFIG


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def fetch_run_meta(conn, run_id: int) -> tuple[str, object]:
    """Return (pipeline_name, started_at) for a run, used by loaders to stamp result rows."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pipeline_name, started_at FROM pipeline_runs WHERE run_id = %s",
            (run_id,),
        )
        return cur.fetchone()
