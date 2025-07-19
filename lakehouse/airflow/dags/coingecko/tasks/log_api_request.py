from utils.db import get_pg_engine
import sqlalchemy as sa
from datetime import datetime

def log_api_request(**kwargs):
    coin_list = kwargs['ti'].xcom_pull(key='coin_list', task_ids='fetch_coins')
    engine = get_pg_engine()

    with engine.begin() as conn:
        conn.execute(
            sa.text("""
                INSERT INTO api_logs (timestamp, source, coins_requested)
                VALUES (:ts, :src, :coins)
            """),
            {"ts": datetime.utcnow(), "src": "coingecko", "coins": str([c['id'] for c in coin_list[:100]])}
        )
