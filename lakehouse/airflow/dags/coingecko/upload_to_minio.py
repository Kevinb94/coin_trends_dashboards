
from utils.storage import upload_to_minio
from datetime import datetime

def upload_parquet_to_minio(**kwargs):
    raw_bytes = kwargs['ti'].xcom_pull(key='parquet_data', task_ids='fetch_market_data')
    today = datetime.utcnow().strftime("%Y/%m/%d")
    path = f"historical_prices/api_name/{today}/coingecko_prices_{datetime.utcnow().strftime('%Y%m%d')}.parquet"
    upload_to_minio(path, raw_bytes)
