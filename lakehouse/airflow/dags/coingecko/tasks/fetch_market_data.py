import requests
import polars as pl
import io
from datetime import datetime

def fetch_market_data(**kwargs):
    coin_list = kwargs['ti'].xcom_pull(key='coin_list', task_ids='fetch_coins')
    prices = []

    for coin in coin_list[:100]:  # limit for example
        res = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={coin['id']}&vs_currencies=usd")
        if res.ok:
            prices.append({**coin, **res.json().get(coin['id'], {})})

    df = pl.DataFrame(prices)
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    kwargs['ti'].xcom_push(key='parquet_data', value=buffer.read())
