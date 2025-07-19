from common.coingecko_client import fetch_price_range
from datetime import datetime

def fetch_historical_prices(**kwargs):
    coin_ids = kwargs["params"]["coin_ids"]
    start = kwargs["params"]["start_date"]  # "2025-07-01"
    end = kwargs["params"]["end_date"]      # "2025-07-07"

    results = []
    for coin in coin_ids:
        prices = fetch_price_range(coin, start, end)
        for ts, price in prices:
            results.append({
                "coin": coin,
                "timestamp": ts,
                "price": price
            })

    kwargs["ti"].xcom_push(key="historical_prices", value=results)
