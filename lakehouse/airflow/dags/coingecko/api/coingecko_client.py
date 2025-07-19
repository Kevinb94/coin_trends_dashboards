import requests
from datetime import datetime
from time import sleep

def fetch_price_range(coin_id, start_date_str, end_date_str):
    """
    Fetch historical price data for a coin between two dates using market_chart/range.
    Returns a list of [timestamp, price_usd] pairs.
    """
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())

    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": from_ts,
        "to": to_ts
    }

    res = requests.get(url, params=params)
    res.raise_for_status()

    # returns list of [timestamp, price]
    return res.json().get("prices", [])
