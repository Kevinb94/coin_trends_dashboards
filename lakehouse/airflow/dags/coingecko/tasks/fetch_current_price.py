
    from common.coingecko_client import get_current_price

def fetch_current_prices(**kwargs):
    coin_ids = kwargs["params"]["coin_ids"]
    prices = get_current_price(coin_ids)

    result = [{"coin": coin, "timestamp": datetime.utcnow().isoformat(), "price": prices[coin]["usd"]} for coin in coin_ids]
    kwargs["ti"].xcom_push(key="current_prices", value=result)
