import psycopg2
import requests
import polars as pl
import time

# --- PostgreSQL config (adjust for Docker Compose) ---
PG_CONFIG = {
    'host': 'postgres',  # or 'localhost' if running outside docker-compose
    'port': 5432,
    'dbname': 'app_db',
    'user': 'app_user',
    'password': 'supersecret'
}

MAX_API_BATCH = 250

# --- Get active coin symbols from watchlists ---
def get_watchlist_coin_ids(limit=MAX_API_BATCH, offset=0):
    query = """
        SELECT DISTINCT coin_symbol
        FROM watchlist_items
        WHERE active = true
        ORDER BY coin_symbol
        LIMIT %s OFFSET %s;
    """
    with psycopg2.connect(**PG_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (limit, offset))
            results = cur.fetchall()
            return [r[0] for r in results]

# --- Log batch API request to postgres ---
def log_api_request(coin_ids, status, api_name='coingecko'):
    query = """
        INSERT INTO api_requests (api_name, requested_symbols, status)
        VALUES (%s, %s, %s)
    """
    with psycopg2.connect(**PG_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (api_name, ','.join(coin_ids), status))
        conn.commit()

# --- Fetch market prices from CoinGecko for a batch ---
def fetch_market_data(batch):
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'ids': ','.join(batch),
        'order': 'market_cap_desc',
        'per_page': len(batch),
        'page': 1,
        'sparkline': False
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# --- Convert API response to Polars DataFrame ---
def build_polars_df(data):
    return pl.DataFrame([
        {
            'id': coin['id'],
            'symbol': coin['symbol'],
            'name': coin['name'],
            'price': coin['current_price'],
            'market_cap': coin['market_cap'],
            'volume': coin['total_volume'],
        }
        for coin in data
    ])

# --- Main collector logic ---
def main():
    print("🔍 Starting batched fetch from PostgreSQL and CoinGecko")
    all_data = []
    offset = 0

    while True:
        coin_ids = get_watchlist_coin_ids(limit=MAX_API_BATCH, offset=offset)
        if not coin_ids:
            break

        print(f"📦 Fetching {len(coin_ids)} coins at offset {offset}")
        try:
            data = fetch_market_data(coin_ids)
            all_data.extend(data)
            log_api_request(coin_ids, status="success")
        except Exception as e:
            print(f"❌ Error fetching batch: {e}")
            log_api_request(coin_ids, status="error")
        offset += MAX_API_BATCH
        time.sleep(1)

    if not all_data:
        print("⚠️ No coin data fetched.")
        return

    df = build_polars_df(all_data)
    print("✅ Final DataFrame:")
    print(df.head())

if __name__ == "__main__":
    main()
