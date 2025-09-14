'''
STEPS TO USE
1. Select the coin you want to fetch data for (coin_id from CoinGecko)
2. Select the vs_currency (usd, eur, btc, etc.)
3. Select the number of days of data to fetch (1-365 for OHLC data)
4. Run the script - data will be saved to data/coingecko/ directory
'''

# ====== BobbyYo's CoinGecko Configuration 🌙 ======
# 🔧 MODIFY THESE SETTINGS TO CHANGE WHAT DATA YOU FETCH:

COIN_ID = 'ethereum'         # CoinGecko coin ID - CHANGE THIS:
                           # Popular coins: 'bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot', 'polygon', 'avalanche-2', 'chainlink', 'uniswap'
                           # More coins: 'ripple', 'litecoin', 'bitcoin-cash', 'algorand', 'cosmos', 'near', 'fantom', 'the-sandbox', 'decentraland'
                           # Find more at: https://api.coingecko.com/api/v3/coins/list

VS_CURRENCY = 'usd'          # Currency to price against - CHANGE THIS:
                           # Available: 'usd', 'eur', 'gbp', 'jpy', 'cad', 'aud', 'chf', 'cny', 'hkd', 'sgd'
                           # Crypto: 'btc', 'eth', 'bnb', 'ada', 'sol', 'dot', 'matic', 'avax'

DAYS = 90                    # Number of days of data to fetch - CHANGE THIS:
                           # Range: 1-365 days for OHLC data
                           # Examples: 7 (1 week), 30 (1 month), 90 (3 months), 180 (6 months), 365 (1 year)
                           # Note: More days = more data points

SAVE_DIR = 'data/coingecko'  # Directory to save files - CHANGE IF NEEDED:
                           # Examples: 'data/coingecko', 'data/historical', 'backup_data'

# ====== Imports ======
import pandas as pd
import datetime
import os
from pathlib import Path
import requests
import time
import json

# Create save directory if it doesn't exist
os.makedirs(SAVE_DIR, exist_ok=True)
print(f"📂 Save directory ready: {SAVE_DIR}")

print("🌙 BobbyYo's CoinGecko Data Fetcher Initialized! 🚀")

def get_coingecko_ohlc(coin_id, vs_currency, days):
    """Fetch OHLC data from CoinGecko API"""
    print(f"🔍 BobbyYo is fetching {days} days of OHLC data for {coin_id}/{vs_currency}")
    
    output_file = os.path.join(SAVE_DIR, f'{coin_id.upper()}{vs_currency.upper()}-{days}d-coingecko-data.csv')
    if os.path.exists(output_file):
        print("📂 Found existing data file!")
        return pd.read_csv(output_file, parse_dates=['datetime'], index_col='datetime')

    try:
        # CoinGecko OHLC endpoint
        url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc'
        params = {
            'vs_currency': vs_currency,
            'days': days
        }
        
        print("🌎 Testing API connection...")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code} - {response.text}")
            # Try market_chart endpoint as fallback
            print("🔄 Trying market_chart endpoint as fallback...")
            return get_coingecko_market_chart_fallback(coin_id, vs_currency, days)
            
        data = response.json()
        print(f"✨ Successfully fetched {len(data)} OHLC candles!")
        
        # Convert to DataFrame
        # CoinGecko OHLC format: [timestamp, open, high, low, close]
        df_data = []
        for candle in data:
            timestamp = datetime.datetime.fromtimestamp(candle[0] / 1000)
            df_data.append({
                'datetime': timestamp,
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': 0.0  # OHLC endpoint doesn't provide volume
            })
        
        df = pd.DataFrame(df_data)
        df = df.set_index('datetime')
        df = df.sort_index()
        
        # Save to file
        df.to_csv(output_file)
        print(f"💾 Data saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Tips:")
        print("   1. Check if the coin_id is correct (use CoinGecko coin list)")
        print("   2. Verify vs_currency is supported")
        print("   3. Try reducing the number of days")
        raise

def get_coingecko_market_chart_fallback(coin_id, vs_currency, days):
    """Fallback method using market_chart endpoint"""
    print(f"🔄 Using market_chart fallback for {coin_id}/{vs_currency}")
    
    try:
        # Market chart endpoint with price and volume
        url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart'
        params = {
            'vs_currency': vs_currency,
            'days': days,
            'interval': 'daily' if days > 90 else 'hourly'
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            raise Exception(f"Market chart API Error: {response.status_code} - {response.text}")
            
        data = response.json()
        
        # Extract prices and volumes
        prices = data.get('prices', [])
        volumes = data.get('total_volumes', [])
        
        print(f"✨ Successfully fetched {len(prices)} price points from market_chart!")
        
        # Create DataFrame with available data
        df_data = []
        volume_dict = {v[0]: v[1] for v in volumes} if volumes else {}
        
        for i, price_point in enumerate(prices):
            timestamp = datetime.datetime.fromtimestamp(price_point[0] / 1000)
            price = float(price_point[1])
            volume = volume_dict.get(price_point[0], 0.0)
            
            df_data.append({
                'datetime': timestamp,
                'open': price,  # Market chart doesn't provide OHLC, using price for all
                'high': price,
                'low': price,
                'close': price,
                'volume': float(volume)
            })
        
        df = pd.DataFrame(df_data)
        df = df.set_index('datetime')
        df = df.sort_index()
        
        output_file = os.path.join(SAVE_DIR, f'{coin_id.upper()}{vs_currency.upper()}-{days}d-coingecko-market-data.csv')
        df.to_csv(output_file)
        print(f"💾 Fallback data saved to {output_file}")
        print("⚠️ Note: Market chart data doesn't include separate OHLC values")
        
        return df
        
    except Exception as e:
        print(f"❌ Fallback method also failed: {str(e)}")
        raise

# Rate limiting compliance
def respect_rate_limits():
    """CoinGecko allows 5-15 calls per minute for free tier"""
    print("⏱️ Respecting CoinGecko rate limits...")
    time.sleep(5)  # 5 second delay between requests (12 calls/minute max)

# Get the data
print(f"\n🚀 Fetching {COIN_ID} data in {VS_CURRENCY}...")
respect_rate_limits()
data = get_coingecko_ohlc(COIN_ID, VS_CURRENCY, DAYS)

if not data.empty:
    print("\n📊 Data Summary:")
    print(f"📈 Rows: {len(data)}")
    print(f"📅 Date range: {data.index.min()} to {data.index.max()}")
    print(f"💰 Price range: ${data['close'].min():.2f} - ${data['close'].max():.2f}")
    print("\n🔢 Sample data:")
    print(data.head())
    print("\n✨ Thanks for using BobbyYo's CoinGecko Data Fetcher! ✨")
else:
    print("❌ No data retrieved")