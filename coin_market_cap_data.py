'''
STEPS TO USE
1. Get API key from https://coinmarketcap.com/api/
2. Create a .env file in the root directory with:
   CMC_API_KEY="your_coinmarketcap_api_key_here"
3. Select the symbol and vs_currency you want to fetch data for
4. Select the time period (count parameter)
5. Run the script - data will be saved to data/coinmarketcap/ directory

NOTE: CoinMarketCap's free tier has limited historical data access
Basic plan: Only latest quotes
Hobbyist ($29/month): 30 days historical
Startup ($99/month): 90 days historical  
Standard ($499/month): 2 years historical
'''

# ====== BobbyYo's CoinMarketCap Configuration 🌙 ======
# 🔧 MODIFY THESE SETTINGS TO CHANGE WHAT DATA YOU FETCH:

SYMBOL = 'ETH'               # Symbol to fetch - CHANGE THIS:
                           # Popular symbols: 'BTC', 'ETH', 'SOL', 'ADA', 'DOT', 'MATIC', 'AVAX', 'LINK', 'UNI', 'XRP'
                           # More symbols: 'LTC', 'BCH', 'ALGO', 'ATOM', 'NEAR', 'FTM', 'SAND', 'MANA', 'AAVE', 'CRV'
                           # Find more at: https://coinmarketcap.com/api/documentation/v1/#operation/getV1CryptocurrencyMap

VS_CURRENCY = 'USD'          # Currency to price against - CHANGE THIS:
                           # Fiat: 'USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'HKD', 'SGD'
                           # Crypto: 'BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'DOT', 'MATIC', 'AVAX'
                           # Note: Free tier has limited currency options

COUNT = 30                   # Number of data points to fetch - CHANGE THIS:
                           # Range: Limited by your API plan
                           # Free tier: Only latest quotes (1 point)
                           # Paid plans: 30-730 days depending on subscription
                           # Examples: 7, 30, 90, 365

INTERVAL = 'daily'           # Interval - CHANGE THIS:
                           # Available: '1h', '2h', '3h', '4h', '6h', '8h', '12h', 'daily', 'weekly', 'monthly'
                           # Note: Free tier only supports 'daily'

SAVE_DIR = 'data/coinmarketcap'  # Directory to save files - CHANGE IF NEEDED:
                               # Examples: 'data/coinmarketcap', 'data/cmc_data', 'backup_data'

# ====== Imports ======
import pandas as pd
import datetime
import os
from pathlib import Path
import requests
import time
import json
from dotenv import load_dotenv

# Create save directory if it doesn't exist
os.makedirs(SAVE_DIR, exist_ok=True)
print(f"📂 Save directory ready: {SAVE_DIR}")

# Load environment variables
project_root = Path(__file__).parent
env_path = project_root / '.env'

print(f"🔍 Looking for .env file in: {project_root}")
print(f"📁 .env file exists: {'✅' if env_path.exists() else '❌'}")

load_dotenv(env_path)

# Get API credentials
api_key = os.getenv('CMC_API_KEY')
print("🔑 CMC API Key loaded:", "✅" if api_key else "❌")

if not api_key:
    print("❌ Error: CMC API key not found in .env file")
    print("💡 Make sure your .env file exists and contains:")
    print("   CMC_API_KEY=your_coinmarketcap_api_key_here")
    raise ValueError("Missing CMC API credentials")

print("🌙 BobbyYo's CoinMarketCap Data Fetcher Initialized! 🚀")

def get_coin_id(symbol):
    """Get CoinMarketCap ID for a given symbol"""
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }
    
    params = {
        'symbol': symbol
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            raise Exception(f"Map API Error: {response.status_code} - {response.text}")
            
        data = response.json()
        
        if data['data']:
            coin_id = data['data'][0]['id']
            coin_name = data['data'][0]['name']
            print(f"✅ Found {symbol}: {coin_name} (ID: {coin_id})")
            return coin_id
        else:
            raise Exception(f"Symbol {symbol} not found")
            
    except Exception as e:
        print(f"❌ Error getting coin ID: {str(e)}")
        raise

def get_cmc_historical_data(symbol, vs_currency, count, interval):
    """Fetch historical data from CoinMarketCap API"""
    print(f"🔍 BobbyYo is fetching {count} {interval} data points for {symbol}/{vs_currency}")
    
    output_file = os.path.join(SAVE_DIR, f'{symbol}{vs_currency}-{interval}-{count}pts-cmc-data.csv')
    if os.path.exists(output_file):
        print("📂 Found existing data file!")
        return pd.read_csv(output_file, parse_dates=['datetime'], index_col='datetime')

    try:
        # Get coin ID first
        coin_id = get_coin_id(symbol)
        
        # Try historical endpoint first (requires paid plan)
        print("🌎 Trying historical quotes endpoint...")
        historical_data = get_historical_quotes(coin_id, vs_currency, count, interval)
        
        if historical_data is not None:
            df = historical_data
        else:
            # Fallback to latest quotes for free tier
            print("🔄 Falling back to latest quotes (free tier limitation)...")
            df = get_latest_quotes_fallback(coin_id, vs_currency)
        
        if not df.empty:
            # Save to file
            df.to_csv(output_file)
            print(f"💾 Data saved to {output_file}")
            
        return df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Tips:")
        print("   1. Check if your API key is valid")
        print("   2. Verify you have the right subscription plan for historical data")
        print("   3. Check if the symbol exists on CoinMarketCap")
        raise

def get_historical_quotes(coin_id, vs_currency, count, interval):
    """Get historical quotes (requires paid subscription)"""
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/historical'
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }
    
    # Calculate time range
    end_time = datetime.datetime.utcnow()
    
    # Calculate start time based on interval and count
    if interval == '1h':
        start_time = end_time - datetime.timedelta(hours=count)
    elif interval == '2h':
        start_time = end_time - datetime.timedelta(hours=count*2)
    elif interval == '3h':
        start_time = end_time - datetime.timedelta(hours=count*3)
    elif interval == '4h':
        start_time = end_time - datetime.timedelta(hours=count*4)
    elif interval == '6h':
        start_time = end_time - datetime.timedelta(hours=count*6)
    elif interval == '8h':
        start_time = end_time - datetime.timedelta(hours=count*8)
    elif interval == '12h':
        start_time = end_time - datetime.timedelta(hours=count*12)
    elif interval == 'daily':
        start_time = end_time - datetime.timedelta(days=count)
    elif interval == 'weekly':
        start_time = end_time - datetime.timedelta(weeks=count)
    elif interval == 'monthly':
        start_time = end_time - datetime.timedelta(days=count*30)
    else:
        start_time = end_time - datetime.timedelta(days=count)
    
    params = {
        'id': coin_id,
        'time_start': start_time.isoformat(),
        'time_end': end_time.isoformat(),
        'interval': interval,
        'count': count,
        'convert': vs_currency
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 400:
            # Likely free tier limitation
            print("⚠️ Historical data not available (requires paid subscription)")
            return None
        elif response.status_code != 200:
            raise Exception(f"Historical API Error: {response.status_code} - {response.text}")
            
        data = response.json()
        
        if not data['data'] or 'quotes' not in data['data']:
            print("⚠️ No historical data returned")
            return None
            
        quotes = data['data']['quotes']
        print(f"✨ Successfully fetched {len(quotes)} historical quotes!")
        
        # Convert to DataFrame
        df_data = []
        for quote in quotes:
            timestamp = datetime.datetime.fromisoformat(quote['timestamp'].replace('Z', '+00:00'))
            quote_data = quote['quote'][vs_currency]
            
            df_data.append({
                'datetime': timestamp,
                'open': float(quote_data.get('open', quote_data['price'])),
                'high': float(quote_data.get('high', quote_data['price'])),
                'low': float(quote_data.get('low', quote_data['price'])),
                'close': float(quote_data['price']),
                'volume': float(quote_data.get('volume_24h', 0.0))
            })
        
        df = pd.DataFrame(df_data)
        df = df.set_index('datetime')
        df = df.sort_index()
        
        return df
        
    except Exception as e:
        print(f"❌ Historical quotes error: {str(e)}")
        return None

def get_latest_quotes_fallback(coin_id, vs_currency):
    """Fallback method for free tier - get latest quotes only"""
    print(f"🔄 Using latest quotes fallback for free tier")
    
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }
    
    params = {
        'id': coin_id,
        'convert': vs_currency
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            raise Exception(f"Latest quotes API Error: {response.status_code} - {response.text}")
            
        data = response.json()
        coin_data = data['data'][str(coin_id)]
        quote_data = coin_data['quote'][vs_currency]
        
        print(f"✨ Successfully fetched latest quote!")
        
        # Create single-row DataFrame
        timestamp = datetime.datetime.fromisoformat(quote_data['last_updated'].replace('Z', '+00:00'))
        
        df_data = [{
            'datetime': timestamp,
            'open': float(quote_data['price']),  # No OHLC data in latest quotes
            'high': float(quote_data['price']),
            'low': float(quote_data['price']),
            'close': float(quote_data['price']),
            'volume': float(quote_data.get('volume_24h', 0.0))
        }]
        
        df = pd.DataFrame(df_data)
        df = df.set_index('datetime')
        
        print("⚠️ Note: Free tier only provides latest quote, not historical OHLC data")
        print("💡 Upgrade to paid plan for historical data access")
        
        return df
        
    except Exception as e:
        print(f"❌ Latest quotes fallback failed: {str(e)}")
        raise

# Rate limiting compliance
def respect_rate_limits():
    """CoinMarketCap allows 10 calls per minute for free tier"""
    print("⏱️ Respecting CoinMarketCap rate limits...")
    time.sleep(7)  # 7 second delay between requests (8.5 calls/minute max)

# Get the data
print(f"\n🚀 Fetching {SYMBOL} data in {VS_CURRENCY}...")
respect_rate_limits()
data = get_cmc_historical_data(SYMBOL, VS_CURRENCY, COUNT, INTERVAL)

if not data.empty:
    print("\n📊 Data Summary:")
    print(f"📈 Rows: {len(data)}")
    print(f"📅 Date range: {data.index.min()} to {data.index.max()}")
    print(f"💰 Price range: ${data['close'].min():.2f} - ${data['close'].max():.2f}")
    print("\n🔢 Sample data:")
    print(data.head())
    print("\n✨ Thanks for using BobbyYo's CoinMarketCap Data Fetcher! ✨")
else:
    print("❌ No data retrieved")