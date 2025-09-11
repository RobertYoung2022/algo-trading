'''
STEPS TO USE
1. Select the symbol and vs_currency you want to fetch data for
2. Select the timeframe (minute, hour, day)  
3. Select the limit (number of data points)
4. Run the script - data will be saved to data/cryptocompare/ directory

NOTE: CryptoCompare API limitations:
- Free tier: 100,000 calls/month
- Rate limit: 100 calls/second (we use 1 call/second to be safe)
- Historical data: 2000 data points max per request
'''

# ====== BobbyYo's CryptoCompare Configuration 🌙 ======
SYMBOL = 'ETH'               # Symbol to fetch (e.g., 'BTC', 'ETH', 'SOL')
VS_CURRENCY = 'USDT'         # Currency to price against (e.g., 'USD', 'USDT', 'BTC', 'EUR')
TIMEFRAME = 'day'            # Timeframe: 'minute', 'hour', 'day'
LIMIT = 100                  # Number of data points to fetch (max 2000)
SAVE_DIR = 'data/cryptocompare'  # Directory to save the data files

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

print("🌙 BobbyYo's CryptoCompare Data Fetcher Initialized! 🚀")

def get_cryptocompare_data(symbol, vs_currency, timeframe, limit):
    """Fetch OHLCV data from CryptoCompare API"""
    print(f"🔍 BobbyYo is fetching {limit} {timeframe} data points for {symbol}/{vs_currency}")
    
    output_file = os.path.join(SAVE_DIR, f'{symbol}{vs_currency}-{timeframe}-{limit}pts-cc-data.csv')
    if os.path.exists(output_file):
        print("📂 Found existing data file!")
        return pd.read_csv(output_file, parse_dates=['datetime'], index_col='datetime')

    try:
        # Determine the correct endpoint based on timeframe
        if timeframe.lower() in ['minute', 'min', '1m', 'm']:
            endpoint = 'histominute'
            timeframe_name = 'minute'
        elif timeframe.lower() in ['hour', 'hr', '1h', 'h']:
            endpoint = 'histohour'  
            timeframe_name = 'hour'
        elif timeframe.lower() in ['day', 'daily', '1d', 'd']:
            endpoint = 'histoday'
            timeframe_name = 'day'
        else:
            # Default to daily
            endpoint = 'histoday'
            timeframe_name = 'day'
            print(f"⚠️ Unknown timeframe '{timeframe}', defaulting to daily")
        
        url = f'https://min-api.cryptocompare.com/data/v2/{endpoint}'
        
        params = {
            'fsym': symbol.upper(),
            'tsym': vs_currency.upper(),
            'limit': min(limit, 2000),  # CryptoCompare max is 2000
            'aggregate': 1  # Get every data point
        }
        
        print(f"🌎 Fetching from {endpoint} endpoint...")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code} - {response.text}")
            
        data = response.json()
        
        # Check API response
        if data.get('Response') == 'Error':
            raise Exception(f"CryptoCompare API Error: {data.get('Message', 'Unknown error')}")
            
        if 'Data' not in data or 'Data' not in data['Data']:
            raise Exception("No data returned from API")
            
        raw_data = data['Data']['Data']
        print(f"✨ Successfully fetched {len(raw_data)} {timeframe_name} candles!")
        
        # Convert to DataFrame
        df_data = []
        for candle in raw_data:
            # Skip incomplete candles (volume = 0 usually means no trading)
            if candle['volumeto'] == 0 and candle['volumefrom'] == 0:
                continue
                
            timestamp = datetime.datetime.fromtimestamp(candle['time'])
            
            df_data.append({
                'datetime': timestamp,
                'open': float(candle['open']),
                'high': float(candle['high']),
                'low': float(candle['low']),
                'close': float(candle['close']),
                'volume': float(candle['volumeto'])  # Volume in target currency
            })
        
        if not df_data:
            raise Exception("No valid candles found in the data")
            
        df = pd.DataFrame(df_data)
        df = df.set_index('datetime')
        df = df.sort_index()
        
        # Remove any duplicate timestamps
        df = df[~df.index.duplicated(keep='first')]
        
        # Save to file
        df.to_csv(output_file)
        print(f"💾 Data saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Tips:")
        print("   1. Check if the symbol exists on CryptoCompare")
        print("   2. Verify the vs_currency is supported")
        print("   3. Try reducing the limit if you're hitting API limits")
        print("   4. Check your internet connection")
        raise

def get_supported_coins():
    """Get list of supported coins from CryptoCompare"""
    try:
        url = 'https://min-api.cryptocompare.com/data/all/coinlist'
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'Success':
                coins = list(data['Data'].keys())
                print(f"📋 Found {len(coins)} supported coins")
                return coins[:20]  # Return first 20 for display
        return []
    except:
        return []

def validate_symbol_pair(symbol, vs_currency):
    """Validate that the symbol pair is available"""
    try:
        url = 'https://min-api.cryptocompare.com/data/price'
        params = {
            'fsym': symbol.upper(),
            'tsyms': vs_currency.upper()
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if vs_currency.upper() in data:
                price = data[vs_currency.upper()]
                print(f"✅ {symbol}/{vs_currency} pair is valid. Current price: {price}")
                return True
            else:
                print(f"❌ {symbol}/{vs_currency} pair not found")
                return False
        else:
            print(f"⚠️ Could not validate pair: {response.status_code}")
            return True  # Assume valid if we can't check
            
    except Exception as e:
        print(f"⚠️ Validation error: {str(e)}")
        return True  # Assume valid if we can't check

# Rate limiting compliance
def respect_rate_limits():
    """CryptoCompare allows 100 calls/second, we use 1/second to be safe"""
    print("⏱️ Respecting CryptoCompare rate limits...")
    time.sleep(1)  # 1 second delay between requests

# Validate the symbol pair first
print(f"\n🔍 Validating {SYMBOL}/{VS_CURRENCY} pair...")
if not validate_symbol_pair(SYMBOL, VS_CURRENCY):
    print("❌ Invalid symbol pair. Here are some popular coins:")
    supported = get_supported_coins()
    if supported:
        print("📋 Popular symbols:", ', '.join(supported))
    raise ValueError(f"Invalid symbol pair: {SYMBOL}/{VS_CURRENCY}")

# Get the data
print(f"\n🚀 Fetching {SYMBOL} data in {VS_CURRENCY}...")
respect_rate_limits()
data = get_cryptocompare_data(SYMBOL, VS_CURRENCY, TIMEFRAME, LIMIT)

if not data.empty:
    print("\n📊 Data Summary:")
    print(f"📈 Rows: {len(data)}")
    print(f"📅 Date range: {data.index.min()} to {data.index.max()}")
    
    # Handle case where close prices might be very small (for some pairs)
    if data['close'].max() >= 0.01:
        print(f"💰 Price range: {data['close'].min():.2f} - {data['close'].max():.2f} {VS_CURRENCY}")
    else:
        print(f"💰 Price range: {data['close'].min():.8f} - {data['close'].max():.8f} {VS_CURRENCY}")
    
    print(f"📊 Volume range: {data['volume'].min():.2f} - {data['volume'].max():.2f} {VS_CURRENCY}")
    print("\n🔢 Sample data:")
    print(data.head())
    print("\n✨ Thanks for using BobbyYo's CryptoCompare Data Fetcher! ✨")
else:
    print("❌ No data retrieved")