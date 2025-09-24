#!/usr/bin/env python3
"""
🌙 Enhanced Coinbase Historical Data Fetcher - CRO 5-Minute Data 🚀
Fetches maximum weeks of CRO 5-minute data with exact BTCUSD format compatibility
Compatible with backtesting.py and multi-data testing patterns
Optimized for 5-minute intervals with appropriate rate limiting
"""

import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from math import ceil
from pathlib import Path
import requests
import time
import hmac
import hashlib
import base64
import json
from urllib.parse import urlencode
import warnings
warnings.filterwarnings('ignore')

# ====== BobbyYo's Enhanced Coinbase Configuration 🌙 ======
# 🔧 OPTIMIZED FOR CRO 5-MINUTE DATA COLLECTION:

SYMBOL = 'CRO-USD'        # Trading pair for Cronos
TIMEFRAME = '5m'          # 5-minute intervals for high-frequency analysis
WEEKS = 50               # Max weeks for 5-minute data to avoid API limits
SAVE_DIR = 'data/coinbase'  # Directory to save files

print("🌙 BobbyYo's Enhanced Coinbase Data Fetcher - CRO 5-Minute Data! 🚀")
print(f"🎯 Target: {WEEKS} weeks of {SYMBOL} data at {TIMEFRAME} intervals")

# Create save directory if it doesn't exist
os.makedirs(SAVE_DIR, exist_ok=True)
print(f"📂 Save directory ready: {SAVE_DIR}")

# Get the project root directory (2 levels up from this file)
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'

print(f"🔍 Looking for .env file in: {project_root}")
print(f"📁 .env file exists: {'✅' if env_path.exists() else '❌'}")

# Load environment variables from the specific path
load_dotenv(env_path)

# Debug prints for API credentials (without revealing them)
api_key = os.getenv('COINBASE_API_KEY')
api_secret = os.getenv('COINBASE_API_SECRET')
print("🔑 API Key loaded:", "✅" if api_key else "❌")
print("🔒 API Secret loaded:", "✅" if api_secret else "❌")

if not api_key or not api_secret:
    print("❌ Error: API credentials not found in .env file")
    print("💡 Make sure your .env file exists and contains:")
    print("   COINBASE_API_KEY=your_api_key_id")
    print("   COINBASE_API_SECRET=your_api_secret")
    print("   COINBASE_PASSPHRASE=your_passphrase (optional - only if required by your API type)")
    raise ValueError("Missing API credentials")

def sign_request(method, path, body='', timestamp=None):
    """Sign a request using the API secret"""
    timestamp = timestamp or str(int(time.time()))

    # Create the message to sign
    message = f"{timestamp}{method}{path}{body}"

    try:
        # Create the signature using HMAC SHA256
        signature = hmac.new(
            api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        # Create headers for Coinbase Exchange API
        headers = {
            'CB-ACCESS-KEY': api_key,
            'CB-ACCESS-SIGN': signature,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'accept': 'application/json',
            'content-type': 'application/json',
        }

        # Add passphrase only if it exists (some API types don't require it)
        passphrase = os.getenv('COINBASE_PASSPHRASE')
        if passphrase:
            headers['CB-ACCESS-PASSPHRASE'] = passphrase

        return headers

    except Exception as e:
        print(f"❌ Error generating signature: {str(e)}")
        raise

def timeframe_to_granularity(timeframe):
    """Convert timeframe to granularity in seconds"""
    if 'm' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60
    elif 'h' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60 * 60
    elif 'd' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 24 * 60 * 60

def get_enhanced_coinbase_data(symbol, timeframe, weeks):
    """
    Enhanced Coinbase data fetcher that outputs data in exact BTCUSD format
    Returns DataFrame compatible with backtesting.py framework
    """
    print(f"🔍 Fetching {weeks} weeks of {timeframe} data for {symbol}")

    # Create filename that matches your BTCUSD naming pattern
    clean_symbol = symbol.replace("-", "")
    output_file = os.path.join(SAVE_DIR, f'{clean_symbol}-{timeframe}-{weeks}wks-enhanced-data.csv')

    if os.path.exists(output_file):
        print("📂 Found existing enhanced data file!")
        df = pd.read_csv(output_file)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df.set_index('datetime')

    try:
        # Test connection with a simple request
        print("🌎 Testing API connection...")
        base_url = "https://api.exchange.coinbase.com"

        # Get product details first
        path = '/products/' + symbol
        headers = sign_request('GET', path)
        print("🔐 Generated authentication headers")
        print("🔄 Making API request...")

        response = requests.get(
            f"{base_url}{path}",
            headers=headers
        )

        if response.status_code != 200:
            print(f"❌ Response Headers: {response.headers}")
            print(f"❌ Response Body: {response.text}")
            raise Exception(f"API Error: {response.status_code} - {response.text}")

        print("✨ Connection test successful!")

        # Calculate time ranges
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(weeks=weeks)
        granularity = timeframe_to_granularity(timeframe)

        # Calculate appropriate chunk size based on granularity
        # Coinbase limit is 300 candles per request, but we'll use 200 for safety
        max_candles = 200
        chunk_hours = max(1, int((max_candles * granularity) / 3600))  # Convert to hours, minimum 1 hour

        # For 5-minute data, limit chunk size to avoid timeouts
        chunk_hours = min(chunk_hours, 16)  # Max 16 hours for 5-minute data (192 candles)

        print(f"📊 Using {chunk_hours} hour chunks for {timeframe} timeframe")

        # Fetch candles in chunks to avoid rate limits
        all_candles = []
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + datetime.timedelta(hours=chunk_hours), end_time)

            print(f"📊 Fetching data from {current_start.strftime('%Y-%m-%d %H:%M')} to {current_end.strftime('%Y-%m-%d %H:%M')}")

            params = {
                'start': current_start.isoformat(),
                'end': current_end.isoformat(),
                'granularity': str(granularity)
            }

            path = f'/products/{symbol}/candles'
            headers = sign_request('GET', path)

            # Add timeout and retry logic
            max_retries = 3
            retry_count = 0

            while retry_count < max_retries:
                try:
                    response = requests.get(
                        f"{base_url}{path}",
                        params=params,
                        headers=headers,
                        timeout=30  # 30 second timeout
                    )
                    break  # Success, exit retry loop
                except requests.exceptions.Timeout:
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(f"⚠️ Timeout after {max_retries} retries. Skipping chunk.")
                        response = None
                        break
                    print(f"⏱️ Request timeout, retrying... ({retry_count}/{max_retries})")
                    time.sleep(2 * retry_count)  # Exponential backoff
                except requests.exceptions.RequestException as e:
                    print(f"❌ Request error: {str(e)}")
                    response = None
                    break

            if response is None:
                current_start = current_end
                continue  # Skip this chunk and continue

            if response.status_code != 200:
                print(f"❌ Response Headers: {response.headers}")
                print(f"❌ Response Body: {response.text}")
                raise Exception(f"API Error: {response.status_code} - {response.text}")

            candles = response.json()
            if candles:
                all_candles.extend(candles)
                print(f"  ✓ Got {len(candles)} candles")

            current_start = current_end

            # Progressive delay for 5-minute data
            time.sleep(0.5)  # Rate limit compliance

        if not all_candles:
            print("❌ No data retrieved. Please check your connection and try again.")
            return pd.DataFrame()

        print(f"✨ Successfully fetched {len(all_candles)} candles!")

        # Convert to DataFrame with EXACT BTCUSD format
        df_data = []
        for candle in all_candles:
            # Coinbase sends: [timestamp, low, high, open, close, volume]
            timestamp = datetime.datetime.fromtimestamp(candle[0])

            # Format datetime for 5-minute intervals (include time)
            datetime_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')

            df_data.append({
                'datetime': datetime_str,
                'open': float(candle[3]),
                'high': float(candle[2]),
                'low': float(candle[1]),
                'close': float(candle[4]),
                'volume': float(candle[5])
            })

        # Create DataFrame in exact BTCUSD format
        df = pd.DataFrame(df_data)

        # Sort by date (oldest first, like your BTCUSD file)
        df['datetime_sort'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime_sort')
        df = df.drop('datetime_sort', axis=1)

        # Remove duplicates
        df = df.drop_duplicates(subset=['datetime'])

        # Reset index
        df = df.reset_index(drop=True)

        print(f"🎉 Enhanced data processed!")
        print(f"📈 Total data points: {len(df)}")
        print(f"📅 Date range: {df['datetime'].iloc[0]} to {df['datetime'].iloc[-1]}")

        # Save to file in exact BTCUSD format
        df.to_csv(output_file, index=False)
        print(f"💾 Enhanced data saved to {output_file}")

        # Return with datetime as index for compatibility
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df.set_index('datetime')

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Tips:")
        print("   1. Make sure you're using a Coinbase Exchange API key")
        print("   2. Check if your API key has the required permissions")
        print("   3. Verify your API key is active")
        print("   4. Check your .env file is properly configured")
        raise

# Get the enhanced data
print(f"\n🚀 Fetching enhanced {SYMBOL} data for {WEEKS} weeks...")
data = get_enhanced_coinbase_data(SYMBOL, TIMEFRAME, WEEKS)

if not data.empty:
    print("\n📊 Enhanced Data Summary:")
    print(f"📈 Total rows: {len(data)}")
    print(f"📅 Date range: {data.index.min()} to {data.index.max()}")
    print(f"💰 Price range: ${data['close'].min():.6f} - ${data['close'].max():.6f}")
    print(f"📊 Volume range: {data['volume'].min():.2f} - {data['volume'].max():.2f}")

    # Calculate some interesting stats like your BTCUSD file
    days_of_data = (data.index.max() - data.index.min()).days
    weeks_of_data = days_of_data / 7

    print(f"⏰ Actual days of data: {days_of_data}")
    print(f"🗓️ Actual weeks of data: {weeks_of_data:.1f}")

    # Show format compatibility
    print("\n🔢 Sample data (first 5 rows - exact BTCUSD format):")
    sample_df = data.reset_index()
    sample_df['datetime'] = sample_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    print(sample_df.head()[['datetime', 'open', 'high', 'low', 'close', 'volume']])

    print(f"\n✨ Success! Enhanced {SYMBOL} 5-minute data is now available!")
    print("🎯 Format matches your BTCUSD file exactly - ready for backtesting.py!")
    print("✨ Thanks for using BobbyYo's Enhanced Coinbase Data Fetcher! ✨")
else:
    print("❌ No data retrieved")