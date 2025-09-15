#!/usr/bin/env python3
"""
Enhanced Coinbase XRP Daily Data Fetcher
Fetches maximum available daily XRP data from Coinbase for backtesting accuracy
"""

import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from pathlib import Path
import requests
import time
import hmac
import hashlib
import warnings
warnings.filterwarnings('ignore')

# Configuration for maximum XRP daily data
SYMBOL = 'XRP-USD'
TIMEFRAME = '1d'  # Daily data for long-term backtesting
WEEKS = 500  # Maximum weeks - will fetch as much as available

SAVE_DIR = 'data/coinbase'
os.makedirs(SAVE_DIR, exist_ok=True)

# Load environment variables
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

api_key = os.getenv('COINBASE_API_KEY')
api_secret = os.getenv('COINBASE_API_SECRET')

print(f"Fetching maximum XRP daily data from Coinbase...")
print(f"Target: {WEEKS} weeks of {SYMBOL} data at {TIMEFRAME} intervals")

def sign_request(method, path, body='', timestamp=None):
    """Sign a request using the API secret"""
    timestamp = timestamp or str(int(time.time()))
    message = f"{timestamp}{method}{path}{body}"

    signature = hmac.new(
        api_secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    headers = {
        'CB-ACCESS-KEY': api_key,
        'CB-ACCESS-SIGN': signature,
        'CB-ACCESS-TIMESTAMP': timestamp,
        'accept': 'application/json',
        'content-type': 'application/json',
    }

    passphrase = os.getenv('COINBASE_PASSPHRASE')
    if passphrase:
        headers['CB-ACCESS-PASSPHRASE'] = passphrase

    return headers

def get_xrp_daily_data():
    """Fetch maximum available daily XRP data from Coinbase"""

    # Create filename
    output_file = os.path.join(SAVE_DIR, f'XRPUSD-1d-{WEEKS}wks-enhanced-data.csv')

    if os.path.exists(output_file):
        print(f"Found existing file: {output_file}")
        df = pd.read_csv(output_file)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df.set_index('datetime')

    try:
        base_url = "https://api.exchange.coinbase.com"

        # Test connection
        print("Testing API connection...")
        path = '/products/' + SYMBOL
        headers = sign_request('GET', path)
        response = requests.get(f"{base_url}{path}", headers=headers, timeout=30)

        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code} - {response.text}")

        print("Connection successful!")

        # Calculate time ranges
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(weeks=WEEKS)

        # For daily data, we can fetch larger chunks
        granularity = 86400  # 1 day in seconds
        max_candles = 300  # Coinbase limit
        chunk_days = max_candles - 1  # 299 days per chunk for safety

        print(f"Using {chunk_days} day chunks for daily data")

        # Fetch candles in chunks
        all_candles = []
        current_start = start_time
        total_requests = 0

        while current_start < end_time:
            current_end = min(current_start + datetime.timedelta(days=chunk_days), end_time)

            print(f"Fetching from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

            params = {
                'start': current_start.isoformat(),
                'end': current_end.isoformat(),
                'granularity': str(granularity)
            }

            path = f'/products/{SYMBOL}/candles'
            headers = sign_request('GET', path)

            max_retries = 3
            for retry in range(max_retries):
                try:
                    response = requests.get(
                        f"{base_url}{path}",
                        params=params,
                        headers=headers,
                        timeout=30
                    )

                    if response.status_code == 200:
                        candles = response.json()
                        if candles:
                            all_candles.extend(candles)
                            print(f"  Got {len(candles)} candles")
                        break
                    else:
                        print(f"  API Error: {response.status_code}")
                        if retry < max_retries - 1:
                            time.sleep(2 ** retry)

                except requests.exceptions.Timeout:
                    print(f"  Timeout (retry {retry + 1}/{max_retries})")
                    if retry < max_retries - 1:
                        time.sleep(2 ** retry)
                except Exception as e:
                    print(f"  Error: {str(e)}")
                    break

            current_start = current_end
            total_requests += 1

            # Rate limiting
            time.sleep(0.5)

            # Progress update
            if total_requests % 5 == 0:
                print(f"Progress: {len(all_candles)} candles fetched so far...")

        if not all_candles:
            print("No data retrieved")
            return pd.DataFrame()

        print(f"Successfully fetched {len(all_candles)} daily candles!")

        # Convert to DataFrame
        df_data = []
        for candle in all_candles:
            timestamp = datetime.datetime.fromtimestamp(candle[0])

            df_data.append({
                'datetime': timestamp.strftime('%Y-%m-%d'),
                'open': float(candle[3]),
                'high': float(candle[2]),
                'low': float(candle[1]),
                'close': float(candle[4]),
                'volume': float(candle[5])
            })

        # Create DataFrame
        df = pd.DataFrame(df_data)

        # Sort by date (oldest first)
        df['datetime_sort'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime_sort')
        df = df.drop('datetime_sort', axis=1)

        # Remove duplicates
        df = df.drop_duplicates(subset=['datetime'])
        df = df.reset_index(drop=True)

        print(f"Total data points: {len(df)}")
        print(f"Date range: {df['datetime'].iloc[0]} to {df['datetime'].iloc[-1]}")

        # Save to file
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

        # Return with datetime as index
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df.set_index('datetime')

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

# Fetch the data
data = get_xrp_daily_data()

if not data.empty:
    print("\nData Summary:")
    print(f"Total rows: {len(data)}")
    print(f"Date range: {data.index.min()} to {data.index.max()}")
    print(f"Price range: ${data['close'].min():.4f} - ${data['close'].max():.4f}")
    print(f"Average daily volume: ${data['volume'].mean():,.0f}")

    # Calculate actual time coverage
    days_of_data = (data.index.max() - data.index.min()).days
    years_of_data = days_of_data / 365.25

    print(f"Actual coverage: {days_of_data} days ({years_of_data:.1f} years)")

    # Show recent data
    print("\nMost recent 5 days:")
    recent_df = data.tail(5).reset_index()
    recent_df['datetime'] = recent_df['datetime'].dt.strftime('%Y-%m-%d')
    print(recent_df[['datetime', 'open', 'high', 'low', 'close', 'volume']])

    print("\nXRP daily data successfully fetched from Coinbase!")
else:
    print("No data retrieved")