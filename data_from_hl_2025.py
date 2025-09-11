'''
2025 NOTE:
THE MAX BARS OF DATA YOU CAN GET FROM HYPERLIQUID IS 5000
IF YOU NEED MORE USE THE COINBASE SCRIPT
NO MATTER WHAT THE VIDEO SAYS, ABOVE IS THE MOST UPDATED 
'''


import pandas as pd
import requests
from datetime import datetime, timedelta
import numpy as np
import time

# ====== BobbyYo's Hyperliquid Configuration 🌙 ======
SYMBOL = 'ETH'               # Symbol to fetch (e.g., 'BTC', 'ETH', 'SOL')
TIMEFRAME = '1h'             # Timeframe: '1m', '1h', '1d', '1w'
SAVE_DIR = 'data/hyperliquid'  # Directory to save the data files

# Define symbol and timeframe (for backward compatibility)
symbol = SYMBOL
timeframe = TIMEFRAME

# Constants
BATCH_SIZE = 5000 # MAX IS 5000 FOR HYPERLIQUID IF YOU NEED MORE USE COINBASE
MAX_RETRIES = 3
MAX_ROWS = 5000  # New constant to limit the number of rows

# Global variable to store timestamp offset
timestamp_offset = None

def adjust_timestamp(dt):
    """Adjust API timestamps by subtracting the timestamp offset."""
    if timestamp_offset is not None:
        corrected_dt = dt - timestamp_offset
        return corrected_dt
    else:
        return dt  # No adjustment needed if offset is not set

def get_ohlcv2(symbol, interval, start_time, end_time, batch_size=BATCH_SIZE):
    global timestamp_offset
    print(f'\n🔍 Requesting data:')
    print(f'📊 Batch Size: {batch_size}')
    
    if start_time and end_time:
        print(f'🚀 Start: {start_time.strftime("%Y-%m-%d %H:%M:%S")} UTC')
        print(f'🎯 End: {end_time.strftime("%Y-%m-%d %H:%M:%S")} UTC')
        start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(end_time.timestamp() * 1000)
    else:
        print('🚀 Requesting most recent data (no specific time range)')
        # Use current time and go back enough to get the requested number of candles
        end_ts = int(datetime.utcnow().timestamp() * 1000)
        # Calculate time range based on interval
        if interval == '1m':
            start_ts = end_ts - (5000 * 60 * 1000)  # 5000 minutes ago
        elif interval == '1h':
            start_ts = end_ts - (5000 * 60 * 60 * 1000)  # 5000 hours ago
        elif interval == '6h':
            start_ts = end_ts - (5000 * 6 * 60 * 60 * 1000)  # 5000 * 6 hours ago
        elif interval == '1d':
            start_ts = end_ts - (5000 * 24 * 60 * 60 * 1000)  # 5000 days ago
        elif interval == '1w':
            start_ts = end_ts - (5000 * 7 * 24 * 60 * 60 * 1000)  # 5000 weeks ago
        else:
            start_ts = end_ts - (60 * 24 * 60 * 60 * 1000)  # Default: 60 days ago

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                'https://api.hyperliquid.xyz/info',
                headers={'Content-Type': 'application/json'},
                json={
                    "type": "candleSnapshot",
                    "req": {
                        "coin": symbol,
                        "interval": interval,
                        "startTime": start_ts,
                        "endTime": end_ts,
                        "limit": batch_size
                    }
                },
                timeout=10
            )

            if response.status_code == 200:
                snapshot_data = response.json()
                if snapshot_data:
                    # Manually calculate timestamp offset if not already done
                    if timestamp_offset is None:
                        latest_api_timestamp = datetime.utcfromtimestamp(snapshot_data[-1]['t'] / 1000)
                        # Your system's current date (adjust to your actual current date)
                        system_current_date = datetime.utcnow()
                        # Manually set the expected latest timestamp (e.g., now)
                        expected_latest_timestamp = system_current_date
                        # Calculate offset
                        timestamp_offset = latest_api_timestamp - expected_latest_timestamp
                        print(f"⏱️ Calculated timestamp offset: {timestamp_offset}")
                    # Adjust timestamps due to API bug
                    for candle in snapshot_data:
                        dt = datetime.utcfromtimestamp(candle['t'] / 1000)
                        # Adjust date
                        adjusted_dt = adjust_timestamp(dt)
                        candle['t'] = int(adjusted_dt.timestamp() * 1000)
                    first_time = datetime.utcfromtimestamp(snapshot_data[0]['t'] / 1000)
                    last_time = datetime.utcfromtimestamp(snapshot_data[-1]['t'] / 1000)
                    print(f'✨ Received {len(snapshot_data)} candles')
                    print(f'📈 First: {first_time}')
                    print(f'📉 Last: {last_time}')
                    return snapshot_data
                else:
                    print('❌ No data returned by API')
                    return None
            else:
                print(f'⚠️ HTTP Error {response.status_code}: {response.text}')
        except requests.exceptions.RequestException as e:
            print(f'⚠️ Request failed (attempt {attempt + 1}): {e}')
            time.sleep(1)
    return None

def process_data_to_df(snapshot_data):
    if snapshot_data:
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        data = []
        for snapshot in snapshot_data:
            timestamp = datetime.utcfromtimestamp(snapshot['t'] / 1000)
            open_price = snapshot['o']
            high_price = snapshot['h']
            low_price = snapshot['l']
            close_price = snapshot['c']
            volume = snapshot['v']
            data.append([timestamp, open_price, high_price, low_price, close_price, volume])

        df = pd.DataFrame(data, columns=columns)
        return df
    else:
        return pd.DataFrame()

def fetch_historical_data(symbol, timeframe):
    """Fetch 5000 rows of historical data."""
    print("\n🌙 BobbyYo's Historical Data Fetcher")
    print(f"🎯 Symbol: {symbol}")
    print(f"⏰ Timeframe: {timeframe}")

    # Just request the most recent data without specific start/end times
    print("\n🔄 Fetching most recent data...")

    # Use a simple approach - request recent data
    data = get_ohlcv2(symbol, timeframe, None, None, batch_size=5000)
    
    if not data:
        print("❌ No data available.")
        return pd.DataFrame()

    df = process_data_to_df(data)

    if not df.empty:
        # Sort by timestamp and take the most recent 5000 rows
        df = df.sort_values('timestamp', ascending=False).head(5000).sort_values('timestamp')
        df = df.reset_index(drop=True)

        print("\n📊 Final data summary:")
        print(f"📈 Total candles: {len(df)}")
        print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print("✨ Thanks for using BobbyYo's Data Fetcher! ✨")

    return df

# Use the function
all_data = fetch_historical_data(symbol, timeframe)

# Create save directory if it doesn't exist
import os
os.makedirs(SAVE_DIR, exist_ok=True)
print(f"📂 Save directory ready: {SAVE_DIR}")

# Save the data with consistent naming
if not all_data.empty:
    # Rename timestamp column to datetime for consistency with other scripts
    all_data = all_data.rename(columns={'timestamp': 'datetime'})
    
    # Create filename consistent with other scripts
    output_file = os.path.join(SAVE_DIR, f'{SYMBOL}{"-USD" if SYMBOL != "BTC" else "USD"}-{TIMEFRAME}-hyperliquid-data.csv')
    all_data.to_csv(output_file, index=False)
    print(f'\n💾 Data saved to {output_file}')
    
    # Also keep the original format for backward compatibility
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    backup_file = f'data/{symbol}_{timeframe}_{timestamp}_historical.csv'
    all_data_backup = all_data.rename(columns={'datetime': 'timestamp'})
    all_data_backup.to_csv(backup_file, index=False)
    print(f'💾 Backup saved to {backup_file}')
else:
    print('❌ No data to save.')
