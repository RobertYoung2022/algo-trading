#!/usr/bin/env python3
"""
🌙 Enhanced Hyperliquid Historical Data Fetcher - Backtesting.py Compatible 🚀
Fetches up to 5000 bars of historical data with exact BTCUSD format compatibility
Compatible with backtesting.py and multi-data testing patterns
Enhanced format standardization and data quality validation
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
import numpy as np
import time
import os
import warnings
warnings.filterwarnings('ignore')

# ====== BobbyYo's Enhanced Hyperliquid Configuration 🌙 ======
# 🔧 MODIFY THESE SETTINGS TO CHANGE WHAT DATA YOU FETCH:

SYMBOL = 'XRP'               # Symbol to fetch - CHANGE THIS:
                           # Available: 'BTC', 'ETH', 'SOL', 'ARB', 'AVAX', 'ATOM', 'DOT', 'MATIC', 'LINK', 'UNI'
                           # More: 'XRP', 'ADA', 'LTC', 'BCH', 'ALGO', 'NEAR', 'FTM', 'SAND', 'MANA', 'AAVE'
                           # Note: Hyperliquid supports specific trading pairs only

TIMEFRAME = '1m'             # Timeframe - CHANGE THIS:
                           # Available: '1m', '1h', '1d', '1w'
                           # Note: '1d' is best for backtesting compatibility (like your BTCUSD file)

BARS_TO_FETCH = 5000         # Number of bars to fetch - CHANGE THIS:
                           # Maximum: 5000 (Hyperliquid limit)
                           # Examples: 365 (1 year daily), 1825 (5 years daily), 5000 (13+ years daily)

SAVE_DIR = 'data/hyperliquid'  # Directory to save files - CHANGE IF NEEDED:
                              # Examples: 'data/hyperliquid', 'data/hl_data', 'backup_data'

print("🌙 BobbyYo's Enhanced Hyperliquid Data Fetcher - Backtesting.py Compatible! 🚀")
print(f"🎯 Target: {BARS_TO_FETCH} bars of {SYMBOL} data at {TIMEFRAME} intervals")
print("⚠️ Note: Hyperliquid max is 5000 bars (great for recent comprehensive data)")

# Create save directory if it doesn't exist
os.makedirs(SAVE_DIR, exist_ok=True)
print(f"📂 Save directory ready: {SAVE_DIR}")

def timeframe_to_seconds(timeframe):
    """Convert timeframe to seconds for API"""
    timeframe_map = {
        '1m': 60,
        '1h': 3600,
        '1d': 86400,
        '1w': 604800
    }
    return timeframe_map.get(timeframe, 86400)  # Default to 1d

def get_enhanced_hyperliquid_data(symbol, timeframe, bars):
    """
    Enhanced Hyperliquid data fetcher that outputs data in exact BTCUSD format
    Returns DataFrame compatible with backtesting.py framework
    """
    print(f"🔍 Fetching {bars} bars of {timeframe} {symbol} data from Hyperliquid")

    # Create filename that matches your BTCUSD naming pattern
    output_file = os.path.join(SAVE_DIR, f'{symbol}USD-{timeframe}-{bars}bars-enhanced-data.csv')

    if os.path.exists(output_file):
        print("📂 Found existing enhanced data file!")
        df = pd.read_csv(output_file)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df.set_index('datetime')

    try:
        print("🌎 Connecting to Hyperliquid API...")

        # Hyperliquid API endpoint
        url = 'https://api.hyperliquid.xyz/info'

        # Calculate end time (now) and start time
        end_time = datetime.now()
        interval_seconds = timeframe_to_seconds(timeframe)
        start_time = end_time - timedelta(seconds=interval_seconds * bars)

        print(f"📅 Fetching data from {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")

        # Prepare request payload for Hyperliquid
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": timeframe,
                "startTime": int(start_time.timestamp() * 1000),  # Convert to milliseconds
                "endTime": int(end_time.timestamp() * 1000)
            }
        }

        print("🔄 Making API request to Hyperliquid...")

        # Make the request
        response = requests.post(url, json=payload, timeout=30)

        if response.status_code != 200:
            raise Exception(f"Hyperliquid API Error: {response.status_code} - {response.text}")

        data = response.json()
        print(f"✨ Successfully received response from Hyperliquid!")

        if not data or len(data) == 0:
            raise Exception("No data returned from Hyperliquid API")

        print(f"📊 Processing {len(data)} candles...")

        # Convert to DataFrame with EXACT BTCUSD format
        df_data = []
        for candle in data:
            # Hyperliquid sends: {'T': timestamp_ms, 'c': close, 'h': high, 'l': low, 'n': count, 'o': open, 'v': volume}
            timestamp = datetime.fromtimestamp(candle['T'] / 1000)

            # Use appropriate datetime format based on timeframe
            if TIMEFRAME in ['1m', '1h']:
                datetime_format = '%Y-%m-%d %H:%M:%S'  # Include time for minute/hour data
            else:
                datetime_format = '%Y-%m-%d'  # Date only for daily/weekly

            df_data.append({
                'datetime': timestamp.strftime(datetime_format),
                'open': float(candle['o']),
                'high': float(candle['h']),
                'low': float(candle['l']),
                'close': float(candle['c']),
                'volume': float(candle['v'])
            })

        # Create DataFrame in exact BTCUSD format
        df = pd.DataFrame(df_data)

        # Sort by date (oldest first, like your BTCUSD file)
        df['datetime_sort'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime_sort')
        df = df.drop('datetime_sort', axis=1)

        # Remove duplicates and handle any data quality issues
        df = df.drop_duplicates(subset=['datetime'])

        # Data validation - ensure OHLC relationships are correct
        invalid_rows = df[
            (df['high'] < df['low']) |  # High can't be lower than low
            (df['high'] < df['open']) |  # High must be >= open
            (df['high'] < df['close']) |  # High must be >= close
            (df['low'] > df['open']) |   # Low must be <= open
            (df['low'] > df['close']) |  # Low must be <= close
            (df['volume'] < 0)           # Volume can't be negative
        ]

        if len(invalid_rows) > 0:
            print(f"⚠️ Found {len(invalid_rows)} invalid OHLC rows - cleaning data...")
            df = df.drop(invalid_rows.index)

        # Reset index
        df = df.reset_index(drop=True)

        print(f"🎉 Enhanced data processed!")
        print(f"📈 Total data points: {len(df)}")

        if len(df) > 0:
            print(f"📅 Date range: {df['datetime'].iloc[0]} to {df['datetime'].iloc[-1]}")

            # Save to file in exact BTCUSD format
            df.to_csv(output_file, index=False)
            print(f"💾 Enhanced data saved to {output_file}")

            # Return with datetime as index for compatibility
            df['datetime'] = pd.to_datetime(df['datetime'])
            return df.set_index('datetime')
        else:
            raise Exception("No valid data after processing")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Tips:")
        print("   1. Check if the symbol is available on Hyperliquid")
        print("   2. Try a different timeframe (1d works best)")
        print("   3. Reduce the number of bars if hitting limits")
        print("   4. Check your internet connection")
        print("   5. Hyperliquid may have temporary API issues")
        raise

def validate_symbol(symbol):
    """Validate that the symbol exists on Hyperliquid"""
    try:
        print(f"🔍 Validating {symbol} on Hyperliquid...")

        url = 'https://api.hyperliquid.xyz/info'
        payload = {
            "type": "meta"
        }

        response = requests.post(url, json=payload, timeout=10)

        if response.status_code == 200:
            meta_data = response.json()
            # Check if symbol is in the available universe
            universe = meta_data.get('universe', [])
            available_symbols = [asset['name'] for asset in universe]

            if symbol in available_symbols:
                print(f"✅ {symbol} is available on Hyperliquid")
                return True
            else:
                print(f"❌ {symbol} not found on Hyperliquid")
                print(f"💡 Available symbols: {', '.join(available_symbols[:10])}...")
                return False
        else:
            print(f"⚠️ Could not validate symbol: {response.status_code}")
            return True  # Assume valid if we can't check

    except Exception as e:
        print(f"⚠️ Validation error: {str(e)}")
        return True  # Assume valid if we can't check

# Validate the symbol first
print(f"\n🔍 Validating {SYMBOL} on Hyperliquid...")
if not validate_symbol(SYMBOL):
    print("💡 Popular Hyperliquid symbols: BTC, ETH, SOL, ARB, AVAX, ATOM, DOT, MATIC, LINK")
    raise ValueError(f"Invalid symbol: {SYMBOL}")

# Get the enhanced data
print(f"\n🚀 Fetching enhanced {SYMBOL} data for {BARS_TO_FETCH} bars...")
data = get_enhanced_hyperliquid_data(SYMBOL, TIMEFRAME, BARS_TO_FETCH)

if not data.empty:
    print("\n📊 Enhanced Data Summary:")
    print(f"📈 Total rows: {len(data)}")
    print(f"📅 Date range: {data.index.min()} to {data.index.max()}")
    print(f"💰 Price range: ${data['close'].min():.2f} - ${data['close'].max():.2f}")
    print(f"📊 Volume range: {data['volume'].min():.2f} - {data['volume'].max():.2f}")

    # Calculate some interesting stats
    days_of_data = (data.index.max() - data.index.min()).days
    years_of_data = days_of_data / 365.25

    print(f"⏰ Actual days of data: {days_of_data}")
    print(f"🗓️ Actual years of data: {years_of_data:.2f}")

    # Calculate performance metrics
    if len(data) > 1:
        total_return = ((data['close'].iloc[-1] / data['close'].iloc[0]) - 1) * 100
        print(f"📈 Total return over period: {total_return:.2f}%")

    # Show format compatibility
    print("\n🔢 Sample data (first 5 rows - exact BTCUSD format):")
    sample_df = data.reset_index()
    # Format datetime based on timeframe
    if TIMEFRAME in ['1m', '1h']:
        sample_df['datetime'] = sample_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        sample_df['datetime'] = sample_df['datetime'].dt.strftime('%Y-%m-%d')
    print(sample_df.head()[['datetime', 'open', 'high', 'low', 'close', 'volume']])

    print(f"\n✨ Success! Enhanced {SYMBOL} data is now available!")
    print("🎯 Format matches your BTCUSD file exactly - ready for backtesting.py!")
    print("⚠️ Remember: Hyperliquid provides excellent recent data (max 5000 bars)")
    print("✨ Thanks for using BobbyYo's Enhanced Hyperliquid Data Fetcher! ✨")
else:
    print("❌ No data retrieved")