#!/usr/bin/env python3
"""
🌙 Enhanced CoinGecko Historical Data Fetcher - Backtesting.py Compatible 🚀
Fetches 10+ years of historical data with proper OHLCV format for Bobby's backtesting framework
Compatible with backtesting.py and multi-data testing patterns
"""

import pandas as pd
import datetime
import os
import requests
import time
from typing import Optional, Dict, List
import numpy as np

# ====== Configuration ======
COIN_ID = 'ripple'           # CoinGecko coin ID
VS_CURRENCY = 'usd'          # Currency to price against
YEARS_OF_DATA = 10           # Number of years to fetch (can go up to 15+)
SAVE_DIR = 'data/coingecko'  # Directory to save files

# Rate limiting settings
API_DELAY = 6  # Seconds between API calls (CoinGecko free tier: 10-30 calls/minute)

def fetch_historical_ohlc_batch(coin_id: str, vs_currency: str, from_timestamp: int, to_timestamp: int) -> List[Dict]:
    """
    Fetch OHLC data for a specific time range
    Returns list of candle dictionaries
    """
    try:
        # Calculate days between timestamps
        days = (to_timestamp - from_timestamp) / 86400

        if days <= 365:
            # Use OHLC endpoint for smaller ranges
            url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc'
            params = {
                'vs_currency': vs_currency,
                'days': int(days)
            }
        else:
            # Use market_chart_range for larger ranges
            url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range'
            params = {
                'vs_currency': vs_currency,
                'from': from_timestamp,
                'to': to_timestamp
            }

        response = requests.get(url, params=params, timeout=30)

        if response.status_code != 200:
            print(f"⚠️ API Error {response.status_code}, trying fallback...")
            return fetch_market_chart_fallback(coin_id, vs_currency, from_timestamp, to_timestamp)

        data = response.json()

        # Process based on endpoint type
        if 'prices' in data:
            # Market chart format
            return process_market_chart_data(data)
        else:
            # OHLC format
            return process_ohlc_data(data)

    except Exception as e:
        print(f"❌ Error fetching batch: {str(e)}")
        return []

def process_ohlc_data(data: List) -> List[Dict]:
    """Process OHLC endpoint data into standard format"""
    processed = []
    for candle in data:
        if len(candle) >= 5:
            timestamp = candle[0] / 1000  # Convert ms to seconds
            date_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

            processed.append({
                'datetime': date_str,
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': 0.0  # OHLC endpoint doesn't provide volume
            })
    return processed

def process_market_chart_data(data: Dict) -> List[Dict]:
    """Process market chart data into OHLC-like format"""
    prices = data.get('prices', [])
    volumes = data.get('total_volumes', [])

    # Create volume lookup
    volume_dict = {v[0]: v[1] for v in volumes} if volumes else {}

    # Group by day
    daily_data = {}
    for price_point in prices:
        timestamp = price_point[0] / 1000
        date_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        price = float(price_point[1])
        volume = float(volume_dict.get(price_point[0], 0.0))

        if date_str not in daily_data:
            daily_data[date_str] = {
                'prices': [],
                'volumes': []
            }
        daily_data[date_str]['prices'].append(price)
        daily_data[date_str]['volumes'].append(volume)

    # Calculate daily OHLC from intraday prices
    processed = []
    for date_str, day_data in sorted(daily_data.items()):
        prices = day_data['prices']
        volumes = day_data['volumes']

        if prices:
            processed.append({
                'datetime': date_str,
                'open': prices[0],
                'high': max(prices),
                'low': min(prices),
                'close': prices[-1],
                'volume': sum(volumes)
            })

    return processed

def fetch_market_chart_fallback(coin_id: str, vs_currency: str, from_timestamp: int, to_timestamp: int) -> List[Dict]:
    """Fallback using market_chart endpoint with daily interval"""
    try:
        days = (to_timestamp - from_timestamp) / 86400
        url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart'
        params = {
            'vs_currency': vs_currency,
            'days': min(int(days), 365),  # Max 365 days per call
            'interval': 'daily'
        }

        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 200:
            return process_market_chart_data(response.json())
    except Exception as e:
        print(f"❌ Fallback failed: {str(e)}")

    return []

def fetch_enhanced_historical_data(coin_id: str, vs_currency: str, years: int) -> pd.DataFrame:
    """
    Fetch multiple years of historical data using multiple API calls
    Returns DataFrame in backtesting.py compatible format
    """
    print(f"🌙 Fetching {years} years of {coin_id}/{vs_currency} data...")

    # Calculate time ranges (work backwards from today)
    end_time = int(datetime.datetime.now().timestamp())
    start_time = end_time - (years * 365 * 86400)  # years in seconds

    # Split into chunks (365 days each for optimal API usage)
    chunk_size = 365 * 86400  # 365 days in seconds
    all_data = []

    current_end = end_time
    batch_num = 1
    total_batches = (end_time - start_time) // chunk_size + 1

    while current_end > start_time:
        current_start = max(current_end - chunk_size, start_time)

        print(f"📊 Fetching batch {batch_num}/{total_batches} ({datetime.datetime.fromtimestamp(current_start).strftime('%Y-%m-%d')} to {datetime.datetime.fromtimestamp(current_end).strftime('%Y-%m-%d')})")

        batch_data = fetch_historical_ohlc_batch(coin_id, vs_currency, current_start, current_end)

        if batch_data:
            all_data.extend(batch_data)
            print(f"✅ Retrieved {len(batch_data)} daily candles")

        # Move to next chunk
        current_end = current_start
        batch_num += 1

        # Rate limiting
        if current_end > start_time:
            print(f"⏱️ Rate limiting: waiting {API_DELAY} seconds...")
            time.sleep(API_DELAY)

    if not all_data:
        raise Exception("No data retrieved from API")

    # Convert to DataFrame
    df = pd.DataFrame(all_data)

    # Remove duplicates and sort
    df = df.drop_duplicates(subset=['datetime'])
    df = df.sort_values('datetime')

    # Ensure proper format for backtesting.py
    df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]

    # Fill any missing volume data with estimated values
    if df['volume'].sum() == 0:
        print("⚠️ No volume data available, estimating based on price volatility...")
        # Estimate volume based on price changes (rough approximation)
        df['price_change'] = df['close'].pct_change().abs()
        df['volume'] = df['price_change'] * df['close'] * 1000000  # Rough volume estimate
        df['volume'] = df['volume'].fillna(df['volume'].mean())
        df = df.drop('price_change', axis=1)

    # Ensure all numeric columns are float type
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Forward fill any NaN values
    df = df.fillna(method='ffill').fillna(method='bfill')

    # Reset index for clean CSV output
    df = df.reset_index(drop=True)

    return df

def validate_data_quality(df: pd.DataFrame) -> Dict:
    """Validate data quality and completeness"""
    issues = []

    # Check for missing values
    missing = df.isnull().sum()
    if missing.any():
        issues.append(f"Missing values found: {missing[missing > 0].to_dict()}")

    # Check for zero prices
    zero_prices = (df[['open', 'high', 'low', 'close']] == 0).sum()
    if zero_prices.any():
        issues.append(f"Zero prices found: {zero_prices[zero_prices > 0].to_dict()}")

    # Check for data gaps
    dates = pd.to_datetime(df['datetime'])
    expected_days = (dates.max() - dates.min()).days + 1
    actual_days = len(df)
    gap_percentage = (1 - actual_days / expected_days) * 100

    if gap_percentage > 5:
        issues.append(f"Data gaps detected: {gap_percentage:.1f}% missing days")

    # Check OHLC consistency
    invalid_ohlc = df[(df['high'] < df['low']) |
                      (df['high'] < df['open']) |
                      (df['high'] < df['close']) |
                      (df['low'] > df['open']) |
                      (df['low'] > df['close'])]

    if not invalid_ohlc.empty:
        issues.append(f"Invalid OHLC relationships in {len(invalid_ohlc)} rows")

    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'rows': len(df),
        'date_range': f"{df['datetime'].min()} to {df['datetime'].max()}",
        'completeness': f"{100 - gap_percentage:.1f}%"
    }

def main():
    """Main execution function"""
    print("🚀 Enhanced CoinGecko Historical Data Fetcher - Backtesting.py Compatible")
    print("=" * 70)

    # Create save directory
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Generate filename matching Bobby's pattern
    output_file = os.path.join(SAVE_DIR,
                               f'{COIN_ID.upper()}{VS_CURRENCY.upper()}-{YEARS_OF_DATA}yr-enhanced-data.csv')

    try:
        # Fetch the data
        df = fetch_enhanced_historical_data(COIN_ID, VS_CURRENCY, YEARS_OF_DATA)

        # Validate data quality
        print("\n📋 Validating data quality...")
        validation = validate_data_quality(df)

        print(f"✅ Data validation: {'PASSED' if validation['valid'] else 'WARNINGS'}")
        print(f"📊 Total rows: {validation['rows']}")
        print(f"📅 Date range: {validation['date_range']}")
        print(f"📈 Data completeness: {validation['completeness']}")

        if validation['issues']:
            print("\n⚠️ Quality issues detected:")
            for issue in validation['issues']:
                print(f"  - {issue}")

        # Save to CSV in backtesting.py format
        df.to_csv(output_file, index=False)
        print(f"\n💾 Data saved to: {output_file}")

        # Display sample
        print("\n🔢 Sample data (first 5 rows):")
        print(df.head())
        print("\n🔢 Sample data (last 5 rows):")
        print(df.tail())

        # Summary statistics
        print("\n📊 Summary Statistics:")
        print(f"  Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
        print(f"  Average daily volume: ${df['volume'].mean():,.0f}")
        print(f"  Total days: {len(df):,}")

        return df

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("\n💡 Troubleshooting tips:")
        print("  1. Check if coin_id is valid (use CoinGecko's coin list)")
        print("  2. Verify internet connection")
        print("  3. Try reducing years of data if hitting limits")
        print("  4. Check CoinGecko API status")
        return None

if __name__ == "__main__":
    data = main()
    if data is not None:
        print("\n✨ Enhanced data fetching complete! Ready for backtesting.py 🚀")