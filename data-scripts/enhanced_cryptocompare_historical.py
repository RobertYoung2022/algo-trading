#!/usr/bin/env python3
"""
🌙 Enhanced CryptoCompare Historical Data Fetcher - Backtesting.py Compatible 🚀
Fetches 7+ years of historical data with proper OHLCV format for Bobby's backtesting framework
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
SYMBOL = 'BTC'                # Symbol to fetch
VS_CURRENCY = 'USD'           # Currency to price against
YEARS_OF_DATA = 7             # Number of years to fetch (max ~7 years with daily data)
SAVE_DIR = 'data/cryptocompare'  # Directory to save files

# Rate limiting settings
API_DELAY = 1.5  # Seconds between API calls (CryptoCompare: 100 calls/second, being conservative)

def fetch_historical_batch(symbol: str, vs_currency: str, endpoint: str, limit: int, to_timestamp: Optional[int] = None) -> List[Dict]:
    """
    Fetch a batch of historical data from CryptoCompare
    Returns list of candle dictionaries
    """
    try:
        url = f'https://min-api.cryptocompare.com/data/v2/{endpoint}'

        params = {
            'fsym': symbol.upper(),
            'tsym': vs_currency.upper(),
            'limit': min(limit, 2000),  # CryptoCompare max is 2000
            'aggregate': 1  # Get every data point
        }

        # Add toTs parameter if provided (for fetching older data)
        if to_timestamp:
            params['toTs'] = to_timestamp

        response = requests.get(url, params=params, timeout=30)

        if response.status_code != 200:
            print(f"⚠️ API Error {response.status_code}")
            return []

        data = response.json()

        # Check API response
        if data.get('Response') == 'Error':
            print(f"⚠️ CryptoCompare API Error: {data.get('Message', 'Unknown error')}")
            return []

        if 'Data' not in data or 'Data' not in data['Data']:
            print("⚠️ No data in response")
            return []

        raw_data = data['Data']['Data']
        processed = []

        for candle in raw_data:
            # Skip candles with no trading activity
            if candle['volumeto'] == 0 and candle['volumefrom'] == 0 and candle['open'] == 0:
                continue

            timestamp = candle['time']
            date_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

            processed.append({
                'datetime': date_str,
                'open': float(candle['open']),
                'high': float(candle['high']),
                'low': float(candle['low']),
                'close': float(candle['close']),
                'volume': float(candle['volumeto']),  # Volume in target currency
                'timestamp': timestamp  # Keep for pagination
            })

        return processed

    except Exception as e:
        print(f"❌ Error fetching batch: {str(e)}")
        return []

def fetch_enhanced_historical_data(symbol: str, vs_currency: str, years: int) -> pd.DataFrame:
    """
    Fetch multiple years of historical data using multiple API calls
    Returns DataFrame in backtesting.py compatible format
    """
    print(f"🌙 Fetching {years} years of {symbol}/{vs_currency} data from CryptoCompare...")

    # For multi-year data, use daily candles
    endpoint = 'histoday'

    # Calculate total days needed
    total_days = years * 365
    max_per_call = 2000  # CryptoCompare limit

    all_data = []
    current_timestamp = None  # Start from most recent
    batches_fetched = 0
    total_batches = (total_days // max_per_call) + 1

    while batches_fetched < total_batches:
        batch_num = batches_fetched + 1

        if current_timestamp:
            # Fetch older data
            print(f"📊 Fetching batch {batch_num}/{total_batches} (before {datetime.datetime.fromtimestamp(current_timestamp).strftime('%Y-%m-%d')})")
        else:
            print(f"📊 Fetching batch {batch_num}/{total_batches} (most recent data)")

        batch_data = fetch_historical_batch(
            symbol,
            vs_currency,
            endpoint,
            max_per_call,
            current_timestamp
        )

        if not batch_data:
            print(f"⚠️ No data in batch {batch_num}, stopping")
            break

        # Remove the timestamp field before adding to all_data
        for candle in batch_data:
            timestamp = candle.pop('timestamp', None)
            # Update current_timestamp to the oldest timestamp in this batch
            if timestamp and (current_timestamp is None or timestamp < current_timestamp):
                current_timestamp = timestamp

        all_data.extend(batch_data)
        print(f"✅ Retrieved {len(batch_data)} daily candles")

        batches_fetched += 1

        # Check if we have enough data
        if len(all_data) >= total_days:
            print(f"📈 Reached target of {total_days} days")
            break

        # Get the oldest timestamp from this batch for next iteration
        if batch_data and current_timestamp:
            # Move to one second before the oldest timestamp to avoid overlap
            current_timestamp = current_timestamp - 1

        # Rate limiting
        if batches_fetched < total_batches:
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

    # Validate and clean OHLC relationships
    df = validate_and_clean_ohlc(df)

    # Ensure all numeric columns are float type
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Forward fill any NaN values
    df = df.fillna(method='ffill').fillna(method='bfill')

    # Reset index for clean CSV output
    df = df.reset_index(drop=True)

    # Trim to requested years if we got more data
    if len(df) > total_days:
        df = df.tail(total_days)

    return df

def validate_and_clean_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean OHLC data to ensure consistency
    """
    # Fix high/low violations
    df['high'] = df[['open', 'high', 'close']].max(axis=1)
    df['low'] = df[['open', 'low', 'close']].min(axis=1)

    # Ensure high >= low
    invalid_rows = df['high'] < df['low']
    if invalid_rows.any():
        print(f"⚠️ Fixed {invalid_rows.sum()} rows with high < low")
        # Swap high and low where invalid
        df.loc[invalid_rows, ['high', 'low']] = df.loc[invalid_rows, ['low', 'high']].values

    # Handle zero or negative prices
    for col in ['open', 'high', 'low', 'close']:
        zero_prices = df[col] <= 0
        if zero_prices.any():
            print(f"⚠️ Found {zero_prices.sum()} zero/negative prices in {col}, using forward fill")
            df.loc[zero_prices, col] = np.nan
            df[col] = df[col].fillna(method='ffill').fillna(method='bfill')

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

    # Check for extreme price changes (> 50% in one day)
    df['price_change'] = df['close'].pct_change()
    extreme_changes = df[df['price_change'].abs() > 0.5]
    if not extreme_changes.empty:
        issues.append(f"Extreme price changes (>50%) in {len(extreme_changes)} days")

    # Clean up temporary column
    df.drop('price_change', axis=1, inplace=True, errors='ignore')

    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'rows': len(df),
        'date_range': f"{df['datetime'].min()} to {df['datetime'].max()}",
        'completeness': f"{100 - gap_percentage:.1f}%"
    }

def main():
    """Main execution function"""
    print("🚀 Enhanced CryptoCompare Historical Data Fetcher - Backtesting.py Compatible")
    print("=" * 70)

    # Create save directory
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Generate filename matching Bobby's pattern
    output_file = os.path.join(SAVE_DIR,
                               f'{SYMBOL.upper()}{VS_CURRENCY.upper()}-{YEARS_OF_DATA}yr-enhanced-data.csv')

    try:
        # Fetch the data
        df = fetch_enhanced_historical_data(SYMBOL, VS_CURRENCY, YEARS_OF_DATA)

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
        if df['volume'].sum() > 0:
            print(f"  Average daily volume: ${df['volume'].mean():,.0f}")
        else:
            print(f"  Volume data: Not available")
        print(f"  Total days: {len(df):,}")

        # Calculate volatility
        returns = df['close'].pct_change().dropna()
        daily_volatility = returns.std()
        annual_volatility = daily_volatility * np.sqrt(252)
        print(f"  Daily volatility: {daily_volatility:.2%}")
        print(f"  Annual volatility: {annual_volatility:.2%}")

        return df

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("\n💡 Troubleshooting tips:")
        print("  1. Check if symbol exists on CryptoCompare")
        print("  2. Verify vs_currency is supported")
        print("  3. Try reducing years of data if hitting limits")
        print("  4. Check CryptoCompare API status")
        print("  5. Ensure internet connection is stable")
        return None

if __name__ == "__main__":
    data = main()
    if data is not None:
        print("\n✨ Enhanced data fetching complete! Ready for backtesting.py 🚀")