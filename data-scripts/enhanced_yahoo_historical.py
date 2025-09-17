#!/usr/bin/env python3
"""
🌙 Enhanced Yahoo Finance Historical Data Fetcher - Backtesting.py Compatible 🚀
Fetches 15-20+ years of historical data with proper OHLCV format for Bobby's backtesting framework
Compatible with backtesting.py and multi-data testing patterns
Supports crypto, stocks, ETFs, and indices
"""

import pandas as pd
import yfinance as yf
import datetime
import os
from typing import Optional, Dict, List
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ====== Configuration ======
TICKER = 'BTC-USD'            # Yahoo Finance ticker symbol
YEARS_OF_DATA = 20            # Number of years to fetch (can go up to 20+ for many assets)
SAVE_DIR = 'data/yahoo'       # Directory to save files

# Popular tickers for reference:
# Crypto: BTC-USD, ETH-USD, SOL-USD, ADA-USD, MATIC-USD, LINK-USD, DOT-USD
# Stocks: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, COIN, SQ, MSTR
# ETFs: SPY, QQQ, IWM, DIA, GLD, TLT, VXX, ARKK, XLF, XLE
# Indices: ^GSPC (S&P 500), ^DJI (Dow Jones), ^IXIC (Nasdaq), ^VIX (VIX)

def fetch_enhanced_yahoo_data(ticker: str, years: int) -> pd.DataFrame:
    """
    Fetch multiple years of historical data from Yahoo Finance
    Returns DataFrame in backtesting.py compatible format
    """
    print(f"🌙 Fetching {years} years of {ticker} data from Yahoo Finance...")

    try:
        # Create ticker object
        ticker_obj = yf.Ticker(ticker)

        # Calculate date range
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=years * 365)

        print(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        # Fetch historical data
        # Yahoo Finance can fetch all data in one request (no rate limiting needed)
        df = ticker_obj.history(
            start=start_date,
            end=end_date,
            interval='1d',  # Daily data
            actions=False,   # Don't include dividends/splits
            auto_adjust=True  # Adjust for splits
        )

        if df.empty:
            raise Exception(f"No data returned for {ticker}")

        print(f"✅ Retrieved {len(df)} daily candles")

        # Convert to backtesting.py format
        df = df.reset_index()

        # Rename columns to match backtesting.py format
        df = df.rename(columns={
            'Date': 'datetime',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })

        # Convert datetime to string format (YYYY-MM-DD)
        df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')

        # Select only required columns
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]

        # Handle any missing values
        df = handle_missing_data(df)

        # Validate and clean OHLC relationships
        df = validate_and_clean_ohlc(df)

        # Ensure all numeric columns are float type
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Forward fill any remaining NaN values
        df = df.fillna(method='ffill').fillna(method='bfill')

        # Sort by date
        df = df.sort_values('datetime')

        # Reset index for clean CSV output
        df = df.reset_index(drop=True)

        return df

    except Exception as e:
        print(f"❌ Error fetching data: {str(e)}")

        # Try alternative method using download function
        print("🔄 Trying alternative download method...")
        return fetch_yahoo_alternative(ticker, years)

def fetch_yahoo_alternative(ticker: str, years: int) -> pd.DataFrame:
    """
    Alternative method using yf.download for batch downloading
    """
    try:
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=years * 365)

        print(f"📊 Using yf.download for {ticker}...")

        # Download data
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval='1d',
            progress=False,
            auto_adjust=True,
            prepost=False,
            threads=True
        )

        if df.empty:
            raise Exception(f"No data available for {ticker}")

        # Reset index to get Date as a column
        df = df.reset_index()

        # Rename columns to match backtesting.py format
        df = df.rename(columns={
            'Date': 'datetime',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })

        # Convert datetime to string format
        df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')

        # Select only required columns
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]

        print(f"✅ Retrieved {len(df)} daily candles via alternative method")

        return df

    except Exception as e:
        print(f"❌ Alternative method also failed: {str(e)}")
        raise

def handle_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing data in the DataFrame
    """
    # Check for missing values
    missing = df.isnull().sum()

    if missing.any():
        print(f"⚠️ Found missing values: {missing[missing > 0].to_dict()}")

        # For price columns, use forward fill then backward fill
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            df[col] = df[col].fillna(method='ffill').fillna(method='bfill')

        # For volume, fill with 0 or average
        if df['volume'].isnull().any():
            # Use average volume for missing values
            avg_volume = df['volume'].mean()
            df['volume'] = df['volume'].fillna(avg_volume)

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

    # Handle zero volume (common for some assets)
    zero_volume = df['volume'] == 0
    if zero_volume.any():
        print(f"⚠️ Found {zero_volume.sum()} days with zero volume")
        # Estimate volume based on price volatility
        df.loc[zero_volume, 'volume'] = (
            df.loc[zero_volume, 'close'] *
            df['close'].pct_change().abs().rolling(20).mean().fillna(0.01) *
            1000000
        ).fillna(100000)

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

    # Account for weekends and holidays (roughly 252 trading days per year)
    if 'USD' in TICKER or 'BTC' in TICKER or 'ETH' in TICKER:
        # Crypto trades 365 days
        gap_percentage = (1 - actual_days / expected_days) * 100
    else:
        # Stocks trade ~252 days per year
        expected_trading_days = expected_days * (252/365)
        gap_percentage = (1 - actual_days / expected_trading_days) * 100

    if gap_percentage > 10:
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
    if not extreme_changes.empty and len(extreme_changes) > 1:
        issues.append(f"Extreme price changes (>50%) in {len(extreme_changes)} days")

    # Clean up temporary column
    df.drop('price_change', axis=1, inplace=True, errors='ignore')

    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'rows': len(df),
        'date_range': f"{df['datetime'].min()} to {df['datetime'].max()}",
        'completeness': f"{100 - abs(gap_percentage):.1f}%"
    }

def get_ticker_info(ticker: str) -> Dict:
    """Get additional information about the ticker"""
    try:
        ticker_obj = yf.Ticker(ticker)
        info = ticker_obj.info

        return {
            'name': info.get('longName', info.get('shortName', ticker)),
            'type': info.get('quoteType', 'Unknown'),
            'exchange': info.get('exchange', 'Unknown'),
            'currency': info.get('currency', 'USD'),
            'market_cap': info.get('marketCap', 0)
        }
    except:
        return {
            'name': ticker,
            'type': 'Unknown',
            'exchange': 'Unknown',
            'currency': 'USD',
            'market_cap': 0
        }

def main():
    """Main execution function"""
    print("🚀 Enhanced Yahoo Finance Historical Data Fetcher - Backtesting.py Compatible")
    print("=" * 70)

    # Create save directory
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Get ticker info
    print(f"\n📋 Fetching ticker information for {TICKER}...")
    ticker_info = get_ticker_info(TICKER)
    print(f"  Name: {ticker_info['name']}")
    print(f"  Type: {ticker_info['type']}")
    print(f"  Exchange: {ticker_info['exchange']}")
    print(f"  Currency: {ticker_info['currency']}")

    # Generate filename matching Bobby's pattern
    clean_ticker = TICKER.replace('^', '').replace('-', '')
    output_file = os.path.join(SAVE_DIR,
                               f'{clean_ticker}-{YEARS_OF_DATA}yr-yahoo-data.csv')

    try:
        # Fetch the data
        df = fetch_enhanced_yahoo_data(TICKER, YEARS_OF_DATA)

        # Validate data quality
        print("\n📋 Validating data quality...")
        validation = validate_data_quality(df)

        print(f"✅ Data validation: {'PASSED' if validation['valid'] else 'WARNINGS'}")
        print(f"📊 Total rows: {validation['rows']:,}")
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

        # Handle different price ranges
        if df['close'].max() < 1:
            # For penny stocks or very low-priced assets
            print(f"  Price range: ${df['close'].min():.6f} - ${df['close'].max():.6f}")
        elif df['close'].max() > 10000:
            # For high-priced assets like BTC
            print(f"  Price range: ${df['close'].min():,.2f} - ${df['close'].max():,.2f}")
        else:
            print(f"  Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")

        if df['volume'].sum() > 0:
            print(f"  Average daily volume: {df['volume'].mean():,.0f}")
        else:
            print(f"  Volume data: Limited or unavailable")

        print(f"  Total days: {len(df):,}")

        # Calculate performance metrics
        total_return = (df['close'].iloc[-1] / df['close'].iloc[0] - 1) * 100
        print(f"  Total return: {total_return:.2f}%")

        # Calculate volatility
        returns = df['close'].pct_change().dropna()
        daily_volatility = returns.std()
        annual_volatility = daily_volatility * np.sqrt(252)
        print(f"  Daily volatility: {daily_volatility:.2%}")
        print(f"  Annual volatility: {annual_volatility:.2%}")

        # Calculate Sharpe ratio (assuming 0% risk-free rate)
        annual_return = (df['close'].iloc[-1] / df['close'].iloc[0]) ** (252/len(df)) - 1
        sharpe_ratio = annual_return / annual_volatility if annual_volatility > 0 else 0
        print(f"  Sharpe ratio: {sharpe_ratio:.2f}")

        return df

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("\n💡 Troubleshooting tips:")
        print("  1. Check if ticker symbol is valid (use Yahoo Finance format)")
        print("  2. Verify internet connection")
        print("  3. Try a different ticker to test connectivity")
        print("  4. Install/update yfinance: pip install --upgrade yfinance")
        print("  5. Check if Yahoo Finance API is accessible")
        return None

if __name__ == "__main__":
    data = main()
    if data is not None:
        print("\n✨ Enhanced data fetching complete! Ready for backtesting.py 🚀")