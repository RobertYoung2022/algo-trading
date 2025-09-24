"""
🔍 ClucMay72018 Strategy Deep Analysis
=======================================
Analyze why the ultra-selective strategy isn't generating trades
and test with additional data sources including Yahoo Finance

Created: September 2025
Author: Bobby 🌙💫🚀
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from backtesting import Backtest
from clucmay72018_momentum_reversal_strategy import ClucMay72018Strategy
import warnings
warnings.filterwarnings('ignore')


def analyze_strategy_conditions():
    """
    Analyze the strategy conditions to understand why no trades are occurring
    """
    print("\n" + "="*80)
    print("🔍 ANALYZING CLUCMAY72018 STRATEGY CONDITIONS")
    print("="*80)

    # Load sample data for analysis
    file_path = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-5m-50wks-enhanced-data.csv"
    df = pd.read_csv(file_path)

    # Print column names to debug
    print(f"\nAvailable columns: {df.columns.tolist()}")

    # Handle column naming
    df.columns = [col.title() if col.lower() in ['open', 'high', 'low', 'close', 'volume']
                  else col for col in df.columns]

    # Set index if datetime column exists
    if 'Date' in df.columns:
        df.index = pd.to_datetime(df['Date'])
    elif 'Datetime' in df.columns:
        df.index = pd.to_datetime(df['Datetime'])
    else:
        df.index = pd.to_datetime(df.index) if df.index.dtype == 'O' else df.index

    print(f"\n📊 Analyzing BTC-USD 5m data:")
    print(f"Data points: {len(df)}")
    print(f"Date range: {df.index[0]} to {df.index[-1]}")

    # Calculate indicators manually to analyze
    import talib

    close = df['Close'].values
    high = df['High'].values
    low = df['Low'].values
    volume = df['Volume'].values

    # Calculate indicators
    rsi = talib.RSI(close, 5)
    ema100 = talib.EMA(close, 100)
    bb_upper, bb_middle, bb_lower = talib.BBANDS(close, 20, 2, 2)
    volume_sma = talib.SMA(volume, 30)

    # Analyze conditions
    print("\n📈 CONDITION ANALYSIS:")
    print("-"*60)

    # 1. Below EMA(100)
    below_ema = close < ema100
    pct_below_ema = (below_ema.sum() / len(close)) * 100
    print(f"1. Price below EMA(100): {pct_below_ema:.1f}% of time")

    # 2. Extreme oversold (< 98.5% of lower BB)
    extreme_oversold = close < (bb_lower * 0.985)
    pct_extreme_os = (extreme_oversold.sum() / len(close)) * 100
    print(f"2. Price < 98.5% of Lower BB: {pct_extreme_os:.1f}% of time")

    # 3. Volume anomaly (< 5% of average)
    volume_ratio = volume / volume_sma
    volume_anomaly = volume_ratio < 0.05
    pct_vol_anomaly = (volume_anomaly.sum() / len(volume_ratio[~np.isnan(volume_ratio)])) * 100
    print(f"3. Volume < 5% of 30-period avg: {pct_vol_anomaly:.1f}% of time")

    # 4. All conditions together
    valid_idx = ~(np.isnan(ema100) | np.isnan(bb_lower) | np.isnan(volume_sma))
    all_conditions = below_ema[valid_idx] & extreme_oversold[valid_idx] & volume_anomaly[valid_idx]
    pct_all_conditions = (all_conditions.sum() / len(all_conditions)) * 100
    print(f"\n🎯 ALL CONDITIONS MET: {pct_all_conditions:.3f}% of time ({all_conditions.sum()} instances)")

    # Show when conditions were met
    if all_conditions.sum() > 0:
        print("\n📅 Instances when all conditions were met:")
        valid_df = df[valid_idx]
        met_indices = valid_df.index[all_conditions]
        for idx in met_indices[:10]:  # Show first 10
            print(f"  {idx}: Close={valid_df.loc[idx, 'Close']:.2f}")

    return df


def test_with_relaxed_parameters():
    """
    Test the strategy with slightly relaxed parameters to generate trades
    """
    print("\n" + "="*80)
    print("🔧 TESTING WITH ADJUSTED PARAMETERS")
    print("="*80)

    class RelaxedClucMay72018Strategy(ClucMay72018Strategy):
        """Modified version with slightly relaxed parameters"""
        bb_entry_threshold = 1.00  # Enter at lower BB instead of 98.5%
        volume_threshold = 0.20    # Volume < 20% of average instead of 5%

    # Test on BTC 5m data
    file_path = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-5m-50wks-enhanced-data.csv"
    df = pd.read_csv(file_path)

    # Handle column naming
    df.columns = [col.title() if col.lower() in ['open', 'high', 'low', 'close', 'volume']
                  else col for col in df.columns]

    # Set index if datetime column exists
    if 'Date' in df.columns:
        df.index = pd.to_datetime(df['Date'])
    elif 'Datetime' in df.columns:
        df.index = pd.to_datetime(df['Datetime'])
    else:
        df.index = pd.to_datetime(df.index) if df.index.dtype == 'O' else df.index

    print("\n📊 Testing relaxed strategy on BTC-USD 5m:")

    bt = Backtest(
        df,
        RelaxedClucMay72018Strategy,
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    stats = bt.run()

    print("\n🚀 RELAXED STRATEGY RESULTS 🚀")
    print("="*80)
    print(stats)
    print("="*80)

    return stats


def test_yahoo_finance_data():
    """
    Test the strategy with Yahoo Finance data
    """
    print("\n" + "="*80)
    print("📊 TESTING WITH YAHOO FINANCE DATA")
    print("="*80)

    # Download multiple crypto assets from Yahoo Finance
    symbols = ['BTC-USD', 'ETH-USD', 'XRP-USD', 'LINK-USD']

    for symbol in symbols:
        try:
            print(f"\n🎯 Testing {symbol}:")

            # Download recent data (5-minute intervals for last 60 days)
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="60d", interval="5m")

            if df.empty:
                # Fall back to hourly data if 5m not available
                df = ticker.history(period="2y", interval="1h")
                print(f"  Using hourly data (5m not available)")

            print(f"  Data points: {len(df)}")
            print(f"  Date range: {df.index[0]} to {df.index[-1]}")

            # Run backtest
            bt = Backtest(
                df,
                ClucMay72018Strategy,
                cash=10000,
                commission=0.002,
                exclusive_orders=True
            )

            stats = bt.run()

            # Display key metrics
            print(f"  Return: {stats['Return [%]']:.2f}%")
            print(f"  Sharpe: {stats.get('Sharpe Ratio', 'N/A')}")
            print(f"  Trades: {stats['# Trades']}")
            print(f"  Win Rate: {stats['Win Rate [%]'] if stats['# Trades'] > 0 else 'N/A'}%")

        except Exception as e:
            print(f"  ⚠️ Error testing {symbol}: {str(e)[:50]}")


def test_with_additional_providers():
    """
    Test with additional data providers
    """
    print("\n" + "="*80)
    print("🌐 TESTING ADDITIONAL DATA PROVIDERS")
    print("="*80)

    # Test patterns for different providers
    test_files = [
        # Yahoo data
        "/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTC-USD-yahoo-data.csv",
        "/Users/bobbyyo/Projects/algo-fun/data/yahoo/ETH-USD-yahoo-data.csv",
        "/Users/bobbyyo/Projects/algo-fun/data/yahoo/CRO-USD-yahoo-data.csv",
        "/Users/bobbyyo/Projects/algo-fun/data/yahoo/HBAR-USD-yahoo-data.csv",
        "/Users/bobbyyo/Projects/algo-fun/data/yahoo/LINK-USD-yahoo-data.csv",

        # Hyperliquid data (validated)
        "/Users/bobbyyo/Projects/algo-fun/data/hyperliquid/XRPUSD-1m-5000bars-enhanced-data.csv",

        # CoinGecko data
        "/Users/bobbyyo/Projects/algo-fun/data/coingecko/RIPPLEUSD-365d-coingecko-data.csv",

        # XRP data from Coinbase (1m for high frequency)
        "/Users/bobbyyo/Projects/algo-fun/data/coinbase/XRPUSD-1m-52wks-enhanced-data.csv",
    ]

    results = []

    for file_path in test_files:
        try:
            # Check if file exists
            import os
            if not os.path.exists(file_path):
                continue

            # Extract info from path
            filename = os.path.basename(file_path)
            provider = file_path.split('/')[-2]

            print(f"\n📁 Testing: {filename}")
            print(f"   Provider: {provider}")

            # Load data
            df = pd.read_csv(file_path)

            # Handle different column formats
            col_mappings = {
                'open': 'Open', 'high': 'High', 'low': 'Low',
                'close': 'Close', 'volume': 'Volume'
            }

            for old_col, new_col in col_mappings.items():
                if old_col in df.columns and new_col not in df.columns:
                    df[new_col] = df[old_col]

            # Set index to datetime if available
            if 'Date' in df.columns:
                df.index = pd.to_datetime(df['Date'])
            elif 'date' in df.columns:
                df.index = pd.to_datetime(df['date'])
            elif 'timestamp' in df.columns:
                df.index = pd.to_datetime(df['timestamp'])

            # Ensure we have required columns
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in df.columns for col in required_cols):
                print(f"   ⚠️ Missing required columns")
                continue

            # Add default volume if missing
            if df['Volume'].sum() == 0:
                df['Volume'] = 1000

            print(f"   Data points: {len(df)}")

            # Run backtest
            bt = Backtest(
                df,
                ClucMay72018Strategy,
                cash=10000,
                commission=0.002,
                exclusive_orders=True
            )

            stats = bt.run()

            # Store results
            results.append({
                'file': filename,
                'provider': provider,
                'return': stats['Return [%]'],
                'trades': stats['# Trades'],
                'sharpe': stats.get('Sharpe Ratio', 0)
            })

            # Display quick summary
            print(f"   Return: {stats['Return [%]']:.2f}%")
            print(f"   Trades: {stats['# Trades']}")

        except Exception as e:
            print(f"   ⚠️ Error: {str(e)[:50]}")

    # Summary
    if results:
        print("\n📊 PROVIDER COMPARISON SUMMARY:")
        print("-"*60)
        df_results = pd.DataFrame(results)
        for provider in df_results['provider'].unique():
            prov_data = df_results[df_results['provider'] == provider]
            print(f"{provider}: Avg Return={prov_data['return'].mean():.2f}%, "
                  f"Total Trades={prov_data['trades'].sum()}")


def main():
    """
    Run comprehensive analysis
    """
    print("🔍 ClucMay72018 Strategy - Deep Analysis & Extended Testing")
    print("="*80)

    # 1. Analyze why no trades are occurring
    df = analyze_strategy_conditions()

    # 2. Test with relaxed parameters
    test_with_relaxed_parameters()

    # 3. Test with Yahoo Finance data
    test_yahoo_finance_data()

    # 4. Test additional providers
    test_with_additional_providers()

    print("\n" + "="*80)
    print("✅ ANALYSIS COMPLETE!")
    print("="*80)

    print("\n💡 KEY FINDINGS:")
    print("-"*60)
    print("1. The strategy is ULTRA-selective by design")
    print("2. ALL three conditions rarely align simultaneously")
    print("3. Volume anomaly condition (< 5% of average) is extremely rare")
    print("4. Strategy may be better suited for highly volatile, low-liquidity periods")
    print("5. Consider relaxing parameters slightly for more trading opportunities")


if __name__ == "__main__":
    main()