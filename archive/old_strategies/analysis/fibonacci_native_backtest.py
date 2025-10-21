"""
🌙 Fibonacci Scalping Strategy - Native Backtesting.py Results 🌙
================================================================
Show the standard backtesting.py format results output

Author: Bobby (algo-fun project)
Date: 2025-01-16
"""

import sys
import os
sys.path.append('/Users/bobbyyo/Projects/algo-fun')

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib
from datetime import datetime, time
import pytz
import warnings
warnings.filterwarnings('ignore')

# Import the strategy
from strategies.indicators.fibonacci_scalping_1m_strategy import FibonacciScalpingStrategy

def load_data(symbol='XRPUSD', timeframe='1m', provider='hyperliquid'):
    """Load data for backtesting"""
    if provider == 'hyperliquid':
        data_path = f'/Users/bobbyyo/Projects/algo-fun/data/hyperliquid/{symbol}-{timeframe}-5000bars-enhanced-data.csv'
    else:
        data_path = f'/Users/bobbyyo/Projects/algo-fun/data/coinbase/{symbol}-{timeframe}-50wks-enhanced-data.csv'

    if not os.path.exists(data_path):
        print(f"❌ Data file not found: {data_path}")
        return None

    df = pd.read_csv(data_path)
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)

    # Rename columns for backtesting.py
    column_mapping = {
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }
    df = df.rename(columns=column_mapping)

    print(f"📊 Loaded {len(df):,} bars for {symbol} ({timeframe}, {provider})")
    print(f"📅 Date range: {df.index.min()} to {df.index.max()}")

    return df

def main():
    """Run backtesting.py native format results"""
    print("🌙💫🚀 FIBONACCI SCALPING - NATIVE BACKTESTING.PY RESULTS 🚀💫🌙")
    print("=" * 80)

    # Test on best performing dataset
    symbol = 'XRPUSD'
    timeframe = '1m'
    provider = 'hyperliquid'

    print(f"Testing {symbol} ({timeframe}) from {provider}...")

    data = load_data(symbol, timeframe, provider)
    if data is None:
        return

    # Run backtest
    bt = Backtest(
        data,
        FibonacciScalpingStrategy,
        cash=10000,
        commission=.002,
        exclusive_orders=True
    )

    print(f"\n🚀 Running backtest...")
    result = bt.run()

    print(f"\n{'='*80}")
    print(f"📊 BACKTESTING.PY NATIVE RESULTS FORMAT")
    print(f"{'='*80}")

    # Print the native backtesting.py results
    print(result)

    print(f"\n{'='*80}")
    print(f"📈 TRADES SUMMARY")
    print(f"{'='*80}")

    if len(result._trades) > 0:
        trades = result._trades
        print(f"📊 Total Trades: {len(trades)}")
        print(f"📊 First 10 trades:")
        print(trades.head(10))

        print(f"\n📊 Last 10 trades:")
        print(trades.tail(10))

        print(f"\n📊 Trade Statistics:")
        print(f"   🏆 Winning Trades: {len(trades[trades['ReturnPct'] > 0])}")
        print(f"   📉 Losing Trades: {len(trades[trades['ReturnPct'] < 0])}")
        print(f"   📊 Average Return per Trade: {trades['ReturnPct'].mean():.4f}%")

    else:
        print("❌ No trades generated")

    print(f"\n📊 Generating plot...")
    bt.plot()

if __name__ == "__main__":
    main()