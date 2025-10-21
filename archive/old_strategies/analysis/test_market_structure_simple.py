"""
🌙 Market Structure Strategy - Simple Test 🌙
===========================================
Simple test script to validate the Market Structure strategy implementation.

Author: Bobby's Algo Trading Systems 🚀
Date: 2025-01-17
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parents[2]
sys.path.insert(0, str(project_root))

# Import strategy
from strategies.indicators.market_structure_supply_demand_strategy import MarketStructureSupplyDemandStrategy


def test_single_asset():
    """Test strategy on single BTC dataset"""

    print("🚀 Testing Market Structure Strategy on BTCUSD")
    print("=" * 60)

    # Load BTC data
    data_path = project_root / 'data' / 'coinbase' / 'BTCUSD-1d-1000wks-enhanced-data.csv'

    if not data_path.exists():
        # Try alternative path
        data_path = project_root / 'data' / 'BTCUSD-1d-1000wks-data.csv'

    if not data_path.exists():
        print(f"❌ Data file not found at {data_path}")
        return

    print(f"📊 Loading data from: {data_path}")

    # Load data
    df = pd.read_csv(data_path)

    # Standardize columns
    df.columns = [col.title() if col.lower() != 'time' else 'Time' for col in df.columns]

    # Set datetime index
    if 'Time' in df.columns:
        df.index = pd.to_datetime(df['Time'])
    elif 'Date' in df.columns:
        df.index = pd.to_datetime(df['Date'])
    else:
        df.index = pd.to_datetime(df.index)

    df = df.sort_index()

    # Remove duplicates
    df = df[~df.index.duplicated(keep='first')]

    print(f"✅ Data loaded: {len(df)} bars from {df.index[0]} to {df.index[-1]}")

    # Ensure required columns
    required = ['Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in df.columns for col in required):
        print(f"❌ Missing required columns. Available: {df.columns.tolist()}")
        return

    # Create backtest
    bt = Backtest(
        df,
        MarketStructureSupplyDemandStrategy,
        cash=10000,
        commission=0.002,
        margin=0.1,
        trade_on_close=False
    )

    # Strategy parameters
    params = {
        'swing_lookback': 5,
        'consolidation_lookback': 3,
        'min_rr_ratio': 2.5,
        'zone_strength_threshold': 70,
        'max_zone_tests': 3,
        'volatility_period': 14,
        'volume_spike_threshold': 1.5,
        'multi_tf_confirm': False,  # Disable for simple test
        'pullback_fib_min': 0.382,
        'correlation_threshold': 0.8
    }

    print("\n📊 Running backtest with parameters:")
    for key, value in params.items():
        print(f"   {key}: {value}")

    # Run backtest
    print("\n🔄 Running backtest...")
    stats = bt.run(**params)

    # Display full results
    print("\n" + "=" * 80)
    print("📊 FULL BACKTESTING.PY NATIVE RESULTS - Market Structure Strategy")
    print("🎯 Asset: BTCUSD | ⏰ Timeframe: 1d | 🏢 Provider: Coinbase")
    print("=" * 80)
    print(stats)
    print("=" * 80)

    # Display key metrics summary
    print("\n📈 KEY METRICS SUMMARY")
    print("=" * 60)
    print(f"Return: {stats['Return [%]']:.2f}%")
    print(f"Buy & Hold Return: {stats['Buy & Hold Return [%]']:.2f}%")
    print(f"Sharpe Ratio: {stats['Sharpe Ratio']:.3f}")
    print(f"Sortino Ratio: {stats['Sortino Ratio']:.3f}")
    print(f"Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
    print(f"Win Rate: {stats['Win Rate [%]']:.1f}%")
    print(f"Number of Trades: {stats['# Trades']}")
    print(f"Exposure Time: {stats['Exposure Time [%]']:.1f}%")

    # Plot if possible
    try:
        bt.plot(open_browser=False)
        print("\n✅ Strategy plot generated")
    except:
        print("\n⚠️ Could not generate plot")

    print("\n✅ Test complete!")


if __name__ == "__main__":
    test_single_asset()