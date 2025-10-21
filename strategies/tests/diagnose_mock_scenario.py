"""
🔍 Diagnostic Script for Mock Scenario Testing
Helps debug why strategy isn't trading on mock scenarios
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from strategies.tests.fixtures.mock_data_generators import generate_valid_breakout_scenario
from strategies.core_strategies.breakout_momentum_strategy import BreakoutMomentumStrategy


def diagnose_scenario():
    """🔬 Diagnose valid breakout scenario"""

    print("🔍 Generating valid breakout scenario...")
    df = generate_valid_breakout_scenario()

    print(f"\n📊 Scenario Stats:")
    print(f"   Total Bars: {len(df)}")
    print(f"   Price Range: ${df['Close'].min():.2f} - ${df['Close'].max():.2f}")
    print(f"   Price Change: {((df['Close'].iloc[-1] / df['Close'].iloc[0]) - 1) * 100:+.2f}%")
    print(f"   Avg Volume: {df['Volume'].mean() / 1_000_000:.2f}M")

    # Check consolidation phase
    consolidation = df.iloc[0:80]
    print(f"\n🔧 Consolidation Phase (bars 0-80):")
    print(f"   Price Range: ${consolidation['Close'].min():.2f} - ${consolidation['Close'].max():.2f}")
    print(f"   Range Size: {(consolidation['Close'].max() - consolidation['Close'].min()):.2f} ({((consolidation['Close'].max() - consolidation['Close'].min()) / consolidation['Close'].mean() * 100):.2f}%)")
    print(f"   Avg Volume: {consolidation['Volume'].mean() / 1_000_000:.2f}M")

    # Check breakout phase
    breakout = df.iloc[80:90]
    print(f"\n🚀 Breakout Phase (bars 80-90):")
    print(f"   Price Range: ${breakout['Close'].min():.2f} - ${breakout['Close'].max():.2f}")
    print(f"   High Point: ${breakout['High'].max():.2f}")
    print(f"   Avg Volume: {breakout['Volume'].mean() / 1_000_000:.2f}M")
    print(f"   Volume Surge: {breakout['Volume'].mean() / consolidation['Volume'].mean():.2f}x")

    # Simulate strategy logic at bar 85
    print(f"\n🎯 Strategy Logic Check at Bar 85:")
    lookback = 20
    bar_idx = 85

    recent_window = df.iloc[bar_idx-lookback:bar_idx+1]
    range_high = recent_window['High'].iloc[:-1].max()  # Excluding current bar
    range_low = recent_window['Low'].iloc[:-1].min()
    current_high = df['High'].iloc[bar_idx]
    current_low = df['Low'].iloc[bar_idx]
    current_price = df['Close'].iloc[bar_idx]

    avg_volume = df['Volume'].iloc[bar_idx-lookback:bar_idx].mean()
    current_volume = df['Volume'].iloc[bar_idx]

    print(f"   Range High (last 20 bars): ${range_high:.2f}")
    print(f"   Current High: ${current_high:.2f}")
    print(f"   Breakout? {current_high > range_high} (current > range)")
    print(f"   Breakout Delta: ${current_high - range_high:.2f}")
    print(f"")
    print(f"   Avg Volume (last 20 bars): {avg_volume / 1_000_000:.2f}M")
    print(f"   Current Volume: {current_volume / 1_000_000:.2f}M")
    print(f"   Volume Confirmed? {current_volume >= avg_volume * 1.5} ({current_volume / avg_volume:.2f}x)")
    print(f"")
    print(f"   Should Enter Trade? {(current_high > range_high) and (current_volume >= avg_volume * 1.5)}")

    # Run backtest
    print(f"\n🧪 Running Backtest...")
    bt = Backtest(
        df,
        BreakoutMomentumStrategy,
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    stats = bt.run()

    print(f"\n📈 Backtest Results:")
    print(f"   Return: {stats['Return [%]']:.2f}%")
    print(f"   # Trades: {stats['# Trades']}")
    print(f"   Win Rate: {stats['Win Rate [%]']:.1f}%")
    print(f"   Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")

    # Check for open trades
    if hasattr(bt, '_trades'):
        print(f"\n🔍 Trade Analysis:")
        print(f"   Total Trades (incl. open): {len(bt._trades)}")
        if bt._trades:
            for i, trade in enumerate(bt._trades):
                print(f"   Trade {i+1}: Entry ${trade.entry_price:.2f}, Exit ${trade.exit_price if trade.exit_price else 'OPEN':.2f}")

    # Try with finalize_trades
    print(f"\n🔄 Re-running with finalize_trades=True...")
    bt2 = Backtest(
        df,
        BreakoutMomentumStrategy,
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    stats2 = bt2.run(finalize_trades=True)

    print(f"\n📈 Backtest Results (with finalized trades):")
    print(f"   Return: {stats2['Return [%]']:.2f}%")
    print(f"   # Trades: {stats2['# Trades']}")
    print(f"   Win Rate: {stats2['Win Rate [%]']:.1f}%")


if __name__ == '__main__':
    diagnose_scenario()
