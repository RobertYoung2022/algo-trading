"""
🔍 Diagnose ATR Filter Behavior
Debug why ATR filter might be blocking valid breakouts
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from strategies.tests.fixtures.mock_data_generators import generate_valid_breakout_scenario, generate_false_breakout_scenario
from strategies.core_strategies.breakout_momentum_strategy import BreakoutMomentumStrategy


def diagnose_atr_on_scenario(scenario_name, df):
    """Analyze ATR behavior on a scenario"""

    print(f"\n{'='*60}")
    print(f"🔍 Diagnosing: {scenario_name}")
    print(f"{'='*60}\n")

    # Calculate ATR manually
    high = df['High'].values
    low = df['Low'].values
    close = df['Close'].values

    # Calculate True Range
    tr_list = []
    for i in range(1, len(df)):
        hl = high[i] - low[i]
        hc = abs(high[i] - close[i-1])
        lc = abs(low[i] - close[i-1])
        tr_list.append(max(hl, hc, lc))

    # Calculate ATR (14-period)
    atr_period = 14
    atr_values = []
    for i in range(atr_period, len(tr_list) + 1):
        atr = sum(tr_list[i-atr_period:i]) / atr_period
        atr_values.append(atr)

    # Calculate percentiles
    atr_50th = sorted(atr_values)[int(len(atr_values) * 0.5)]
    atr_70th = sorted(atr_values)[int(len(atr_values) * 0.7)]
    atr_80th = sorted(atr_values)[int(len(atr_values) * 0.8)]
    atr_90th = sorted(atr_values)[int(len(atr_values) * 0.9)]

    # Check ATR at breakout point (around bar 85)
    breakout_bar = 85 if len(df) >= 100 else len(df) - 20
    if breakout_bar < len(atr_values):
        atr_at_breakout = atr_values[breakout_bar - atr_period]

        print(f"📊 ATR Statistics:")
        print(f"   50th percentile: {atr_50th:.2f}")
        print(f"   70th percentile: {atr_70th:.2f}")
        print(f"   80th percentile: {atr_80th:.2f}")
        print(f"   90th percentile: {atr_90th:.2f}")
        print(f"\n🎯 ATR at Breakout (bar {breakout_bar}):")
        print(f"   Value: {atr_at_breakout:.2f}")
        print(f"   vs 70th percentile: {'REJECTED ❌' if atr_at_breakout > atr_70th else 'ACCEPTED ✅'}")
        print(f"   vs 80th percentile: {'REJECTED ❌' if atr_at_breakout > atr_80th else 'ACCEPTED ✅'}")
        print(f"   vs 90th percentile: {'REJECTED ❌' if atr_at_breakout > atr_90th else 'ACCEPTED ✅'}")

        # Calculate ATR as % of price
        avg_price = df['Close'].mean()
        atr_pct = (atr_at_breakout / avg_price) * 100
        print(f"   ATR as % of price: {atr_pct:.2f}%")

    # Test with strategy
    print(f"\n🧪 Testing with Strategy (70th percentile):")

    bt = Backtest(
        df,
        BreakoutMomentumStrategy,
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    stats = bt.run()
    print(f"   Return: {stats['Return [%]']:.2f}%")
    print(f"   # Trades: {stats['# Trades']}")

    return atr_values


if __name__ == '__main__':
    print("🔍 ATR Filter Diagnostic Tool\n")

    # Test valid breakout
    valid_df = generate_valid_breakout_scenario()
    valid_atr = diagnose_atr_on_scenario("Valid Breakout", valid_df)

    # Test false breakout
    false_df = generate_false_breakout_scenario()
    false_atr = diagnose_atr_on_scenario("False Breakout", false_df)

    # Compare
    print(f"\n{'='*60}")
    print(f"📊 Comparison")
    print(f"{'='*60}")
    print(f"Valid breakout mean ATR: {np.mean(valid_atr):.2f}")
    print(f"False breakout mean ATR: {np.mean(false_atr):.2f}")
    print(f"Ratio: {np.mean(false_atr) / np.mean(valid_atr):.2f}x")
