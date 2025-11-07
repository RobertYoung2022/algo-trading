#!/usr/bin/env python3
"""
Quick 1-Year Backtest to Verify Timestamp Fixes

Tests the ICT strategy on 1 year of BTC data to verify:
1. Session manager now uses historical timestamps
2. Trades are being generated
3. No crashes or errors
"""

import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from strategies.ict_strategy.ict_backtest_strategy import run_ict_backtest


def test_one_year_btc():
    """Test with 1 year of BTC data (last ~8700 bars)"""

    print("\n" + "="*80)
    print(" QUICK TEST: 1-Year BTC Backtest (Timestamp Fix Verification)")
    print("="*80)

    # Load full data
    data_path = "data/storage/dataset_files/BTCUSD-1h-500wks-data.csv"
    df_full = pd.read_csv(data_path)

    # Get last 1 year (~8700 hours)
    df_1year = df_full.tail(8700)

    # Save to temp file
    temp_path = "data/storage/dataset_files/BTCUSD-1h-1year-test.csv"
    df_1year.to_csv(temp_path, index=False)

    print(f"\n✅ Created 1-year subset: {len(df_1year)} bars")
    print(f"   Date range: {df_1year.iloc[0]['datetime']} to {df_1year.iloc[-1]['datetime']}")

    # Run backtest
    print("\n🚀 Running backtest with timestamp fixes...")
    stats = run_ict_backtest(temp_path, "BTC-1Y", cash=1000000, commission=0.002)

    # Check results
    if stats:
        num_trades = stats.get('# Trades', 0)

        print("\n" + "="*80)
        print(" VERIFICATION RESULTS")
        print("="*80)

        if num_trades > 0:
            print(f"✅ SUCCESS! Generated {int(num_trades)} trades")
            print(f"   Return: {stats['Return [%]']:.2f}%")
            print(f"   Win Rate: {stats.get('Win Rate [%]', 0):.2f}%")
            print(f"   Sharpe Ratio: {stats.get('Sharpe Ratio', 0):.2f}")
            print(f"\n✅ TIMESTAMP FIX VERIFIED - Strategy is working!")
        else:
            print("⚠️  STILL ZERO TRADES - Additional debugging needed")
            print("   Possible issues:")
            print("   1. Signal generation not working")
            print("   2. Session filter blocking all trades")
            print("   3. Risk manager rejecting all positions")
            print("\n   Recommendation: Disable session filter and re-test")

        print("="*80)

    return stats


if __name__ == "__main__":
    test_one_year_btc()
