#!/usr/bin/env python3
"""
Simple Test Strategy - Verify Backtesting Framework

This creates a dead-simple strategy that just buys on bar 100 and sells on bar 200
to verify the backtesting framework is working correctly.
"""

import pandas as pd
from backtesting import Strategy
from backtesting.lib import crossover
from backtesting.test import GOOG
import backtesting.backtesting as bt_module
from pathlib import Path


class SimpleTestStrategy(Strategy):
    """
    Ultra-simple strategy to verify backtesting works:
    - Buy on bar 100
    - Sell on bar 200
    - Should generate exactly 1 trade
    """

    def init(self):
        """Initialize - nothing needed"""
        self.bar_count = 0
        print("✅ SimpleTestStrategy initialized")

    def next(self):
        """Execute simple buy/sell logic"""
        self.bar_count += 1

        # Buy on bar 100 (use 10% of capital)
        if self.bar_count == 100 and not self.position:
            self.buy(size=0.1)  # 10% of capital
            print(f"📈 BUY (10% capital) at bar {self.bar_count}, price ${self.data.Close[-1]:.2f}")

        # Sell on bar 200
        elif self.bar_count == 200 and self.position:
            self.position.close()
            print(f"📉 SELL at bar {self.bar_count}, price ${self.data.Close[-1]:.2f}")


def test_framework():
    """Test backtesting framework with simple strategy"""

    print("\n" + "="*80)
    print(" FRAMEWORK TEST: Simple Buy/Sell Strategy")
    print("="*80)

    # Load 1-year BTC data
    data_path = "data/storage/dataset_files/BTCUSD-1h-1year-test.csv"
    print(f"\n📁 Loading data: {data_path}")

    df = pd.read_csv(data_path)

    # Parse datetime
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    # Capitalize columns
    df.columns = [c.capitalize() for c in df.columns]

    print(f"📊 Data loaded: {len(df)} bars")
    print(f"📅 Date range: {df.index[0]} to {df.index[-1]}")

    # Run backtest
    print("\n🚀 Running simple test strategy...")
    print("   Expected: 1 trade (buy bar 100, sell bar 200)")

    # Use cash=1000000 (1M) to ensure we have enough margin for 10% position
    bt = bt_module.Backtest(df, SimpleTestStrategy, cash=1000000, commission=0.002, trade_on_close=False)
    stats = bt.run()

    # Check results
    print("\n" + "="*80)
    print(" RESULTS")
    print("="*80)

    num_trades = stats['# Trades']
    print(f"\n# Trades: {num_trades}")

    if num_trades == 1:
        print("✅ SUCCESS! Framework is working correctly!")
        print(f"   Return: {stats['Return [%]']:.2f}%")
        print(f"   Entry: Bar 100")
        print(f"   Exit: Bar 200")
        print("\n✅ FRAMEWORK VERIFIED - Can proceed to test ICT signal generation")
    elif num_trades == 0:
        print("❌ FAILURE! No trades executed")
        print("   Issue: backtesting.buy() or position.close() not working")
        print("   This indicates a framework problem, NOT an ICT strategy problem")
    else:
        print(f"⚠️  Unexpected: {num_trades} trades (expected 1)")

    print("="*80)

    return stats


if __name__ == "__main__":
    test_framework()
