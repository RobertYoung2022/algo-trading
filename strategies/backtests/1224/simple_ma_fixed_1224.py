"""
🚀 Simple MA Crossover (Fixed) - December 2024
===============================================
Simplified working version of MA crossover strategy.
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from data_loader_1224 import load_and_validate_data
import warnings
warnings.filterwarnings('ignore')


class SimpleMAFixed1224(Strategy):
    """Simple MA Crossover that actually works"""

    fast_period = 10
    slow_period = 30

    def init(self):
        # Calculate SMAs
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)

    def next(self):
        # Skip if indicators not ready
        if len(self.data) < self.slow_period:
            return

        # Simple crossover logic
        if crossover(self.sma_fast, self.sma_slow):
            if not self.position:
                self.buy()
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()


def run_backtest(strategy_class, data, initial_cash=10000, commission=0.002):
    """Run backtest with full stats output"""
    try:
        bt = Backtest(
            data,
            strategy_class,
            cash=initial_cash,
            commission=commission,
            exclusive_orders=True
        )

        stats = bt.run()

        print("\n" + "="*80)
        print("📊 COMPLETE BACKTESTING RESULTS")
        print("="*80)
        print(stats)
        print("="*80 + "\n")

        return stats, bt

    except Exception as e:
        print(f"❌ Backtest error: {e}")
        return None, None


if __name__ == "__main__":
    # Test on daily BTC data
    test_file = "/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv"

    print("\n" + "="*80)
    print("🚀 SIMPLE MA CROSSOVER (FIXED) - DECEMBER 2024")
    print("="*80)

    data, quality_score, valid = load_and_validate_data(test_file)

    if valid and data is not None:
        print(f"✅ Data loaded: {len(data)} bars")
        print(f"📊 Quality Score: {quality_score:.1f}/100")

        # Test with different MA periods
        test_configs = [
            (10, 30, "Fast"),
            (20, 50, "Medium"),
            (50, 200, "Slow")
        ]

        for fast, slow, name in test_configs:
            print(f"\n🔍 Testing {name} ({fast}/{slow}) MA Crossover:")

            class TestStrategy(SimpleMAFixed1224):
                fast_period = fast
                slow_period = slow

            stats, bt = run_backtest(TestStrategy, data)

            if stats is not None:
                print(f"\n📈 {name} Results:")
                print(f"   Return: {stats['Return [%]']:.2f}%")
                print(f"   Sharpe: {stats['Sharpe Ratio']:.2f}")
                print(f"   Max DD: {stats['Max. Drawdown [%]']:.2f}%")
                print(f"   Trades: {stats['# Trades']}")

                # Check deployment criteria
                if (stats['Return [%]'] >= 20 and
                    stats['Sharpe Ratio'] >= 1.5 and
                    stats['Max. Drawdown [%]'] >= -15):
                    print(f"   ✅ MEETS DEPLOYMENT CRITERIA!")
                else:
                    print(f"   ❌ Does not meet deployment criteria")