"""
💫 Simple SMA Crossover Strategy - December 2024
==================================================
Clean implementation of classic SMA crossover with proper risk management.
Uses pandas_ta for technical indicators and slippage modeling.

🌟 Key Features:
    - Classic 50/200 SMA crossover
    - Position sizing with 1% risk allocation
    - Slippage modeling (0.1%)
    - Clean entry/exit logic
    - Multi-asset compatibility

💫 Strategy Logic:
    - BUY: Fast SMA crosses above slow SMA (golden cross)
    - SELL: Fast SMA crosses below slow SMA (death cross)
    - Position Size: Risk-adjusted based on account equity
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from data_loader_1224 import load_and_validate_data
import warnings
warnings.filterwarnings('ignore')


class SimpleSMACrossover1224(Strategy):
    """
    🎯 Simple SMA Crossover Strategy
    Classic trend-following approach with clean implementation
    """

    # Strategy parameters
    fast_sma = 50        # Fast SMA period
    slow_sma = 200       # Slow SMA period
    position_pct = 0.95  # Use 95% of available capital
    slippage_pct = 0.001 # 0.1% slippage

    def init(self):
        """
        🚀 Initialize SMA indicators using pandas_ta
        """
        # Calculate SMAs using talib
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_sma)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_sma)

        # Track signals for cleaner logic
        self.golden_cross = False
        self.death_cross = False

    def next(self):
        """
        🎯 Execute crossover logic on each bar
        """
        # Skip if indicators not ready
        if len(self.data) < self.slow_sma:
            return

        # Skip if SMAs are invalid
        if not self.sma_fast[-1] or not self.sma_slow[-1]:
            return

        # Get current price for position sizing
        current_price = self.data.Close[-1]

        # Calculate effective price with slippage
        buy_price = current_price * (1 + self.slippage_pct)
        sell_price = current_price * (1 - self.slippage_pct)

        # Check for crossover signals
        if not self.position:
            # Golden Cross: Fast SMA crosses above Slow SMA
            if crossover(self.sma_fast, self.sma_slow):
                # Calculate position size accounting for slippage
                available_cash = self.equity * self.position_pct
                position_size = available_cash / buy_price

                # Enter long position
                self.buy(size=position_size)
                self.golden_cross = True
                self.death_cross = False

        elif self.position.is_long:
            # Death Cross: Fast SMA crosses below Slow SMA
            if crossover(self.sma_slow, self.sma_fast):
                # Exit position with slippage consideration
                self.position.close()
                self.death_cross = True
                self.golden_cross = False

            # Optional: Add profit protection
            elif self.position.pl_pct > 0.20:  # 20% profit
                # Tighten exit conditions when in significant profit
                if self.sma_fast[-1] < self.sma_fast[-2]:  # Fast SMA turning down
                    self.position.close()


# Data loading function imported from data_loader_1224


def run_backtest(strategy_class, data, initial_cash=10000, commission=0.002):
    """
    🚀 Run backtest with full stats output
    """
    try:
        # Create backtest
        bt = Backtest(
            data,
            strategy_class,
            cash=initial_cash,
            commission=commission,
            exclusive_orders=True,
            hedging=False
        )

        # Run backtest
        stats = bt.run()

        # Print complete stats (never summarized)
        print("\n" + "="*80)
        print("📊 COMPLETE BACKTESTING RESULTS")
        print("="*80)
        print(stats)
        print("="*80 + "\n")

        return stats, bt

    except Exception as e:
        print(f"❌ Backtest error: {e}")
        return None, None


def test_on_single_asset(file_path, strategy_class=SimpleSMACrossover1224):
    """
    🎯 Test strategy on a single asset
    """
    print(f"\n🔍 Testing on: {file_path}")

    # Load and validate data
    data, quality_score, validation_passed = load_and_validate_data(file_path)

    print(f"📊 Data Quality Score: {quality_score:.1f}/100")

    if not validation_passed:
        print(f"❌ Data quality too low (< 75), skipping...")
        return None

    if data is None or len(data) < 250:  # Need extra data for 200 SMA
        print(f"❌ Insufficient data for testing")
        return None

    # Run backtest
    stats, bt = run_backtest(strategy_class, data)

    if stats is not None:
        # Show plot
        try:
            bt.plot(open_browser=False)
        except:
            pass

    return stats


if __name__ == "__main__":
    """
    🚀 Test the strategy on sample data
    """
    # Test on a single asset first
    test_file = "/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv"

    print("\n" + "="*80)
    print("💫 SIMPLE SMA CROSSOVER STRATEGY - DECEMBER 2024")
    print("="*80)

    stats = test_on_single_asset(test_file)

    if stats is not None:
        # Analyze performance
        print("\n🎯 Performance Summary:")
        print(f"Total Return: {stats['Return [%]']:.2f}%")
        print(f"Buy & Hold Return: {stats['Buy & Hold Return [%]']:.2f}%")
        print(f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}")
        print(f"Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"Number of Trades: {stats['# Trades']}")

        # Check if meets bot deployment criteria
        if (stats['Return [%]'] >= 20 and
            stats['Sharpe Ratio'] >= 1.5 and
            stats['Max. Drawdown [%]'] >= -15):
            print("\n✅ MEETS BOT DEPLOYMENT CRITERIA!")
        else:
            print("\n❌ Does not meet bot deployment criteria")
            print(f"   - Return: {stats['Return [%]']:.2f}% (need ≥20%)")
            print(f"   - Sharpe: {stats['Sharpe Ratio']:.2f} (need ≥1.5)")
            print(f"   - Max DD: {stats['Max. Drawdown [%]']:.2f}% (need ≥-15%)")