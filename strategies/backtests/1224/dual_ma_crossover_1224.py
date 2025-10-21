"""
🚀 Dual Moving Average Crossover Strategy - December 2024
===========================================================
Advanced SMA crossover strategy with dynamic position sizing and risk management.
Uses pandas_ta for indicators and implements proper backtesting.py framework.

🌟 Key Features:
    - Dual SMA (50/200) crossover signals
    - ATR-based stop loss positioning
    - Dynamic position sizing based on risk
    - Multi-asset testing capability
    - Data quality validation

💫 Strategy Logic:
    - BUY: When fast SMA crosses above slow SMA (golden cross)
    - SELL: When fast SMA crosses below slow SMA (death cross)
    - Risk: 1% per trade with ATR-based stops
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from data_loader_1224 import load_and_validate_data
import warnings
warnings.filterwarnings('ignore')


class DualMACrossover1224(Strategy):
    """
    🎯 Dual Moving Average Crossover with Risk Management
    Advanced trend-following strategy using SMA crossovers
    """

    # Strategy parameters
    fast_period = 50      # Fast moving average period
    slow_period = 200     # Slow moving average period
    atr_period = 14       # ATR period for volatility
    atr_multiplier = 2.0  # ATR multiplier for stop loss
    risk_per_trade = 0.01 # Risk 1% per trade

    def init(self):
        """
        🚀 Initialize indicators using pandas_ta
        """
        # Calculate SMAs using talib
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)

        # Calculate ATR for stop loss using talib
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)

        # Track entry price for risk management
        self.entry_price = 0
        self.entry_bar = 0

    def next(self):
        """
        🎯 Execute trading logic on each bar
        """
        # Skip if indicators not ready
        if len(self.data) < self.slow_period:
            return

        # Get current values
        current_price = self.data.Close[-1]
        current_atr = self.atr[-1] if self.atr[-1] else 0

        # Skip if ATR is invalid
        if current_atr <= 0:
            return

        # Position sizing based on risk
        stop_distance = current_atr * self.atr_multiplier
        risk_amount = self.equity * self.risk_per_trade
        position_size = min(risk_amount / stop_distance, self.equity * 0.95)

        # Check for golden cross (bullish signal)
        if not self.position and crossover(self.sma_fast, self.sma_slow):
            # Calculate stop loss price
            stop_loss = current_price - stop_distance

            # Enter long position
            self.buy(size=position_size, sl=stop_loss)
            self.entry_price = current_price
            self.entry_bar = len(self.data)

        # Check for death cross (bearish signal)
        elif self.position and crossover(self.sma_slow, self.sma_fast):
            # Exit all positions
            self.position.close()
            self.entry_price = 0
            self.entry_bar = 0

        # Trail stop loss if in profit
        elif self.position and self.position.is_long:
            bars_since_entry = len(self.data) - self.entry_bar

            # After 10 bars, start trailing stop
            if bars_since_entry > 10:
                new_stop = current_price - (current_atr * self.atr_multiplier * 0.8)
                if new_stop > self.position.sl:
                    self.position.sl = new_stop


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
            exclusive_orders=True
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


def test_on_single_asset(file_path, strategy_class=DualMACrossover1224):
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

    if data is None or len(data) < 200:
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
    print("🚀 DUAL MOVING AVERAGE CROSSOVER STRATEGY - DECEMBER 2024")
    print("="*80)

    stats = test_on_single_asset(test_file)

    if stats is not None:
        # Analyze performance
        print("\n🎯 Performance Summary:")
        print(f"Total Return: {stats['Return [%]']:.2f}%")
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