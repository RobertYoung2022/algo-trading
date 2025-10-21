"""
🌙 Bollinger Bands Mean Reversion Strategy - December 2024
============================================================
Advanced mean reversion strategy using Bollinger Bands and multi-exit conditions.
Implements proper position sizing and time-based exits.

🌟 Key Features:
    - Bollinger Bands (20, 2) for overbought/oversold detection
    - Dynamic position sizing based on risk
    - Multiple exit conditions (target, stop, time)
    - Proper risk management with 3% stop loss
    - Multi-asset testing capability

💫 Strategy Logic:
    - BUY: When price touches lower band (oversold)
    - SELL: When price touches upper band (overbought)
    - EXIT: At middle band (profit target), stop loss, or 10 bars
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from data_loader_1224 import load_and_validate_data
import warnings
warnings.filterwarnings('ignore')


class BollingerMeanReversion1224(Strategy):
    """
    🎯 Bollinger Bands Mean Reversion Strategy
    Capitalizes on price extremes reverting to the mean
    """

    # Strategy parameters
    bb_period = 20        # Bollinger Bands period
    bb_std = 2           # Number of standard deviations
    stop_loss_pct = 0.03 # 3% stop loss
    risk_per_trade = 0.01 # Risk 1% per trade
    max_holding_bars = 10 # Maximum holding period

    def init(self):
        """
        🚀 Initialize Bollinger Bands using talib
        """
        # Calculate Bollinger Bands using talib
        self.bb_upper, self.bb_middle, self.bb_lower = self.I(
            talib.BBANDS,
            self.data.Close,
            timeperiod=self.bb_period,
            nbdevup=self.bb_std,
            nbdevdn=self.bb_std,
            matype=0
        )

        # Track entry information
        self.entry_price = 0
        self.entry_bar = 0
        self.position_type = None  # 'long' or 'short'

    def next(self):
        """
        🎯 Execute mean reversion logic on each bar
        """
        # Skip if indicators not ready
        if len(self.data) < self.bb_period:
            return

        # Get current values
        current_price = self.data.Close[-1]
        current_low = self.data.Low[-1]
        current_high = self.data.High[-1]

        # Skip if bands are invalid
        if not self.bb_lower[-1] or not self.bb_upper[-1] or not self.bb_middle[-1]:
            return

        # Check if we have an open position
        if not self.position:
            # Long entry: Price touches lower band (oversold)
            if current_low <= self.bb_lower[-1]:
                # Calculate position size
                stop_loss = current_price * (1 - self.stop_loss_pct)
                stop_distance = current_price - stop_loss
                risk_amount = self.equity * self.risk_per_trade
                position_size = min(risk_amount / stop_distance, self.equity * 0.95)

                # Enter long position
                self.buy(size=position_size, sl=stop_loss)
                self.entry_price = current_price
                self.entry_bar = len(self.data)
                self.position_type = 'long'

            # Short entry: Price touches upper band (overbought)
            elif current_high >= self.bb_upper[-1]:
                # Calculate position size
                stop_loss = current_price * (1 + self.stop_loss_pct)
                stop_distance = stop_loss - current_price
                risk_amount = self.equity * self.risk_per_trade
                position_size = min(risk_amount / stop_distance, self.equity * 0.95)

                # Enter short position
                self.sell(size=position_size, sl=stop_loss)
                self.entry_price = current_price
                self.entry_bar = len(self.data)
                self.position_type = 'short'

        # Manage existing positions
        else:
            bars_since_entry = len(self.data) - self.entry_bar

            # Long position management
            if self.position.is_long:
                # Exit conditions for long
                exit_signal = False

                # 1. Target reached: Price reaches middle band
                if current_price >= self.bb_middle[-1]:
                    exit_signal = True

                # 2. Time exit: Held for max bars
                elif bars_since_entry >= self.max_holding_bars:
                    exit_signal = True

                # 3. Trailing stop in profit
                elif current_price > self.entry_price * 1.01:  # In profit
                    new_stop = current_price * (1 - self.stop_loss_pct * 0.5)
                    if new_stop > self.position.sl:
                        self.position.sl = new_stop

                if exit_signal:
                    self.position.close()
                    self.entry_price = 0
                    self.entry_bar = 0
                    self.position_type = None

            # Short position management
            elif self.position.is_short:
                # Exit conditions for short
                exit_signal = False

                # 1. Target reached: Price reaches middle band
                if current_price <= self.bb_middle[-1]:
                    exit_signal = True

                # 2. Time exit: Held for max bars
                elif bars_since_entry >= self.max_holding_bars:
                    exit_signal = True

                # 3. Trailing stop in profit
                elif current_price < self.entry_price * 0.99:  # In profit
                    new_stop = current_price * (1 + self.stop_loss_pct * 0.5)
                    if new_stop < self.position.sl:
                        self.position.sl = new_stop

                if exit_signal:
                    self.position.close()
                    self.entry_price = 0
                    self.entry_bar = 0
                    self.position_type = None


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
            hedging=False,
            trade_on_close=False
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


def test_on_single_asset(file_path, strategy_class=BollingerMeanReversion1224):
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
    print("🌙 BOLLINGER BANDS MEAN REVERSION STRATEGY - DECEMBER 2024")
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