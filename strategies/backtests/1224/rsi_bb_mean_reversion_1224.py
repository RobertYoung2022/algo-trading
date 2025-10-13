"""
🌙 RSI + Bollinger Bands Mean Reversion Strategy - December 2024
==================================================================
Advanced mean reversion combining RSI oversold/overbought with Bollinger Bands.
Implements reversal pattern detection and multi-condition exits.

🌟 Key Features:
    - Dual confirmation: RSI + Bollinger Bands
    - Reversal pattern detection (pin bars, engulfing)
    - Dynamic stop loss with buffer
    - Time-based exit after 5 days
    - Risk-adjusted position sizing

💫 Strategy Logic:
    - LONG: Price < Lower BB AND RSI < 30 AND reversal pattern
    - SHORT: Price > Upper BB AND RSI > 70 AND reversal pattern
    - EXIT: Middle band reached, stop loss hit, or 5-day timeout
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from data_loader_1224 import load_and_validate_data
import warnings
warnings.filterwarnings('ignore')


class RSIBBMeanReversion1224(Strategy):
    """
    🎯 Advanced Mean Reversion with RSI and Bollinger Bands
    Combines momentum and volatility for high-probability reversals
    """

    # Strategy parameters
    bb_period = 20          # Bollinger Bands period
    bb_std = 2             # Standard deviations
    rsi_period = 14        # RSI period
    rsi_oversold = 30      # RSI oversold threshold
    rsi_overbought = 70    # RSI overbought threshold
    stop_loss_pct = 0.01   # 1% stop loss buffer
    risk_per_trade = 0.01  # Risk 1% per trade
    max_holding_days = 5   # Maximum holding period

    def init(self):
        """
        🚀 Initialize indicators using talib
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

        # Calculate RSI using talib
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)

        # Track entry information
        self.entry_price = 0
        self.entry_bar = 0
        self.position_type = None

    def is_bullish_reversal(self):
        """
        🔍 Detect bullish reversal patterns
        """
        if len(self.data) < 3:
            return False

        # Current and previous candles
        curr_open = self.data.Open[-1]
        curr_close = self.data.Close[-1]
        curr_high = self.data.High[-1]
        curr_low = self.data.Low[-1]

        prev_open = self.data.Open[-2]
        prev_close = self.data.Close[-2]
        prev_high = self.data.High[-2]
        prev_low = self.data.Low[-2]

        # Pin bar (hammer) pattern
        body = abs(curr_close - curr_open)
        lower_wick = min(curr_open, curr_close) - curr_low
        upper_wick = curr_high - max(curr_open, curr_close)

        if lower_wick > body * 2 and upper_wick < body * 0.5:
            return True

        # Bullish engulfing pattern
        if (prev_close < prev_open and  # Previous bearish
            curr_close > curr_open and   # Current bullish
            curr_close > prev_open and   # Engulfs previous
            curr_open < prev_close):
            return True

        # Strong bullish candle after decline
        if curr_close > curr_open and (curr_close - curr_open) > (curr_high - curr_low) * 0.7:
            return True

        return False

    def is_bearish_reversal(self):
        """
        🔍 Detect bearish reversal patterns
        """
        if len(self.data) < 3:
            return False

        # Current and previous candles
        curr_open = self.data.Open[-1]
        curr_close = self.data.Close[-1]
        curr_high = self.data.High[-1]
        curr_low = self.data.Low[-1]

        prev_open = self.data.Open[-2]
        prev_close = self.data.Close[-2]
        prev_high = self.data.High[-2]
        prev_low = self.data.Low[-2]

        # Inverted pin bar (shooting star) pattern
        body = abs(curr_close - curr_open)
        upper_wick = curr_high - max(curr_open, curr_close)
        lower_wick = min(curr_open, curr_close) - curr_low

        if upper_wick > body * 2 and lower_wick < body * 0.5:
            return True

        # Bearish engulfing pattern
        if (prev_close > prev_open and  # Previous bullish
            curr_close < curr_open and   # Current bearish
            curr_close < prev_open and   # Engulfs previous
            curr_open > prev_close):
            return True

        # Strong bearish candle after rally
        if curr_close < curr_open and (curr_open - curr_close) > (curr_high - curr_low) * 0.7:
            return True

        return False

    def next(self):
        """
        🎯 Execute mean reversion logic with multiple confirmations
        """
        # Skip if indicators not ready
        if len(self.data) < max(self.bb_period, self.rsi_period):
            return

        # Get current values
        current_price = self.data.Close[-1]
        current_low = self.data.Low[-1]
        current_high = self.data.High[-1]

        # Skip if indicators are invalid
        if (not self.bb_lower[-1] or not self.bb_upper[-1] or
            not self.bb_middle[-1] or not self.rsi[-1]):
            return

        # Check if we have an open position
        if not self.position:
            # Long entry conditions
            if (current_price < self.bb_lower[-1] and
                self.rsi[-1] < self.rsi_oversold and
                self.is_bullish_reversal()):

                # Calculate stop loss with buffer
                recent_low = min(self.data.Low[-3:])
                stop_loss = recent_low * (1 - self.stop_loss_pct)

                # Calculate position size
                stop_distance = current_price - stop_loss
                risk_amount = self.equity * self.risk_per_trade
                position_size = min(risk_amount / stop_distance, self.equity * 0.95)

                # Enter long position
                self.buy(size=position_size, sl=stop_loss)
                self.entry_price = current_price
                self.entry_bar = len(self.data)
                self.position_type = 'long'

            # Short entry conditions
            elif (current_price > self.bb_upper[-1] and
                  self.rsi[-1] > self.rsi_overbought and
                  self.is_bearish_reversal()):

                # Calculate stop loss with buffer
                recent_high = max(self.data.High[-3:])
                stop_loss = recent_high * (1 + self.stop_loss_pct)

                # Calculate position size
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
                exit_signal = False

                # 1. Target reached: Price at or above middle band
                if current_price >= self.bb_middle[-1]:
                    exit_signal = True

                # 2. Time exit: Held for max days
                elif bars_since_entry >= self.max_holding_days:
                    exit_signal = True

                # 3. RSI divergence: RSI turning down from overbought
                elif self.rsi[-1] > 70 and self.rsi[-1] < self.rsi[-2]:
                    exit_signal = True

                # 4. Trail stop in significant profit
                elif current_price > self.entry_price * 1.02:  # 2% profit
                    new_stop = current_price * (1 - self.stop_loss_pct * 0.7)
                    if new_stop > self.position.sl:
                        self.position.sl = new_stop

                if exit_signal:
                    self.position.close()
                    self.entry_price = 0
                    self.entry_bar = 0
                    self.position_type = None

            # Short position management
            elif self.position.is_short:
                exit_signal = False

                # 1. Target reached: Price at or below middle band
                if current_price <= self.bb_middle[-1]:
                    exit_signal = True

                # 2. Time exit: Held for max days
                elif bars_since_entry >= self.max_holding_days:
                    exit_signal = True

                # 3. RSI divergence: RSI turning up from oversold
                elif self.rsi[-1] < 30 and self.rsi[-1] > self.rsi[-2]:
                    exit_signal = True

                # 4. Trail stop in significant profit
                elif current_price < self.entry_price * 0.98:  # 2% profit
                    new_stop = current_price * (1 + self.stop_loss_pct * 0.7)
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


def test_on_single_asset(file_path, strategy_class=RSIBBMeanReversion1224):
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

    if data is None or len(data) < 100:
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
    print("🌙 RSI + BOLLINGER BANDS MEAN REVERSION STRATEGY - DECEMBER 2024")
    print("="*80)

    stats = test_on_single_asset(test_file)

    if stats is not None:
        # Analyze performance
        print("\n🎯 Performance Summary:")
        print(f"Total Return: {stats['Return [%]']:.2f}%")
        print(f"Buy & Hold Return: {stats['Buy & Hold Return [%]']:.2f}%")
        print(f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}")
        print(f"Sortino Ratio: {stats['Sortino Ratio']:.2f}")
        print(f"Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"Number of Trades: {stats['# Trades']}")
        print(f"Profit Factor: {stats.get('Profit Factor', 'N/A')}")

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