"""
🌙 BinHV45 Mean-Reversion Strategy 🌙
====================================
Advanced mean-reversion strategy using Bollinger Bands for 1-minute scalping.
Implements specific entry conditions based on BB width, price action, and candle structure.

Strategy Type: Mean-Reversion
Timeframe: 1-minute (primary), 5-minute (comparison)
Framework: backtesting.py with native results display

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Strategy
from backtesting.lib import crossover


class BinHV45Strategy(Strategy):
    """
    🚀 BinHV45 Mean-Reversion Strategy Implementation 🚀

    Advanced mean-reversion strategy exploiting oversold conditions below Bollinger Bands
    with multiple confirmation filters for high-probability entries.

    Entry Conditions (ALL must be met):
    1. Prior lower BB > 0 (valid BB calculation)
    2. bbdelta is large (significant BB width)
    3. closedelta is large (significant price movement)
    4. Current close < prior lower BB (price below previous lower band)
    5. Current close <= prior close (price declining or flat)
    6. tail is small (close near the low of the candle)

    Exit Conditions:
    - Fixed Stop Loss: -5% from entry
    - Take Profit: +1.25% from entry
    """

    # 🎯 Strategy Parameters
    bb_period = 40  # Bollinger Bands period
    bb_std = 2.0    # Standard deviations for BB

    # Entry thresholds
    bbdelta_threshold_pct = 1.5  # BB width as % of middle band
    closedelta_threshold_pct = 0.5  # Close delta as % of price
    tail_threshold_pct = 0.2  # Tail as % of candle range

    # Exit parameters
    stop_loss_pct = 5.0    # Stop loss percentage
    take_profit_pct = 1.25  # Take profit percentage

    def init(self):
        """
        🔧 Initialize all technical indicators and signals
        """
        # 🌙 Calculate Bollinger Bands using talib (Bobby's preference)
        close = self.data.Close

        # Calculate BB components
        self.bb_upper, self.bb_middle, self.bb_lower = self.I(
            talib.BBANDS,
            close,
            timeperiod=self.bb_period,
            nbdevup=self.bb_std,
            nbdevdn=self.bb_std,
            matype=0  # SMA
        )

        # 📊 Calculate derived indicators
        # bbdelta: Bollinger Band width
        self.bbdelta = self.I(lambda: self.bb_upper - self.bb_lower)

        # closedelta: Absolute close price change
        self.closedelta = self.I(lambda: np.abs(np.diff(np.concatenate([[close[0]], close]))))

        # tail: Distance from close to low
        self.tail = self.I(lambda: self.data.Close - self.data.Low)

        # 💫 Additional helper signals
        # Candle range (high - low)
        self.candle_range = self.I(lambda: self.data.High - self.data.Low)

        # Price as percentage of BB middle (for threshold calculations)
        self.price_pct = self.I(lambda: self.bb_middle)

    def next(self):
        """
        🎯 Main trading logic - evaluate entry and exit conditions
        """
        # Skip if we don't have enough data for indicators
        if len(self.data) < self.bb_period + 1:
            return

        # 🛡️ Risk management - only one position at a time
        if self.position:
            return  # Let stop loss and take profit handle exits

        # 📊 Get current and previous indicator values
        current_close = self.data.Close[-1]
        prior_close = self.data.Close[-2] if len(self.data) >= 2 else current_close

        current_lower_bb = self.bb_lower[-1]
        prior_lower_bb = self.bb_lower[-2] if len(self.bb_lower) >= 2 else current_lower_bb

        current_bbdelta = self.bbdelta[-1]
        current_closedelta = self.closedelta[-1]
        current_tail = self.tail[-1]
        current_range = self.candle_range[-1]
        current_bb_middle = self.bb_middle[-1]

        # 🔍 Entry Condition 1: Prior lower BB > 0 (valid BB)
        condition1 = prior_lower_bb > 0

        # 🔍 Entry Condition 2: bbdelta is large (significant BB width)
        bbdelta_threshold = current_bb_middle * (self.bbdelta_threshold_pct / 100)
        condition2 = current_bbdelta > bbdelta_threshold

        # 🔍 Entry Condition 3: closedelta is large (significant price movement)
        closedelta_threshold = current_close * (self.closedelta_threshold_pct / 100)
        condition3 = current_closedelta > closedelta_threshold

        # 🔍 Entry Condition 4: Current close < prior lower BB
        condition4 = current_close < prior_lower_bb

        # 🔍 Entry Condition 5: Current close <= prior close (declining/flat)
        condition5 = current_close <= prior_close

        # 🔍 Entry Condition 6: tail is small (close near low)
        tail_threshold = current_range * (self.tail_threshold_pct / 100) if current_range > 0 else 0
        condition6 = current_tail <= tail_threshold

        # 🚀 Execute long entry if ALL conditions are met
        if all([condition1, condition2, condition3, condition4, condition5, condition6]):
            # Calculate position size (conservative for 1-minute scalping)
            position_size = 0.95  # Use 95% of available capital

            # Set stop loss and take profit levels
            stop_loss_price = current_close * (1 - self.stop_loss_pct / 100)
            take_profit_price = current_close * (1 + self.take_profit_pct / 100)

            # 💰 Enter long position with fixed SL/TP
            self.buy(
                size=position_size,
                sl=stop_loss_price,
                tp=take_profit_price
            )

            # 📝 Log entry (optional - for debugging)
            # print(f"🎯 Entry at {current_close:.4f} | SL: {stop_loss_price:.4f} | TP: {take_profit_price:.4f}")


# 🌙💫🚀 BinHV45 Mean-Reversion Strategy Ready for Deployment 🌙💫🚀