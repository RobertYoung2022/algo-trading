"""
ADX Trend Strength System (ATSS) - Simplified Version
=====================================================

Simplified ATSS with more relaxed conditions to ensure trades are generated
while maintaining the core strategy logic of ADX trend strength with pullback entries.

Key Simplifications:
- Single EMA for pullback reference (50-period)
- Relaxed ADX threshold (20 instead of 25-30)
- Wider pullback tolerance (5%)
- Simplified entry logic
- Fixed position sizing

Author: Bobby's Algo Trading System
Date: 2025-01-17
"""

from backtesting import Strategy
from backtesting.lib import crossover
import talib as ta
import pandas as pd
import numpy as np


class ATSSSimpleStrategy(Strategy):
    """
    Simplified ADX Trend Strength System

    Core Logic:
    1. ADX > 20 indicates trending market
    2. Buy when price pulls back to EMA50 in uptrend
    3. Sell when price pulls back to EMA50 in downtrend
    4. Exit when ADX < 20 or after fixed holding period
    """

    # ADX parameters
    adx_period = 14
    adx_threshold = 20  # Lower threshold for more signals
    adx_exit = 18

    # Moving average for pullback reference
    ema_period = 50

    # Entry parameters
    pullback_zone = 0.05  # 5% zone around EMA

    # Risk management
    stop_loss_pct = 0.05  # 5% stop loss
    take_profit_pct = 0.10  # 10% take profit
    max_holding_bars = 30  # Maximum holding period

    # Position sizing
    position_size_pct = 0.95  # Use 95% of equity

    def init(self):
        """Initialize indicators"""

        # Price data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low

        # ADX and directional indicators
        self.adx = self.I(ta.ADX, high, low, close, self.adx_period)
        self.plus_di = self.I(ta.PLUS_DI, high, low, close, self.adx_period)
        self.minus_di = self.I(ta.MINUS_DI, high, low, close, self.adx_period)

        # Moving average for pullback reference
        self.ema = self.I(ta.EMA, close, self.ema_period)

        # ATR for volatility-based stops
        self.atr = self.I(ta.ATR, high, low, close, 14)

        # Position tracking
        self.entry_bar = 0
        self.entry_price = 0

    def next(self):
        """Execute simplified trading logic"""

        # Skip if not enough data
        if len(self.data) < self.ema_period + 1:
            return

        current_price = self.data.Close[-1]
        current_bar = len(self.data) - 1

        # Exit existing positions
        if self.position:
            bars_held = current_bar - self.entry_bar

            # Exit conditions
            should_exit = False

            # 1. ADX drops below exit threshold (trend weakening)
            if self.adx[-1] < self.adx_exit:
                should_exit = True

            # 2. Maximum holding period reached
            elif bars_held >= self.max_holding_bars:
                should_exit = True

            # 3. Stop loss or take profit
            elif self.position.size > 0:  # Long position
                if current_price <= self.entry_price * (1 - self.stop_loss_pct):
                    should_exit = True
                elif current_price >= self.entry_price * (1 + self.take_profit_pct):
                    should_exit = True
            else:  # Short position
                if current_price >= self.entry_price * (1 + self.stop_loss_pct):
                    should_exit = True
                elif current_price <= self.entry_price * (1 - self.take_profit_pct):
                    should_exit = True

            if should_exit:
                self.position.close()
                self.entry_bar = 0
                self.entry_price = 0

        # Entry logic for new positions
        else:
            # Check if market is trending (ADX above threshold)
            if self.adx[-1] > self.adx_threshold:

                # Calculate pullback zones
                ema_upper = self.ema[-1] * (1 + self.pullback_zone)
                ema_lower = self.ema[-1] * (1 - self.pullback_zone)

                # Bullish trend with pullback entry
                if (self.plus_di[-1] > self.minus_di[-1] and  # Uptrend
                    current_price > self.ema[-1] * 0.95 and  # Above EMA zone
                    current_price < self.ema[-1] * 1.05):  # Within pullback zone

                    # Additional confirmation: price should be coming down from above
                    if len(self.data) > 2 and self.data.Close[-2] > current_price:
                        self.buy(size=self.position_size_pct)
                        self.entry_bar = current_bar
                        self.entry_price = current_price

                # Bearish trend with pullback entry
                elif (self.minus_di[-1] > self.plus_di[-1] and  # Downtrend
                      current_price < self.ema[-1] * 1.05 and  # Below EMA zone
                      current_price > self.ema[-1] * 0.95):  # Within pullback zone

                    # Additional confirmation: price should be coming up from below
                    if len(self.data) > 2 and self.data.Close[-2] < current_price:
                        self.sell(size=self.position_size_pct)
                        self.entry_bar = current_bar
                        self.entry_price = current_price