"""
Triple EMA Momentum System (TEMS) - Long Only Version
=======================================================
Long-only trend following strategy optimized for crypto markets
Uses triple EMA alignment with momentum confirmation

Key Features:
- Long-only positioning (no shorting)
- Triple EMA system (8, 21, 55) for trend identification
- RSI and volume confirmation
- ATR-based dynamic stops
- Partial profit taking at key levels
"""

from backtesting import Strategy
import talib as ta
import pandas as pd
import numpy as np


class TEMSLongOnlyStrategy(Strategy):
    """Long-only TEMS Strategy optimized for crypto uptrends"""

    # EMA Periods
    ema_fast = 8      # Fast EMA for signals
    ema_medium = 21   # Medium EMA for trend
    ema_slow = 55     # Slow EMA for major trend

    # Indicators
    rsi_period = 14   # RSI for momentum
    atr_period = 14   # ATR for volatility
    volume_sma = 20   # Volume moving average

    # Risk Management
    stop_loss_atr_mult = 2.0      # Initial stop loss (2x ATR)
    take_profit_atr_mult = 4.0    # Take profit target (4x ATR)
    trailing_stop_atr_mult = 2.5  # Trailing stop distance
    breakeven_atr_mult = 1.5      # Move stop to breakeven after 1.5x ATR profit

    # Position Management
    max_hold_bars = 40             # Maximum holding period
    position_size_pct = 0.95       # Use 95% of available equity

    def init(self):
        """Initialize all indicators"""

        # Triple EMA System
        self.ema_fast = self.I(ta.EMA, self.data.Close, self.ema_fast)
        self.ema_medium = self.I(ta.EMA, self.data.Close, self.ema_medium)
        self.ema_slow = self.I(ta.EMA, self.data.Close, self.ema_slow)

        # Momentum and Volatility
        self.rsi = self.I(ta.RSI, self.data.Close, self.rsi_period)
        self.atr = self.I(ta.ATR, self.data.High, self.data.Low,
                          self.data.Close, self.atr_period)

        # Volume Analysis
        self.volume_sma = self.I(ta.SMA, self.data.Volume, self.volume_sma)

        # Position tracking
        self.entry_price = 0
        self.entry_bar = 0
        self.stop_loss = 0
        self.take_profit = 0
        self.highest_price = 0
        self.partial_exit_done = False

    def next(self):
        """Execute trading logic"""

        # Wait for indicators to be ready
        if len(self.data) < self.ema_slow:
            return

        price = self.data.Close[-1]
        volume = self.data.Volume[-1]

        # Position Management
        if self.position.size > 0:
            # Track highest price for trailing stop
            self.highest_price = max(self.highest_price, price)

            # Check holding period
            bars_held = len(self.data) - self.entry_bar
            if bars_held >= self.max_hold_bars:
                self.position.close()
                return

            # Stop Loss Hit
            if price <= self.stop_loss:
                self.position.close()
                return

            # Take Profit (partial exit at 50%)
            if not self.partial_exit_done and price >= self.take_profit:
                self.position.close(0.5)  # Take 50% profit
                self.partial_exit_done = True
                # Move stop to breakeven
                self.stop_loss = self.entry_price

            # Trailing Stop Logic
            if price > self.entry_price + (self.atr[-1] * self.breakeven_atr_mult):
                # Calculate trailing stop from highest price
                trailing_stop = self.highest_price - (self.atr[-1] * self.trailing_stop_atr_mult)
                self.stop_loss = max(self.stop_loss, trailing_stop)

            # Exit on trend reversal (EMAs cross bearish)
            if self.ema_fast[-1] < self.ema_medium[-1]:
                self.position.close()
                return

        # Entry Logic (only when flat)
        elif self.position.size == 0:

            # Primary Trend Condition: Triple EMA Alignment
            trend_bullish = (
                self.ema_fast[-1] > self.ema_medium[-1] and
                self.ema_medium[-1] > self.ema_slow[-1]
            )

            # Entry Filters
            momentum_bullish = self.rsi[-1] > 45  # RSI above 45 (relaxed from 50)
            volume_confirm = volume > self.volume_sma[-1] * 0.75  # Volume at least 75% of average
            price_above_fast = price > self.ema_fast[-1] * 0.995  # Price near or above fast EMA

            # Additional trend strength check
            ema_spreading = (
                (self.ema_fast[-1] - self.ema_medium[-1]) > 0 and
                (self.ema_medium[-1] - self.ema_slow[-1]) > 0
            )

            # Entry Signal: Trend + at least 2 of 3 filters
            filters_passed = sum([momentum_bullish, volume_confirm, price_above_fast])

            if trend_bullish and filters_passed >= 2 and ema_spreading:
                # Enter Long Position
                self.buy(size=self.position_size_pct)

                # Set position parameters
                self.entry_price = price
                self.entry_bar = len(self.data)
                self.stop_loss = price - (self.atr[-1] * self.stop_loss_atr_mult)
                self.take_profit = price + (self.atr[-1] * self.take_profit_atr_mult)
                self.highest_price = price
                self.partial_exit_done = False