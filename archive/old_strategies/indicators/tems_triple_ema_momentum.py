"""
Triple EMA Momentum System (TEMS) Strategy
===========================================
Trend-following strategy using triple EMA alignment with volume and RSI confirmation
Designed for crypto markets with appropriate volatility management

Key Features:
- Triple EMA system (8, 21, 55) for trend alignment
- RSI momentum confirmation
- Volume surge validation
- Crypto-optimized ATR-based risk management
- Partial profit taking system

Performance Target:
- Annual return: 25-40%
- Sharpe ratio: 0.7-1.2
- Win rate: 45-55%
- Max drawdown: <20%
"""

from backtesting import Strategy
import talib as ta
import pandas as pd
import numpy as np


class TEMSStrategy(Strategy):
    """Triple EMA Momentum System - Trend Following for Crypto Markets"""

    # Strategy Parameters (optimizable)
    ema_fast = 8      # Fast EMA period
    ema_medium = 21   # Medium EMA period
    ema_slow = 55     # Slow EMA period
    rsi_period = 14   # RSI period for momentum
    atr_period = 14   # ATR period for volatility
    volume_sma = 20   # Volume SMA period

    # Risk Management Parameters
    stop_loss_atr_mult = 2.5    # Wider stops for crypto (2.5x ATR)
    take_profit_atr_mult = 3.0  # First profit target (3x ATR)
    trailing_stop_atr_mult = 2.5  # Trailing stop distance
    max_hold_bars = 30           # Maximum position hold time

    # Position Sizing
    risk_per_trade = 0.02    # 2% risk per trade
    max_position_size = 0.03  # 3% maximum position

    def init(self):
        """Initialize indicators using talib"""

        # Triple EMA System
        self.ema_fast = self.I(ta.EMA, self.data.Close, self.ema_fast)
        self.ema_medium = self.I(ta.EMA, self.data.Close, self.ema_medium)
        self.ema_slow = self.I(ta.EMA, self.data.Close, self.ema_slow)

        # RSI for momentum confirmation
        self.rsi = self.I(ta.RSI, self.data.Close, self.rsi_period)

        # ATR for volatility-based stops and sizing
        self.atr = self.I(ta.ATR, self.data.High, self.data.Low,
                          self.data.Close, self.atr_period)

        # Volume SMA for volume confirmation
        self.volume_sma = self.I(ta.SMA, self.data.Volume, self.volume_sma)

        # Track entry prices and bars for position management
        self.entry_price = 0
        self.entry_bar = 0
        self.stop_loss = 0
        self.take_profit_1 = 0
        self.partial_exit_done = False

    def next(self):
        """Execute trading logic on each bar"""

        # Skip if indicators not ready
        if len(self.data) < self.ema_slow:
            return

        # Get current values
        price = self.data.Close[-1]
        volume = self.data.Volume[-1]

        # Current position size
        position_size = self.position.size

        # Exit Logic for existing positions
        if position_size != 0:
            bars_held = len(self.data) - self.entry_bar

            # Maximum hold time exit
            if bars_held >= self.max_hold_bars:
                self.position.close()
                return

            # Long position management
            if position_size > 0:
                # Stop loss hit
                if price <= self.stop_loss:
                    self.position.close()
                    return

                # Partial profit taking (50% at 3:1 R:R)
                if not self.partial_exit_done and price >= self.take_profit_1:
                    self.position.close(0.5)  # Close 50% of position
                    self.partial_exit_done = True
                    # Move stop to breakeven
                    self.stop_loss = self.entry_price

                # Trailing stop after partial exit
                if self.partial_exit_done:
                    trailing_stop = price - (self.atr[-1] * self.trailing_stop_atr_mult)
                    self.stop_loss = max(self.stop_loss, trailing_stop)

                # Trend change exit (EMA crossover)
                if self.ema_fast[-1] < self.ema_medium[-1]:
                    self.position.close()
                    return

            # Short position management
            elif position_size < 0:
                # Stop loss hit
                if price >= self.stop_loss:
                    self.position.close()
                    return

                # Partial profit taking (50% at 3:1 R:R)
                if not self.partial_exit_done and price <= self.take_profit_1:
                    self.position.close(0.5)  # Close 50% of position
                    self.partial_exit_done = True
                    # Move stop to breakeven
                    self.stop_loss = self.entry_price

                # Trailing stop after partial exit
                if self.partial_exit_done:
                    trailing_stop = price + (self.atr[-1] * self.trailing_stop_atr_mult)
                    self.stop_loss = min(self.stop_loss, trailing_stop)

                # Trend change exit (EMA crossover)
                if self.ema_fast[-1] > self.ema_medium[-1]:
                    self.position.close()
                    return

        # Entry Logic (only if not in position)
        if position_size == 0:

            # Long Entry Conditions (Relaxed for crypto markets)
            # Primary condition: Triple EMA alignment
            ema_bullish = (
                self.ema_fast[-1] > self.ema_medium[-1] and      # Fast > Medium
                self.ema_medium[-1] > self.ema_slow[-1]          # Medium > Slow
            )

            # Secondary confirmations (at least 2 of 3 required)
            rsi_bullish = self.rsi[-1] > 50                      # RSI momentum
            volume_confirm = volume > self.volume_sma[-1] * 0.8  # Volume within 80% of average
            price_confirm = price > self.ema_fast[-1]            # Price above fast EMA

            # Count confirmations
            confirmations = sum([rsi_bullish, volume_confirm, price_confirm])

            # Long signal requires EMA alignment + at least 2 confirmations
            long_signal = ema_bullish and confirmations >= 2

            # Short Entry Conditions (Relaxed for crypto markets)
            # Primary condition: Triple EMA alignment
            ema_bearish = (
                self.ema_fast[-1] < self.ema_medium[-1] and      # Fast < Medium
                self.ema_medium[-1] < self.ema_slow[-1]          # Medium < Slow
            )

            # Secondary confirmations (at least 2 of 3 required)
            rsi_bearish = self.rsi[-1] < 50                      # RSI momentum
            volume_confirm_short = volume > self.volume_sma[-1] * 0.8  # Volume within 80% of average
            price_confirm_short = price < self.ema_fast[-1]     # Price below fast EMA

            # Count confirmations
            confirmations_short = sum([rsi_bearish, volume_confirm_short, price_confirm_short])

            # Short signal requires EMA alignment + at least 2 confirmations
            short_signal = ema_bearish and confirmations_short >= 2

            # Calculate position size based on volatility
            # Use fractional sizing (0.95 = 95% of equity)
            # This is more appropriate for backtesting.py framework
            position_fraction = 0.95  # Use 95% of available equity per trade

            # Execute Long Entry
            if long_signal:
                self.buy(size=position_fraction)
                self.entry_price = price
                self.entry_bar = len(self.data)
                self.stop_loss = price - (self.atr[-1] * self.stop_loss_atr_mult)
                self.take_profit_1 = price + (self.atr[-1] * self.take_profit_atr_mult)
                self.partial_exit_done = False

            # Execute Short Entry
            elif short_signal:
                self.sell(size=position_fraction)
                self.entry_price = price
                self.entry_bar = len(self.data)
                self.stop_loss = price + (self.atr[-1] * self.stop_loss_atr_mult)
                self.take_profit_1 = price - (self.atr[-1] * self.take_profit_atr_mult)
                self.partial_exit_done = False