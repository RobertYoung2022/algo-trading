"""
ADX Trend Strength System (ATSS) Strategy
==========================================

A trend-following strategy that identifies strong trending markets using ADX
and enters on pullbacks to key moving averages for optimal risk-reward.

Key Features:
- ADX-based trend strength identification (>30 for strong trends)
- Pullback entries to EMA50 in trending markets
- Trend-adaptive position sizing based on ADX strength
- Comprehensive risk management with trailing stops
- Multi-timeframe trend confirmation

Author: Bobby's Algo Trading System
Date: 2025-01-17
"""

from backtesting import Strategy
from backtesting.lib import crossover
import talib as ta
import pandas as pd
import numpy as np


class ATSSStrategy(Strategy):
    """
    ADX Trend Strength System (ATSS)

    Parameters:
    -----------
    adx_period : int
        Period for ADX calculation (default: 14)
    adx_threshold : float
        Minimum ADX value for strong trend (default: 30)
    adx_very_strong : float
        ADX value for very strong trend (default: 40)
    ema_fast : int
        Fast EMA period for pullback entries (default: 50)
    ema_slow : int
        Slow EMA period for trend structure (default: 200)
    sma_period : int
        SMA period for additional confirmation (default: 20)
    rsi_period : int
        RSI period for momentum confirmation (default: 14)
    rsi_pullback_long : float
        Maximum RSI for long pullback entries (default: 60)
    rsi_pullback_short : float
        Minimum RSI for short pullback entries (default: 40)
    pullback_tolerance : float
        Price tolerance to EMA for pullback entries (default: 0.02)
    initial_stop_pct : float
        Initial stop loss percentage from EMA (default: 0.05)
    adx_exit_threshold : float
        ADX level below which to exit (trend weakening) (default: 20)
    max_holding_bars : int
        Maximum bars to hold position (default: 25)
    risk_base : float
        Base risk percentage for position sizing (default: 0.02)
    risk_strong : float
        Risk percentage for strong trends (ADX > 40) (default: 0.025)
    risk_moderate : float
        Risk percentage for moderate trends (ADX < 30) (default: 0.015)
    first_target_rr : float
        Risk-reward ratio for first profit target (default: 2.0)
    scale_entry_pct : float
        Initial position size as percentage of full size (default: 0.5)
    add_on_pct : float
        Additional position size on confirmation (default: 0.25)
    """

    # Strategy parameters
    adx_period = 14
    adx_threshold = 30
    adx_very_strong = 40
    ema_fast = 50
    ema_slow = 200
    sma_period = 20
    rsi_period = 14
    rsi_pullback_long = 60
    rsi_pullback_short = 40
    pullback_tolerance = 0.02
    initial_stop_pct = 0.05
    adx_exit_threshold = 20
    max_holding_bars = 25
    risk_base = 0.02
    risk_strong = 0.025
    risk_moderate = 0.015
    first_target_rr = 2.0
    scale_entry_pct = 0.5
    add_on_pct = 0.25

    def init(self):
        """Initialize strategy indicators"""

        # Price data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low

        # ADX for trend strength
        self.adx = self.I(ta.ADX, high, low, close, self.adx_period)
        self.plus_di = self.I(ta.PLUS_DI, high, low, close, self.adx_period)
        self.minus_di = self.I(ta.MINUS_DI, high, low, close, self.adx_period)

        # Moving averages for trend structure
        self.ema_fast = self.I(ta.EMA, close, self.ema_fast)
        self.ema_slow = self.I(ta.EMA, close, self.ema_slow)
        self.sma = self.I(ta.SMA, close, self.sma_period)

        # RSI for momentum confirmation
        self.rsi = self.I(ta.RSI, close, self.rsi_period)

        # MACD for additional confirmation
        def calculate_macd():
            return ta.MACD(close, 12, 26, 9)

        macd_result = self.I(calculate_macd, name='MACD_Calc')
        self.macd = self.I(lambda: macd_result[0], name='MACD')
        self.macd_signal = self.I(lambda: macd_result[1], name='MACD_Signal')
        self.macd_histogram = self.I(lambda: macd_result[2], name='MACD_Histogram')

        # Parabolic SAR for trailing stops
        self.sar = self.I(ta.SAR, high, low, 0.02, 0.2)

        # Position tracking
        self.entry_bar = 0
        self.entry_price = 0
        self.stop_loss = 0
        self.take_profit_1 = 0
        self.position_scaled = False
        self.first_target_hit = False
        self.trend_type = None  # 'bullish' or 'bearish'

    def calculate_position_size(self):
        """Calculate position size based on ADX strength"""
        current_adx = self.adx[-1]

        if current_adx > self.adx_very_strong:
            return self.risk_strong  # Very strong trend
        elif current_adx > self.adx_threshold:
            return self.risk_base  # Strong trend
        else:
            return self.risk_moderate  # Moderate trend

    def check_strong_uptrend(self):
        """Check if market is in a strong uptrend"""
        return (
            self.adx[-1] > self.adx_threshold and
            self.plus_di[-1] > self.minus_di[-1] and
            self.data.Close[-1] > self.ema_fast[-1] and
            self.ema_fast[-1] > self.ema_slow[-1] and
            self.ema_fast[-1] > self.ema_fast[-2]  # EMA50 rising
        )

    def check_strong_downtrend(self):
        """Check if market is in a strong downtrend"""
        return (
            self.adx[-1] > self.adx_threshold and
            self.minus_di[-1] > self.plus_di[-1] and
            self.data.Close[-1] < self.ema_fast[-1] and
            self.ema_fast[-1] < self.ema_slow[-1] and
            self.ema_fast[-1] < self.ema_fast[-2]  # EMA50 falling
        )

    def check_pullback_buy(self):
        """Check for pullback buying opportunity"""
        price_near_ema = self.data.Close[-1] <= self.ema_fast[-1] * (1 + self.pullback_tolerance)

        return (
            self.check_strong_uptrend() and
            price_near_ema and
            self.rsi[-1] < self.rsi_pullback_long and
            self.macd[-1] > self.macd_signal[-1]
        )

    def check_pullback_sell(self):
        """Check for pullback selling opportunity"""
        price_near_ema = self.data.Close[-1] >= self.ema_fast[-1] * (1 - self.pullback_tolerance)

        return (
            self.check_strong_downtrend() and
            price_near_ema and
            self.rsi[-1] > self.rsi_pullback_short and
            self.macd[-1] < self.macd_signal[-1]
        )

    def next(self):
        """Execute trading logic"""

        # Skip if not enough data
        if len(self.data) < self.ema_slow:
            return

        current_price = self.data.Close[-1]
        current_bar = len(self.data) - 1

        # Position management for existing positions
        if self.position:
            bars_held = current_bar - self.entry_bar

            # Check for trend weakening exit
            if self.adx[-1] < self.adx_exit_threshold:
                self.position.close()
                self.reset_position_tracking()
                return

            # Time-based exit
            if bars_held >= self.max_holding_bars:
                self.position.close()
                self.reset_position_tracking()
                return

            # Long position management
            if self.position.size > 0:
                # Scale in if not yet scaled and trend continues
                if not self.position_scaled and bars_held > 2:
                    if self.check_strong_uptrend() and current_price > self.entry_price:
                        add_size = self.position.size * (self.add_on_pct / self.scale_entry_pct)
                        self.buy(size=add_size)
                        self.position_scaled = True

                # Check first profit target
                if not self.first_target_hit and current_price >= self.take_profit_1:
                    # Close half position at first target
                    self.position.close(0.5)
                    self.first_target_hit = True
                    # Update stop to breakeven
                    self.stop_loss = self.entry_price

                # Trailing stop with SAR
                if self.first_target_hit and current_price < self.sar[-1]:
                    self.position.close()
                    self.reset_position_tracking()
                    return

                # Initial stop loss
                if current_price < self.stop_loss:
                    self.position.close()
                    self.reset_position_tracking()
                    return

            # Short position management
            elif self.position.size < 0:
                # Scale in if not yet scaled and trend continues
                if not self.position_scaled and bars_held > 2:
                    if self.check_strong_downtrend() and current_price < self.entry_price:
                        add_size = abs(self.position.size) * (self.add_on_pct / self.scale_entry_pct)
                        self.sell(size=add_size)
                        self.position_scaled = True

                # Check first profit target
                if not self.first_target_hit and current_price <= self.take_profit_1:
                    # Close half position at first target
                    self.position.close(0.5)
                    self.first_target_hit = True
                    # Update stop to breakeven
                    self.stop_loss = self.entry_price

                # Trailing stop with SAR
                if self.first_target_hit and current_price > self.sar[-1]:
                    self.position.close()
                    self.reset_position_tracking()
                    return

                # Initial stop loss
                if current_price > self.stop_loss:
                    self.position.close()
                    self.reset_position_tracking()
                    return

        # Entry logic for new positions
        else:
            # Check for pullback buy opportunity
            if self.check_pullback_buy():
                position_size = self.calculate_position_size() * self.scale_entry_pct
                self.buy(size=position_size)

                # Set position tracking
                self.entry_bar = current_bar
                self.entry_price = current_price
                self.stop_loss = self.ema_fast[-1] * (1 - self.initial_stop_pct)
                risk_amount = self.entry_price - self.stop_loss
                self.take_profit_1 = self.entry_price + (risk_amount * self.first_target_rr)
                self.position_scaled = False
                self.first_target_hit = False
                self.trend_type = 'bullish'

            # Check for pullback sell opportunity
            elif self.check_pullback_sell():
                position_size = self.calculate_position_size() * self.scale_entry_pct
                self.sell(size=position_size)

                # Set position tracking
                self.entry_bar = current_bar
                self.entry_price = current_price
                self.stop_loss = self.ema_fast[-1] * (1 + self.initial_stop_pct)
                risk_amount = self.stop_loss - self.entry_price
                self.take_profit_1 = self.entry_price - (risk_amount * self.first_target_rr)
                self.position_scaled = False
                self.first_target_hit = False
                self.trend_type = 'bearish'

    def reset_position_tracking(self):
        """Reset all position tracking variables"""
        self.entry_bar = 0
        self.entry_price = 0
        self.stop_loss = 0
        self.take_profit_1 = 0
        self.position_scaled = False
        self.first_target_hit = False
        self.trend_type = None