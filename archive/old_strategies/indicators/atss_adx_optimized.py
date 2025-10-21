"""
ADX Trend Strength System (ATSS) - Optimized Version
====================================================

Optimized parameters for better trade frequency while maintaining quality.
Adjusted thresholds to capture more pullback opportunities in trending markets.

Key Optimizations:
- Lower ADX threshold (25 vs 30) to capture more trending markets
- Wider pullback tolerance (3% vs 2%) to catch more entries
- Relaxed RSI thresholds for pullback entries
- Shorter holding period for quicker capital rotation
- Dynamic EMA selection based on timeframe

Author: Bobby's Algo Trading System
Date: 2025-01-17
"""

from backtesting import Strategy
from backtesting.lib import crossover
import talib as ta
import pandas as pd
import numpy as np


class ATSSOptimizedStrategy(Strategy):
    """
    ADX Trend Strength System - Optimized for Crypto Markets

    Optimized Parameters:
    ---------------------
    adx_threshold: 25 (lowered from 30)
    pullback_tolerance: 0.03 (increased from 0.02)
    rsi_pullback_long: 65 (increased from 60)
    rsi_pullback_short: 35 (decreased from 40)
    max_holding_bars: 20 (reduced from 25)
    """

    # Optimized strategy parameters
    adx_period = 14
    adx_threshold = 25  # Lowered to capture more trends
    adx_very_strong = 35  # Lowered from 40
    adx_exit_threshold = 18  # Slightly lower exit

    # Dynamic EMA periods
    ema_fast = 20  # Faster EMA for quicker signals
    ema_medium = 50
    ema_slow = 200
    sma_period = 20

    # RSI parameters - more relaxed
    rsi_period = 14
    rsi_pullback_long = 65  # Allow higher RSI
    rsi_pullback_short = 35  # Allow lower RSI
    rsi_oversold = 30
    rsi_overbought = 70

    # Entry parameters - wider tolerance
    pullback_tolerance = 0.03  # 3% tolerance
    breakout_tolerance = 0.005  # For alternative entry

    # Risk management
    initial_stop_pct = 0.04  # Tighter stop
    trailing_stop_pct = 0.03
    max_holding_bars = 20  # Shorter holding

    # Position sizing
    risk_base = 0.025  # Slightly higher base risk
    risk_strong = 0.03
    risk_moderate = 0.02

    # Profit targets
    first_target_rr = 1.5  # Lower first target for quicker profits
    second_target_rr = 2.5
    scale_entry_pct = 0.6  # Start with larger position
    add_on_pct = 0.4

    def init(self):
        """Initialize optimized indicators"""

        # Price data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low

        # ADX for trend strength
        self.adx = self.I(ta.ADX, high, low, close, self.adx_period)
        self.plus_di = self.I(ta.PLUS_DI, high, low, close, self.adx_period)
        self.minus_di = self.I(ta.MINUS_DI, high, low, close, self.adx_period)

        # Multiple EMAs for better trend structure
        self.ema_fast = self.I(ta.EMA, close, self.ema_fast)
        self.ema_medium = self.I(ta.EMA, close, self.ema_medium)
        self.ema_slow = self.I(ta.EMA, close, self.ema_slow)
        self.sma = self.I(ta.SMA, close, self.sma_period)

        # RSI for momentum
        self.rsi = self.I(ta.RSI, close, self.rsi_period)

        # MACD for confirmation
        def calculate_macd():
            return ta.MACD(close, 12, 26, 9)

        macd_result = self.I(calculate_macd, name='MACD_Full')
        self.macd = self.I(lambda: macd_result[0], name='MACD_Line')
        self.macd_signal = self.I(lambda: macd_result[1], name='MACD_Signal')
        self.macd_histogram = self.I(lambda: macd_result[2], name='MACD_Hist')

        # Additional indicators for better signals
        self.atr = self.I(ta.ATR, high, low, close, 14)

        # Bollinger Bands for volatility context
        def calculate_bbands():
            return ta.BBANDS(close, 20, 2, 2)

        bbands = self.I(calculate_bbands, name='BBands')
        self.bb_upper = self.I(lambda: bbands[0], name='BB_Upper')
        self.bb_middle = self.I(lambda: bbands[1], name='BB_Middle')
        self.bb_lower = self.I(lambda: bbands[2], name='BB_Lower')

        # Parabolic SAR for trailing
        self.sar = self.I(ta.SAR, high, low, 0.02, 0.2)

        # Position tracking
        self.entry_bar = 0
        self.entry_price = 0
        self.stop_loss = 0
        self.take_profit_1 = 0
        self.take_profit_2 = 0
        self.position_scaled = False
        self.first_target_hit = False
        self.trend_type = None

    def calculate_position_size(self):
        """Dynamic position sizing based on ADX and volatility"""
        current_adx = self.adx[-1]

        # Consider volatility for position sizing
        atr_pct = (self.atr[-1] / self.data.Close[-1]) * 100
        volatility_adjustment = 1.0

        if atr_pct > 5:  # High volatility
            volatility_adjustment = 0.7
        elif atr_pct < 2:  # Low volatility
            volatility_adjustment = 1.2

        if current_adx > self.adx_very_strong:
            base_size = self.risk_strong
        elif current_adx > self.adx_threshold:
            base_size = self.risk_base
        else:
            base_size = self.risk_moderate

        return base_size * volatility_adjustment

    def check_strong_uptrend(self):
        """Enhanced uptrend detection with multiple confirmations"""

        # Basic ADX trend
        adx_uptrend = (
            self.adx[-1] > self.adx_threshold and
            self.plus_di[-1] > self.minus_di[-1] and
            self.adx[-1] > self.adx[-2]  # ADX increasing
        )

        # EMA alignment
        ema_alignment = (
            self.data.Close[-1] > self.ema_fast[-1] and
            self.ema_fast[-1] > self.ema_medium[-1] and
            self.ema_medium[-1] > self.ema_slow[-1]
        )

        # Momentum confirmation
        momentum_confirm = (
            self.macd[-1] > 0 or  # MACD above zero
            self.rsi[-1] > 45  # RSI showing strength
        )

        return adx_uptrend and ema_alignment and momentum_confirm

    def check_strong_downtrend(self):
        """Enhanced downtrend detection with multiple confirmations"""

        # Basic ADX trend
        adx_downtrend = (
            self.adx[-1] > self.adx_threshold and
            self.minus_di[-1] > self.plus_di[-1] and
            self.adx[-1] > self.adx[-2]  # ADX increasing
        )

        # EMA alignment
        ema_alignment = (
            self.data.Close[-1] < self.ema_fast[-1] and
            self.ema_fast[-1] < self.ema_medium[-1] and
            self.ema_medium[-1] < self.ema_slow[-1]
        )

        # Momentum confirmation
        momentum_confirm = (
            self.macd[-1] < 0 or  # MACD below zero
            self.rsi[-1] < 55  # RSI showing weakness
        )

        return adx_downtrend and ema_alignment and momentum_confirm

    def check_pullback_buy(self):
        """Optimized pullback buying with multiple entry zones"""

        # Check for uptrend
        if not self.check_strong_uptrend():
            return False

        current_price = self.data.Close[-1]

        # Multiple pullback zones for entries
        pullback_to_fast_ema = (
            current_price <= self.ema_fast[-1] * (1 + self.pullback_tolerance) and
            current_price >= self.ema_fast[-1] * (1 - 0.01)  # Not too far below
        )

        pullback_to_medium_ema = (
            current_price <= self.ema_medium[-1] * (1 + self.pullback_tolerance) and
            current_price >= self.ema_medium[-1] * (1 - 0.01)
        )

        # Bollinger Band squeeze entry
        bb_squeeze_entry = (
            current_price <= self.bb_lower[-1] * 1.02 and
            self.rsi[-1] < 40  # Oversold in uptrend
        )

        # RSI and MACD conditions
        momentum_ready = (
            self.rsi[-1] < self.rsi_pullback_long and
            (self.macd[-1] > self.macd_signal[-1] or
             self.macd_histogram[-1] > self.macd_histogram[-2])  # MACD improving
        )

        return (pullback_to_fast_ema or pullback_to_medium_ema or bb_squeeze_entry) and momentum_ready

    def check_pullback_sell(self):
        """Optimized pullback selling with multiple entry zones"""

        # Check for downtrend
        if not self.check_strong_downtrend():
            return False

        current_price = self.data.Close[-1]

        # Multiple pullback zones for entries
        pullback_to_fast_ema = (
            current_price >= self.ema_fast[-1] * (1 - self.pullback_tolerance) and
            current_price <= self.ema_fast[-1] * (1 + 0.01)  # Not too far above
        )

        pullback_to_medium_ema = (
            current_price >= self.ema_medium[-1] * (1 - self.pullback_tolerance) and
            current_price <= self.ema_medium[-1] * (1 + 0.01)
        )

        # Bollinger Band squeeze entry
        bb_squeeze_entry = (
            current_price >= self.bb_upper[-1] * 0.98 and
            self.rsi[-1] > 60  # Overbought in downtrend
        )

        # RSI and MACD conditions
        momentum_ready = (
            self.rsi[-1] > self.rsi_pullback_short and
            (self.macd[-1] < self.macd_signal[-1] or
             self.macd_histogram[-1] < self.macd_histogram[-2])  # MACD weakening
        )

        return (pullback_to_fast_ema or pullback_to_medium_ema or bb_squeeze_entry) and momentum_ready

    def check_breakout_entry(self):
        """Alternative entry on strong breakouts when ADX is very high"""

        if self.adx[-1] < self.adx_very_strong:
            return None

        current_price = self.data.Close[-1]

        # Bullish breakout
        if (self.plus_di[-1] > self.minus_di[-1] and
            current_price > self.ema_fast[-1] * (1 + self.breakout_tolerance) and
            self.rsi[-1] > 60 and self.rsi[-1] < 80 and
            self.macd[-1] > self.macd_signal[-1] and
            self.macd_histogram[-1] > self.macd_histogram[-2]):
            return 'long'

        # Bearish breakout
        if (self.minus_di[-1] > self.plus_di[-1] and
            current_price < self.ema_fast[-1] * (1 - self.breakout_tolerance) and
            self.rsi[-1] < 40 and self.rsi[-1] > 20 and
            self.macd[-1] < self.macd_signal[-1] and
            self.macd_histogram[-1] < self.macd_histogram[-2]):
            return 'short'

        return None

    def next(self):
        """Execute optimized trading logic"""

        # Skip if not enough data
        if len(self.data) < self.ema_slow:
            return

        current_price = self.data.Close[-1]
        current_bar = len(self.data) - 1

        # Position management
        if self.position:
            bars_held = current_bar - self.entry_bar

            # Exit conditions
            # 1. Trend weakening
            if self.adx[-1] < self.adx_exit_threshold:
                self.position.close()
                self.reset_position_tracking()
                return

            # 2. Time-based exit
            if bars_held >= self.max_holding_bars:
                self.position.close()
                self.reset_position_tracking()
                return

            # 3. Trend reversal signals
            if self.position.size > 0:  # Long position
                if self.minus_di[-1] > self.plus_di[-1] * 1.2:  # Strong reversal
                    self.position.close()
                    self.reset_position_tracking()
                    return
            else:  # Short position
                if self.plus_di[-1] > self.minus_di[-1] * 1.2:  # Strong reversal
                    self.position.close()
                    self.reset_position_tracking()
                    return

            # Position-specific management
            if self.position.size > 0:  # Long position

                # Scale in if trend strengthens
                if not self.position_scaled and bars_held > 1:
                    if (self.adx[-1] > self.adx[-2] and
                        current_price > self.entry_price * 1.005):
                        add_size = self.position.size * (self.add_on_pct / self.scale_entry_pct)
                        self.buy(size=add_size)
                        self.position_scaled = True

                # First profit target
                if not self.first_target_hit and current_price >= self.take_profit_1:
                    self.position.close(0.5)
                    self.first_target_hit = True
                    self.stop_loss = self.entry_price  # Move to breakeven

                # Trailing stop
                if self.first_target_hit:
                    trailing_stop = current_price * (1 - self.trailing_stop_pct)
                    self.stop_loss = max(self.stop_loss, trailing_stop)

                # Stop loss check
                if current_price <= self.stop_loss:
                    self.position.close()
                    self.reset_position_tracking()
                    return

            else:  # Short position

                # Scale in if trend strengthens
                if not self.position_scaled and bars_held > 1:
                    if (self.adx[-1] > self.adx[-2] and
                        current_price < self.entry_price * 0.995):
                        add_size = abs(self.position.size) * (self.add_on_pct / self.scale_entry_pct)
                        self.sell(size=add_size)
                        self.position_scaled = True

                # First profit target
                if not self.first_target_hit and current_price <= self.take_profit_1:
                    self.position.close(0.5)
                    self.first_target_hit = True
                    self.stop_loss = self.entry_price  # Move to breakeven

                # Trailing stop
                if self.first_target_hit:
                    trailing_stop = current_price * (1 + self.trailing_stop_pct)
                    self.stop_loss = min(self.stop_loss, trailing_stop)

                # Stop loss check
                if current_price >= self.stop_loss:
                    self.position.close()
                    self.reset_position_tracking()
                    return

        # Entry logic for new positions
        else:
            # Primary: Pullback entries
            if self.check_pullback_buy():
                position_size = self.calculate_position_size() * self.scale_entry_pct
                self.buy(size=position_size)

                # Set position parameters
                self.entry_bar = current_bar
                self.entry_price = current_price
                self.stop_loss = current_price * (1 - self.initial_stop_pct)
                risk_amount = self.entry_price - self.stop_loss
                self.take_profit_1 = self.entry_price + (risk_amount * self.first_target_rr)
                self.take_profit_2 = self.entry_price + (risk_amount * self.second_target_rr)
                self.position_scaled = False
                self.first_target_hit = False
                self.trend_type = 'bullish'

            elif self.check_pullback_sell():
                position_size = self.calculate_position_size() * self.scale_entry_pct
                self.sell(size=position_size)

                # Set position parameters
                self.entry_bar = current_bar
                self.entry_price = current_price
                self.stop_loss = current_price * (1 + self.initial_stop_pct)
                risk_amount = self.stop_loss - self.entry_price
                self.take_profit_1 = self.entry_price - (risk_amount * self.first_target_rr)
                self.take_profit_2 = self.entry_price - (risk_amount * self.second_target_rr)
                self.position_scaled = False
                self.first_target_hit = False
                self.trend_type = 'bearish'

            # Secondary: Breakout entries for very strong trends
            else:
                breakout_signal = self.check_breakout_entry()
                if breakout_signal == 'long':
                    position_size = self.calculate_position_size() * 0.5  # Smaller size for breakouts
                    self.buy(size=position_size)

                    self.entry_bar = current_bar
                    self.entry_price = current_price
                    self.stop_loss = self.ema_fast[-1] * (1 - self.initial_stop_pct)
                    risk_amount = self.entry_price - self.stop_loss
                    self.take_profit_1 = self.entry_price + (risk_amount * self.first_target_rr)
                    self.position_scaled = False
                    self.first_target_hit = False
                    self.trend_type = 'bullish_breakout'

                elif breakout_signal == 'short':
                    position_size = self.calculate_position_size() * 0.5
                    self.sell(size=position_size)

                    self.entry_bar = current_bar
                    self.entry_price = current_price
                    self.stop_loss = self.ema_fast[-1] * (1 + self.initial_stop_pct)
                    risk_amount = self.stop_loss - self.entry_price
                    self.take_profit_1 = self.entry_price - (risk_amount * self.first_target_rr)
                    self.position_scaled = False
                    self.first_target_hit = False
                    self.trend_type = 'bearish_breakout'

    def reset_position_tracking(self):
        """Reset all position tracking variables"""
        self.entry_bar = 0
        self.entry_price = 0
        self.stop_loss = 0
        self.take_profit_1 = 0
        self.take_profit_2 = 0
        self.position_scaled = False
        self.first_target_hit = False
        self.trend_type = None