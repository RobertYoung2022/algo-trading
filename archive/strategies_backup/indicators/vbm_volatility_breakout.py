"""
🚀 VBM (Volatility Breakout Momentum) Strategy 🚀
================================================
Captures explosive volatility expansion breakouts with volume confirmation
Targets high-volatility cryptos: HBAR, LINK, XRP, CRO

Author: Bobby 🌙💫
Date: January 2025
Framework: backtesting.py

Strategy Philosophy:
- Enter on Bollinger Band breakouts with ATR expansion
- Confirm with volume surge and ADX trend strength
- Dynamic position sizing based on volatility
- Complementary to TEMS for portfolio diversification
"""

from backtesting import Strategy
import talib as ta
import numpy as np
from datetime import datetime


class VBMVolatilityBreakout(Strategy):
    """
    🔥 Volatility Breakout Momentum Strategy 🔥

    Parameters:
    -----------
    bb_period : int (20)
        Bollinger Bands period
    bb_std : float (2.5)
        Bollinger Bands standard deviation multiplier
    atr_period : int (14)
        ATR period for volatility measurement
    atr_ma_period : int (50)
        Moving average period for ATR baseline
    volatility_expansion_mult : float (1.5)
        ATR expansion multiplier for breakout detection
    volume_ma_period : int (20)
        Volume moving average period
    volume_surge_mult : float (2.0)
        Volume surge multiplier for confirmation
    adx_period : int (14)
        ADX period for trend strength
    adx_threshold : int (25)
        Minimum ADX for trend confirmation
    stop_loss_atr_mult : float (2.0)
        Stop loss distance in ATR multiples
    take_profit_atr_mult : float (4.0)
        First take profit target in ATR multiples
    trailing_stop_atr_mult : float (2.5)
        Trailing stop distance in ATR multiples
    time_stop_bars : int (10)
        Exit if no profit after N bars
    base_risk_pct : float (0.02)
        Base position risk percentage
    max_position_pct : float (0.04)
        Maximum position size percentage
    scale_in_pct : float (0.5)
        Additional position on first profitable close
    """

    # Strategy Parameters (Optimizable)
    bb_period = 20
    bb_std = 2.5
    atr_period = 14
    atr_ma_period = 50
    volatility_expansion_mult = 1.5
    volume_ma_period = 20
    volume_surge_mult = 2.0
    adx_period = 14
    adx_threshold = 25
    stop_loss_atr_mult = 2.0
    take_profit_atr_mult = 4.0
    trailing_stop_atr_mult = 2.5
    time_stop_bars = 10
    base_risk_pct = 0.02
    max_position_pct = 0.04
    scale_in_pct = 0.5

    def init(self):
        """Initialize strategy indicators and signals"""

        # 📊 Price Data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        volume = self.data.Volume if hasattr(self.data, 'Volume') else None

        # 🎯 Bollinger Bands for breakout detection
        bb_data = ta.BBANDS(close,
                            timeperiod=self.bb_period,
                            nbdevup=self.bb_std,
                            nbdevdn=self.bb_std)
        self.bb_upper = self.I(lambda: bb_data[0])
        self.bb_middle = self.I(lambda: bb_data[1])
        self.bb_lower = self.I(lambda: bb_data[2])

        # 💨 ATR for volatility measurement
        self.atr = self.I(ta.ATR, high, low, close, self.atr_period)
        self.atr_ma = self.I(ta.SMA, self.atr, self.atr_ma_period)

        # 📈 Volume analysis (if available)
        if volume is not None:
            self.volume_ma = self.I(ta.SMA, volume, self.volume_ma_period)
            self.has_volume = True
        else:
            self.has_volume = False

        # 💪 ADX for trend strength
        self.adx = self.I(ta.ADX, high, low, close, self.adx_period)
        self.plus_di = self.I(ta.PLUS_DI, high, low, close, self.adx_period)
        self.minus_di = self.I(ta.MINUS_DI, high, low, close, self.adx_period)

        # 🎯 Initialize tracking variables
        self.entry_bar = None
        self.entry_price = 0
        self.highest_price = 0
        self.lowest_price = float('inf')
        self.scaled_in = False
        self.initial_position_size = 0

    def next(self):
        """Execute trading logic on each bar"""

        # Skip if indicators not ready
        if len(self.data) < max(self.bb_period, self.atr_ma_period, self.adx_period):
            return

        # Get current values
        close = self.data.Close[-1]
        high = self.data.High[-1]
        low = self.data.Low[-1]

        # 🔥 Detect volatility expansion
        current_atr = self.atr[-1]
        atr_baseline = self.atr_ma[-1]
        volatility_expansion = current_atr > atr_baseline * self.volatility_expansion_mult

        # 📊 Check volume surge (if volume data available)
        volume_surge = True  # Default to True if no volume data
        if self.has_volume:
            current_volume = self.data.Volume[-1]
            volume_baseline = self.volume_ma[-1]
            volume_surge = current_volume > volume_baseline * self.volume_surge_mult

        # 💪 Check trend strength
        adx_strong = self.adx[-1] > self.adx_threshold
        bullish_momentum = self.plus_di[-1] > self.minus_di[-1]
        bearish_momentum = self.minus_di[-1] > self.plus_di[-1]

        # 🎯 Entry Conditions
        long_breakout = (
            close > self.bb_upper[-1] and           # Bollinger Band breakout
            volatility_expansion and                # ATR expansion confirmed
            volume_surge and                        # Volume confirmation
            adx_strong and                          # Strong trend present
            bullish_momentum                        # Bullish direction
        )

        short_breakout = (
            close < self.bb_lower[-1] and           # Bollinger Band breakdown
            volatility_expansion and                # ATR expansion confirmed
            volume_surge and                        # Volume confirmation
            adx_strong and                          # Strong trend present
            bearish_momentum                        # Bearish direction
        )

        # 💼 Position Management
        if not self.position:
            # Calculate dynamic position size based on volatility
            volatility_factor = current_atr / close if close > 0 else 1
            adjusted_size = self.base_risk_pct * (0.02 / volatility_factor) if volatility_factor > 0 else self.base_risk_pct
            position_size_pct = min(adjusted_size, self.max_position_pct)

            # Ensure minimum position size
            position_size_pct = max(position_size_pct, 0.01)  # Minimum 1% position

            # 🚀 Enter Long Position
            if long_breakout:
                # Use simple percentage sizing for backtesting.py
                self.buy(size=position_size_pct)
                self.entry_bar = len(self.data)
                self.entry_price = close
                self.highest_price = close
                self.scaled_in = False
                self.initial_position_size = position_size_pct

            # 📉 Enter Short Position
            elif short_breakout:
                # Use simple percentage sizing for backtesting.py
                self.sell(size=position_size_pct)
                self.entry_bar = len(self.data)
                self.entry_price = close
                self.lowest_price = close
                self.scaled_in = False
                self.initial_position_size = position_size_pct

        else:
            # 📊 Manage existing position
            bars_since_entry = len(self.data) - self.entry_bar if self.entry_bar else 0

            if self.position.is_long:
                # Update highest price
                self.highest_price = max(self.highest_price, high)

                # 🎯 Scale in on first profitable close (if not already done)
                if not self.scaled_in and close > self.entry_price:
                    scale_in_size = self.initial_position_size * self.scale_in_pct
                    # Ensure scale-in size is at least 0.01 (1%)
                    scale_in_size = max(scale_in_size, 0.01)
                    self.buy(size=scale_in_size)
                    self.scaled_in = True

                # 🛑 Exit Conditions for Long
                stop_loss = self.entry_price - (current_atr * self.stop_loss_atr_mult)
                take_profit = self.entry_price + (current_atr * self.take_profit_atr_mult)
                trailing_stop = self.highest_price - (current_atr * self.trailing_stop_atr_mult)

                # Time stop - exit if no profit after N bars
                time_stop = bars_since_entry >= self.time_stop_bars and close <= self.entry_price

                # Trend reversal stop
                trend_weak = self.adx[-1] < 20

                # Exit logic
                if (close <= stop_loss or
                    close >= take_profit or
                    (close > self.entry_price and close <= trailing_stop) or
                    time_stop or
                    trend_weak):

                    # Take partial profits at first target
                    if close >= take_profit and not self.scaled_in:
                        # Exit 50% at target
                        self.position.close(0.5)
                    else:
                        # Full exit on stops
                        self.position.close()

                    self.entry_bar = None
                    self.entry_price = 0
                    self.highest_price = 0
                    self.scaled_in = False
                    self.initial_position_size = 0

            elif self.position.is_short:
                # Update lowest price
                self.lowest_price = min(self.lowest_price, low)

                # 🎯 Scale in on first profitable close (if not already done)
                if not self.scaled_in and close < self.entry_price:
                    scale_in_size = self.initial_position_size * self.scale_in_pct
                    # Ensure scale-in size is at least 0.01 (1%)
                    scale_in_size = max(scale_in_size, 0.01)
                    self.sell(size=scale_in_size)
                    self.scaled_in = True

                # 🛑 Exit Conditions for Short
                stop_loss = self.entry_price + (current_atr * self.stop_loss_atr_mult)
                take_profit = self.entry_price - (current_atr * self.take_profit_atr_mult)
                trailing_stop = self.lowest_price + (current_atr * self.trailing_stop_atr_mult)

                # Time stop - exit if no profit after N bars
                time_stop = bars_since_entry >= self.time_stop_bars and close >= self.entry_price

                # Trend reversal stop
                trend_weak = self.adx[-1] < 20

                # Exit logic
                if (close >= stop_loss or
                    close <= take_profit or
                    (close < self.entry_price and close >= trailing_stop) or
                    time_stop or
                    trend_weak):

                    # Take partial profits at first target
                    if close <= take_profit and not self.scaled_in:
                        # Exit 50% at target
                        self.position.close(0.5)
                    else:
                        # Full exit on stops
                        self.position.close()

                    self.entry_bar = None
                    self.entry_price = 0
                    self.lowest_price = float('inf')
                    self.scaled_in = False
                    self.initial_position_size = 0


# 🎯 Strategy Configuration Presets
VBM_CONSERVATIVE = {
    'bb_std': 3.0,                      # Wider bands for fewer signals
    'volatility_expansion_mult': 2.0,    # Higher threshold
    'volume_surge_mult': 2.5,            # Stronger volume requirement
    'base_risk_pct': 0.01,              # Lower risk
    'max_position_pct': 0.02,           # Smaller max position
}

VBM_AGGRESSIVE = {
    'bb_std': 2.0,                      # Tighter bands for more signals
    'volatility_expansion_mult': 1.2,    # Lower threshold
    'volume_surge_mult': 1.5,            # Easier volume requirement
    'base_risk_pct': 0.03,              # Higher risk
    'max_position_pct': 0.06,           # Larger max position
}

VBM_BALANCED = {
    'bb_std': 2.5,
    'volatility_expansion_mult': 1.5,
    'volume_surge_mult': 2.0,
    'base_risk_pct': 0.02,
    'max_position_pct': 0.04,
}