"""
🚀 Crypto Momentum Surge Trading Strategy 🚀
=============================================
A comprehensive momentum-based trading strategy for cryptocurrency markets
that detects and trades price surges with strict risk management.

Strategy Logic:
- Detects rapid price surges using Rate of Change (ROC) indicator
- Confirms momentum with MACD crossovers and RSI climbing
- Validates with volume spikes and OBV confirmation
- Uses tight stop losses for crypto's volatile nature
- Multi-timeframe surge detection for higher accuracy

Author: Bobby's Algo Trading Systems 🌙
Date: 2025-01-18
Version: 1.0.0
"""

from backtesting import Strategy
from backtesting.lib import crossover
import talib
import numpy as np
import pandas as pd


class CryptoMomentumSurgeStrategy(Strategy):
    """
    🔥 Crypto Momentum Surge Strategy 🔥

    Identifies and trades cryptocurrency price surges using a combination of:
    - MACD for trend direction
    - RSI for momentum strength
    - Rate of Change for surge detection
    - Volume analysis for fake pump filtering
    - OBV for volume momentum confirmation
    """

    # Strategy Parameters - Optimizable
    # EMA Parameters
    ema_short_period = 5  # Short-term EMA for trend
    ema_long_period = 20  # Long-term EMA for trend filter

    # MACD Parameters
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9

    # RSI Parameters
    rsi_period = 14
    rsi_buy_threshold = 50  # RSI must be above this to enter
    rsi_sell_threshold = 70  # RSI declining from overbought

    # Rate of Change Parameters
    roc_period = 5  # Period for rate of change calculation
    roc_surge_threshold = 3.0  # % change to qualify as surge (3% in 5 periods)
    roc_exit_threshold = 0.5  # % change below this indicates momentum fading

    # Volume Parameters
    volume_ma_period = 20  # Period for volume moving average
    volume_spike_multiplier = 1.8  # Volume must be this * MA to confirm surge
    volume_fade_multiplier = 0.8  # Volume below this * MA indicates fade

    # OBV Parameters
    obv_ma_period = 10  # Period for OBV moving average

    # Risk Management Parameters
    stop_loss_pct = 0.02  # 2% stop loss (tight for crypto)
    take_profit_pct = 0.06  # 6% take profit (3:1 reward-risk)
    position_size_pct = 0.95  # Use 95% of available capital

    # Additional Filters
    min_volume_filter = 100  # Minimum volume to consider trading
    max_holding_bars = 100  # Maximum bars to hold position

    def init(self):
        """
        🎯 Initialize all technical indicators 🎯
        """
        # Price data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        volume = self.data.Volume

        # 📊 Exponential Moving Averages
        self.ema_short = self.I(talib.EMA, close, self.ema_short_period)
        self.ema_long = self.I(talib.EMA, close, self.ema_long_period)

        # 📈 MACD Indicator
        macd_line, macd_signal, macd_hist = talib.MACD(
            close,
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow,
            signalperiod=self.macd_signal
        )
        self.macd = self.I(lambda: macd_line)
        self.macd_signal_line = self.I(lambda: macd_signal)
        self.macd_histogram = self.I(lambda: macd_hist)

        # 💪 RSI (Relative Strength Index)
        self.rsi = self.I(talib.RSI, close, self.rsi_period)

        # 🚀 Rate of Change (ROC) - Surge Detection
        self.roc = self.I(talib.ROC, close, self.roc_period)

        # Multi-timeframe ROC for comprehensive surge detection
        self.roc_1 = self.I(talib.ROC, close, 1)  # 1-period ROC
        self.roc_3 = self.I(talib.ROC, close, 3)  # 3-period ROC
        self.roc_10 = self.I(talib.ROC, close, 10)  # 10-period ROC

        # 📊 Volume Analysis
        self.volume_ma = self.I(talib.SMA, volume, self.volume_ma_period)

        # Calculate volume ratio (current vs average)
        self.volume_ratio = self.I(lambda: volume / self.volume_ma)

        # 🌊 On-Balance Volume (OBV)
        obv = talib.OBV(close, volume)
        self.obv = self.I(lambda: obv)
        self.obv_ma = self.I(talib.SMA, obv, self.obv_ma_period)

        # 💎 ATR for Dynamic Stop Loss
        self.atr = self.I(talib.ATR, high, low, close, 14)

        # 📈 Additional Momentum Indicators
        self.momentum = self.I(talib.MOM, close, 10)  # 10-period momentum

        # 🎯 Track entry prices and bars since entry
        self.entry_price = 0
        self.bars_since_entry = 0

    def detect_surge(self):
        """
        🚀 Detect authentic price surges vs fake pumps 🚀

        Returns:
            bool: True if authentic surge detected
        """
        # Check if we have enough data
        if len(self.data.Close) < 20:
            return False

        # Primary surge detection using ROC
        primary_surge = self.roc[-1] > self.roc_surge_threshold

        # Multi-timeframe confirmation (short, medium, long-term momentum)
        short_momentum = self.roc_1[-1] > 0.5  # 0.5% in 1 period
        medium_momentum = self.roc_3[-1] > 1.5  # 1.5% in 3 periods
        long_momentum = self.roc_10[-1] > 5.0  # 5% in 10 periods

        # Volume confirmation - must have volume spike
        volume_confirms = self.volume_ratio[-1] > self.volume_spike_multiplier

        # OBV confirmation - must be rising
        obv_confirms = self.obv[-1] > self.obv[-2] if len(self.obv) > 1 else False

        # Momentum acceleration check
        momentum_accelerating = (
            self.momentum[-1] > self.momentum[-2] if len(self.momentum) > 1 else False
        )

        # Combine all surge criteria
        authentic_surge = (
            primary_surge and
            (short_momentum or medium_momentum) and
            volume_confirms and
            obv_confirms
        )

        return authentic_surge

    def detect_fake_pump(self):
        """
        🚨 Detect fake pumps to avoid trap trades 🚨

        Returns:
            bool: True if fake pump detected
        """
        # Check if we have enough data
        if len(self.data.Close) < 20:
            return False

        # Volume too low for authentic move
        volume_too_low = self.volume_ratio[-1] < 1.2

        # No follow-through in momentum
        if len(self.roc) >= 3:
            no_follow_through = self.roc[-1] < self.roc[-3]
        else:
            no_follow_through = False

        # RSI divergence (price up but RSI down)
        if len(self.rsi) >= 3 and len(self.data.Close) >= 3:
            price_rising = self.data.Close[-1] > self.data.Close[-3]
            rsi_falling = self.rsi[-1] < self.rsi[-3]
            rsi_divergence = price_rising and rsi_falling
        else:
            rsi_divergence = False

        # OBV divergence (price up but OBV down)
        if len(self.obv) >= 3 and len(self.data.Close) >= 3:
            price_rising = self.data.Close[-1] > self.data.Close[-3]
            obv_falling = self.obv[-1] < self.obv[-3]
            obv_divergence = price_rising and obv_falling
        else:
            obv_divergence = False

        # Combine fake pump indicators
        fake_pump = (
            volume_too_low or
            no_follow_through or
            rsi_divergence or
            obv_divergence
        )

        return fake_pump

    def calculate_dynamic_stop_loss(self):
        """
        💎 Calculate dynamic stop loss based on volatility 💎

        Returns:
            float: Dynamic stop loss percentage
        """
        if len(self.atr) < 1:
            return self.stop_loss_pct

        # Base stop loss
        base_stop = self.stop_loss_pct

        # Volatility adjustment (ATR-based)
        current_atr = self.atr[-1]
        price = self.data.Close[-1]

        # Calculate ATR as percentage of price
        atr_pct = (current_atr / price) if price > 0 else 0

        # Adjust stop loss based on volatility
        # Higher volatility = wider stop
        volatility_adjustment = min(atr_pct * 0.5, 0.03)  # Max 3% additional

        dynamic_stop = base_stop + volatility_adjustment

        return min(dynamic_stop, 0.05)  # Cap at 5% max stop loss

    def next(self):
        """
        🎯 Main trading logic executed on each bar 🎯
        """
        # Skip if not enough data
        if len(self.data.Close) < 30:
            return

        # Update bars since entry if in position
        if self.position:
            self.bars_since_entry += 1

        # Get current values
        current_price = self.data.Close[-1]
        current_volume = self.data.Volume[-1]

        # Skip if volume too low
        if current_volume < self.min_volume_filter:
            return

        # 📈 ENTRY LOGIC
        if not self.position:
            # Reset tracking variables
            self.bars_since_entry = 0

            # Primary entry conditions
            macd_bullish = crossover(self.macd, self.macd_signal_line)

            # RSI conditions
            rsi_climbing = (
                self.rsi[-1] > self.rsi[-2] and
                self.rsi[-1] > self.rsi_buy_threshold
            ) if len(self.rsi) > 1 else False

            # Surge detection
            surge_detected = self.detect_surge()

            # Fake pump filter
            fake_pump = self.detect_fake_pump()

            # EMA trend filter
            if len(self.ema_short) > 1 and len(self.ema_long) > 0:
                ema_bullish = (
                    self.ema_short[-1] > self.ema_short[-2] and
                    current_price > self.ema_short[-1] and
                    self.ema_short[-1] > self.ema_long[-1]
                )
            else:
                ema_bullish = False

            # Volume confirmation
            volume_confirms = self.volume_ratio[-1] > self.volume_spike_multiplier

            # OBV confirmation
            obv_rising = self.obv[-1] > self.obv_ma[-1] if len(self.obv_ma) > 0 else False

            # Combine all entry signals
            entry_signal = (
                (macd_bullish or surge_detected) and
                rsi_climbing and
                not fake_pump and
                ema_bullish and
                volume_confirms and
                obv_rising
            )

            # Execute buy if all conditions met
            if entry_signal:
                self.entry_price = current_price
                dynamic_stop = self.calculate_dynamic_stop_loss()
                stop_loss_price = current_price * (1 - dynamic_stop)
                take_profit_price = current_price * (1 + self.take_profit_pct)

                # Place buy order with stop loss and take profit
                self.buy(
                    size=self.position_size_pct,
                    sl=stop_loss_price,
                    tp=take_profit_price
                )

        # 📉 EXIT LOGIC (for positions without SL/TP hit)
        elif self.position:
            # MACD bearish crossover
            macd_bearish = crossover(self.macd_signal_line, self.macd)

            # RSI conditions for exit
            rsi_declining = (
                self.rsi[-1] < self.rsi[-2] and
                self.rsi[-1] < self.rsi_sell_threshold
            ) if len(self.rsi) > 1 else False

            # Momentum fading
            momentum_fading = self.roc[-1] < self.roc_exit_threshold

            # Volume drying up
            volume_fading = self.volume_ratio[-1] < self.volume_fade_multiplier

            # OBV declining
            obv_declining = self.obv[-1] < self.obv_ma[-1] if len(self.obv_ma) > 0 else False

            # Time-based exit (max holding period)
            held_too_long = self.bars_since_entry >= self.max_holding_bars

            # Combine exit signals
            exit_signal = (
                macd_bearish or
                rsi_declining or
                momentum_fading or
                volume_fading or
                obv_declining or
                held_too_long
            )

            # Execute sell if exit conditions met
            if exit_signal:
                self.position.close()
                self.entry_price = 0
                self.bars_since_entry = 0


class CryptoMomentumAdaptiveStrategy(CryptoMomentumSurgeStrategy):
    """
    🌟 Adaptive Crypto Momentum Strategy 🌟

    Enhanced version with adaptive parameters based on market conditions
    and multiple signal generation modes.
    """

    # Adaptive Parameters
    adapt_to_volatility = True
    volatility_lookback = 20

    # Signal Mode: 'aggressive', 'moderate', 'conservative'
    signal_mode = 'moderate'

    def init(self):
        """
        🎯 Initialize with additional adaptive indicators 🎯
        """
        super().init()

        # Historical Volatility for adaptation
        close = self.data.Close
        returns = pd.Series(close).pct_change()
        self.volatility = self.I(lambda: returns.rolling(self.volatility_lookback).std() * np.sqrt(252))

        # Market regime detection
        self.sma_50 = self.I(talib.SMA, close, 50)
        self.sma_200 = self.I(talib.SMA, close, 200)

    def get_adaptive_parameters(self):
        """
        🔄 Adjust parameters based on market conditions 🔄
        """
        if len(self.volatility) < 1:
            return

        current_vol = self.volatility[-1]

        # Adjust ROC threshold based on volatility
        if current_vol > 0.8:  # High volatility
            self.roc_surge_threshold = 4.0  # Require bigger moves
            self.volume_spike_multiplier = 2.0
            self.stop_loss_pct = 0.03  # Wider stop
        elif current_vol < 0.4:  # Low volatility
            self.roc_surge_threshold = 2.0  # Smaller moves acceptable
            self.volume_spike_multiplier = 1.5
            self.stop_loss_pct = 0.015  # Tighter stop

        # Adjust based on signal mode
        if self.signal_mode == 'aggressive':
            self.roc_surge_threshold *= 0.7
            self.rsi_buy_threshold = 45
            self.volume_spike_multiplier *= 0.8
        elif self.signal_mode == 'conservative':
            self.roc_surge_threshold *= 1.3
            self.rsi_buy_threshold = 55
            self.volume_spike_multiplier *= 1.2

    def next(self):
        """
        🎯 Enhanced trading logic with adaptation 🎯
        """
        # Adapt parameters if enabled
        if self.adapt_to_volatility:
            self.get_adaptive_parameters()

        # Execute parent strategy logic
        super().next()


# 🌙💫🚀 Bobby's signature emoji style preserved throughout 🌙💫🚀