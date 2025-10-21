"""
🚀 ClucMay72018 Optimized Strategy
===================================
Balanced version with optimal parameters for meaningful trade generation
Combines selectivity with practicality for better performance

Key Optimizations:
- Balanced BB threshold: 100-101% (touching or slightly below)
- Moderate volume filter: 30-40% of average
- RSI confirmation required for extra selectivity
- Better position sizing: 50% instead of 95%
- Additional filters to reduce overtrading

Created: September 2025
Author: Bobby 🌙💫🚀
"""

import numpy as np
import pandas as pd
import talib
from backtesting import Strategy
from backtesting.lib import crossover
from typing import Optional


class ClucMay72018OptimizedStrategy(Strategy):
    """
    Optimized momentum-reversal strategy balancing selectivity with trade generation
    Sweet spot between ultra-strict and too-flexible parameters
    """

    # Strategy parameters - OPTIMIZED
    rsi_period = 5           # Short-term RSI for momentum
    rsi_ema_period = 5       # EMA smoothing for RSI
    ema_period = 100         # Long-term trend filter
    bb_period = 20           # Bollinger Bands period
    bb_std = 2               # Bollinger Bands standard deviation
    adx_period = 14          # ADX period for trend strength

    # MACD parameters
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9

    # 🎯 OPTIMIZED PARAMETERS
    volume_lookback = 30         # Periods for volume comparison
    volume_threshold = 0.35      # Volume must be < 35% of average (balanced)
    bb_entry_threshold = 1.01    # Enter when price < 101% of lower BB (touching/below)

    # Additional selectivity parameters
    rsi_oversold_level = 30      # RSI must be below this for entry
    require_rsi_oversold = True  # Require RSI oversold as additional filter
    min_bars_between_entries = 5 # Minimum bars between consecutive entries
    adx_max_threshold = 35       # Skip if ADX too high (strong trend)

    # MACD filter
    use_macd_bearish = True      # Require MACD to be bearish

    # Risk management - MORE CONSERVATIVE
    stop_loss_pct = 0.03         # 3% stop loss (tighter)
    take_profit_pct = 0.015      # 1.5% take profit (more achievable)
    position_size_pct = 0.50     # Use 50% of capital (not 95%)

    def init(self):
        """Initialize all technical indicators"""

        # 🌟 Price and volume data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        volume = self.data.Volume

        # 📊 RSI and its EMA
        self.rsi = self.I(talib.RSI, close, self.rsi_period)
        self.rsi_ema = self.I(talib.EMA, self.rsi, self.rsi_ema_period)

        # 📈 MACD
        macd_result = talib.MACD(close,
                                  fastperiod=self.macd_fast,
                                  slowperiod=self.macd_slow,
                                  signalperiod=self.macd_signal)
        self.macd = self.I(lambda: macd_result[0])  # MACD line
        self.macd_signal_line = self.I(lambda: macd_result[1])  # Signal line
        self.macd_histogram = self.I(lambda: macd_result[2])  # Histogram

        # 💪 ADX for trend strength
        self.adx = self.I(talib.ADX, high, low, close, self.adx_period)

        # 📉 EMA for trend filter
        self.ema = self.I(talib.EMA, close, self.ema_period)

        # 🎯 Bollinger Bands
        bb_result = talib.BBANDS(close,
                                  timeperiod=self.bb_period,
                                  nbdevup=self.bb_std,
                                  nbdevdn=self.bb_std)
        self.bb_upper = self.I(lambda: bb_result[0])  # Upper band
        self.bb_middle = self.I(lambda: bb_result[1])  # Middle band (SMA)
        self.bb_lower = self.I(lambda: bb_result[2])  # Lower band

        # 📊 Volume analysis
        self.volume_sma = self.I(talib.SMA, volume, self.volume_lookback)

        # 🎯 Additional indicators for selectivity
        # Stochastic for extra oversold confirmation
        stoch_result = talib.STOCH(high, low, close,
                                    fastk_period=14,
                                    slowk_period=3,
                                    slowd_period=3)
        self.stoch_k = self.I(lambda: stoch_result[0])
        self.stoch_d = self.I(lambda: stoch_result[1])

        # Track entry management
        self.entry_price = None
        self.bars_since_entry = 0
        self.last_entry_bar = -self.min_bars_between_entries

    def next(self):
        """Execute optimized trading logic"""

        # Track bars since last entry
        self.bars_since_entry = len(self.data) - self.last_entry_bar

        # Skip if we don't have enough data
        if len(self.data) < max(self.ema_period, self.volume_lookback):
            return

        # Get current values
        current_price = self.data.Close[-1]
        current_volume = self.data.Volume[-1]
        current_rsi = self.rsi[-1]
        current_macd = self.macd[-1]
        current_macd_signal = self.macd_signal_line[-1]
        current_adx = self.adx[-1]
        current_ema = self.ema[-1]
        current_bb_lower = self.bb_lower[-1]
        current_bb_middle = self.bb_middle[-1]
        current_bb_upper = self.bb_upper[-1]
        current_volume_avg = self.volume_sma[-1]
        current_stoch_k = self.stoch_k[-1]
        current_stoch_d = self.stoch_d[-1]

        # Skip if any critical indicators are NaN
        if any(pd.isna(x) for x in [current_rsi, current_adx, current_ema,
                                      current_bb_lower, current_volume_avg]):
            return

        # 🎯 OPTIMIZED Entry Logic
        if not self.position:

            # Prevent rapid re-entry
            if self.bars_since_entry < self.min_bars_between_entries:
                return

            # PRIMARY CONDITIONS (all required)

            # 1. Trend filter: Price below EMA(100)
            below_ema = current_price < current_ema

            # 2. OPTIMIZED BB condition: Price touching or below lower BB
            bb_condition = current_price <= (current_bb_lower * self.bb_entry_threshold)

            # 3. OPTIMIZED Volume condition: Moderately low volume
            volume_condition = False
            if current_volume_avg > 0:
                volume_ratio = current_volume / current_volume_avg
                volume_condition = volume_ratio < self.volume_threshold

            # 4. RSI oversold confirmation (required for selectivity)
            rsi_oversold = current_rsi < self.rsi_oversold_level

            # 5. ADX not too high (avoid strong trends)
            adx_acceptable = current_adx < self.adx_max_threshold

            # 6. MACD bearish (if enabled)
            macd_bearish = True
            if self.use_macd_bearish:
                macd_bearish = current_macd < current_macd_signal

            # 7. Additional confirmation: Stochastic oversold
            stoch_oversold = current_stoch_k < 20 and current_stoch_d < 20

            # Calculate BB position for extra selectivity
            bb_width = current_bb_upper - current_bb_lower
            price_position_in_bb = (current_price - current_bb_lower) / bb_width if bb_width > 0 else 0

            # Enter only if ALL primary conditions are met
            if (below_ema and bb_condition and volume_condition and
                rsi_oversold and adx_acceptable and macd_bearish):

                # Extra confirmation: price is in bottom 10% of BB range
                if price_position_in_bb < 0.1:

                    # Calculate position size (more conservative)
                    size = self.position_size_pct

                    # Enter long position
                    self.buy(size=size)
                    self.entry_price = current_price
                    self.last_entry_bar = len(self.data)

        # 🚪 Exit Logic (keep original conservative exits)
        elif self.position:

            if self.entry_price is None:
                self.entry_price = self.position.open_price

            # Calculate current P&L
            pnl_pct = (current_price - self.entry_price) / self.entry_price

            # Exit conditions
            # 1. Stop Loss: 3% loss (tighter)
            stop_loss_hit = pnl_pct <= -self.stop_loss_pct

            # 2. Take Profit: 1.5% gain (more achievable)
            take_profit_hit = pnl_pct >= self.take_profit_pct

            # 3. BB Midline Cross: Mean reversion complete
            bb_midline_cross = current_price >= current_bb_middle

            # 4. RSI overbought: Exit on strength
            rsi_overbought = current_rsi > 70

            # Exit if any condition met
            if stop_loss_hit or take_profit_hit or bb_midline_cross or rsi_overbought:
                self.position.close()
                self.entry_price = None


def test_optimized_strategy(data, cash=10000, commission=0.002):
    """
    Test the optimized ClucMay72018 strategy

    Args:
        data: DataFrame with OHLCV data
        cash: Starting capital
        commission: Trading commission rate

    Returns:
        Backtest results
    """
    from backtesting import Backtest

    bt = Backtest(data, ClucMay72018OptimizedStrategy,
                  cash=cash,
                  commission=commission,
                  exclusive_orders=True)

    stats = bt.run()
    return stats, bt


if __name__ == "__main__":
    print("🌙 ClucMay72018 OPTIMIZED Strategy")
    print("=" * 60)
    print("Balanced parameters for meaningful trade generation")
    print("\nOptimizations:")
    print("- BB threshold: 101% (touching/below lower band)")
    print("- Volume: <35% of average")
    print("- RSI: Must be <30 for entry")
    print("- Position size: 50% (not 95%)")
    print("- Min 5 bars between entries")
    print("- Tighter stop loss: 3%")
    print("=" * 60)