"""
🚀 ClucMay72018 Momentum-Reversal Strategy
===========================================
Ultra-selective momentum reversal strategy for undervalued conditions
Designed for 5-minute timeframe with extreme oversold + volume anomaly detection

Strategy Components:
- RSI(5) + EMA of RSI for momentum
- MACD for trend convergence/divergence
- ADX for trend strength
- EMA(100) for trend filter
- Bollinger Bands(20) for volatility extremes
- Ultra-low volume anomaly detection

Entry: ALL conditions must be met
- Close < EMA(100) (bearish trend)
- Price < 98.5% of lower Bollinger Band (extreme oversold)
- Volume anomaly (significantly low vs 30-period average)
- Additional confirmations from RSI, MACD, ADX

Exit:
- Take Profit: Price crosses above BB midline OR 1% ROI
- Stop Loss: 5% from entry

Created: September 2025
Author: Bobby 🌙💫🚀
"""

import numpy as np
import pandas as pd
import talib
from backtesting import Strategy
from backtesting.lib import crossover
from typing import Optional


class ClucMay72018Strategy(Strategy):
    """
    Ultra-selective momentum-reversal strategy for 5-minute timeframe
    Targets extreme oversold conditions with volume anomalies
    """

    # Strategy parameters
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

    # Volume parameters
    volume_lookback = 30     # Periods for volume comparison
    volume_threshold = 0.05  # Volume must be < 5% of recent average (20x lower)

    # Risk management
    stop_loss_pct = 0.05     # 5% stop loss
    take_profit_pct = 0.01   # 1% take profit
    bb_entry_threshold = 0.985  # Enter when price < 98.5% of lower BB

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
        # Calculate rolling mean volume for anomaly detection
        self.volume_sma = self.I(talib.SMA, volume, self.volume_lookback)

        # Track entry price for stop loss and take profit
        self.entry_price = None

    def next(self):
        """Execute trading logic"""

        # Skip if we don't have enough data
        if len(self.data) < max(self.ema_period, self.volume_lookback):
            return

        # Get current values
        current_price = self.data.Close[-1]
        current_volume = self.data.Volume[-1]
        current_rsi = self.rsi[-1]
        current_rsi_ema = self.rsi_ema[-1]
        current_macd = self.macd[-1]
        current_macd_signal = self.macd_signal_line[-1]
        current_adx = self.adx[-1]
        current_ema = self.ema[-1]
        current_bb_lower = self.bb_lower[-1]
        current_bb_middle = self.bb_middle[-1]
        current_volume_avg = self.volume_sma[-1]

        # Skip if any indicators are NaN
        if any(pd.isna(x) for x in [current_rsi, current_adx, current_ema,
                                      current_bb_lower, current_volume_avg]):
            return

        # 🎯 Entry Logic - Ultra-selective conditions
        if not self.position:

            # Check ALL entry conditions
            # 1. Trend filter: Price below EMA(100)
            below_ema = current_price < current_ema

            # 2. Extreme oversold: Price < 98.5% of lower BB
            extreme_oversold = current_price < (current_bb_lower * self.bb_entry_threshold)

            # 3. Volume anomaly: Current volume significantly low
            volume_anomaly = False
            if current_volume_avg > 0:
                volume_ratio = current_volume / current_volume_avg
                volume_anomaly = volume_ratio < self.volume_threshold

            # 4. Additional confirmations (optional but helpful)
            rsi_oversold = current_rsi < 30  # RSI oversold
            macd_bearish = current_macd < current_macd_signal  # MACD bearish

            # Enter long position if ALL primary conditions met
            if below_ema and extreme_oversold and volume_anomaly:
                # Calculate position size (use most of available cash)
                size = 0.95

                # Enter long position
                self.buy(size=size)
                self.entry_price = current_price

                # Log entry conditions for analysis
                print(f"🚀 ENTRY at {current_price:.2f}")
                print(f"  RSI: {current_rsi:.2f}, ADX: {current_adx:.2f}")
                print(f"  Volume Ratio: {volume_ratio:.4f}")
                print(f"  Below EMA: {below_ema}, Extreme OS: {extreme_oversold}")

        # 🚪 Exit Logic
        elif self.position:

            if self.entry_price is None:
                self.entry_price = self.position.open_price

            # Calculate current P&L
            pnl_pct = (current_price - self.entry_price) / self.entry_price

            # Exit conditions
            # 1. Stop Loss: 5% loss
            stop_loss_hit = pnl_pct <= -self.stop_loss_pct

            # 2. Take Profit: 1% gain
            take_profit_hit = pnl_pct >= self.take_profit_pct

            # 3. BB Midline Cross: Mean reversion complete
            bb_midline_cross = current_price >= current_bb_middle

            # Exit if any condition met
            if stop_loss_hit or take_profit_hit or bb_midline_cross:
                exit_reason = "Stop Loss" if stop_loss_hit else \
                             "Take Profit" if take_profit_hit else \
                             "BB Midline Cross"

                print(f"💫 EXIT at {current_price:.2f} - {exit_reason}")
                print(f"  P&L: {pnl_pct:.2%}")

                self.position.close()
                self.entry_price = None


def test_strategy(data, cash=10000, commission=0.002):
    """
    Test the ClucMay72018 strategy on provided data

    Args:
        data: DataFrame with OHLCV data
        cash: Starting capital
        commission: Trading commission rate

    Returns:
        Backtest results
    """
    from backtesting import Backtest

    bt = Backtest(data, ClucMay72018Strategy,
                  cash=cash,
                  commission=commission,
                  exclusive_orders=True)

    stats = bt.run()
    return stats, bt


if __name__ == "__main__":
    print("🌙 ClucMay72018 Momentum-Reversal Strategy")
    print("=" * 60)
    print("Ultra-selective strategy for extreme oversold conditions")
    print("Requires: Price < EMA(100), < 98.5% Lower BB, Volume Anomaly")
    print("Risk: 5% Stop Loss, 1% Take Profit or BB Midline Cross")
    print("=" * 60)