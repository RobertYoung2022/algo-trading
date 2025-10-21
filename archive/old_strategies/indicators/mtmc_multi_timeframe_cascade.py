"""
Multi-Timeframe Momentum Cascade (MTMC) Strategy
=================================================
The pinnacle of trend-following sophistication combining multiple timeframes
for confluence-based entries with maximum accuracy and reduced false signals.

Strategy Architecture:
- HTF (1d): Macro trend direction filter
- MTF (4h): Momentum swing confirmation
- LTF (1h): Precise entry timing execution
- Confluence scoring for dynamic position sizing
- Adaptive stop loss based on timeframe alignment

Performance Targets:
- Win rate: 55-65% (multi-timeframe confluence premium)
- Average R:R: 1.8:1 (balanced risk-reward)
- Sharpe ratio: 0.7-1.3
- Max drawdown: 15-20%

Created: 2025-01-17
Author: Bobby's Algo-Trading System 🌙💫🚀
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib as ta
from typing import Tuple, Optional
import warnings
warnings.filterwarnings('ignore')


class MTMCStrategy(Strategy):
    """
    Multi-Timeframe Momentum Cascade Strategy

    Combines daily trend, 4h momentum, and 1h timing for high-probability entries.
    Uses confluence scoring to dynamically adjust position sizing based on signal strength.
    """

    # Strategy Parameters
    # HTF (Daily) Parameters
    htf_ema_fast = 21        # Fast EMA for daily trend
    htf_ema_slow = 55        # Slow EMA for daily trend

    # MTF (4H) Parameters
    mtf_macd_fast = 12       # MACD fast period
    mtf_macd_slow = 26       # MACD slow period
    mtf_macd_signal = 9      # MACD signal period
    mtf_rsi_period = 14      # RSI period for momentum
    mtf_rsi_threshold = 50   # RSI neutral level

    # LTF (1H) Parameters
    ltf_ema_period = 8       # Fast EMA for entry timing
    ltf_volume_ma = 20       # Volume MA period

    # Risk Management
    atr_period = 14          # ATR period for stops
    atr_multiplier_1h = 1.5  # 1h timeframe stop multiplier
    atr_multiplier_4h = 2.0  # 4h timeframe stop multiplier
    atr_multiplier_1d = 3.0  # Daily timeframe stop multiplier

    # Position Sizing
    max_risk_per_trade = 0.03  # 3% max risk (full confluence)
    med_risk_per_trade = 0.02  # 2% medium risk (partial confluence)
    min_confluence_score = 0.5  # Minimum score to enter

    # Exit Management
    cascade_exit_pct = 0.5   # Exit 50% on first signal
    time_decay_periods = 48  # Exit if no progress after 48 periods
    profit_scale_threshold = 1.5  # Add to position at 1.5R profit

    def init(self):
        """Initialize multi-timeframe indicators and alignment logic"""

        # Get price and volume data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        volume = self.data.Volume

        # 🌅 HTF (Daily) Trend Analysis - Simulated with longer periods
        # Since we can't resample in backtesting.py, use longer period indicators
        self.htf_ema_fast_line = self.I(ta.EMA, close, self.htf_ema_fast * 24)  # Approximate daily
        self.htf_ema_slow_line = self.I(ta.EMA, close, self.htf_ema_slow * 24)  # Approximate daily

        # 📊 MTF (4H) Momentum Analysis - Medium period indicators
        # Approximate 4h with 4x multiplier
        macd_line, signal_line, hist = ta.MACD(close,
                                               self.mtf_macd_fast * 4,
                                               self.mtf_macd_slow * 4,
                                               self.mtf_macd_signal * 4)
        self.mtf_macd = self.I(lambda: macd_line)
        self.mtf_signal = self.I(lambda: signal_line)
        self.mtf_histogram = self.I(lambda: hist)
        self.mtf_rsi = self.I(ta.RSI, close, self.mtf_rsi_period * 4)

        # ⚡ LTF (1H) Entry Timing - Standard indicators
        self.ltf_ema = self.I(ta.EMA, close, self.ltf_ema_period)

        # Calculate VWAP manually
        typical_price = (high + low + close) / 3
        cumulative_tpv = (typical_price * volume).cumsum()
        cumulative_volume = volume.cumsum()
        vwap = cumulative_tpv / cumulative_volume
        self.ltf_vwap = self.I(lambda: vwap)

        # Volume analysis - handle potential NaN/zero values
        volume_sma = ta.SMA(volume, self.ltf_volume_ma)
        # Replace NaN and zeros with 1 to avoid division errors
        volume_sma = np.where(np.isnan(volume_sma) | (volume_sma == 0), 1, volume_sma)
        self.ltf_volume_avg = self.I(lambda: volume_sma)
        self.volume_surge = self.I(lambda: volume / volume_sma)

        # 🛡️ Risk Management Indicators
        self.atr = self.I(ta.ATR, high, low, close, self.atr_period)

        # Bollinger Bands for volatility context
        bb_upper, bb_middle, bb_lower = ta.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
        self.bb_upper = self.I(lambda: bb_upper)
        self.bb_lower = self.I(lambda: bb_lower)
        self.bb_width = self.I(lambda: (bb_upper - bb_lower) / bb_middle)

        # 📈 Trend strength indicators
        self.adx = self.I(ta.ADX, high, low, close, timeperiod=14)

        # Track entry prices and holding periods
        self.entry_price = 0
        self.holding_periods = 0
        self.peak_profit = 0
        self.confluence_score = 0

    def calculate_confluence_score(self) -> float:
        """
        Calculate confluence score based on timeframe alignment

        Returns:
            float: Score between 0 and 1 indicating signal strength
        """
        score = 0.0

        # HTF Trend (50% weight)
        htf_bullish = self.htf_ema_fast_line[-1] > self.htf_ema_slow_line[-1]
        htf_trend_strength = abs(self.htf_ema_fast_line[-1] - self.htf_ema_slow_line[-1]) / self.data.Close[-1]
        if htf_bullish:
            score += 0.5 * min(htf_trend_strength * 100, 1.0)  # Cap at 1.0

        # MTF Momentum (30% weight)
        mtf_bullish = (self.mtf_macd[-1] > self.mtf_signal[-1] and
                      self.mtf_rsi[-1] > self.mtf_rsi_threshold)
        if mtf_bullish:
            # Stronger weight if RSI is in sweet spot (55-70)
            rsi_strength = 1.0 if 55 <= self.mtf_rsi[-1] <= 70 else 0.7
            score += 0.3 * rsi_strength

        # LTF Timing (20% weight)
        ltf_ready = (self.data.Close[-1] > self.ltf_ema[-1] and
                    self.data.Close[-1] > self.ltf_vwap[-1] and
                    self.volume_surge[-1] > 1.0)
        if ltf_ready:
            # Stronger weight if volume surge is significant
            volume_strength = min(self.volume_surge[-1] / 1.5, 1.0)
            score += 0.2 * volume_strength

        return score

    def calculate_position_size(self, confluence_score: float) -> float:
        """
        Dynamic position sizing based on confluence strength

        Args:
            confluence_score: Signal strength from 0 to 1

        Returns:
            float: Position size as percentage of equity
        """
        if confluence_score >= 0.8:  # Very strong confluence
            return self.max_risk_per_trade
        elif confluence_score >= 0.65:  # Moderate confluence
            return self.med_risk_per_trade
        elif confluence_score >= self.min_confluence_score:  # Minimum viable
            return self.med_risk_per_trade * 0.5
        else:
            return 0.0

    def calculate_dynamic_stop(self) -> float:
        """
        Calculate adaptive stop loss based on timeframe alignment

        Returns:
            float: Stop loss distance in price units
        """
        base_atr = self.atr[-1]

        # Adjust multiplier based on confluence strength
        if self.confluence_score >= 0.8:
            # Strong confluence - tighter stop
            multiplier = self.atr_multiplier_1h
        elif self.confluence_score >= 0.65:
            # Medium confluence - medium stop
            multiplier = self.atr_multiplier_4h
        else:
            # Weak confluence - wider stop
            multiplier = self.atr_multiplier_1d

        # Additional adjustment for volatility regime
        if self.bb_width[-1] > 0.05:  # High volatility
            multiplier *= 1.2
        elif self.bb_width[-1] < 0.02:  # Low volatility
            multiplier *= 0.8

        return base_atr * multiplier

    def check_exit_conditions(self) -> Tuple[bool, float]:
        """
        Check for cascade exit conditions

        Returns:
            Tuple[bool, float]: (should_exit, exit_percentage)
        """
        if not self.position:
            return False, 0.0

        # Check HTF trend reversal - full exit
        htf_reversal = self.htf_ema_fast_line[-1] < self.htf_ema_slow_line[-1]
        if self.position.is_long and htf_reversal:
            return True, 1.0
        elif self.position.is_short and not htf_reversal:
            return True, 1.0

        # Check MTF momentum reversal - partial exit
        mtf_reversal = self.mtf_macd[-1] < self.mtf_signal[-1]
        if self.position.is_long and mtf_reversal:
            return True, self.cascade_exit_pct

        # Check time decay - full exit if no progress
        if self.holding_periods > self.time_decay_periods:
            current_profit = (self.data.Close[-1] - self.entry_price) / self.entry_price
            if abs(current_profit) < 0.01:  # Less than 1% move
                return True, 1.0

        # Check profit scaling opportunity
        if self.position.is_long:
            current_profit_r = (self.data.Close[-1] - self.entry_price) / self.calculate_dynamic_stop()
            if current_profit_r > self.profit_scale_threshold and current_profit_r > self.peak_profit:
                self.peak_profit = current_profit_r
                # This would be where we add to position in live trading

        return False, 0.0

    def next(self):
        """Execute multi-timeframe momentum cascade logic"""

        # Skip if not enough data
        if len(self.data) < 100:
            return

        # Update holding period
        if self.position:
            self.holding_periods += 1
        else:
            self.holding_periods = 0
            self.peak_profit = 0

        # Calculate current confluence score
        self.confluence_score = self.calculate_confluence_score()

        # Check exit conditions first
        should_exit, exit_pct = self.check_exit_conditions()
        if should_exit and self.position:
            if exit_pct >= 1.0:
                self.position.close()
            else:
                # Partial exit (close half position)
                self.position.close(portion=exit_pct)
            return

        # Entry logic - only enter if no position
        if not self.position:
            position_size = self.calculate_position_size(self.confluence_score)

            if position_size > 0:
                # Long entry conditions
                long_signal = (
                    self.htf_ema_fast_line[-1] > self.htf_ema_slow_line[-1] and  # HTF bullish
                    self.mtf_macd[-1] > self.mtf_signal[-1] and  # MTF momentum up
                    self.mtf_rsi[-1] > self.mtf_rsi_threshold and  # RSI bullish
                    self.data.Close[-1] > self.ltf_ema[-1] and  # LTF above EMA
                    self.data.Close[-1] > self.ltf_vwap[-1] and  # Above VWAP
                    self.volume_surge[-1] > 1.0  # Volume confirmation
                )

                # Short entry conditions
                short_signal = (
                    self.htf_ema_fast_line[-1] < self.htf_ema_slow_line[-1] and  # HTF bearish
                    self.mtf_macd[-1] < self.mtf_signal[-1] and  # MTF momentum down
                    self.mtf_rsi[-1] < self.mtf_rsi_threshold and  # RSI bearish
                    self.data.Close[-1] < self.ltf_ema[-1] and  # LTF below EMA
                    self.data.Close[-1] < self.ltf_vwap[-1] and  # Below VWAP
                    self.volume_surge[-1] > 1.0  # Volume confirmation
                )

                # Execute trades with dynamic sizing
                if long_signal:
                    # Additional trend strength filter
                    if self.adx[-1] > 25:  # Trending market
                        stop_distance = self.calculate_dynamic_stop()
                        stop_price = self.data.Close[-1] - stop_distance
                        take_profit = self.data.Close[-1] + (stop_distance * 2)  # 2:1 R:R

                        self.buy(size=position_size)
                        self.entry_price = self.data.Close[-1]
                        # Store stop loss values for manual management
                        self.stop_loss = stop_price
                        self.take_profit = take_profit

                elif short_signal:
                    # Additional trend strength filter
                    if self.adx[-1] > 25:  # Trending market
                        stop_distance = self.calculate_dynamic_stop()
                        stop_price = self.data.Close[-1] + stop_distance
                        take_profit = self.data.Close[-1] - (stop_distance * 2)  # 2:1 R:R

                        self.sell(size=position_size)
                        self.entry_price = self.data.Close[-1]
                        # Store stop loss values for manual management
                        self.stop_loss = stop_price
                        self.take_profit = take_profit

        # Manual stop loss and take profit management
        elif self.position:
            current_price = self.data.Close[-1]

            if self.position.is_long:
                # Check stop loss
                if hasattr(self, 'stop_loss') and current_price <= self.stop_loss:
                    self.position.close()
                    return
                # Check take profit
                if hasattr(self, 'take_profit') and current_price >= self.take_profit:
                    self.position.close()
                    return
                # Trail stop loss
                stop_distance = self.calculate_dynamic_stop()
                new_stop = current_price - stop_distance
                if hasattr(self, 'stop_loss') and new_stop > self.stop_loss:
                    self.stop_loss = new_stop

            elif self.position.is_short:
                # Check stop loss
                if hasattr(self, 'stop_loss') and current_price >= self.stop_loss:
                    self.position.close()
                    return
                # Check take profit
                if hasattr(self, 'take_profit') and current_price <= self.take_profit:
                    self.position.close()
                    return
                # Trail stop loss
                stop_distance = self.calculate_dynamic_stop()
                new_stop = current_price + stop_distance
                if hasattr(self, 'stop_loss') and new_stop < self.stop_loss:
                    self.stop_loss = new_stop


def test_mtmc_strategy(data_path: str, symbol: str = "Unknown", timeframe: str = "1h"):
    """
    Test MTMC strategy on provided data

    Args:
        data_path: Path to CSV file with OHLCV data
        symbol: Symbol being tested
        timeframe: Timeframe of the data
    """
    print(f"\n{'='*80}")
    print(f"🌙 Testing MTMC Strategy on {symbol} ({timeframe}) 🌙")
    print(f"Data: {data_path}")
    print(f"{'='*80}\n")

    try:
        # Load data
        df = pd.read_csv(data_path)

        # Ensure proper column names
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        # Run backtest
        bt = Backtest(
            df,
            MTMCStrategy,
            cash=100000,
            commission=0.001,
            exclusive_orders=True
        )

        # Run with default parameters
        stats = bt.run()

        # Display results
        print("\n📊 MTMC Strategy Performance Metrics:")
        print("="*50)
        print(stats)

        # Show plot
        bt.plot(show_legend=False, open_browser=False)

        return stats

    except Exception as e:
        print(f"❌ Error testing MTMC strategy: {str(e)}")
        return None


if __name__ == "__main__":
    # Test with sample data if available
    test_path = "/Users/bobbyyo/Projects/algo-fun/data/yahoo/ETH-USD_1h_2020-2024.csv"
    print("🚀 Multi-Timeframe Momentum Cascade Strategy Test 🚀")
    print("="*80)

    # Check if test data exists
    import os
    if os.path.exists(test_path):
        test_mtmc_strategy(test_path, "ETH-USD", "1h")
    else:
        print(f"⚠️ Test data not found at: {test_path}")
        print("Please provide valid data path for testing")