"""
🚀 TEST ONE CANDLE STRATEGY ON ALL DATA SOURCES 🚀
==================================================
Test the One Candle FTX Strategy across multiple data sources
using Bobby's multi-data testing framework.

Author: Bobby's Algo Fun 💫
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
import warnings
warnings.filterwarnings('ignore')

print("💫 Bobby's One Candle Strategy Multi-Data Tester... 🌙")

# ============================================================
# SIMPLIFIED ONE CANDLE STRATEGY FOR MULTI-DATA TESTING
# ============================================================

class OneCandleStrategy(Strategy):
    """
    🎯 Simplified One Candle Strategy for multi-data testing

    This version focuses on:
    - Daily session ranges (for 24/7 crypto markets)
    - Fair Value Gap detection
    - Engulfing pattern entries
    - Strict 3:1 reward-to-risk ratio
    """

    # Strategy parameters
    reward_to_risk = 3.0
    position_size_pct = 0.02
    min_gap_size_pct = 0.001
    max_fvg_age = 20

    def init(self):
        """Initialize indicators"""
        # Technical indicators
        self.rsi = self.I(talib.RSI, self.data.Close, 14)
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, 14)
        self.sma_volume = self.I(talib.SMA, self.data.Volume, 20)

        # Track session and trade state
        self.session_high = None
        self.session_low = None
        self.trades_today = 0
        self.current_date = None

    def next(self):
        """Main strategy logic"""
        # Skip if not enough data
        if len(self.data) < 20:
            return

        # Update daily session range
        current_time = self.data.index[-1]
        if self.current_date != current_time.date():
            self.current_date = current_time.date()
            self.trades_today = 0
            # Set new session range
            self.session_high = self.data.High[-1]
            self.session_low = self.data.Low[-1]

        # Update session range if still in first hour
        if current_time.hour < 1 and self.session_high:
            self.session_high = max(self.session_high, self.data.High[-1])
            self.session_low = min(self.session_low, self.data.Low[-1])

        # Skip if no session range or max trades reached
        if not self.session_high or self.trades_today >= 1:
            return

        # Only trade if price breaks session range
        current_price = self.data.Close[-1]
        if self.session_low < current_price < self.session_high:
            return

        # Entry logic
        if not self.position:
            # Look for Fair Value Gaps
            if self.detect_bullish_fvg() and current_price > self.session_high:
                # Bullish setup
                if self.detect_bullish_engulfing():
                    # Calculate position
                    entry = self.data.Close[-1]
                    stop = self.data.Low[-2] - (self.atr[-1] * 0.1)
                    target = entry + ((entry - stop) * self.reward_to_risk)

                    # Enter long
                    size = self.calculate_position_size(entry - stop)
                    self.buy(size=size, sl=stop, tp=target)
                    self.trades_today += 1

            elif self.detect_bearish_fvg() and current_price < self.session_low:
                # Bearish setup
                if self.detect_bearish_engulfing():
                    # Calculate position
                    entry = self.data.Close[-1]
                    stop = self.data.High[-2] + (self.atr[-1] * 0.1)
                    target = entry - ((stop - entry) * self.reward_to_risk)

                    # Enter short
                    size = self.calculate_position_size(stop - entry)
                    self.sell(size=size, sl=stop, tp=target)
                    self.trades_today += 1

    def detect_bullish_fvg(self):
        """Detect bullish Fair Value Gap"""
        if len(self.data) < 3:
            return False

        # Check last 3 candles for gap up
        for i in range(-min(self.max_fvg_age, len(self.data)-3), -2):
            if (self.data.Low[i] > self.data.High[i-2] and
                (self.data.Low[i] - self.data.High[i-2]) / self.data.High[i-2] >= self.min_gap_size_pct):
                return True
        return False

    def detect_bearish_fvg(self):
        """Detect bearish Fair Value Gap"""
        if len(self.data) < 3:
            return False

        # Check last 3 candles for gap down
        for i in range(-min(self.max_fvg_age, len(self.data)-3), -2):
            if (self.data.High[i] < self.data.Low[i-2] and
                (self.data.Low[i-2] - self.data.High[i]) / self.data.High[i] >= self.min_gap_size_pct):
                return True
        return False

    def detect_bullish_engulfing(self):
        """Detect bullish engulfing pattern"""
        if len(self.data) < 2:
            return False

        prev_body = abs(self.data.Close[-2] - self.data.Open[-2])
        curr_body = abs(self.data.Close[-1] - self.data.Open[-1])

        return (self.data.Close[-2] < self.data.Open[-2] and  # Prev bearish
                self.data.Close[-1] > self.data.Open[-1] and  # Curr bullish
                curr_body > prev_body * 1.5 and  # Significant engulfing
                self.data.Close[-1] > self.data.Open[-2])  # Closes above prev open

    def detect_bearish_engulfing(self):
        """Detect bearish engulfing pattern"""
        if len(self.data) < 2:
            return False

        prev_body = abs(self.data.Close[-2] - self.data.Open[-2])
        curr_body = abs(self.data.Close[-1] - self.data.Open[-1])

        return (self.data.Close[-2] > self.data.Open[-2] and  # Prev bullish
                self.data.Close[-1] < self.data.Open[-1] and  # Curr bearish
                curr_body > prev_body * 1.5 and  # Significant engulfing
                self.data.Close[-1] < self.data.Open[-2])  # Closes below prev open

    def calculate_position_size(self, stop_distance):
        """Calculate position size based on risk"""
        if stop_distance <= 0:
            return 0.1

        risk_amount = self.equity * self.position_size_pct
        position_value = risk_amount / stop_distance
        return min(position_value / self.data.Close[-1], 0.95)


# ============================================================
# TEST ON ALL DATA SOURCES
# ============================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🚀 TESTING ONE CANDLE STRATEGY ON ALL DATA SOURCES")
    print("="*80)

    import sys
    import os
    sys.path.append('/Users/bobbyyo/Projects/algo-fun')
    from multi_data_tester import test_on_all_data

    # Test the strategy on all configured data sources
    results = test_on_all_data(
        OneCandleStrategy,
        'One_Candle_Strategy',
        optimize=False,
        cash=1000000,
        commission=0.001,
        verbose=True
    )

    if results is not None:
        print("\n" + "="*80)
        print("📊 ONE CANDLE STRATEGY - FINAL RESULTS SUMMARY")
        print("="*80)

        # Calculate aggregate statistics
        avg_return = results['Return_%'].mean()
        avg_sharpe = results['Sharpe'].mean()
        avg_win_rate = results['Win_Rate_%'].mean()
        total_trades = results['Trades'].sum()

        print(f"\n🎯 AGGREGATE PERFORMANCE:")
        print(f"{'Average Return:':<25} {avg_return:.2f}%")
        print(f"{'Average Sharpe Ratio:':<25} {avg_sharpe:.3f}")
        print(f"{'Average Win Rate:':<25} {avg_win_rate:.2f}%")
        print(f"{'Total Trades:':<25} {total_trades}")

        # Best and worst performers
        best_idx = results['Return_%'].idxmax()
        worst_idx = results['Return_%'].idxmin()

        print(f"\n🏆 BEST PERFORMER:")
        print(f"  Data Source: {results.loc[best_idx, 'Data_Source']}")
        print(f"  Return: {results.loc[best_idx, 'Return_%']:.2f}%")
        print(f"  Sharpe: {results.loc[best_idx, 'Sharpe']:.3f}")

        print(f"\n📉 WORST PERFORMER:")
        print(f"  Data Source: {results.loc[worst_idx, 'Data_Source']}")
        print(f"  Return: {results.loc[worst_idx, 'Return_%']:.2f}%")
        print(f"  Sharpe: {results.loc[worst_idx, 'Sharpe']:.3f}")

        print("\n✅ One Candle Strategy multi-data testing complete!")
        print(f"📁 Results saved in: ./results/One_Candle_Strategy.csv")
        print("="*80)