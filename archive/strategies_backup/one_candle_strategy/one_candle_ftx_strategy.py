"""
🌙 ONE CANDLE IS ALL YOU NEED STRATEGY 🚀
=========================================
An institutional trading strategy that uses Fair Value Gaps (FVGs)
and session opening ranges to capture market inefficiencies.

Strategy Components:
1. Session Opening Box (9:30-9:35 AM ET) - Establishes day's range
2. Range Break Detection - Identifies directional bias
3. Fair Value Gap (FVG) Identification - Finds 3-candle gaps
4. Engulfing Pattern Entry - Precise entry signal within FVG
5. Risk Management - Strict 3:1 reward-to-risk ratio

Author: Bobby's Algo Fun 💫
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from datetime import datetime, time, timedelta
import warnings
warnings.filterwarnings('ignore')

print("💫 Bobby's One Candle FTX Strategy Loading... 🌙")
print("=" * 80)

# ============================================================
# STRATEGY PARAMETERS 🎯
# ============================================================
SESSION_START = time(9, 30)  # 9:30 AM ET
SESSION_END = time(9, 35)    # 9:35 AM ET (5-minute range)
MARKET_CLOSE = time(16, 0)   # 4:00 PM ET

# Risk Management
REWARD_TO_RISK = 3.0         # 3:1 reward-to-risk ratio
MAX_TRADES_PER_DAY = 1       # Maximum 1 trade per session
POSITION_SIZE_PERCENT = 2.0  # Risk 2% per trade

# FVG Parameters
MIN_GAP_SIZE_PERCENT = 0.1   # Minimum gap size as % of price
MAX_FVG_AGE_BARS = 20        # Maximum bars to look back for FVG

class OneCandleFTXStrategy(Strategy):
    """
    🎯 The One Candle Is All You Need Strategy

    This strategy captures institutional order flow through Fair Value Gaps.
    It waits for price to break the session opening range, identifies FVGs,
    and enters on engulfing patterns within the gap zone.
    """

    # Strategy parameters (can be optimized)
    session_minutes = 5           # Minutes for opening range (default 5)
    reward_to_risk = REWARD_TO_RISK
    max_trades_per_day = MAX_TRADES_PER_DAY
    position_size_pct = POSITION_SIZE_PERCENT / 100
    min_gap_size_pct = MIN_GAP_SIZE_PERCENT / 100
    max_fvg_age = MAX_FVG_AGE_BARS

    def init(self):
        """
        🌟 Initialize strategy indicators and variables
        """
        # Session tracking
        self.session_high = None
        self.session_low = None
        self.session_established = False
        self.current_date = None
        self.trades_today = 0

        # FVG tracking
        self.bullish_fvgs = []  # List of (start_idx, end_idx, gap_high, gap_low)
        self.bearish_fvgs = []  # List of (start_idx, end_idx, gap_high, gap_low)

        # Trade tracking
        self.trade_direction = None  # 'long' or 'short'
        self.entry_price = None
        self.stop_loss = None
        self.take_profit = None

        # Technical indicators for confirmation
        self.rsi = self.I(talib.RSI, self.data.Close, 14)
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, 14)

        # Volume analysis
        self.volume_sma = self.I(talib.SMA, self.data.Volume, 20)

        print(f"🚀 Strategy initialized with {len(self.data)} bars")

    def identify_session_range(self):
        """
        📊 Identify the session opening range (first 5 minutes)
        For crypto, we'll use hourly data and define sessions differently
        """
        # For crypto markets (24/7), we'll define "sessions" as daily periods
        # Since we're using hourly data, we'll take the first hour as the "opening range"

        current_time = self.data.index[-1]

        # Check if we're at the start of a new day (or session)
        if self.current_date != current_time.date():
            self.current_date = current_time.date()
            self.session_established = False
            self.trades_today = 0
            self.session_high = None
            self.session_low = None

            # For hourly data, use the first bar of the day as the opening range
            if current_time.hour == 0:  # First hour of the day
                self.session_high = self.data.High[-1]
                self.session_low = self.data.Low[-1]
                self.session_established = True
                print(f"📍 Session range established: High={self.session_high:.2f}, Low={self.session_low:.2f}")

    def detect_range_break(self):
        """
        🔍 Detect if price has broken above or below the session range
        Returns: 'bullish', 'bearish', or None
        """
        if not self.session_established or self.session_high is None:
            return None

        current_price = self.data.Close[-1]

        # Check for range break
        if current_price > self.session_high:
            return 'bullish'
        elif current_price < self.session_low:
            return 'bearish'

        return None

    def find_fair_value_gaps(self):
        """
        🕳️ Identify Fair Value Gaps (FVGs) in recent price action

        Bullish FVG: Gap where candle 3 low > candle 1 high
        Bearish FVG: Gap where candle 3 high < candle 1 low
        """
        # Clear old FVGs
        self.bullish_fvgs = []
        self.bearish_fvgs = []

        # Need at least 3 bars to identify FVG
        if len(self.data) < self.max_fvg_age + 3:
            return

        # Look for FVGs in recent bars
        for i in range(-self.max_fvg_age, -2):
            # Get three consecutive candles
            candle1_high = self.data.High[i-2]
            candle1_low = self.data.Low[i-2]
            candle2_high = self.data.High[i-1]
            candle2_low = self.data.Low[i-1]
            candle3_high = self.data.High[i]
            candle3_low = self.data.Low[i]

            # Check for Bullish FVG (gap up)
            if candle3_low > candle1_high:
                gap_size = (candle3_low - candle1_high) / candle1_high
                if gap_size >= self.min_gap_size_pct:
                    # FVG zone is between candle 1 high and candle 3 low
                    self.bullish_fvgs.append({
                        'start_idx': i-2,
                        'end_idx': i,
                        'gap_high': candle3_low,
                        'gap_low': candle1_high,
                        'gap_size': gap_size
                    })

            # Check for Bearish FVG (gap down)
            elif candle3_high < candle1_low:
                gap_size = (candle1_low - candle3_high) / candle3_high
                if gap_size >= self.min_gap_size_pct:
                    # FVG zone is between candle 3 high and candle 1 low
                    self.bearish_fvgs.append({
                        'start_idx': i-2,
                        'end_idx': i,
                        'gap_high': candle1_low,
                        'gap_low': candle3_high,
                        'gap_size': gap_size
                    })

    def detect_engulfing_pattern(self):
        """
        🕯️ Detect bullish or bearish engulfing patterns
        Returns: 'bullish', 'bearish', or None
        """
        if len(self.data) < 2:
            return None

        # Previous and current candle
        prev_open = self.data.Open[-2]
        prev_close = self.data.Close[-2]
        prev_high = self.data.High[-2]
        prev_low = self.data.Low[-2]

        curr_open = self.data.Open[-1]
        curr_close = self.data.Close[-1]
        curr_high = self.data.High[-1]
        curr_low = self.data.Low[-1]

        # Bullish engulfing: current green candle completely engulfs previous red candle
        if (prev_close < prev_open and  # Previous was red
            curr_close > curr_open and  # Current is green
            curr_open <= prev_close and  # Opens at or below prev close
            curr_close >= prev_open and  # Closes above prev open
            curr_high > prev_high and   # Higher high
            curr_low < prev_low):        # Lower low
            return 'bullish'

        # Bearish engulfing: current red candle completely engulfs previous green candle
        if (prev_close > prev_open and  # Previous was green
            curr_close < curr_open and  # Current is red
            curr_open >= prev_close and  # Opens at or above prev close
            curr_close <= prev_open and  # Closes below prev open
            curr_high > prev_high and   # Higher high
            curr_low < prev_low):        # Lower low
            return 'bearish'

        return None

    def check_price_in_fvg(self, fvg_list):
        """
        ✅ Check if current price is within any FVG zone
        Returns: The FVG dict if price is in zone, None otherwise
        """
        current_price = self.data.Close[-1]

        for fvg in fvg_list:
            if fvg['gap_low'] <= current_price <= fvg['gap_high']:
                return fvg

        return None

    def calculate_position_size(self, stop_distance):
        """
        💰 Calculate position size based on risk management rules
        """
        # Risk 2% of account per trade
        account_value = self.equity
        risk_amount = account_value * self.position_size_pct

        # Position size = Risk Amount / Stop Distance
        if stop_distance > 0:
            position_size = risk_amount / stop_distance
            # Limit to available equity
            return min(position_size / self.data.Close[-1], 0.95)

        return 0.1  # Default small position

    def next(self):
        """
        🎯 Main strategy logic - executed on each new bar
        """
        # Skip if not enough data
        if len(self.data) < 20:
            return

        # Update session range
        self.identify_session_range()

        # Check if we've hit max trades for the day
        if self.trades_today >= self.max_trades_per_day:
            return

        # Only trade if we have a session range established
        if not self.session_established:
            return

        # Detect range break to determine bias
        range_break = self.detect_range_break()
        if not range_break:
            return  # Price still within opening range

        # Find Fair Value Gaps
        self.find_fair_value_gaps()

        # Entry Logic
        if not self.position:
            # Detect engulfing pattern
            engulfing = self.detect_engulfing_pattern()

            if range_break == 'bullish' and engulfing == 'bullish':
                # Check if we're in a bullish FVG zone
                fvg = self.check_price_in_fvg(self.bullish_fvgs)
                if fvg:
                    # Calculate entry, stop loss, and take profit
                    entry_price = self.data.Close[-1]
                    stop_loss = self.data.Low[-2] - (self.atr[-1] * 0.1)  # Below engulfed candle
                    stop_distance = entry_price - stop_loss
                    take_profit = entry_price + (stop_distance * self.reward_to_risk)

                    # Calculate position size
                    size = self.calculate_position_size(stop_distance)

                    # Enter long position
                    self.buy(size=size, sl=stop_loss, tp=take_profit)
                    self.trades_today += 1

                    print(f"🟢 LONG Entry @ {entry_price:.2f}")
                    print(f"   Stop Loss: {stop_loss:.2f} | Take Profit: {take_profit:.2f}")
                    print(f"   FVG Gap Size: {fvg['gap_size']*100:.2f}%")

            elif range_break == 'bearish' and engulfing == 'bearish':
                # Check if we're in a bearish FVG zone
                fvg = self.check_price_in_fvg(self.bearish_fvgs)
                if fvg:
                    # Calculate entry, stop loss, and take profit
                    entry_price = self.data.Close[-1]
                    stop_loss = self.data.High[-2] + (self.atr[-1] * 0.1)  # Above engulfed candle
                    stop_distance = stop_loss - entry_price
                    take_profit = entry_price - (stop_distance * self.reward_to_risk)

                    # Calculate position size
                    size = self.calculate_position_size(stop_distance)

                    # Enter short position
                    self.sell(size=size, sl=stop_loss, tp=take_profit)
                    self.trades_today += 1

                    print(f"🔴 SHORT Entry @ {entry_price:.2f}")
                    print(f"   Stop Loss: {stop_loss:.2f} | Take Profit: {take_profit:.2f}")
                    print(f"   FVG Gap Size: {fvg['gap_size']*100:.2f}%")

# ============================================================
# BACKTESTING EXECUTION 🚀
# ============================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🌙 TESTING ONE CANDLE FTX STRATEGY 💫")
    print("="*80)

    # Load Bitcoin hourly data for initial testing
    data_path = '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1h-500wks-data.csv'

    print(f"\n📂 Loading data from: {data_path}")

    # Load data
    data = pd.read_csv(data_path, parse_dates=['datetime'], index_col='datetime')
    data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    data = data.sort_index()

    print(f"📊 Data loaded: {len(data)} bars from {data.index[0]} to {data.index[-1]}")

    # Run backtest
    bt = Backtest(
        data,
        OneCandleFTXStrategy,
        cash=1000000,
        commission=0.001,
        exclusive_orders=True
    )

    # Run the strategy
    print("\n🔄 Running backtest...")
    stats = bt.run()

    # Print comprehensive stats
    print("\n" + "="*80)
    print("📊 BACKTEST RESULTS")
    print("="*80)
    print(stats)
    print("="*80)

    # Key metrics summary
    print("\n🎯 KEY PERFORMANCE METRICS:")
    print(f"{'Return:':<20} {stats['Return [%]']:.2f}%")
    print(f"{'Buy & Hold Return:':<20} {stats['Buy & Hold Return [%]']:.2f}%")
    print(f"{'Sharpe Ratio:':<20} {stats['Sharpe Ratio']:.3f}")
    print(f"{'Sortino Ratio:':<20} {stats['Sortino Ratio']:.3f}")
    print(f"{'Max Drawdown:':<20} {stats['Max. Drawdown [%]']:.2f}%")
    print(f"{'Win Rate:':<20} {stats['Win Rate [%]']:.2f}%")
    print(f"{'Number of Trades:':<20} {stats['# Trades']}")
    print(f"{'Profit Factor:':<20} {stats.get('Profit Factor', 0):.3f}")

    # Show plot
    print("\n📈 Generating performance plot...")
    bt.plot(resample='1D')

    print("\n✅ One Candle FTX Strategy testing complete! 🚀")