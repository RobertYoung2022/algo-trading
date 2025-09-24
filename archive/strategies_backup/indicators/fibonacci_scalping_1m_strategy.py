"""
🌙 1-Minute Fibonacci Scalping Strategy 🌙
================================================
Advanced scalping strategy based on Fibonacci retracement levels and market structure breaks.
Designed for 1-minute timeframe with session filtering and golden pocket entry logic.

Strategy Components:
1. Trading Session Filter (London & New York sessions only)
2. Impulse Move & Market Structure Break Detection
3. Fibonacci Retracement Application (0.5 and 0.618 levels)
4. Golden Pocket Entry Logic (between 0.5 and 0.618)
5. Risk Management with proper R:R ratios
6. Trade Management Rules

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib
from datetime import datetime, time
import pytz

class FibonacciScalpingStrategy(Strategy):
    """
    🚀 Advanced 1-Minute Fibonacci Scalping Strategy 🚀

    This strategy identifies strong impulse moves during major trading sessions,
    waits for retracements to the golden pocket (0.5-0.618 Fib levels),
    and enters trades with precise risk management.
    """

    # Strategy Parameters
    swing_lookback = 20  # Bars to look back for swing detection
    min_impulse_bars = 3  # Minimum consecutive bars for impulse
    min_impulse_size_pips = 10  # Minimum pips for valid impulse
    volume_multiplier = 1.2  # Volume must be this x average for impulse

    # Risk Management Parameters
    risk_reward_ratio = 1.5  # Target R:R ratio
    max_risk_percent = 1.0  # Max risk per trade as % of capital

    # Session Parameters (in UTC)
    london_start = time(7, 0)  # 8:00 CET = 7:00 UTC
    london_end = time(16, 0)   # 17:00 CET = 16:00 UTC
    newyork_start = time(12, 0)  # 13:00 CET = 12:00 UTC
    newyork_end = time(21, 0)   # 22:00 CET = 21:00 UTC

    def init(self):
        """
        🎯 Initialize indicators and strategy state 🎯
        """
        # Price data
        self.high = self.data.High
        self.low = self.data.Low
        self.close = self.data.Close
        self.volume = self.data.Volume

        # Calculate ATR for dynamic thresholds
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, timeperiod=14)

        # Volume moving average for impulse detection
        self.volume_ma = self.I(talib.SMA, self.data.Volume, timeperiod=20)

        # Initialize swing points tracking
        self.swing_highs = self.I(self._detect_swing_highs)
        self.swing_lows = self.I(self._detect_swing_lows)

        # Strategy state tracking
        self.current_impulse = None  # Track current impulse move
        self.fib_levels = None  # Store Fibonacci levels
        self.entry_triggered = False  # Track if entry was triggered for current impulse
        self.last_structure_break = None  # Track last structure break

    def _detect_swing_highs(self):
        """
        🔍 Detect swing high points in price action 🔍
        """
        highs = self.data.High
        swing_highs = np.zeros(len(highs))

        for i in range(self.swing_lookback, len(highs) - self.swing_lookback):
            # Check if current high is highest in the window
            window_start = max(0, i - self.swing_lookback)
            window_end = min(len(highs), i + self.swing_lookback + 1)

            if highs[i] == max(highs[window_start:window_end]):
                swing_highs[i] = highs[i]

        return swing_highs

    def _detect_swing_lows(self):
        """
        🔍 Detect swing low points in price action 🔍
        """
        lows = self.data.Low
        swing_lows = np.zeros(len(lows))

        for i in range(self.swing_lookback, len(lows) - self.swing_lookback):
            # Check if current low is lowest in the window
            window_start = max(0, i - self.swing_lookback)
            window_end = min(len(lows), i + self.swing_lookback + 1)

            if lows[i] == min(lows[window_start:window_end]):
                swing_lows[i] = lows[i]

        return swing_lows

    def _is_trading_session(self, current_time):
        """
        ⏰ Check if current time is within London or New York trading sessions ⏰
        """
        # Extract time from timestamp
        if hasattr(current_time, 'hour'):
            # If it's a pandas timestamp or datetime
            hour_min = time(current_time.hour, current_time.minute)
        elif hasattr(current_time, 'time'):
            hour_min = current_time.time()
        else:
            # If it's already a time object or something else
            return True  # Allow trading if we can't determine time

        # Check London session
        if self.london_start <= hour_min <= self.london_end:
            return True

        # Check New York session
        if self.newyork_start <= hour_min <= self.newyork_end:
            return True

        return False

    def _detect_impulse_move(self, lookback=10):
        """
        💥 Detect strong impulse moves that break market structure 💥

        Returns: dict with impulse details or None
        """
        if len(self.data) < lookback + self.min_impulse_bars:
            return None

        # Get recent price action
        recent_highs = self.data.High[-lookback:]
        recent_lows = self.data.Low[-lookback:]
        recent_closes = self.data.Close[-lookback:]
        recent_volumes = self.data.Volume[-lookback:]

        # Check for bullish impulse
        bullish_bars = 0
        for i in range(-self.min_impulse_bars, 0):
            if recent_closes[i] > recent_closes[i-1]:
                bullish_bars += 1

        if bullish_bars >= self.min_impulse_bars:
            # Calculate impulse size
            impulse_start_idx = -self.min_impulse_bars - 1
            impulse_low = min(recent_lows[impulse_start_idx:])
            impulse_high = max(recent_highs[impulse_start_idx:])
            impulse_size = impulse_high - impulse_low

            # Check volume confirmation
            avg_volume = np.mean(self.volume_ma[-lookback:])
            impulse_volume = np.mean(recent_volumes[impulse_start_idx:])

            # Check if impulse is significant
            if (impulse_size >= self.min_impulse_size_pips * 0.0001 and  # Convert pips to price
                impulse_volume >= avg_volume * self.volume_multiplier):

                # Check for structure break (new high)
                prev_swing_high = self._get_last_swing_high(lookback * 2)
                if impulse_high > prev_swing_high:
                    return {
                        'type': 'bullish',
                        'start': impulse_low,
                        'end': impulse_high,
                        'size': impulse_size,
                        'volume_ratio': impulse_volume / avg_volume
                    }

        # Check for bearish impulse
        bearish_bars = 0
        for i in range(-self.min_impulse_bars, 0):
            if recent_closes[i] < recent_closes[i-1]:
                bearish_bars += 1

        if bearish_bars >= self.min_impulse_bars:
            # Calculate impulse size
            impulse_start_idx = -self.min_impulse_bars - 1
            impulse_high = max(recent_highs[impulse_start_idx:])
            impulse_low = min(recent_lows[impulse_start_idx:])
            impulse_size = impulse_high - impulse_low

            # Check volume confirmation
            avg_volume = np.mean(self.volume_ma[-lookback:])
            impulse_volume = np.mean(recent_volumes[impulse_start_idx:])

            # Check if impulse is significant
            if (impulse_size >= self.min_impulse_size_pips * 0.0001 and  # Convert pips to price
                impulse_volume >= avg_volume * self.volume_multiplier):

                # Check for structure break (new low)
                prev_swing_low = self._get_last_swing_low(lookback * 2)
                if impulse_low < prev_swing_low:
                    return {
                        'type': 'bearish',
                        'start': impulse_high,
                        'end': impulse_low,
                        'size': impulse_size,
                        'volume_ratio': impulse_volume / avg_volume
                    }

        return None

    def _get_last_swing_high(self, lookback):
        """
        📊 Get the last significant swing high 📊
        """
        for i in range(-1, -min(lookback, len(self.swing_highs)), -1):
            if self.swing_highs[i] > 0:
                return self.swing_highs[i]
        return self.data.High[-lookback] if len(self.data) >= lookback else self.data.High[0]

    def _get_last_swing_low(self, lookback):
        """
        📊 Get the last significant swing low 📊
        """
        for i in range(-1, -min(lookback, len(self.swing_lows)), -1):
            if self.swing_lows[i] > 0:
                return self.swing_lows[i]
        return self.data.Low[-lookback] if len(self.data) >= lookback else self.data.Low[0]

    def _calculate_fibonacci_levels(self, impulse):
        """
        📐 Calculate Fibonacci retracement levels for the impulse move 📐
        """
        if impulse['type'] == 'bullish':
            # For bullish impulse, Fib levels from low to high
            fib_0 = impulse['start']  # 0% level (low)
            fib_100 = impulse['end']  # 100% level (high)

            fib_levels = {
                '0.0': fib_0,
                '0.382': fib_0 + (fib_100 - fib_0) * 0.382,
                '0.5': fib_0 + (fib_100 - fib_0) * 0.5,
                '0.618': fib_0 + (fib_100 - fib_0) * 0.618,
                '1.0': fib_100,
                'golden_pocket_top': fib_0 + (fib_100 - fib_0) * 0.618,
                'golden_pocket_bottom': fib_0 + (fib_100 - fib_0) * 0.5
            }
        else:
            # For bearish impulse, Fib levels from high to low
            fib_0 = impulse['start']  # 0% level (high)
            fib_100 = impulse['end']  # 100% level (low)

            fib_levels = {
                '0.0': fib_0,
                '0.382': fib_0 - (fib_0 - fib_100) * 0.382,
                '0.5': fib_0 - (fib_0 - fib_100) * 0.5,
                '0.618': fib_0 - (fib_0 - fib_100) * 0.618,
                '1.0': fib_100,
                'golden_pocket_top': fib_0 - (fib_0 - fib_100) * 0.5,
                'golden_pocket_bottom': fib_0 - (fib_0 - fib_100) * 0.618
            }

        return fib_levels

    def _is_in_golden_pocket(self, price, fib_levels, impulse_type):
        """
        ✨ Check if price is within the golden pocket (0.5-0.618 Fib zone) ✨
        """
        if impulse_type == 'bullish':
            # For bullish, golden pocket is a support zone
            return (fib_levels['golden_pocket_bottom'] <= price <= fib_levels['golden_pocket_top'])
        else:
            # For bearish, golden pocket is a resistance zone
            return (fib_levels['golden_pocket_bottom'] <= price <= fib_levels['golden_pocket_top'])

    def next(self):
        """
        🎮 Main strategy execution logic 🎮
        """
        # Skip if not enough data
        if len(self.data) < self.swing_lookback * 2:
            return

        # Get current timestamp
        current_time = self.data.index[-1]

        # Check if we're in a trading session
        if not self._is_trading_session(current_time):
            return

        # Current price
        current_price = self.data.Close[-1]

        # Detect new impulse moves
        impulse = self._detect_impulse_move()

        # Update current impulse if new one detected
        if impulse and impulse != self.current_impulse:
            self.current_impulse = impulse
            self.fib_levels = self._calculate_fibonacci_levels(impulse)
            self.entry_triggered = False  # Reset entry flag for new impulse

        # Skip if no active impulse or entry already triggered
        if not self.current_impulse or self.entry_triggered:
            return

        # Check for golden pocket entry
        if self._is_in_golden_pocket(current_price, self.fib_levels, self.current_impulse['type']):

            # Calculate position size and risk parameters
            if self.current_impulse['type'] == 'bullish':
                # Long entry
                stop_loss = self.fib_levels['0.0']  # Below impulse start
                entry_price = current_price
                take_profit = entry_price + (entry_price - stop_loss) * self.risk_reward_ratio

                # Check if setup is still valid (price hasn't exceeded impulse high)
                if current_price < self.fib_levels['1.0']:
                    # Calculate position size based on risk
                    risk_amount = self.equity * (self.max_risk_percent / 100)
                    price_risk = entry_price - stop_loss

                    if price_risk > 0:
                        position_size = min(risk_amount / price_risk, 0.95)  # Max 95% of equity

                        if not self.position:
                            self.buy(size=position_size, sl=stop_loss, tp=take_profit)
                            self.entry_triggered = True

            elif self.current_impulse['type'] == 'bearish':
                # Short entry
                stop_loss = self.fib_levels['0.0']  # Above impulse start
                entry_price = current_price
                take_profit = entry_price - (stop_loss - entry_price) * self.risk_reward_ratio

                # Check if setup is still valid (price hasn't gone below impulse low)
                if current_price > self.fib_levels['1.0']:
                    # Calculate position size based on risk
                    risk_amount = self.equity * (self.max_risk_percent / 100)
                    price_risk = stop_loss - entry_price

                    if price_risk > 0:
                        position_size = min(risk_amount / price_risk, 0.95)  # Max 95% of equity

                        if not self.position:
                            self.sell(size=position_size, sl=stop_loss, tp=take_profit)
                            self.entry_triggered = True

        # Invalidate setup if price goes beyond impulse extremes
        if self.current_impulse:
            if self.current_impulse['type'] == 'bullish' and current_price > self.fib_levels['1.0'] * 1.02:
                # Price exceeded impulse high significantly, invalidate setup
                self.current_impulse = None
                self.fib_levels = None

            elif self.current_impulse['type'] == 'bearish' and current_price < self.fib_levels['1.0'] * 0.98:
                # Price exceeded impulse low significantly, invalidate setup
                self.current_impulse = None
                self.fib_levels = None


def run_fibonacci_scalping_backtest(data_path, symbol="XRPUSD", cash=10000, commission=0.002):
    """
    🚀 Run backtest for the Fibonacci Scalping Strategy 🚀

    Parameters:
    -----------
    data_path : str
        Path to the CSV file containing 1-minute OHLCV data
    symbol : str
        Trading symbol (e.g., 'XRPUSD', 'BTCUSD')
    cash : float
        Starting capital for backtest
    commission : float
        Commission rate per trade

    Returns:
    --------
    stats : pandas.Series
        Backtest statistics
    """

    # Load data
    print(f"\n{'='*60}")
    print(f"🌙 Loading 1-minute data for {symbol} 🌙")
    print(f"{'='*60}")

    try:
        df = pd.read_csv(data_path)

        # Handle different date column names
        date_columns = ['timestamp', 'datetime', 'date', 'Date', 'Timestamp']
        date_col = None
        for col in date_columns:
            if col in df.columns:
                date_col = col
                break

        if date_col:
            df[date_col] = pd.to_datetime(df[date_col])
            df.set_index(date_col, inplace=True)

        # Ensure proper column names
        column_mapping = {
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }

        df.rename(columns=column_mapping, inplace=True)

        # Required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        print(f"✅ Data loaded successfully!")
        print(f"📊 Data shape: {df.shape}")
        print(f"📅 Date range: {df.index[0]} to {df.index[-1]}")
        print(f"💹 Price range: ${df['Low'].min():.2f} - ${df['High'].max():.2f}")

    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

    # Run backtest
    print(f"\n{'='*60}")
    print(f"🚀 Running Fibonacci Scalping Strategy Backtest 🚀")
    print(f"{'='*60}")

    bt = Backtest(
        df,
        FibonacciScalpingStrategy,
        cash=cash,
        commission=commission,
        exclusive_orders=True,
        trade_on_close=False
    )

    # Run the backtest
    stats = bt.run()

    # Display results
    print(f"\n{'='*60}")
    print(f"📊 BACKTEST RESULTS FOR {symbol} 📊")
    print(f"{'='*60}")
    print(stats)

    # Plot the results
    print(f"\n🎨 Generating performance visualization...")
    bt.plot(resample='1H', show_legend=True, open_browser=False)

    return stats


if __name__ == "__main__":
    # Test with available 1-minute data
    xrp_1m_path = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/XRPUSD-1m-52wks-enhanced-data.csv"

    # Run backtest
    stats = run_fibonacci_scalping_backtest(
        data_path=xrp_1m_path,
        symbol="XRPUSD",
        cash=10000,
        commission=0.002
    )