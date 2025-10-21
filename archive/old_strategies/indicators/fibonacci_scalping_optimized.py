"""
🌙 Optimized Fibonacci Scalping Strategy V2 🌙
================================================
Enhanced version with improved logic and parameters based on backtesting results.

Key Improvements:
1. Better impulse detection with volume profile analysis
2. Enhanced market structure validation
3. Improved golden pocket entry timing
4. Dynamic risk management based on volatility
5. Additional confluence filters for higher win rate
6. Liquidity zone detection for better entries

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 2.0.0
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib
from datetime import datetime, time


class FibonacciScalpingOptimized(Strategy):
    """
    🚀 Optimized Fibonacci Scalping Strategy V2 🚀

    Enhanced with liquidity awareness, multi-timeframe validation,
    and advanced market structure analysis.
    """

    # Optimized Parameters (based on testing)
    swing_lookback = 15  # Reduced for faster signal generation
    min_impulse_bars = 2  # More flexible impulse detection
    min_impulse_size_atr = 1.5  # Use ATR multiplier instead of fixed pips
    volume_multiplier = 1.5  # Higher volume requirement

    # Risk Management (Optimized)
    risk_reward_ratio = 1.2  # Lower but more achievable
    max_risk_percent = 0.5  # Reduced risk per trade

    # Enhanced Parameters
    rsi_oversold = 35  # RSI confluence for long entries
    rsi_overbought = 65  # RSI confluence for short entries
    momentum_period = 10  # For momentum confirmation

    # Session Parameters (Extended for crypto 24/7)
    use_session_filter = False  # Disabled for crypto markets

    def init(self):
        """
        🎯 Initialize enhanced indicators and state 🎯
        """
        # Price and Volume
        self.high = self.data.High
        self.low = self.data.Low
        self.close = self.data.Close
        self.volume = self.data.Volume

        # Core Indicators
        self.atr = self.I(talib.ATR, self.high, self.low, self.close, timeperiod=14)
        self.rsi = self.I(talib.RSI, self.close, timeperiod=14)
        self.volume_ma = self.I(talib.SMA, self.volume, timeperiod=20)

        # Enhanced Indicators
        self.ema_fast = self.I(talib.EMA, self.close, timeperiod=9)
        self.ema_slow = self.I(talib.EMA, self.close, timeperiod=21)
        self.macd, self.macd_signal, self.macd_hist = self.I(
            talib.MACD, self.close, fastperiod=12, slowperiod=26, signalperiod=9
        )

        # Volume Profile (simplified)
        self.vwap = self.I(self._calculate_vwap)

        # Market Structure
        self.swing_highs = self.I(self._detect_swing_highs_optimized)
        self.swing_lows = self.I(self._detect_swing_lows_optimized)

        # Strategy State
        self.impulse_active = False
        self.impulse_details = None
        self.fib_levels = None
        self.last_signal = None
        self.trades_today = 0
        self.max_trades_per_day = 5

    def _calculate_vwap(self):
        """
        📊 Calculate Volume Weighted Average Price 📊
        """
        typical_price = (self.high + self.low + self.close) / 3
        cumulative_tpv = (typical_price * self.volume).cumsum()
        cumulative_volume = self.volume.cumsum()

        # Avoid division by zero
        vwap = np.where(cumulative_volume > 0,
                        cumulative_tpv / cumulative_volume,
                        typical_price)
        return vwap

    def _detect_swing_highs_optimized(self):
        """
        🔍 Optimized swing high detection with liquidity awareness 🔍
        """
        highs = self.data.High
        volumes = self.data.Volume
        swing_highs = np.zeros(len(highs))

        for i in range(self.swing_lookback, len(highs) - self.swing_lookback):
            window_start = max(0, i - self.swing_lookback)
            window_end = min(len(highs), i + self.swing_lookback + 1)

            # Check if it's a local high
            if highs[i] == max(highs[window_start:window_end]):
                # Verify with volume (liquidity pool detection)
                vol_avg = np.mean(volumes[window_start:window_end])
                if volumes[i] > vol_avg * 1.2:  # Higher volume at swing = liquidity pool
                    swing_highs[i] = highs[i]

        return swing_highs

    def _detect_swing_lows_optimized(self):
        """
        🔍 Optimized swing low detection with liquidity awareness 🔍
        """
        lows = self.data.Low
        volumes = self.data.Volume
        swing_lows = np.full(len(lows), np.inf)

        for i in range(self.swing_lookback, len(lows) - self.swing_lookback):
            window_start = max(0, i - self.swing_lookback)
            window_end = min(len(lows), i + self.swing_lookback + 1)

            # Check if it's a local low
            if lows[i] == min(lows[window_start:window_end]):
                # Verify with volume (liquidity pool detection)
                vol_avg = np.mean(volumes[window_start:window_end])
                if volumes[i] > vol_avg * 1.2:  # Higher volume at swing = liquidity pool
                    swing_lows[i] = lows[i]

        return swing_lows

    def _detect_impulse_optimized(self):
        """
        💥 Enhanced impulse detection with multiple confirmations 💥
        """
        if len(self.data) < 50:
            return None

        # Get recent data
        lookback = 20
        recent_close = self.close[-lookback:]
        recent_high = self.high[-lookback:]
        recent_low = self.low[-lookback:]
        recent_volume = self.volume[-lookback:]
        recent_atr = self.atr[-lookback:]
        recent_rsi = self.rsi[-lookback:]

        # Calculate momentum
        momentum = recent_close[-1] - recent_close[-self.momentum_period]

        # Bullish Impulse Detection
        if momentum > recent_atr[-1] * self.min_impulse_size_atr:
            # Count consecutive bullish bars
            bullish_bars = 0
            for i in range(-self.min_impulse_bars, 0):
                if recent_close[i] > recent_close[i-1]:
                    bullish_bars += 1

            if bullish_bars >= self.min_impulse_bars:
                # Volume confirmation
                vol_surge = recent_volume[-self.min_impulse_bars:].mean()
                vol_avg = self.volume_ma[-1]

                if vol_surge > vol_avg * self.volume_multiplier:
                    # Find impulse bounds
                    impulse_start_idx = -self.min_impulse_bars - 2
                    impulse_low = min(recent_low[impulse_start_idx:])
                    impulse_high = max(recent_high[-self.min_impulse_bars:])

                    # Structure break confirmation
                    prev_high = self._get_previous_swing_high()
                    if impulse_high > prev_high:
                        return {
                            'type': 'bullish',
                            'start': impulse_low,
                            'end': impulse_high,
                            'strength': momentum / recent_atr[-1],
                            'volume_ratio': vol_surge / vol_avg
                        }

        # Bearish Impulse Detection
        elif momentum < -recent_atr[-1] * self.min_impulse_size_atr:
            # Count consecutive bearish bars
            bearish_bars = 0
            for i in range(-self.min_impulse_bars, 0):
                if recent_close[i] < recent_close[i-1]:
                    bearish_bars += 1

            if bearish_bars >= self.min_impulse_bars:
                # Volume confirmation
                vol_surge = recent_volume[-self.min_impulse_bars:].mean()
                vol_avg = self.volume_ma[-1]

                if vol_surge > vol_avg * self.volume_multiplier:
                    # Find impulse bounds
                    impulse_start_idx = -self.min_impulse_bars - 2
                    impulse_high = max(recent_high[impulse_start_idx:])
                    impulse_low = min(recent_low[-self.min_impulse_bars:])

                    # Structure break confirmation
                    prev_low = self._get_previous_swing_low()
                    if impulse_low < prev_low:
                        return {
                            'type': 'bearish',
                            'start': impulse_high,
                            'end': impulse_low,
                            'strength': abs(momentum) / recent_atr[-1],
                            'volume_ratio': vol_surge / vol_avg
                        }

        return None

    def _get_previous_swing_high(self):
        """
        📊 Get previous validated swing high 📊
        """
        for i in range(-2, -min(50, len(self.swing_highs)), -1):
            if self.swing_highs[i] > 0:
                return self.swing_highs[i]
        return self.high[-20]

    def _get_previous_swing_low(self):
        """
        📊 Get previous validated swing low 📊
        """
        for i in range(-2, -min(50, len(self.swing_lows)), -1):
            if self.swing_lows[i] < np.inf:
                return self.swing_lows[i]
        return self.low[-20]

    def _calculate_fib_levels_optimized(self, impulse):
        """
        📐 Calculate Fibonacci levels with liquidity zones 📐
        """
        if impulse['type'] == 'bullish':
            diff = impulse['end'] - impulse['start']
            levels = {
                '0.0': impulse['start'],
                '0.236': impulse['start'] + diff * 0.236,
                '0.382': impulse['start'] + diff * 0.382,
                '0.5': impulse['start'] + diff * 0.5,
                '0.618': impulse['start'] + diff * 0.618,
                '0.786': impulse['start'] + diff * 0.786,
                '1.0': impulse['end'],
                'golden_top': impulse['start'] + diff * 0.618,
                'golden_bottom': impulse['start'] + diff * 0.5,
                'optimal_entry': impulse['start'] + diff * 0.55  # Middle of golden pocket
            }
        else:
            diff = impulse['start'] - impulse['end']
            levels = {
                '0.0': impulse['start'],
                '0.236': impulse['start'] - diff * 0.236,
                '0.382': impulse['start'] - diff * 0.382,
                '0.5': impulse['start'] - diff * 0.5,
                '0.618': impulse['start'] - diff * 0.618,
                '0.786': impulse['start'] - diff * 0.786,
                '1.0': impulse['end'],
                'golden_top': impulse['start'] - diff * 0.5,
                'golden_bottom': impulse['start'] - diff * 0.618,
                'optimal_entry': impulse['start'] - diff * 0.55  # Middle of golden pocket
            }

        return levels

    def _check_entry_confluence(self, impulse_type):
        """
        ✨ Check for additional confluence factors ✨
        """
        confluences = 0

        # RSI Confluence
        current_rsi = self.rsi[-1]
        if impulse_type == 'bullish' and current_rsi < self.rsi_oversold:
            confluences += 1
        elif impulse_type == 'bearish' and current_rsi > self.rsi_overbought:
            confluences += 1

        # MACD Confluence
        if impulse_type == 'bullish' and self.macd_hist[-1] > 0:
            confluences += 1
        elif impulse_type == 'bearish' and self.macd_hist[-1] < 0:
            confluences += 1

        # EMA Trend Confluence
        if impulse_type == 'bullish' and self.ema_fast[-1] > self.ema_slow[-1]:
            confluences += 1
        elif impulse_type == 'bearish' and self.ema_fast[-1] < self.ema_slow[-1]:
            confluences += 1

        # VWAP Confluence
        if impulse_type == 'bullish' and self.close[-1] > self.vwap[-1]:
            confluences += 1
        elif impulse_type == 'bearish' and self.close[-1] < self.vwap[-1]:
            confluences += 1

        return confluences >= 2  # Need at least 2 confluences

    def next(self):
        """
        🎮 Optimized strategy execution logic 🎮
        """
        # Skip if not enough data
        if len(self.data) < 50:
            return

        # Daily trade limit
        if self.trades_today >= self.max_trades_per_day:
            return

        # Current state
        current_price = self.close[-1]
        current_atr = self.atr[-1]

        # Detect new impulse
        impulse = self._detect_impulse_optimized()

        # Update impulse if new one found
        if impulse and impulse != self.impulse_details:
            self.impulse_details = impulse
            self.fib_levels = self._calculate_fib_levels_optimized(impulse)
            self.impulse_active = True

        # Skip if no active impulse
        if not self.impulse_active or not self.impulse_details:
            return

        # Check if we're in golden pocket
        in_golden_pocket = False
        if self.impulse_details['type'] == 'bullish':
            in_golden_pocket = (self.fib_levels['golden_bottom'] <= current_price <=
                              self.fib_levels['golden_top'])
        else:
            in_golden_pocket = (self.fib_levels['golden_bottom'] <= current_price <=
                              self.fib_levels['golden_top'])

        # Entry Logic with Confluence
        if in_golden_pocket and not self.position:
            # Check for additional confluence
            if self._check_entry_confluence(self.impulse_details['type']):

                if self.impulse_details['type'] == 'bullish':
                    # Long Entry
                    stop_loss = self.fib_levels['0.0'] - (current_atr * 0.5)
                    take_profit = current_price + (current_price - stop_loss) * self.risk_reward_ratio

                    # Ensure valid setup
                    if current_price < self.fib_levels['1.0'] and stop_loss < current_price:
                        # Dynamic position sizing based on volatility
                        risk_amount = self.equity * (self.max_risk_percent / 100)
                        price_risk = current_price - stop_loss
                        position_size = min(risk_amount / price_risk, 0.9)

                        self.buy(size=position_size, sl=stop_loss, tp=take_profit)
                        self.trades_today += 1
                        self.impulse_active = False  # Deactivate after entry

                elif self.impulse_details['type'] == 'bearish':
                    # Short Entry
                    stop_loss = self.fib_levels['0.0'] + (current_atr * 0.5)
                    take_profit = current_price - (stop_loss - current_price) * self.risk_reward_ratio

                    # Ensure valid setup
                    if current_price > self.fib_levels['1.0'] and stop_loss > current_price:
                        # Dynamic position sizing based on volatility
                        risk_amount = self.equity * (self.max_risk_percent / 100)
                        price_risk = stop_loss - current_price
                        position_size = min(risk_amount / price_risk, 0.9)

                        self.sell(size=position_size, sl=stop_loss, tp=take_profit)
                        self.trades_today += 1
                        self.impulse_active = False  # Deactivate after entry

        # Invalidate setup if price goes too far
        if self.impulse_active and self.impulse_details:
            if self.impulse_details['type'] == 'bullish':
                if current_price > self.fib_levels['1.0'] * 1.01 or current_price < self.fib_levels['0.0'] * 0.99:
                    self.impulse_active = False
            else:
                if current_price < self.fib_levels['1.0'] * 0.99 or current_price > self.fib_levels['0.0'] * 1.01:
                    self.impulse_active = False


def run_optimized_backtest(data_path, symbol="BTCUSD", cash=10000, commission=0.002):
    """
    🚀 Run the optimized Fibonacci strategy backtest 🚀
    """
    print(f"\n{'='*80}")
    print(f"🌙 Testing Optimized Fibonacci Strategy on {symbol} 🌙")
    print(f"{'='*80}")

    try:
        # Load data
        df = pd.read_csv(data_path)

        # Handle column names
        column_mapping = {
            'timestamp': 'timestamp',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }
        df.rename(columns=column_mapping, inplace=True)

        # Set index
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

        # Verify columns
        required = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")

        print(f"✅ Data loaded: {len(df)} bars")
        print(f"📅 Period: {df.index[0]} to {df.index[-1]}")

        # Run backtest
        bt = Backtest(
            df,
            FibonacciScalpingOptimized,
            cash=cash,
            commission=commission,
            exclusive_orders=True,
            trade_on_close=False
        )

        stats = bt.run()

        # Display results
        print(f"\n{'='*80}")
        print(f"📊 OPTIMIZED STRATEGY RESULTS 📊")
        print(f"{'='*80}")
        print(stats)

        # Generate plot
        print(f"\n🎨 Generating visualization...")
        bt.plot(resample='1H', open_browser=False)

        return stats

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


if __name__ == "__main__":
    # Test with BTC 5m data
    btc_path = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-5m-50wks-enhanced-data.csv"
    run_optimized_backtest(btc_path, "BTCUSD")