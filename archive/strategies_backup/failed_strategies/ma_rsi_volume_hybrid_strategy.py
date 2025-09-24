"""
🚀 MA-RSI-Volume Hybrid Strategy 🚀
=====================================
Advanced hybrid strategy combining Moving Average trend, RSI oversold conditions,
and Volume spike confirmation for high-probability entry signals.

Strategy Logic:
- ENTRY: ALL three signals must align:
  1. Price > 20-period Moving Average (uptrend confirmation)
  2. RSI < 30 (oversold condition for mean reversion)
  3. Volume > 1.2x average (20% spike indicating momentum)
- EXIT: Multiple conditions:
  1. RSI > 70 (overbought - take profits)
  2. Price < MA (trend reversal)
  3. Stop Loss: -3% from entry
  4. Take Profit: +6% from entry

Author: Bobby's Algo Trading System 🌙💫
Date: 2025-01-18
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover

# 🌙 Import modern risk management and validation
try:
    from trading_functions import (
        calculate_position_size,
        validate_trade_risk,
        check_drawdown_limits,
        DataQualityValidator,
        validate_data_source_quality
    )
    TRADING_FUNCTIONS_AVAILABLE = True
except ImportError:
    print("⚠️ Trading functions not available - using basic risk management")
    TRADING_FUNCTIONS_AVAILABLE = False

print("🚀 MA-RSI-Volume Hybrid Strategy Loading... 💫")

# 🎯 Strategy Parameters - Optimized for crypto markets
MA_PERIOD = 20              # Moving Average period for trend
RSI_PERIOD = 14             # RSI period for momentum
RSI_OVERSOLD = 35           # RSI oversold threshold (buy zone) - increased for more signals
RSI_OVERBOUGHT = 65         # RSI overbought threshold (sell zone) - decreased for earlier exits
VOLUME_SPIKE_MULTIPLIER = 1.1  # Volume must be 10% above average - reduced for more signals
VOLUME_MA_PERIOD = 20       # Period for volume moving average
TAKE_PROFIT_PERCENT = 5.0   # Take profit at 5% - more realistic
STOP_LOSS_PERCENT = 2.5     # Stop loss at 2.5% - tighter risk control

# 🛡️ Risk Management Parameters
RISK_PER_TRADE = 2.0        # Risk 2% of account per trade
MAX_DRAWDOWN = 20.0         # Maximum allowable drawdown %
MIN_TRADE_SIZE = 100        # Minimum trade size in USD
MAX_POSITION_SIZE = 0.30    # Maximum 30% of account per position
DEFAULT_POSITION_SIZE = 0.25  # Default to 25% of account


class MARSIVolumeHybridStrategy(Strategy):
    """
    🌙 MA-RSI-Volume Hybrid Strategy 🌙

    This strategy combines three powerful technical indicators to identify
    high-probability entry points with strong risk/reward profiles.

    Key Features:
    - Triple confirmation system reduces false signals
    - Volume spike validation ensures genuine momentum
    - RSI oversold condition captures mean reversion opportunities
    - MA trend filter ensures trading with the trend
    - Dynamic position sizing based on signal strength

    Ideal Market Conditions:
    - Trending markets with periodic pullbacks
    - High volume breakouts from consolidation
    - Mean reversion after oversold conditions
    """

    # 🎯 Strategy parameters (can be optimized)
    ma_period = MA_PERIOD
    rsi_period = RSI_PERIOD
    rsi_oversold = RSI_OVERSOLD
    rsi_overbought = RSI_OVERBOUGHT
    volume_spike = VOLUME_SPIKE_MULTIPLIER
    volume_ma_period = VOLUME_MA_PERIOD
    take_profit = TAKE_PROFIT_PERCENT / 100
    stop_loss = STOP_LOSS_PERCENT / 100

    def init(self):
        """
        🚀 Initialize all indicators and signals 🚀

        Sets up MA, RSI, and Volume indicators with proper validation
        """
        print("🌙 Initializing MA-RSI-Volume Hybrid indicators...")

        # 📊 Moving Average for trend direction
        self.ma = self.I(talib.SMA, self.data.Close, self.ma_period)

        # 📈 RSI for momentum and oversold/overbought conditions
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)

        # 📊 Volume analysis - calculate volume moving average
        self.volume_ma = self.I(talib.SMA, self.data.Volume, self.volume_ma_period)

        # 🎯 Signal tracking
        self.entry_price = None
        self.trade_count = 0
        self.signal_count = 0

        # 📊 Performance tracking
        self.perfect_signals = 0  # All 3 conditions met
        self.partial_signals = 0  # 2 conditions met
        self.volume_spikes = 0
        self.rsi_oversold_hits = 0
        self.ma_crossovers = 0

        print(f"✅ Indicators initialized - MA:{self.ma_period}, RSI:{self.rsi_period}, Vol MA:{self.volume_ma_period}")

    def next(self):
        """
        🎯 Main trading logic - Triple confirmation system 🎯

        Evaluates all three conditions and manages positions accordingly
        """

        # Skip if indicators not ready
        if len(self.data) < max(self.ma_period, self.rsi_period, self.volume_ma_period):
            return

        # Skip if any indicator is NaN
        if pd.isna(self.ma[-1]) or pd.isna(self.rsi[-1]) or pd.isna(self.volume_ma[-1]):
            return

        # 📊 Calculate current conditions
        current_price = self.data.Close[-1]
        current_volume = self.data.Volume[-1]
        current_rsi = self.rsi[-1]
        current_ma = self.ma[-1]
        current_vol_ma = self.volume_ma[-1]

        # 🎯 Check individual signals
        price_above_ma = current_price > current_ma
        rsi_oversold = current_rsi < self.rsi_oversold
        volume_spike = current_volume > (current_vol_ma * self.volume_spike) if current_vol_ma > 0 else False

        # Track signal occurrences for analysis
        if volume_spike:
            self.volume_spikes += 1
        if rsi_oversold:
            self.rsi_oversold_hits += 1
        if price_above_ma and len(self.data) > 1 and self.data.Close[-2] <= self.ma[-2]:
            self.ma_crossovers += 1

        # Count signal combinations
        signals_met = sum([price_above_ma, rsi_oversold, volume_spike])
        if signals_met == 3:
            self.perfect_signals += 1
        elif signals_met == 2:
            self.partial_signals += 1

        # 🚀 ENTRY LOGIC - ALL THREE CONDITIONS MUST BE TRUE
        if not self.position:
            if price_above_ma and rsi_oversold and volume_spike:
                # 💫 Perfect setup detected - enter position
                self.signal_count += 1

                # Calculate position size (default to 95% of equity)
                position_size = 0.95

                # Optional: Use dynamic position sizing if available
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        # Validate trade risk
                        risk_check = validate_trade_risk(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss=current_price * (1 - self.stop_loss),
                            position_size=self.equity * DEFAULT_POSITION_SIZE,
                            max_risk_pct=RISK_PER_TRADE
                        )
                        if risk_check['risk_acceptable']:
                            position_size = min(0.95, DEFAULT_POSITION_SIZE * 4)  # Scale up for strong signals
                    except:
                        pass

                # Enter long position
                self.buy(size=position_size)
                self.entry_price = current_price
                self.trade_count += 1

                # Log entry for debugging
                if self.trade_count <= 5:  # Log first 5 trades
                    print(f"   🎯 ENTRY #{self.trade_count}: Price={current_price:.2f}, "
                          f"MA={current_ma:.2f}, RSI={current_rsi:.1f}, "
                          f"Vol/Avg={current_volume/current_vol_ma:.2f}x")

        # 📉 EXIT LOGIC - Multiple exit conditions
        elif self.position:
            # Calculate current P&L
            pnl_pct = (current_price - self.entry_price) / self.entry_price if self.entry_price else 0

            # Exit conditions
            exit_rsi_overbought = current_rsi > self.rsi_overbought
            exit_price_below_ma = current_price < current_ma
            exit_take_profit = pnl_pct >= self.take_profit
            exit_stop_loss = pnl_pct <= -self.stop_loss

            if exit_rsi_overbought or exit_price_below_ma or exit_take_profit or exit_stop_loss:
                # Determine exit reason for logging
                if exit_stop_loss:
                    exit_reason = f"STOP LOSS ({pnl_pct*100:.1f}%)"
                elif exit_take_profit:
                    exit_reason = f"TAKE PROFIT ({pnl_pct*100:.1f}%)"
                elif exit_rsi_overbought:
                    exit_reason = f"RSI OVERBOUGHT ({current_rsi:.1f})"
                else:
                    exit_reason = f"PRICE < MA ({pnl_pct*100:.1f}%)"

                # Close position
                self.position.close()

                # Log exit for first few trades
                if self.trade_count <= 5:
                    print(f"   📈 EXIT #{self.trade_count}: {exit_reason}")

                self.entry_price = None


def validate_data_for_strategy(df):
    """
    🛡️ Validate data quality for strategy requirements 🛡️

    Ensures data has required columns and quality for strategy execution
    """
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # Check columns
    for col in required_columns:
        if col not in df.columns:
            return False, f"Missing required column: {col}"

    # Check data length
    min_length = max(MA_PERIOD, RSI_PERIOD, VOLUME_MA_PERIOD) * 2
    if len(df) < min_length:
        return False, f"Insufficient data: {len(df)} rows (need {min_length})"

    # Check for volume data
    if df['Volume'].sum() == 0:
        return False, "No volume data available"

    # Check for NaN values in critical columns
    critical_nans = df[required_columns].isna().sum().sum()
    if critical_nans > len(df) * 0.01:  # Allow max 1% NaN
        return False, f"Too many NaN values: {critical_nans}"

    return True, "Data validation passed"


def analyze_strategy_signals(df, strategy_params=None):
    """
    📊 Analyze potential signals without running full backtest 📊

    Quick analysis to check signal frequency and distribution
    """
    if strategy_params is None:
        strategy_params = {
            'ma_period': MA_PERIOD,
            'rsi_period': RSI_PERIOD,
            'rsi_oversold': RSI_OVERSOLD,
            'volume_spike': VOLUME_SPIKE_MULTIPLIER,
            'volume_ma_period': VOLUME_MA_PERIOD
        }

    # Calculate indicators
    df['MA'] = talib.SMA(df['Close'], strategy_params['ma_period'])
    df['RSI'] = talib.RSI(df['Close'], strategy_params['rsi_period'])
    df['Volume_MA'] = talib.SMA(df['Volume'], strategy_params['volume_ma_period'])

    # Check conditions
    df['Price_Above_MA'] = df['Close'] > df['MA']
    df['RSI_Oversold'] = df['RSI'] < strategy_params['rsi_oversold']
    df['Volume_Spike'] = df['Volume'] > (df['Volume_MA'] * strategy_params['volume_spike'])

    # Perfect signals (all 3 conditions)
    df['Perfect_Signal'] = df['Price_Above_MA'] & df['RSI_Oversold'] & df['Volume_Spike']

    # Analysis results
    total_bars = len(df.dropna())
    perfect_signals = df['Perfect_Signal'].sum()

    results = {
        'total_bars': total_bars,
        'perfect_signals': perfect_signals,
        'signal_frequency': perfect_signals / total_bars * 100 if total_bars > 0 else 0,
        'price_above_ma_pct': df['Price_Above_MA'].sum() / total_bars * 100,
        'rsi_oversold_pct': df['RSI_Oversold'].sum() / total_bars * 100,
        'volume_spike_pct': df['Volume_Spike'].sum() / total_bars * 100
    }

    return results


# 🌙💫🚀 Strategy ready for comprehensive multi-asset testing 🌙💫🚀
print("✅ MA-RSI-Volume Hybrid Strategy loaded successfully!")