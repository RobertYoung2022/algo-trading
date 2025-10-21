"""
🛠️ ONE CANDLE STRATEGY UTILITY FUNCTIONS 🛠️
===========================================
Helper functions for the One Candle Is All You Need strategy.
Includes FVG detection, pattern recognition, and data analysis tools.

Author: Bobby's Algo Fun 💫
"""

import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import talib

# ============================================================
# FAIR VALUE GAP DETECTION 🕳️
# ============================================================

def identify_fair_value_gaps(data, min_gap_percent=0.1, lookback_bars=20):
    """
    🕳️ Identify all Fair Value Gaps (FVGs) in the data

    Args:
        data: DataFrame with OHLC data
        min_gap_percent: Minimum gap size as percentage (default 0.1%)
        lookback_bars: Number of bars to look back (default 20)

    Returns:
        tuple: (bullish_fvgs, bearish_fvgs) - Lists of FVG dictionaries
    """
    bullish_fvgs = []
    bearish_fvgs = []

    # Need at least 3 bars to identify FVG
    if len(data) < lookback_bars + 3:
        return bullish_fvgs, bearish_fvgs

    # Look for FVGs in recent bars
    for i in range(len(data) - lookback_bars, len(data) - 2):
        # Get three consecutive candles
        candle1 = data.iloc[i-2]
        candle2 = data.iloc[i-1]
        candle3 = data.iloc[i]

        # Check for Bullish FVG (gap up)
        if candle3['Low'] > candle1['High']:
            gap_size = (candle3['Low'] - candle1['High']) / candle1['High']
            if gap_size >= min_gap_percent:
                bullish_fvgs.append({
                    'timestamp': data.index[i],
                    'gap_high': candle3['Low'],
                    'gap_low': candle1['High'],
                    'gap_size_percent': gap_size * 100,
                    'candle1_idx': i-2,
                    'candle3_idx': i
                })

        # Check for Bearish FVG (gap down)
        elif candle3['High'] < candle1['Low']:
            gap_size = (candle1['Low'] - candle3['High']) / candle3['High']
            if gap_size >= min_gap_percent:
                bearish_fvgs.append({
                    'timestamp': data.index[i],
                    'gap_high': candle1['Low'],
                    'gap_low': candle3['High'],
                    'gap_size_percent': gap_size * 100,
                    'candle1_idx': i-2,
                    'candle3_idx': i
                })

    return bullish_fvgs, bearish_fvgs


# ============================================================
# PATTERN RECOGNITION 🕯️
# ============================================================

def detect_engulfing_pattern(data):
    """
    🕯️ Detect engulfing patterns in the last two candles

    Args:
        data: DataFrame with OHLC data

    Returns:
        str: 'bullish', 'bearish', or None
    """
    if len(data) < 2:
        return None

    # Previous and current candle
    prev = data.iloc[-2]
    curr = data.iloc[-1]

    # Bullish engulfing
    if (prev['Close'] < prev['Open'] and  # Previous was bearish
        curr['Close'] > curr['Open'] and  # Current is bullish
        curr['Open'] <= prev['Close'] and  # Opens at or below prev close
        curr['Close'] >= prev['Open'] and  # Closes above prev open
        curr['High'] > prev['High'] and   # Higher high
        curr['Low'] < prev['Low']):        # Lower low
        return 'bullish'

    # Bearish engulfing
    if (prev['Close'] > prev['Open'] and  # Previous was bullish
        curr['Close'] < curr['Open'] and  # Current is bearish
        curr['Open'] >= prev['Close'] and  # Opens at or above prev close
        curr['Close'] <= prev['Open'] and  # Closes below prev open
        curr['High'] > prev['High'] and   # Higher high
        curr['Low'] < prev['Low']):        # Lower low
        return 'bearish'

    return None


def detect_pin_bar(data, min_wick_ratio=2.0):
    """
    📍 Detect pin bar (hammer/shooting star) patterns

    Args:
        data: DataFrame with OHLC data
        min_wick_ratio: Minimum ratio of wick to body (default 2.0)

    Returns:
        str: 'bullish_pin', 'bearish_pin', or None
    """
    if len(data) < 1:
        return None

    candle = data.iloc[-1]
    body = abs(candle['Close'] - candle['Open'])
    upper_wick = candle['High'] - max(candle['Close'], candle['Open'])
    lower_wick = min(candle['Close'], candle['Open']) - candle['Low']

    # Bullish pin bar (hammer) - long lower wick
    if lower_wick > body * min_wick_ratio and upper_wick < body:
        return 'bullish_pin'

    # Bearish pin bar (shooting star) - long upper wick
    if upper_wick > body * min_wick_ratio and lower_wick < body:
        return 'bearish_pin'

    return None


# ============================================================
# SESSION AND TIME ANALYSIS ⏰
# ============================================================

def identify_session_ranges(data, session_hours=1):
    """
    📊 Identify session ranges for crypto markets (24/7)

    Args:
        data: DataFrame with OHLC data
        session_hours: Number of hours to define as session (default 1)

    Returns:
        DataFrame with session high/low columns added
    """
    data = data.copy()

    # Create session identifier (daily sessions for crypto)
    data['session_date'] = data.index.date

    # Calculate session high/low for each day
    session_stats = data.groupby('session_date').agg({
        'High': 'max',
        'Low': 'min',
        'Open': 'first',
        'Close': 'last',
        'Volume': 'sum'
    })

    # Map back to original dataframe
    data['session_high'] = data['session_date'].map(session_stats['High'])
    data['session_low'] = data['session_date'].map(session_stats['Low'])

    return data


def is_price_in_fvg(price, fvg_list):
    """
    ✅ Check if price is within any FVG zone

    Args:
        price: Current price
        fvg_list: List of FVG dictionaries

    Returns:
        FVG dict if price is in zone, None otherwise
    """
    for fvg in fvg_list:
        if fvg['gap_low'] <= price <= fvg['gap_high']:
            return fvg
    return None


# ============================================================
# RISK MANAGEMENT CALCULATIONS 💰
# ============================================================

def calculate_position_size(account_value, risk_percent, stop_distance, entry_price):
    """
    💰 Calculate optimal position size based on risk management

    Args:
        account_value: Total account value
        risk_percent: Percentage of account to risk (e.g., 2.0 for 2%)
        stop_distance: Distance from entry to stop loss
        entry_price: Entry price for the trade

    Returns:
        float: Position size (number of shares/units)
    """
    if stop_distance <= 0:
        return 0

    risk_amount = account_value * (risk_percent / 100)
    position_value = risk_amount / stop_distance
    position_size = position_value / entry_price

    # Limit to 95% of account to avoid margin calls
    max_position = (account_value * 0.95) / entry_price

    return min(position_size, max_position)


def calculate_reward_to_risk(entry, stop_loss, take_profit):
    """
    📈 Calculate reward-to-risk ratio

    Args:
        entry: Entry price
        stop_loss: Stop loss price
        take_profit: Take profit price

    Returns:
        float: Reward-to-risk ratio
    """
    risk = abs(entry - stop_loss)
    reward = abs(take_profit - entry)

    if risk == 0:
        return 0

    return reward / risk


# ============================================================
# VOLUME ANALYSIS 📊
# ============================================================

def analyze_volume_profile(data, lookback_bars=20):
    """
    📊 Analyze volume profile and identify high volume nodes

    Args:
        data: DataFrame with OHLC and Volume data
        lookback_bars: Number of bars to analyze

    Returns:
        dict: Volume analysis results
    """
    if len(data) < lookback_bars:
        return None

    recent_data = data.tail(lookback_bars)

    # Calculate volume statistics
    avg_volume = recent_data['Volume'].mean()
    volume_std = recent_data['Volume'].std()
    current_volume = data['Volume'].iloc[-1]

    # Identify volume spikes (> 2 standard deviations)
    volume_spike = current_volume > (avg_volume + 2 * volume_std)

    # Calculate volume-weighted average price (VWAP)
    vwap = ((recent_data['Close'] * recent_data['Volume']).sum() /
            recent_data['Volume'].sum())

    return {
        'avg_volume': avg_volume,
        'current_volume': current_volume,
        'volume_ratio': current_volume / avg_volume if avg_volume > 0 else 0,
        'volume_spike': volume_spike,
        'vwap': vwap
    }


# ============================================================
# MARKET STRUCTURE ANALYSIS 📈
# ============================================================

def identify_swing_points(data, lookback=5):
    """
    🏔️ Identify swing highs and lows in price action

    Args:
        data: DataFrame with OHLC data
        lookback: Number of bars to look back/forward for swings

    Returns:
        tuple: (swing_highs, swing_lows) - Lists of indices
    """
    swing_highs = []
    swing_lows = []

    for i in range(lookback, len(data) - lookback):
        # Check for swing high
        is_swing_high = True
        for j in range(i - lookback, i + lookback + 1):
            if j != i and data['High'].iloc[j] >= data['High'].iloc[i]:
                is_swing_high = False
                break

        if is_swing_high:
            swing_highs.append(i)

        # Check for swing low
        is_swing_low = True
        for j in range(i - lookback, i + lookback + 1):
            if j != i and data['Low'].iloc[j] <= data['Low'].iloc[i]:
                is_swing_low = False
                break

        if is_swing_low:
            swing_lows.append(i)

    return swing_highs, swing_lows


def calculate_market_structure(data, lookback=20):
    """
    📈 Determine market structure (trending/ranging)

    Args:
        data: DataFrame with OHLC data
        lookback: Number of bars to analyze

    Returns:
        dict: Market structure analysis
    """
    if len(data) < lookback:
        return None

    recent_data = data.tail(lookback)

    # Calculate trend using linear regression
    from scipy import stats
    x = np.arange(len(recent_data))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, recent_data['Close'])

    # Determine trend strength
    trend_strength = abs(r_value)

    # Calculate ATR for volatility
    atr = talib.ATR(recent_data['High'].values,
                    recent_data['Low'].values,
                    recent_data['Close'].values, 14)

    current_atr = atr[-1] if len(atr) > 0 else 0

    # Determine market state
    if trend_strength > 0.7:
        if slope > 0:
            market_state = 'strong_uptrend'
        else:
            market_state = 'strong_downtrend'
    elif trend_strength > 0.3:
        if slope > 0:
            market_state = 'weak_uptrend'
        else:
            market_state = 'weak_downtrend'
    else:
        market_state = 'ranging'

    return {
        'market_state': market_state,
        'trend_strength': trend_strength,
        'slope': slope,
        'atr': current_atr,
        'r_squared': r_value ** 2
    }


# ============================================================
# PERFORMANCE METRICS 📊
# ============================================================

def calculate_strategy_metrics(trades_df):
    """
    📊 Calculate comprehensive strategy performance metrics

    Args:
        trades_df: DataFrame with trade results

    Returns:
        dict: Performance metrics
    """
    if trades_df.empty:
        return {}

    # Basic metrics
    total_trades = len(trades_df)
    winning_trades = len(trades_df[trades_df['pnl'] > 0])
    losing_trades = len(trades_df[trades_df['pnl'] < 0])

    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

    # Profit metrics
    gross_profit = trades_df[trades_df['pnl'] > 0]['pnl'].sum()
    gross_loss = abs(trades_df[trades_df['pnl'] < 0]['pnl'].sum())
    net_profit = gross_profit - gross_loss

    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')

    # Average metrics
    avg_win = gross_profit / winning_trades if winning_trades > 0 else 0
    avg_loss = gross_loss / losing_trades if losing_trades > 0 else 0

    # Expectancy
    expectancy = (win_rate/100 * avg_win) - ((100-win_rate)/100 * avg_loss)

    # Maximum consecutive wins/losses
    trades_df['is_win'] = trades_df['pnl'] > 0
    max_consec_wins = trades_df['is_win'].groupby((~trades_df['is_win']).cumsum()).sum().max()
    max_consec_losses = (~trades_df['is_win']).groupby(trades_df['is_win'].cumsum()).sum().max()

    return {
        'total_trades': total_trades,
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': win_rate,
        'gross_profit': gross_profit,
        'gross_loss': gross_loss,
        'net_profit': net_profit,
        'profit_factor': profit_factor,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'expectancy': expectancy,
        'max_consecutive_wins': max_consec_wins,
        'max_consecutive_losses': max_consec_losses
    }


print("🛠️ One Candle Strategy Utils loaded successfully! 💫")