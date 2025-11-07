"""
Market Structure Shift (MSS) Confirmation Pattern

MSS is a break of previous swing point confirming trend change.

Pattern Requirements:
1. Identify recent swing high/low
2. Price breaks structure (close beyond swing point)
3. Confirms with displacement (strong move)
4. Retest optional but preferred
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List


def check_mss(df: pd.DataFrame, direction: str) -> bool:
    """
    Check for Market Structure Shift pattern

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'

    Returns:
        True if MSS pattern is detected, False otherwise
    """
    if len(df) < 30:
        return False

    # Find swing points using rolling windows
    window = 5
    highs = df['high'].rolling(window, center=True).max()
    lows = df['low'].rolling(window, center=True).min()

    current_price = df.iloc[-1]['close']

    if direction == 'bullish':
        # Bullish MSS: break above recent swing high
        # Find recent swing highs (local maxima)
        recent_highs = []
        for i in range(len(df) - 10, len(df) - 5):
            if i >= window and i < len(df) - window:
                if df.iloc[i]['high'] == highs.iloc[i]:
                    recent_highs.append(df.iloc[i]['high'])

        if recent_highs:
            prev_high = max(recent_highs[-3:]) if len(recent_highs) >= 3 else recent_highs[-1]
            return current_price > prev_high

    else:
        # Bearish MSS: break below recent swing low
        recent_lows = []
        for i in range(len(df) - 10, len(df) - 5):
            if i >= window and i < len(df) - window:
                if df.iloc[i]['low'] == lows.iloc[i]:
                    recent_lows.append(df.iloc[i]['low'])

        if recent_lows:
            prev_low = min(recent_lows[-3:]) if len(recent_lows) >= 3 else recent_lows[-1]
            return current_price < prev_low

    return False


def identify_swing_points(df: pd.DataFrame, lookback: int = 20) -> Dict[str, List]:
    """
    Identify swing highs and lows in the data

    Args:
        df: DataFrame with OHLC data
        lookback: Number of candles to analyze

    Returns:
        Dict with 'swing_highs' and 'swing_lows' lists
    """
    swing_highs = []
    swing_lows = []

    data = df.tail(lookback) if len(df) > lookback else df

    for i in range(2, len(data) - 2):
        candle = data.iloc[i]

        # Swing high: higher than 2 before and 2 after
        is_swing_high = (
            candle['high'] > data.iloc[i-1]['high'] and
            candle['high'] > data.iloc[i-2]['high'] and
            candle['high'] > data.iloc[i+1]['high'] and
            candle['high'] > data.iloc[i+2]['high']
        )

        if is_swing_high:
            swing_highs.append({
                'index': i,
                'price': candle['high'],
                'timestamp': candle.name if hasattr(candle, 'name') else i
            })

        # Swing low: lower than 2 before and 2 after
        is_swing_low = (
            candle['low'] < data.iloc[i-1]['low'] and
            candle['low'] < data.iloc[i-2]['low'] and
            candle['low'] < data.iloc[i+1]['low'] and
            candle['low'] < data.iloc[i+2]['low']
        )

        if is_swing_low:
            swing_lows.append({
                'index': i,
                'price': candle['low'],
                'timestamp': candle.name if hasattr(candle, 'name') else i
            })

    return {
        'swing_highs': swing_highs,
        'swing_lows': swing_lows
    }


def check_displacement(df: pd.DataFrame, threshold: float = 0.015) -> bool:
    """
    Check if recent move shows displacement (strong institutional move)

    Args:
        df: DataFrame with OHLC data (last 5 candles)
        threshold: Minimum price change percentage for displacement

    Returns:
        True if displacement is detected
    """
    if len(df) < 3:
        return False

    recent = df.tail(5)

    # Calculate average range of recent candles
    avg_range = recent.apply(lambda x: abs(x['high'] - x['low']), axis=1).mean()

    # Check if any recent candle is significantly larger
    for _, candle in recent.iterrows():
        candle_range = abs(candle['high'] - candle['low'])
        if candle_range > avg_range * 1.5:  # 50% larger than average
            return True

    # Also check for strong directional move
    price_change = abs(recent.iloc[-1]['close'] - recent.iloc[0]['open']) / recent.iloc[0]['open']
    return price_change > threshold


def identify_mss_detailed(df: pd.DataFrame, direction: str) -> Optional[Dict]:
    """
    Identify MSS with detailed information

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'

    Returns:
        Dict with MSS details or None if not found
    """
    if len(df) < 20:
        return None

    swing_points = identify_swing_points(df)
    current_price = df.iloc[-1]['close']
    has_displacement = check_displacement(df.tail(5))

    if direction == 'bullish':
        if swing_points['swing_highs']:
            recent_high = swing_points['swing_highs'][-1]
            if current_price > recent_high['price']:
                return {
                    'type': 'bullish_mss',
                    'broken_level': recent_high['price'],
                    'current_price': current_price,
                    'displacement': has_displacement,
                    'strength': 'strong' if has_displacement else 'medium',
                    'swing_index': recent_high['index']
                }
    else:
        if swing_points['swing_lows']:
            recent_low = swing_points['swing_lows'][-1]
            if current_price < recent_low['price']:
                return {
                    'type': 'bearish_mss',
                    'broken_level': recent_low['price'],
                    'current_price': current_price,
                    'displacement': has_displacement,
                    'strength': 'strong' if has_displacement else 'medium',
                    'swing_index': recent_low['index']
                }

    return None
