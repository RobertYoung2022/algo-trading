"""
Order Block (OB) Confirmation Pattern

An Order Block is the last opposing candle before a strong move,
showing institutional accumulation/distribution.

Pattern Requirements:
1. Down-close candle before up move (bullish OB)
2. Up-close candle before down move (bearish OB)
3. Followed by displacement (strong directional move)
4. Price respects zone on retest
"""

import pandas as pd
from typing import Dict, Optional


def check_order_block(df: pd.DataFrame, direction: str, move_threshold: float = 0.01) -> bool:
    """
    Check for Order Block pattern

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'
        move_threshold: Minimum price move percentage to qualify as displacement (default 1%, lowered from 2% for 1H timeframe)

    Returns:
        True if Order Block pattern is detected, False otherwise
    """
    if len(df) < 10:
        return False

    # Check recent candles for OB pattern
    for i in range(len(df) - 10, len(df) - 3):
        candle = df.iloc[i]

        # Calculate move after the candle
        future_candles = df.iloc[i+1:min(i+5, len(df))]
        if len(future_candles) < 3:
            continue

        # Price move from candle close to 3 candles later
        price_move = (future_candles.iloc[2]['close'] - candle['close']) / candle['close']

        if direction == 'bullish':
            # Bullish OB: bearish candle before upward displacement
            is_bearish_candle = candle['close'] < candle['open']

            if is_bearish_candle and price_move > move_threshold:
                # Check if there was displacement (strong move up)
                displacement_found = any(
                    (fc['close'] - fc['open']) / fc['open'] > move_threshold / 2
                    for _, fc in future_candles.iterrows()
                )

                if displacement_found:
                    return True

        else:
            # Bearish OB: bullish candle before downward displacement
            is_bullish_candle = candle['close'] > candle['open']

            if is_bullish_candle and price_move < -move_threshold:
                # Check if there was displacement (strong move down)
                displacement_found = any(
                    (fc['open'] - fc['close']) / fc['open'] > move_threshold / 2
                    for _, fc in future_candles.iterrows()
                )

                if displacement_found:
                    return True

    return False


def calculate_ob_strength(candle: pd.Series, move_size: float) -> str:
    """
    Calculate Order Block strength based on candle size and subsequent move

    Args:
        candle: The order block candle
        move_size: Size of the move after the OB (absolute percentage)

    Returns:
        'strong', 'medium', or 'weak'
    """
    body_size = abs(candle['close'] - candle['open'])
    total_range = candle['high'] - candle['low']

    # Body to range ratio
    body_ratio = body_size / total_range if total_range > 0 else 0

    # Strong OB: Large body + large subsequent move
    if body_ratio > 0.7 and move_size > 0.03:
        return 'strong'
    # Medium OB: Decent body or decent move
    elif body_ratio > 0.5 or move_size > 0.02:
        return 'medium'
    else:
        return 'weak'


def identify_order_block_detailed(df: pd.DataFrame, direction: str, move_threshold: float = 0.01) -> Optional[Dict]:
    """
    Identify Order Block with detailed information

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'
        move_threshold: Minimum price move percentage

    Returns:
        Dict with Order Block details or None if not found
    """
    if len(df) < 10:
        return None

    for i in range(len(df) - 10, len(df) - 3):
        candle = df.iloc[i]
        future_candles = df.iloc[i+1:min(i+5, len(df))]

        if len(future_candles) < 3:
            continue

        price_move = (future_candles.iloc[2]['close'] - candle['close']) / candle['close']

        if direction == 'bullish':
            is_bearish_candle = candle['close'] < candle['open']

            if is_bearish_candle and price_move > move_threshold:
                displacement_candles = [
                    fc for _, fc in future_candles.iterrows()
                    if (fc['close'] - fc['open']) / fc['open'] > move_threshold / 2
                ]

                if displacement_candles:
                    strength = calculate_ob_strength(candle, abs(price_move))

                    return {
                        'type': 'bullish_ob',
                        'zone': (candle['low'], candle['high']),
                        'zone_midpoint': (candle['low'] + candle['high']) / 2,
                        'open': candle['open'],
                        'close': candle['close'],
                        'strength': strength,
                        'displacement_size': round(price_move * 100, 2),
                        'candle_index': i
                    }

        else:
            is_bullish_candle = candle['close'] > candle['open']

            if is_bullish_candle and price_move < -move_threshold:
                displacement_candles = [
                    fc for _, fc in future_candles.iterrows()
                    if (fc['open'] - fc['close']) / fc['open'] > move_threshold / 2
                ]

                if displacement_candles:
                    strength = calculate_ob_strength(candle, abs(price_move))

                    return {
                        'type': 'bearish_ob',
                        'zone': (candle['low'], candle['high']),
                        'zone_midpoint': (candle['low'] + candle['high']) / 2,
                        'open': candle['open'],
                        'close': candle['close'],
                        'strength': strength,
                        'displacement_size': round(abs(price_move) * 100, 2),
                        'candle_index': i
                    }

    return None
