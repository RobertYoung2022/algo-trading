"""
Change in State of Delivery (CISD) Confirmation Pattern

CISD is an aggressive engulfing pattern showing institutional commitment
to a direction change.

Pattern Requirements:
1. Complete engulfing of previous candle body
2. Opposite color candles
3. Current body > 1.5x previous body (shows commitment)
4. Close near extreme (shows conviction)
"""

import pandas as pd
from typing import Dict, Optional


def check_cisd(df: pd.DataFrame, direction: str) -> bool:
    """
    Check for Change in State of Delivery (Engulfing) pattern

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'

    Returns:
        True if CISD pattern is detected, False otherwise
    """
    if len(df) < 2:
        return False

    current = df.iloc[-1]
    previous = df.iloc[-2]

    # Calculate body sizes
    current_body = abs(current['close'] - current['open'])
    prev_body = abs(previous['close'] - previous['open'])

    if direction == 'bullish':
        # Bullish engulfing
        is_current_green = current['close'] > current['open']
        is_prev_red = previous['close'] < previous['open']
        engulfs_low = current['open'] <= previous['close']
        engulfs_high = current['close'] >= previous['open']
        significant_body = current_body > prev_body * 1.1  # Changed from 1.2 to 1.1 for increased signal frequency

        return (is_current_green and is_prev_red and engulfs_low and
                engulfs_high and significant_body)
    else:
        # Bearish engulfing
        is_current_red = current['close'] < current['open']
        is_prev_green = previous['close'] > previous['open']
        engulfs_high = current['open'] >= previous['close']
        engulfs_low = current['close'] <= previous['open']
        significant_body = current_body > prev_body * 1.1  # Changed from 1.2 to 1.1 for increased signal frequency

        return (is_current_red and is_prev_green and engulfs_high and
                engulfs_low and significant_body)


def identify_cisd_detailed(df: pd.DataFrame, direction: str) -> Optional[Dict]:
    """
    Identify CISD with detailed information including conviction check

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'

    Returns:
        Dict with CISD details or None if not found
    """
    if len(df) < 2:
        return None

    current = df.iloc[-1]
    previous = df.iloc[-2]

    current_body = abs(current['close'] - current['open'])
    prev_body = abs(previous['close'] - previous['open'])

    if direction == 'bullish':
        if (current['close'] > current['open'] and
            previous['close'] < previous['open'] and
            current['open'] <= previous['close'] and
            current['close'] >= previous['open'] and
            current_body > prev_body * 1.2):

            # Check close near high (conviction)
            range_size = current['high'] - current['low']
            if range_size > 0:
                close_position = (current['close'] - current['low']) / range_size

                # Determine strength based on body ratio and close position
                body_ratio = current_body / prev_body if prev_body > 0 else 2.0
                strength = 'strong' if body_ratio > 2.0 and close_position > 0.8 else 'medium'

                return {
                    'type': 'bullish_cisd',
                    'strength': strength,
                    'entry': current['close'],
                    'body_ratio': round(body_ratio, 2),
                    'close_position': round(close_position, 2),
                    'conviction': 'high' if close_position > 0.8 else 'medium'
                }
    else:
        if (current['close'] < current['open'] and
            previous['close'] > previous['open'] and
            current['open'] >= previous['close'] and
            current['close'] <= previous['open'] and
            current_body > prev_body * 1.2):

            # Check close near low (conviction)
            range_size = current['high'] - current['low']
            if range_size > 0:
                close_position = (current['high'] - current['close']) / range_size

                body_ratio = current_body / prev_body if prev_body > 0 else 2.0
                strength = 'strong' if body_ratio > 2.0 and close_position > 0.8 else 'medium'

                return {
                    'type': 'bearish_cisd',
                    'strength': strength,
                    'entry': current['close'],
                    'body_ratio': round(body_ratio, 2),
                    'close_position': round(close_position, 2),
                    'conviction': 'high' if close_position > 0.8 else 'medium'
                }

    return None
