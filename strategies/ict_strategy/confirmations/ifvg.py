"""
Inverted Fair Value Gap (IFVG) Confirmation Pattern

An IFVG occurs when a fair value gap gets partially filled and then rejected,
showing institutional defense of the level.

Pattern Requirements:
1. Initial FVG forms (gap between candles)
2. Price retraces into gap
3. Gap acts as support (bullish) or resistance (bearish)
4. Price continues in original direction
"""

import pandas as pd
from typing import Dict, Optional


def check_ifvg(df: pd.DataFrame, direction: str) -> bool:
    """
    Check for Inverted Fair Value Gap pattern

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'

    Returns:
        True if IFVG pattern is detected, False otherwise
    """
    if len(df) < 5:
        return False

    recent = df.tail(10)  # Check last 10 candles for pattern

    for i in range(2, len(recent)):
        current = recent.iloc[i]
        prev_prev = recent.iloc[i-2]

        if direction == 'bullish':
            # Bullish IFVG: gap up that acts as support
            if prev_prev['high'] < current['low']:
                fvg_top = current['low']
                fvg_bottom = prev_prev['high']
                fvg_mid = (fvg_top + fvg_bottom) / 2

                # Check if price returned to gap in subsequent candles
                for j in range(i, len(recent)):
                    test_candle = recent.iloc[j]

                    # Price touched midpoint or lower
                    if test_candle['low'] <= fvg_mid:
                        # But closed above bottom (gap held as support)
                        if test_candle['close'] > fvg_bottom:
                            # Check for continuation (next candle higher)
                            if j + 1 < len(recent):
                                next_candle = recent.iloc[j + 1]
                                if next_candle['close'] > test_candle['high']:
                                    return True
        else:
            # Bearish IFVG: gap down that acts as resistance
            if prev_prev['low'] > current['high']:
                fvg_bottom = current['high']
                fvg_top = prev_prev['low']
                fvg_mid = (fvg_top + fvg_bottom) / 2

                # Check if price returned to gap
                for j in range(i, len(recent)):
                    test_candle = recent.iloc[j]

                    # Price touched midpoint or higher
                    if test_candle['high'] >= fvg_mid:
                        # But closed below top (gap held as resistance)
                        if test_candle['close'] < fvg_top:
                            # Check for continuation (next candle lower)
                            if j + 1 < len(recent):
                                next_candle = recent.iloc[j + 1]
                                if next_candle['close'] < test_candle['low']:
                                    return True

    return False


def identify_ifvg_detailed(df: pd.DataFrame, direction: str) -> Optional[Dict]:
    """
    Identify IFVG with detailed information

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'

    Returns:
        Dict with IFVG details or None if not found
    """
    if len(df) < 5:
        return None

    recent = df.tail(10)

    for i in range(2, len(recent)):
        current = recent.iloc[i]
        prev_prev = recent.iloc[i-2]

        if direction == 'bullish':
            if prev_prev['high'] < current['low']:
                fvg_top = current['low']
                fvg_bottom = prev_prev['high']
                fvg_mid = (fvg_top + fvg_bottom) / 2

                # Check for retest and hold
                for j in range(i, len(recent)):
                    test_candle = recent.iloc[j]

                    if test_candle['low'] <= fvg_mid and test_candle['close'] > fvg_bottom:
                        if j + 1 < len(recent):
                            next_candle = recent.iloc[j + 1]
                            if next_candle['close'] > test_candle['high']:
                                return {
                                    'confirmed': True,
                                    'type': 'bullish_ifvg',
                                    'entry_zone': (fvg_bottom, fvg_mid),
                                    'fvg_top': fvg_top,
                                    'fvg_bottom': fvg_bottom,
                                    'fvg_mid': fvg_mid,
                                    'pattern_strength': 'strong',
                                    'retest_index': j
                                }
        else:
            if prev_prev['low'] > current['high']:
                fvg_bottom = current['high']
                fvg_top = prev_prev['low']
                fvg_mid = (fvg_top + fvg_bottom) / 2

                # Check for retest and hold
                for j in range(i, len(recent)):
                    test_candle = recent.iloc[j]

                    if test_candle['high'] >= fvg_mid and test_candle['close'] < fvg_top:
                        if j + 1 < len(recent):
                            next_candle = recent.iloc[j + 1]
                            if next_candle['close'] < test_candle['low']:
                                return {
                                    'confirmed': True,
                                    'type': 'bearish_ifvg',
                                    'entry_zone': (fvg_mid, fvg_top),
                                    'fvg_top': fvg_top,
                                    'fvg_bottom': fvg_bottom,
                                    'fvg_mid': fvg_mid,
                                    'pattern_strength': 'strong',
                                    'retest_index': j
                                }

    return None
