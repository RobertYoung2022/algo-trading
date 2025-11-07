"""
Swing Failure Pattern (SFP) Confirmation

SFP is a false breakout that traps traders on the wrong side,
creating liquidity for smart money.

Pattern Requirements:
1. Price wicks through key level
2. Fails to close beyond level
3. Reverses strongly (long wick relative to body)
4. Traps breakout traders
"""

import pandas as pd
from typing import Dict, Optional


def check_sfp(df: pd.DataFrame, direction: str, key_level: float) -> bool:
    """
    Check for Swing Failure Pattern (false breakout)

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'
        key_level: Price level to check for sweep

    Returns:
        True if SFP pattern is detected, False otherwise
    """
    if len(df) < 3:
        return False

    recent = df.tail(10)  # Check last 10 candles (increased from 5)
    buffer = 0.002  # 0.2% buffer for sweep detection

    for _, candle in recent.iterrows():
        if direction == 'bullish':
            # Bullish SFP: sweep below level, close above
            swept_below = candle['low'] < key_level * (1 - buffer)
            closed_above = candle['close'] > key_level

            if swept_below and closed_above:
                # Check for long lower wick
                body = abs(candle['close'] - candle['open'])
                lower_wick = min(candle['open'], candle['close']) - candle['low']

                # Wick should be at least 2x the body size
                if lower_wick > body * 2:
                    return True

        else:
            # Bearish SFP: sweep above level, close below
            swept_above = candle['high'] > key_level * (1 + buffer)
            closed_below = candle['close'] < key_level

            if swept_above and closed_below:
                # Check for long upper wick
                body = abs(candle['close'] - candle['open'])
                upper_wick = candle['high'] - max(candle['open'], candle['close'])

                # Wick should be at least 2x the body size
                if upper_wick > body * 2:
                    return True

    return False


def identify_sfp_detailed(df: pd.DataFrame, direction: str, key_level: float) -> Optional[Dict]:
    """
    Identify SFP with detailed information

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'
        key_level: Price level to check

    Returns:
        Dict with SFP details or None if not found
    """
    if len(df) < 3:
        return None

    recent = df.tail(5)
    buffer = 0.002

    for idx, candle in recent.iterrows():
        if direction == 'bullish':
            if (candle['low'] < key_level * (1 - buffer) and
                candle['close'] > key_level):

                body = abs(candle['close'] - candle['open'])
                lower_wick = min(candle['open'], candle['close']) - candle['low']
                wick_ratio = lower_wick / body if body > 0 else 0

                if lower_wick > body * 2:
                    return {
                        'type': 'bullish_sfp',
                        'swept_level': key_level,
                        'reversal_point': candle['low'],
                        'entry': candle['close'],
                        'trapped_direction': 'shorts',
                        'wick_ratio': round(wick_ratio, 2),
                        'strength': 'strong' if wick_ratio > 3 else 'medium'
                    }

        else:
            if (candle['high'] > key_level * (1 + buffer) and
                candle['close'] < key_level):

                body = abs(candle['close'] - candle['open'])
                upper_wick = candle['high'] - max(candle['open'], candle['close'])
                wick_ratio = upper_wick / body if body > 0 else 0

                if upper_wick > body * 2:
                    return {
                        'type': 'bearish_sfp',
                        'swept_level': key_level,
                        'reversal_point': candle['high'],
                        'entry': candle['close'],
                        'trapped_direction': 'longs',
                        'wick_ratio': round(wick_ratio, 2),
                        'strength': 'strong' if wick_ratio > 3 else 'medium'
                    }

    return None
