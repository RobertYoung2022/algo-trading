"""
ICT Confirmation Patterns

This package contains standalone modules for each ICT confirmation pattern.
Each pattern validates that smart money has taken action at key levels.

Available Confirmations:
- IFVG: Inverted Fair Value Gap
- CISD: Change in State of Delivery
- SFP: Swing Failure Pattern
- MSS: Market Structure Shift
- OB: Order Block

Usage:
    from strategies.ict_strategy.confirmations import check_all_confirmations

    confirmations = check_all_confirmations(df, direction='bullish', key_level=50000)
"""

from .ifvg import check_ifvg
from .cisd import check_cisd
from .sfp import check_sfp
from .mss import check_mss
from .order_block import check_order_block


def check_all_confirmations(df, direction='bullish', key_level=None):
    """
    Check all confirmation patterns

    Args:
        df: DataFrame with OHLC data
        direction: 'bullish' or 'bearish'
        key_level: Price level to check (optional for some patterns)

    Returns:
        List of confirmation pattern names that are present
    """
    confirmations = []

    # IFVG - needs at least 5 candles
    if len(df) >= 5 and check_ifvg(df, direction):
        confirmations.append('IFVG')

    # CISD - needs at least 2 candles
    if len(df) >= 2 and check_cisd(df, direction):
        confirmations.append('CISD')

    # SFP - needs key level and at least 3 candles
    if key_level is not None and len(df) >= 3 and check_sfp(df, direction, key_level):
        confirmations.append('SFP')

    # MSS - needs at least 20 candles for swing identification
    if len(df) >= 20 and check_mss(df, direction):
        confirmations.append('MSS')

    # OB - needs at least 10 candles
    if len(df) >= 10 and check_order_block(df, direction):
        confirmations.append('OB')

    return confirmations


__all__ = [
    'check_ifvg',
    'check_cisd',
    'check_sfp',
    'check_mss',
    'check_order_block',
    'check_all_confirmations'
]
