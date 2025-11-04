import pytest
import pandas as pd
from strategies.ict_strategy.detectors.market_structure import MarketStructureDetector

def test_detect_bullish_structure_shift():
    """Test detection of bullish market structure shift (higher lows, higher highs)"""
    # Create uptrend with clear swing points
    # Need enough data for lookback=5 to work (at least 11 candles for each swing)

    dates = pd.date_range(start='2025-01-01', periods=50, freq='1h')

    # Build bullish structure: higher lows and higher highs
    # Swing low 1 around index 10, swing high 1 around index 20,
    # swing low 2 around index 30, swing high 2 around index 40
    prices = []

    # Initial decline to first low
    prices.extend([100, 98, 96, 94, 92, 90, 88, 86, 84, 82, 80])  # Low at 80 (index 10)

    # Rally to first high
    prices.extend([82, 85, 88, 91, 94, 97, 100, 103, 106, 109, 110])  # High at 110 (index 21)

    # Pullback to higher low
    prices.extend([108, 105, 102, 99, 96, 93, 90, 88, 87, 86, 87])  # Higher low at 86 (index 32)

    # Rally to higher high
    prices.extend([87, 90, 94, 98, 102, 106, 110, 114, 118, 122, 125])  # Higher high at 125 (index 43)

    # Minor pullback
    prices.extend([123, 121, 120, 119, 118, 117])

    data = pd.DataFrame({
        'open': prices,
        'high': [p + 1 for p in prices],
        'low': [p - 1 for p in prices],
        'close': prices,
        'volume': [1000] * 50
    }, index=dates)

    detector = MarketStructureDetector()
    structure = detector.analyze(data)

    assert structure.trend == 'bullish'
    assert structure.last_higher_low is not None
    assert structure.last_higher_high is not None

def test_detect_bearish_structure_shift():
    """Test detection of bearish market structure shift (lower highs, lower lows)"""
    # Create downtrend with clear swing points
    # Need enough data for lookback=5 to work

    dates = pd.date_range(start='2025-01-01', periods=50, freq='1h')

    # Build bearish structure: lower highs and lower lows
    # Swing high 1 around index 10, swing low 1 around index 20,
    # swing high 2 around index 30, swing low 2 around index 40
    prices = []

    # Initial rally to first high
    prices.extend([100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120])  # High at 120 (index 10)

    # Decline to first low
    prices.extend([118, 115, 112, 109, 106, 103, 100, 97, 94, 91, 90])  # Low at 90 (index 21)

    # Rally to lower high
    prices.extend([92, 95, 98, 101, 104, 107, 110, 112, 113, 114, 115])  # Lower high at 115 (index 32)

    # Decline to lower low
    prices.extend([113, 110, 106, 102, 98, 94, 90, 86, 82, 78, 75])  # Lower low at 75 (index 43)

    # Minor rally
    prices.extend([77, 79, 80, 81, 82, 83])

    data = pd.DataFrame({
        'open': prices,
        'high': [p + 1 for p in prices],
        'low': [p - 1 for p in prices],
        'close': prices,
        'volume': [1000] * 50
    }, index=dates)

    detector = MarketStructureDetector()
    structure = detector.analyze(data)

    assert structure.trend == 'bearish'
    assert structure.last_lower_high is not None
    assert structure.last_lower_low is not None
