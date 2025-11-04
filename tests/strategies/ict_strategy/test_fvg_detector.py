import pytest
import pandas as pd
from datetime import datetime, timedelta
from strategies.ict_strategy.detectors.fvg_detector import FVGDetector
from strategies.ict_strategy.models.fvg import FVGType

def test_detect_bullish_fvg():
    """Test detection of bullish FVG (gap up)"""
    # Create 3-candle bullish FVG pattern
    # Candle 1: Low at 100
    # Candle 2: Big up move (doesn't matter)
    # Candle 3: Open/Low at 105 (gap above candle 1 high of 101)

    dates = pd.date_range(start='2025-01-01', periods=10, freq='1H')
    data = pd.DataFrame({
        'open':  [100, 102, 105, 106, 107, 108, 109, 110, 111, 112],
        'high':  [101, 104, 106, 107, 108, 109, 110, 111, 112, 113],
        'low':   [99,  102, 105, 106, 107, 108, 109, 110, 111, 112],
        'close': [100, 103, 106, 106, 107, 108, 109, 110, 111, 112],
        'volume': [1000] * 10
    }, index=dates)

    detector = FVGDetector(timeframe='1H', min_gap_size=1.0)
    fvgs = detector.detect(data)

    assert len(fvgs) >= 1
    bullish_fvgs = [f for f in fvgs if f.type == FVGType.BULLISH]
    assert len(bullish_fvgs) >= 1

    fvg = bullish_fvgs[0]
    assert fvg.gap_low == 101.0  # Candle 1 high
    assert fvg.gap_high == 105.0  # Candle 3 low
    assert fvg.gap_size >= 1.0

def test_detect_bearish_fvg():
    """Test detection of bearish FVG (gap down)"""
    # Candle 1: High at 105
    # Candle 2: Big down move
    # Candle 3: High at 101 (gap below candle 1 low of 104)

    dates = pd.date_range(start='2025-01-01', periods=10, freq='1H')
    data = pd.DataFrame({
        'open':  [105, 103, 100, 99, 98, 97, 96, 95, 94, 93],
        'high':  [106, 103, 101, 99, 98, 97, 96, 95, 94, 93],
        'low':   [104, 100, 99,  98, 97, 96, 95, 94, 93, 92],
        'close': [105, 100, 100, 99, 98, 97, 96, 95, 94, 93],
        'volume': [1000] * 10
    }, index=dates)

    detector = FVGDetector(timeframe='1H', min_gap_size=1.0)
    fvgs = detector.detect(data)

    assert len(fvgs) >= 1
    bearish_fvgs = [f for f in fvgs if f.type == FVGType.BEARISH]
    assert len(bearish_fvgs) >= 1

    fvg = bearish_fvgs[0]
    assert fvg.gap_high == 104.0  # Candle 1 low
    assert fvg.gap_low == 101.0   # Candle 3 high
    assert fvg.gap_size >= 1.0

def test_no_fvg_on_continuous_price():
    """Test that no FVG is detected when there's no gap"""
    dates = pd.date_range(start='2025-01-01', periods=10, freq='1H')
    # Overlapping candles - each candle's range overlaps with next
    # No gap between candle[0].high and candle[2].low
    data = pd.DataFrame({
        'open':  [100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5],
        'high':  [102, 102.5, 103, 103.5, 104, 104.5, 105, 105.5, 106, 106.5],
        'low':   [99,  99.5,  100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5],
        'close': [101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5, 105, 105.5],
        'volume': [1000] * 10
    }, index=dates)

    detector = FVGDetector(timeframe='1H', min_gap_size=1.0)
    fvgs = detector.detect(data)

    assert len(fvgs) == 0
