import pytest
from datetime import datetime
from strategies.ict_strategy.models.fvg import FVG, FVGType

def test_fvg_creation():
    """Test FVG object creation with all required fields"""
    fvg = FVG(
        type=FVGType.BULLISH,
        high=100.0,
        low=95.0,
        gap_high=99.0,
        gap_low=96.0,
        timestamp=datetime(2025, 1, 1, 12, 0),
        timeframe="1H",
        quality_score=85.0
    )

    assert fvg.type == FVGType.BULLISH
    assert fvg.high == 100.0
    assert fvg.low == 95.0
    assert fvg.gap_size == 3.0  # 99 - 96
    assert fvg.is_valid()

def test_fvg_invalidation():
    """Test FVG invalidation when price fills the gap"""
    fvg = FVG(
        type=FVGType.BULLISH,
        high=100.0,
        low=95.0,
        gap_high=99.0,
        gap_low=96.0,
        timestamp=datetime(2025, 1, 1, 12, 0),
        timeframe="1H",
        quality_score=85.0
    )

    # Price fills 50% of gap
    fvg.update_fill_percentage(97.5)
    assert fvg.fill_percentage == 50.0
    assert fvg.is_valid()  # Still valid at 50%

    # Price fills 100% of gap
    fvg.update_fill_percentage(96.0)
    assert fvg.fill_percentage == 100.0
    assert not fvg.is_valid()  # Invalidated

def test_bearish_fvg_invalidation():
    """Test bearish FVG invalidation when price fills the gap"""
    fvg = FVG(
        type=FVGType.BEARISH,
        high=105.0,
        low=100.0,
        gap_high=104.0,
        gap_low=101.0,
        timestamp=datetime(2025, 1, 1, 12, 0),
        timeframe="1H",
        quality_score=85.0
    )

    # Price fills 50% of gap
    fvg.update_fill_percentage(102.5)
    assert fvg.fill_percentage == 50.0
    assert fvg.is_valid()  # Still valid at 50%

    # Price fills 100% of gap
    fvg.update_fill_percentage(104.0)
    assert fvg.fill_percentage == 100.0
    assert not fvg.is_valid()  # Invalidated
