import pytest
import pandas as pd
from strategies.ict_strategy.detectors.liquidity_detector import LiquidityDetector
from strategies.ict_strategy.models.liquidity_pool import LiquidityType

def test_detect_liquidity_pools():
    """Test detection of liquidity pools at highs and lows"""
    # Create data with clear swing highs and lows
    dates = pd.date_range(start='2025-01-01', periods=20, freq='1h')
    prices = [100, 102, 105, 103, 101,  # Swing high at 105
              99, 97, 95, 97, 99,        # Swing low at 95
              102, 105, 108, 106, 104,   # Swing high at 108
              102, 100, 98, 100, 102]    # Swing low at 98

    data = pd.DataFrame({
        'open': prices,
        'high': [p + 1 for p in prices],
        'low': [p - 1 for p in prices],
        'close': prices,
        'volume': [1000] * 20
    }, index=dates)

    detector = LiquidityDetector(lookback=3)
    pools = detector.detect(data)

    assert len(pools) >= 2  # At least one high and one low

    highs = [p for p in pools if p.type == LiquidityType.HIGH]
    lows = [p for p in pools if p.type == LiquidityType.LOW]

    assert len(highs) >= 1
    assert len(lows) >= 1

    # Check that highest pool is around 108
    highest_pool = max(highs, key=lambda p: p.price)
    assert 107 <= highest_pool.price <= 109

def test_liquidity_sweep():
    """Test detection of liquidity sweep (false breakout)"""
    detector = LiquidityDetector(lookback=2)

    # Create a liquidity pool at 105
    dates = pd.date_range(start='2025-01-01', periods=10, freq='1h')
    data = pd.DataFrame({
        'open':  [100, 102, 105, 103, 101, 102, 103, 106, 104, 102],
        'high':  [101, 103, 106, 104, 102, 103, 104, 107, 105, 103],
        'low':   [99,  101, 104, 102, 100, 101, 102, 105, 103, 101],
        'close': [100, 102, 105, 103, 101, 102, 103, 106, 104, 102],
        'volume': [1000] * 10
    }, index=dates)

    pools = detector.detect(data)

    # Check if price swept above 106 then reversed
    swept = detector.check_sweep(pools[0], current_price=107, then_reversed_to=104)
    assert swept is True
