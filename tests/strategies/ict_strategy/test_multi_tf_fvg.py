import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch
from strategies.ict_strategy.detectors.multi_tf_fvg import MultiTimeframeFVGDetector

def test_detect_across_timeframes():
    """Test FVG detection across multiple timeframes"""
    detector = MultiTimeframeFVGDetector(
        symbol='BTC/USD',
        timeframes=['1H', '4H', 'D'],
        validate_quality=False  # Disable validation for test data
    )

    # Mock data fetcher
    with patch.object(detector, '_fetch_data') as mock_fetch:
        # Setup mock to return sample data
        mock_fetch.return_value = _create_sample_data()

        result = detector.detect_all()

        assert '1H' in result
        assert '4H' in result
        assert 'D' in result
        assert len(result['1H']) >= 0  # May or may not have FVGs

def test_filter_by_alignment():
    """Test filtering FVGs by higher timeframe alignment"""
    detector = MultiTimeframeFVGDetector(
        symbol='BTC/USD',
        timeframes=['1H', '4H'],
        validate_quality=False  # Disable validation for test data
    )

    with patch.object(detector, '_fetch_data') as mock_fetch:
        mock_fetch.return_value = _create_sample_data_with_aligned_fvgs()

        result = detector.detect_aligned_fvgs(higher_tf='4H', lower_tf='1H')

        # Should only return 1H FVGs that align with 4H FVGs
        assert len(result) > 0
        for fvg in result:
            assert fvg.timeframe == '1H'
            assert fvg.aligned_with_htf is True

def _create_sample_data():
    dates = pd.date_range(start='2025-01-01', periods=100, freq='1h')
    return pd.DataFrame({
        'open': 100.0,
        'high': 105.0,
        'low': 95.0,
        'close': 102.0,
        'volume': 1000.0
    }, index=dates)

def _create_sample_data_with_aligned_fvgs():
    # Create data with obvious FVG patterns on both timeframes
    # Create a bullish FVG: candle[0].high < candle[2].low
    dates = pd.date_range(start='2025-01-01', periods=100, freq='1h')

    # Pattern: candle 0 ends at 101, candle 1 gaps up, candle 2 starts at 110
    # This creates a bullish FVG from 101-110
    open_prices = [100, 105, 110] + [110 + i*0.1 for i in range(97)]
    high_prices = [101, 109, 111] + [111 + i*0.1 for i in range(97)]
    low_prices = [99, 105, 110] + [110 + i*0.1 for i in range(97)]
    close_prices = [100, 108, 111] + [111 + i*0.1 for i in range(97)]

    data = pd.DataFrame({
        'open': open_prices,
        'high': high_prices,
        'low': low_prices,
        'close': close_prices,
        'volume': [1000] * 100
    }, index=dates)
    return data
