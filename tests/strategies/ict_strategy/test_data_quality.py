import pytest
import pandas as pd
from datetime import datetime, timedelta
from strategies.ict_strategy.utils.data_quality import ICTDataValidator

def test_validate_ohlcv_quality():
    """Test OHLCV data quality validation for FVG detection"""
    # Create sample data
    dates = pd.date_range(start='2025-01-01', periods=100, freq='1H')
    data = pd.DataFrame({
        'open': 100.0,
        'high': 105.0,
        'low': 95.0,
        'close': 102.0,
        'volume': 1000.0
    }, index=dates)

    validator = ICTDataValidator()
    result = validator.validate_for_fvg_detection(data, timeframe='1H')

    assert result.is_valid is True
    assert result.quality_score >= 80.0
    assert result.timeframe == '1H'
    assert len(result.issues) == 0

def test_reject_low_quality_data():
    """Test that low quality data is rejected"""
    # Create data with gaps and anomalies
    dates = pd.date_range(start='2025-01-01', periods=100, freq='1H')
    data = pd.DataFrame({
        'open': 100.0,
        'high': 105.0,
        'low': 95.0,
        'close': 102.0,
        'volume': 1000.0
    }, index=dates)

    # Introduce quality issues
    data.loc[dates[10]:dates[15], :] = None  # Missing data gap
    data.loc[dates[50], 'high'] = 1000000.0  # Anomaly

    validator = ICTDataValidator()
    result = validator.validate_for_fvg_detection(data, timeframe='1H')

    assert result.is_valid is False
    assert result.quality_score < 70.0
    assert len(result.issues) > 0
    assert any('gap' in issue.lower() or 'missing' in issue.lower() or 'nan' in issue.lower() for issue in result.issues)
