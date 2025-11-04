import pytest
import pandas as pd
import numpy as np
from strategies.ict_strategy.detectors.correlation import CorrelationAnalyzer

def test_calculate_correlation():
    """Test correlation calculation between two assets"""
    # Create two perfectly correlated series
    dates = pd.date_range(start='2025-01-01', periods=100, freq='1H')

    data1 = pd.DataFrame({
        'close': np.linspace(100, 150, 100)
    }, index=dates)

    data2 = pd.DataFrame({
        'close': np.linspace(200, 250, 100)  # Different scale but same trend
    }, index=dates)

    analyzer = CorrelationAnalyzer()
    corr = analyzer.calculate_correlation(data1, data2, window=20)

    assert corr > 0.95  # Should be highly correlated

def test_detect_divergence():
    """Test detection of correlation breakdown (SMT divergence)"""
    dates = pd.date_range(start='2025-01-01', periods=100, freq='1H')

    # First 50: correlated
    # Last 50: diverge (one goes up, other goes down)
    prices1 = list(np.linspace(100, 125, 50)) + list(np.linspace(125, 140, 50))
    prices2 = list(np.linspace(200, 225, 50)) + list(np.linspace(225, 210, 50))

    data1 = pd.DataFrame({'close': prices1}, index=dates)
    data2 = pd.DataFrame({'close': prices2}, index=dates)

    analyzer = CorrelationAnalyzer(correlation_threshold=0.7)
    divergences = analyzer.detect_divergence(data1, data2, window=20)

    assert len(divergences) > 0
    # Divergence should be detected in second half
    assert any(d['timestamp'] > dates[50] for d in divergences)
