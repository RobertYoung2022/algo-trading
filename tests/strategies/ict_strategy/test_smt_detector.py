import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from strategies.ict_strategy.detectors.smt_detector import SMTDetector

def test_detect_smt_divergence():
    """Test detection of SMT divergence between BTC and ETH"""
    detector = SMTDetector(
        asset1='BTC/USD',
        asset2='ETH/USD',
        timeframe='1H'
    )

    with patch.object(detector, '_fetch_data') as mock_fetch:
        # BTC makes new high, ETH fails to make new high = bearish divergence
        dates = pd.date_range(start='2025-01-01', periods=100, freq='1H')

        btc_prices = list(np.linspace(40000, 42000, 50)) + list(np.linspace(42000, 43000, 50))
        eth_prices = list(np.linspace(2000, 2100, 50)) + list(np.linspace(2100, 2050, 50))

        mock_fetch.side_effect = [
            pd.DataFrame({'close': btc_prices}, index=dates),
            pd.DataFrame({'close': eth_prices}, index=dates)
        ]

        divergences = detector.detect()

        assert len(divergences) > 0
        assert divergences[0]['type'] == 'bearish'  # BTC stronger = bearish signal

def test_no_divergence_when_aligned():
    """Test that no divergence detected when assets move together"""
    detector = SMTDetector(
        asset1='BTC/USD',
        asset2='ETH/USD',
        timeframe='1H'
    )

    with patch.object(detector, '_fetch_data') as mock_fetch:
        dates = pd.date_range(start='2025-01-01', periods=100, freq='1H')

        # Both move up together
        btc_prices = np.linspace(40000, 45000, 100)
        eth_prices = np.linspace(2000, 2250, 100)

        mock_fetch.side_effect = [
            pd.DataFrame({'close': btc_prices}, index=dates),
            pd.DataFrame({'close': eth_prices}, index=dates)
        ]

        divergences = detector.detect()

        assert len(divergences) == 0
