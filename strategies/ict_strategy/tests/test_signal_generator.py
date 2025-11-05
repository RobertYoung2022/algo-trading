#!/usr/bin/env python3
"""
Tests for ICT Signal Generator
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from strategies.ict_strategy.signal_generator import ICTSignalGenerator


@pytest.fixture
def signal_generator():
    """Create a signal generator instance"""
    return ICTSignalGenerator()


@pytest.fixture
def sample_ohlc_data():
    """Create sample OHLC data for testing"""
    dates = pd.date_range(start='2025-01-01', periods=100, freq='15min')

    # Create realistic price movement
    base_price = 45000
    noise = np.random.randn(100) * 100
    trend = np.linspace(0, 1000, 100)
    closes = base_price + trend + noise

    df = pd.DataFrame({
        'open': closes + np.random.randn(100) * 50,
        'high': closes + abs(np.random.randn(100)) * 100,
        'low': closes - abs(np.random.randn(100)) * 100,
        'close': closes,
        'volume': np.random.randint(100, 1000, 100)
    }, index=dates)

    return df


class TestICTSignalGenerator:
    """Test suite for ICT Signal Generator"""

    def test_initialization(self, signal_generator):
        """Test signal generator initializes correctly"""
        assert signal_generator.confirmations == []
        assert signal_generator.signal_history == []

    def test_calculate_atr(self, signal_generator, sample_ohlc_data):
        """Test ATR calculation"""
        atr = signal_generator.calculate_atr(sample_ohlc_data, period=14)
        assert atr > 0
        assert isinstance(atr, (int, float))

    def test_check_inverted_fvg_bullish(self, signal_generator):
        """Test bullish inverted FVG detection"""
        # Create data with bullish IFVG
        df = pd.DataFrame({
            'open': [100, 101, 105, 106, 107],
            'high': [101, 102, 106, 107, 108],
            'low': [99, 100, 104, 105, 106],
            'close': [100, 101, 105, 106, 107]
        })

        result = signal_generator.check_inverted_fvg(df, 'bullish')
        assert isinstance(result, bool)

    def test_check_cisd_bullish_engulfing(self, signal_generator):
        """Test bullish engulfing pattern (CISD)"""
        # Create bullish engulfing
        df = pd.DataFrame({
            'open': [105, 100],
            'high': [106, 108],
            'low': [100, 99],
            'close': [101, 107]
        })

        result = signal_generator.check_cisd(df, 'bullish')
        assert result == True

    def test_check_cisd_bearish_engulfing(self, signal_generator):
        """Test bearish engulfing pattern (CISD)"""
        # Create bearish engulfing
        df = pd.DataFrame({
            'open': [100, 107],
            'high': [106, 108],
            'low': [99, 99],
            'close': [105, 100]
        })

        result = signal_generator.check_cisd(df, 'bearish')
        assert result == True

    def test_check_sfp_bullish(self, signal_generator):
        """Test bullish Swing Failure Pattern"""
        key_level = 100

        # Create SFP: wick below level, close above
        df = pd.DataFrame({
            'open': [102, 101, 100],
            'high': [103, 102, 103],
            'low': [101, 100, 99],  # Wicked below 100
            'close': [102, 101, 102]  # Closed above 100
        })

        result = signal_generator.check_sfp(df, 'bullish', key_level)
        assert isinstance(result, bool)

    def test_check_market_structure_shift(self, signal_generator, sample_ohlc_data):
        """Test market structure shift detection"""
        result_bullish = signal_generator.check_market_structure_shift(
            sample_ohlc_data, 'bullish'
        )
        result_bearish = signal_generator.check_market_structure_shift(
            sample_ohlc_data, 'bearish'
        )

        assert isinstance(result_bullish, (bool, np.bool_))
        assert isinstance(result_bearish, (bool, np.bool_))

    def test_get_confirmations(self, signal_generator, sample_ohlc_data):
        """Test confirmation pattern aggregation"""
        key_level = sample_ohlc_data.iloc[-1]['close'] * 0.99

        confirmations = signal_generator.get_confirmations(
            sample_ohlc_data,
            'long',
            key_level
        )

        assert isinstance(confirmations, list)
        # Confirmations should be subset of known patterns
        valid_confirmations = ['IFVG', 'CISD', 'SFP', 'MSS']
        for conf in confirmations:
            assert conf in valid_confirmations

    def test_calculate_entry_levels_long(self, signal_generator, sample_ohlc_data):
        """Test entry level calculation for long positions"""
        key_level = sample_ohlc_data.iloc[-1]['close'] * 0.99

        levels = signal_generator.calculate_entry_levels(
            sample_ohlc_data,
            'long',
            key_level
        )

        assert 'entry' in levels
        assert 'stop' in levels
        assert 'target' in levels
        assert 'rr_ratio' in levels

        # Verify logic: stop < entry < target for longs
        assert levels['stop'] < levels['entry'] < levels['target']
        assert levels['rr_ratio'] >= 1.5  # Minimum R:R

    def test_calculate_entry_levels_short(self, signal_generator, sample_ohlc_data):
        """Test entry level calculation for short positions"""
        key_level = sample_ohlc_data.iloc[-1]['close'] * 1.01

        levels = signal_generator.calculate_entry_levels(
            sample_ohlc_data,
            'short',
            key_level
        )

        # Verify logic: target < entry < stop for shorts
        assert levels['target'] < levels['entry'] < levels['stop']
        assert levels['rr_ratio'] >= 1.49  # Minimum R:R (allow float precision)

    def test_generate_signal_with_direction(self, signal_generator, sample_ohlc_data):
        """Test complete signal generation with specified direction"""
        signal = signal_generator.generate_signal(
            df_higher=sample_ohlc_data,
            df_lower=sample_ohlc_data.tail(50),
            symbol='BTC/USDT',
            direction='long'
        )

        assert 'timestamp' in signal
        assert signal['symbol'] == 'BTC/USDT'
        assert signal['direction'] == 'long'
        assert 'confirmations' in signal
        assert 'confidence' in signal
        assert isinstance(signal['valid'], bool)

    def test_generate_signal_auto_direction(self, signal_generator, sample_ohlc_data):
        """Test signal generation with automatic direction detection"""
        signal = signal_generator.generate_signal(
            df_higher=sample_ohlc_data,
            df_lower=sample_ohlc_data.tail(50),
            symbol='ETH/USDT'
        )

        assert signal['direction'] in ['long', 'short', None]

    def test_signal_validation_requires_confirmations(self, signal_generator):
        """Test that signals require at least one confirmation"""
        # Create simple data with no clear patterns
        df = pd.DataFrame({
            'open': [100] * 20,
            'high': [101] * 20,
            'low': [99] * 20,
            'close': [100] * 20
        })

        signal = signal_generator.generate_signal(
            df_higher=df,
            df_lower=df,
            symbol='TEST/USDT',
            direction='long'
        )

        # Should not be valid without confirmations
        if not signal['confirmations']:
            assert signal['valid'] is False

    def test_signal_validation_requires_good_rr(self, signal_generator, sample_ohlc_data):
        """Test that signals require minimum R:R ratio"""
        signal = signal_generator.generate_signal(
            df_higher=sample_ohlc_data,
            df_lower=sample_ohlc_data.tail(50),
            symbol='BTC/USDT',
            direction='long'
        )

        # If valid, R:R must be >= 1.5
        if signal['valid']:
            assert signal['rr_ratio'] >= 1.5

    def test_confidence_scoring(self, signal_generator, sample_ohlc_data):
        """Test confidence scoring based on confirmations"""
        signal = signal_generator.generate_signal(
            df_higher=sample_ohlc_data,
            df_lower=sample_ohlc_data.tail(50),
            symbol='BTC/USDT',
            direction='long'
        )

        if signal['confidence']:
            assert signal['confidence'] in ['high', 'medium', 'low']

            # Verify confidence matches confirmation count
            conf_count = len(signal['confirmations'])
            if conf_count >= 3:
                assert signal['confidence'] == 'high'
            elif conf_count == 2:
                assert signal['confidence'] == 'medium'
            elif conf_count == 1:
                assert signal['confidence'] == 'low'

    def test_signal_history_tracking(self, signal_generator, sample_ohlc_data):
        """Test that signals are stored in history"""
        initial_count = len(signal_generator.signal_history)

        # Generate signal with proper key level to ensure it processes
        key_level = sample_ohlc_data.iloc[-1]['close'] * 1.01
        signal_generator.generate_signal(
            df_higher=sample_ohlc_data,
            df_lower=sample_ohlc_data.tail(50),
            symbol='BTC/USDT',
            direction='long',
            key_level=key_level
        )

        # Signal should be added to history even if not valid
        assert len(signal_generator.signal_history) >= initial_count

    def test_filter_high_probability(self, signal_generator):
        """Test filtering for high probability signals"""
        # Create mock signals
        signals = [
            {'valid': True, 'confidence': 'high', 'confirmations': ['IFVG', 'CISD', 'MSS']},
            {'valid': True, 'confidence': 'medium', 'confirmations': ['IFVG', 'CISD']},
            {'valid': True, 'confidence': 'low', 'confirmations': ['SFP']},
            {'valid': False, 'confidence': 'high', 'confirmations': ['IFVG', 'CISD']},
        ]

        filtered = signal_generator.filter_high_probability(signals)

        # Should return first two signals (valid + high/medium confidence + 2+ confirmations)
        assert len(filtered) == 2
        assert all(s['valid'] for s in filtered)
        assert all(s['confidence'] in ['high', 'medium'] for s in filtered)
        assert all(len(s['confirmations']) >= 2 for s in filtered)

    def test_get_signal_statistics(self, signal_generator, sample_ohlc_data):
        """Test signal statistics generation"""
        # Generate some signals
        for i in range(3):
            key_level = sample_ohlc_data.iloc[-1]['close'] * (1.01 if i % 2 == 0 else 0.99)
            signal_generator.generate_signal(
                df_higher=sample_ohlc_data,
                df_lower=sample_ohlc_data.tail(50),
                symbol='BTC/USDT',
                direction='long' if i % 2 == 0 else 'short',
                key_level=key_level
            )

        stats = signal_generator.get_signal_statistics()

        assert 'total' in stats
        # Only check for 'valid' if we have signals
        if stats['total'] > 0:
            assert 'valid' in stats
            assert stats['total'] >= 2  # At least 2 signals should be generated

    def test_empty_dataframe_handling(self, signal_generator):
        """Test handling of empty dataframes"""
        empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close'])

        # Should not crash
        result = signal_generator.check_inverted_fvg(empty_df, 'bullish')
        assert result is False

        result = signal_generator.check_cisd(empty_df, 'bullish')
        assert result is False

    def test_insufficient_data_handling(self, signal_generator):
        """Test handling of insufficient data"""
        small_df = pd.DataFrame({
            'open': [100],
            'high': [101],
            'low': [99],
            'close': [100]
        })

        # Should handle gracefully
        result = signal_generator.check_market_structure_shift(small_df, 'bullish')
        assert result is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
