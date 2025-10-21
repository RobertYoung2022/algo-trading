"""
Test Configuration and Fixtures for Backend Testing
===================================================

This module provides pytest configuration, fixtures, and utilities for testing
the cryptocurrency market monitoring backend system.
"""

import pytest
import tempfile
import shutil
import os
import json
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import sys

# Add project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'data-scripts'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'data-streams'))


@pytest.fixture
def temp_data_directory():
    """Provide a temporary directory for test data files"""
    temp_dir = tempfile.mkdtemp()
    live_market_dir = os.path.join(temp_dir, 'live_market')
    os.makedirs(live_market_dir, exist_ok=True)

    yield temp_dir

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_current_prices_data():
    """Provide realistic mock current prices data matching system format"""
    return {
        'BTC': {
            'price': 116104.01,
            'volume_24h': 1804.32,
            'change_24h': 0.081,
            'market_cap': 2312808693760,  # $2.31T
            'symbol': 'BTC',
            'source': 'coinbase',
            'timestamp': datetime.now().isoformat(),
            'all_sources': ['yahoo', 'coinbase'],
            'source_count': 2,
            'price_variance': {'min': 116100.88, 'max': 116104.01, 'avg': 116102.44, 'std': 1.56}
        },
        'ETH': {
            'price': 4665.14,
            'volume_24h': 55844.38,
            'change_24h': -1.18,
            'market_cap': 563351650304,  # $563B
            'symbol': 'ETH',
            'source': 'coinbase',
            'timestamp': datetime.now().isoformat(),
            'all_sources': ['yahoo', 'coinbase'],
            'source_count': 2,
            'price_variance': {'min': 4665.14, 'max': 4667.19, 'avg': 4666.17, 'std': 1.03}
        },
        'HOOD': {
            'price': 115.02,
            'volume_24h': 67700238.0,
            'change_24h': 0.0,
            'market_cap': 102224281600,  # $102B - STOCK (should be excluded from crypto market cap)
            'symbol': 'HOOD',
            'source': 'yahoo',
            'timestamp': datetime.now().isoformat()
        },
        'SPY': {
            'price': 657.40,
            'volume_24h': 126157507.0,
            'change_24h': 0.0,
            'market_cap': 603359019008,  # $603B - ETF (should be excluded from crypto market cap)
            'symbol': 'SPY',
            'source': 'yahoo',
            'timestamp': datetime.now().isoformat()
        }
    }


@pytest.fixture
def mock_market_overview_data(mock_current_prices_data):
    """Provide mock market overview data"""
    crypto_symbols = ['BTC', 'ETH']
    total_market_cap = sum(
        data['market_cap'] for symbol, data in mock_current_prices_data.items()
        if symbol in crypto_symbols
    )

    return {
        'total_symbols': len(mock_current_prices_data),
        'crypto_symbols': len(crypto_symbols),
        'total_market_cap': total_market_cap,
        'total_volume_24h': sum(data['volume_24h'] for data in mock_current_prices_data.values()),
        'positive_changes': len([d for d in mock_current_prices_data.values() if d.get('change_24h', 0) > 0]),
        'negative_changes': len([d for d in mock_current_prices_data.values() if d.get('change_24h', 0) < 0]),
        'timestamp': datetime.now().isoformat(),
        'data_sources_status': {
            'yahoo': {'success': 183, 'failures': 0},
            'coingecko': {'success': 102, 'failures': 81},
            'coinbase': {'success': 183, 'failures': 0}
        }
    }


@pytest.fixture
def mock_quality_metrics():
    """Provide mock data quality metrics"""
    return {
        'yahoo': {'success': 183, 'failures': 0},
        'coingecko': {'success': 102, 'failures': 81},  # 55.7% success rate
        'coinbase': {'success': 183, 'failures': 0}
    }


@pytest.fixture
def mock_fear_greed_response():
    """Provide mock Fear & Greed Index API response"""
    return {
        'data': [{
            'value': '45',
            'value_classification': 'Fear',
            'timestamp': str(int(time.time())),
            'time_until_update': '43200'  # 12 hours in seconds
        }]
    }


@pytest.fixture
def mock_arbitrage_data():
    """Provide mock data for arbitrage detection testing"""
    return [
        {
            'symbol': 'BTC',
            'price': 116104.01,
            'all_sources': ['yahoo', 'coinbase', 'coingecko'],
            'source_count': 3,
            'price_variance': {
                'min': 116100.88,    # Small spread for testing
                'max': 116104.01,
                'avg': 116102.44,
                'std': 1.56
            }
        },
        {
            'symbol': 'ETH',
            'price': 4665.14,
            'all_sources': ['yahoo', 'coinbase'],
            'source_count': 2,
            'price_variance': {
                'min': 4660.00,      # Larger spread for arbitrage opportunity
                'max': 4670.00,
                'avg': 4665.00,
                'std': 5.00
            }
        }
    ]


@pytest.fixture
def mock_watchlist_data():
    """Provide mock watchlist data for sentiment analysis"""
    return [
        {'symbol': 'BTC', 'change_24h': 2.1, 'volume_24h': 1803.6},
        {'symbol': 'ETH', 'change_24h': -1.1, 'volume_24h': 55805.3},
        {'symbol': 'XRP', 'change_24h': -2.4, 'volume_24h': 53288855.3},
        {'symbol': 'SUI', 'change_24h': -0.3, 'volume_24h': 11716272.8}
    ]


@pytest.fixture
def mock_global_data():
    """Provide mock global market data"""
    return {
        'bitcoin_dominance': 58.5,
        'ethereum_dominance': 13.2,
        'total_market_cap': 2876864327650.67,
        'total_volume_24h': 125000000000,
        'active_cryptocurrencies': 14,
        'timestamp': datetime.now().isoformat()
    }


class TestDataBuilder:
    """Utility class for building test data with various scenarios"""

    @staticmethod
    def create_price_data(symbol: str, price: float, **kwargs):
        """Create price data for a specific symbol"""
        default_data = {
            'price': price,
            'volume_24h': 1000.0,
            'change_24h': 0.0,
            'market_cap': price * 1000000,  # Rough market cap estimate
            'symbol': symbol,
            'source': 'test',
            'timestamp': datetime.now().isoformat()
        }
        default_data.update(kwargs)
        return default_data

    @staticmethod
    def create_multi_source_data(symbol: str, prices: dict):
        """Create multi-source price data for arbitrage testing"""
        all_prices = list(prices.values())
        return {
            'symbol': symbol,
            'all_sources': list(prices.keys()),
            'source_count': len(prices),
            'price_variance': {
                'min': min(all_prices),
                'max': max(all_prices),
                'avg': sum(all_prices) / len(all_prices),
                'std': (sum((p - sum(all_prices)/len(all_prices))**2 for p in all_prices) / len(all_prices))**0.5
            }
        }

    @staticmethod
    def create_api_failure_scenario(source: str, success_rate: float):
        """Create API failure scenario for testing"""
        total_calls = 100
        successes = int(total_calls * success_rate)
        failures = total_calls - successes

        return {
            source: {
                'success': successes,
                'failures': failures
            }
        }


@pytest.fixture
def test_data_builder():
    """Provide TestDataBuilder instance"""
    return TestDataBuilder()


# Performance testing fixtures
@pytest.fixture
def performance_timer():
    """Provide performance timing utility"""
    class PerformanceTimer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.perf_counter()

        def stop(self):
            self.end_time = time.perf_counter()
            return self.elapsed()

        def elapsed(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None

    return PerformanceTimer()


# Mock API responses for external services
@pytest.fixture
def mock_api_responses():
    """Provide mock API responses for external services"""
    return {
        'alternative_me_fng': {
            'data': [{
                'value': '45',
                'value_classification': 'Fear',
                'timestamp': str(int(time.time())),
                'time_until_update': '43200'
            }]
        },
        'coingecko_simple_price': {
            'bitcoin': {
                'usd': 50000,
                'usd_24h_change': 2.5,
                'usd_24h_vol': 25000000000,
                'usd_market_cap': 1000000000000
            },
            'ethereum': {
                'usd': 4000,
                'usd_24h_change': -1.2,
                'usd_24h_vol': 15000000000,
                'usd_market_cap': 500000000000
            }
        },
        'coinbase_ticker': {
            'BTC-USD': {
                'price': '50000.00',
                'volume_24h': '1000.00'
            }
        },
        'yahoo_finance_data': {
            'BTC-USD': {
                'regularMarketPrice': 50000.0,
                'regularMarketVolume': 1000000000,
                'regularMarketChangePercent': 2.5
            }
        }
    }


# Test configuration and markers
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance test"
    )
    config.addinivalue_line(
        "markers", "api_dependent: mark test as dependent on external APIs"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


# Utility functions for test setup
def create_test_files(directory: str, data: dict):
    """Create test data files in specified directory"""
    os.makedirs(directory, exist_ok=True)

    files_created = []

    # Create current_prices.json
    if 'current_prices' in data:
        current_prices_file = os.path.join(directory, 'current_prices.json')
        with open(current_prices_file, 'w') as f:
            json.dump(data['current_prices'], f, indent=2)
        files_created.append(current_prices_file)

    # Create market_overview.json
    if 'market_overview' in data:
        market_overview_file = os.path.join(directory, 'market_overview.json')
        with open(market_overview_file, 'w') as f:
            json.dump(data['market_overview'], f, indent=2)
        files_created.append(market_overview_file)

    # Create quality_metrics.json
    if 'quality_metrics' in data:
        quality_metrics_file = os.path.join(directory, 'quality_metrics.json')
        with open(quality_metrics_file, 'w') as f:
            json.dump(data['quality_metrics'], f, indent=2)
        files_created.append(quality_metrics_file)

    return files_created


@pytest.fixture
def test_file_creator():
    """Provide test file creation utility"""
    return create_test_files


# Environment setup
@pytest.fixture(autouse=True)
def setup_test_environment():
    """Setup test environment for all tests"""
    # Set test environment variables
    test_env = {
        'CMC_API_KEY': 'test_key_12345',
        'COINBASE_API_KEY': 'test_coinbase_key',
        'COINBASE_API_SECRET': 'test_coinbase_secret',
        'ALPHA_VANTAGE_API_KEY': 'test_alphavantage_key'
    }

    with patch.dict(os.environ, test_env):
        yield


# Database/file cleanup
@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Clean up after each test"""
    yield

    # Clean up any temporary files or connections
    # This runs after each test completes
    pass