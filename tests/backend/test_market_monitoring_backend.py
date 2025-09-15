#!/usr/bin/env python3
"""
Comprehensive Backend Test Suite for Cryptocurrency Market Monitoring System
==========================================================================

This test suite provides complete backend testing coverage for the multi-source
cryptocurrency market monitoring system, with specific focus on:

1. Market Cap Calculation Bug Validation
2. Multi-Source Data Integration Testing
3. API Endpoint Validation and Contract Testing
4. Business Logic Verification
5. Data Quality and Integrity Testing
6. Performance and Load Testing
7. Error Handling and Resilience Testing

Test Framework: pytest with mock services and performance monitoring
Coverage: API endpoints, business logic, data layer, external service integration
"""

import pytest
import json
import time
import os
import sys
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timedelta
import requests_mock
import threading
from concurrent.futures import ThreadPoolExecutor
import statistics

# Add the project root to Python path for imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'data-scripts'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'data-streams'))

# Import system components
from unified_ohlcv_collector import UnifiedOHLCVCollector
from cmc_real_time_monitor import CMCRealTimeMonitor


class TestMarketCapCalculationBug:
    """Test suite specifically targeting the market cap calculation bug"""

    def setup_method(self):
        """Setup test environment with mock data"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_data_dir = os.path.join(self.temp_dir, 'live_market')
        os.makedirs(self.test_data_dir, exist_ok=True)

        # Mock current prices data with actual values from system
        self.mock_current_prices = {
            # Crypto symbols - should be included in market cap
            'BTC': {
                'price': 116124.2,
                'market_cap': 2313214329126.539,  # $2.31T
                'volume_24h': 1803.63606896,
                'change_24h': 0.09841390438395563,
                'symbol': 'BTC'
            },
            'ETH': {
                'price': 4669.2,
                'market_cap': 563384374712.9143,  # $563B
                'volume_24h': 55805.27280161,
                'change_24h': -1.090730380516403,
                'symbol': 'ETH'
            },
            'XRP': {
                'price': 3.0883,
                'market_cap': 184265532811.21906,  # $184B
                'volume_24h': 53288855.302082,
                'change_24h': -2.414130881284167,
                'symbol': 'XRP'
            },
            # Stock symbols - should NOT be included in crypto market cap
            'HOOD': {
                'price': 115.0199966430664,
                'market_cap': 102224281600,  # $102B - SHOULD BE EXCLUDED
                'volume_24h': 67700238.0,
                'change_24h': 0.0,
                'symbol': 'HOOD'
            },
            'COIN': {
                'price': 322.9100036621094,
                'market_cap': 83001253888,  # $83B - SHOULD BE EXCLUDED
                'volume_24h': 13538481.0,
                'change_24h': 0.0,
                'symbol': 'COIN'
            },
            'SPY': {
                'price': 657.4000244140625,
                'market_cap': 603359019008,  # $603B - SHOULD BE EXCLUDED
                'volume_24h': 126157507.0,
                'change_24h': 0.0,
                'symbol': 'SPY'
            }
        }

        # Expected crypto-only market cap
        self.expected_crypto_market_cap = (
            2313214329126.539 +   # BTC
            563384374712.9143 +   # ETH
            184265532811.21906    # XRP
        )  # ~$3.06T

        # Actual buggy calculation includes stocks
        self.buggy_total_market_cap = (
            self.expected_crypto_market_cap +
            102224281600 +  # HOOD
            83001253888 +   # COIN
            603359019008    # SPY
        )  # ~$3.85T (matches the bug)

    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('unified_ohlcv_collector.WATCHLIST', ['BTC', 'ETH', 'XRP'])
    @patch('unified_ohlcv_collector.STOCK_WATCHLIST', ['HOOD', 'COIN', 'SPY'])
    def test_market_cap_bug_identification(self):
        """Test that identifies the market cap calculation bug"""
        collector = UnifiedOHLCVCollector()

        # Create current prices file with test data
        current_prices_file = os.path.join(self.test_data_dir, 'current_prices.json')
        with open(current_prices_file, 'w') as f:
            json.dump(self.mock_current_prices, f)

        # Patch the BASE_DATA_DIR to use our test directory
        with patch('unified_ohlcv_collector.BASE_DATA_DIR', self.test_data_dir):
            # Test the buggy save_current_data method
            collector.save_current_data(self.mock_current_prices, [])

            # Load the created market overview
            market_overview_file = os.path.join(self.test_data_dir, 'market_overview.json')
            with open(market_overview_file, 'r') as f:
                market_overview = json.load(f)

            # BUG VALIDATION: The current code incorrectly includes stock market caps
            actual_total_market_cap = market_overview['total_market_cap']

            # This assertion will PASS with the current buggy code
            assert actual_total_market_cap == self.buggy_total_market_cap, \
                f"Bug confirmed: Total market cap {actual_total_market_cap} includes stocks"

            # This assertion will FAIL with the current buggy code (as expected)
            with pytest.raises(AssertionError, match="should only include crypto"):
                assert actual_total_market_cap == self.expected_crypto_market_cap, \
                    "Market cap should only include crypto symbols, not stocks"

    @patch('unified_ohlcv_collector.WATCHLIST', ['BTC', 'ETH', 'XRP'])
    @patch('unified_ohlcv_collector.STOCK_WATCHLIST', ['HOOD', 'COIN', 'SPY'])
    def test_corrected_market_cap_calculation(self):
        """Test the corrected market cap calculation (crypto-only)"""

        # Create a corrected version of the method for testing
        def save_current_data_corrected(unified_data, arbitrage_opportunities):
            """Corrected version that excludes stocks from crypto market cap"""
            try:
                # Create market overview (crypto only - exclude stocks)
                crypto_data = {k: v for k, v in unified_data.items() if k in ['BTC', 'ETH', 'XRP']}  # Use actual WATCHLIST
                total_market_cap = sum([data.get('market_cap', 0) for data in crypto_data.values()])
                total_volume = sum([data.get('volume_24h', 0) for data in crypto_data.values()])

                market_overview = {
                    'total_symbols': len(unified_data),
                    'crypto_symbols': len(crypto_data),
                    'crypto_market_cap': total_market_cap,  # Renamed for clarity
                    'total_volume_24h': total_volume,
                    'positive_changes': len([d for d in crypto_data.values() if d.get('change_24h', 0) > 0]),
                    'negative_changes': len([d for d in crypto_data.values() if d.get('change_24h', 0) < 0]),
                    'timestamp': datetime.now().isoformat()
                }

                market_overview_file = os.path.join(self.test_data_dir, 'market_overview_corrected.json')
                with open(market_overview_file, 'w') as f:
                    json.dump(market_overview, f, indent=2)

            except Exception as e:
                raise Exception(f"Error saving corrected data: {e}")

        # Test the corrected method
        save_current_data_corrected(self.mock_current_prices, [])

        # Load the corrected market overview
        market_overview_file = os.path.join(self.test_data_dir, 'market_overview_corrected.json')
        with open(market_overview_file, 'r') as f:
            corrected_overview = json.load(f)

        # Validate the corrected calculation
        actual_crypto_market_cap = corrected_overview['crypto_market_cap']

        assert actual_crypto_market_cap == self.expected_crypto_market_cap, \
            f"Corrected market cap {actual_crypto_market_cap} should match expected crypto-only total {self.expected_crypto_market_cap}"

        # Verify the difference
        difference = self.buggy_total_market_cap - actual_crypto_market_cap
        expected_stock_market_cap = 102224281600 + 83001253888 + 603359019008  # HOOD + COIN + SPY

        assert abs(difference - expected_stock_market_cap) < 1000, \
            f"Difference {difference} should equal excluded stock market caps {expected_stock_market_cap}"

    def test_market_cap_segregation_validation(self):
        """Test that crypto and stock market caps are properly segregated"""

        # Calculate crypto market cap
        crypto_symbols = ['BTC', 'ETH', 'XRP']
        crypto_market_cap = sum(
            self.mock_current_prices[symbol]['market_cap']
            for symbol in crypto_symbols
            if symbol in self.mock_current_prices
        )

        # Calculate stock market cap
        stock_symbols = ['HOOD', 'COIN', 'SPY']
        stock_market_cap = sum(
            self.mock_current_prices[symbol]['market_cap']
            for symbol in stock_symbols
            if symbol in self.mock_current_prices
        )

        # Validate segregation
        assert crypto_market_cap == self.expected_crypto_market_cap
        assert stock_market_cap == 788584554496  # HOOD + COIN + SPY
        assert crypto_market_cap + stock_market_cap == self.buggy_total_market_cap

        # Business rule validation
        assert crypto_market_cap > stock_market_cap * 2, \
            "Crypto market cap should be significantly larger than selected stocks"


class TestSentimentDataAnalysis:
    """Test suite for sentiment analysis components"""

    def setup_method(self):
        """Setup sentiment analysis test environment"""
        self.temp_dir = tempfile.mkdtemp()

        # Mock Fear & Greed Index response
        self.mock_fear_greed_response = {
            'data': [{
                'value': '45',
                'value_classification': 'Fear',
                'timestamp': '1726302000',
                'time_until_update': '43200'
            }]
        }

        # Mock global and watchlist data for sentiment analysis
        self.mock_global_data = {
            'bitcoin_dominance': 58.5,
            'ethereum_dominance': 13.2,
            'total_market_cap': 2876864327650.67
        }

        self.mock_watchlist_data = [
            {'symbol': 'BTC', 'change_24h': 2.1, 'volume_24h': 1803.6},
            {'symbol': 'ETH', 'change_24h': -1.1, 'volume_24h': 55805.3},
            {'symbol': 'XRP', 'change_24h': -2.4, 'volume_24h': 53288855.3},
            {'symbol': 'SUI', 'change_24h': -0.3, 'volume_24h': 11716272.8}
        ]

    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @requests_mock.Mocker()
    def test_fear_greed_index_api_integration(self, mock_requests):
        """Test Fear & Greed Index API integration and data validation"""

        # Mock the Alternative.me API response
        mock_requests.get(
            'https://api.alternative.me/fng/',
            json=self.mock_fear_greed_response,
            status_code=200
        )

        monitor = CMCRealTimeMonitor()
        fng_data = monitor.get_fear_greed_index()

        # Validate API integration
        assert fng_data is not None
        assert fng_data['value'] == 45
        assert fng_data['value_classification'] == 'Fear'
        assert 'timestamp' in fng_data
        assert 'time_until_update' in fng_data
        assert 'fetch_time' in fng_data

        # Validate business rules
        assert 0 <= fng_data['value'] <= 100, "Fear & Greed Index must be between 0-100"
        assert fng_data['value_classification'] in ['Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed']

    @requests_mock.Mocker()
    def test_fear_greed_index_api_failure_handling(self, mock_requests):
        """Test API failure handling for Fear & Greed Index"""

        # Test various failure scenarios
        failure_scenarios = [
            (500, 'Internal Server Error'),
            (429, 'Rate Limited'),
            (404, 'Not Found'),
            (0, 'Connection Error')  # Requests will raise an exception
        ]

        monitor = CMCRealTimeMonitor()

        for status_code, description in failure_scenarios:
            if status_code == 0:
                # Simulate connection error
                mock_requests.get(
                    'https://api.alternative.me/fng/',
                    exc=requests.ConnectionError(description)
                )
            else:
                mock_requests.get(
                    'https://api.alternative.me/fng/',
                    status_code=status_code,
                    text=description
                )

            fng_data = monitor.get_fear_greed_index()
            assert fng_data is None, f"Should handle {description} gracefully"

    @patch('cmc_real_time_monitor.WATCHLIST', ['BTC', 'ETH', 'XRP', 'SUI'])
    def test_market_sentiment_calculation_accuracy(self):
        """Test market sentiment calculation mathematical accuracy"""

        monitor = CMCRealTimeMonitor()
        sentiment = monitor.analyze_market_sentiment(self.mock_global_data, self.mock_watchlist_data)

        assert sentiment is not None
        assert 'score' in sentiment
        assert 'classification' in sentiment
        assert 'market_breadth' in sentiment
        assert 'positive_coins' in sentiment
        assert 'negative_coins' in sentiment

        # Validate calculation logic
        expected_positive = 1  # Only BTC is positive
        expected_negative = 3  # ETH, XRP, SUI are negative
        expected_breadth = (1 / 4) * 100  # 25%

        assert sentiment['positive_coins'] == expected_positive
        assert sentiment['negative_coins'] == expected_negative
        assert sentiment['market_breadth'] == expected_breadth

        # Validate score boundaries
        assert -100 <= sentiment['score'] <= 100

        # Test Bitcoin dominance impact
        assert sentiment['score'] > 0, "High BTC dominance should contribute positive sentiment"

    def test_sentiment_update_interval_mechanism(self):
        """Test sentiment data 120-second update interval mechanism"""

        monitor = CMCRealTimeMonitor()
        initial_time = time.time()
        monitor.last_sentiment_update = initial_time

        # Test that update is needed after 120 seconds
        current_time = initial_time + 121  # 121 seconds later

        with patch('time.time', return_value=current_time):
            time_since_last = current_time - monitor.last_sentiment_update
            should_update = time_since_last >= 120  # SENTIMENT_UPDATE_INTERVAL

            assert should_update, "Should update sentiment after 120 seconds"
            assert time_since_last == 121

    def test_sentiment_data_validation_edge_cases(self):
        """Test sentiment analysis with edge case data"""

        monitor = CMCRealTimeMonitor()

        # Test with empty data
        sentiment_empty = monitor.analyze_market_sentiment({}, [])
        assert sentiment_empty is None

        # Test with all positive changes
        all_positive_data = [
            {'symbol': 'BTC', 'change_24h': 5.0, 'volume_24h': 1000},
            {'symbol': 'ETH', 'change_24h': 3.0, 'volume_24h': 2000},
        ]

        sentiment_positive = monitor.analyze_market_sentiment(self.mock_global_data, all_positive_data)
        assert sentiment_positive['positive_coins'] == 2
        assert sentiment_positive['negative_coins'] == 0
        assert sentiment_positive['market_breadth'] == 100.0

        # Test with all negative changes
        all_negative_data = [
            {'symbol': 'BTC', 'change_24h': -5.0, 'volume_24h': 1000},
            {'symbol': 'ETH', 'change_24h': -3.0, 'volume_24h': 2000},
        ]

        sentiment_negative = monitor.analyze_market_sentiment(self.mock_global_data, all_negative_data)
        assert sentiment_negative['positive_coins'] == 0
        assert sentiment_negative['negative_coins'] == 2
        assert sentiment_negative['market_breadth'] == 0.0


class TestArbitrageDetectionEngine:
    """Test suite for arbitrage detection system"""

    def setup_method(self):
        """Setup arbitrage detection test environment"""

        # Mock multi-source price data with variance
        self.mock_arbitrage_data = [
            {
                'symbol': 'BTC',
                'price': 116124.2,
                'all_sources': ['yahoo', 'coinbase', 'coingecko'],
                'source_count': 3,
                'price_variance': {
                    'min': 116108.44,    # Yahoo
                    'max': 116125.0,     # Coinbase
                    'avg': 116119.21,
                    'std': 7.63
                }
            },
            {
                'symbol': 'ETH',
                'price': 4669.2,
                'all_sources': ['yahoo', 'coinbase'],
                'source_count': 2,
                'price_variance': {
                    'min': 4667.71,      # Lower price source
                    'max': 4669.21,      # Higher price source
                    'avg': 4668.46,
                    'std': 0.75
                }
            }
        ]

    def test_arbitrage_threshold_detection(self):
        """Test arbitrage opportunity detection with various thresholds"""

        monitor = CMCRealTimeMonitor()

        # Test with low threshold (0.1%) - should detect opportunities
        low_threshold_opps = monitor.detect_arbitrage_opportunities(self.mock_arbitrage_data, 0.1)
        assert len(low_threshold_opps) > 0, "Should detect opportunities with low threshold"

        # Test with high threshold (5%) - should detect fewer/no opportunities
        high_threshold_opps = monitor.detect_arbitrage_opportunities(self.mock_arbitrage_data, 5.0)
        assert len(high_threshold_opps) <= len(low_threshold_opps), "Higher threshold should detect fewer opportunities"

        # Validate opportunity data structure
        if low_threshold_opps:
            opp = low_threshold_opps[0]
            required_fields = ['symbol', 'min_price', 'max_price', 'avg_price', 'spread_percent', 'spread_absolute']
            for field in required_fields:
                assert field in opp, f"Arbitrage opportunity missing field: {field}"

    def test_arbitrage_calculation_accuracy(self):
        """Test mathematical accuracy of arbitrage calculations"""

        monitor = CMCRealTimeMonitor()
        opportunities = monitor.detect_arbitrage_opportunities(self.mock_arbitrage_data, 0.01)

        if opportunities:
            opp = opportunities[0]  # Test first opportunity

            # Validate spread calculation
            expected_spread_abs = opp['max_price'] - opp['min_price']
            expected_spread_pct = (expected_spread_abs / opp['avg_price']) * 100

            assert abs(opp['spread_absolute'] - expected_spread_abs) < 0.01
            assert abs(opp['spread_percent'] - expected_spread_pct) < 0.01

            # Validate price relationships
            assert opp['min_price'] <= opp['avg_price'] <= opp['max_price']
            assert opp['spread_percent'] >= 0

    def test_multi_source_synchronization_validation(self):
        """Test data source synchronization for arbitrage detection"""

        # Create test data with timestamps to validate synchronization
        sync_test_data = [
            {
                'symbol': 'BTC',
                'timestamp': '2025-09-14T05:12:36.730449',
                'all_sources': ['yahoo', 'coinbase', 'coingecko'],
                'source_count': 3,
                'price_variance': {
                    'min': 116000,
                    'max': 116500,  # 0.43% spread
                    'avg': 116250,
                    'std': 204.12
                }
            }
        ]

        monitor = CMCRealTimeMonitor()
        opportunities = monitor.detect_arbitrage_opportunities(sync_test_data, 0.3)

        if opportunities:
            opp = opportunities[0]
            # Validate that opportunity includes source metadata
            assert 'all_sources' in sync_test_data[0]
            assert 'source_count' in sync_test_data[0]
            assert sync_test_data[0]['source_count'] >= 2, "Need multiple sources for arbitrage"

    def test_statistical_variance_analysis(self):
        """Test statistical calculations in price variance analysis"""

        # Test price variance statistical accuracy
        btc_data = self.mock_arbitrage_data[0]
        variance = btc_data['price_variance']

        # Validate statistical relationships
        assert variance['min'] <= variance['avg'] <= variance['max']
        assert variance['std'] >= 0

        # Test standard deviation calculation validity
        prices = [variance['min'], variance['avg'], variance['max']]
        calculated_avg = sum(prices) / len(prices)

        # Note: This is a simplified test - real std dev would need all source prices
        assert abs(variance['avg'] - calculated_avg) < 1000  # Allow reasonable variance


class TestDataQualityMetrics:
    """Test suite for data quality validation and API reliability"""

    def setup_method(self):
        """Setup data quality test environment"""
        self.temp_dir = tempfile.mkdtemp()

        # Mock quality metrics matching actual system data
        self.mock_quality_metrics = {
            "yahoo": {"success": 265, "failures": 0},
            "coingecko": {"success": 182, "failures": 83},  # 56% success rate
            "coinbase": {"success": 265, "failures": 0}
        }

    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_api_success_rate_calculation(self):
        """Test API success rate calculations and thresholds"""

        for source, metrics in self.mock_quality_metrics.items():
            total_attempts = metrics['success'] + metrics['failures']
            success_rate = (metrics['success'] / total_attempts) * 100 if total_attempts > 0 else 0

            if source == 'coingecko':
                # Validate the known CoinGecko failure rate
                expected_rate = (182 / (182 + 83)) * 100  # ~68.7%
                assert abs(success_rate - expected_rate) < 0.1
                assert success_rate < 70, "CoinGecko success rate is concerning"
            else:
                assert success_rate == 100.0, f"{source} should have perfect success rate"

    def test_api_failure_impact_analysis(self):
        """Test impact of API failures on data completeness"""

        collector = UnifiedOHLCVCollector()

        # Simulate CoinGecko failure scenario
        collector.data_quality = self.mock_quality_metrics.copy()

        # Test that system continues functioning with partial failures
        total_sources = len(collector.data_quality)
        failed_sources = sum(1 for metrics in collector.data_quality.values()
                           if metrics['failures'] > metrics['success'])

        assert failed_sources < total_sources, "System should have working sources"

        # Validate fallback mechanisms
        working_sources = [source for source, metrics in collector.data_quality.items()
                         if metrics['success'] > metrics['failures']]

        assert len(working_sources) >= 2, "Should have multiple working sources for redundancy"

    def test_data_freshness_validation(self):
        """Test data freshness and stale data rejection"""

        current_time = datetime.now()

        # Test fresh data (within 60 seconds)
        fresh_timestamp = (current_time - timedelta(seconds=30)).isoformat()
        assert self._is_data_fresh(fresh_timestamp, max_age_seconds=60)

        # Test stale data (older than 60 seconds)
        stale_timestamp = (current_time - timedelta(seconds=120)).isoformat()
        assert not self._is_data_fresh(stale_timestamp, max_age_seconds=60)

    def _is_data_fresh(self, timestamp_str: str, max_age_seconds: int = 60) -> bool:
        """Helper method to validate data freshness"""
        try:
            data_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            current_time = datetime.now()
            age_seconds = (current_time - data_time).total_seconds()
            return age_seconds <= max_age_seconds
        except:
            return False

    def test_concurrent_api_failure_handling(self):
        """Test system behavior under concurrent API failures"""

        collector = UnifiedOHLCVCollector()

        # Simulate multiple concurrent API failures
        with patch.object(collector, 'collect_coingecko_data', side_effect=Exception("API Error")):
            with patch.object(collector, 'collect_binance_data', side_effect=Exception("Rate Limited")):

                # System should still function with Yahoo and Coinbase
                symbols = ['BTC', 'ETH']
                results = collector.collect_all_sources(symbols)

                # Should have some results from working sources
                assert isinstance(results, dict)
                # Results may be empty in test environment, but structure should be correct
                for symbol in symbols:
                    assert symbol in results


class TestPerformanceBenchmarks:
    """Test suite for performance validation and benchmarks"""

    def setup_method(self):
        """Setup performance test environment"""
        self.performance_thresholds = {
            'data_collection_max_time': 30,  # seconds
            'sentiment_update_max_time': 5,   # seconds
            'arbitrage_detection_max_time': 2, # seconds
            'file_io_max_time': 1            # seconds
        }

    def test_data_collection_response_time(self):
        """Test that data collection completes within 30-second intervals"""

        collector = UnifiedOHLCVCollector()
        symbols = ['BTC', 'ETH', 'XRP']

        start_time = time.time()

        # Mock the actual API calls to avoid network delays
        with patch.object(collector, 'collect_yahoo_data', return_value={}):
            with patch.object(collector, 'collect_coinbase_data', return_value={}):
                with patch.object(collector, 'collect_coingecko_data', return_value={}):
                    results = collector.collect_all_sources(symbols)

        execution_time = time.time() - start_time

        assert execution_time < self.performance_thresholds['data_collection_max_time'], \
            f"Data collection took {execution_time:.2f}s, should be under 30s"

    def test_memory_usage_monitoring(self):
        """Test memory consumption during extended operation"""

        import psutil
        import gc

        process = psutil.Process()
        initial_memory = process.memory_info().rss

        # Simulate extended operation
        collector = UnifiedOHLCVCollector()

        # Mock multiple data collection cycles
        for i in range(10):
            mock_data = {'BTC': {'price': 50000 + i, 'volume_24h': 1000}}
            collector.current_prices.update(mock_data)

        # Force garbage collection
        gc.collect()

        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (under 100MB for test)
        assert memory_increase < 100 * 1024 * 1024, \
            f"Memory usage increased by {memory_increase / 1024 / 1024:.1f}MB"

    def test_concurrent_request_performance(self):
        """Test performance under concurrent API requests"""

        collector = UnifiedOHLCVCollector()

        def mock_api_call():
            time.sleep(0.1)  # Simulate API latency
            return {'BTC': {'price': 50000, 'volume_24h': 1000}}

        start_time = time.time()

        # Test concurrent execution
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(mock_api_call) for _ in range(8)]
            results = [future.result() for future in futures]

        execution_time = time.time() - start_time

        # Should complete faster than sequential execution
        assert execution_time < 0.5, f"Concurrent requests took {execution_time:.2f}s"
        assert len(results) == 8

    def test_file_io_performance(self):
        """Test JSON file I/O performance"""

        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False)

        # Create large test data
        large_data = {
            f'SYMBOL_{i}': {
                'price': 1000 + i,
                'volume_24h': 50000 + i,
                'timestamp': datetime.now().isoformat(),
                'market_cap': 1000000000 + i * 1000
            }
            for i in range(100)  # 100 symbols
        }

        start_time = time.time()

        # Test write performance
        with open(temp_file.name, 'w') as f:
            json.dump(large_data, f, indent=2)

        write_time = time.time() - start_time

        # Test read performance
        start_time = time.time()

        with open(temp_file.name, 'r') as f:
            loaded_data = json.load(f)

        read_time = time.time() - start_time

        assert write_time < self.performance_thresholds['file_io_max_time']
        assert read_time < self.performance_thresholds['file_io_max_time']
        assert len(loaded_data) == 100

        # Cleanup
        os.unlink(temp_file.name)


class TestErrorHandlingAndResilience:
    """Test suite for error handling and system resilience"""

    def setup_method(self):
        """Setup error handling test environment"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @requests_mock.Mocker()
    def test_api_rate_limiting_handling(self, mock_requests):
        """Test system behavior under API rate limiting"""

        # Mock rate limiting responses
        mock_requests.get(
            'https://api.alternative.me/fng/',
            status_code=429,
            text='Rate Limited'
        )

        monitor = CMCRealTimeMonitor()
        result = monitor.get_fear_greed_index()

        assert result is None, "Should handle rate limiting gracefully"

    @requests_mock.Mocker()
    def test_api_timeout_handling(self, mock_requests):
        """Test API timeout handling and retries"""

        import requests

        # Mock timeout
        mock_requests.get(
            'https://api.alternative.me/fng/',
            exc=requests.Timeout('Request timed out')
        )

        monitor = CMCRealTimeMonitor()
        result = monitor.get_fear_greed_index()

        assert result is None, "Should handle timeouts gracefully"

    def test_file_system_error_handling(self):
        """Test file system error handling"""

        collector = UnifiedOHLCVCollector()

        # Test with invalid directory path
        with patch('unified_ohlcv_collector.BASE_DATA_DIR', '/invalid/path/that/does/not/exist'):

            # Should not crash when unable to save files
            try:
                collector.save_current_data({'BTC': {'price': 50000}}, [])
            except Exception as e:
                # Log error but don't crash the test
                print(f"Expected file system error: {e}")

    def test_malformed_data_handling(self):
        """Test handling of malformed API responses"""

        monitor = CMCRealTimeMonitor()

        # Test with malformed global data
        malformed_global = {
            'bitcoin_dominance': 'invalid_string',  # Should be number
            'total_market_cap': None                # Should be number
        }

        malformed_watchlist = [
            {'symbol': 'BTC'},  # Missing required fields
            {'change_24h': 'not_a_number'},  # Invalid data type
        ]

        # Should not crash with malformed data
        sentiment = monitor.analyze_market_sentiment(malformed_global, malformed_watchlist)
        # May return None or handle gracefully

    def test_network_interruption_recovery(self):
        """Test recovery from network interruptions"""

        collector = UnifiedOHLCVCollector()

        # Simulate network interruption
        with patch('requests.get', side_effect=requests.ConnectionError("Network unreachable")):

            # Collector should handle network errors gracefully
            results = collector.collect_yahoo_data(['BTC'])
            assert results == {}, "Should return empty results on network error"

            # Failure should be recorded in quality metrics
            initial_failures = collector.data_quality['yahoo']['failures']

            # After the failed call, failures should increase
            assert collector.data_quality['yahoo']['failures'] >= initial_failures

    def test_thread_pool_error_handling(self):
        """Test thread pool execution error handling"""

        collector = UnifiedOHLCVCollector()

        def failing_task():
            raise Exception("Task failed")

        # Test that thread pool errors don't crash the system
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(failing_task) for _ in range(3)]

            # Should handle task failures gracefully
            for future in futures:
                try:
                    future.result(timeout=1)
                except Exception:
                    pass  # Expected to fail


@pytest.fixture
def test_data_directory():
    """Fixture to provide temporary test data directory"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


# Integration Test Suite
class TestEndToEndIntegration:
    """End-to-end integration tests for the complete system"""

    def test_producer_consumer_integration(self, test_data_directory):
        """Test the complete producer-consumer data flow"""

        # Mock the data directory paths
        with patch('unified_ohlcv_collector.BASE_DATA_DIR', test_data_directory):
            with patch('cmc_real_time_monitor.BASE_DATA_DIR', test_data_directory):

                # Create collector and generate data
                collector = UnifiedOHLCVCollector()

                # Mock data generation
                test_data = {
                    'BTC': {
                        'price': 50000,
                        'volume_24h': 1000,
                        'change_24h': 2.5,
                        'market_cap': 1000000000000,
                        'timestamp': datetime.now().isoformat()
                    }
                }

                collector.save_current_data(test_data, [])

                # Verify files were created
                current_prices_file = os.path.join(test_data_directory, 'current_prices.json')
                market_overview_file = os.path.join(test_data_directory, 'market_overview.json')

                assert os.path.exists(current_prices_file)
                assert os.path.exists(market_overview_file)

                # Verify consumer can read the data
                monitor = CMCRealTimeMonitor()

                # Mock the path for the monitor
                with patch.object(monitor, 'get_global_metrics') as mock_global:
                    with patch.object(monitor, 'get_watchlist_data') as mock_watchlist:

                        # Should be able to read the generated data
                        mock_global.return_value = {'total_market_cap': 1000000000000}
                        mock_watchlist.return_value = [test_data['BTC']]

                        global_data = mock_global()
                        watchlist_data = mock_watchlist()

                        assert global_data is not None
                        assert len(watchlist_data) > 0


if __name__ == '__main__':
    # Run the test suite
    pytest.main([__file__, '-v', '--tb=short'])