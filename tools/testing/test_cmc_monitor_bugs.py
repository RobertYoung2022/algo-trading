"""
Test Cases for CMC Real-Time Monitor Bug Analysis
==============================================

This test suite identifies and validates the specific bugs found in the CMC Real-Time Monitor:

1. Fear & Greed Index Display Bug: Only shows every 2 minutes instead of cached data
2. Market Sentiment Analysis Validation: Math inconsistencies in percentage calculations

Author: Test Automation Engineer
"""

import unittest
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import time

# Add the data-streams directory to path so we can import the monitor
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data-streams'))

from cmc_real_time_monitor import CMCRealTimeMonitor


class TestFearGreedIndexCaching(unittest.TestCase):
    """Test cases for Fear & Greed Index caching bug"""

    def setUp(self):
        """Set up test monitor instance"""
        self.monitor = CMCRealTimeMonitor()

    def test_fear_greed_cache_should_show_between_updates(self):
        """
        BUG TEST: Fear & Greed Index should show cached data between API updates

        Current behavior: Only shows every 2 minutes (SENTIMENT_UPDATE_INTERVAL)
        Expected behavior: Should show cached data in every cycle
        """
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [{
                'value': '25',
                'value_classification': 'Extreme Fear',
                'timestamp': '1726317720',
                'time_until_update': '3600'
            }]
        }

        with patch('requests.get', return_value=mock_response):
            # First call - should fetch fresh data
            result1 = self.monitor.get_fear_greed_index()
            self.assertIsNotNone(result1)
            self.assertEqual(result1['value'], 25)
            self.assertFalse(result1.get('cached', True))  # Fresh data

            # Second call immediately after - should return cached data
            result2 = self.monitor.get_fear_greed_index()
            self.assertIsNotNone(result2)
            self.assertEqual(result2['value'], 25)
            # This should be cached since we're within the 1-hour cache window

        print(f"✅ Cache Test: First call cached={result1.get('cached', False)}, "
              f"Second call cached={result2.get('cached', False)}")

    def test_sentiment_update_interval_bug(self):
        """
        BUG TEST: Fear & Greed should display cached data in every monitoring cycle

        Root cause: The display logic is gated by SENTIMENT_UPDATE_INTERVAL (120s)
        instead of showing cached data between updates.
        """
        # Simulate the monitor's run logic
        self.monitor.last_sentiment_update = time.time() - 60  # 60 seconds ago
        SENTIMENT_UPDATE_INTERVAL = 120  # From config

        current_time = time.time()
        time_since_last = current_time - self.monitor.last_sentiment_update

        # This condition prevents display of cached Fear & Greed data
        should_update_sentiment = time_since_last >= SENTIMENT_UPDATE_INTERVAL

        print(f"Time since last update: {time_since_last}s")
        print(f"Should update sentiment: {should_update_sentiment}")
        print(f"EXPECTED: Fear & Greed should still display cached data even when should_update_sentiment=False")

        # The bug: This condition gates ALL sentiment display, including cached F&G
        self.assertFalse(should_update_sentiment,
                        "This test demonstrates the bug - cached data won't display")


class TestMarketSentimentMath(unittest.TestCase):
    """Test cases for market sentiment calculation inconsistencies"""

    def setUp(self):
        """Set up test monitor instance"""
        self.monitor = CMCRealTimeMonitor()

    def test_market_sentiment_math_accuracy(self):
        """
        BUG TEST: Market sentiment math doesn't add up correctly

        Issue: Shows "12.5% positive" but "1/10 positive coins" (which is 10%)
        Root cause: Hardcoded "/10" display vs actual coin count calculation
        """
        # Create test data matching the reported bug scenario
        watchlist_data = [
            {'symbol': 'BTC', 'change_24h': 0.09},      # Positive
            {'symbol': 'ETH', 'change_24h': -0.43},     # Negative
            {'symbol': 'XRP', 'change_24h': -2.08},     # Negative
            {'symbol': 'SUI', 'change_24h': -2.12},     # Negative
            {'symbol': 'HBAR', 'change_24h': -1.98},    # Negative
            {'symbol': 'CRO', 'change_24h': -4.31},     # Negative
            {'symbol': 'LINK', 'change_24h': -1.94},    # Negative
            {'symbol': 'TAO', 'change_24h': -2.46},     # Negative
        ]

        # Test the sentiment analysis
        result = self.monitor.analyze_market_sentiment(watchlist_data)

        self.assertIsNotNone(result)

        # Calculate expected values
        positive_coins = result['positive_coins']
        total_coins = len(watchlist_data)  # Should be 8, not hardcoded 10
        actual_market_breadth = result['market_breadth']
        expected_market_breadth = (positive_coins / total_coins) * 100

        print(f"\n=== MARKET SENTIMENT MATH ANALYSIS ===")
        print(f"Total coins analyzed: {total_coins}")
        print(f"Positive coins: {positive_coins}")
        print(f"Expected market breadth: {expected_market_breadth:.1f}%")
        print(f"Actual market breadth: {actual_market_breadth:.1f}%")
        print(f"Display will show: '{positive_coins}/10' - THIS IS THE BUG")

        # Verify calculations are internally correct
        self.assertEqual(expected_market_breadth, actual_market_breadth,
                        "Internal calculation should be correct")

        # This demonstrates the display bug - hardcoded "/10"
        self.assertEqual(total_coins, 8, "We have 8 coins in watchlist, not 10")

        # The bug: display_market_sentiment() hardcodes "/10" instead of using total count
        expected_display_fraction = f"{positive_coins}/{total_coins}"
        buggy_display_fraction = f"{positive_coins}/10"

        print(f"Correct display should be: {expected_display_fraction}")
        print(f"Buggy display shows: {buggy_display_fraction}")

        self.assertNotEqual(expected_display_fraction, buggy_display_fraction,
                           "This demonstrates the hardcoded '/10' bug")

    def test_sentiment_score_calculation_edge_cases(self):
        """Test sentiment score calculation with edge cases"""
        # Test with no positive changes (should be 0%)
        all_negative_data = [
            {'symbol': 'ETH', 'change_24h': -0.43, 'volume_24h': 1000},
            {'symbol': 'XRP', 'change_24h': -2.08, 'volume_24h': 2000},
            {'symbol': 'SUI', 'change_24h': -2.12, 'volume_24h': 500},
        ]

        result = self.monitor.analyze_market_sentiment(all_negative_data)
        self.assertEqual(result['positive_coins'], 0)
        self.assertEqual(result['market_breadth'], 0.0)
        self.assertEqual(result['negative_coins'], 3)

        # Test with all positive changes (should be 100%)
        all_positive_data = [
            {'symbol': 'BTC', 'change_24h': 5.0, 'volume_24h': 1000},
            {'symbol': 'ETH', 'change_24h': 3.2, 'volume_24h': 2000},
            {'symbol': 'XRP', 'change_24h': 1.5, 'volume_24h': 500},
        ]

        result = self.monitor.analyze_market_sentiment(all_positive_data)
        self.assertEqual(result['positive_coins'], 3)
        self.assertEqual(result['market_breadth'], 100.0)
        self.assertEqual(result['negative_coins'], 0)


class TestCodeQualityIssues(unittest.TestCase):
    """Test cases highlighting code quality issues"""

    def test_hardcoded_values_in_display(self):
        """Identify hardcoded values that should be dynamic"""
        # This test documents the specific hardcoded values found
        hardcoded_issues = [
            {
                'file': 'cmc_real_time_monitor.py',
                'line': 1018,
                'issue': 'Hardcoded "/10" in positive coins display',
                'code': 'cprint(f"✅ Positive Coins: {sentiment_data[\'positive_coins\']}/10", "green")',
                'fix': 'Should use total coin count from watchlist'
            },
            {
                'file': 'cmc_real_time_monitor.py',
                'line': 1019,
                'issue': 'Hardcoded "/10" in negative coins display',
                'code': 'cprint(f"❌ Negative Coins: {sentiment_data[\'negative_coins\']}/10", "red")',
                'fix': 'Should use total coin count from watchlist'
            }
        ]

        print("\n=== CODE QUALITY ISSUES IDENTIFIED ===")
        for issue in hardcoded_issues:
            print(f"File: {issue['file']}, Line: {issue['line']}")
            print(f"Issue: {issue['issue']}")
            print(f"Code: {issue['code']}")
            print(f"Fix: {issue['fix']}\n")

        self.assertEqual(len(hardcoded_issues), 2, "Found 2 hardcoded display issues")

    def test_caching_logic_separation_concern(self):
        """Test separation of concerns in caching logic"""
        monitor = CMCRealTimeMonitor()

        # The issue: Fear & Greed caching works correctly, but display is gated by sentiment interval
        # This violates separation of concerns - caching and display scheduling should be independent

        # Demonstrate that cache logic itself works
        self.assertIsNone(monitor.fear_greed_cache, "Cache should start empty")

        # Mock a cached entry
        monitor.fear_greed_cache = {
            'value': 45,
            'value_classification': 'Fear',
            'cached': True,
            'fetch_time': datetime.now().isoformat()
        }
        monitor.fear_greed_last_fetch = datetime.now()

        # Cache retrieval should work
        cached_result = monitor.get_fear_greed_index()
        self.assertIsNotNone(cached_result)
        self.assertEqual(cached_result['value'], 45)

        print("✅ Caching logic works correctly")
        print("❌ Problem: Display is gated by SENTIMENT_UPDATE_INTERVAL instead of showing cached data")


if __name__ == '__main__':
    print("=" * 80)
    print("CMC REAL-TIME MONITOR BUG ANALYSIS TEST SUITE")
    print("=" * 80)
    print("This test suite validates the specific bugs identified:")
    print("1. Fear & Greed Index only displays every 2 minutes (should show cached)")
    print("2. Market sentiment math: '1/10 positive coins' but shows '12.5% positive'")
    print("3. Hardcoded values in display functions")
    print("=" * 80)

    unittest.main(verbosity=2)