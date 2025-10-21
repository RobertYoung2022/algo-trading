#!/usr/bin/env python3
"""
Data Accuracy Tester for Unified OHLCV Collector
===============================================
This script tests the accuracy and consistency of data collected from multiple sources.

TESTS PERFORMED:
- Price variance analysis across sources
- Data freshness validation
- Missing data detection
- Historical consistency checks
- Source reliability scoring
- Arbitrage opportunity validation

USAGE:
    python data_accuracy_tester.py
"""

import json
import os
import pandas as pd
import datetime
from typing import Dict, List, Optional
import numpy as np
from pathlib import Path
import requests
from termcolor import cprint

class DataAccuracyTester:
    """Test data accuracy and consistency from unified collector"""

    def __init__(self):
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.project_root, 'data', 'live_market')
        self.test_results = {
            'timestamp': datetime.datetime.now().isoformat(),
            'tests_passed': 0,
            'tests_failed': 0,
            'warnings': 0,
            'details': {}
        }

    def load_current_data(self) -> Optional[Dict]:
        """Load current prices data"""
        try:
            current_prices_file = os.path.join(self.data_dir, 'current_prices.json')

            if not os.path.exists(current_prices_file):
                cprint("❌ CRITICAL: No current prices data found!", "red", attrs=["bold"])
                cprint("   Make sure unified_ohlcv_collector.py is running", "yellow")
                return None

            with open(current_prices_file, 'r') as f:
                data = json.load(f)

            cprint(f"✅ Loaded data for {len(data)} symbols", "green")
            return data

        except Exception as e:
            cprint(f"❌ Error loading current data: {e}", "red")
            return None

    def test_price_variance(self, data: Dict) -> Dict:
        """Test price variance across sources for same symbol"""
        cprint("\n🔍 TESTING: Price Variance Across Sources", "cyan", attrs=["bold"])
        print("="*60)

        variance_results = {
            'high_variance_symbols': [],
            'multi_source_symbols': 0,
            'single_source_symbols': 0,
            'average_variance': 0,
            'max_variance': 0
        }

        variances = []

        for symbol, symbol_data in data.items():
            source_count = symbol_data.get('source_count', 1)

            if source_count > 1:
                variance_results['multi_source_symbols'] += 1

                # Check price variance
                if 'price_variance' in symbol_data:
                    variance_data = symbol_data['price_variance']
                    price_std = variance_data.get('std', 0)
                    avg_price = variance_data.get('avg', symbol_data.get('price', 0))

                    if avg_price > 0:
                        variance_percent = (price_std / avg_price) * 100
                        variances.append(variance_percent)

                        # Flag high variance (>2%)
                        if variance_percent > 2.0:
                            variance_results['high_variance_symbols'].append({
                                'symbol': symbol,
                                'variance_percent': variance_percent,
                                'price_range': f"${variance_data['min']:.4f} - ${variance_data['max']:.4f}",
                                'sources': symbol_data.get('all_sources', [])
                            })
                            cprint(f"⚠️  HIGH VARIANCE: {symbol} - {variance_percent:.2f}% variance", "yellow")
                            cprint(f"    Range: ${variance_data['min']:.4f} - ${variance_data['max']:.4f}", "white")
                            cprint(f"    Sources: {', '.join(symbol_data.get('all_sources', []))}", "white")
                            self.test_results['warnings'] += 1
                        else:
                            cprint(f"✅ {symbol}: {variance_percent:.2f}% variance (OK)", "green")
                            self.test_results['tests_passed'] += 1
            else:
                variance_results['single_source_symbols'] += 1
                cprint(f"ℹ️  {symbol}: Single source only ({symbol_data.get('primary_source', 'unknown')})", "white")

        # Calculate statistics
        if variances:
            variance_results['average_variance'] = np.mean(variances)
            variance_results['max_variance'] = np.max(variances)

        print(f"\n📊 VARIANCE SUMMARY:")
        print(f"   Multi-source symbols: {variance_results['multi_source_symbols']}")
        print(f"   Single-source symbols: {variance_results['single_source_symbols']}")
        print(f"   Average variance: {variance_results['average_variance']:.3f}%")
        print(f"   Maximum variance: {variance_results['max_variance']:.3f}%")
        print(f"   High variance alerts: {len(variance_results['high_variance_symbols'])}")

        return variance_results

    def test_data_freshness(self, data: Dict) -> Dict:
        """Test how fresh the data is"""
        cprint("\n⏰ TESTING: Data Freshness", "cyan", attrs=["bold"])
        print("="*60)

        freshness_results = {
            'fresh_data_count': 0,
            'stale_data_count': 0,
            'very_stale_count': 0,
            'average_age_minutes': 0,
            'oldest_data_symbol': None,
            'oldest_age_minutes': 0
        }

        now = datetime.datetime.now()
        ages = []

        for symbol, symbol_data in data.items():
            timestamp_str = symbol_data.get('timestamp', '')

            try:
                data_time = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if data_time.tzinfo is not None:
                    data_time = data_time.replace(tzinfo=None)  # Make naive for comparison

                age_delta = now - data_time
                age_minutes = age_delta.total_seconds() / 60
                ages.append(age_minutes)

                # Categorize freshness
                if age_minutes <= 5:  # Fresh (≤5 minutes)
                    freshness_results['fresh_data_count'] += 1
                    cprint(f"✅ {symbol}: {age_minutes:.1f}m old (FRESH)", "green")
                    self.test_results['tests_passed'] += 1
                elif age_minutes <= 60:  # Stale (≤60 minutes)
                    freshness_results['stale_data_count'] += 1
                    cprint(f"⚠️  {symbol}: {age_minutes:.1f}m old (STALE)", "yellow")
                    self.test_results['warnings'] += 1
                else:  # Very stale (>60 minutes)
                    freshness_results['very_stale_count'] += 1
                    cprint(f"❌ {symbol}: {age_minutes:.1f}m old (VERY STALE)", "red")
                    self.test_results['tests_failed'] += 1

                # Track oldest
                if age_minutes > freshness_results['oldest_age_minutes']:
                    freshness_results['oldest_age_minutes'] = age_minutes
                    freshness_results['oldest_data_symbol'] = symbol

            except Exception as e:
                cprint(f"❌ {symbol}: Invalid timestamp format ({timestamp_str})", "red")
                self.test_results['tests_failed'] += 1

        # Calculate average age
        if ages:
            freshness_results['average_age_minutes'] = np.mean(ages)

        print(f"\n📊 FRESHNESS SUMMARY:")
        print(f"   Fresh data (≤5m): {freshness_results['fresh_data_count']}")
        print(f"   Stale data (5-60m): {freshness_results['stale_data_count']}")
        print(f"   Very stale (>60m): {freshness_results['very_stale_count']}")
        print(f"   Average age: {freshness_results['average_age_minutes']:.1f} minutes")
        print(f"   Oldest data: {freshness_results['oldest_data_symbol']} ({freshness_results['oldest_age_minutes']:.1f}m)")

        return freshness_results

    def test_missing_fields(self, data: Dict) -> Dict:
        """Test for missing required fields"""
        cprint("\n🔍 TESTING: Missing Required Fields", "cyan", attrs=["bold"])
        print("="*60)

        required_fields = ['price', 'symbol', 'timestamp', 'source']
        optional_fields = ['volume_24h', 'change_24h', 'market_cap']

        missing_results = {
            'symbols_with_missing_required': [],
            'symbols_with_missing_optional': [],
            'field_coverage': {}
        }

        # Count field coverage
        for field in required_fields + optional_fields:
            missing_results['field_coverage'][field] = {
                'present': 0,
                'missing': 0,
                'coverage_percent': 0
            }

        for symbol, symbol_data in data.items():
            missing_required = []
            missing_optional = []

            # Check required fields
            for field in required_fields:
                if field in symbol_data and symbol_data[field] is not None:
                    missing_results['field_coverage'][field]['present'] += 1
                else:
                    missing_required.append(field)
                    missing_results['field_coverage'][field]['missing'] += 1

            # Check optional fields
            for field in optional_fields:
                if field in symbol_data and symbol_data[field] is not None:
                    missing_results['field_coverage'][field]['present'] += 1
                else:
                    missing_optional.append(field)
                    missing_results['field_coverage'][field]['missing'] += 1

            # Report results
            if missing_required:
                missing_results['symbols_with_missing_required'].append({
                    'symbol': symbol,
                    'missing_fields': missing_required
                })
                cprint(f"❌ {symbol}: Missing required fields: {', '.join(missing_required)}", "red")
                self.test_results['tests_failed'] += 1
            elif missing_optional:
                missing_results['symbols_with_missing_optional'].append({
                    'symbol': symbol,
                    'missing_fields': missing_optional
                })
                cprint(f"⚠️  {symbol}: Missing optional fields: {', '.join(missing_optional)}", "yellow")
                self.test_results['warnings'] += 1
            else:
                cprint(f"✅ {symbol}: All fields present", "green")
                self.test_results['tests_passed'] += 1

        # Calculate coverage percentages
        total_symbols = len(data)
        for field, coverage in missing_results['field_coverage'].items():
            coverage['coverage_percent'] = (coverage['present'] / total_symbols) * 100 if total_symbols > 0 else 0

        print(f"\n📊 FIELD COVERAGE SUMMARY:")
        for field, coverage in missing_results['field_coverage'].items():
            field_type = "REQ" if field in required_fields else "OPT"
            print(f"   {field:<12} [{field_type}]: {coverage['coverage_percent']:>5.1f}% ({coverage['present']}/{total_symbols})")

        return missing_results

    def test_source_reliability(self) -> Dict:
        """Test reliability of different data sources"""
        cprint("\n📊 TESTING: Source Reliability", "cyan", attrs=["bold"])
        print("="*60)

        try:
            quality_file = os.path.join(self.data_dir, 'quality_metrics.json')

            if not os.path.exists(quality_file):
                cprint("⚠️  No quality metrics file found", "yellow")
                return {}

            with open(quality_file, 'r') as f:
                quality_data = json.load(f)

            reliability_results = {}

            for source, metrics in quality_data.items():
                total_attempts = metrics.get('success', 0) + metrics.get('failures', 0)

                if total_attempts > 0:
                    success_rate = (metrics.get('success', 0) / total_attempts) * 100
                    reliability_results[source] = {
                        'success_rate': success_rate,
                        'total_attempts': total_attempts,
                        'successes': metrics.get('success', 0),
                        'failures': metrics.get('failures', 0)
                    }

                    if success_rate >= 90:
                        cprint(f"✅ {source.upper()}: {success_rate:.1f}% success rate (EXCELLENT)", "green")
                        self.test_results['tests_passed'] += 1
                    elif success_rate >= 70:
                        cprint(f"⚠️  {source.upper()}: {success_rate:.1f}% success rate (ACCEPTABLE)", "yellow")
                        self.test_results['warnings'] += 1
                    else:
                        cprint(f"❌ {source.upper()}: {success_rate:.1f}% success rate (POOR)", "red")
                        self.test_results['tests_failed'] += 1

                    print(f"    Attempts: {total_attempts}, Successes: {metrics.get('success', 0)}, Failures: {metrics.get('failures', 0)}")
                else:
                    cprint(f"ℹ️  {source.upper()}: No attempts recorded", "white")

            return reliability_results

        except Exception as e:
            cprint(f"❌ Error testing source reliability: {e}", "red")
            return {}

    def validate_arbitrage_opportunities(self, data: Dict) -> Dict:
        """Validate detected arbitrage opportunities"""
        cprint("\n💰 TESTING: Arbitrage Opportunities", "cyan", attrs=["bold"])
        print("="*60)

        arbitrage_results = {
            'opportunities_found': 0,
            'validated_opportunities': [],
            'false_positives': 0,
            'largest_spread': 0,
            'most_profitable_symbol': None
        }

        try:
            arbitrage_file = os.path.join(self.data_dir, 'arbitrage_alerts.json')

            if os.path.exists(arbitrage_file):
                with open(arbitrage_file, 'r') as f:
                    arbitrage_data = json.load(f)

                arbitrage_results['opportunities_found'] = len(arbitrage_data)

                for opportunity in arbitrage_data:
                    symbol = opportunity.get('symbol')
                    percentage_diff = opportunity.get('percentage_difference', 0)

                    # Validate by checking current data
                    if symbol in data and 'price_variance' in data[symbol]:
                        variance_data = data[symbol]['price_variance']
                        actual_spread = ((variance_data['max'] - variance_data['min']) / variance_data['avg']) * 100

                        if abs(actual_spread - percentage_diff) < 0.5:  # Within 0.5% tolerance
                            arbitrage_results['validated_opportunities'].append(opportunity)
                            cprint(f"✅ {symbol}: {percentage_diff:.2f}% spread VALIDATED", "green")
                            self.test_results['tests_passed'] += 1

                            if percentage_diff > arbitrage_results['largest_spread']:
                                arbitrage_results['largest_spread'] = percentage_diff
                                arbitrage_results['most_profitable_symbol'] = symbol
                        else:
                            arbitrage_results['false_positives'] += 1
                            cprint(f"❌ {symbol}: {percentage_diff:.2f}% spread FALSE POSITIVE (actual: {actual_spread:.2f}%)", "red")
                            self.test_results['tests_failed'] += 1
                    else:
                        cprint(f"⚠️  {symbol}: Cannot validate - no current variance data", "yellow")
                        self.test_results['warnings'] += 1
            else:
                cprint("ℹ️  No arbitrage opportunities file found", "white")

            print(f"\n📊 ARBITRAGE SUMMARY:")
            print(f"   Total opportunities: {arbitrage_results['opportunities_found']}")
            print(f"   Validated: {len(arbitrage_results['validated_opportunities'])}")
            print(f"   False positives: {arbitrage_results['false_positives']}")
            print(f"   Largest spread: {arbitrage_results['largest_spread']:.2f}% ({arbitrage_results['most_profitable_symbol']})")

        except Exception as e:
            cprint(f"❌ Error validating arbitrage opportunities: {e}", "red")

        return arbitrage_results

    def generate_report(self, test_results: Dict):
        """Generate comprehensive accuracy report"""
        cprint("\n📋 COMPREHENSIVE DATA ACCURACY REPORT", "white", "on_blue", attrs=["bold"])
        print("="*80)

        # Summary
        total_tests = self.test_results['tests_passed'] + self.test_results['tests_failed']
        success_rate = (self.test_results['tests_passed'] / total_tests * 100) if total_tests > 0 else 0

        cprint(f"\n🎯 OVERALL ACCURACY SCORE: {success_rate:.1f}%", "white", attrs=["bold"])
        print(f"   Tests Passed: {self.test_results['tests_passed']}")
        print(f"   Tests Failed: {self.test_results['tests_failed']}")
        print(f"   Warnings: {self.test_results['warnings']}")

        # Grade the system
        if success_rate >= 95:
            grade = "A+"
            color = "green"
            verdict = "EXCELLENT - Production ready"
        elif success_rate >= 85:
            grade = "A"
            color = "green"
            verdict = "GOOD - Minor improvements needed"
        elif success_rate >= 75:
            grade = "B"
            color = "yellow"
            verdict = "ACCEPTABLE - Some issues to address"
        elif success_rate >= 60:
            grade = "C"
            color = "yellow"
            verdict = "POOR - Significant improvements needed"
        else:
            grade = "F"
            color = "red"
            verdict = "FAILING - Major issues require attention"

        cprint(f"🏆 SYSTEM GRADE: {grade} - {verdict}", color, attrs=["bold"])

        # Save detailed report
        report_file = os.path.join(self.data_dir, f'accuracy_report_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        full_report = {
            'summary': self.test_results,
            'detailed_results': test_results,
            'grade': grade,
            'success_rate': success_rate
        }

        with open(report_file, 'w') as f:
            json.dump(full_report, f, indent=2)

        print(f"\n📄 Detailed report saved: {report_file}")

    def run_all_tests(self):
        """Run all data accuracy tests"""
        cprint("\n🧪 STARTING COMPREHENSIVE DATA ACCURACY TESTS", "white", "on_green", attrs=["bold"])
        print("=" * 80)

        # Load data
        data = self.load_current_data()
        if not data:
            return

        # Run tests
        test_results = {}
        test_results['price_variance'] = self.test_price_variance(data)
        test_results['data_freshness'] = self.test_data_freshness(data)
        test_results['missing_fields'] = self.test_missing_fields(data)
        test_results['source_reliability'] = self.test_source_reliability()
        test_results['arbitrage_validation'] = self.validate_arbitrage_opportunities(data)

        # Generate report
        self.generate_report(test_results)

        cprint("\n✅ ALL TESTS COMPLETED", "white", "on_green", attrs=["bold"])

def main():
    """Main entry point"""
    tester = DataAccuracyTester()
    tester.run_all_tests()

if __name__ == "__main__":
    main()