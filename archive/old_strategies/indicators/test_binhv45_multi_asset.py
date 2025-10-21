"""
🚀 BinHV45 Multi-Asset Testing Framework 🚀
===========================================
Comprehensive testing framework for BinHV45 mean-reversion strategy across
multiple cryptocurrencies, timeframes, and data providers.

This framework uses the MANDATORY universal native results display to show
full backtesting.py output for EVERY test.

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import os
import sys
import glob
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 🚨 MANDATORY: Import universal native results display
from analysis.universal_native_results_display import enhanced_backtest_runner, create_data_source_info

# Import our strategy
from binhv45_mean_reversion_strategy import BinHV45Strategy

# Import data quality validator
try:
    from trading_functions.utils.data_quality_validator import DataQualityValidator
    HAS_VALIDATOR = True
except ImportError:
    print("⚠️ Warning: DataQualityValidator not available")
    HAS_VALIDATOR = False


class BinHV45MultiAssetTester:
    """
    🌙 Comprehensive Multi-Asset Testing Framework for BinHV45 Strategy 🌙

    Tests across ALL available cryptocurrencies and data providers with
    mandatory native backtesting.py results display.
    """

    def __init__(self):
        """Initialize the multi-asset tester"""
        self.base_data_path = "/Users/bobbyyo/Projects/algo-fun/data"
        self.results_path = "/Users/bobbyyo/Projects/algo-fun/strategies/results"
        self.min_quality_score = 75  # Minimum data quality score required
        self.all_results = []
        self.validator = DataQualityValidator() if HAS_VALIDATOR else None

        # Create results directory if it doesn't exist
        os.makedirs(self.results_path, exist_ok=True)

        print("🌙 BinHV45 Multi-Asset Tester Initialized 🌙")
        print(f"📊 Base data path: {self.base_data_path}")
        print(f"📁 Results path: {self.results_path}")
        print(f"✅ Data quality validation: {'Enabled' if HAS_VALIDATOR else 'Disabled'}")
        print("=" * 100)

    def discover_data_sources(self, timeframes=['1m', '5m', '6h', '1d', '10yr', '20yr', '1000wks', '200wks']):
        """
        🔍 Discover ALL available data sources across providers and timeframes

        Searches for data files matching ALL timeframes from:
        - Coinbase
        - Hyperliquid
        - Yahoo Finance (multi-year data)
        - CoinGecko
        - CryptoCompare
        """
        discovered_sources = []

        # Define provider directories and patterns
        providers = {
            'coinbase': 'coinbase',
            'hyperliquid': 'hyperliquid',
            'yahoo': 'yahoo',
            'coingecko': 'coingecko',
            'cryptocompare': 'cryptocompare'
        }

        print("\n🔍 Discovering Data Sources...")
        print("=" * 60)

        for provider_name, provider_dir in providers.items():
            provider_path = os.path.join(self.base_data_path, provider_dir)

            if not os.path.exists(provider_path):
                print(f"⚠️ Provider directory not found: {provider_path}")
                continue

            # Search for CSV files matching ALL timeframes including multi-year data
            search_patterns = [
                f"*{tf}*.csv" for tf in timeframes
            ] + [
                "*1min*.csv", "*5min*.csv", "*6hour*.csv", "*1day*.csv",
                "*daily*.csv", "*yahoo*.csv", "*multi*.csv", "*long*.csv"
            ]

            all_files = set()
            for pattern in search_patterns:
                full_pattern = os.path.join(provider_path, pattern)
                files = glob.glob(full_pattern)
                all_files.update(files)

            for file_path in all_files:
                # Extract symbol and timeframe from filename
                filename = os.path.basename(file_path)
                parts = filename.split('-')

                if len(parts) >= 1:
                    symbol = parts[0].replace('USD', '')  # Extract base symbol

                    # Detect timeframe from filename
                    detected_timeframe = 'unknown'
                    for tf in timeframes:
                        if tf in filename:
                            detected_timeframe = tf
                            break

                    # Additional timeframe detection for various naming patterns
                    if detected_timeframe == 'unknown':
                        if '1min' in filename or '1m' in filename:
                            detected_timeframe = '1m'
                        elif '5min' in filename or '5m' in filename:
                            detected_timeframe = '5m'
                        elif '6hour' in filename or '6h' in filename:
                            detected_timeframe = '6h'
                        elif '1day' in filename or '1d' in filename or 'daily' in filename:
                            detected_timeframe = '1d'
                        elif 'yahoo' in filename:
                            detected_timeframe = 'multi-year'
                        elif '1000wks' in filename:
                            detected_timeframe = '1000wks'
                        elif '200wks' in filename:
                            detected_timeframe = '200wks'
                        elif '10yr' in filename:
                            detected_timeframe = '10yr'
                        elif '20yr' in filename:
                            detected_timeframe = '20yr'

                    # Skip corrupted files
                    if 'BTCUSD-1d-1000wks-data.csv' in filename:
                        print(f"⚠️ Skipping known corrupted file: {filename}")
                        continue

                    discovered_sources.append({
                        'path': file_path,
                        'symbol': symbol,
                        'timeframe': detected_timeframe,
                        'provider': provider_name,
                        'filename': filename
                    })

        # Sort by symbol and timeframe for organized testing
        discovered_sources.sort(key=lambda x: (x['symbol'], x['timeframe'], x['provider']))

        print(f"\n✅ Discovered {len(discovered_sources)} data sources")

        # Display summary
        symbols = set(s['symbol'] for s in discovered_sources)
        print(f"📊 Assets found: {', '.join(sorted(symbols))}")
        print(f"⏰ Timeframes: {', '.join(timeframes)}")

        return discovered_sources

    def validate_data_quality(self, data, source_info):
        """
        🛡️ Validate data quality before testing
        """
        if not self.validator:
            return True, 100, "Validator not available"

        try:
            is_valid, score, issues = self.validator.validate_data(
                data,
                source_name=f"{source_info['symbol']}_{source_info['provider']}"
            )

            if score < self.min_quality_score:
                return False, score, f"Quality score {score} below minimum {self.min_quality_score}"

            return is_valid, score, issues
        except Exception as e:
            print(f"⚠️ Validation error: {e}")
            return True, 0, "Validation failed"

    def load_and_prepare_data(self, source_info):
        """
        📊 Load and prepare data for backtesting
        """
        try:
            # Load data
            data = pd.read_csv(source_info['path'], index_col=0, parse_dates=True)

            # Ensure required columns exist
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']

            # Handle different column naming conventions
            column_mapping = {
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }

            data.rename(columns=column_mapping, inplace=True)

            # Check for required columns
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                print(f"⚠️ Missing columns: {missing_cols}")
                return None

            # Basic data validation
            if len(data) < 100:
                print(f"⚠️ Insufficient data: only {len(data)} rows")
                return None

            # Remove any rows with NaN values
            data = data[required_cols].dropna()

            return data

        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return None

    def test_single_source(self, source_info):
        """
        🎯 Test BinHV45 strategy on a single data source with NATIVE RESULTS
        """
        print(f"\n{'='*100}")
        print(f"🚀 Testing: {source_info['symbol']} | {source_info['timeframe']} | {source_info['provider']}")
        print(f"📁 File: {source_info['filename']}")
        print(f"{'='*100}")

        # Load and prepare data
        data = self.load_and_prepare_data(source_info)

        if data is None:
            print("❌ Failed to load data - skipping")
            return None

        print(f"📊 Data loaded: {len(data)} bars from {data.index[0]} to {data.index[-1]}")

        # Validate data quality
        if self.validator:
            is_valid, score, issues = self.validate_data_quality(data, source_info)
            print(f"🛡️ Data Quality Score: {score}/100")

            if not is_valid:
                print(f"❌ Data validation failed: {issues}")
                return None

        # 🚨 MANDATORY: Use enhanced_backtest_runner for native results display
        try:
            # Create proper data source info for display
            data_source_info = create_data_source_info(
                source_info['path'],
                symbol=source_info['symbol'],
                timeframe=source_info['timeframe'],
                provider=source_info['provider']
            )

            # Run backtest with FULL NATIVE RESULTS DISPLAY
            summary_stats, full_stats = enhanced_backtest_runner(
                data=data,
                strategy_class=BinHV45Strategy,
                data_source_info=data_source_info,
                strategy_name="BinHV45 Mean-Reversion",
                cash=10000,
                commission=0.002,
                exclusive_orders=True
            )

            # Store results for comparison
            self.all_results.append({
                **source_info,
                **summary_stats,
                'full_stats': full_stats
            })

            return summary_stats

        except Exception as e:
            print(f"❌ Backtest error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def test_all_sources(self, max_sources_per_symbol=3):
        """
        🌍 Test strategy across ALL discovered data sources
        """
        print("\n" + "="*100)
        print("🌍 COMPREHENSIVE MULTI-ASSET TESTING STARTING")
        print("="*100)

        # Discover all available sources - UPDATED to include ALL timeframes
        sources = self.discover_data_sources(timeframes=['1m', '5m', '6h', '1d', '10yr', '20yr', '1000wks', '200wks'])

        if not sources:
            print("❌ No data sources found!")
            return

        # Group by symbol for organized testing
        from collections import defaultdict
        sources_by_symbol = defaultdict(list)

        for source in sources:
            sources_by_symbol[source['symbol']].append(source)

        # Test each symbol
        tested_count = 0
        successful_tests = 0

        for symbol in sorted(sources_by_symbol.keys()):
            print(f"\n{'='*80}")
            print(f"📊 Testing {symbol}")
            print(f"{'='*80}")

            # Test up to max_sources_per_symbol for this symbol
            symbol_sources = sources_by_symbol[symbol][:max_sources_per_symbol]

            for source_info in symbol_sources:
                result = self.test_single_source(source_info)
                tested_count += 1

                if result:
                    successful_tests += 1

        # Generate comprehensive report
        self.generate_comprehensive_report()

        print("\n" + "="*100)
        print(f"✅ TESTING COMPLETE: {successful_tests}/{tested_count} successful")
        print("="*100)

    def generate_comprehensive_report(self):
        """
        📊 Generate comprehensive performance report and rankings
        """
        if not self.all_results:
            print("❌ No results to report")
            return

        print("\n" + "="*100)
        print("📊 COMPREHENSIVE PERFORMANCE ANALYSIS")
        print("="*100)

        # Convert to DataFrame for analysis
        df_results = pd.DataFrame(self.all_results)

        # Remove full_stats column for CSV export
        df_export = df_results.drop(columns=['full_stats'], errors='ignore')

        # Sort by Sharpe ratio (best to worst)
        df_sorted = df_export.sort_values('Sharpe', ascending=False, na_position='last')

        print("\n🏆 TOP PERFORMERS BY SHARPE RATIO:")
        print("="*60)

        # Display top 5 performers
        top_performers = df_sorted.head(5)
        for idx, row in top_performers.iterrows():
            print(f"{row['Symbol']} ({row['Timeframe']}, {row['Provider']}):")
            print(f"  📈 Return: {row['Return_%']:.2f}%")
            print(f"  📊 Sharpe: {row['Sharpe']:.3f}")
            print(f"  🎯 Win Rate: {row['Win_Rate_%']:.1f}%")
            print(f"  📉 Max DD: {row['Max_DD_%']:.2f}%")
            print(f"  🔢 Trades: {row['Trades']:.0f}")
            print()

        # Asset performance summary
        print("\n📊 ASSET PERFORMANCE SUMMARY:")
        print("="*60)

        asset_summary = df_export.groupby('Symbol').agg({
            'Return_%': 'mean',
            'Sharpe': 'mean',
            'Win_Rate_%': 'mean',
            'Trades': 'sum',
            'Max_DD_%': 'mean'
        }).sort_values('Sharpe', ascending=False)

        for symbol in asset_summary.index[:10]:  # Top 10 assets
            stats = asset_summary.loc[symbol]
            print(f"{symbol}:")
            print(f"  📈 Avg Return: {stats['Return_%']:.2f}%")
            print(f"  📊 Avg Sharpe: {stats['Sharpe']:.3f}")
            print(f"  🎯 Avg Win Rate: {stats['Win_Rate_%']:.1f}%")
            print(f"  📉 Avg Max DD: {stats['Max_DD_%']:.2f}%")
            print(f"  🔢 Total Trades: {stats['Trades']:.0f}")
            print()

        # Timeframe comparison
        print("\n⏰ TIMEFRAME PERFORMANCE COMPARISON:")
        print("="*60)

        timeframe_summary = df_export.groupby('Timeframe').agg({
            'Return_%': 'mean',
            'Sharpe': 'mean',
            'Win_Rate_%': 'mean',
            'Trades': 'mean'
        })

        for timeframe in timeframe_summary.index:
            stats = timeframe_summary.loc[timeframe]
            print(f"{timeframe}:")
            print(f"  📈 Avg Return: {stats['Return_%']:.2f}%")
            print(f"  📊 Avg Sharpe: {stats['Sharpe']:.3f}")
            print(f"  🎯 Avg Win Rate: {stats['Win_Rate_%']:.1f}%")
            print(f"  🔢 Avg Trades: {stats['Trades']:.1f}")
            print()

        # Save results to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"binhv45_results_{timestamp}.csv"
        csv_path = os.path.join(self.results_path, csv_filename)

        df_export.to_csv(csv_path, index=False)
        print(f"\n💾 Results saved to: {csv_path}")

        # Production readiness assessment
        self.assess_production_readiness(df_results)

    def assess_production_readiness(self, df_results):
        """
        🚀 Assess strategy's readiness for live trading deployment
        """
        print("\n" + "="*100)
        print("🚀 PRODUCTION READINESS ASSESSMENT")
        print("="*100)

        # Calculate overall metrics
        avg_sharpe = df_results['Sharpe'].mean()
        avg_return = df_results['Return_%'].mean()
        avg_win_rate = df_results['Win_Rate_%'].mean()
        avg_max_dd = df_results['Max_DD_%'].mean()
        total_tests = len(df_results)
        profitable_tests = len(df_results[df_results['Return_%'] > 0])

        print(f"📊 Overall Statistics:")
        print(f"  Tests Run: {total_tests}")
        print(f"  Profitable: {profitable_tests}/{total_tests} ({profitable_tests/total_tests*100:.1f}%)")
        print(f"  Avg Sharpe: {avg_sharpe:.3f}")
        print(f"  Avg Return: {avg_return:.2f}%")
        print(f"  Avg Win Rate: {avg_win_rate:.1f}%")
        print(f"  Avg Max DD: {avg_max_dd:.2f}%")

        # Production readiness criteria
        print(f"\n✅ Production Readiness Criteria:")

        criteria = {
            "Positive Average Sharpe (>0.5)": avg_sharpe > 0.5,
            "Positive Average Return": avg_return > 0,
            "Win Rate > 40%": avg_win_rate > 40,
            "Max Drawdown < 20%": abs(avg_max_dd) < 20,
            "Profitable in >50% of tests": (profitable_tests/total_tests) > 0.5
        }

        passed_criteria = 0
        for criterion, passed in criteria.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {criterion}: {status}")
            if passed:
                passed_criteria += 1

        # Final recommendation
        print(f"\n🎯 FINAL ASSESSMENT:")
        if passed_criteria >= 4:
            print("  ✅ Strategy shows STRONG potential for live trading")
            print("  💡 Recommendation: Consider paper trading with small capital")
        elif passed_criteria >= 3:
            print("  ⚠️ Strategy shows MODERATE potential")
            print("  💡 Recommendation: Further optimization recommended")
        else:
            print("  ❌ Strategy needs significant improvement")
            print("  💡 Recommendation: Review entry/exit logic and parameters")

        # Specific recommendations
        print(f"\n💡 OPTIMIZATION RECOMMENDATIONS:")

        if avg_sharpe < 0.5:
            print("  • Consider adjusting BB period or standard deviations")

        if avg_win_rate < 45:
            print("  • Review entry conditions - may be too restrictive")

        if abs(avg_max_dd) > 15:
            print("  • Tighten stop loss or reduce position sizing")

        if avg_return < 0:
            print("  • Adjust take profit target or entry thresholds")

        # Best performing configuration
        best_config = df_results.nlargest(1, 'Sharpe').iloc[0]
        print(f"\n🏆 BEST CONFIGURATION:")
        print(f"  Symbol: {best_config['Symbol']}")
        print(f"  Timeframe: {best_config['Timeframe']}")
        print(f"  Provider: {best_config['Provider']}")
        print(f"  Sharpe: {best_config['Sharpe']:.3f}")
        print(f"  Return: {best_config['Return_%']:.2f}%")


def main():
    """
    🚀 Main execution function
    """
    print("="*100)
    print("🌙 BinHV45 Mean-Reversion Strategy Multi-Asset Testing 🌙")
    print("="*100)

    # Initialize tester
    tester = BinHV45MultiAssetTester()

    # Run comprehensive testing across all assets
    tester.test_all_sources(max_sources_per_symbol=2)  # Test up to 2 sources per symbol

    print("\n🌙💫🚀 BinHV45 Multi-Asset Testing Complete 🌙💫🚀")


if __name__ == "__main__":
    main()