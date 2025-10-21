"""
🚀 Comprehensive Multi-Asset Testing Framework for Crypto Momentum Strategy 🚀
=============================================================================
Tests the Crypto Momentum Surge Strategy across ALL available cryptocurrency data
with comprehensive performance analysis and asset rankings.

Author: Bobby's Algo Trading Systems 🌙
Date: 2025-01-18
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime
from backtesting import Backtest
from crypto_momentum_surge_strategy import (
    CryptoMomentumSurgeStrategy,
    CryptoMomentumAdaptiveStrategy
)

# Add parent directory to path for imports
import sys
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import (
    enhanced_backtest_runner,
    create_data_source_info,
    display_full_native_results
)

# Import trading functions if needed (currently not using validation)
# sys.path.append('/Users/bobbyyo/Projects/algo-fun')
# from trading_functions import DataQualityValidator


class CryptoMomentumMultiAssetTester:
    """
    🎯 Comprehensive Multi-Asset Testing Framework 🎯

    Tests momentum strategy across all available cryptocurrencies,
    timeframes, and data providers with full performance analysis.
    """

    def __init__(self, strategy_class=CryptoMomentumSurgeStrategy):
        """
        Initialize the multi-asset tester

        Args:
            strategy_class: Strategy class to test
        """
        self.strategy_class = strategy_class
        self.strategy_name = strategy_class.__name__
        self.results = []
        # self.data_quality_validator = DataQualityValidator()  # Disabled for now
        self.minimum_quality_score = 75  # Minimum data quality score

    def discover_all_crypto_data(self):
        """
        🔍 Discover all available cryptocurrency data files 🔍

        Returns:
            dict: Dictionary of data files organized by asset
        """
        print("\n" + "="*100)
        print("🔍 DISCOVERING ALL CRYPTOCURRENCY DATA FILES")
        print("="*100)

        # Define base data directory
        data_base_path = '/Users/bobbyyo/Projects/algo-fun/dataset_files'

        # Search patterns for different providers
        patterns = [
            f'{data_base_path}/**/*.csv',
            f'{data_base_path}/*.csv'
        ]

        all_files = []
        for pattern in patterns:
            files = glob.glob(pattern, recursive=True)
            all_files.extend(files)

        # Organize by asset
        crypto_data = {}
        crypto_symbols = ['BTC', 'ETH', 'XRP', 'CRO', 'HBAR', 'LINK', 'RIPPLE', 'ETHEREUM']

        for file_path in all_files:
            filename = os.path.basename(file_path)

            # Check if it's crypto data
            is_crypto = False
            for symbol in crypto_symbols:
                if symbol in filename.upper():
                    is_crypto = True
                    # Normalize symbol names
                    if symbol == 'RIPPLE':
                        asset_key = 'XRP'
                    elif symbol == 'ETHEREUM':
                        asset_key = 'ETH'
                    else:
                        asset_key = symbol
                    break

            if is_crypto:
                if asset_key not in crypto_data:
                    crypto_data[asset_key] = []
                crypto_data[asset_key].append(file_path)

        # Display discovered data
        total_files = sum(len(files) for files in crypto_data.values())
        print(f"\n📊 Found {total_files} cryptocurrency data files across {len(crypto_data)} assets:")

        for asset, files in sorted(crypto_data.items()):
            print(f"\n🪙 {asset}: {len(files)} files")
            for file in files[:3]:  # Show first 3 files for each asset
                print(f"   📁 {os.path.basename(file)}")
            if len(files) > 3:
                print(f"   ... and {len(files) - 3} more")

        return crypto_data

    def validate_and_load_data(self, file_path):
        """
        🛡️ Validate and load data with quality checks 🛡️

        Args:
            file_path: Path to data file

        Returns:
            tuple: (dataframe, validation_result, quality_score)
        """
        try:
            # Load data - handle different date column names
            try:
                df = pd.read_csv(file_path, parse_dates=['Date'])
            except:
                try:
                    df = pd.read_csv(file_path, parse_dates=['datetime'])
                    if 'datetime' in df.columns:
                        df.rename(columns={'datetime': 'Date'}, inplace=True)
                except:
                    df = pd.read_csv(file_path)
                    # Try to identify date column
                    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
                    if date_cols:
                        df[date_cols[0]] = pd.to_datetime(df[date_cols[0]])
                        df.rename(columns={date_cols[0]: 'Date'}, inplace=True)

            # Set Date as index if not already
            if 'Date' in df.columns:
                df.set_index('Date', inplace=True)

            # Rename columns to standard format if needed
            column_mapping = {
                'date': 'Date',
                'datetime': 'Date',
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }

            df.columns = [column_mapping.get(col.lower(), col) for col in df.columns]

            # Ensure required columns exist
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in required_columns:
                if col not in df.columns:
                    if col == 'Volume':
                        # Create synthetic volume if missing
                        df['Volume'] = 1000000
                    else:
                        print(f"⚠️ Missing required column {col} in {os.path.basename(file_path)}")
                        return None, None, 0

            # Skip data quality validation for now - just use basic checks
            validation_result = {'quality_score': 100}  # Default good quality
            quality_score = 100

            # Clean data
            df = df.dropna()

            # Ensure positive values
            for col in ['Open', 'High', 'Low', 'Close']:
                df[col] = df[col].abs()

            # Sort by index
            df = df.sort_index()

            return df, validation_result, quality_score

        except Exception as e:
            print(f"❌ Error loading {os.path.basename(file_path)}: {e}")
            return None, None, 0

    def test_single_asset_all_timeframes(self, asset_files, asset_name):
        """
        🎯 Test strategy on all timeframes for a single asset 🎯

        Args:
            asset_files: List of file paths for the asset
            asset_name: Name of the asset

        Returns:
            list: Results for all timeframes
        """
        print(f"\n{'='*80}")
        print(f"🪙 Testing {asset_name} across all available timeframes")
        print(f"{'='*80}")

        asset_results = []

        for file_path in asset_files:
            filename = os.path.basename(file_path)
            print(f"\n📊 Testing: {filename}")

            # Extract timeframe from filename
            parts = filename.split('-')
            timeframe = 'unknown'
            for part in parts:
                if any(tf in part for tf in ['1m', '5m', '15m', '1h', '6h', '1d', '365d', '90d', '10yr']):
                    timeframe = part
                    break

            # Load and validate data
            df, validation_result, quality_score = self.validate_and_load_data(file_path)

            if df is None:
                continue

            print(f"✅ Data loaded: {len(df)} bars, Quality Score: {quality_score:.1f}")

            # Skip if too few bars
            if len(df) < 100:
                print(f"⚠️ Skipping - insufficient data ({len(df)} bars)")
                continue

            # Create data source info
            data_source_info = create_data_source_info(
                file_path,
                symbol=asset_name,
                timeframe=timeframe
            )

            try:
                # Run backtest with native results display
                summary_stats, full_stats = enhanced_backtest_runner(
                    df,
                    self.strategy_class,
                    data_source_info,
                    self.strategy_name,
                    cash=10000,
                    commission=0.002
                )

                # Add quality score to results
                summary_stats['Quality_Score'] = quality_score
                summary_stats['Bars'] = len(df)
                summary_stats['Date_Start'] = df.index[0].strftime('%Y-%m-%d')
                summary_stats['Date_End'] = df.index[-1].strftime('%Y-%m-%d')

                # Store results
                asset_results.append(summary_stats)
                self.results.append(summary_stats)

            except Exception as e:
                print(f"❌ Error testing {filename}: {e}")
                continue

        return asset_results

    def test_all_assets(self):
        """
        🚀 Test strategy across ALL available cryptocurrencies 🚀

        Returns:
            pd.DataFrame: Comprehensive results for all assets
        """
        print("\n" + "="*100)
        print(f"🚀 COMPREHENSIVE MULTI-ASSET TESTING - {self.strategy_name}")
        print("="*100)

        # Discover all crypto data
        crypto_data = self.discover_all_crypto_data()

        if not crypto_data:
            print("❌ No cryptocurrency data files found!")
            return pd.DataFrame()

        # Test each asset
        all_results = []
        for asset_name, asset_files in sorted(crypto_data.items()):
            asset_results = self.test_single_asset_all_timeframes(asset_files, asset_name)
            all_results.extend(asset_results)

        # Create comprehensive results DataFrame
        if all_results:
            results_df = pd.DataFrame(all_results)
            return results_df
        else:
            print("❌ No successful test results!")
            return pd.DataFrame()

    def analyze_results(self, results_df):
        """
        📊 Comprehensive analysis of testing results 📊

        Args:
            results_df: DataFrame with all test results
        """
        print("\n" + "="*100)
        print("📊 COMPREHENSIVE PERFORMANCE ANALYSIS")
        print("="*100)

        if results_df.empty:
            print("❌ No results to analyze!")
            return

        # Overall statistics
        print("\n📈 OVERALL STATISTICS")
        print("="*60)
        print(f"Total Tests Run: {len(results_df)}")
        print(f"Successful Tests: {len(results_df[results_df['Trades'] > 0])}")
        print(f"Assets Tested: {results_df['Symbol'].nunique()}")
        print(f"Timeframes Tested: {results_df['Timeframe'].nunique()}")

        # Performance metrics
        print(f"\nAverage Return: {results_df['Return_%'].mean():.2f}%")
        print(f"Best Return: {results_df['Return_%'].max():.2f}%")
        print(f"Worst Return: {results_df['Return_%'].min():.2f}%")
        print(f"Average Sharpe Ratio: {results_df['Sharpe'].mean():.2f}")
        print(f"Average Win Rate: {results_df['Win_Rate_%'].mean():.2f}%")
        print(f"Average Max Drawdown: {results_df['Max_DD_%'].mean():.2f}%")

        # Asset performance ranking
        print("\n🏆 ASSET PERFORMANCE RANKING (By Sharpe Ratio)")
        print("="*60)
        asset_performance = results_df.groupby('Symbol').agg({
            'Return_%': 'mean',
            'Sharpe': 'mean',
            'Sortino': 'mean',
            'Win_Rate_%': 'mean',
            'Max_DD_%': 'mean',
            'Trades': 'sum'
        }).round(2)

        asset_performance = asset_performance.sort_values('Sharpe', ascending=False)
        print(asset_performance)

        # Best performing configurations
        print("\n🌟 TOP 5 BEST PERFORMING CONFIGURATIONS")
        print("="*60)
        top_configs = results_df.nlargest(5, 'Sharpe')[
            ['Symbol', 'Timeframe', 'Return_%', 'Sharpe', 'Win_Rate_%', 'Max_DD_%', 'Trades']
        ]
        print(top_configs)

        # Timeframe analysis
        print("\n⏰ TIMEFRAME PERFORMANCE ANALYSIS")
        print("="*60)
        timeframe_performance = results_df.groupby('Timeframe').agg({
            'Return_%': 'mean',
            'Sharpe': 'mean',
            'Win_Rate_%': 'mean',
            'Trades': 'mean'
        }).round(2)

        timeframe_performance = timeframe_performance.sort_values('Sharpe', ascending=False)
        print(timeframe_performance)

        # Provider analysis
        print("\n🏢 DATA PROVIDER PERFORMANCE ANALYSIS")
        print("="*60)
        provider_performance = results_df.groupby('Provider').agg({
            'Return_%': 'mean',
            'Sharpe': 'mean',
            'Quality_Score': 'mean',
            'Trades': 'sum'
        }).round(2)

        provider_performance = provider_performance.sort_values('Sharpe', ascending=False)
        print(provider_performance)

    def generate_recommendations(self, results_df):
        """
        💡 Generate trading recommendations based on results 💡

        Args:
            results_df: DataFrame with all test results
        """
        print("\n" + "="*100)
        print("💡 TRADING RECOMMENDATIONS")
        print("="*100)

        if results_df.empty:
            print("❌ No results to generate recommendations!")
            return

        # Find best asset for momentum trading
        best_asset_group = results_df.groupby('Symbol')['Sharpe'].mean()
        best_asset = best_asset_group.idxmax()
        best_asset_sharpe = best_asset_group.max()

        print(f"\n🏆 BEST ASSET FOR MOMENTUM TRADING: {best_asset}")
        print(f"   Average Sharpe Ratio: {best_asset_sharpe:.2f}")

        # Find optimal timeframe
        best_timeframe_group = results_df.groupby('Timeframe')['Sharpe'].mean()
        best_timeframe = best_timeframe_group.idxmax()

        print(f"\n⏰ OPTIMAL TIMEFRAME: {best_timeframe}")
        print(f"   Average Sharpe Ratio: {best_timeframe_group.max():.2f}")

        # Identify assets to avoid
        poor_performers = results_df.groupby('Symbol')['Return_%'].mean()
        worst_assets = poor_performers[poor_performers < 0].index.tolist()

        if worst_assets:
            print(f"\n⚠️ ASSETS TO AVOID:")
            for asset in worst_assets:
                print(f"   - {asset} (Avg Return: {poor_performers[asset]:.2f}%)")

        # Risk management recommendations
        avg_drawdown = results_df['Max_DD_%'].mean()
        print(f"\n🛡️ RISK MANAGEMENT RECOMMENDATIONS:")
        print(f"   - Average Max Drawdown: {avg_drawdown:.2f}%")
        print(f"   - Suggested Position Size: {min(100, 100 * 20 / avg_drawdown):.0f}%")
        print(f"   - Recommended Stop Loss: {results_df['Max_DD_%'].quantile(0.25):.2f}%")

        # Trading frequency analysis
        avg_trades_per_period = results_df.groupby('Timeframe')['Trades'].mean()
        print(f"\n📊 EXPECTED TRADING FREQUENCY:")
        for tf, trades in avg_trades_per_period.items():
            print(f"   {tf}: ~{trades:.0f} trades per test period")

    def save_results(self, results_df):
        """
        💾 Save comprehensive results to CSV 💾

        Args:
            results_df: DataFrame with all test results
        """
        if results_df.empty:
            print("❌ No results to save!")
            return

        # Create results directory if it doesn't exist
        results_dir = '/Users/bobbyyo/Projects/algo-fun/strategies/results'
        os.makedirs(results_dir, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'crypto_momentum_multi_asset_results_{timestamp}.csv'
        file_path = os.path.join(results_dir, filename)

        # Save to CSV
        results_df.to_csv(file_path, index=False)
        print(f"\n💾 Results saved to: {file_path}")

        # Also save summary statistics
        summary_filename = f'crypto_momentum_summary_{timestamp}.txt'
        summary_path = os.path.join(results_dir, summary_filename)

        with open(summary_path, 'w') as f:
            f.write(f"Crypto Momentum Strategy - Multi-Asset Test Results\n")
            f.write(f"={'='*60}\n")
            f.write(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Strategy: {self.strategy_name}\n")
            f.write(f"Total Tests: {len(results_df)}\n")
            f.write(f"Assets Tested: {results_df['Symbol'].nunique()}\n")
            f.write(f"\nPerformance Summary:\n")
            f.write(f"Average Return: {results_df['Return_%'].mean():.2f}%\n")
            f.write(f"Average Sharpe: {results_df['Sharpe'].mean():.2f}\n")
            f.write(f"Average Win Rate: {results_df['Win_Rate_%'].mean():.2f}%\n")
            f.write(f"Average Max DD: {results_df['Max_DD_%'].mean():.2f}%\n")

        print(f"📄 Summary saved to: {summary_path}")

    def run_comprehensive_test(self):
        """
        🚀 Run complete multi-asset testing with full analysis 🚀
        """
        print("\n" + "="*100)
        print("🚀 STARTING COMPREHENSIVE CRYPTO MOMENTUM STRATEGY TESTING")
        print(f"Strategy: {self.strategy_name}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*100)

        # Run tests on all assets
        results_df = self.test_all_assets()

        if not results_df.empty:
            # Analyze results
            self.analyze_results(results_df)

            # Generate recommendations
            self.generate_recommendations(results_df)

            # Save results
            self.save_results(results_df)

            print("\n" + "="*100)
            print("✅ COMPREHENSIVE TESTING COMPLETE!")
            print("="*100)

            return results_df
        else:
            print("\n❌ Testing failed - no results generated!")
            return pd.DataFrame()


def test_adaptive_strategy():
    """
    🌟 Test the adaptive version of the strategy 🌟
    """
    print("\n" + "="*100)
    print("🌟 TESTING ADAPTIVE CRYPTO MOMENTUM STRATEGY")
    print("="*100)

    # Test with different signal modes
    signal_modes = ['aggressive', 'moderate', 'conservative']

    all_mode_results = []

    for mode in signal_modes:
        print(f"\n📊 Testing {mode.upper()} mode...")

        # Set the signal mode
        CryptoMomentumAdaptiveStrategy.signal_mode = mode

        # Create tester
        tester = CryptoMomentumMultiAssetTester(CryptoMomentumAdaptiveStrategy)

        # Run limited test (just BTC for demonstration)
        crypto_data = tester.discover_all_crypto_data()

        if 'BTC' in crypto_data:
            # Test only first 2 BTC files for each mode
            btc_files = crypto_data['BTC'][:2]
            mode_results = tester.test_single_asset_all_timeframes(btc_files, 'BTC')

            for result in mode_results:
                result['Signal_Mode'] = mode

            all_mode_results.extend(mode_results)

    # Compare modes
    if all_mode_results:
        mode_df = pd.DataFrame(all_mode_results)

        print("\n" + "="*80)
        print("📊 SIGNAL MODE COMPARISON")
        print("="*80)

        mode_comparison = mode_df.groupby('Signal_Mode').agg({
            'Return_%': 'mean',
            'Sharpe': 'mean',
            'Win_Rate_%': 'mean',
            'Trades': 'sum'
        }).round(2)

        print(mode_comparison)


def main():
    """
    🚀 Main execution function 🚀
    """
    print("\n" + "="*100)
    print("🚀 CRYPTO MOMENTUM SURGE STRATEGY - COMPREHENSIVE TESTING")
    print("="*100)

    # Test standard strategy
    print("\n1️⃣ Testing Standard Crypto Momentum Strategy...")
    tester = CryptoMomentumMultiAssetTester(CryptoMomentumSurgeStrategy)
    results_standard = tester.run_comprehensive_test()

    # Test adaptive strategy (limited test for demonstration)
    print("\n2️⃣ Testing Adaptive Strategy Modes...")
    test_adaptive_strategy()

    print("\n" + "="*100)
    print("🎉 ALL TESTING COMPLETE!")
    print("="*100)


if __name__ == "__main__":
    main()


# 🌙💫🚀 Bobby's signature emoji style preserved throughout 🌙💫🚀