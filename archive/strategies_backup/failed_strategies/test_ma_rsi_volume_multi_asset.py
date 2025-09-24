"""
🚀 Comprehensive Multi-Asset Testing for MA-RSI-Volume Hybrid Strategy 🚀
==========================================================================
Tests the MA-RSI-Volume Hybrid strategy across ALL available cryptocurrencies,
timeframes, and data providers for comprehensive validation.

Features:
- Tests ALL available cryptocurrencies (BTC, ETH, CRO, HBAR, LINK, XRP)
- Multiple timeframes (1d, 6h, 5m where available)
- Multiple data providers (Coinbase, Yahoo, CoinGecko, etc.)
- Comprehensive performance metrics and rankings
- Signal frequency analysis
- Asset suitability assessment

Author: Bobby's Algo Trading System 🌙💫
Date: 2025-01-18
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 🚀 Import strategy and backtesting framework
from backtesting import Backtest
# Try loading adaptive strategy first, fallback to hybrid
try:
    from ma_rsi_volume_adaptive_strategy import MARSIVolumeAdaptiveStrategy as MARSIVolumeHybridStrategy
    print("📊 Using Adaptive Strategy (more signals)")
except:
    from ma_rsi_volume_hybrid_strategy import MARSIVolumeHybridStrategy
    print("📊 Using Original Hybrid Strategy")

from ma_rsi_volume_hybrid_strategy import (
    validate_data_for_strategy,
    analyze_strategy_signals
)

# 🌙 Import universal native results display
import sys
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import (
    enhanced_backtest_runner,
    create_data_source_info,
    display_full_native_results
)

# 🛡️ Import data validation
try:
    from trading_functions import (
        DataQualityValidator,
        validate_data_source_quality
    )
    DATA_VALIDATION_AVAILABLE = True
except ImportError:
    print("⚠️ Data validation not available - proceeding without quality checks")
    DATA_VALIDATION_AVAILABLE = False

print("🚀 MA-RSI-Volume Multi-Asset Tester Starting... 💫")
print("=" * 100)


def load_and_validate_data(file_path):
    """
    📊 Load and validate data from CSV file 📊

    Returns tuple of (data_df, is_valid, validation_message, quality_score)
    """
    try:
        # Load data - handle different date column names
        df = pd.read_csv(file_path)

        # Find the date column
        date_col = None
        for col in df.columns:
            if col.lower() in ['date', 'datetime', 'time', 'timestamp']:
                date_col = col
                break

        if date_col:
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col)
            df.index.name = 'Date'

        # Standardize column names
        column_mapping = {
            'date': 'Date',
            'datetime': 'Date',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }

        # Rename columns to standard format
        df.columns = [column_mapping.get(col.lower(), col) for col in df.columns]

        # Validate data quality if available
        quality_score = 100  # Default if validation not available
        if DATA_VALIDATION_AVAILABLE:
            try:
                validation_result = validate_data_source_quality(df, file_path)
                quality_score = validation_result.get('quality_score', 0)
                if quality_score < 75:
                    return None, False, f"Quality score too low: {quality_score}", quality_score
            except Exception as e:
                print(f"   ⚠️ Quality validation failed: {e}")

        # Validate for strategy requirements
        is_valid, message = validate_data_for_strategy(df)

        if not is_valid:
            return None, False, message, quality_score

        return df, True, "Data loaded successfully", quality_score

    except Exception as e:
        return None, False, f"Error loading data: {e}", 0


def extract_asset_info(file_path):
    """
    🔍 Extract asset information from file path 🔍
    """
    filename = os.path.basename(file_path)
    parts = filename.replace('.csv', '').split('-')

    # Extract symbol - handle different formats
    symbol = parts[0].upper()
    if 'USD' in symbol:
        symbol = symbol.replace('USD', '')
        symbol = symbol.replace('USD', '')  # Handle BTCUSD, ETHUSD etc

    # Extract timeframe
    timeframe = 'unknown'
    for part in parts:
        if part in ['1m', '5m', '15m', '1h', '6h', '1d', 'day', 'hour', 'daily']:
            timeframe = part
            break

    # Extract provider
    provider = 'unknown'
    if 'coinbase' in file_path.lower():
        provider = 'Coinbase'
    elif 'yahoo' in file_path.lower():
        provider = 'Yahoo'
    elif 'coingecko' in file_path.lower():
        provider = 'CoinGecko'
    elif 'hyperliquid' in file_path.lower():
        provider = 'Hyperliquid'
    elif 'cryptocompare' in file_path.lower():
        provider = 'CryptoCompare'
    elif 'coinmarketcap' in file_path.lower():
        provider = 'CoinMarketCap'

    return symbol, timeframe, provider


def test_single_asset(file_path, show_details=True):
    """
    🎯 Test strategy on single asset/data source 🎯
    """
    print(f"\n📊 Testing: {os.path.basename(file_path)}")
    print("-" * 80)

    # Load and validate data
    df, is_valid, message, quality_score = load_and_validate_data(file_path)

    if not is_valid:
        print(f"   ❌ Skipping: {message}")
        return None

    # Extract asset info
    symbol, timeframe, provider = extract_asset_info(file_path)

    # Quick signal analysis
    signal_analysis = analyze_strategy_signals(df)
    print(f"   📊 Signal Analysis:")
    print(f"      - Perfect signals: {signal_analysis['perfect_signals']} "
          f"({signal_analysis['signal_frequency']:.2f}% of bars)")
    print(f"      - Price > MA: {signal_analysis['price_above_ma_pct']:.1f}%")
    print(f"      - RSI Oversold: {signal_analysis['rsi_oversold_pct']:.1f}%")
    print(f"      - Volume Spikes: {signal_analysis['volume_spike_pct']:.1f}%")

    # Skip if no signals would be generated
    if signal_analysis['perfect_signals'] == 0:
        print(f"   ⚠️ No signals generated - skipping backtest")
        return {
            'Symbol': symbol,
            'Timeframe': timeframe,
            'Provider': provider,
            'File': file_path,
            'Quality_Score': quality_score,
            'Signals': 0,
            'Status': 'No Signals'
        }

    # Create data source info
    data_source_info = {
        'path': file_path,
        'symbol': symbol,
        'timeframe': timeframe,
        'provider': provider
    }

    # Run backtest with native results display
    print(f"\n   🚀 Running backtest for {symbol} ({timeframe}, {provider})...")
    try:
        summary_stats, full_stats = enhanced_backtest_runner(
            data=df,
            strategy_class=MARSIVolumeHybridStrategy,
            data_source_info=data_source_info,
            strategy_name="MA-RSI-Volume Hybrid",
            cash=10000,
            commission=0.002
        )

        # Add additional metrics
        summary_stats['Quality_Score'] = quality_score
        summary_stats['Perfect_Signals'] = signal_analysis['perfect_signals']
        summary_stats['Signal_Frequency_%'] = signal_analysis['signal_frequency']
        summary_stats['Status'] = 'Success'

        return summary_stats

    except Exception as e:
        print(f"   ❌ Backtest failed: {e}")
        return {
            'Symbol': symbol,
            'Timeframe': timeframe,
            'Provider': provider,
            'File': file_path,
            'Quality_Score': quality_score,
            'Signals': signal_analysis['perfect_signals'],
            'Status': f'Error: {str(e)}'
        }


def find_all_data_files():
    """
    🔍 Find all available data files for testing 🔍
    """
    data_dir = '/Users/bobbyyo/Projects/algo-fun/data'

    # Priority order for data sources
    priority_patterns = [
        'coinbase/*enhanced*.csv',
        'yahoo/*.csv',
        'coingecko/*.csv',
        'hyperliquid/*.csv',
        'cryptocompare/*.csv',
        'coinmarketcap/*.csv',
        '*.csv'  # Root level files
    ]

    all_files = []
    for pattern in priority_patterns:
        files = glob.glob(os.path.join(data_dir, pattern))
        all_files.extend(files)

    # Remove duplicates while preserving order
    seen = set()
    unique_files = []
    for f in all_files:
        if f not in seen and 'monitor' not in f and 'metrics' not in f:
            seen.add(f)
            unique_files.append(f)

    return unique_files


def categorize_files_by_asset(files):
    """
    📊 Categorize files by asset symbol 📊
    """
    assets = {}
    for file_path in files:
        symbol, _, _ = extract_asset_info(file_path)
        if symbol not in assets:
            assets[symbol] = []
        assets[symbol].append(file_path)
    return assets


def run_comprehensive_test():
    """
    🚀 Run comprehensive multi-asset testing 🚀
    """
    print("\n" + "=" * 100)
    print("🌙 MA-RSI-VOLUME HYBRID STRATEGY - COMPREHENSIVE MULTI-ASSET TESTING 🌙")
    print("=" * 100)

    # Find all data files
    print("\n📊 Discovering available data sources...")
    all_files = find_all_data_files()
    print(f"✅ Found {len(all_files)} data files")

    # Categorize by asset
    assets = categorize_files_by_asset(all_files)
    print(f"📊 Assets discovered: {', '.join(sorted(assets.keys()))}")

    # Test each asset
    all_results = []
    asset_summaries = {}

    for asset_symbol in sorted(assets.keys()):
        asset_files = assets[asset_symbol]
        print(f"\n{'='*100}")
        print(f"🎯 Testing {asset_symbol} - {len(asset_files)} data sources")
        print(f"{'='*100}")

        asset_results = []
        for file_path in asset_files:
            result = test_single_asset(file_path)
            if result:
                all_results.append(result)
                asset_results.append(result)

        # Summarize results for this asset
        if asset_results:
            successful_tests = [r for r in asset_results if r.get('Status') == 'Success' and r.get('Trades', 0) > 0]
            if successful_tests:
                best_result = max(successful_tests, key=lambda x: x.get('Sharpe', -999))
                asset_summaries[asset_symbol] = {
                    'Total_Tests': len(asset_results),
                    'Successful_Tests': len(successful_tests),
                    'Best_Sharpe': best_result.get('Sharpe', np.nan),
                    'Best_Return_%': best_result.get('Return_%', 0),
                    'Best_Source': f"{best_result.get('Provider')} {best_result.get('Timeframe')}",
                    'Avg_Signals': np.mean([r.get('Perfect_Signals', 0) for r in asset_results]),
                    'Avg_Win_Rate_%': np.mean([r.get('Win_Rate_%', 0) for r in successful_tests]) if successful_tests else 0
                }

    # Display comprehensive results
    print("\n" + "=" * 100)
    print("🏆 COMPREHENSIVE MULTI-ASSET PERFORMANCE SUMMARY 🏆")
    print("=" * 100)

    # Convert to DataFrame for better display
    if asset_summaries:
        summary_df = pd.DataFrame(asset_summaries).T
        summary_df = summary_df.sort_values('Best_Sharpe', ascending=False)

        print("\n📊 Asset Performance Rankings (by Sharpe Ratio):")
        print("-" * 80)
        for idx, (asset, row) in enumerate(summary_df.iterrows(), 1):
            print(f"\n{idx}. {asset}:")
            print(f"   📈 Best Sharpe: {row['Best_Sharpe']:.3f}")
            print(f"   💰 Best Return: {row['Best_Return_%']:.2f}%")
            print(f"   🎯 Win Rate: {row['Avg_Win_Rate_%']:.1f}%")
            print(f"   📊 Avg Signals: {row['Avg_Signals']:.1f}")
            print(f"   🏢 Best Source: {row['Best_Source']}")
            print(f"   ✅ Successful Tests: {int(row['Successful_Tests'])}/{int(row['Total_Tests'])}")

    # Create results DataFrame
    if all_results:
        results_df = pd.DataFrame(all_results)

        # Save results to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = f'/Users/bobbyyo/Projects/algo-fun/strategies/results/ma_rsi_volume_results_{timestamp}.csv'
        os.makedirs(os.path.dirname(results_file), exist_ok=True)
        results_df.to_csv(results_file, index=False)
        print(f"\n💾 Results saved to: {results_file}")

        # Top performers across all tests
        successful_df = results_df[results_df['Status'] == 'Success'].copy()
        if not successful_df.empty and 'Sharpe' in successful_df.columns:
            successful_df = successful_df.sort_values('Sharpe', ascending=False)

            print("\n🏆 TOP 10 PERFORMERS (All Assets/Timeframes):")
            print("-" * 80)
            for idx, row in successful_df.head(10).iterrows():
                print(f"   {row['Symbol']} ({row['Timeframe']}, {row['Provider']}): "
                      f"Sharpe={row['Sharpe']:.3f}, Return={row['Return_%']:.2f}%, "
                      f"Trades={int(row.get('Trades', 0))}")

        # Strategy insights
        print("\n💡 STRATEGY INSIGHTS:")
        print("-" * 80)
        print(f"📊 Total tests run: {len(all_results)}")
        print(f"✅ Successful tests: {len(successful_df)}")
        print(f"📈 Tests with trades: {len(successful_df[successful_df['Trades'] > 0])}")

        if not successful_df.empty:
            print(f"\n🎯 Performance Statistics:")
            print(f"   - Average Sharpe: {successful_df['Sharpe'].mean():.3f}")
            print(f"   - Average Return: {successful_df['Return_%'].mean():.2f}%")
            print(f"   - Average Win Rate: {successful_df['Win_Rate_%'].mean():.1f}%")
            print(f"   - Average Trades: {successful_df['Trades'].mean():.1f}")

            # Best timeframes
            timeframe_performance = successful_df.groupby('Timeframe')['Sharpe'].agg(['mean', 'count'])
            timeframe_performance = timeframe_performance.sort_values('mean', ascending=False)
            print(f"\n⏰ Best Timeframes:")
            for tf, row in timeframe_performance.iterrows():
                if row['count'] > 0:
                    print(f"   - {tf}: Avg Sharpe={row['mean']:.3f} ({int(row['count'])} tests)")

            # Best providers
            provider_performance = successful_df.groupby('Provider')['Sharpe'].agg(['mean', 'count'])
            provider_performance = provider_performance.sort_values('mean', ascending=False)
            print(f"\n🏢 Best Data Providers:")
            for provider, row in provider_performance.iterrows():
                if row['count'] > 0:
                    print(f"   - {provider}: Avg Sharpe={row['mean']:.3f} ({int(row['count'])} tests)")

        # Signal frequency analysis
        signal_cols = ['Perfect_Signals', 'Signal_Frequency_%']
        if all([col in results_df.columns for col in signal_cols]):
            print(f"\n📡 Signal Generation Analysis:")
            print(f"   - Avg Perfect Signals: {results_df['Perfect_Signals'].mean():.1f}")
            print(f"   - Avg Signal Frequency: {results_df['Signal_Frequency_%'].mean():.2f}%")

            # Assets with most signals
            asset_signals = results_df.groupby('Symbol')['Perfect_Signals'].mean().sort_values(ascending=False)
            print(f"\n   📊 Assets by Signal Generation:")
            for asset, signals in asset_signals.head(5).items():
                print(f"      - {asset}: {signals:.1f} avg signals")

    print("\n" + "=" * 100)
    print("✅ MA-RSI-VOLUME HYBRID STRATEGY TESTING COMPLETE! 🌙💫🚀")
    print("=" * 100)

    return results_df if all_results else None


def run_optimization_test(file_path):
    """
    🔧 Run parameter optimization for a specific asset 🔧
    """
    print(f"\n🔧 Running parameter optimization for: {os.path.basename(file_path)}")
    print("-" * 80)

    # Load data
    df, is_valid, message, quality_score = load_and_validate_data(file_path)
    if not is_valid:
        print(f"❌ Cannot optimize: {message}")
        return

    # Parameter ranges to test
    param_ranges = {
        'ma_period': [10, 15, 20, 25, 30],
        'rsi_period': [10, 14, 20],
        'rsi_oversold': [25, 30, 35],
        'volume_spike': [1.1, 1.2, 1.3, 1.5]
    }

    print(f"📊 Testing {np.prod([len(v) for v in param_ranges.values()])} parameter combinations...")

    best_sharpe = -999
    best_params = {}
    results = []

    # Test all combinations
    from itertools import product
    for params in product(*param_ranges.values()):
        param_dict = dict(zip(param_ranges.keys(), params))

        # Quick signal check
        signal_analysis = analyze_strategy_signals(df, param_dict)
        if signal_analysis['perfect_signals'] < 5:  # Skip if too few signals
            continue

        # Run backtest
        try:
            bt = Backtest(df, MARSIVolumeHybridStrategy, cash=10000, commission=0.002)

            # Set parameters
            for key, value in param_dict.items():
                setattr(MARSIVolumeHybridStrategy, key, value)

            stats = bt.run()
            sharpe = stats.get('Sharpe Ratio', -999)

            results.append({
                **param_dict,
                'Sharpe': sharpe,
                'Return_%': stats.get('Return [%]', 0),
                'Trades': stats.get('# Trades', 0),
                'Win_Rate_%': stats.get('Win Rate [%]', 0)
            })

            if sharpe > best_sharpe:
                best_sharpe = sharpe
                best_params = param_dict

        except:
            continue

    # Display results
    if results:
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('Sharpe', ascending=False)

        print("\n🏆 Top 5 Parameter Combinations:")
        print(results_df.head(5))

        print(f"\n✨ Best Parameters:")
        for key, value in best_params.items():
            print(f"   - {key}: {value}")
        print(f"   📈 Sharpe Ratio: {best_sharpe:.3f}")


# Main execution
if __name__ == "__main__":
    # Run comprehensive test
    results = run_comprehensive_test()

    # Optional: Run optimization on best performing asset
    if results is not None and not results.empty:
        successful_results = results[results['Status'] == 'Success']
        if not successful_results.empty:
            best_result = successful_results.nlargest(1, 'Sharpe').iloc[0]
            if 'File' in best_result:
                print("\n" + "=" * 100)
                print("🔧 RUNNING OPTIMIZATION ON BEST PERFORMER 🔧")
                print("=" * 100)
                # Note: Commenting out optimization to focus on comprehensive testing
                # run_optimization_test(best_result['File'])

    print("\n🌙💫🚀 All testing complete! 🌙💫🚀")