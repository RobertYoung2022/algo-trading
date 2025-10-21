"""
🚀 MA-RSI-Volume Optimized Strategy - Comprehensive Multi-Asset Tester 🚀
=======================================================================
Tests optimized MA-RSI-Volume strategy across all available cryptocurrencies
with full native backtesting.py results display and performance comparison.

Validates optimization improvements:
- Before: 25-42% win rates, -58% to -99% returns
- Target: 50%+ win rates, positive returns, <20% drawdowns

Author: Bobby's Algo Trading System 🌙💫
Date: 2025-01-18
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import glob
import os
from datetime import datetime
import json

# Import strategies for comparison
from ma_rsi_volume_optimized_strategy import MARSIVolumeOptimizedStrategy
from ma_rsi_volume_adaptive_strategy import MARSIVolumeAdaptiveStrategy

# Import mandatory native results display module
import sys
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import (
    display_full_native_results,
    display_trades_summary,
    enhanced_backtest_runner
)

print("🚀 MA-RSI-Volume Optimized Multi-Asset Tester Starting... 💫")


def find_all_crypto_data(timeframe='1h'):
    """
    🔍 Find all available cryptocurrency data files

    Returns dict mapping symbols to file paths
    """

    print(f"\n🔍 Searching for {timeframe.upper()} cryptocurrency data...")

    # Search patterns
    patterns = [
        f'/Users/bobbyyo/Projects/algo-fun/data/*-{timeframe}-*.csv',
        f'/Users/bobbyyo/Projects/algo-fun/data/coinbase/*-{timeframe}-*.csv',
        f'/Users/bobbyyo/Projects/algo-fun/data/yahoo/*-{timeframe}*.csv',
        f'/Users/bobbyyo/Projects/algo-fun/data/cryptocompare/*-{timeframe}*.csv'
    ]

    # Collect all files
    all_files = []
    for pattern in patterns:
        found_files = glob.glob(pattern)
        all_files.extend(found_files)

    # Group by symbol
    symbol_files = {}
    crypto_symbols = ['BTC', 'ETH', 'CRO', 'HBAR', 'LINK', 'XRP', 'SOL', 'MATIC',
                     'ADA', 'AVAX', 'DOT', 'ATOM', 'ALGO', 'NEAR', 'FTM']

    for file_path in all_files:
        filename = os.path.basename(file_path).upper()

        # Skip known corrupted files
        if 'BTCUSD-1D-1000WKS' in filename:
            continue

        # Extract symbol
        for symbol in crypto_symbols:
            if symbol in filename:
                if symbol not in symbol_files:
                    symbol_files[symbol] = []
                symbol_files[symbol].append(file_path)
                break

    print(f"📊 Found data for {len(symbol_files)} cryptocurrencies:")
    for symbol, files in symbol_files.items():
        print(f"   {symbol}: {len(files)} data sources")

    return symbol_files


def test_strategy_on_asset(
    strategy_class,
    data_path,
    symbol,
    timeframe='1H',
    strategy_name='Strategy',
    use_optimized_params=True
):
    """
    🧪 Test strategy on single asset with full native results display

    Returns stats dict or None if test fails
    """

    print(f"\n🔬 Testing {strategy_name} on {symbol} ({timeframe})")
    print(f"📁 Data source: {data_path}")

    try:
        # Load data
        df = pd.read_csv(data_path)

        # Find and set date column
        date_col = None
        for col in df.columns:
            if col.lower() in ['date', 'datetime', 'time']:
                date_col = col
                break

        if date_col:
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col)

        # Standardize columns
        df.columns = [col.capitalize() for col in df.columns]

        # Validate data
        if len(df) < 500:
            print(f"⚠️ Insufficient data for {symbol}: {len(df)} bars")
            return None

        print(f"📊 Data loaded: {len(df)} bars")
        print(f"📅 Date range: {df.index[0]} to {df.index[-1]}")

        # Create backtest
        bt = Backtest(
            df,
            strategy_class,
            cash=10000,
            commission=0.002,
            exclusive_orders=True
        )

        # Run with appropriate parameters
        if use_optimized_params and strategy_class == MARSIVolumeOptimizedStrategy:
            # Use optimized parameters with asset name
            stats = bt.run(asset_name=symbol)
        else:
            # Use default parameters
            stats = bt.run()

        # Display full native results (MANDATORY)
        data_source_info = {
            'symbol': symbol,
            'timeframe': timeframe,
            'provider': os.path.dirname(data_path).split('/')[-1] or 'root',
            'path': data_path
        }

        display_full_native_results(
            stats,
            data_source_info,
            strategy_name=strategy_name
        )

        return stats

    except Exception as e:
        print(f"❌ Test failed for {symbol}: {e}")
        return None


def compare_strategies(symbol_files):
    """
    📊 Compare original vs optimized strategy performance

    Tests both strategies on all assets and generates comparison report
    """

    print("\n" + "="*80)
    print("📊 STRATEGY COMPARISON: ORIGINAL vs OPTIMIZED")
    print("="*80)

    # Results storage
    original_results = {}
    optimized_results = {}
    comparison_data = []

    # Test each asset
    for symbol, file_paths in symbol_files.items():
        print(f"\n{'='*60}")
        print(f"🎯 Testing {symbol}")
        print(f"{'='*60}")

        # Use first available file
        data_path = file_paths[0]

        # Test original adaptive strategy
        print(f"\n📊 Testing ORIGINAL Adaptive Strategy on {symbol}...")
        original_stats = test_strategy_on_asset(
            MARSIVolumeAdaptiveStrategy,
            data_path,
            symbol,
            strategy_name="MA-RSI-Volume Adaptive (Original)",
            use_optimized_params=False
        )

        if original_stats is not None:
            original_results[symbol] = {
                'return': original_stats['Return [%]'],
                'sharpe': original_stats['Sharpe Ratio'],
                'max_dd': original_stats['Max. Drawdown [%]'],
                'win_rate': original_stats['Win Rate [%]'],
                'trades': original_stats['# Trades']
            }

        # Test optimized strategy
        print(f"\n📊 Testing OPTIMIZED Strategy on {symbol}...")
        optimized_stats = test_strategy_on_asset(
            MARSIVolumeOptimizedStrategy,
            data_path,
            symbol,
            strategy_name="MA-RSI-Volume Optimized",
            use_optimized_params=True
        )

        if optimized_stats is not None:
            optimized_results[symbol] = {
                'return': optimized_stats['Return [%]'],
                'sharpe': optimized_stats['Sharpe Ratio'],
                'max_dd': optimized_stats['Max. Drawdown [%]'],
                'win_rate': optimized_stats['Win Rate [%]'],
                'trades': optimized_stats['# Trades']
            }

        # Calculate improvement
        if symbol in original_results and symbol in optimized_results:
            orig = original_results[symbol]
            opt = optimized_results[symbol]

            improvement = {
                'symbol': symbol,
                'original_return': orig['return'],
                'optimized_return': opt['return'],
                'return_improvement': opt['return'] - orig['return'],
                'original_win_rate': orig['win_rate'],
                'optimized_win_rate': opt['win_rate'],
                'win_rate_improvement': opt['win_rate'] - orig['win_rate'],
                'original_sharpe': orig['sharpe'],
                'optimized_sharpe': opt['sharpe'],
                'sharpe_improvement': opt['sharpe'] - orig['sharpe'],
                'original_max_dd': orig['max_dd'],
                'optimized_max_dd': opt['max_dd'],
                'dd_improvement': orig['max_dd'] - opt['max_dd'],  # Lower is better
                'original_trades': orig['trades'],
                'optimized_trades': opt['trades']
            }

            comparison_data.append(improvement)

            # Print comparison
            print(f"\n🔄 {symbol} IMPROVEMENT ANALYSIS:")
            print(f"   Return: {orig['return']:.2f}% → {opt['return']:.2f}% "
                  f"({'✅' if opt['return'] > orig['return'] else '❌'} "
                  f"{improvement['return_improvement']:+.2f}%)")
            print(f"   Win Rate: {orig['win_rate']:.1f}% → {opt['win_rate']:.1f}% "
                  f"({'✅' if opt['win_rate'] > orig['win_rate'] else '❌'} "
                  f"{improvement['win_rate_improvement']:+.1f}%)")
            print(f"   Sharpe: {orig['sharpe']:.3f} → {opt['sharpe']:.3f} "
                  f"({'✅' if opt['sharpe'] > orig['sharpe'] else '❌'} "
                  f"{improvement['sharpe_improvement']:+.3f})")
            print(f"   Max DD: {orig['max_dd']:.1f}% → {opt['max_dd']:.1f}% "
                  f"({'✅' if abs(opt['max_dd']) < abs(orig['max_dd']) else '❌'} "
                  f"{improvement['dd_improvement']:+.1f}%)")

    return comparison_data, original_results, optimized_results


def generate_performance_report(comparison_data, original_results, optimized_results):
    """
    📈 Generate comprehensive performance report

    Creates detailed analysis of optimization improvements
    """

    print("\n" + "="*80)
    print("📈 COMPREHENSIVE PERFORMANCE REPORT")
    print("="*80)

    if not comparison_data:
        print("⚠️ No comparison data available")
        return

    # Convert to DataFrame for analysis
    df = pd.DataFrame(comparison_data)

    # Overall statistics
    print("\n🏆 OPTIMIZATION SUCCESS METRICS:")
    print(f"   Assets Tested: {len(df)}")
    print(f"   Assets Improved (Return): {(df['return_improvement'] > 0).sum()} / {len(df)}")
    print(f"   Assets Improved (Win Rate): {(df['win_rate_improvement'] > 0).sum()} / {len(df)}")
    print(f"   Assets Improved (Sharpe): {(df['sharpe_improvement'] > 0).sum()} / {len(df)}")
    print(f"   Assets Improved (Drawdown): {(df['dd_improvement'] > 0).sum()} / {len(df)}")

    # Average improvements
    print("\n📊 AVERAGE IMPROVEMENTS:")
    print(f"   Return: {df['return_improvement'].mean():+.2f}%")
    print(f"   Win Rate: {df['win_rate_improvement'].mean():+.1f}%")
    print(f"   Sharpe Ratio: {df['sharpe_improvement'].mean():+.3f}")
    print(f"   Max Drawdown: {df['dd_improvement'].mean():+.1f}%")

    # Best improvements
    print("\n🚀 BEST IMPROVEMENTS:")
    best_return = df.loc[df['return_improvement'].idxmax()]
    best_win_rate = df.loc[df['win_rate_improvement'].idxmax()]
    best_sharpe = df.loc[df['sharpe_improvement'].idxmax()]

    print(f"   Best Return Improvement: {best_return['symbol']} "
          f"({best_return['return_improvement']:+.2f}%)")
    print(f"   Best Win Rate Improvement: {best_win_rate['symbol']} "
          f"({best_win_rate['win_rate_improvement']:+.1f}%)")
    print(f"   Best Sharpe Improvement: {best_sharpe['symbol']} "
          f"({best_sharpe['sharpe_improvement']:+.3f})")

    # Target achievement analysis
    print("\n🎯 TARGET ACHIEVEMENT (Optimized Strategy):")

    targets_met = {
        'win_rate_50': 0,
        'positive_returns': 0,
        'dd_under_20': 0,
        'sharpe_over_1': 0,
        'all_targets': 0
    }

    for symbol, results in optimized_results.items():
        win_rate_met = results['win_rate'] >= 50
        return_met = results['return'] > 0
        dd_met = abs(results['max_dd']) < 20
        sharpe_met = results['sharpe'] > 1.0

        if win_rate_met:
            targets_met['win_rate_50'] += 1
        if return_met:
            targets_met['positive_returns'] += 1
        if dd_met:
            targets_met['dd_under_20'] += 1
        if sharpe_met:
            targets_met['sharpe_over_1'] += 1

        if all([win_rate_met, return_met, dd_met, sharpe_met]):
            targets_met['all_targets'] += 1

    total_assets = len(optimized_results)
    print(f"   Win Rate ≥50%: {targets_met['win_rate_50']}/{total_assets} "
          f"({targets_met['win_rate_50']*100/total_assets:.1f}%)")
    print(f"   Positive Returns: {targets_met['positive_returns']}/{total_assets} "
          f"({targets_met['positive_returns']*100/total_assets:.1f}%)")
    print(f"   Max DD <20%: {targets_met['dd_under_20']}/{total_assets} "
          f"({targets_met['dd_under_20']*100/total_assets:.1f}%)")
    print(f"   Sharpe >1.0: {targets_met['sharpe_over_1']}/{total_assets} "
          f"({targets_met['sharpe_over_1']*100/total_assets:.1f}%)")
    print(f"   All Targets Met: {targets_met['all_targets']}/{total_assets} "
          f"({targets_met['all_targets']*100/total_assets:.1f}%)")

    # Asset rankings
    print("\n📊 ASSET PERFORMANCE RANKINGS (Optimized Strategy):")

    # Sort by return
    sorted_by_return = sorted(optimized_results.items(),
                             key=lambda x: x[1]['return'], reverse=True)

    print("\n   Top 3 by Return:")
    for i, (symbol, results) in enumerate(sorted_by_return[:3], 1):
        print(f"   {i}. {symbol}: {results['return']:.2f}% "
              f"(WR: {results['win_rate']:.1f}%, Sharpe: {results['sharpe']:.3f})")

    # Sort by Sharpe
    sorted_by_sharpe = sorted(optimized_results.items(),
                             key=lambda x: x[1]['sharpe'], reverse=True)

    print("\n   Top 3 by Sharpe Ratio:")
    for i, (symbol, results) in enumerate(sorted_by_sharpe[:3], 1):
        print(f"   {i}. {symbol}: {results['sharpe']:.3f} "
              f"(Return: {results['return']:.2f}%, WR: {results['win_rate']:.1f}%)")

    # Production readiness assessment
    print("\n🏭 PRODUCTION READINESS ASSESSMENT:")

    production_ready = []
    for symbol, results in optimized_results.items():
        if (results['return'] > 10 and
            results['win_rate'] >= 45 and
            abs(results['max_dd']) < 25 and
            results['sharpe'] > 0.5):
            production_ready.append(symbol)

    if production_ready:
        print(f"   ✅ Production-Ready Assets: {', '.join(production_ready)}")
        print(f"   Total: {len(production_ready)} assets ready for live trading")
    else:
        print(f"   ⚠️ No assets fully meet production criteria yet")
        print(f"   Consider further optimization or different strategy approach")

    return targets_met


def save_results(comparison_data, original_results, optimized_results):
    """
    💾 Save comprehensive results to CSV and JSON files

    Creates persistent record of optimization improvements
    """

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_dir = '/Users/bobbyyo/Projects/algo-fun/strategies/results'
    os.makedirs(results_dir, exist_ok=True)

    # Save comparison data as CSV
    if comparison_data:
        df = pd.DataFrame(comparison_data)
        comparison_file = f'{results_dir}/ma_rsi_volume_comparison_{timestamp}.csv'
        df.to_csv(comparison_file, index=False)
        print(f"\n💾 Comparison results saved to: {comparison_file}")

    # Save detailed results as JSON
    detailed_results = {
        'timestamp': timestamp,
        'original_strategy': original_results,
        'optimized_strategy': optimized_results,
        'improvements': comparison_data
    }

    json_file = f'{results_dir}/ma_rsi_volume_detailed_{timestamp}.json'
    with open(json_file, 'w') as f:
        json.dump(detailed_results, f, indent=2, default=str)

    print(f"💾 Detailed results saved to: {json_file}")


def main():
    """
    🚀 Main execution function for comprehensive multi-asset testing
    """

    print("\n" + "="*80)
    print("🚀 MA-RSI-VOLUME OPTIMIZED STRATEGY - COMPREHENSIVE TESTING")
    print("="*80)
    print("Testing optimized parameters across all available cryptocurrencies")
    print("Comparing performance against original adaptive strategy")
    print("="*80)

    # Find all available cryptocurrency data
    symbol_files = find_all_crypto_data(timeframe='1h')

    if not symbol_files:
        print("❌ No cryptocurrency data found!")
        return

    # Run comprehensive comparison
    comparison_data, original_results, optimized_results = compare_strategies(symbol_files)

    # Generate performance report
    targets_met = generate_performance_report(
        comparison_data,
        original_results,
        optimized_results
    )

    # Save results
    save_results(comparison_data, original_results, optimized_results)

    # Final summary
    print("\n" + "="*80)
    print("✅ COMPREHENSIVE TESTING COMPLETE!")
    print("="*80)

    if comparison_data:
        avg_improvement = sum(d['return_improvement'] for d in comparison_data) / len(comparison_data)
        if avg_improvement > 0:
            print(f"🎉 SUCCESS: Average return improved by {avg_improvement:+.2f}%")
            print(f"🚀 Optimization has successfully transformed the strategy!")
        else:
            print(f"⚠️ Mixed results: Some assets need further optimization")

    print("\n🌙💫🚀 MA-RSI-Volume Optimized Strategy testing complete!")


if __name__ == "__main__":
    main()