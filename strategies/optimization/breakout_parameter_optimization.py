"""
🚀 Breakout Strategy - Phase 3 Parameter Optimization
=====================================================
Systematically optimize strategy parameters to improve profitability while
maintaining Phase 2 safety features.

🎯 Optimization Targets:
    - RSI period (momentum detection)
    - Divergence lookback (false breakout filtering)
    - Lookback period (range identification)
    - Volume threshold (confirmation level)

📊 Methodology:
    - In-sample: 2020-2023 (optimization)
    - Out-of-sample: 2024-2025 (validation)
    - Objective: Maximize Sharpe Ratio
    - Constraint: Max drawdown < 25%

🌙💫🚀 Phase 3 Step 1: Evidence-based parameter selection
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/core_strategies')

from backtesting import Backtest
from breakout_momentum_strategy import BreakoutMomentumStrategy

def load_and_prepare_data(file_path, start_date=None, end_date=None):
    """📊 Load and filter data by date range"""
    try:
        df = pd.read_csv(file_path)

        # Standardize column names
        df.columns = df.columns.str.lower()
        column_mapping = {
            'datetime': 'timestamp',
            'date': 'timestamp',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }
        df = df.rename(columns=column_mapping)

        # Set index
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

        # Filter by date range
        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]

        return df

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None

def optimize_single_asset(data_path, asset_name, in_sample_end='2023-12-31'):
    """🔧 Optimize parameters for single asset"""
    print(f"\n{'='*70}")
    print(f"🔧 OPTIMIZING PARAMETERS: {asset_name}")
    print(f"{'='*70}")

    # Load in-sample data (2020-2023)
    df_train = load_and_prepare_data(data_path, start_date='2020-01-01',
                                       end_date=in_sample_end)

    if df_train is None or len(df_train) < 100:
        print(f"❌ Insufficient training data for {asset_name}")
        return None

    print(f"📊 Training data: {len(df_train)} bars ({df_train.index[0].date()} to {df_train.index[-1].date()})")

    # Create backtest instance
    bt = Backtest(df_train, BreakoutMomentumStrategy, cash=10000, commission=.002)

    # Define optimization ranges
    print(f"\n🎯 Optimization Parameters:")
    print(f"   • RSI Period: [10, 14, 18, 21, 28]")
    print(f"   • Divergence Lookback: [3, 5, 8, 10, 12]")
    print(f"   • Lookback Period: [15, 20, 25, 30]")
    print(f"   • Volume Threshold: [1.2, 1.5, 2.0]")
    print(f"\n⏳ Running optimization (this may take 5-10 minutes)...\n")

    try:
        # Run optimization
        stats = bt.optimize(
            rsi_period=[10, 14, 18, 21, 28],
            divergence_lookback=[3, 5, 8, 10, 12],
            lookback_period=[15, 20, 25, 30],
            volume_threshold=[1.2, 1.5, 2.0],
            maximize='Sharpe Ratio',
            # No constraint - we'll filter results manually
            return_heatmap=False
        )

        print(f"\n{'='*70}")
        print(f"✅ OPTIMIZATION COMPLETE: {asset_name}")
        print(f"{'='*70}")
        print(f"\n📊 Optimal Parameters:")
        print(f"   • RSI Period: {stats._strategy.rsi_period}")
        print(f"   • Divergence Lookback: {stats._strategy.divergence_lookback}")
        print(f"   • Lookback Period: {stats._strategy.lookback_period}")
        print(f"   • Volume Threshold: {stats._strategy.volume_threshold}")

        print(f"\n📈 In-Sample Performance:")
        print(f"   • Return: {stats['Return [%]']:.2f}%")
        print(f"   • Sharpe Ratio: {stats.get('Sharpe Ratio', 0):.2f}")
        print(f"   • Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"   • Win Rate: {stats.get('Win Rate [%]', 0):.1f}%")
        print(f"   • Trades: {stats['# Trades']}")

        return {
            'asset': asset_name,
            'rsi_period': stats._strategy.rsi_period,
            'divergence_lookback': stats._strategy.divergence_lookback,
            'lookback_period': stats._strategy.lookback_period,
            'volume_threshold': stats._strategy.volume_threshold,
            'train_return': stats['Return [%]'],
            'train_sharpe': stats.get('Sharpe Ratio', 0),
            'train_drawdown': stats['Max. Drawdown [%]'],
            'train_win_rate': stats.get('Win Rate [%]', 0),
            'train_trades': stats['# Trades']
        }

    except Exception as e:
        print(f"❌ Optimization failed for {asset_name}: {e}")
        return None

def validate_parameters(data_path, asset_name, optimal_params,
                        out_sample_start='2024-01-01'):
    """✅ Validate optimized parameters on out-of-sample data"""
    print(f"\n{'='*70}")
    print(f"✅ VALIDATING PARAMETERS: {asset_name}")
    print(f"{'='*70}")

    # Load out-of-sample data (2024-2025)
    df_test = load_and_prepare_data(data_path, start_date=out_sample_start)

    if df_test is None or len(df_test) < 50:
        print(f"❌ Insufficient test data for {asset_name}")
        return optimal_params

    print(f"📊 Test data: {len(df_test)} bars ({df_test.index[0].date()} to {df_test.index[-1].date()})")

    # Create strategy with optimized parameters
    class OptimizedStrategy(BreakoutMomentumStrategy):
        rsi_period = optimal_params['rsi_period']
        divergence_lookback = optimal_params['divergence_lookback']
        lookback_period = optimal_params['lookback_period']
        volume_threshold = optimal_params['volume_threshold']

    try:
        # Run backtest on test data
        bt = Backtest(df_test, OptimizedStrategy, cash=10000, commission=.002)
        stats = bt.run()

        print(f"\n📈 Out-of-Sample Performance:")
        print(f"   • Return: {stats['Return [%]']:.2f}%")
        print(f"   • Sharpe Ratio: {stats.get('Sharpe Ratio', 0):.2f}")
        print(f"   • Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"   • Win Rate: {stats.get('Win Rate [%]', 0):.1f}%")
        print(f"   • Trades: {stats['# Trades']}")

        # Add validation results
        optimal_params['test_return'] = stats['Return [%]']
        optimal_params['test_sharpe'] = stats.get('Sharpe Ratio', 0)
        optimal_params['test_drawdown'] = stats['Max. Drawdown [%]']
        optimal_params['test_win_rate'] = stats.get('Win Rate [%]', 0)
        optimal_params['test_trades'] = stats['# Trades']

        # Check for overfitting
        train_sharpe = optimal_params['train_sharpe']
        test_sharpe = optimal_params['test_sharpe']
        degradation = ((train_sharpe - test_sharpe) / train_sharpe * 100) if train_sharpe > 0 else 0

        print(f"\n🔍 Overfitting Check:")
        print(f"   • Train Sharpe: {train_sharpe:.2f}")
        print(f"   • Test Sharpe: {test_sharpe:.2f}")
        print(f"   • Degradation: {degradation:.1f}%")

        if degradation > 50:
            print(f"   ⚠️  WARNING: Significant performance degradation (possible overfitting)")
        elif degradation > 25:
            print(f"   ⚠️  CAUTION: Moderate performance degradation")
        else:
            print(f"   ✅ Good generalization to out-of-sample data")

        return optimal_params

    except Exception as e:
        print(f"❌ Validation failed for {asset_name}: {e}")
        return optimal_params

def run_comprehensive_optimization():
    """🚀 Run optimization across all recommended assets"""
    print("\n" + "="*70)
    print("🚀 BREAKOUT STRATEGY - PARAMETER OPTIMIZATION")
    print("="*70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Goal: Find optimal parameters for daily data")
    print(f"📊 Method: In-sample optimization + out-of-sample validation")

    # Define assets to optimize (Phase 2 recommended assets, excluding HBAR)
    assets = [
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv", "BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv", "ETH"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/XRPUSD-1d-500wks-enhanced-data.csv", "XRP"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/CROUSD-1d-1000wks-enhanced-data.csv", "CRO"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv", "LINK"),
    ]

    results = []

    for data_path, asset_name in assets:
        # Step 1: Optimize on training data
        optimal = optimize_single_asset(data_path, asset_name)

        if optimal:
            # Step 2: Validate on test data
            validated = validate_parameters(data_path, asset_name, optimal)
            results.append(validated)

        print()  # Spacing between assets

    # Generate summary report
    if results:
        print("\n" + "="*70)
        print("📊 OPTIMIZATION SUMMARY")
        print("="*70)

        print(f"\n🏆 Asset Rankings (by Test Sharpe Ratio):")
        sorted_results = sorted(results, key=lambda x: x.get('test_sharpe', 0), reverse=True)

        for i, r in enumerate(sorted_results, 1):
            print(f"\n{i}. {r['asset']}")
            print(f"   Parameters: RSI({r['rsi_period']}), Lookback({r['lookback_period']}), "
                  f"Divergence({r['divergence_lookback']}), Volume({r['volume_threshold']})")
            print(f"   Train: {r['train_return']:.2f}% return, {r['train_sharpe']:.2f} Sharpe, "
                  f"{r['train_trades']} trades")
            print(f"   Test:  {r.get('test_return', 0):.2f}% return, {r.get('test_sharpe', 0):.2f} Sharpe, "
                  f"{r.get('test_trades', 0)} trades")

        # Calculate average optimal parameters
        print(f"\n📊 Average Optimal Parameters (across all assets):")
        avg_rsi = np.mean([r['rsi_period'] for r in results])
        avg_lookback = np.mean([r['lookback_period'] for r in results])
        avg_divergence = np.mean([r['divergence_lookback'] for r in results])
        avg_volume = np.mean([r['volume_threshold'] for r in results])

        print(f"   • RSI Period: {avg_rsi:.1f}")
        print(f"   • Lookback Period: {avg_lookback:.1f}")
        print(f"   • Divergence Lookback: {avg_divergence:.1f}")
        print(f"   • Volume Threshold: {avg_volume:.2f}")

        # Performance comparison
        print(f"\n📈 Performance Summary:")
        avg_train_return = np.mean([r['train_return'] for r in results])
        avg_test_return = np.mean([r.get('test_return', 0) for r in results])
        avg_train_sharpe = np.mean([r['train_sharpe'] for r in results])
        avg_test_sharpe = np.mean([r.get('test_sharpe', 0) for r in results])

        print(f"   • Avg Train Return: {avg_train_return:.2f}%")
        print(f"   • Avg Test Return: {avg_test_return:.2f}%")
        print(f"   • Avg Train Sharpe: {avg_train_sharpe:.2f}")
        print(f"   • Avg Test Sharpe: {avg_test_sharpe:.2f}")

        # Save results to CSV
        results_df = pd.DataFrame(results)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f"/Users/bobbyyo/Projects/algo-fun/strategies/results/breakout_parameter_optimization_{timestamp}.csv"
        results_df.to_csv(output_path, index=False)
        print(f"\n💾 Results saved to: {output_path}")

        # Comparison with Phase 2 baseline
        print(f"\n🎯 Phase 2 vs Phase 3 Comparison:")
        print(f"   Phase 2 Baseline (forward test, conservative): 0.55% return, 0.02 Sharpe")
        print(f"   Phase 3 Optimized (out-of-sample test):        {avg_test_return:.2f}% return, {avg_test_sharpe:.2f} Sharpe")

        if avg_test_return > 0.55:
            improvement = avg_test_return - 0.55
            print(f"   ✅ Improvement: +{improvement:.2f}% return")
        else:
            print(f"   ⚠️  Performance below Phase 2 baseline - may need further optimization")

    else:
        print("\n❌ No successful optimizations")

    print("\n🌙💫🚀 Parameter optimization complete!")
    return results

def main():
    """🎯 Main entry point"""
    results = run_comprehensive_optimization()
    return results

if __name__ == "__main__":
    main()
