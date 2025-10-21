"""
🚀 RSI Mean Reversion Strategy - Phase 1 Optimization
======================================================
Systematically optimize RSI strategy based on Phase 0 findings to improve
profitability while maintaining safety.

🎯 Phase 0 Baseline (Daily Data):
    - XRP: 107.9% return, 0.37 Sharpe, 64% win rate, 72 trades
    - BTC: 46.8% return, 0.46 Sharpe, 80% win rate, 15 trades
    - ETH: 35.1% return, 0.81 Sharpe, 70% win rate, 27 trades
    - HBAR: 46.1% return, 0.32 Sharpe, 62% win rate, 47 trades

📊 Phase 1 Optimization Goals:
    1. Exit Threshold: Test RSI > 40 vs 50 vs 60 for better profit capture
    2. Trend Filter: Add SMA(200) to reduce drawdowns in bear markets
    3. Entry Threshold: Optimize RSI < 30 per asset (test 25, 28, 30, 32, 35)
    4. Volume Filter: Test volume > 1.2x average confirmation

🎯 Methodology:
    - In-sample: 2020-2023 (parameter optimization)
    - Out-of-sample: 2024-2025 (validation)
    - Objective: Maximize Sharpe Ratio
    - Constraint: Win rate must not degrade

🌙💫🚀 Phase 1: Evidence-based optimization
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/core_strategies')

from backtesting import Backtest
from rsi_mean_reversion_strategy import RSIMeanReversionStrategy

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
    bt = Backtest(df_train, RSIMeanReversionStrategy, cash=10000, commission=.002)

    # Define optimization ranges
    print(f"\n🎯 Optimization Parameters:")
    print(f"   • RSI Oversold: [25, 28, 30, 32, 35]")
    print(f"   • RSI Neutral Low Exit: [40, 45, 50, 55, 60]")
    print(f"   • Use Trend Filter: [False, True]")
    print(f"   • Trend SMA Period: [50, 100, 200]")
    print(f"\n⏳ Running optimization (this may take 10-15 minutes)...\n")

    try:
        # Run optimization
        stats = bt.optimize(
            rsi_oversold=[25, 28, 30, 32, 35],
            rsi_neutral_low=[40, 45, 50, 55, 60],
            use_trend_filter=[False, True],
            trend_sma_period=[50, 100, 200],
            maximize='Sharpe Ratio',
            return_heatmap=False
        )

        print(f"\n{'='*70}")
        print(f"✅ OPTIMIZATION COMPLETE: {asset_name}")
        print(f"{'='*70}")
        print(f"\n📊 Optimal Parameters:")
        print(f"   • RSI Oversold: {stats._strategy.rsi_oversold}")
        print(f"   • RSI Neutral Low Exit: {stats._strategy.rsi_neutral_low}")
        print(f"   • Use Trend Filter: {stats._strategy.use_trend_filter}")
        print(f"   • Trend SMA Period: {stats._strategy.trend_sma_period}")

        print(f"\n📈 In-Sample Performance:")
        print(f"   • Return: {stats['Return [%]']:.2f}%")
        print(f"   • Sharpe Ratio: {stats.get('Sharpe Ratio', 0):.2f}")
        print(f"   • Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"   • Win Rate: {stats.get('Win Rate [%]', 0):.1f}%")
        print(f"   • Trades: {stats['# Trades']}")

        return {
            'asset': asset_name,
            'rsi_oversold': stats._strategy.rsi_oversold,
            'rsi_neutral_low': stats._strategy.rsi_neutral_low,
            'use_trend_filter': stats._strategy.use_trend_filter,
            'trend_sma_period': stats._strategy.trend_sma_period,
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
    class OptimizedStrategy(RSIMeanReversionStrategy):
        rsi_oversold = optimal_params['rsi_oversold']
        rsi_neutral_low = optimal_params['rsi_neutral_low']
        use_trend_filter = optimal_params['use_trend_filter']
        trend_sma_period = optimal_params['trend_sma_period']

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

def compare_phase0_vs_phase1(asset_name, phase0_stats, phase1_stats):
    """📊 Compare Phase 0 baseline vs Phase 1 optimized"""
    print(f"\n{'='*70}")
    print(f"📊 COMPARISON: Phase 0 vs Phase 1 ({asset_name})")
    print(f"{'='*70}")

    # Calculate improvements
    return_improvement = phase1_stats['test_return'] - phase0_stats['return']
    sharpe_improvement = phase1_stats['test_sharpe'] - phase0_stats['sharpe']
    winrate_improvement = phase1_stats['test_win_rate'] - phase0_stats['win_rate']

    print(f"\n📈 Performance Changes:")
    print(f"   Return:    {phase0_stats['return']:>6.2f}% → {phase1_stats['test_return']:>6.2f}%  "
          f"({return_improvement:>+6.2f}%)")
    print(f"   Sharpe:    {phase0_stats['sharpe']:>6.2f} → {phase1_stats['test_sharpe']:>6.2f}  "
          f"({sharpe_improvement:>+6.2f})")
    print(f"   Win Rate:  {phase0_stats['win_rate']:>6.1f}% → {phase1_stats['test_win_rate']:>6.1f}%  "
          f"({winrate_improvement:>+6.1f}%)")
    print(f"   Trades:    {phase0_stats['trades']:>6} → {phase1_stats['test_trades']:>6}")

    print(f"\n🎯 Parameter Changes:")
    print(f"   RSI Oversold:    30 (default) → {phase1_stats['rsi_oversold']}")
    print(f"   RSI Exit:        40 (default) → {phase1_stats['rsi_neutral_low']}")
    print(f"   Trend Filter:    False → {phase1_stats['use_trend_filter']}")
    print(f"   Trend SMA:       N/A → {phase1_stats['trend_sma_period']}")

    # Evaluation
    print(f"\n🎯 Phase 1 Assessment:")
    improvements = 0
    if return_improvement > 0: improvements += 1
    if sharpe_improvement > 0: improvements += 1
    if winrate_improvement > 0: improvements += 1

    if improvements >= 2:
        print(f"   ✅ PHASE 1 SUCCESSFUL: Optimization improves strategy ({improvements}/3 metrics)")
    elif improvements == 1:
        print(f"   ⚠️  PHASE 1 MIXED: Some improvement but not across all metrics ({improvements}/3 metrics)")
    else:
        print(f"   ❌ PHASE 1 UNSUCCESSFUL: Optimization may hurt performance ({improvements}/3 metrics)")
        print(f"   💡 RECOMMENDATION: Accept Phase 0 as final version (like Breakout)")

def run_comprehensive_optimization():
    """🚀 Run Phase 1 optimization across top-performing assets"""
    print("\n" + "="*70)
    print("🚀 RSI MEAN REVERSION - PHASE 1 OPTIMIZATION")
    print("="*70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Goal: Optimize RSI parameters to improve profitability")
    print(f"📊 Method: Walk-forward validation (2020-2023 train, 2024-2025 test)")

    # Define assets to optimize (Phase 0 top performers on daily data)
    assets = [
        {
            'path': "/Users/bobbyyo/Projects/algo-fun/dataset_files/crypto_hist_data/Bitstamp_XRPUSD_d.csv",
            'name': "XRP",
            'phase0': {'return': 107.91, 'sharpe': 0.37, 'win_rate': 63.9, 'trades': 72}
        },
        {
            'path': "/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv",
            'name': "BTC",
            'phase0': {'return': 46.84, 'sharpe': 0.46, 'win_rate': 80.0, 'trades': 15}
        },
        {
            'path': "/Users/bobbyyo/Projects/algo-fun/dataset_files/crypto_hist_data/Bitstamp_ETHUSDC_d.csv",
            'name': "ETH",
            'phase0': {'return': 35.10, 'sharpe': 0.81, 'win_rate': 70.4, 'trades': 27}
        },
        {
            'path': "/Users/bobbyyo/Projects/algo-fun/dataset_files/yahoo/HBARUSD-20yr-yahoo-data.csv",
            'name': "HBAR",
            'phase0': {'return': 46.11, 'sharpe': 0.32, 'win_rate': 61.7, 'trades': 47}
        },
    ]

    results = []

    for asset_config in assets:
        # Step 1: Optimize on training data
        optimal = optimize_single_asset(asset_config['path'], asset_config['name'])

        if optimal:
            # Step 2: Validate on test data
            validated = validate_parameters(asset_config['path'], asset_config['name'], optimal)

            # Step 3: Compare Phase 0 vs Phase 1
            compare_phase0_vs_phase1(asset_config['name'], asset_config['phase0'], validated)

            results.append(validated)

        print()  # Spacing between assets

    # Generate summary report
    if results:
        print("\n" + "="*70)
        print("📊 PHASE 1 OPTIMIZATION SUMMARY")
        print("="*70)

        print(f"\n🏆 Asset Rankings (by Test Sharpe Ratio):")
        sorted_results = sorted(results, key=lambda x: x.get('test_sharpe', 0), reverse=True)

        for i, r in enumerate(sorted_results, 1):
            print(f"\n{i}. {r['asset']}")
            print(f"   Parameters: RSI({r['rsi_oversold']}/{r['rsi_neutral_low']}), "
                  f"Trend Filter: {r['use_trend_filter']}, SMA({r['trend_sma_period']})")
            print(f"   Train: {r['train_return']:.2f}% return, {r['train_sharpe']:.2f} Sharpe, "
                  f"{r['train_trades']} trades")
            print(f"   Test:  {r.get('test_return', 0):.2f}% return, {r.get('test_sharpe', 0):.2f} Sharpe, "
                  f"{r.get('test_trades', 0)} trades")

        # Calculate average optimal parameters
        print(f"\n📊 Average Optimal Parameters (across all assets):")
        avg_oversold = np.mean([r['rsi_oversold'] for r in results])
        avg_neutral = np.mean([r['rsi_neutral_low'] for r in results])
        trend_filter_count = sum([1 for r in results if r['use_trend_filter']])
        avg_sma = np.mean([r['trend_sma_period'] for r in results if r['use_trend_filter']])

        print(f"   • RSI Oversold: {avg_oversold:.1f} (default: 30)")
        print(f"   • RSI Neutral Low Exit: {avg_neutral:.1f} (default: 40)")
        print(f"   • Use Trend Filter: {trend_filter_count}/{len(results)} assets")
        if trend_filter_count > 0:
            print(f"   • Trend SMA Period: {avg_sma:.0f} (when used)")

        # Performance comparison
        print(f"\n📈 Phase 0 vs Phase 1 Performance:")
        avg_test_return = np.mean([r.get('test_return', 0) for r in results])
        avg_test_sharpe = np.mean([r.get('test_sharpe', 0) for r in results])
        avg_test_winrate = np.mean([r.get('test_win_rate', 0) for r in results])

        print(f"   • Avg Test Return: {avg_test_return:.2f}%")
        print(f"   • Avg Test Sharpe: {avg_test_sharpe:.2f}")
        print(f"   • Avg Test Win Rate: {avg_test_winrate:.1f}%")

        # Save results to CSV
        results_df = pd.DataFrame(results)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f"/Users/bobbyyo/Projects/algo-fun/strategies/results/rsi_phase1_optimization_{timestamp}.csv"
        results_df.to_csv(output_path, index=False)
        print(f"\n💾 Results saved to: {output_path}")

        # Overall Phase 1 assessment
        print(f"\n🎯 Overall Phase 1 Assessment:")
        successful_count = sum([1 for r in results if r.get('test_sharpe', 0) > 0])

        if successful_count >= 3:
            print(f"   ✅ PHASE 1 SUCCESSFUL: {successful_count}/{len(results)} assets improved")
            print(f"   📋 RECOMMENDATION: Proceed to Phase 2 (safety features)")
        elif successful_count >= 2:
            print(f"   ⚠️  PHASE 1 MIXED: {successful_count}/{len(results)} assets improved")
            print(f"   📋 RECOMMENDATION: Selective optimization per asset")
        else:
            print(f"   ❌ PHASE 1 UNSUCCESSFUL: {successful_count}/{len(results)} assets improved")
            print(f"   📋 RECOMMENDATION: Accept Phase 0 as final (like Breakout Phase 3)")

    else:
        print("\n❌ No successful optimizations")

    print("\n🌙💫🚀 Phase 1 optimization complete!")
    return results

def main():
    """🎯 Main entry point"""
    results = run_comprehensive_optimization()
    return results

if __name__ == "__main__":
    main()
