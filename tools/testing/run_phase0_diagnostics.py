"""
🔬 Phase 0 Diagnostic Runner - Complete Analysis Pipeline
=========================================================
Runs all core strategies, exports trade data, and executes full diagnostic suite.

🎯 What This Does:
    1. Runs backtests on all 3 core strategies
    2. Exports trade-by-trade CSV files
    3. Runs all 5 Phase 0 diagnostic tools on each strategy
    4. Generates comprehensive decision matrix

🌙💫🚀 Bobby's Algo-Fun Project - Phase 0 Execution
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import warnings
import os
from datetime import datetime

# Import core strategies
from strategies.core_strategies.breakout_momentum_strategy import BreakoutMomentumStrategy
from strategies.core_strategies.rsi_mean_reversion_strategy import RSIMeanReversionStrategy
from strategies.core_strategies.sma_crossover_strategy import SMAStrategy

# Import Phase 0 diagnostic tools
from strategies.diagnostics.trade_autopsy import run_trade_autopsy
from strategies.diagnostics.regime_analyzer import run_regime_analysis
from strategies.diagnostics.statistical_validator import run_statistical_validation
from strategies.diagnostics.cost_modeler import run_cost_modeling
from strategies.diagnostics.benchmark_comparator import run_benchmark_comparison

warnings.filterwarnings('ignore')

# 🎯 Strategy configurations
STRATEGIES = {
    'Breakout_LINK_1d': {
        'strategy_class': BreakoutMomentumStrategy,
        'data_path': 'dataset_files/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv',
        'asset': 'LINK',
        'timeframe': '1d'
    },
    'RSI_XRP_1d': {
        'strategy_class': RSIMeanReversionStrategy,
        'data_path': 'dataset_files/coinbase/XRPUSD-1d-500wks-enhanced-data.csv',
        'asset': 'XRP',
        'timeframe': '1d'
    }
}


def load_and_prepare_data(data_path):
    """📊 Load and prepare OHLCV data for backtesting"""
    print(f"   Loading data: {data_path}")

    df = pd.read_csv(data_path)

    # Handle datetime column
    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
    if date_cols:
        df[date_cols[0]] = pd.to_datetime(df[date_cols[0]])
        df.set_index(date_cols[0], inplace=True)

    # Ensure required columns exist
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        if col not in df.columns:
            # Try case-insensitive match
            matching = [c for c in df.columns if c.lower() == col.lower()]
            if matching:
                df[col] = df[matching[0]]
            else:
                raise ValueError(f"Missing required column: {col}")

    return df[required_cols]


def run_backtest_and_export_trades(strategy_name, config):
    """🚀 Run backtest and export trade data"""
    print(f"\n{'='*70}")
    print(f"🚀 Running Backtest: {strategy_name}")
    print(f"{'='*70}")

    # Load data
    df = load_and_prepare_data(config['data_path'])
    print(f"   Data loaded: {len(df)} bars")

    # Run backtest
    print(f"   Running backtest with {config['strategy_class'].__name__}...")
    bt = Backtest(
        df,
        config['strategy_class'],
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    stats = bt.run()

    # Print summary stats
    print(f"\n📊 Backtest Results:")
    print(f"   Return: {stats['Return [%]']:.2f}%")
    print(f"   Sharpe: {stats.get('Sharpe Ratio', 0):.2f}")
    print(f"   Max DD: {stats.get('Max. Drawdown [%]', 0):.2f}%")
    print(f"   Win Rate: {stats.get('Win Rate [%]', 0):.2f}%")
    print(f"   # Trades: {stats.get('# Trades', 0)}")

    # Export trades to CSV
    trades_dir = 'strategies/results/trades'
    os.makedirs(trades_dir, exist_ok=True)

    trades_path = os.path.join(trades_dir, f'{strategy_name}_trades.csv')

    # Access trades DataFrame
    if hasattr(stats, '_trades') and stats._trades is not None:
        trades_df = stats._trades

        # Add readable column names if needed
        if not trades_df.empty:
            print(f"   Exporting {len(trades_df)} trades to CSV...")
            trades_df.to_csv(trades_path, index=False)
            print(f"   ✅ Trades saved: {trades_path}")
        else:
            print(f"   ⚠️ No trades generated for {strategy_name}")
            return None, None
    else:
        print(f"   ⚠️ No trade data available for {strategy_name}")
        return None, None

    return trades_path, config['data_path']


def run_full_diagnostics(strategy_name, trades_path, price_path):
    """🔬 Run complete Phase 0 diagnostic suite"""
    print(f"\n{'='*70}")
    print(f"🔬 Running Phase 0 Diagnostics: {strategy_name}")
    print(f"{'='*70}")

    results = {}

    # 1. Trade Autopsy
    print(f"\n1️⃣ Running Trade Autopsy...")
    try:
        autopsy = run_trade_autopsy(strategy_name, price_path, trades_path)
        results['autopsy'] = autopsy
        print(f"   ✅ Trade Autopsy complete")
    except Exception as e:
        print(f"   ❌ Trade Autopsy failed: {e}")
        results['autopsy'] = None

    # 2. Regime Analysis
    print(f"\n2️⃣ Running Regime Analysis...")
    try:
        regime = run_regime_analysis(strategy_name, price_path, trades_path)
        results['regime'] = regime
        print(f"   ✅ Regime Analysis complete")
    except Exception as e:
        print(f"   ❌ Regime Analysis failed: {e}")
        results['regime'] = None

    # 3. Statistical Validation
    print(f"\n3️⃣ Running Statistical Validation...")
    try:
        stats = run_statistical_validation(strategy_name, trades_path)
        results['stats'] = stats
        print(f"   ✅ Statistical Validation complete")
    except Exception as e:
        print(f"   ❌ Statistical Validation failed: {e}")
        results['stats'] = None

    # 4. Cost Modeling
    print(f"\n4️⃣ Running Cost Modeling...")
    try:
        costs = run_cost_modeling(
            strategy_name,
            trades_path,
            price_path,
            commission_pct=0.10,
            slippage_pct=0.15,
            spread_pct=0.05
        )
        results['costs'] = costs
        print(f"   ✅ Cost Modeling complete")
    except Exception as e:
        print(f"   ❌ Cost Modeling failed: {e}")
        results['costs'] = None

    # 5. Benchmark Comparison
    print(f"\n5️⃣ Running Benchmark Comparison...")
    try:
        benchmark = run_benchmark_comparison(strategy_name, price_path, trades_path)
        results['benchmark'] = benchmark
        print(f"   ✅ Benchmark Comparison complete")
    except Exception as e:
        print(f"   ❌ Benchmark Comparison failed: {e}")
        results['benchmark'] = None

    return results


def generate_decision_matrix(all_results):
    """📊 Generate Fix/Kill/Optimize decision matrix"""
    print(f"\n{'='*70}")
    print(f"📊 PHASE 0 DECISION MATRIX")
    print(f"{'='*70}")

    decision_data = []

    for strategy_name, results in all_results.items():
        print(f"\n🎯 {strategy_name}:")

        # Evaluate each diagnostic
        autopsy_pass = False
        regime_pass = False
        stats_pass = False
        costs_pass = False
        benchmark_pass = False

        if results.get('autopsy'):
            fixable_pct = (results['autopsy'].false_breakouts + results['autopsy'].late_exits) / max(results['autopsy'].losing_trades, 1) * 100
            autopsy_pass = fixable_pct >= 50
            print(f"   Trade Autopsy: {'✅ PASS' if autopsy_pass else '❌ FAIL'} ({fixable_pct:.0f}% fixable losses)")

        if results.get('regime'):
            # Check if any regime is profitable
            best_regime_name = results['regime'].best_regime
            best_regime_return = results['regime'].regime_returns.get(best_regime_name, 0)
            regime_pass = best_regime_return > 0
            print(f"   Regime Analysis: {'✅ PASS' if regime_pass else '❌ FAIL'} (best: {best_regime_return:.1f}%)")

        if results.get('stats'):
            bootstrap_positive = results['stats'].ci_95_lower > 0
            tests_passed = sum([
                results['stats'].sample_size_adequate,
                results['stats'].win_rate_significant,
                bootstrap_positive,
                results['stats'].monte_carlo_passed,
                results['stats'].sharpe_stable
            ])
            stats_pass = tests_passed >= 3
            print(f"   Statistical Tests: {'✅ PASS' if stats_pass else '❌ FAIL'} ({tests_passed}/5)")

        if results.get('costs'):
            costs_pass = results['costs'].real_total_return_dollars > 0
            print(f"   Cost Modeling: {'✅ PASS' if costs_pass else '❌ FAIL'}")

        if results.get('benchmark'):
            benchmark_pass = results['benchmark'].worth_active_trading
            print(f"   Benchmark: {'✅ PASS' if benchmark_pass else '❌ FAIL'}")

        # Calculate total score
        total_score = sum([autopsy_pass, regime_pass, stats_pass, costs_pass, benchmark_pass])

        # Make decision
        if total_score >= 4:
            decision = "✅ OPTIMIZE"
            action = "Strong edge - proceed to Phase 1 optimization"
        elif total_score == 3:
            decision = "⚠️ FIX"
            action = "Weak edge - fix issues then re-evaluate"
        else:
            decision = "❌ KILL"
            action = "No edge - abandon strategy"

        print(f"\n   Score: {total_score}/5")
        print(f"   Decision: {decision}")
        print(f"   Action: {action}")

        decision_data.append({
            'Strategy': strategy_name,
            'Autopsy': 'PASS' if autopsy_pass else 'FAIL',
            'Regime': 'PASS' if regime_pass else 'FAIL',
            'Statistical': 'PASS' if stats_pass else 'FAIL',
            'Costs': 'PASS' if costs_pass else 'FAIL',
            'Benchmark': 'PASS' if benchmark_pass else 'FAIL',
            'Total_Score': f"{total_score}/5",
            'Decision': decision,
            'Action': action
        })

    # Save decision matrix
    decision_df = pd.DataFrame(decision_data)
    output_path = 'strategies/diagnostics/results/PHASE0_DECISION_MATRIX.csv'
    decision_df.to_csv(output_path, index=False)
    print(f"\n💾 Decision matrix saved: {output_path}")

    return decision_df


def main():
    """🚀 Main execution pipeline"""
    print(f"\n{'='*70}")
    print(f"🔬 PHASE 0 DIAGNOSTIC PIPELINE - STARTING")
    print(f"{'='*70}")
    print(f"📅 Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    all_results = {}

    # Step 1: Run backtests and export trades
    print(f"\n📦 STEP 1: Running Backtests & Exporting Trades")
    print(f"{'='*70}")

    trades_exports = {}

    for strategy_name, config in STRATEGIES.items():
        try:
            trades_path, price_path = run_backtest_and_export_trades(strategy_name, config)
            if trades_path:
                trades_exports[strategy_name] = {
                    'trades_path': trades_path,
                    'price_path': price_path
                }
        except Exception as e:
            print(f"❌ Failed to run {strategy_name}: {e}")
            import traceback
            traceback.print_exc()

    # Step 2: Run diagnostics on each strategy
    print(f"\n🔬 STEP 2: Running Phase 0 Diagnostics")
    print(f"{'='*70}")

    for strategy_name, paths in trades_exports.items():
        try:
            results = run_full_diagnostics(
                strategy_name,
                paths['trades_path'],
                paths['price_path']
            )
            all_results[strategy_name] = results
        except Exception as e:
            print(f"❌ Failed diagnostics for {strategy_name}: {e}")
            import traceback
            traceback.print_exc()

    # Step 3: Generate decision matrix
    print(f"\n📊 STEP 3: Generating Decision Matrix")
    print(f"{'='*70}")

    if all_results:
        decision_matrix = generate_decision_matrix(all_results)
        print(f"\n{decision_matrix.to_string()}")
    else:
        print("⚠️ No results to generate decision matrix")

    print(f"\n{'='*70}")
    print(f"✅ PHASE 0 DIAGNOSTIC PIPELINE - COMPLETE")
    print(f"{'='*70}")
    print(f"📅 End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n📁 All results saved to: strategies/diagnostics/results/")
    print(f"🎯 Review decision matrix to determine Fix/Kill/Optimize for each strategy")


if __name__ == "__main__":
    main()
