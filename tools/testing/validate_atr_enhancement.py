"""
🔬 Validate ATR Volatility Filter Enhancement on Real Data
Compare baseline vs enhanced strategy performance on in-sample and out-of-sample data
"""

import pandas as pd
import json
from pathlib import Path
from backtesting import Backtest
import sys

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from strategies.core_strategies.breakout_momentum_strategy import BreakoutMomentumStrategy


def load_data_splits():
    """📅 Load data split configuration"""
    splits_path = project_root / 'strategies/results/phase0_improvements/data_splits.json'
    with open(splits_path, 'r') as f:
        return json.load(f)


def load_baseline_metrics():
    """📊 Load baseline metrics from Phase 0"""
    metrics_path = project_root / 'strategies/results/phase0_improvements/baseline_metrics.json'
    with open(metrics_path, 'r') as f:
        return json.load(f)


def load_real_data():
    """💾 Load real LINK OHLCV data"""
    data_path = project_root / 'dataset_files/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv'
    df = pd.read_csv(data_path)

    # Normalize column names
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        if col not in df.columns:
            matching = [c for c in df.columns if c.lower() == col.lower()]
            if matching:
                df[col] = df[matching[0]]

    # Parse date column
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

    return df


def split_data(df, data_splits):
    """📊 Split data into in-sample, validation, and out-of-sample"""
    splits = data_splits['splits']

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # Extract dates
    val_start = pd.to_datetime(splits['validation']['start_date'])
    val_end = pd.to_datetime(splits['validation']['end_date'])
    oos_start = pd.to_datetime(splits['out_of_sample']['start_date'])

    # Split data by date
    in_sample = df.loc[df.index < val_start].copy()
    validation = df.loc[(df.index >= val_start) & (df.index <= val_end)].copy()
    out_of_sample = df.loc[df.index >= oos_start].copy()

    return {
        'in_sample': in_sample,
        'validation': validation,
        'out_of_sample': out_of_sample,
        'full': df
    }


def run_backtest(df, strategy_class, use_atr_filter=True, **kwargs):
    """🧪 Run backtest with given parameters"""
    bt = Backtest(
        df,
        strategy_class,
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    stats = bt.run(use_atr_filter=use_atr_filter, **kwargs)
    return stats


def format_metric(value, is_percentage=False, decimals=2):
    """Format metric for display"""
    if pd.isna(value) or value is None:
        return "N/A"
    if is_percentage:
        return f"{value:.{decimals}f}%"
    return f"{value:.{decimals}f}"


def compare_results(baseline_stats, enhanced_stats, period_name):
    """📊 Compare baseline vs enhanced results"""
    print(f"\n{'='*60}")
    print(f"📊 {period_name} Results")
    print(f"{'='*60}\n")

    metrics = [
        ('Return [%]', True, 2),
        ('Sharpe Ratio', False, 3),
        ('Sortino Ratio', False, 3),
        ('Max. Drawdown [%]', True, 2),
        ('# Trades', False, 0),
        ('Win Rate [%]', True, 2),
        ('Avg. Trade [%]', True, 2),
    ]

    print(f"{'Metric':<25} {'Baseline':<15} {'Enhanced':<15} {'Change':<15}")
    print("-" * 70)

    improvements = {}

    for metric, is_pct, decimals in metrics:
        baseline_val = baseline_stats.get(metric, None)
        enhanced_val = enhanced_stats.get(metric, None)

        if baseline_val is not None and enhanced_val is not None:
            if metric == '# Trades':
                change = enhanced_val - baseline_val
                change_str = f"{change:+.0f}"
            elif metric == 'Max. Drawdown [%]':
                # For drawdown, negative change is improvement
                change = enhanced_val - baseline_val
                change_str = f"{change:+.2f}%"
                improvements[metric] = change < 0  # Smaller drawdown is better
            else:
                # For other metrics, positive change is improvement
                if baseline_val != 0:
                    pct_change = ((enhanced_val - baseline_val) / abs(baseline_val)) * 100
                    change_str = f"{pct_change:+.1f}%"
                    improvements[metric] = pct_change > 0
                else:
                    change_str = "N/A"
                    improvements[metric] = enhanced_val > baseline_val

            baseline_str = format_metric(baseline_val, is_pct, decimals)
            enhanced_str = format_metric(enhanced_val, is_pct, decimals)

            print(f"{metric:<25} {baseline_str:<15} {enhanced_str:<15} {change_str:<15}")
        else:
            print(f"{metric:<25} {'N/A':<15} {'N/A':<15} {'N/A':<15}")

    return improvements


def main():
    """🚀 Main validation workflow"""
    print("🔬 ATR Volatility Filter Enhancement Validation")
    print("=" * 60)

    # Load data and configuration
    print("\n📥 Loading data and configuration...")
    df = load_real_data()
    data_splits = load_data_splits()
    baseline_metrics = load_baseline_metrics()
    splits = split_data(df, data_splits)

    print(f"✅ Data loaded: {len(df)} total bars")
    print(f"   In-sample: {len(splits['in_sample'])} bars")
    print(f"   Validation: {len(splits['validation'])} bars")
    print(f"   Out-of-sample: {len(splits['out_of_sample'])} bars")

    # Test on in-sample data
    print("\n" + "="*60)
    print("📈 Testing on In-Sample Data")
    print("="*60)

    print("\n🔄 Running baseline strategy (no ATR filter)...")
    baseline_in_sample = run_backtest(splits['in_sample'], BreakoutMomentumStrategy, use_atr_filter=False)

    print("\n🌪️ Running enhanced strategy (with ATR filter)...")
    enhanced_in_sample = run_backtest(splits['in_sample'], BreakoutMomentumStrategy, use_atr_filter=True)

    in_sample_improvements = compare_results(baseline_in_sample, enhanced_in_sample, "In-Sample")

    # Test on validation data
    print("\n" + "="*60)
    print("📈 Testing on Validation Data")
    print("="*60)

    print("\n🔄 Running baseline strategy (no ATR filter)...")
    baseline_validation = run_backtest(splits['validation'], BreakoutMomentumStrategy, use_atr_filter=False)

    print("\n🌪️ Running enhanced strategy (with ATR filter)...")
    enhanced_validation = run_backtest(splits['validation'], BreakoutMomentumStrategy, use_atr_filter=True)

    validation_improvements = compare_results(baseline_validation, enhanced_validation, "Validation")

    # Test on out-of-sample data (if available)
    if len(splits['out_of_sample']) > 50:
        print("\n" + "="*60)
        print("📈 Testing on Out-of-Sample Data")
        print("="*60)

        print("\n🔄 Running baseline strategy (no ATR filter)...")
        baseline_oos = run_backtest(splits['out_of_sample'], BreakoutMomentumStrategy, use_atr_filter=False)

        print("\n🌪️ Running enhanced strategy (with ATR filter)...")
        enhanced_oos = run_backtest(splits['out_of_sample'], BreakoutMomentumStrategy, use_atr_filter=True)

        oos_improvements = compare_results(baseline_oos, enhanced_oos, "Out-of-Sample")
    else:
        print("\n⚠️ Out-of-sample data too small for meaningful validation")
        oos_improvements = None

    # Decision gate
    print("\n" + "="*60)
    print("🎯 Enhancement Decision Gate")
    print("="*60)

    # Check Sharpe ratio improvement
    sharpe_improved_in_sample = in_sample_improvements.get('Sharpe Ratio', False)
    sharpe_improved_validation = validation_improvements.get('Sharpe Ratio', False)

    sharpe_baseline_in_sample = baseline_in_sample.get('Sharpe Ratio', 0)
    sharpe_enhanced_in_sample = enhanced_in_sample.get('Sharpe Ratio', 0)
    sharpe_baseline_validation = baseline_validation.get('Sharpe Ratio', 0)
    sharpe_enhanced_validation = enhanced_validation.get('Sharpe Ratio', 0)

    if sharpe_baseline_in_sample != 0:
        sharpe_improvement_in_sample_pct = ((sharpe_enhanced_in_sample - sharpe_baseline_in_sample) / abs(sharpe_baseline_in_sample)) * 100
    else:
        sharpe_improvement_in_sample_pct = 0

    if sharpe_baseline_validation != 0:
        sharpe_improvement_validation_pct = ((sharpe_enhanced_validation - sharpe_baseline_validation) / abs(sharpe_baseline_validation)) * 100
    else:
        sharpe_improvement_validation_pct = 0

    print(f"\n📊 Sharpe Ratio Improvement:")
    print(f"   In-Sample: {sharpe_improvement_in_sample_pct:+.1f}%")
    print(f"   Validation: {sharpe_improvement_validation_pct:+.1f}%")

    # Decision thresholds (from Step 1.2 success criteria)
    print(f"\n🎯 Decision Criteria:")
    print(f"   KEEP: Validation Sharpe improves by >20%")
    print(f"   ADJUST: Validation Sharpe improves 10-20%")
    print(f"   REVERT: Validation Sharpe degrades or improves <10%")

    print(f"\n🏆 Final Decision:")
    if sharpe_improvement_validation_pct > 20:
        print(f"   ✅ KEEP ENHANCEMENT - Validation Sharpe improved by {sharpe_improvement_validation_pct:.1f}%")
        decision = "KEEP"
    elif sharpe_improvement_validation_pct > 10:
        print(f"   ⚠️ ADJUST ENHANCEMENT - Validation Sharpe improved by {sharpe_improvement_validation_pct:.1f}%")
        print(f"   Consider tuning ATR thresholds or percentile cutoffs")
        decision = "ADJUST"
    else:
        print(f"   ❌ REVERT ENHANCEMENT - Validation Sharpe only improved by {sharpe_improvement_validation_pct:.1f}%")
        print(f"   Enhancement does not meet minimum improvement threshold")
        decision = "REVERT"

    return {
        'decision': decision,
        'in_sample': {
            'baseline': baseline_in_sample,
            'enhanced': enhanced_in_sample,
            'improvements': in_sample_improvements
        },
        'validation': {
            'baseline': baseline_validation,
            'enhanced': enhanced_validation,
            'improvements': validation_improvements
        }
    }


if __name__ == '__main__':
    results = main()
    print("\n🌙💫🚀 Validation complete!")
