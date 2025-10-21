"""
🚀 Simple Test of MA-RSI-Volume Optimized Strategy 🚀
=====================================================
Quick test of optimized strategy performance improvements
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
from ma_rsi_volume_optimized_strategy import MARSIVolumeOptimizedStrategy
from ma_rsi_volume_adaptive_strategy import MARSIVolumeAdaptiveStrategy

print("🚀 Testing MA-RSI-Volume Optimization Results... 💫\n")


def test_xrp_performance():
    """Test on XRP data which should work better"""

    # Load XRP data
    df = pd.read_csv('/Users/bobbyyo/Projects/algo-fun/data/XRP_1h_20250914_210236_historical.csv')

    # Prepare data
    if 'timestamp' in df.columns:
        df['Date'] = pd.to_datetime(df['timestamp'])
    elif 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])

    df = df.set_index('Date')

    # Standardize columns
    column_mapping = {
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }
    df = df.rename(columns=column_mapping)

    print(f"📊 Testing on XRP - 1H Data")
    print(f"📅 Data range: {df.index[0]} to {df.index[-1]}")
    print(f"📈 Data points: {len(df)}\n")

    # Test original strategy
    print("="*60)
    print("🔧 TESTING ORIGINAL ADAPTIVE STRATEGY")
    print("="*60)

    bt_original = Backtest(df, MARSIVolumeAdaptiveStrategy, cash=10000, commission=0.002)
    stats_original = bt_original.run()

    print(f"Return: {stats_original['Return [%]']:.2f}%")
    print(f"Sharpe Ratio: {stats_original['Sharpe Ratio']:.3f}")
    print(f"Max Drawdown: {stats_original['Max. Drawdown [%]']:.2f}%")
    print(f"Win Rate: {stats_original['Win Rate [%]']:.1f}%")
    print(f"Number of Trades: {stats_original['# Trades']}")
    print(f"Profit Factor: {stats_original.get('Profit Factor', 'N/A')}")

    # Test optimized strategy
    print("\n" + "="*60)
    print("🚀 TESTING OPTIMIZED STRATEGY")
    print("="*60)

    bt_optimized = Backtest(df, MARSIVolumeOptimizedStrategy, cash=10000, commission=0.002)
    # Use XRP optimized parameters
    stats_optimized = bt_optimized.run(asset_name='XRP')

    print(f"Return: {stats_optimized['Return [%]']:.2f}%")
    print(f"Sharpe Ratio: {stats_optimized['Sharpe Ratio']:.3f}")
    print(f"Max Drawdown: {stats_optimized['Max. Drawdown [%]']:.2f}%")
    print(f"Win Rate: {stats_optimized['Win Rate [%]']:.1f}%")
    print(f"Number of Trades: {stats_optimized['# Trades']}")
    print(f"Profit Factor: {stats_optimized.get('Profit Factor', 'N/A')}")

    # Calculate improvements
    print("\n" + "="*60)
    print("📊 PERFORMANCE IMPROVEMENT ANALYSIS")
    print("="*60)

    return_improvement = stats_optimized['Return [%]'] - stats_original['Return [%]']
    sharpe_improvement = stats_optimized['Sharpe Ratio'] - stats_original['Sharpe Ratio']
    winrate_improvement = stats_optimized['Win Rate [%]'] - stats_original['Win Rate [%]']
    dd_improvement = abs(stats_original['Max. Drawdown [%]']) - abs(stats_optimized['Max. Drawdown [%]'])

    print(f"Return Improvement: {return_improvement:+.2f}%")
    print(f"Sharpe Improvement: {sharpe_improvement:+.3f}")
    print(f"Win Rate Improvement: {winrate_improvement:+.1f}%")
    print(f"Drawdown Reduction: {dd_improvement:+.2f}%")

    # Success assessment
    print("\n" + "="*60)
    print("🎯 OPTIMIZATION TARGET ACHIEVEMENT")
    print("="*60)

    targets_met = []
    targets_failed = []

    # Check targets
    if stats_optimized['Win Rate [%]'] >= 50:
        targets_met.append(f"✅ Win Rate ≥50% (Achieved: {stats_optimized['Win Rate [%]']:.1f}%)")
    else:
        targets_failed.append(f"❌ Win Rate <50% (Current: {stats_optimized['Win Rate [%]']:.1f}%)")

    if stats_optimized['Return [%]'] > 0:
        targets_met.append(f"✅ Positive Returns (Achieved: {stats_optimized['Return [%]']:.2f}%)")
    else:
        targets_failed.append(f"❌ Negative Returns (Current: {stats_optimized['Return [%]']:.2f}%)")

    if abs(stats_optimized['Max. Drawdown [%]']) < 20:
        targets_met.append(f"✅ Max Drawdown <20% (Achieved: {abs(stats_optimized['Max. Drawdown [%]']):.2f}%)")
    else:
        targets_failed.append(f"❌ Max Drawdown >20% (Current: {abs(stats_optimized['Max. Drawdown [%]']):.2f}%)")

    if stats_optimized['Sharpe Ratio'] > 1.0:
        targets_met.append(f"✅ Sharpe Ratio >1.0 (Achieved: {stats_optimized['Sharpe Ratio']:.3f})")
    else:
        targets_failed.append(f"⚠️ Sharpe Ratio <1.0 (Current: {stats_optimized['Sharpe Ratio']:.3f})")

    for target in targets_met:
        print(target)
    for target in targets_failed:
        print(target)

    # Overall assessment
    print("\n" + "="*60)
    print("📈 OVERALL ASSESSMENT")
    print("="*60)

    if len(targets_met) >= 3:
        print("🎉 SUCCESS: Optimization has significantly improved the strategy!")
        print(f"   {len(targets_met)}/4 targets achieved")
    elif len(targets_met) >= 2:
        print("✅ PARTIAL SUCCESS: Strategy improved but needs more tuning")
        print(f"   {len(targets_met)}/4 targets achieved")
    else:
        print("⚠️ LIMITED SUCCESS: Strategy needs different approach or parameters")
        print(f"   {len(targets_met)}/4 targets achieved")

    return stats_original, stats_optimized


if __name__ == "__main__":
    try:
        stats_original, stats_optimized = test_xrp_performance()

        print("\n" + "="*60)
        print("🌙💫🚀 Optimization Test Complete!")
        print("="*60)

        # Summary recommendations
        print("\n📝 RECOMMENDATIONS:")
        print("1. The optimized strategy uses asset-specific parameters")
        print("2. Signal mode, thresholds, and risk parameters are tuned per asset")
        print("3. Consider further optimization on specific timeframes")
        print("4. Test on more recent data for production readiness")

    except Exception as e:
        print(f"❌ Error during test: {e}")