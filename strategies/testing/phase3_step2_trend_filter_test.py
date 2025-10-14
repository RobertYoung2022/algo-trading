"""
🚀 Phase 3 Step 2: Multi-Timeframe Trend Filter Test
===================================================
Compare Phase 2 baseline vs Phase 3 with multi-timeframe trend confirmation

🎯 Hypothesis:
    Adding weekly SMA(50) trend filter will:
    - Improve win rate from 34-40% to 45-55%
    - Reduce losing trades (filter counter-trend signals)
    - Maintain or improve overall return

📊 Test: ETH daily data (2024-2025)
"""

import sys
import pandas as pd
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/core_strategies')

from backtesting import Backtest
from breakout_momentum_strategy import BreakoutMomentumStrategy

def load_data(file_path):
    """Load and prepare data"""
    df = pd.read_csv(file_path)
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

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

    return df

def test_phase2_baseline(df):
    """Test Phase 2 baseline (no multi-timeframe filter)"""
    print("\n" + "="*70)
    print("PHASE 2 BASELINE (No Multi-Timeframe Filter)")
    print("="*70)

    class Phase2Strategy(BreakoutMomentumStrategy):
        use_multi_timeframe_filter = False  # Disable trend filter

    bt = Backtest(df, Phase2Strategy, cash=10000, commission=.002)
    stats = bt.run()

    print(f"\n📊 Phase 2 Results:")
    print(f"   Return:    {stats['Return [%]']:>7.2f}%")
    print(f"   Sharpe:    {stats.get('Sharpe Ratio', 0):>7.2f}")
    print(f"   Win Rate:  {stats.get('Win Rate [%]', 0):>7.1f}%")
    print(f"   Trades:    {stats['# Trades']:>7}")
    print(f"   Max DD:    {stats['Max. Drawdown [%]']:>7.2f}%")

    return stats

def test_phase3_step2(df):
    """Test Phase 3 Step 2 (with multi-timeframe filter)"""
    print("\n" + "="*70)
    print("PHASE 3 STEP 2 (With Multi-Timeframe Filter)")
    print("="*70)

    class Phase3Step2Strategy(BreakoutMomentumStrategy):
        use_multi_timeframe_filter = True  # Enable trend filter
        weekly_trend_period = 50

    bt = Backtest(df, Phase3Step2Strategy, cash=10000, commission=.002)
    stats = bt.run()

    print(f"\n📊 Phase 3 Step 2 Results:")
    print(f"   Return:    {stats['Return [%]']:>7.2f}%")
    print(f"   Sharpe:    {stats.get('Sharpe Ratio', 0):>7.2f}")
    print(f"   Win Rate:  {stats.get('Win Rate [%]', 0):>7.1f}%")
    print(f"   Trades:    {stats['# Trades']:>7}")
    print(f"   Max DD:    {stats['Max. Drawdown [%]']:>7.2f}%")

    return stats

def compare_results(phase2, phase3):
    """Compare Phase 2 vs Phase 3 Step 2"""
    print("\n" + "="*70)
    print("COMPARISON: Phase 2 vs Phase 3 Step 2")
    print("="*70)

    # Calculate improvements
    return_improvement = phase3['Return [%]'] - phase2['Return [%]']
    sharpe_improvement = phase3.get('Sharpe Ratio', 0) - phase2.get('Sharpe Ratio', 0)
    winrate_improvement = phase3.get('Win Rate [%]', 0) - phase2.get('Win Rate [%]', 0)
    trade_reduction = phase2['# Trades'] - phase3['# Trades']

    print(f"\n📈 Performance Changes:")
    print(f"   Return:    {phase2['Return [%]']:>6.2f}% → {phase3['Return [%]']:>6.2f}%  "
          f"({return_improvement:>+6.2f}%)")
    print(f"   Sharpe:    {phase2.get('Sharpe Ratio', 0):>6.2f} → {phase3.get('Sharpe Ratio', 0):>6.2f}  "
          f"({sharpe_improvement:>+6.2f})")
    print(f"   Win Rate:  {phase2.get('Win Rate [%]', 0):>6.1f}% → {phase3.get('Win Rate [%]', 0):>6.1f}%  "
          f"({winrate_improvement:>+6.1f}%)")
    print(f"   Trades:    {phase2['# Trades']:>6} → {phase3['# Trades']:>6}  "
          f"({-trade_reduction:>+6} change)")

    # Evaluation
    print(f"\n🎯 Hypothesis Test:")
    print(f"   Target: Win rate improvement to 45-55%")

    if winrate_improvement > 0:
        print(f"   ✅ Win rate improved by {winrate_improvement:.1f}%")
    else:
        print(f"   ❌ Win rate declined by {abs(winrate_improvement):.1f}%")

    if phase3.get('Win Rate [%]', 0) >= 45:
        print(f"   ✅ Win rate {phase3.get('Win Rate [%]', 0):.1f}% meets target (45-55%)")
    else:
        print(f"   ⚠️  Win rate {phase3.get('Win Rate [%]', 0):.1f}% below target (45-55%)")

    if return_improvement > 0:
        print(f"   ✅ Return improved by {return_improvement:.2f}%")
    else:
        print(f"   ⚠️  Return decreased by {abs(return_improvement):.2f}%")

    # Overall assessment
    print(f"\n📊 Overall Assessment:")
    improvements = 0
    if winrate_improvement > 0: improvements += 1
    if return_improvement > 0: improvements += 1
    if sharpe_improvement > 0: improvements += 1

    if improvements >= 2:
        print(f"   ✅ STEP 2 SUCCESSFUL: Multi-timeframe filter improves strategy ({improvements}/3 metrics)")
    elif improvements == 1:
        print(f"   ⚠️  STEP 2 MIXED: Some improvement but not across all metrics ({improvements}/3 metrics)")
    else:
        print(f"   ❌ STEP 2 UNSUCCESSFUL: Multi-timeframe filter may hurt performance ({improvements}/3 metrics)")

def main():
    """Run Phase 2 vs Phase 3 Step 2 comparison"""
    print("\n🚀 PHASE 3 STEP 2: MULTI-TIMEFRAME TREND FILTER TEST")
    print("="*70)

    # Load ETH daily data
    data_path = "/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv"
    print(f"📁 Testing on: ETH Daily (Coinbase)")
    print(f"📁 File: {data_path}")

    df = load_data(data_path)
    print(f"📊 Data: {len(df)} bars ({df.index[0].date()} to {df.index[-1].date()})")

    # Run tests
    phase2_stats = test_phase2_baseline(df)
    phase3_stats = test_phase3_step2(df)

    # Compare
    compare_results(phase2_stats, phase3_stats)

    print("\n🌙💫🚀 Phase 3 Step 2 testing complete!")

if __name__ == "__main__":
    main()
