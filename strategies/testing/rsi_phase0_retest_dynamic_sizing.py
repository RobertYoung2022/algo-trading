"""
🔄 RSI Phase 0 Re-Test with Dynamic Position Sizing
===================================================
Re-run Phase 0 baseline tests using Phase 2's dynamic position sizing
to create fair apples-to-apples comparison.

🎯 Purpose:
- Phase 0 used fixed 5% position sizing
- Phase 2 uses dynamic 0.05-0.95 position sizing based on RSI strength
- This re-test uses Phase 2's dynamic sizing WITHOUT safety features
- Determines if performance differences are due to:
  ✓ Position sizing changes (expected)
  ✓ Safety features (expected to be minimal)
  ✗ Data differences or bugs (would be concerning)
"""

import sys
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/core_strategies')
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/testing')

from backtesting import Backtest
from rsi_mean_reversion_strategy import RSIMeanReversionStrategy
from rsi_phase2_validation_test import load_data

class RSIPhase0RetestStrategy(RSIMeanReversionStrategy):
    """
    Phase 0 Re-Test Version:
    - Uses dynamic position sizing (from Phase 2)
    - DISABLES Phase 2 safety features (for fair Phase 0 comparison)
    """

    def _validate_safety_features(self):
        """🔄 DISABLED for Phase 0 re-test - skip all safety validations"""
        print("🔄 Phase 0 Re-Test Mode: Safety features DISABLED")
        return

def run_phase0_retest():
    """Run Phase 0 baseline re-test with dynamic position sizing"""

    print("\n" + "="*70)
    print("🔄 RSI PHASE 0 RE-TEST: Dynamic Position Sizing Comparison")
    print("="*70)
    print("\nTesting 4 assets with Phase 2's dynamic position sizing")
    print("(WITHOUT Phase 2 safety features - pure Phase 0 baseline)")

    # Test configuration
    tests = [
        {
            'asset': 'XRP',
            'file': '/Users/bobbyyo/Projects/algo-fun/dataset_files/crypto_hist_data/Bitstamp_XRPUSD_d.csv',
            'phase0_return': 107.91,
            'phase0_win_rate': 63.9,
            'phase0_trades': 72,
            'phase2_return': 84.24,
            'phase2_win_rate': 59.8,
            'phase2_trades': 82
        },
        {
            'asset': 'BTC',
            'file': '/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv',
            'phase0_return': 46.84,
            'phase0_win_rate': 80.0,
            'phase0_trades': 15,
            'phase2_return': 90.53,
            'phase2_win_rate': 67.6,
            'phase2_trades': 37
        },
        {
            'asset': 'ETH',
            'file': '/Users/bobbyyo/Projects/algo-fun/dataset_files/crypto_hist_data/Bitstamp_ETHUSDC_d.csv',
            'phase0_return': 35.10,
            'phase0_win_rate': 70.4,
            'phase0_trades': 27,
            'phase2_return': 42.17,
            'phase2_win_rate': 66.7,
            'phase2_trades': 30
        },
        {
            'asset': 'HBAR',
            'file': '/Users/bobbyyo/Projects/algo-fun/dataset_files/yahoo/HBARUSD-20yr-yahoo-data.csv',
            'phase0_return': 46.11,
            'phase0_win_rate': 61.7,
            'phase0_trades': 47,
            'phase2_return': 6.17,
            'phase2_win_rate': 59.6,
            'phase2_trades': 52
        }
    ]

    results = []

    for test in tests:
        print(f"\n" + "="*70)
        print(f"Testing {test['asset']}...")
        print("="*70)

        try:
            # Load data
            df = load_data(test['file'])
            print(f"✅ Loaded: {len(df)} bars")

            # Run backtest with Phase 0 re-test strategy (dynamic sizing, no safety)
            bt = Backtest(df, RSIPhase0RetestStrategy, cash=10000, commission=.002)
            stats = bt.run()

            # Extract results
            retest_return = stats['Return [%]']
            retest_win_rate = stats.get('Win Rate [%]', 0)
            retest_trades = stats['# Trades']
            retest_sharpe = stats.get('Sharpe Ratio', 0)

            # Calculate differences
            diff_from_phase0 = retest_return - test['phase0_return']
            diff_from_phase2 = retest_return - test['phase2_return']

            results.append({
                'asset': test['asset'],
                'phase0_original': test['phase0_return'],
                'phase0_retest': retest_return,
                'phase2': test['phase2_return'],
                'diff_phase0': diff_from_phase0,
                'diff_phase2': diff_from_phase2,
                'trades': retest_trades,
                'win_rate': retest_win_rate,
                'sharpe': retest_sharpe
            })

            # Display results
            print(f"\n📊 {test['asset']} Results:")
            print(f"   Phase 0 Original (fixed 5%):  {test['phase0_return']:.2f}% return, {test['phase0_win_rate']:.1f}% WR, {test['phase0_trades']} trades")
            print(f"   Phase 0 Re-Test (dynamic):    {retest_return:.2f}% return, {retest_win_rate:.1f}% WR, {retest_trades} trades")
            print(f"   Phase 2 (dynamic + safety):   {test['phase2_return']:.2f}% return, {test['phase2_win_rate']:.1f}% WR, {test['phase2_trades']} trades")
            print(f"\n   📈 Change from Phase 0 Original: {diff_from_phase0:+.2f}% ({diff_from_phase0/test['phase0_return']*100:+.1f}%)")
            print(f"   🛡️ Change from Phase 2 (safety impact): {diff_from_phase2:+.2f}%")

            # Interpretation
            if abs(diff_from_phase2) < 5:
                print(f"   ✅ Safety features have minimal impact (< 5% difference)")
            elif diff_from_phase2 > 5:
                print(f"   ⚠️  Re-test outperforms Phase 2 by {diff_from_phase2:.1f}% (safety features may be restrictive)")
            else:
                print(f"   ⚠️  Phase 2 outperforms re-test by {-diff_from_phase2:.1f}% (unexpected - investigate)")

        except Exception as e:
            print(f"❌ {test['asset']} failed: {e}")
            import traceback
            traceback.print_exc()

    # Summary table
    print("\n" + "="*70)
    print("📊 PHASE 0 RE-TEST SUMMARY")
    print("="*70)
    print("\nComparison Table:")
    print(f"{'Asset':<8} {'Phase0 Orig':<12} {'Phase0 Retest':<14} {'Phase2':<10} {'ΔOrig%':<10} {'ΔP2%':<8}")
    print("-" * 70)

    for r in results:
        delta_orig_pct = (r['diff_phase0'] / r['phase0_original'] * 100) if r['phase0_original'] != 0 else 0
        delta_p2_pct = (r['diff_phase2'] / r['phase2'] * 100) if r['phase2'] != 0 else 0

        print(f"{r['asset']:<8} {r['phase0_original']:>11.2f}% {r['phase0_retest']:>13.2f}% {r['phase2']:>9.2f}% "
              f"{delta_orig_pct:>+9.1f}% {delta_p2_pct:>+7.1f}%")

    # Key findings
    print("\n" + "="*70)
    print("🔍 KEY FINDINGS")
    print("="*70)

    avg_diff_from_orig = sum([r['diff_phase0'] for r in results]) / len(results)
    avg_diff_from_p2 = sum([abs(r['diff_phase2']) for r in results]) / len(results)

    print(f"\n1. Average difference from Phase 0 Original: {avg_diff_from_orig:+.2f}%")
    if abs(avg_diff_from_orig) > 10:
        print(f"   → Dynamic position sizing shows {abs(avg_diff_from_orig):.1f}% average impact")
        print(f"   → {'Improvement' if avg_diff_from_orig > 0 else 'Degradation'} from fixed 5% sizing")
    else:
        print(f"   → Position sizing change has minimal overall impact")

    print(f"\n2. Average difference from Phase 2 (safety features): {avg_diff_from_p2:.2f}%")
    if avg_diff_from_p2 < 3:
        print(f"   → ✅ Safety features have negligible impact on performance")
        print(f"   → Phase 2 is production-ready without performance penalty")
    elif avg_diff_from_p2 < 10:
        print(f"   → ⚠️  Safety features have minor impact ({avg_diff_from_p2:.1f}% avg)")
        print(f"   → Acceptable tradeoff for risk reduction")
    else:
        print(f"   → ❌ Safety features significantly impact performance ({avg_diff_from_p2:.1f}% avg)")
        print(f"   → Investigate which safety features are too restrictive")

    print(f"\n3. Performance Variance Pattern:")
    for r in results:
        if r['diff_phase0'] > 20:
            print(f"   • {r['asset']}: Large improvement with dynamic sizing (+{r['diff_phase0']:.1f}%)")
        elif r['diff_phase0'] < -20:
            print(f"   • {r['asset']}: Large degradation with dynamic sizing ({r['diff_phase0']:.1f}%)")
        else:
            print(f"   • {r['asset']}: Stable performance ({r['diff_phase0']:+.1f}%)")

    # Verdict
    print("\n" + "="*70)
    print("🎯 VERDICT")
    print("="*70)

    if avg_diff_from_p2 < 5:
        print("\n✅ PHASE 2 APPROVED - Fair comparison confirmed")
        print("   • Dynamic position sizing explains performance variance")
        print("   • Safety features have minimal performance impact")
        print("   • Phase 2 is production-ready")
    else:
        print("\n⚠️  PHASE 2 NEEDS REVIEW")
        print(f"   • Safety features showing {avg_diff_from_p2:.1f}% average impact")
        print("   • Investigate which safety features are too restrictive")

    print("\n🌙💫🚀 Phase 0 re-test complete!")

    return results

if __name__ == "__main__":
    run_phase0_retest()
