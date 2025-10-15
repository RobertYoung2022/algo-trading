"""
🧪 RSI Phase 2 Quick Validation - Streamlined Results
====================================================
Run all 8 tests and show summary without @trading_functions spam
"""

import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/testing')

from rsi_phase2_validation_test import (
    test_minute_data_rejection,
    test_cro_asset_exclusion,
    test_trade_limit_enforcement,
    test_data_quality_requirement,
    test_xrp_daily_performance,
    test_btc_daily_performance,
    test_eth_daily_performance,
    test_hbar_daily_performance
)

print("\n" + "="*70)
print("🧪 RSI PHASE 2 QUICK VALIDATION")
print("="*70)

# Run tests
results = {
    'negative': [
        ('Minute Data Rejection', test_minute_data_rejection()),
        ('CRO Asset Exclusion', test_cro_asset_exclusion()),
        ('Trade Limit Enforcement', test_trade_limit_enforcement()),
        ('Data Quality Requirement', test_data_quality_requirement())
    ],
    'positive': [
        ('XRP Daily', test_xrp_daily_performance()),
        ('BTC Daily', test_btc_daily_performance()),
        ('ETH Daily', test_eth_daily_performance()),
        ('HBAR Daily', test_hbar_daily_performance())
    ]
}

# Summary
print("\n" + "="*70)
print("📊 PHASE 2 VALIDATION SUMMARY")
print("="*70)

negative_passed = sum([1 for _, result in results['negative'] if result])
positive_passed = sum([1 for _, result in results['positive'] if result])
total_passed = negative_passed + positive_passed

print(f"\n❌ Negative Tests (Safety Features): {negative_passed}/4 passed")
for test_name, result in results['negative']:
    status = "✅ PASS" if result else "❌ FAIL"
    print(f"   {status}: {test_name}")

print(f"\n✅ Positive Tests (Performance Preservation): {positive_passed}/4 passed")
for test_name, result in results['positive']:
    status = "✅ PASS" if result else "⚠️  WARN"
    print(f"   {status}: {test_name}")

print(f"\n🎯 Overall: {total_passed}/8 tests passed")

if total_passed == 8:
    print("\n🎉 PHASE 2 VALIDATION SUCCESSFUL!")
    print("   ✅ All safety features working correctly")
    print("   ✅ Daily performance preserved from Phase 0")
    print("   ✅ Strategy ready for Phase 2 completion")
elif total_passed >= 6:
    print("\n⚠️  PHASE 2 MOSTLY SUCCESSFUL")
    print("   Review warnings above and proceed with caution")
else:
    print("\n❌ PHASE 2 VALIDATION FAILED")
    print("   Fix failing tests before proceeding")

print("\n🌙💫🚀 Phase 2 validation complete!")
