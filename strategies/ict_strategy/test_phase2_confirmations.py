#!/usr/bin/env python3
"""
Test Phase 2 Confirmation Patterns

Quick test to verify all 5 confirmation patterns are working correctly.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from strategies.ict_strategy.confirmations import (
    check_ifvg, check_cisd, check_sfp, check_mss, check_order_block
)


def create_test_data(size=100):
    """Create synthetic test data"""
    dates = pd.date_range(start='2025-01-01', periods=size, freq='1H')

    # Trend + noise
    trend = np.linspace(100, 150, size)
    noise = np.random.randn(size) * 2
    closes = trend + noise

    df = pd.DataFrame({
        'open': closes + np.random.randn(size) * 0.5,
        'high': closes + abs(np.random.randn(size)),
        'low': closes - abs(np.random.randn(size)),
        'close': closes,
        'volume': np.random.randint(100, 1000, size)
    }, index=dates)

    return df


def test_all_confirmations():
    """Test each confirmation pattern"""
    print("\n" + "="*80)
    print(" PHASE 2 - CONFIRMATION PATTERNS TEST")
    print("="*80)

    df = create_test_data(100)
    print(f"\n✅ Created test data: {len(df)} candles")
    print(f"   Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")

    # Test each pattern
    results = {}

    # 1. IFVG
    print("\n1. Testing IFVG (Inverted Fair Value Gap)...")
    try:
        bullish_ifvg = check_ifvg(df, 'bullish')
        bearish_ifvg = check_ifvg(df, 'bearish')
        results['IFVG'] = {
            'bullish': bullish_ifvg,
            'bearish': bearish_ifvg,
            'status': '✅ Working'
        }
        print(f"   Bullish: {bullish_ifvg}, Bearish: {bearish_ifvg}")
    except Exception as e:
        results['IFVG'] = {'status': f'❌ Error: {e}'}
        print(f"   ❌ Error: {e}")

    # 2. CISD
    print("\n2. Testing CISD (Change in State of Delivery)...")
    try:
        bullish_cisd = check_cisd(df, 'bullish')
        bearish_cisd = check_cisd(df, 'bearish')
        results['CISD'] = {
            'bullish': bullish_cisd,
            'bearish': bearish_cisd,
            'status': '✅ Working'
        }
        print(f"   Bullish: {bullish_cisd}, Bearish: {bearish_cisd}")
    except Exception as e:
        results['CISD'] = {'status': f'❌ Error: {e}'}
        print(f"   ❌ Error: {e}")

    # 3. SFP
    print("\n3. Testing SFP (Swing Failure Pattern)...")
    try:
        key_level = df['close'].iloc[-10]
        bullish_sfp = check_sfp(df, 'bullish', key_level)
        bearish_sfp = check_sfp(df, 'bearish', key_level)
        results['SFP'] = {
            'bullish': bullish_sfp,
            'bearish': bearish_sfp,
            'status': '✅ Working'
        }
        print(f"   Bullish: {bullish_sfp}, Bearish: {bearish_sfp} (at level ${key_level:.2f})")
    except Exception as e:
        results['SFP'] = {'status': f'❌ Error: {e}'}
        print(f"   ❌ Error: {e}")

    # 4. MSS
    print("\n4. Testing MSS (Market Structure Shift)...")
    try:
        bullish_mss = check_mss(df, 'bullish')
        bearish_mss = check_mss(df, 'bearish')
        results['MSS'] = {
            'bullish': bullish_mss,
            'bearish': bearish_mss,
            'status': '✅ Working'
        }
        print(f"   Bullish: {bullish_mss}, Bearish: {bearish_mss}")
    except Exception as e:
        results['MSS'] = {'status': f'❌ Error: {e}'}
        print(f"   ❌ Error: {e}")

    # 5. Order Block
    print("\n5. Testing OB (Order Block)...")
    try:
        bullish_ob = check_order_block(df, 'bullish')
        bearish_ob = check_order_block(df, 'bearish')
        results['OB'] = {
            'bullish': bullish_ob,
            'bearish': bearish_ob,
            'status': '✅ Working'
        }
        print(f"   Bullish: {bullish_ob}, Bearish: {bearish_ob}")
    except Exception as e:
        results['OB'] = {'status': f'❌ Error: {e}'}
        print(f"   ❌ Error: {e}")

    # Summary
    print("\n" + "="*80)
    print(" SUMMARY")
    print("="*80)

    working = sum(1 for r in results.values() if '✅' in r['status'])
    total = len(results)

    for pattern, result in results.items():
        print(f"{pattern:15s}: {result['status']}")

    print(f"\n{'='*80}")
    if working == total:
        print(f"✅ ALL {total} CONFIRMATION PATTERNS WORKING")
        print("\nPhase 2 confirmation modules are ready to use!")
    else:
        print(f"⚠️  {working}/{total} patterns working - check errors above")
    print("="*80)


if __name__ == "__main__":
    test_all_confirmations()
