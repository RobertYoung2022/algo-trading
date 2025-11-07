#!/usr/bin/env python3
"""
Debug Signal Generation - Show ALL signals with rejection reasons

This helps us understand:
1. Are signals being generated at all?
2. Why are they being rejected?
3. Are entry/stop/target levels correct now?
"""

import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from strategies.ict_strategy.signal_generator import ICTSignalGenerator


def test_signal_debug():
    """Debug signal generator with detailed output"""

    print("\n" + "="*80)
    print(" ICT SIGNAL GENERATION DEBUG")
    print("="*80)

    # Load 1-year BTC data
    df = pd.read_csv("data/storage/dataset_files/BTCUSD-1h-1year-test.csv")
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    print(f"\n📊 Loaded {len(df)} bars of BTC data")
    print(f"📅 Range: {df.index[0]} to {df.index[-1]}")
    print(f"💵 Price: ${df['close'].iloc[0]:.2f} -> ${df['close'].iloc[-1]:.2f}")

    # Initialize signal generator
    sig_gen = ICTSignalGenerator()

    # Test on first window only for detailed analysis
    print("\n🔍 Testing signal generation on ONE window with full debug output...")
    print("="*80)

    i = 4500  # Use same window that found signal before
    df_higher = df.iloc[i-300:i+100]  # 400 bars for HTF
    df_lower = df.iloc[i-150:i+50]     # 200 bars for LTF

    print(f"\n📍 Testing at bar {i} (datetime: {df.index[i]})")
    print(f"   Current price: ${df['close'].iloc[i]:.2f}")

    # Test LONG signal
    print("\n" + "="*80)
    print("🟢 LONG SIGNAL TEST")
    print("="*80)

    long_signal = sig_gen.generate_signal(
        df_higher=df_higher,
        df_lower=df_lower,
        symbol='BTC',
        direction='long'
    )

    print(f"\n📊 LONG Signal Results:")
    print(f"   Valid: {long_signal['valid']}")
    print(f"   Confirmations: {long_signal['confirmations']} ({len(long_signal['confirmations'])})")
    print(f"   Confidence: {long_signal['confidence']}")
    print(f"   Notes: {long_signal['notes']}")

    if long_signal['entry']:
        print(f"\n💰 Entry Levels:")
        print(f"   Entry: ${long_signal['entry']:.2f}")
        print(f"   Stop: ${long_signal['stop']:.2f}")
        print(f"   Target: ${long_signal['target']:.2f}")
        print(f"   Risk: ${long_signal['risk']:.2f}")
        print(f"   Reward: ${long_signal['reward']:.2f}")
        print(f"   R:R Ratio: {long_signal['rr_ratio']:.2f}")

        # Validate levels
        print(f"\n✅ Level Validation:")
        if long_signal['stop'] < long_signal['entry']:
            print(f"   ✅ Stop is BELOW entry (correct for LONG)")
        else:
            print(f"   ❌ Stop is ABOVE entry (WRONG for LONG)")

        if long_signal['target'] > long_signal['entry']:
            print(f"   ✅ Target is ABOVE entry (correct for LONG)")
        else:
            print(f"   ❌ Target is BELOW entry (WRONG for LONG)")

        if long_signal['rr_ratio'] >= 1.5:
            print(f"   ✅ R:R ratio >= 1.5 (meets requirement)")
        else:
            print(f"   ❌ R:R ratio < 1.5 (fails requirement)")

    # Test SHORT signal
    print("\n" + "="*80)
    print("🔴 SHORT SIGNAL TEST")
    print("="*80)

    short_signal = sig_gen.generate_signal(
        df_higher=df_higher,
        df_lower=df_lower,
        symbol='BTC',
        direction='short'
    )

    print(f"\n📊 SHORT Signal Results:")
    print(f"   Valid: {short_signal['valid']}")
    print(f"   Confirmations: {short_signal['confirmations']} ({len(short_signal['confirmations'])})")
    print(f"   Confidence: {short_signal['confidence']}")
    print(f"   Notes: {short_signal['notes']}")

    if short_signal['entry']:
        print(f"\n💰 Entry Levels:")
        print(f"   Entry: ${short_signal['entry']:.2f}")
        print(f"   Stop: ${short_signal['stop']:.2f}")
        print(f"   Target: ${short_signal['target']:.2f}")
        print(f"   Risk: ${short_signal['risk']:.2f}")
        print(f"   Reward: ${short_signal['reward']:.2f}")
        print(f"   R:R Ratio: {short_signal['rr_ratio']:.2f}")

        # Validate levels
        print(f"\n✅ Level Validation:")
        if short_signal['stop'] > short_signal['entry']:
            print(f"   ✅ Stop is ABOVE entry (correct for SHORT)")
        else:
            print(f"   ❌ Stop is BELOW entry (WRONG for SHORT)")

        if short_signal['target'] < short_signal['entry']:
            print(f"   ✅ Target is BELOW entry (correct for SHORT)")
        else:
            print(f"   ❌ Target is ABOVE entry (WRONG for SHORT)")

        if short_signal['rr_ratio'] >= 1.5:
            print(f"   ✅ R:R ratio >= 1.5 (meets requirement)")
        else:
            print(f"   ❌ R:R ratio < 1.5 (fails requirement)")

    # Summary
    print("\n" + "="*80)
    print(" DIAGNOSIS")
    print("="*80)

    if long_signal['valid'] or short_signal['valid']:
        print("\n✅ Signal generator IS producing valid signals")
        print("   Entry level fix appears to be working correctly")
    else:
        print("\n❌ No valid signals produced")
        if not long_signal['confirmations'] and not short_signal['confirmations']:
            print("   🔍 Issue: No confirmations detected")
            print("   💡 Recommendation: Test individual confirmation patterns")
        elif long_signal.get('rr_ratio', 0) < 1.5:
            print("   🔍 Issue: R:R ratio too low")
            print("   💡 Recommendation: Adjust ATR multiplier or target calculation")
        else:
            print("   🔍 Issue: Unknown - check notes above")

    print("="*80)

    return long_signal, short_signal


if __name__ == "__main__":
    test_signal_debug()
