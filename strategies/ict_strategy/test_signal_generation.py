#!/usr/bin/env python3
"""
Test ICT Signal Generation Directly

This tests the signal_generator directly with backtest data
to see if it's producing any valid signals at all.
"""

import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from strategies.ict_strategy.signal_generator import ICTSignalGenerator


def test_signal_generation():
    """Test signal generator with real BTC data"""

    print("\n" + "="*80)
    print(" ICT SIGNAL GENERATION TEST")
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

    # Test on multiple windows throughout the dataset
    print("\n🔍 Testing signal generation on 10 random windows...")
    print("="*80)

    valid_signals = 0
    total_tests = 0

    # Test every 1000 bars
    for i in range(500, len(df) - 500, 1000):
        # Get HTF and LTF data
        df_higher = df.iloc[i-300:i+100]  # 400 bars for HTF
        df_lower = df.iloc[i-150:i+50]     # 200 bars for LTF

        total_tests += 2

        # Test long signal
        long_signal = sig_gen.generate_signal(
            df_higher=df_higher,
            df_lower=df_lower,
            symbol='BTC',
            direction='long'
        )

        # Test short signal
        short_signal = sig_gen.generate_signal(
            df_higher=df_higher,
            df_lower=df_lower,
            symbol='BTC',
            direction='short'
        )

        if long_signal['valid']:
            valid_signals += 1
            print(f"\n✅ Bar {i} - LONG signal found!")
            print(f"   Confirmations: {long_signal['confirmations']}")
            print(f"   Confidence: {long_signal['confidence']}")
            print(f"   Entry: ${long_signal['entry']:.2f}, Stop: ${long_signal['stop']:.2f}, Target: ${long_signal['target']:.2f}")
            print(f"   R:R: {long_signal['rr_ratio']:.2f}")

        if short_signal['valid']:
            valid_signals += 1
            print(f"\n✅ Bar {i} - SHORT signal found!")
            print(f"   Confirmations: {short_signal['confirmations']}")
            print(f"   Confidence: {short_signal['confidence']}")
            print(f"   Entry: ${short_signal['entry']:.2f}, Stop: ${short_signal['stop']:.2f}, Target: ${short_signal['target']:.2f}")
            print(f"   R:R: {short_signal['rr_ratio']:.2f}")

    # Summary
    print("\n" + "="*80)
    print(" SIGNAL GENERATION SUMMARY")
    print("="*80)
    print(f"\nTotal tests: {total_tests}")
    print(f"Valid signals found: {valid_signals}")
    print(f"Signal rate: {(valid_signals/total_tests)*100:.1f}%")

    if valid_signals == 0:
        print("\n❌ PROBLEM IDENTIFIED: Signal generator is NOT producing any valid signals!")
        print("\n📝 Possible causes:")
        print("   1. Confirmation patterns are too strict (not detecting patterns)")
        print("   2. Trend detection is rejecting all setups")
        print("   3. Risk/reward requirements are too high")
        print("   4. DataFrame format issues")
        print("\n🔧 Next steps:")
        print("   - Test individual confirmation patterns")
        print("   - Lower min_confirmations to 0")
        print("   - Test with synthetic data that should generate signals")
    else:
        print(f"\n✅ Signal generator IS working - found {valid_signals} valid signals")
        print("\n   But backtest had 0 trades - issue must be in:")
        print("   1. Position sizing calculation")
        print("   2. Risk manager rejecting trades")
        print("   3. Timing/execution issues")

    print("="*80)

    return valid_signals


if __name__ == "__main__":
    test_signal_generation()
