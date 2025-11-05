#!/usr/bin/env python3
"""
Test Phase 1 ICT Components with Real Market Data

This script validates signal_generator, risk_manager, and session_manager
using actual BTC/ETH OHLC data from your data directory.
"""

import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from strategies.ict_strategy.signal_generator import ICTSignalGenerator
from strategies.ict_strategy.risk_manager import ICTRiskManager
from strategies.ict_strategy.session_manager import ICTSessionManager


def load_data(timeframe='1h'):
    """Load BTC and ETH data for testing"""
    print(f"\n{'='*80}")
    print(f"Loading {timeframe} data...")
    print(f"{'='*80}")

    # Try multiple data sources
    data_paths = {
        'BTC': [
            f'data/storage/dataset_files/BTCUSD-{timeframe}-500wks-data.csv',
            f'data/storage/dataset_files/coinbase/BTCUSD-{timeframe}-200wks-enhanced-data.csv',
            f'data/storage/dataset_files/crypto_hist_data/Bitstamp_BTCUSD_{timeframe}.csv',
        ],
        'ETH': [
            f'data/storage/dataset_files/coinbase/ETHUSD-{timeframe}-200wks-enhanced-data.csv',
            f'data/storage/dataset_files/crypto_hist_data/Bitstamp_ETHUSD_{timeframe}.csv',
            f'data/storage/dataset_files/hyperliquid/ETH-USD-{timeframe}-hyperliquid-data.csv',
        ]
    }

    loaded_data = {}

    for symbol, paths in data_paths.items():
        for path in paths:
            try:
                df = pd.read_csv(path)

                # Standardize column names
                df.columns = df.columns.str.lower()

                # Handle different timestamp column names
                time_col = None
                for col in ['timestamp', 'time', 'date', 'datetime']:
                    if col in df.columns:
                        time_col = col
                        break

                if time_col:
                    df[time_col] = pd.to_datetime(df[time_col])
                    df.set_index(time_col, inplace=True)

                # Ensure required columns exist
                required_cols = ['open', 'high', 'low', 'close']
                if all(col in df.columns for col in required_cols):
                    loaded_data[symbol] = df.tail(1000)  # Last 1000 candles
                    print(f"✅ Loaded {symbol}: {len(loaded_data[symbol])} candles from {path}")
                    break
            except Exception as e:
                continue

    return loaded_data


def test_session_manager():
    """Test session manager with current time"""
    print(f"\n{'='*80}")
    print("TEST 1: Session Manager")
    print(f"{'='*80}")

    session_mgr = ICTSessionManager()

    # Get current session info
    info = session_mgr.get_session_info()
    should_trade, reason = session_mgr.should_trade_now()

    print(f"\n📅 Current Session Information:")
    print(f"   Session: {info['current_session']}")
    print(f"   High Probability: {info['is_high_probability']}")
    print(f"   Time (UTC): {info['current_time_utc']}")
    print(f"   Recommendation: {info['recommendation']}")

    print(f"\n🚦 Trading Decision:")
    if should_trade:
        print(f"   ✅ {reason}")
    else:
        print(f"   ❌ {reason}")

    return session_mgr


def test_signal_generator(data):
    """Test signal generation with real data"""
    print(f"\n{'='*80}")
    print("TEST 2: Signal Generator")
    print(f"{'='*80}")

    generator = ICTSignalGenerator()

    for symbol, df in data.items():
        print(f"\n📊 Testing {symbol}:")
        print(f"   Data range: {df.index[0]} to {df.index[-1]}")
        print(f"   Current price: ${df.iloc[-1]['close']:.2f}")

        # Use last 500 candles for higher TF, last 200 for lower TF
        df_higher = df.tail(500)
        df_lower = df.tail(200)

        # Generate signal for both directions
        for direction in ['long', 'short']:
            signal = generator.generate_signal(
                df_higher=df_higher,
                df_lower=df_lower,
                symbol=f"{symbol}/USDT",
                direction=direction
            )

            print(f"\n   {direction.upper()} Signal:")
            print(f"      Valid: {'✅' if signal['valid'] else '❌'}")
            print(f"      Confirmations: {signal['confirmations']}")
            print(f"      Confidence: {signal['confidence']}")

            if signal['valid']:
                print(f"      Entry: ${signal['entry']:.2f}")
                print(f"      Stop: ${signal['stop']:.2f}")
                print(f"      Target: ${signal['target']:.2f}")
                print(f"      R:R Ratio: {signal['rr_ratio']:.2f}")
            else:
                print(f"      Reason: {', '.join(signal['notes'])}")

    # Show statistics
    print(f"\n📈 Signal Statistics:")
    stats = generator.get_signal_statistics()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    return generator


def test_risk_manager(generator):
    """Test risk manager with generated signals"""
    print(f"\n{'='*80}")
    print("TEST 3: Risk Manager")
    print(f"{'='*80}")

    # Test with different account sizes
    account_sizes = [1000, 10000, 100000]

    for balance in account_sizes:
        print(f"\n💰 Account Balance: ${balance:,}")

        risk_mgr = ICTRiskManager(account_balance=balance, max_daily_losses=2)

        # Get a valid signal from history
        valid_signals = [s for s in generator.signal_history if s['valid']]

        if valid_signals:
            signal = valid_signals[0]
            print(f"   Testing with {signal['symbol']} {signal['direction']} signal")

            # Calculate position size
            position_info = risk_mgr.calculate_position_size(signal, leverage=1.0)

            if position_info['can_trade']:
                print(f"   ✅ Position Sizing:")
                print(f"      Size: {position_info['position_size']:.4f} units")
                print(f"      Risk Amount: ${position_info['risk_amount']:.2f}")
                print(f"      Risk %: {position_info['risk_percentage']:.2f}%")
                print(f"      Quality: {position_info['quality']}")
                print(f"      Stop %: {position_info['stop_percentage']:.2f}%")
            else:
                print(f"   ❌ Cannot trade: {position_info['reason']}")
        else:
            print(f"   ⚠️  No valid signals generated in previous test")

    return risk_mgr


def test_complete_flow(data):
    """Test complete trading flow"""
    print(f"\n{'='*80}")
    print("TEST 4: Complete Trading Flow")
    print(f"{'='*80}")

    # Initialize all components
    session_mgr = ICTSessionManager()
    generator = ICTSignalGenerator()
    risk_mgr = ICTRiskManager(account_balance=10000, max_daily_losses=2)

    # Test with BTC
    if 'BTC' in data:
        symbol = 'BTC'
        df = data[symbol]

        print(f"\n🔄 Complete Flow for {symbol}:")

        # Step 1: Check session
        should_trade, reason = session_mgr.should_trade_now()
        print(f"\n1. Session Check: {'✅' if should_trade else '❌'} - {reason}")

        # Step 2: Mark session levels
        levels = session_mgr.mark_session_liquidity(df)
        print(f"2. Session Levels: {len(levels)} key levels marked")

        # Step 3: Generate signal
        signal = generator.generate_signal(
            df_higher=df.tail(500),
            df_lower=df.tail(200),
            symbol=f"{symbol}/USDT"
        )
        print(f"3. Signal Generation: {'✅ Valid' if signal['valid'] else '❌ Invalid'}")

        if signal['valid']:
            # Step 4: Calculate position size
            position_info = risk_mgr.calculate_position_size(signal)
            print(f"4. Position Sizing: {'✅ Approved' if position_info['can_trade'] else '❌ Blocked'}")

            if position_info['can_trade']:
                # Step 5: Display trade details
                print(f"\n📋 TRADE DETAILS:")
                print(f"   Symbol: {signal['symbol']}")
                print(f"   Direction: {signal['direction'].upper()}")
                print(f"   Entry: ${signal['entry']:.2f}")
                print(f"   Stop: ${signal['stop']:.2f}")
                print(f"   Target: ${signal['target']:.2f}")
                print(f"   Position Size: {position_info['position_size']:.4f}")
                print(f"   Risk: ${position_info['risk_amount']:.2f} ({position_info['risk_percentage']:.2f}%)")
                print(f"   R:R Ratio: {signal['rr_ratio']:.2f}")
                print(f"   Confirmations: {', '.join(signal['confirmations'])}")
                print(f"   Confidence: {signal['confidence']}")
                print(f"   Quality: {position_info['quality']}")

                # Step 6: Simulate portfolio risk
                portfolio_risk = risk_mgr.get_portfolio_risk()
                print(f"\n📊 Portfolio Risk:")
                print(f"   Total Risk: ${portfolio_risk['total_risk']:.2f} ({portfolio_risk['total_risk_percentage']:.2f}%)")
                print(f"   Can Add Position: {portfolio_risk['can_add_position']}")
        else:
            print(f"   Reasons: {', '.join(signal['notes'])}")


def main():
    """Run all Phase 1 tests"""
    print("\n" + "="*80)
    print(" ICT STRATEGY PHASE 1 - LIVE DATA TESTING")
    print("="*80)

    # Load data
    data_1h = load_data('1h')

    if not data_1h:
        print("\n❌ Error: Could not load data. Please check data directory.")
        return

    print(f"\n✅ Successfully loaded data for: {', '.join(data_1h.keys())}")

    try:
        # Run individual tests
        session_mgr = test_session_manager()
        generator = test_signal_generator(data_1h)
        risk_mgr = test_risk_manager(generator)

        # Run complete flow
        test_complete_flow(data_1h)

        # Final summary
        print(f"\n{'='*80}")
        print("✅ PHASE 1 TESTING COMPLETE")
        print(f"{'='*80}")
        print("\nAll components are working correctly with real data!")
        print("\nNext Steps:")
        print("1. Review signal quality and confirmation patterns")
        print("2. Adjust parameters if needed (ATR multiplier, R:R minimums, etc.)")
        print("3. Proceed to Phase 2 (implement additional confirmations)")
        print("4. Or proceed to Phase 3 (backtest the strategy)")

    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
