"""
🚀 Simplified Crypto Momentum Testing - Focus on Lower-Priced Assets 🚀
========================================================================
Tests the Crypto Momentum Strategy on assets with reasonable prices for $10k capital.

Author: Bobby's Algo Trading Systems 🌙
Date: 2025-01-18
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
from backtesting import Backtest
from crypto_momentum_surge_strategy import CryptoMomentumSurgeStrategy

# Add parent directory to path for imports
import sys
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import enhanced_backtest_runner, create_data_source_info

def test_selected_assets():
    """
    🎯 Test momentum strategy on selected crypto assets 🎯
    """
    print("\n" + "="*100)
    print("🚀 CRYPTO MOMENTUM STRATEGY - SELECTED ASSET TESTING")
    print("="*100)

    # Selected files that should work with $10k capital
    test_files = [
        # XRP - Lower priced assets
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/XRPUSD-1d-500wks-enhanced-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/XRPUSD-1h-100wks-enhanced-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/hyperliquid/XRP-USD-1h-hyperliquid-data.csv',

        # CRO - Lower priced
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/CROUSD-1d-1000wks-enhanced-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/CROUSD-6h-200wks-enhanced-data.csv',

        # HBAR - Lower priced
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/HBARUSD-1d-1000wks-enhanced-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/HBARUSD-6h-200wks-enhanced-data.csv',

        # LINK - Mid-range price
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/LINKUSD-6h-200wks-enhanced-data.csv',
    ]

    results = []
    successful_tests = 0

    for file_path in test_files:
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            continue

        filename = os.path.basename(file_path)
        print(f"\n📊 Testing: {filename}")

        try:
            # Load data
            df = pd.read_csv(file_path)

            # Handle datetime column
            if 'datetime' in df.columns:
                df['Date'] = pd.to_datetime(df['datetime'])
                df.set_index('Date', inplace=True)
            elif 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index('Date', inplace=True)

            # Rename columns to standard format
            df.columns = [col.capitalize() for col in df.columns]

            # Ensure required columns
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in required_cols:
                if col not in df.columns:
                    if col == 'Volume':
                        df['Volume'] = 1000000
                    else:
                        print(f"⚠️ Missing {col} column")
                        continue

            # Clean data
            df = df.dropna()

            # Skip if too few bars
            if len(df) < 100:
                print(f"⚠️ Insufficient data: {len(df)} bars")
                continue

            print(f"✅ Data loaded: {len(df)} bars")

            # Extract metadata
            parts = filename.split('-')
            symbol = parts[0].replace('USD', '') if 'USD' in parts[0] else parts[0]
            timeframe = parts[1] if len(parts) > 1 else 'unknown'

            # Create data source info
            data_source_info = create_data_source_info(
                file_path,
                symbol=symbol,
                timeframe=timeframe
            )

            # Run backtest with native results display
            summary_stats, full_stats = enhanced_backtest_runner(
                df,
                CryptoMomentumSurgeStrategy,
                data_source_info,
                'CryptoMomentumSurgeStrategy',
                cash=10000,
                commission=0.002
            )

            # Store results
            summary_stats['Bars'] = len(df)
            summary_stats['File'] = filename
            results.append(summary_stats)
            successful_tests += 1

        except Exception as e:
            print(f"❌ Error testing {filename}: {e}")
            continue

    # Create results DataFrame
    if results:
        results_df = pd.DataFrame(results)

        print("\n" + "="*100)
        print("📊 SUMMARY RESULTS")
        print("="*100)

        # Sort by Sharpe ratio
        results_df = results_df.sort_values('Sharpe', ascending=False)

        # Display summary table
        summary_cols = ['Symbol', 'Timeframe', 'Return_%', 'Sharpe', 'Win_Rate_%', 'Max_DD_%', 'Trades']
        print("\n🏆 PERFORMANCE RANKING:")
        print(results_df[summary_cols].to_string())

        # Asset performance summary
        print("\n📊 ASSET PERFORMANCE SUMMARY:")
        asset_summary = results_df.groupby('Symbol').agg({
            'Return_%': 'mean',
            'Sharpe': 'mean',
            'Win_Rate_%': 'mean',
            'Max_DD_%': 'mean',
            'Trades': 'sum'
        }).round(2)
        print(asset_summary)

        # Best performing configuration
        best_config = results_df.iloc[0]
        print(f"\n🏆 BEST CONFIGURATION:")
        print(f"Asset: {best_config['Symbol']}")
        print(f"Timeframe: {best_config['Timeframe']}")
        print(f"Return: {best_config['Return_%']:.2f}%")
        print(f"Sharpe Ratio: {best_config['Sharpe']:.2f}")
        print(f"Win Rate: {best_config['Win_Rate_%']:.2f}%")

        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_path = f'/Users/bobbyyo/Projects/algo-fun/strategies/results/crypto_momentum_simplified_{timestamp}.csv'
        os.makedirs(os.path.dirname(results_path), exist_ok=True)
        results_df.to_csv(results_path, index=False)
        print(f"\n💾 Results saved to: {results_path}")

    else:
        print("\n❌ No successful tests completed!")

    print(f"\n✅ Testing complete! {successful_tests}/{len(test_files)} tests successful")

    return results


def optimize_parameters():
    """
    🔧 Parameter optimization for the momentum strategy 🔧
    """
    print("\n" + "="*100)
    print("🔧 PARAMETER OPTIMIZATION")
    print("="*100)

    # Load a good test file
    test_file = '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/XRPUSD-1d-500wks-enhanced-data.csv'

    if not os.path.exists(test_file):
        print("❌ Test file not found for optimization")
        return

    # Load data
    df = pd.read_csv(test_file)
    if 'datetime' in df.columns:
        df['Date'] = pd.to_datetime(df['datetime'])
        df.set_index('Date', inplace=True)
    df.columns = [col.capitalize() for col in df.columns]
    if 'Volume' not in df.columns:
        df['Volume'] = 1000000
    df = df.dropna()

    print(f"📊 Optimizing on: {os.path.basename(test_file)}")
    print(f"Data points: {len(df)}")

    # Parameter ranges to test
    param_ranges = {
        'roc_surge_threshold': [2.0, 3.0, 4.0, 5.0],
        'volume_spike_multiplier': [1.5, 1.8, 2.0, 2.5],
        'stop_loss_pct': [0.01, 0.02, 0.03],
        'take_profit_pct': [0.04, 0.06, 0.08]
    }

    print("\n📊 Testing parameter combinations...")

    best_sharpe = -999
    best_params = {}

    # Test different combinations (simplified grid search)
    for roc_thresh in param_ranges['roc_surge_threshold']:
        for vol_mult in param_ranges['volume_spike_multiplier']:
            for sl in param_ranges['stop_loss_pct']:
                for tp in param_ranges['take_profit_pct']:
                    # Set parameters
                    CryptoMomentumSurgeStrategy.roc_surge_threshold = roc_thresh
                    CryptoMomentumSurgeStrategy.volume_spike_multiplier = vol_mult
                    CryptoMomentumSurgeStrategy.stop_loss_pct = sl
                    CryptoMomentumSurgeStrategy.take_profit_pct = tp

                    try:
                        # Run backtest
                        bt = Backtest(
                            df,
                            CryptoMomentumSurgeStrategy,
                            cash=10000,
                            commission=0.002
                        )
                        stats = bt.run()

                        # Check if better
                        if stats['Sharpe Ratio'] > best_sharpe:
                            best_sharpe = stats['Sharpe Ratio']
                            best_params = {
                                'roc_surge_threshold': roc_thresh,
                                'volume_spike_multiplier': vol_mult,
                                'stop_loss_pct': sl,
                                'take_profit_pct': tp,
                                'sharpe': stats['Sharpe Ratio'],
                                'return': stats['Return [%]'],
                                'trades': stats['# Trades']
                            }
                            print(f"🎯 New best Sharpe: {best_sharpe:.3f} with ROC={roc_thresh}, Vol={vol_mult}, SL={sl:.1%}, TP={tp:.1%}")

                    except:
                        continue

    if best_params:
        print("\n🏆 OPTIMAL PARAMETERS FOUND:")
        print(f"ROC Surge Threshold: {best_params['roc_surge_threshold']}")
        print(f"Volume Spike Multiplier: {best_params['volume_spike_multiplier']}")
        print(f"Stop Loss: {best_params['stop_loss_pct']:.1%}")
        print(f"Take Profit: {best_params['take_profit_pct']:.1%}")
        print(f"Sharpe Ratio: {best_params['sharpe']:.3f}")
        print(f"Return: {best_params['return']:.2f}%")
        print(f"Trades: {best_params['trades']}")

    return best_params


if __name__ == "__main__":
    print("\n" + "="*100)
    print("🚀 CRYPTO MOMENTUM STRATEGY - COMPREHENSIVE ANALYSIS")
    print("="*100)

    # Run tests on selected assets
    print("\n1️⃣ Testing on selected crypto assets...")
    test_results = test_selected_assets()

    # Run parameter optimization
    print("\n2️⃣ Running parameter optimization...")
    optimal_params = optimize_parameters()

    print("\n" + "="*100)
    print("🎉 ALL ANALYSIS COMPLETE!")
    print("="*100)

# 🌙💫🚀 Bobby's signature emoji style preserved throughout 🌙💫🚀