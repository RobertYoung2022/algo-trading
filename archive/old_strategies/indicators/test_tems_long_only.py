"""
TEMS Long-Only Multi-Asset Comprehensive Testing
=================================================
Tests the long-only version of TEMS across all available crypto assets
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import glob
from backtesting import Backtest
from tems_long_only_strategy import TEMSLongOnlyStrategy


def enhanced_backtest_runner(data, strategy_class, cash=10000, commission=0.002):
    """Run backtest with full native display"""
    bt = Backtest(
        data,
        strategy_class,
        cash=cash,
        commission=commission,
        exclusive_orders=True
    )
    stats = bt.run()
    return stats, bt


def load_and_validate_data(file_path):
    """Load and validate CSV data"""
    try:
        df = pd.read_csv(file_path)

        # Standardize columns
        column_mapping = {
            'open': 'Open', 'high': 'High', 'low': 'Low',
            'close': 'Close', 'volume': 'Volume'
        }
        df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)

        # Check required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in required_cols:
            if col not in df.columns:
                return None, f"Missing column: {col}"

        # Handle datetime
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
            df.set_index('datetime', inplace=True)
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
            df.set_index('timestamp', inplace=True)
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.set_index('date', inplace=True)

        df = df.dropna()

        if len(df) < 100:
            return None, f"Insufficient data: {len(df)} bars"

        return df, None

    except Exception as e:
        return None, str(e)


def find_all_crypto_data():
    """Find all crypto data files"""
    data_dir = "/Users/bobbyyo/Projects/algo-fun/data"
    all_files = []

    # Search patterns
    patterns = [
        "coinbase/*.csv",
        "yahoo/*.csv",
        "cryptocompare/*.csv",
        "coingecko/*.csv"
    ]

    for pattern in patterns:
        files = glob.glob(os.path.join(data_dir, pattern))
        all_files.extend(files)

    # Filter for crypto assets only
    crypto_keywords = ['BTC', 'ETH', 'CRO', 'HBAR', 'LINK', 'XRP']
    crypto_files = []
    for f in all_files:
        if any(keyword in f.upper() for keyword in crypto_keywords):
            crypto_files.append(f)

    # Remove known corrupted files
    corrupted = ["BTCUSD-1d-1000wks-data.csv", "hyperliquid_BTCUSD_1d.csv"]
    crypto_files = [f for f in crypto_files if not any(bad in f for bad in corrupted)]

    return sorted(crypto_files)


def extract_info(file_path):
    """Extract asset and timeframe from filename"""
    filename = os.path.basename(file_path)

    # Extract asset
    for asset in ['BTCUSD', 'ETHUSD', 'CROUSD', 'HBARUSD', 'LINKUSD', 'XRPUSD']:
        if asset in filename.upper():
            asset_name = asset.replace('USD', '')
            break
    else:
        asset_name = 'UNKNOWN'

    # Extract timeframe
    timeframes = ['1m', '5m', '15m', '1h', '6h', '1d', '1w']
    timeframe = 'unknown'
    for tf in timeframes:
        if tf in filename.lower():
            timeframe = tf
            break

    return asset_name, timeframe


def test_eth_6h_baseline():
    """Test on ETH 6h - our baseline"""
    print("\n" + "="*80)
    print("PHASE 1A: ETH 6h COINBASE BASELINE TEST")
    print("Testing Long-Only TEMS against +273% benchmark")
    print("="*80)

    eth_file = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-6h-200wks-enhanced-data.csv"

    data, error = load_and_validate_data(eth_file)
    if error:
        print(f"Error: {error}")
        return None

    print(f"\nData loaded: {len(data)} bars")
    print(f"Period: {data.index[0]} to {data.index[-1]}")

    stats, bt = enhanced_backtest_runner(data, TEMSLongOnlyStrategy)

    print("\n" + "="*80)
    print("TEMS LONG-ONLY RESULTS - ETH 6h")
    print("="*80)
    print(stats)

    # Show plot
    try:
        bt.plot()
    except:
        pass

    return stats


def test_all_crypto_assets():
    """Test on all available crypto assets"""
    print("\n" + "="*80)
    print("PHASE 1B: COMPREHENSIVE MULTI-ASSET TESTING")
    print("Testing Long-Only TEMS across all cryptocurrencies")
    print("="*80)

    crypto_files = find_all_crypto_data()
    print(f"\nFound {len(crypto_files)} crypto data files")

    results = []
    asset_performance = {}

    for file_path in crypto_files:
        asset, timeframe = extract_info(file_path)

        print(f"\n" + "-"*60)
        print(f"Testing: {asset} - {timeframe}")
        print(f"File: {os.path.basename(file_path)}")

        data, error = load_and_validate_data(file_path)
        if error:
            print(f"Skip: {error}")
            continue

        print(f"Bars: {len(data)}")

        try:
            stats, _ = enhanced_backtest_runner(data, TEMSLongOnlyStrategy)

            result = {
                'Asset': asset,
                'Timeframe': timeframe,
                'Return %': stats['Return [%]'],
                'Sharpe': stats['Sharpe Ratio'],
                'Max DD %': stats['Max. Drawdown [%]'],
                'Win Rate %': stats['Win Rate [%]'],
                'Trades': stats['# Trades'],
                'File': os.path.basename(file_path)
            }

            results.append(result)

            # Group by asset
            if asset not in asset_performance:
                asset_performance[asset] = []
            asset_performance[asset].append(result)

            # Quick summary
            if stats['Return [%]'] > 0:
                print(f"✅ PROFITABLE: {stats['Return [%]']:.2f}% return, Sharpe: {stats['Sharpe Ratio']:.2f}")
            else:
                print(f"❌ Loss: {stats['Return [%]']:.2f}%")

        except Exception as e:
            print(f"Error: {str(e)[:100]}")

    return results, asset_performance


def generate_final_report(results, asset_performance):
    """Generate comprehensive performance report"""
    print("\n" + "="*80)
    print("TEMS LONG-ONLY STRATEGY - FINAL PERFORMANCE REPORT")
    print("="*80)

    if not results:
        print("No results to report")
        return

    df = pd.DataFrame(results)

    # Overall Stats
    print("\n### OVERALL PERFORMANCE ###")
    print(f"Total tests: {len(df)}")
    print(f"Profitable: {len(df[df['Return %'] > 0])} ({len(df[df['Return %'] > 0])/len(df)*100:.1f}%)")
    print(f"Average return: {df['Return %'].mean():.2f}%")
    print(f"Best return: {df['Return %'].max():.2f}%")
    print(f"Worst return: {df['Return %'].min():.2f}%")
    print(f"Average Sharpe: {df['Sharpe'].mean():.2f}")

    # Top 10 Performers
    print("\n### TOP 10 PERFORMERS ###")
    top_10 = df.nlargest(10, 'Return %')
    for idx, row in top_10.iterrows():
        print(f"{row['Asset']} {row['Timeframe']}: {row['Return %']:.2f}% (Sharpe: {row['Sharpe']:.2f}, Trades: {row['Trades']:.0f})")

    # Asset Rankings
    print("\n### ASSET RANKINGS (Average Performance) ###")
    asset_avg = []
    for asset in asset_performance:
        asset_df = pd.DataFrame(asset_performance[asset])
        avg_return = asset_df['Return %'].mean()
        best_return = asset_df['Return %'].max()
        asset_avg.append({
            'Asset': asset,
            'Avg Return': avg_return,
            'Best Return': best_return,
            'Tests': len(asset_df)
        })

    asset_ranking = pd.DataFrame(asset_avg).sort_values('Avg Return', ascending=False)
    for idx, row in asset_ranking.iterrows():
        print(f"{row['Asset']}: Avg={row['Avg Return']:.2f}%, Best={row['Best Return']:.2f}% ({row['Tests']:.0f} tests)")

    # Timeframe Analysis
    print("\n### TIMEFRAME ANALYSIS ###")
    tf_grouped = df.groupby('Timeframe')['Return %'].agg(['mean', 'max', 'count'])
    tf_grouped = tf_grouped.sort_values('mean', ascending=False)
    for tf in tf_grouped.index:
        stats = tf_grouped.loc[tf]
        print(f"{tf}: Avg={stats['mean']:.2f}%, Best={stats['max']:.2f}% ({stats['count']:.0f} tests)")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"/Users/bobbyyo/Projects/algo-fun/strategies/results/tems_long_only_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    print(f"\n✅ Results saved to: {output_file}")

    # Comparison to catastrophic portfolio
    print("\n" + "="*80)
    print("COMPARISON TO EXISTING CATASTROPHIC PORTFOLIO")
    print("="*80)
    print("Existing strategies: -44% to -100% losses")
    print(f"TEMS Long-Only Avg: {df['Return %'].mean():.2f}%")
    print(f"Improvement: {df['Return %'].mean() - (-70):.1f}% absolute gain")
    print("="*80)

    return df


def main():
    """Main execution"""
    print("\n" + "="*80)
    print("TEMS LONG-ONLY STRATEGY - COMPREHENSIVE TESTING")
    print("Trend-following approach for crypto markets")
    print("="*80)

    # Phase 1A: ETH baseline
    eth_stats = test_eth_6h_baseline()

    if eth_stats is not None:
        print(f"\n✅ ETH 6h Result: {eth_stats['Return [%]']:.2f}%")
        print(f"Benchmark: +273%")

    # Phase 1B: All assets
    print("\nContinuing with comprehensive multi-asset testing...")
    results, asset_performance = test_all_crypto_assets()

    # Final report
    if results:
        results_df = generate_final_report(results, asset_performance)


if __name__ == "__main__":
    main()