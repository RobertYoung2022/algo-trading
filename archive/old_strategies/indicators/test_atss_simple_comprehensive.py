"""
Comprehensive Test of Simplified ATSS Strategy
==============================================

Test the simplified ATSS strategy across all available assets
and timeframes to validate performance.

Author: Bobby's Algo Trading System
Date: 2025-01-17
"""

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from backtesting import Backtest
from atss_simple import ATSSSimpleStrategy
import warnings
warnings.filterwarnings('ignore')


def load_data(filepath):
    """Load and prepare data for backtesting"""
    try:
        # Try multiple date column names
        for date_col in ['Date', 'date', 'datetime', 'Datetime']:
            try:
                df = pd.read_csv(filepath, parse_dates=[date_col])
                # Standardize columns
                df.columns = [col.capitalize() for col in df.columns]
                # Rename datetime to Date if needed
                if 'Datetime' in df.columns:
                    df.rename(columns={'Datetime': 'Date'}, inplace=True)
                df.set_index('Date', inplace=True)
                df.sort_index(inplace=True)

                # Validate required columns
                required = ['Open', 'High', 'Low', 'Close', 'Volume']
                if all(col in df.columns for col in required):
                    return df
            except:
                continue
        return None
    except:
        return None


def discover_all_data():
    """Discover all available data files"""
    base_path = "/Users/bobbyyo/Projects/algo-fun/data"
    all_files = []

    # Search patterns for different sources
    patterns = [
        'coinbase/*.csv',
        'yahoo/*.csv',
        'coingecko/*.csv',
        'cryptocompare/*.csv'
    ]

    for pattern in patterns:
        search_path = os.path.join(base_path, pattern)
        files = glob.glob(search_path)
        all_files.extend(files)

    return all_files


def parse_asset_info(filepath):
    """Extract asset, timeframe, and source from filepath"""
    filename = os.path.basename(filepath)
    source = os.path.basename(os.path.dirname(filepath))

    # Parse asset name
    asset = filename.split('-')[0].upper() if '-' in filename else filename.split('.')[0].upper()
    asset = asset.replace('USD', '') if asset.endswith('USD') else asset

    # Parse timeframe
    timeframe = None
    timeframe_patterns = ['1m', '5m', '15m', '1h', '4h', '6h', '1d', 'daily']
    for tf in timeframe_patterns:
        if tf in filename.lower():
            timeframe = tf if tf != 'daily' else '1d'
            break

    return asset, timeframe, source


def test_strategy(filepath):
    """Test ATSS strategy on a single data file"""
    try:
        # Load data
        data = load_data(filepath)
        if data is None or len(data) < 100:
            return None

        # Parse asset info
        asset, timeframe, source = parse_asset_info(filepath)
        if not asset or not timeframe:
            return None

        # Run backtest
        bt = Backtest(
            data,
            ATSSSimpleStrategy,
            cash=10000,
            commission=0.001,
            exclusive_orders=True,
            hedging=False,
            trade_on_close=False
        )

        stats = bt.run()

        # Extract key metrics
        return {
            'asset': asset,
            'timeframe': timeframe,
            'source': source,
            'filepath': filepath,
            'data_points': len(data),
            'return': stats['Return [%]'],
            'buy_hold': stats['Buy & Hold Return [%]'],
            'sharpe': stats.get('Sharpe Ratio', 0),
            'sortino': stats.get('Sortino Ratio', 0),
            'calmar': stats.get('Calmar Ratio', 0),
            'win_rate': stats.get('Win Rate [%]', 0),
            'num_trades': stats.get('# Trades', 0),
            'max_drawdown': stats.get('Max. Drawdown [%]', 0),
            'avg_drawdown': stats.get('Avg. Drawdown [%]', 0),
            'profit_factor': stats.get('Profit Factor', 0),
            'expectancy': stats.get('Expectancy [%]', 0),
            'exposure_time': stats.get('Exposure Time [%]', 0),
            'avg_trade': stats.get('Avg. Trade [%]', 0),
            'best_trade': stats.get('Best Trade [%]', 0),
            'worst_trade': stats.get('Worst Trade [%]', 0),
            'stats': stats  # Store full stats
        }

    except Exception as e:
        return None


def main():
    """Main execution function"""

    print("=" * 80)
    print("ATSS SIMPLIFIED - COMPREHENSIVE MULTI-ASSET TESTING")
    print("=" * 80)

    # Discover all data files
    all_files = discover_all_data()
    print(f"Found {len(all_files)} data files")

    # Priority assets and timeframes
    priority_assets = ['ETH', 'BTC', 'HBAR', 'LINK', 'CRO', 'XRP']
    priority_timeframes = ['1d', '6h', '4h', '1h']

    # Test all files
    results = []
    tested = 0
    successful = 0

    for filepath in sorted(all_files):
        tested += 1
        asset, timeframe, source = parse_asset_info(filepath)

        print(f"\n[{tested}/{len(all_files)}] Testing {asset} {timeframe} from {source}")
        print(f"  File: {filepath}")

        result = test_strategy(filepath)
        if result and result['num_trades'] > 0:
            results.append(result)
            successful += 1
            print(f"  ✓ Return: {result['return']:.2f}%, Trades: {result['num_trades']}, "
                  f"Sharpe: {result['sharpe']:.2f}, Win Rate: {result['win_rate']:.1f}%")
        elif result:
            print(f"  ✗ No trades generated")
        else:
            print(f"  ✗ Failed to test (insufficient data or error)")

    print("\n" + "=" * 80)
    print(f"TESTING COMPLETE: {successful}/{tested} successful with trades")
    print("=" * 80)

    if results:
        df = pd.DataFrame(results)

        # Overall summary
        print("\n1. OVERALL PERFORMANCE SUMMARY")
        print("-" * 40)
        print(f"Configurations with trades: {len(df)}")
        print(f"Profitable strategies: {len(df[df['return'] > 0])} ({len(df[df['return'] > 0])/len(df)*100:.1f}%)")
        print(f"Average Return: {df['return'].mean():.2f}%")
        print(f"Best Return: {df['return'].max():.2f}%")
        print(f"Average Sharpe: {df['sharpe'].mean():.2f}")
        print(f"Average Win Rate: {df['win_rate'].mean():.1f}%")
        print(f"Total Trades: {df['num_trades'].sum()}")

        # Top performers
        print("\n2. TOP 10 PERFORMERS BY RETURN")
        print("-" * 40)
        top_10 = df.nlargest(10, 'return')
        for _, row in top_10.iterrows():
            print(f"{row['asset']}-{row['timeframe']} ({row['source']}): "
                  f"Return={row['return']:.2f}%, Sharpe={row['sharpe']:.2f}, "
                  f"Trades={row['num_trades']}, WR={row['win_rate']:.1f}%")

        # Asset performance
        print("\n3. ASSET PERFORMANCE RANKING")
        print("-" * 40)
        asset_perf = df.groupby('asset').agg({
            'return': 'mean',
            'sharpe': 'mean',
            'win_rate': 'mean',
            'num_trades': 'sum'
        }).round(2).sort_values('return', ascending=False)

        for asset in asset_perf.index[:10]:
            stats = asset_perf.loc[asset]
            print(f"{asset}: Avg Return={stats['return']:.2f}%, "
                  f"Avg Sharpe={stats['sharpe']:.2f}, "
                  f"Avg WR={stats['win_rate']:.1f}%, "
                  f"Total Trades={stats['num_trades']:.0f}")

        # Timeframe analysis
        print("\n4. TIMEFRAME PERFORMANCE")
        print("-" * 40)
        tf_perf = df.groupby('timeframe').agg({
            'return': 'mean',
            'sharpe': 'mean',
            'win_rate': 'mean',
            'num_trades': 'mean'
        }).round(2).sort_values('return', ascending=False)

        for tf in tf_perf.index:
            stats = tf_perf.loc[tf]
            print(f"{tf}: Avg Return={stats['return']:.2f}%, "
                  f"Avg Sharpe={stats['sharpe']:.2f}, "
                  f"Avg WR={stats['win_rate']:.1f}%, "
                  f"Avg Trades={stats['num_trades']:.1f}")

        # Display top performer's full stats
        if not df.empty:
            best = df.nlargest(1, 'return').iloc[0]
            print("\n" + "=" * 80)
            print(f"BEST PERFORMER - COMPLETE STATS")
            print(f"{best['asset']} {best['timeframe']} ({best['source']})")
            print("=" * 80)
            print(best['stats'])

        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_dir = "/Users/bobbyyo/Projects/algo-fun/strategies/results"
        os.makedirs(results_dir, exist_ok=True)

        # Summary file
        summary_file = os.path.join(results_dir, f'atss_simple_summary_{timestamp}.csv')
        df.drop('stats', axis=1).to_csv(summary_file, index=False)
        print(f"\nResults saved to: {summary_file}")

        # Detailed file for priority assets
        priority_df = df[df['asset'].isin(priority_assets)]
        if not priority_df.empty:
            priority_file = os.path.join(results_dir, f'atss_simple_priority_{timestamp}.csv')
            priority_df.drop('stats', axis=1).to_csv(priority_file, index=False)
            print(f"Priority assets saved to: {priority_file}")

    print("\n" + "=" * 80)
    print("ATSS SIMPLIFIED TESTING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()