"""
Test ATSS Optimized Strategy on Priority Assets
===============================================

Quick test of the optimized ATSS strategy on high-priority assets
and timeframes for validation.

Author: Bobby's Algo Trading System
Date: 2025-01-17
"""

import pandas as pd
import numpy as np
import os
from backtesting import Backtest
from atss_adx_optimized import ATSSOptimizedStrategy
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
                return df
            except:
                continue
        return None
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None


def test_strategy(data, asset, timeframe, source):
    """Test ATSS optimized strategy on given data"""
    try:
        bt = Backtest(
            data,
            ATSSOptimizedStrategy,
            cash=10000,
            commission=0.001,
            exclusive_orders=True,
            hedging=False,
            trade_on_close=False
        )

        stats = bt.run()

        print(f"\n{'=' * 80}")
        print(f"RESULTS FOR {asset} {timeframe} ({source})")
        print('=' * 80)
        print(stats)
        print('=' * 80)

        # Return key metrics
        return {
            'asset': asset,
            'timeframe': timeframe,
            'source': source,
            'return': stats['Return [%]'],
            'sharpe': stats.get('Sharpe Ratio', 0),
            'win_rate': stats.get('Win Rate [%]', 0),
            'num_trades': stats.get('# Trades', 0),
            'max_drawdown': stats.get('Max. Drawdown [%]', 0),
            'profit_factor': stats.get('Profit Factor', 0)
        }

    except Exception as e:
        print(f"Error testing {asset} {timeframe}: {e}")
        return None


def main():
    """Main test execution"""

    base_path = "/Users/bobbyyo/Projects/algo-fun/data"

    # Priority test configurations
    test_configs = [
        # ETH - Primary focus
        ('ETHUSD', '1d', 'coinbase', 'coinbase/ETHUSD-1d-1000wks-enhanced-data.csv'),
        ('ETHUSD', '6h', 'coinbase', 'coinbase/ETHUSD-6h-200wks-enhanced-data.csv'),
        ('ETHUSD', '5m', 'coinbase', 'coinbase/ETHUSD-5m-50wks-enhanced-data.csv'),

        # BTC - Secondary
        ('BTCUSD', '1d', 'coinbase', 'coinbase/BTCUSD-1d-1000wks-enhanced-data.csv'),
        ('BTCUSD', '6h', 'coinbase', 'coinbase/BTCUSD-6h-200wks-enhanced-data.csv'),

        # HBAR - Strong performer
        ('HBARUSD', '6h', 'coinbase', 'coinbase/HBARUSD-6h-200wks-enhanced-data.csv'),
        ('HBARUSD', '1d', 'coinbase', 'coinbase/HBARUSD-1d-1000wks-enhanced-data.csv'),

        # LINK
        ('LINKUSD', '6h', 'coinbase', 'coinbase/LINKUSD-6h-200wks-enhanced-data.csv'),
        ('LINKUSD', '1d', 'coinbase', 'coinbase/LINKUSD-1d-1000wks-enhanced-data.csv'),
    ]

    print("=" * 80)
    print("ATSS OPTIMIZED STRATEGY - PRIORITY ASSET TESTING")
    print("=" * 80)
    print(f"Testing {len(test_configs)} priority configurations")
    print("=" * 80)

    results = []

    for asset, timeframe, source, filepath in test_configs:
        full_path = os.path.join(base_path, filepath)

        print(f"\nTesting {asset} {timeframe} from {source}...")
        print(f"File: {full_path}")

        data = load_data(full_path)
        if data is None:
            print(f"Failed to load data for {asset} {timeframe}")
            continue

        print(f"Data loaded: {len(data)} bars")

        result = test_strategy(data, asset, timeframe, source)
        if result:
            results.append(result)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY OF RESULTS")
    print("=" * 80)

    if results:
        df_results = pd.DataFrame(results)
        df_results = df_results.sort_values('return', ascending=False)

        print("\nTop Performers by Return:")
        for _, row in df_results.head(5).iterrows():
            print(f"{row['asset']}-{row['timeframe']}: Return={row['return']:.2f}%, "
                  f"Sharpe={row['sharpe']:.2f}, WR={row['win_rate']:.1f}%, "
                  f"Trades={row['num_trades']}")

        print(f"\nAverage Return: {df_results['return'].mean():.2f}%")
        print(f"Best Return: {df_results['return'].max():.2f}%")
        print(f"Average Sharpe: {df_results['sharpe'].mean():.2f}")
        print(f"Total Trades: {df_results['num_trades'].sum()}")

        # Save results
        results_dir = "/Users/bobbyyo/Projects/algo-fun/strategies/results"
        os.makedirs(results_dir, exist_ok=True)
        results_file = os.path.join(results_dir, 'atss_optimized_results.csv')
        df_results.to_csv(results_file, index=False)
        print(f"\nResults saved to: {results_file}")

    print("\n" + "=" * 80)
    print("TESTING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()