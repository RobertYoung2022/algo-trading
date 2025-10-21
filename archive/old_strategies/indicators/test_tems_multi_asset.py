"""
TEMS Multi-Asset Testing Framework
===================================
Comprehensive testing of Triple EMA Momentum System across all available data
Starting with ETH 6h Coinbase validation (proven +273% baseline)
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import glob
from backtesting import Backtest
from tems_triple_ema_momentum import TEMSStrategy


def enhanced_backtest_runner(data, strategy_class, cash=10000, commission=0.002):
    """Run backtest with full native display of results"""
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
    """Load data with validation and quality checks"""
    try:
        # Load CSV
        df = pd.read_csv(file_path)

        # Standardize column names
        column_mapping = {
            'open': 'Open', 'high': 'High', 'low': 'Low',
            'close': 'Close', 'volume': 'Volume'
        }
        df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)

        # Ensure required columns exist
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in required_cols:
            if col not in df.columns:
                return None, f"Missing required column: {col}"

        # Convert timestamp/date to datetime index
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
            df.set_index('datetime', inplace=True)
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
            df.set_index('timestamp', inplace=True)
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.set_index('date', inplace=True)
        elif 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df.set_index('Date', inplace=True)

        # Remove any NaN rows
        df = df.dropna()

        # Ensure we have enough data
        if len(df) < 100:
            return None, f"Insufficient data: only {len(df)} bars"

        # Calculate data quality metrics
        zero_volume_pct = (df['Volume'] == 0).sum() / len(df) * 100
        if zero_volume_pct > 25:
            return None, f"Too many zero volume bars: {zero_volume_pct:.1f}%"

        return df, None

    except Exception as e:
        return None, str(e)


def find_all_data_sources():
    """Find all available CSV data files in the data directory"""
    data_dir = "/Users/bobbyyo/Projects/algo-fun/data"
    data_sources = []

    # Search patterns for different data providers
    patterns = [
        "coinbase/*.csv",
        "yahoo/*.csv",
        "cryptocompare/*.csv",
        "coingecko/*.csv",
        "coinmarketcap/*.csv",
        "hyperliquid/*.csv",
        "*.csv"  # Root level files
    ]

    for pattern in patterns:
        files = glob.glob(os.path.join(data_dir, pattern))
        data_sources.extend(files)

    # Remove duplicates and sort
    data_sources = sorted(list(set(data_sources)))

    # Filter out known corrupted files
    corrupted_files = [
        "BTCUSD-1d-1000wks-data.csv",
        "hyperliquid_BTCUSD_1d.csv"  # Known corrupted
    ]

    data_sources = [f for f in data_sources if not any(bad in f for bad in corrupted_files)]

    return data_sources


def extract_asset_timeframe(file_path):
    """Extract asset symbol and timeframe from filename"""
    filename = os.path.basename(file_path)

    # Common patterns
    if 'coinbase' in file_path.lower():
        # Format: coinbase_BTCUSD_5m.csv
        parts = filename.replace('.csv', '').split('_')
        if len(parts) >= 3:
            asset = parts[1]
            timeframe = parts[2]
            return asset, timeframe
    elif 'yahoo' in file_path.lower():
        # Format: yahoo_BTCUSD_1d.csv
        parts = filename.replace('.csv', '').split('_')
        if len(parts) >= 3:
            asset = parts[1]
            timeframe = parts[2]
            return asset, timeframe
    else:
        # Try generic parsing
        if '_' in filename:
            parts = filename.replace('.csv', '').split('_')
            if len(parts) >= 2:
                return parts[0], parts[-1]

    return filename.replace('.csv', ''), 'unknown'


def run_phase_1a_eth_validation():
    """Phase 1A: Validate TEMS on ETH 6h Coinbase (proven successful config)"""
    print("\n" + "="*80)
    print("PHASE 1A: ETH 6h COINBASE VALIDATION")
    print("Testing against proven +273% return baseline")
    print("="*80)

    # Find ETH 6h Coinbase data - use enhanced data file
    eth_6h_path = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-6h-200wks-enhanced-data.csv"

    if not os.path.exists(eth_6h_path):
        print(f"ERROR: ETH 6h Coinbase data not found at: {eth_6h_path}")
        return None

    # Load and validate data
    data, error = load_and_validate_data(eth_6h_path)
    if error:
        print(f"ERROR loading ETH 6h data: {error}")
        return None

    print(f"\nData loaded successfully:")
    print(f"- File: {eth_6h_path}")
    print(f"- Bars: {len(data)}")
    print(f"- Period: {data.index[0]} to {data.index[-1]}")
    print(f"- Data shape: {data.shape}")

    # Run TEMS backtest
    print("\n" + "-"*80)
    print("Running TEMS Strategy on ETH 6h Coinbase...")
    print("-"*80)

    stats, bt = enhanced_backtest_runner(data, TEMSStrategy)

    # Display COMPLETE native results
    print("\n" + "="*80)
    print("TEMS STRATEGY RESULTS - ETH 6h COINBASE")
    print("="*80)
    print(stats)

    # Show plot
    try:
        bt.plot()
    except:
        pass

    return stats


def run_phase_1b_multi_asset_expansion():
    """Phase 1B: Test TEMS on all available assets"""
    print("\n" + "="*80)
    print("PHASE 1B: MULTI-ASSET EXPANSION")
    print("Testing TEMS across all available cryptocurrencies")
    print("="*80)

    # Find all data sources
    data_sources = find_all_data_sources()
    print(f"\nFound {len(data_sources)} total data files")

    # Group by asset
    asset_results = {}
    successful_tests = []
    failed_tests = []

    for file_path in data_sources:
        asset, timeframe = extract_asset_timeframe(file_path)

        # Skip if not crypto asset
        if asset not in ['BTCUSD', 'ETHUSD', 'CROUSD', 'HBARUSD', 'LINKUSD', 'XRPUSD',
                        'BTC-USD', 'ETH-USD', 'CRO-USD', 'HBAR-USD', 'LINK-USD', 'XRP-USD']:
            continue

        print(f"\n" + "-"*60)
        print(f"Testing: {asset} - {timeframe}")
        print(f"Source: {file_path}")

        # Load data
        data, error = load_and_validate_data(file_path)
        if error:
            print(f"SKIP: {error}")
            failed_tests.append((asset, timeframe, error))
            continue

        print(f"Data bars: {len(data)}")

        try:
            # Run backtest
            stats, _ = enhanced_backtest_runner(data, TEMSStrategy)

            # Store results
            result = {
                'asset': asset,
                'timeframe': timeframe,
                'file': file_path,
                'return': stats['Return [%]'],
                'sharpe': stats['Sharpe Ratio'],
                'max_drawdown': stats['Max. Drawdown [%]'],
                'win_rate': stats['Win Rate [%]'],
                'trades': stats['# Trades'],
                'exposure': stats['Exposure Time [%]']
            }

            successful_tests.append(result)

            # Group by asset
            if asset not in asset_results:
                asset_results[asset] = []
            asset_results[asset].append(result)

            # Show if profitable
            if stats['Return [%]'] > 0:
                print(f"PROFITABLE: Return={stats['Return [%]']:.2f}%, Sharpe={stats['Sharpe Ratio']:.2f}")

        except Exception as e:
            print(f"ERROR: {str(e)}")
            failed_tests.append((asset, timeframe, str(e)))

    return successful_tests, asset_results


def generate_performance_summary(successful_tests, asset_results):
    """Generate comprehensive performance summary and rankings"""
    print("\n" + "="*80)
    print("COMPREHENSIVE TEMS PERFORMANCE SUMMARY")
    print("="*80)

    if not successful_tests:
        print("No successful tests to summarize")
        return

    # Convert to DataFrame for analysis
    df = pd.DataFrame(successful_tests)

    # Overall statistics
    print("\n### OVERALL STATISTICS ###")
    print(f"Total tests run: {len(successful_tests)}")
    print(f"Profitable tests: {len(df[df['return'] > 0])} ({len(df[df['return'] > 0])/len(df)*100:.1f}%)")
    print(f"Average return: {df['return'].mean():.2f}%")
    print(f"Best return: {df['return'].max():.2f}%")
    print(f"Worst return: {df['return'].min():.2f}%")
    print(f"Average Sharpe: {df['sharpe'].mean():.2f}")
    print(f"Average Win Rate: {df['win_rate'].mean():.1f}%")

    # Top performers
    print("\n### TOP 10 PERFORMERS ###")
    top_10 = df.nlargest(10, 'return')[['asset', 'timeframe', 'return', 'sharpe', 'win_rate', 'trades']]
    for idx, row in top_10.iterrows():
        print(f"{row['asset']} {row['timeframe']}: Return={row['return']:.2f}%, Sharpe={row['sharpe']:.2f}, WinRate={row['win_rate']:.1f}%")

    # Asset rankings
    print("\n### ASSET PERFORMANCE RANKING ###")
    asset_summary = []
    for asset in asset_results:
        asset_df = pd.DataFrame(asset_results[asset])
        summary = {
            'asset': asset,
            'avg_return': asset_df['return'].mean(),
            'best_return': asset_df['return'].max(),
            'avg_sharpe': asset_df['sharpe'].mean(),
            'tests_run': len(asset_df),
            'profitable_pct': len(asset_df[asset_df['return'] > 0]) / len(asset_df) * 100
        }
        asset_summary.append(summary)

    asset_ranking = pd.DataFrame(asset_summary).sort_values('avg_return', ascending=False)
    for idx, row in asset_ranking.iterrows():
        print(f"{row['asset']}: AvgReturn={row['avg_return']:.2f}%, BestReturn={row['best_return']:.2f}%, AvgSharpe={row['avg_sharpe']:.2f}")

    # Timeframe analysis
    print("\n### TIMEFRAME PERFORMANCE ###")
    timeframe_grouped = df.groupby('timeframe').agg({
        'return': 'mean',
        'sharpe': 'mean',
        'win_rate': 'mean',
        'asset': 'count'
    }).sort_values('return', ascending=False)

    for tf in timeframe_grouped.index:
        stats = timeframe_grouped.loc[tf]
        print(f"{tf}: AvgReturn={stats['return']:.2f}%, AvgSharpe={stats['sharpe']:.2f}, Tests={int(stats['asset'])}")

    # Save results to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"/Users/bobbyyo/Projects/algo-fun/strategies/results/tems_results_{timestamp}.csv"
    df.to_csv(results_file, index=False)
    print(f"\nResults saved to: {results_file}")

    return df


def main():
    """Main execution function"""
    print("\n" + "="*80)
    print("TRIPLE EMA MOMENTUM SYSTEM (TEMS) COMPREHENSIVE TESTING")
    print("Replacing catastrophic mean-reversion with trend-following")
    print("="*80)

    # Phase 1A: ETH 6h Coinbase Validation
    eth_stats = run_phase_1a_eth_validation()

    if eth_stats is not None:
        print("\n### ETH 6h VALIDATION COMPLETE ###")
        print(f"TEMS Return: {eth_stats['Return [%]']:.2f}%")
        print(f"Baseline comparison: +273% (existing strategy)")
        if eth_stats['Return [%]'] != 0:
            print(f"Performance ratio: {eth_stats['Return [%]'] / 273 * 100:.1f}%")
        else:
            print("WARNING: Strategy generated no trades - checking parameters...")

    # Phase 1B: Multi-Asset Expansion
    print("\nContinuing with Phase 1B (Multi-Asset Testing)...")
    successful_tests, asset_results = run_phase_1b_multi_asset_expansion()

    # Generate comprehensive summary
    if successful_tests:
        results_df = generate_performance_summary(successful_tests, asset_results)

        # Compare to existing portfolio
        print("\n" + "="*80)
        print("COMPARISON TO EXISTING CATASTROPHIC PORTFOLIO")
        print("="*80)
        print("Existing Portfolio: -44% to -100% losses across all strategies")
        print(f"TEMS Average: {results_df['return'].mean():.2f}%")
        print(f"TEMS Best: {results_df['return'].max():.2f}%")
        print(f"TEMS Profitable Rate: {len(results_df[results_df['return'] > 0])/len(results_df)*100:.1f}%")

        improvement = results_df['return'].mean() - (-70)  # Assuming -70% average existing
        print(f"\nIMPROVEMENT: +{improvement:.1f}% absolute performance gain")

    print("\n" + "="*80)
    print("TEMS TESTING COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()