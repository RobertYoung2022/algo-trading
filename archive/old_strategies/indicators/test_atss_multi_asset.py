"""
ATSS Multi-Asset Testing Framework
===================================

Comprehensive testing framework for ADX Trend Strength System (ATSS) strategy
across multiple cryptocurrencies and timeframes with complete performance analysis.

Features:
- Automatic discovery of all available crypto data
- Multi-timeframe testing (1m, 5m, 15m, 1h, 4h, 6h, 1d)
- Cross-provider validation (Coinbase, Yahoo, CoinGecko, etc.)
- Complete performance metrics and rankings
- Results export to CSV for analysis

Author: Bobby's Algo Trading System
Date: 2025-01-17
"""

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from backtesting import Backtest
from atss_adx_trend_strength import ATSSStrategy
import warnings
warnings.filterwarnings('ignore')


class ATSSMultiAssetTester:
    """Comprehensive multi-asset testing framework for ATSS strategy"""

    def __init__(self, base_data_path="/Users/bobbyyo/Projects/algo-fun/data"):
        """Initialize the ATSS multi-asset tester"""
        self.base_data_path = base_data_path
        self.results = []
        self.detailed_results = {}

        # Priority assets for ATSS testing (strong trending assets)
        self.priority_assets = ['ETH', 'BTC', 'HBAR', 'LINK']

        # Optimal timeframes for trend-following with ADX
        self.priority_timeframes = ['4h', '1d', '6h']

        # All timeframes for comprehensive testing
        self.all_timeframes = ['1m', '5m', '15m', '1h', '4h', '6h', '1d']

        print("=" * 80)
        print("ADX TREND STRENGTH SYSTEM (ATSS) - MULTI-ASSET TESTER")
        print("=" * 80)
        print(f"Base Data Path: {self.base_data_path}")
        print(f"Priority Assets: {', '.join(self.priority_assets)}")
        print(f"Priority Timeframes: {', '.join(self.priority_timeframes)}")
        print("=" * 80 + "\n")

    def discover_data_files(self):
        """Discover all available cryptocurrency data files"""
        data_sources = {
            'coinbase': 'coinbase/*.csv',
            'yahoo': 'yahoo/*.csv',
            'coingecko': 'coingecko/*.csv',
            'coinmarketcap': 'coinmarketcap/*.csv',
            'cryptocompare': 'cryptocompare/*.csv'
        }

        discovered_files = {}
        for source, pattern in data_sources.items():
            source_path = os.path.join(self.base_data_path, pattern)
            files = glob.glob(source_path)

            for file in files:
                filename = os.path.basename(file)

                # Parse asset and timeframe from filename
                parts = filename.replace('.csv', '').split('-')
                if len(parts) >= 2:
                    asset = parts[0].upper()

                    # Handle different naming conventions
                    if source == 'yahoo':
                        asset = asset.replace('USD', '')

                    # Extract timeframe if present
                    timeframe = None
                    for part in parts:
                        if part in self.all_timeframes:
                            timeframe = part
                            break

                    # If no timeframe found, check for common patterns
                    if not timeframe:
                        if '1d' in filename.lower() or 'daily' in filename.lower():
                            timeframe = '1d'
                        elif '6h' in filename.lower():
                            timeframe = '6h'
                        elif '4h' in filename.lower():
                            timeframe = '4h'
                        elif '1h' in filename.lower() or 'hourly' in filename.lower():
                            timeframe = '1h'
                        elif '15m' in filename.lower():
                            timeframe = '15m'
                        elif '5m' in filename.lower():
                            timeframe = '5m'
                        elif '1m' in filename.lower():
                            timeframe = '1m'

                    if asset and timeframe:
                        key = f"{asset}_{timeframe}_{source}"
                        discovered_files[key] = file

        print(f"Discovered {len(discovered_files)} data files across all sources")

        # Group by asset for summary
        asset_summary = {}
        for key in discovered_files:
            asset = key.split('_')[0]
            if asset not in asset_summary:
                asset_summary[asset] = []
            asset_summary[asset].append(key)

        print("\nAssets available for testing:")
        for asset in sorted(asset_summary.keys()):
            print(f"  {asset}: {len(asset_summary[asset])} data files")

        return discovered_files

    def load_and_validate_data(self, filepath):
        """Load and validate data from CSV file"""
        try:
            # Load data - try different date column names
            date_columns = ['Date', 'date', 'datetime', 'Datetime', 'timestamp', 'Timestamp']
            df = None
            date_col_found = None

            for date_col in date_columns:
                try:
                    df = pd.read_csv(filepath, parse_dates=[date_col])
                    date_col_found = date_col
                    break
                except:
                    continue

            if df is None:
                # Try loading without date parsing first
                df = pd.read_csv(filepath)
                # Find date column
                for col in df.columns:
                    if any(date_name in col.lower() for date_name in ['date', 'time']):
                        date_col_found = col
                        df[col] = pd.to_datetime(df[col])
                        break

            if df is None or date_col_found is None:
                return None, "Could not find date column"

            # Standardize column names
            df.columns = [col.capitalize() for col in df.columns]

            # Handle the date column specifically
            if date_col_found.lower() != 'date':
                # Find the date column in capitalized columns
                for col in df.columns:
                    if date_col_found.lower() == col.lower():
                        df.rename(columns={col: 'Date'}, inplace=True)
                        break

            # Ensure required columns exist
            required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            for col in required_columns:
                if col not in df.columns:
                    return None, f"Missing required column: {col}"

            # Set Date as index
            df.set_index('Date', inplace=True)

            # Sort by date
            df.sort_index(inplace=True)

            # Remove duplicates
            df = df[~df.index.duplicated(keep='first')]

            # Check minimum data points for ADX calculation
            if len(df) < 200:  # Need sufficient data for ADX and EMAs
                return None, f"Insufficient data ({len(df)} bars)"

            # Check for data quality
            if df[['Open', 'High', 'Low', 'Close']].isnull().any().any():
                # Fill forward for minor gaps
                df.fillna(method='ffill', inplace=True)

            return df, "Valid"

        except Exception as e:
            return None, f"Error loading: {str(e)}"

    def test_strategy_on_data(self, data, asset, timeframe, source, initial_cash=10000):
        """Test ATSS strategy on given data"""
        try:
            # Run backtest with ATSS strategy
            bt = Backtest(
                data,
                ATSSStrategy,
                cash=initial_cash,
                commission=0.001,  # 0.1% commission
                exclusive_orders=True,
                hedging=False,
                trade_on_close=False
            )

            # Run the backtest
            stats = bt.run()

            # Store detailed results
            result_key = f"{asset}_{timeframe}_{source}"
            self.detailed_results[result_key] = {
                'stats': stats,
                'data_points': len(data),
                'date_range': f"{data.index[0].strftime('%Y-%m-%d')} to {data.index[-1].strftime('%Y-%m-%d')}"
            }

            # Calculate additional metrics
            total_return = stats['Return [%]']
            sharpe_ratio = stats.get('Sharpe Ratio', 0)
            win_rate = stats.get('Win Rate [%]', 0)
            max_drawdown = stats.get('Max. Drawdown [%]', 0)
            num_trades = stats.get('# Trades', 0)

            # Score calculation (weighted by importance for ATSS)
            score = (
                total_return * 0.3 +  # Return weight
                sharpe_ratio * 20 +    # Risk-adjusted return
                win_rate * 0.3 +       # Win rate importance for pullback strategy
                max(-max_drawdown, -50) * 0.2 +  # Drawdown penalty
                min(num_trades / 10, 10)  # Trade frequency bonus
            )

            return {
                'asset': asset,
                'timeframe': timeframe,
                'source': source,
                'return': total_return,
                'sharpe': sharpe_ratio,
                'win_rate': win_rate,
                'max_drawdown': max_drawdown,
                'num_trades': num_trades,
                'score': score,
                'exposure_time': stats.get('Exposure Time [%]', 0),
                'avg_trade': stats.get('Avg. Trade [%]', 0),
                'profit_factor': stats.get('Profit Factor', 0),
                'expectancy': stats.get('Expectancy [%]', 0)
            }

        except Exception as e:
            print(f"  Error testing {asset} {timeframe} ({source}): {str(e)}")
            return None

    def run_comprehensive_test(self, test_priority_only=False):
        """Run comprehensive testing across all discovered assets and timeframes"""

        # Discover available data files
        discovered_files = self.discover_data_files()

        if not discovered_files:
            print("No data files found!")
            return

        print("\n" + "=" * 80)
        print("STARTING COMPREHENSIVE ATSS TESTING")
        print("=" * 80 + "\n")

        # Filter for priority testing if requested
        if test_priority_only:
            filtered_files = {}
            for key, filepath in discovered_files.items():
                asset = key.split('_')[0]
                timeframe = key.split('_')[1]
                if asset in self.priority_assets and timeframe in self.priority_timeframes:
                    filtered_files[key] = filepath
            test_files = filtered_files
            print(f"Testing priority assets and timeframes only: {len(test_files)} configurations")
        else:
            test_files = discovered_files
            print(f"Testing all discovered files: {len(test_files)} configurations")

        # Test each configuration
        total_tests = len(test_files)
        completed_tests = 0
        successful_tests = 0

        for key, filepath in sorted(test_files.items()):
            completed_tests += 1
            asset, timeframe, source = key.split('_')

            print(f"\n[{completed_tests}/{total_tests}] Testing {asset} {timeframe} from {source}")
            print(f"  File: {filepath}")

            # Load and validate data
            data, validation_msg = self.load_and_validate_data(filepath)

            if data is None:
                print(f"  Skipping: {validation_msg}")
                continue

            print(f"  Data loaded: {len(data)} bars, {validation_msg}")

            # Test strategy
            result = self.test_strategy_on_data(data, asset, timeframe, source)

            if result:
                self.results.append(result)
                successful_tests += 1

                # Print summary
                print(f"  Results: Return={result['return']:.2f}%, Sharpe={result['sharpe']:.2f}, "
                      f"Win Rate={result['win_rate']:.1f}%, Trades={result['num_trades']}")

        print("\n" + "=" * 80)
        print(f"TESTING COMPLETE: {successful_tests}/{total_tests} successful")
        print("=" * 80)

    def analyze_results(self):
        """Analyze and display comprehensive results"""

        if not self.results:
            print("No results to analyze!")
            return

        print("\n" + "=" * 80)
        print("ATSS STRATEGY - COMPREHENSIVE ANALYSIS")
        print("=" * 80)

        # Convert to DataFrame for analysis
        df_results = pd.DataFrame(self.results)

        # Overall Performance Summary
        print("\n1. OVERALL PERFORMANCE SUMMARY")
        print("-" * 40)
        print(f"Total configurations tested: {len(df_results)}")
        print(f"Profitable configurations: {len(df_results[df_results['return'] > 0])} "
              f"({len(df_results[df_results['return'] > 0])/len(df_results)*100:.1f}%)")
        print(f"Average Return: {df_results['return'].mean():.2f}%")
        print(f"Best Return: {df_results['return'].max():.2f}%")
        print(f"Worst Return: {df_results['return'].min():.2f}%")
        print(f"Average Sharpe Ratio: {df_results['sharpe'].mean():.2f}")
        print(f"Average Win Rate: {df_results['win_rate'].mean():.1f}%")

        # Top Performing Configurations
        print("\n2. TOP 10 PERFORMING CONFIGURATIONS (by Score)")
        print("-" * 40)
        top_10 = df_results.nlargest(10, 'score')[
            ['asset', 'timeframe', 'source', 'return', 'sharpe', 'win_rate', 'num_trades', 'score']
        ]
        for idx, row in top_10.iterrows():
            print(f"{row['asset']}-{row['timeframe']} ({row['source']}): "
                  f"Return={row['return']:.2f}%, Sharpe={row['sharpe']:.2f}, "
                  f"WR={row['win_rate']:.1f}%, Trades={row['num_trades']:.0f}, Score={row['score']:.2f}")

        # Asset Performance Ranking
        print("\n3. ASSET PERFORMANCE RANKING (Average Across All Timeframes)")
        print("-" * 40)
        asset_performance = df_results.groupby('asset').agg({
            'return': 'mean',
            'sharpe': 'mean',
            'win_rate': 'mean',
            'score': 'mean',
            'num_trades': 'sum'
        }).round(2)
        asset_performance = asset_performance.sort_values('score', ascending=False)

        for asset in asset_performance.index:
            stats = asset_performance.loc[asset]
            print(f"{asset}: Return={stats['return']:.2f}%, Sharpe={stats['sharpe']:.2f}, "
                  f"WR={stats['win_rate']:.1f}%, Score={stats['score']:.2f}, "
                  f"Total Trades={stats['num_trades']:.0f}")

        # Timeframe Performance Analysis
        print("\n4. TIMEFRAME PERFORMANCE ANALYSIS")
        print("-" * 40)
        timeframe_performance = df_results.groupby('timeframe').agg({
            'return': 'mean',
            'sharpe': 'mean',
            'win_rate': 'mean',
            'score': 'mean',
            'num_trades': 'mean'
        }).round(2)
        timeframe_performance = timeframe_performance.sort_values('score', ascending=False)

        for tf in timeframe_performance.index:
            stats = timeframe_performance.loc[tf]
            print(f"{tf}: Return={stats['return']:.2f}%, Sharpe={stats['sharpe']:.2f}, "
                  f"WR={stats['win_rate']:.1f}%, Avg Trades={stats['num_trades']:.1f}, "
                  f"Score={stats['score']:.2f}")

        # Priority Assets Deep Dive
        print("\n5. PRIORITY ASSETS DEEP DIVE")
        print("-" * 40)
        for asset in self.priority_assets:
            asset_data = df_results[df_results['asset'] == asset]
            if not asset_data.empty:
                print(f"\n{asset} Performance:")
                best_config = asset_data.nlargest(1, 'score').iloc[0]
                print(f"  Best Config: {best_config['timeframe']} ({best_config['source']})")
                print(f"  Best Return: {best_config['return']:.2f}%")
                print(f"  Best Sharpe: {best_config['sharpe']:.2f}")
                print(f"  Best Win Rate: {best_config['win_rate']:.1f}%")
                print(f"  Configurations Tested: {len(asset_data)}")
                print(f"  Profitable Configs: {len(asset_data[asset_data['return'] > 0])}")

        # Data Source Comparison
        print("\n6. DATA SOURCE RELIABILITY")
        print("-" * 40)
        source_performance = df_results.groupby('source').agg({
            'return': 'mean',
            'sharpe': 'mean',
            'score': 'mean'
        }).round(2)
        source_performance = source_performance.sort_values('score', ascending=False)

        for source in source_performance.index:
            stats = source_performance.loc[source]
            configs = len(df_results[df_results['source'] == source])
            print(f"{source}: Avg Return={stats['return']:.2f}%, "
                  f"Avg Sharpe={stats['sharpe']:.2f}, Score={stats['score']:.2f} "
                  f"({configs} configs)")

        return df_results

    def save_results(self, df_results=None):
        """Save comprehensive results to CSV files"""

        if df_results is None:
            df_results = pd.DataFrame(self.results)

        if df_results.empty:
            print("No results to save!")
            return

        # Create results directory if it doesn't exist
        results_dir = "/Users/bobbyyo/Projects/algo-fun/strategies/results"
        os.makedirs(results_dir, exist_ok=True)

        # Generate timestamp for unique filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save summary results
        summary_file = os.path.join(results_dir, f'atss_summary_{timestamp}.csv')
        df_results.to_csv(summary_file, index=False)
        print(f"\nSummary results saved to: {summary_file}")

        # Save detailed results with full stats
        detailed_data = []
        for key, details in self.detailed_results.items():
            stats = details['stats']
            detailed_data.append({
                'configuration': key,
                'data_points': details['data_points'],
                'date_range': details['date_range'],
                **{k: v for k, v in stats.items() if not isinstance(v, (pd.Series, pd.DataFrame))}
            })

        if detailed_data:
            detailed_file = os.path.join(results_dir, f'atss_detailed_{timestamp}.csv')
            pd.DataFrame(detailed_data).to_csv(detailed_file, index=False)
            print(f"Detailed results saved to: {detailed_file}")

        # Save asset performance ranking
        asset_performance = df_results.groupby('asset').agg({
            'return': ['mean', 'max', 'min'],
            'sharpe': ['mean', 'max'],
            'win_rate': ['mean', 'max'],
            'score': 'mean'
        }).round(2)
        asset_file = os.path.join(results_dir, f'atss_asset_ranking_{timestamp}.csv')
        asset_performance.to_csv(asset_file)
        print(f"Asset rankings saved to: {asset_file}")

        return summary_file, detailed_file, asset_file

    def display_detailed_stats(self, config_key):
        """Display complete backtesting.py stats for a specific configuration"""

        if config_key not in self.detailed_results:
            print(f"No results found for {config_key}")
            return

        stats = self.detailed_results[config_key]['stats']

        print("\n" + "=" * 80)
        print(f"DETAILED STATS FOR {config_key}")
        print("=" * 80)
        print(stats)


def main():
    """Main execution function"""

    # Initialize tester
    tester = ATSSMultiAssetTester()

    # Run comprehensive testing (set to True for priority assets only)
    tester.run_comprehensive_test(test_priority_only=False)

    # Analyze results
    df_results = tester.analyze_results()

    # Save results to CSV
    if df_results is not None and not df_results.empty:
        tester.save_results(df_results)

        # Display top configuration details
        print("\n" + "=" * 80)
        print("TOP CONFIGURATION - COMPLETE BACKTESTING STATS")
        print("=" * 80)

        if not df_results.empty:
            top_config = df_results.nlargest(1, 'score').iloc[0]
            config_key = f"{top_config['asset']}_{top_config['timeframe']}_{top_config['source']}"
            tester.display_detailed_stats(config_key)

    print("\n" + "=" * 80)
    print("ATSS MULTI-ASSET TESTING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()