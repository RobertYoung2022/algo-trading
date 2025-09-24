"""
🔧 ClucMay72018 Flexible Parameter Testing Suite
================================================
Comprehensive testing of flexible ClucMay72018 strategy across all phases and data sources

Tests all three flexibility phases:
- Phase 1: Moderate (BB 102%, Volume 50%, All conditions)
- Phase 2: High (BB 105%, Volume 75%, 2-out-of-3)
- Phase 3: Alternative (RSI/BB, Any below-avg, MACD)

Created: September 2025
Author: Bobby 🌙💫🚀
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from backtesting import Backtest

# Import strategy classes
from clucmay72018_flexible_params import (
    ClucMay72018FlexibleStrategy,
    create_phase1_strategy,
    create_phase2_strategy,
    create_phase3_strategy
)

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent))


class FlexibleStrategyTester:
    """
    Test runner for flexible ClucMay72018 strategy phases
    """

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.data_dir = self.project_root / "data"
        self.results = []
        self.test_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def discover_all_data_sources(self) -> List[Dict]:
        """
        Discover ALL available data sources across all directories
        Returns list of data info dicts with paths and metadata
        """
        data_sources = []

        # Define search patterns for each data provider
        search_patterns = [
            # Coinbase data (all variations)
            ("coinbase", "**/*USDT*.csv"),
            ("coinbase", "**/*USD*.csv"),
            ("coinbase", "**/*-*.csv"),

            # Yahoo Finance data
            ("yahoo", "**/*yahoo*.csv"),
            ("yahoo", "**/*USD*.csv"),

            # CryptoCompare data
            ("cryptocompare", "**/*.csv"),

            # CoinGecko data
            ("coingecko", "**/*.csv"),

            # CoinMarketCap data
            ("coinmarketcap", "**/*.csv"),

            # Hyperliquid data (only validated)
            ("hyperliquid", "**/ETH*.csv"),
            ("hyperliquid", "**/BTC*.csv"),
        ]

        # Search each provider directory
        for provider, pattern in search_patterns:
            provider_dir = self.data_dir / provider
            if provider_dir.exists():
                for csv_file in provider_dir.glob(pattern):
                    # Skip test files and backups
                    if any(x in str(csv_file).lower() for x in ['test', 'backup', 'temp', 'old']):
                        continue

                    # Skip corrupted files we know about
                    if "BTCUSD-1d-1000wks-data.csv" in csv_file.name:
                        continue

                    data_sources.append({
                        'path': csv_file,
                        'provider': provider,
                        'filename': csv_file.name,
                        'size_mb': csv_file.stat().st_size / (1024 * 1024)
                    })

        # Also check root data directory
        for csv_file in self.data_dir.glob("*.csv"):
            if any(x in str(csv_file).lower() for x in ['test', 'backup', 'temp']):
                continue

            data_sources.append({
                'path': csv_file,
                'provider': 'root',
                'filename': csv_file.name,
                'size_mb': csv_file.stat().st_size / (1024 * 1024)
            })

        return data_sources

    def load_and_validate_data(self, data_path: Path) -> Optional[pd.DataFrame]:
        """
        Load data with validation
        Returns None if data fails validation
        """
        try:
            # Load the data
            df = pd.read_csv(data_path)

            # Standardize column names
            df.columns = [col.strip().title() for col in df.columns]

            # Check for required columns
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in df.columns for col in required_cols):
                # Try alternative naming
                alt_mappings = {
                    'open': 'Open', 'high': 'High', 'low': 'Low',
                    'close': 'Close', 'volume': 'Volume',
                    'price': 'Close', 'vol': 'Volume'
                }

                df.columns = [alt_mappings.get(col.lower(), col) for col in df.columns]

                if not all(col in df.columns for col in required_cols):
                    print(f"  ⚠️ Missing columns in {data_path.name}")
                    return None

            # Handle date column
            date_cols = ['Date', 'Datetime', 'Time', 'Timestamp']
            date_col = None
            for col in date_cols:
                if col in df.columns:
                    date_col = col
                    break

            if date_col:
                df['Date'] = pd.to_datetime(df[date_col])
                df.set_index('Date', inplace=True)
            elif not isinstance(df.index, pd.DatetimeIndex):
                # Try to parse index as date
                try:
                    df.index = pd.to_datetime(df.index)
                except:
                    print(f"  ⚠️ Could not parse dates in {data_path.name}")
                    return None

            # Sort by date
            df.sort_index(inplace=True)

            # Basic data quality checks
            if len(df) < 100:
                print(f"  ⚠️ Insufficient data in {data_path.name}: {len(df)} rows")
                return None

            # Check for invalid values
            if df[['Open', 'High', 'Low', 'Close']].isna().any().any():
                print(f"  ⚠️ NaN values in price data: {data_path.name}")
                return None

            if (df[['Open', 'High', 'Low', 'Close']] <= 0).any().any():
                print(f"  ⚠️ Invalid price values in {data_path.name}")
                return None

            return df

        except Exception as e:
            print(f"  ❌ Error loading {data_path.name}: {str(e)}")
            return None

    def enhanced_backtest_runner(self, data: pd.DataFrame, strategy_class,
                                  data_info: Dict, phase_name: str) -> Optional[Dict]:
        """
        Run backtest with COMPLETE native stats display
        """
        try:
            # Run backtest
            bt = Backtest(
                data,
                strategy_class,
                cash=10000,
                commission=0.002,
                exclusive_orders=True
            )

            # Run strategy
            stats = bt.run()

            # Display COMPLETE backtesting.py output
            print("\n" + "="*80)
            print(f"📊 COMPLETE BACKTESTING.PY OUTPUT - {phase_name}")
            print(f"📁 Data: {data_info['filename']}")
            print("="*80)
            print(stats)  # This prints the FULL 30+ line output
            print("="*80 + "\n")

            # Extract key metrics for comparison
            result = {
                'phase': phase_name,
                'data_source': data_info['filename'],
                'provider': data_info['provider'],
                'data_rows': len(data),
                'date_range': f"{data.index[0]} to {data.index[-1]}",

                # Performance metrics
                'total_return': stats['Return [%]'],
                'buy_hold_return': stats['Buy & Hold Return [%]'],
                'sharpe_ratio': stats.get('Sharpe Ratio', np.nan),
                'sortino_ratio': stats.get('Sortino Ratio', np.nan),
                'calmar_ratio': stats.get('Calmar Ratio', np.nan),

                # Trade statistics
                'num_trades': stats['# Trades'],
                'win_rate': stats.get('Win Rate [%]', 0),
                'avg_trade': stats.get('Avg. Trade [%]', 0),
                'best_trade': stats.get('Best Trade [%]', 0),
                'worst_trade': stats.get('Worst Trade [%]', 0),

                # Risk metrics
                'max_drawdown': stats['Max. Drawdown [%]'],
                'avg_drawdown': stats.get('Avg. Drawdown [%]', np.nan),
                'max_dd_duration': stats.get('Max. Drawdown Duration', 'N/A'),

                # Additional metrics
                'profit_factor': stats.get('Profit Factor', np.nan),
                'expectancy': stats.get('Expectancy [%]', np.nan),
                'sqn': stats.get('SQN', np.nan),
                'exposure_time': stats.get('Exposure Time [%]', np.nan),
            }

            return result

        except Exception as e:
            print(f"  ❌ Backtest failed for {phase_name} on {data_info['filename']}: {str(e)}")
            return None

    def run_all_phase_tests(self):
        """
        Run all three phases across all discovered data sources
        """
        print("\n" + "🚀"*20)
        print("🔧 CLUCMAY72018 FLEXIBLE PARAMETER TESTING SUITE")
        print("🚀"*20 + "\n")

        # Discover all data sources
        print("📡 Discovering data sources...")
        data_sources = self.discover_all_data_sources()
        print(f"✅ Found {len(data_sources)} potential data sources\n")

        # Define strategy phases
        phases = [
            ('Phase 1: Moderate', create_phase1_strategy()),
            ('Phase 2: High Flex', create_phase2_strategy()),
            ('Phase 3: Alternative', create_phase3_strategy())
        ]

        # Test each phase on each data source
        total_tests = len(phases) * len(data_sources)
        test_num = 0

        for phase_name, strategy_class in phases:
            print("\n" + "="*80)
            print(f"🎯 TESTING {phase_name.upper()}")
            print("="*80)

            phase_results = []

            for data_info in data_sources:
                test_num += 1
                print(f"\n[{test_num}/{total_tests}] Testing {data_info['filename']} ({data_info['provider']})")

                # Load and validate data
                data = self.load_and_validate_data(data_info['path'])
                if data is None:
                    continue

                # Run backtest with full output
                result = self.enhanced_backtest_runner(
                    data, strategy_class, data_info, phase_name
                )

                if result:
                    phase_results.append(result)
                    self.results.append(result)

                    # Show trade count immediately
                    print(f"  ✅ Trades Generated: {result['num_trades']}")
                    if result['num_trades'] > 0:
                        print(f"     Win Rate: {result['win_rate']:.1f}%")
                        print(f"     Sharpe: {result['sharpe_ratio']:.2f}")

            # Phase summary
            self.print_phase_summary(phase_name, phase_results)

        # Final comprehensive analysis
        self.print_final_analysis()

        # Save results to CSV
        self.save_results_to_csv()

    def print_phase_summary(self, phase_name: str, results: List[Dict]):
        """
        Print summary for a single phase
        """
        print(f"\n{'='*80}")
        print(f"📊 {phase_name} SUMMARY")
        print(f"{'='*80}")

        if not results:
            print("No successful tests for this phase")
            return

        # Calculate statistics
        total_trades = sum(r['num_trades'] for r in results)
        assets_with_trades = sum(1 for r in results if r['num_trades'] > 0)

        print(f"✅ Total Tests: {len(results)}")
        print(f"📈 Assets with Trades: {assets_with_trades}/{len(results)}")
        print(f"🔄 Total Trades Generated: {total_trades}")

        if total_trades > 0:
            # Find best performers
            by_sharpe = sorted([r for r in results if r['num_trades'] > 0],
                               key=lambda x: x['sharpe_ratio'], reverse=True)

            print(f"\n🏆 Top 3 Performers (by Sharpe):")
            for i, r in enumerate(by_sharpe[:3], 1):
                print(f"  {i}. {r['data_source']}: Sharpe {r['sharpe_ratio']:.2f}, "
                      f"Trades {r['num_trades']}, Win Rate {r['win_rate']:.1f}%")

    def print_final_analysis(self):
        """
        Print comprehensive final analysis across all phases
        """
        print("\n" + "🌟"*20)
        print("📊 COMPREHENSIVE FINAL ANALYSIS")
        print("🌟"*20 + "\n")

        if not self.results:
            print("No successful tests completed")
            return

        # Group by phase
        phases = {}
        for r in self.results:
            if r['phase'] not in phases:
                phases[r['phase']] = []
            phases[r['phase']].append(r)

        print("📈 PHASE COMPARISON:")
        print("-"*60)

        for phase_name, phase_results in phases.items():
            total_trades = sum(r['num_trades'] for r in phase_results)
            assets_with_trades = sum(1 for r in phase_results if r['num_trades'] > 0)

            if total_trades > 0:
                avg_sharpe = np.mean([r['sharpe_ratio'] for r in phase_results if r['num_trades'] > 0])
                avg_win_rate = np.mean([r['win_rate'] for r in phase_results if r['num_trades'] > 0])
            else:
                avg_sharpe = 0
                avg_win_rate = 0

            print(f"\n{phase_name}:")
            print(f"  - Total Trades: {total_trades}")
            print(f"  - Assets with Trades: {assets_with_trades}/{len(phase_results)}")
            if total_trades > 0:
                print(f"  - Avg Sharpe: {avg_sharpe:.2f}")
                print(f"  - Avg Win Rate: {avg_win_rate:.1f}%")

        # Find overall best performers
        all_with_trades = [r for r in self.results if r['num_trades'] > 0]

        if all_with_trades:
            print(f"\n🏆 TOP 5 OVERALL PERFORMERS (ALL PHASES):")
            print("-"*60)

            by_sharpe = sorted(all_with_trades, key=lambda x: x['sharpe_ratio'], reverse=True)
            for i, r in enumerate(by_sharpe[:5], 1):
                print(f"{i}. {r['phase']} on {r['data_source']}")
                print(f"   Sharpe: {r['sharpe_ratio']:.2f} | Trades: {r['num_trades']} | "
                      f"Win Rate: {r['win_rate']:.1f}% | Return: {r['total_return']:.2f}%")

        # Asset performance across phases
        print(f"\n📊 ASSET PERFORMANCE SUMMARY:")
        print("-"*60)

        assets = {}
        for r in self.results:
            asset = r['data_source'].split('-')[0] if '-' in r['data_source'] else r['data_source'].split('.')[0]
            if asset not in assets:
                assets[asset] = []
            assets[asset].append(r)

        for asset, asset_results in assets.items():
            total_trades = sum(r['num_trades'] for r in asset_results)
            if total_trades > 0:
                print(f"\n{asset}:")
                print(f"  Total Trades across phases: {total_trades}")
                best = max(asset_results, key=lambda x: x['sharpe_ratio'] if x['num_trades'] > 0 else -999)
                if best['num_trades'] > 0:
                    print(f"  Best Phase: {best['phase']} (Sharpe: {best['sharpe_ratio']:.2f})")

    def save_results_to_csv(self):
        """
        Save all results to CSV files
        """
        if not self.results:
            print("\nNo results to save")
            return

        # Create results dataframe
        df = pd.DataFrame(self.results)

        # Save to results directory
        results_dir = Path(__file__).parent.parent / "results"
        results_dir.mkdir(exist_ok=True)

        # Save detailed results
        detailed_file = results_dir / f"clucmay72018_flexible_results_{self.test_timestamp}.csv"
        df.to_csv(detailed_file, index=False)
        print(f"\n💾 Detailed results saved to: {detailed_file}")

        # Create and save summary by phase
        summary_data = []
        for phase in df['phase'].unique():
            phase_df = df[df['phase'] == phase]
            phase_with_trades = phase_df[phase_df['num_trades'] > 0]

            if len(phase_with_trades) > 0:
                summary_data.append({
                    'phase': phase,
                    'total_tests': len(phase_df),
                    'tests_with_trades': len(phase_with_trades),
                    'total_trades': phase_with_trades['num_trades'].sum(),
                    'avg_trades_per_asset': phase_with_trades['num_trades'].mean(),
                    'avg_sharpe': phase_with_trades['sharpe_ratio'].mean(),
                    'avg_win_rate': phase_with_trades['win_rate'].mean(),
                    'avg_return': phase_with_trades['total_return'].mean(),
                    'best_sharpe': phase_with_trades['sharpe_ratio'].max(),
                    'best_return': phase_with_trades['total_return'].max(),
                })

        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_file = results_dir / f"clucmay72018_flexible_summary_{self.test_timestamp}.csv"
            summary_df.to_csv(summary_file, index=False)
            print(f"💾 Summary results saved to: {summary_file}")


def main():
    """
    Main execution function
    """
    print("🌙 ClucMay72018 Flexible Parameter Testing 🌙")
    print("=" * 80)
    print("Testing three flexibility phases across ALL available data sources")
    print("Phase 1: Moderate flexibility (BB 102%, Volume 50%)")
    print("Phase 2: High flexibility (BB 105%, Volume 75%, 2-of-3)")
    print("Phase 3: Alternative approach (RSI/BB, any below-avg, MACD)")
    print("=" * 80)

    # Run comprehensive tests
    tester = FlexibleStrategyTester()
    tester.run_all_phase_tests()

    print("\n" + "🚀"*20)
    print("✅ FLEXIBLE PARAMETER TESTING COMPLETE!")
    print("🚀"*20)


if __name__ == "__main__":
    main()