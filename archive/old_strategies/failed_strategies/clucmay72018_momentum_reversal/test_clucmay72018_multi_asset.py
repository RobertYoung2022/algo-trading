"""
🚀 Comprehensive Multi-Asset Tester for ClucMay72018 Strategy
==============================================================
Tests the ultra-selective momentum-reversal strategy across ALL available data sources
Includes 28+ data sources from multiple providers and timeframes

Features:
- Automatic discovery of all available data sources
- Cross-provider validation (Coinbase, Yahoo, Hyperliquid, etc.)
- Multi-timeframe testing where available
- Complete native backtesting.py results display
- Asset performance ranking and analysis
- Results saved to CSV for further analysis

Created: September 2025
Author: Bobby 🌙💫🚀
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import glob
from typing import Dict, List, Tuple, Optional
from backtesting import Backtest
from clucmay72018_momentum_reversal_strategy import ClucMay72018Strategy
import warnings
warnings.filterwarnings('ignore')


class ComprehensiveMultiAssetTester:
    """
    Enhanced tester for ClucMay72018 strategy across all available data sources
    """

    def __init__(self, strategy_class=ClucMay72018Strategy):
        self.strategy_class = strategy_class
        self.results = []
        self.data_base_path = "/Users/bobbyyo/Projects/algo-fun/data"
        self.results_path = "/Users/bobbyyo/Projects/algo-fun/strategies/results"

        # Ensure results directory exists
        Path(self.results_path).mkdir(parents=True, exist_ok=True)

    def discover_all_data_sources(self) -> List[Dict]:
        """
        Discover ALL available data sources across all providers
        Returns list of data source info dicts
        """
        data_sources = []

        # Pattern list for comprehensive discovery
        patterns = [
            # Coinbase patterns
            "coinbase/*5m*.csv",
            "coinbase/*6h*.csv",
            "coinbase/*1d*.csv",
            "coinbase/*1h*.csv",
            "coinbase/*15m*.csv",

            # Yahoo Finance patterns
            "yahoo/*.csv",
            "yahoo_finance/*.csv",

            # Hyperliquid patterns (validated only)
            "hyperliquid/*5m*.csv",
            "hyperliquid/*1h*.csv",
            "hyperliquid/*1d*.csv",

            # CryptoCompare patterns
            "cryptocompare/*.csv",

            # CoinGecko patterns
            "coingecko/*.csv",

            # CoinMarketCap patterns
            "coinmarketcap/*.csv",

            # Any other CSV files in subdirectories
            "*/*.csv",
            "*/*/*.csv"
        ]

        # Collect all CSV files
        all_files = set()
        for pattern in patterns:
            full_pattern = os.path.join(self.data_base_path, pattern)
            files = glob.glob(full_pattern)
            all_files.update(files)

        # Process each discovered file
        for file_path in sorted(all_files):
            # Skip known corrupted files
            if "CORRUPTED" in file_path.upper():
                continue

            # Extract metadata from path
            rel_path = os.path.relpath(file_path, self.data_base_path)
            parts = rel_path.split(os.sep)

            provider = parts[0] if len(parts) > 0 else "unknown"
            filename = os.path.basename(file_path)

            # Parse asset and timeframe from filename
            asset = self._extract_asset_from_filename(filename)
            timeframe = self._extract_timeframe_from_filename(filename)

            data_sources.append({
                'path': file_path,
                'provider': provider,
                'asset': asset,
                'timeframe': timeframe,
                'filename': filename,
                'relative_path': rel_path
            })

        print(f"🔍 Discovered {len(data_sources)} total data sources")
        return data_sources

    def _extract_asset_from_filename(self, filename: str) -> str:
        """Extract asset symbol from filename"""
        filename_upper = filename.upper()

        # Common crypto symbols to look for
        symbols = ['BTC', 'ETH', 'CRO', 'HBAR', 'LINK', 'XRP', 'SOL', 'AVAX',
                   'MATIC', 'DOT', 'ADA', 'DOGE', 'SHIB', 'UNI', 'ATOM']

        for symbol in symbols:
            if symbol in filename_upper:
                return symbol

        # Fallback: try to extract from pattern like "BTCUSD" or "BTC-USD"
        import re
        match = re.search(r'([A-Z]{2,5})[-_]?USD', filename_upper)
        if match:
            return match.group(1)

        return "UNKNOWN"

    def _extract_timeframe_from_filename(self, filename: str) -> str:
        """Extract timeframe from filename"""
        filename_lower = filename.lower()

        # Common timeframe patterns
        timeframes = {
            '1m': ['1m', '1min', '1-min', '1_min', 'minute'],
            '5m': ['5m', '5min', '5-min', '5_min'],
            '15m': ['15m', '15min', '15-min', '15_min'],
            '30m': ['30m', '30min', '30-min', '30_min'],
            '1h': ['1h', '1hr', '1hour', '1-hour', '60m', '60min'],
            '6h': ['6h', '6hr', '6hour', '6-hour', '360m'],
            '1d': ['1d', '1day', 'daily', '24h', '24hr']
        }

        for tf, patterns in timeframes.items():
            for pattern in patterns:
                if pattern in filename_lower:
                    return tf

        return "unknown"

    def load_and_validate_data(self, data_source: Dict) -> Optional[pd.DataFrame]:
        """
        Load and validate data from a source
        Returns DataFrame if valid, None otherwise
        """
        try:
            # Load the CSV
            df = pd.read_csv(data_source['path'])

            # Ensure required columns exist
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']

            # Handle different column naming conventions
            df.columns = [col.title() if col.lower() in [c.lower() for c in required_cols]
                          else col for col in df.columns]

            # Check for required columns
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                # Try alternative names
                alt_mappings = {
                    'Open': ['open', 'Open', 'OPEN', 'o'],
                    'High': ['high', 'High', 'HIGH', 'h'],
                    'Low': ['low', 'Low', 'LOW', 'l'],
                    'Close': ['close', 'Close', 'CLOSE', 'c'],
                    'Volume': ['volume', 'Volume', 'VOLUME', 'vol', 'v']
                }

                for req_col, alt_names in alt_mappings.items():
                    if req_col not in df.columns:
                        for alt in alt_names:
                            if alt in df.columns:
                                df[req_col] = df[alt]
                                break

            # Final check for required columns
            if not all(col in df.columns for col in required_cols):
                return None

            # Handle datetime index
            date_cols = ['Date', 'date', 'Datetime', 'datetime', 'Time', 'time', 'timestamp']
            for col in date_cols:
                if col in df.columns:
                    df.index = pd.to_datetime(df[col])
                    break
            else:
                # If no date column, use numeric index
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index) if df.index.dtype == 'O' else df.index

            # Ensure numeric data types
            for col in required_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Remove NaN values
            df = df.dropna(subset=required_cols)

            # Require minimum data points
            if len(df) < 100:
                return None

            # Add Volume if it's all zeros (some data sources don't have volume)
            if df['Volume'].sum() == 0:
                df['Volume'] = 1000  # Default volume for testing

            return df

        except Exception as e:
            print(f"  ⚠️ Error loading {data_source['filename']}: {str(e)[:50]}")
            return None

    def run_backtest(self, df: pd.DataFrame, data_source: Dict) -> Optional[Dict]:
        """
        Run backtest on a single data source
        Returns results dict if successful, None otherwise
        """
        try:
            # Initialize backtest
            bt = Backtest(
                df,
                self.strategy_class,
                cash=10000,
                commission=0.002,
                exclusive_orders=True
            )

            # Run the backtest
            stats = bt.run()

            # Extract key metrics
            result = {
                'provider': data_source['provider'],
                'asset': data_source['asset'],
                'timeframe': data_source['timeframe'],
                'filename': data_source['filename'],
                'path': data_source['path'],
                'data_points': len(df),
                'date_range': f"{df.index[0]} to {df.index[-1]}",

                # Performance metrics
                'return_pct': stats['Return [%]'],
                'buy_hold_return': stats['Buy & Hold Return [%]'],
                'sharpe_ratio': stats.get('Sharpe Ratio', 0),
                'sortino_ratio': stats.get('Sortino Ratio', 0),
                'calmar_ratio': stats.get('Calmar Ratio', 0),
                'max_drawdown': stats['Max. Drawdown [%]'],
                'avg_drawdown': stats['Avg. Drawdown [%]'],

                # Trade metrics
                'num_trades': stats['# Trades'],
                'win_rate': stats['Win Rate [%]'] if stats['# Trades'] > 0 else 0,
                'best_trade': stats['Best Trade [%]'] if stats['# Trades'] > 0 else 0,
                'worst_trade': stats['Worst Trade [%]'] if stats['# Trades'] > 0 else 0,
                'avg_trade': stats['Avg. Trade [%]'] if stats['# Trades'] > 0 else 0,
                'profit_factor': stats.get('Profit Factor', 0),
                'expectancy': stats.get('Expectancy [%]', 0),
                'sqn': stats.get('SQN', 0),

                # Exposure metrics
                'exposure_time': stats['Exposure Time [%]'],

                # Store full stats for display
                'full_stats': stats
            }

            return result

        except Exception as e:
            print(f"  ⚠️ Backtest error for {data_source['filename']}: {str(e)[:50]}")
            return None

    def enhanced_backtest_runner(self, df: pd.DataFrame, data_source: Dict) -> None:
        """
        Run backtest with COMPLETE native results display
        This ensures we show the full 30+ line backtesting.py output
        """
        try:
            print(f"\n{'='*80}")
            print(f"🎯 Testing: {data_source['asset']} - {data_source['provider']} - {data_source['timeframe']}")
            print(f"📁 File: {data_source['filename']}")
            print(f"📊 Data Points: {len(df)}, Range: {df.index[0]} to {df.index[-1]}")
            print(f"{'='*80}")

            # Initialize backtest
            bt = Backtest(
                df,
                self.strategy_class,
                cash=10000,
                commission=0.002,
                exclusive_orders=True
            )

            # Run the backtest
            stats = bt.run()

            # MANDATORY: Display COMPLETE native backtesting.py results
            print("\n" + "🚀 COMPLETE BACKTESTING.PY RESULTS " + "🚀")
            print("=" * 80)
            print(stats)  # This shows the FULL 30+ line output
            print("=" * 80)

            # Store result
            result = {
                'provider': data_source['provider'],
                'asset': data_source['asset'],
                'timeframe': data_source['timeframe'],
                'filename': data_source['filename'],
                'return_pct': stats['Return [%]'],
                'sharpe_ratio': stats.get('Sharpe Ratio', 0),
                'num_trades': stats['# Trades'],
                'win_rate': stats['Win Rate [%]'] if stats['# Trades'] > 0 else 0,
                'max_drawdown': stats['Max. Drawdown [%]']
            }

            self.results.append(result)

        except Exception as e:
            print(f"⚠️ Error: {str(e)}")

    def run_comprehensive_test(self):
        """
        Run comprehensive test across ALL discovered data sources
        Focus on 5-minute data but test others for comparison
        """
        print("\n" + "="*80)
        print("🚀 COMPREHENSIVE MULTI-ASSET TESTING FOR CLUCMAY72018 STRATEGY")
        print("="*80)

        # Discover all data sources
        all_sources = self.discover_all_data_sources()

        # Separate by timeframe (prioritize 5m for this strategy)
        sources_5m = [s for s in all_sources if s['timeframe'] == '5m']
        sources_other = [s for s in all_sources if s['timeframe'] != '5m']

        print(f"\n📊 Found {len(sources_5m)} 5-minute sources (primary timeframe)")
        print(f"📊 Found {len(sources_other)} other timeframe sources")

        # Test 5-minute sources first (primary timeframe)
        if sources_5m:
            print("\n" + "🎯 TESTING 5-MINUTE DATA SOURCES (PRIMARY) " + "🎯")
            print("="*60)

            for source in sources_5m:
                df = self.load_and_validate_data(source)
                if df is not None and len(df) > 100:
                    self.enhanced_backtest_runner(df, source)

        # Test a sample of other timeframes for comparison
        if sources_other:
            print("\n" + "📈 TESTING OTHER TIMEFRAMES FOR COMPARISON " + "📈")
            print("="*60)

            # Test up to 10 other sources for comparison
            for source in sources_other[:10]:
                df = self.load_and_validate_data(source)
                if df is not None and len(df) > 100:
                    self.enhanced_backtest_runner(df, source)

        # Generate comprehensive analysis
        self.generate_comprehensive_analysis()

        # Save results to CSV
        self.save_results_to_csv()

    def generate_comprehensive_analysis(self):
        """
        Generate comprehensive analysis of all results
        """
        if not self.results:
            print("\n⚠️ No valid results to analyze")
            return

        print("\n" + "="*80)
        print("📊 COMPREHENSIVE MULTI-ASSET PERFORMANCE ANALYSIS")
        print("="*80)

        # Convert to DataFrame for analysis
        df_results = pd.DataFrame(self.results)

        # 1. Overall Performance Summary
        print("\n🎯 OVERALL PERFORMANCE SUMMARY:")
        print("-"*60)
        print(f"Total Assets Tested: {df_results['asset'].nunique()}")
        print(f"Total Data Sources: {len(df_results)}")
        print(f"Average Return: {df_results['return_pct'].mean():.2f}%")
        print(f"Best Return: {df_results['return_pct'].max():.2f}%")
        print(f"Worst Return: {df_results['return_pct'].min():.2f}%")
        print(f"Average Sharpe: {df_results['sharpe_ratio'].mean():.2f}")
        print(f"Average Win Rate: {df_results['win_rate'].mean():.1f}%")

        # 2. Asset Performance Ranking (5m timeframe focus)
        df_5m = df_results[df_results['timeframe'] == '5m']
        if not df_5m.empty:
            print("\n🏆 ASSET PERFORMANCE RANKING (5-MINUTE DATA):")
            print("-"*60)
            asset_perf = df_5m.groupby('asset').agg({
                'return_pct': 'mean',
                'sharpe_ratio': 'mean',
                'win_rate': 'mean',
                'num_trades': 'mean'
            }).sort_values('sharpe_ratio', ascending=False)

            for i, (asset, row) in enumerate(asset_perf.iterrows(), 1):
                print(f"{i}. {asset}: Return={row['return_pct']:.2f}%, "
                      f"Sharpe={row['sharpe_ratio']:.2f}, "
                      f"WinRate={row['win_rate']:.1f}%, "
                      f"Trades={row['num_trades']:.0f}")

        # 3. Provider Comparison
        print("\n📈 PROVIDER PERFORMANCE COMPARISON:")
        print("-"*60)
        provider_perf = df_results.groupby('provider').agg({
            'return_pct': 'mean',
            'sharpe_ratio': 'mean',
            'asset': 'count'
        }).sort_values('sharpe_ratio', ascending=False)

        for provider, row in provider_perf.iterrows():
            print(f"{provider}: Return={row['return_pct']:.2f}%, "
                  f"Sharpe={row['sharpe_ratio']:.2f}, "
                  f"Sources={row['asset']:.0f}")

        # 4. Timeframe Analysis
        print("\n⏰ TIMEFRAME PERFORMANCE ANALYSIS:")
        print("-"*60)
        tf_perf = df_results.groupby('timeframe').agg({
            'return_pct': 'mean',
            'sharpe_ratio': 'mean',
            'win_rate': 'mean',
            'num_trades': 'mean',
            'asset': 'count'
        }).sort_values('sharpe_ratio', ascending=False)

        for tf, row in tf_perf.iterrows():
            print(f"{tf}: Return={row['return_pct']:.2f}%, "
                  f"Sharpe={row['sharpe_ratio']:.2f}, "
                  f"WinRate={row['win_rate']:.1f}%, "
                  f"AvgTrades={row['num_trades']:.0f}, "
                  f"Sources={row['asset']:.0f}")

        # 5. Strategy Characteristics
        print("\n💡 STRATEGY CHARACTERISTICS:")
        print("-"*60)
        total_trades = df_results['num_trades'].sum()
        avg_trades = df_results['num_trades'].mean()
        print(f"Total Trades Across All Tests: {total_trades:.0f}")
        print(f"Average Trades Per Test: {avg_trades:.1f}")
        print(f"Strategy Selectivity: {'ULTRA-HIGH' if avg_trades < 10 else 'HIGH' if avg_trades < 50 else 'MODERATE'}")

        # 6. Best Performers
        print("\n🌟 TOP 5 BEST PERFORMING CONFIGURATIONS:")
        print("-"*60)
        top_5 = df_results.nlargest(5, 'sharpe_ratio')
        for i, row in enumerate(top_5.itertuples(), 1):
            print(f"{i}. {row.asset}-{row.timeframe} ({row.provider}): "
                  f"Return={row.return_pct:.2f}%, Sharpe={row.sharpe_ratio:.2f}")

    def save_results_to_csv(self):
        """
        Save all results to CSV files
        """
        if not self.results:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save detailed results
        df_results = pd.DataFrame(self.results)
        detailed_file = os.path.join(self.results_path,
                                      f"clucmay72018_detailed_results_{timestamp}.csv")
        df_results.to_csv(detailed_file, index=False)

        print(f"\n📁 Results saved to:")
        print(f"   {detailed_file}")

        # Save summary by asset
        summary = df_results.groupby('asset').agg({
            'return_pct': ['mean', 'max', 'min'],
            'sharpe_ratio': 'mean',
            'win_rate': 'mean',
            'num_trades': 'sum'
        }).round(2)

        summary_file = os.path.join(self.results_path,
                                     f"clucmay72018_asset_summary_{timestamp}.csv")
        summary.to_csv(summary_file)
        print(f"   {summary_file}")


def main():
    """
    Main execution function
    """
    print("🚀 ClucMay72018 Strategy - Comprehensive Multi-Asset Testing")
    print("="*80)
    print("Ultra-selective momentum-reversal strategy")
    print("Testing across ALL available data sources with focus on 5-minute data")
    print("="*80)

    # Create tester instance
    tester = ComprehensiveMultiAssetTester(ClucMay72018Strategy)

    # Run comprehensive test
    tester.run_comprehensive_test()

    print("\n" + "="*80)
    print("✅ TESTING COMPLETE!")
    print("="*80)


if __name__ == "__main__":
    main()