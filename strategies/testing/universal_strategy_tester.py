"""
🚀 Universal Strategy Tester - Minimal Testing Framework
======================================================
Streamlined testing framework for Bobby's core strategies with full stats display.
Supports SMA, RSI, and Breakout strategies with auto-discovery of data sources.

🌟 Features:
    - Auto-discovers all available data in /data directory
    - Tests strategies on ALL cryptocurrencies automatically
    - Uses enhanced_backtest_runner for complete stats display
    - Generates performance rankings across assets
    - Integrated data quality validation

💫 Usage:
    python universal_strategy_tester.py SMAStrategy
    python universal_strategy_tester.py RSIMeanReversionStrategy
    python universal_strategy_tester.py BreakoutMomentumStrategy

🔧 Bobby's Framework Integration:
    - Uses @trading_functions for data validation
    - Displays complete backtesting.py stats (never summarized)
    - Multi-asset testing with asset performance ranking
    - Production readiness validation
"""

import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path
from backtesting import Backtest
import warnings
from datetime import datetime

# 🚀 Import Bobby's modern trading functions
try:
    from trading_functions import (
        DataQualityValidator,
        validate_data_source_quality,
        production_readiness_check
    )
    TRADING_FUNCTIONS_AVAILABLE = True
    print("✅ @trading_functions library loaded successfully")
except ImportError as e:
    TRADING_FUNCTIONS_AVAILABLE = False
    print(f"⚠️ @trading_functions not available: {e}")

# 📁 Add core strategies to path
sys.path.append(str(Path(__file__).parent.parent / "core_strategies"))

warnings.filterwarnings('ignore')

class UniversalStrategyTester:
    """
    🎯 Universal Strategy Testing Framework

    Minimal, functional testing system that:
    - Auto-discovers data sources
    - Tests on multiple assets
    - Displays complete backtesting stats
    - Generates performance rankings
    """

    def __init__(self, data_directory="/Users/bobbyyo/Projects/algo-fun/data"):
        self.data_directory = data_directory
        self.results = []
        self.data_sources = self._discover_data_sources()

    def _discover_data_sources(self):
        """🔍 Auto-discover all available data sources"""
        data_sources = []
        data_dir = Path(self.data_directory)

        if not data_dir.exists():
            print(f"⚠️ Data directory not found: {self.data_directory}")
            return data_sources

        print(f"🔍 Scanning for data in: {self.data_directory}")

        # Scan all subdirectories for CSV files
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith('.csv') and not file.startswith('.'):
                    file_path = os.path.join(root, file)

                    # Extract symbol and source from path/filename
                    symbol = self._extract_symbol(file)
                    source = self._extract_source(root, file)

                    data_sources.append({
                        'name': f"{symbol}-{source}",
                        'path': file_path,
                        'symbol': symbol,
                        'source': source
                    })

        print(f"📊 Found {len(data_sources)} data sources")
        for source in data_sources[:5]:  # Show first 5
            print(f"   • {source['name']}: {source['path']}")
        if len(data_sources) > 5:
            print(f"   • ... and {len(data_sources) - 5} more")

        return data_sources

    def _extract_symbol(self, filename):
        """🔍 Extract cryptocurrency symbol from filename"""
        filename_upper = filename.upper()

        symbols = ['BTC', 'ETH', 'CRO', 'HBAR', 'LINK', 'XRP', 'LTC', 'ADA', 'DOT', 'SOL']
        for symbol in symbols:
            if symbol in filename_upper:
                return symbol

        # Default extraction from filename
        return filename.split('-')[0].split('_')[0].replace('.csv', '')

    def _extract_source(self, root_path, filename):
        """🔍 Extract data source from path"""
        root_lower = root_path.lower()
        filename_lower = filename.lower()

        if 'yahoo' in root_lower or 'yahoo' in filename_lower:
            return 'Yahoo'
        elif 'coinbase' in root_lower or 'coinbase' in filename_lower:
            return 'Coinbase'
        elif 'coingecko' in root_lower or 'coingecko' in filename_lower:
            return 'CoinGecko'
        elif 'hyperliquid' in root_lower or 'hyperliquid' in filename_lower:
            return 'Hyperliquid'
        elif 'cmc' in filename_lower or 'coinmarketcap' in root_lower:
            return 'CoinMarketCap'
        elif 'cryptocompare' in root_lower or 'cc' in filename_lower:
            return 'CryptoCompare'
        else:
            return 'Unknown'

    def enhanced_backtest_runner(self, data_df, strategy_class, symbol, source):
        """
        🚨 MANDATORY: Enhanced backtest runner with complete stats display

        This function ensures FULL backtesting.py stats output is displayed
        following Bobby's requirements - NEVER summarize or truncate results
        """
        try:
            print(f"\n{'='*80}")
            print(f"🎯 RUNNING ENHANCED BACKTEST: {strategy_class.__name__} on {symbol}-{source}")
            print(f"{'='*80}")

            # 🛡️ Data quality validation
            if TRADING_FUNCTIONS_AVAILABLE:
                try:
                    validation_result = validate_data_source_quality(data_df)
                    print(f"📊 Data Quality Score: {validation_result.quality_score}")

                    if validation_result.quality_score < 75:
                        print(f"⚠️ Warning: Data quality below threshold ({validation_result.quality_score} < 75)")
                except Exception as e:
                    print(f"⚠️ Data validation error: {e}")

            # 📊 Data summary
            print(f"📁 Data Range: {data_df.index[0]} to {data_df.index[-1]}")
            print(f"📈 Total Bars: {len(data_df)}")
            print(f"💰 Price Range: ${data_df['Close'].min():.2f} - ${data_df['Close'].max():.2f}")

            # 🚀 Initialize and run backtest
            bt = Backtest(data_df, strategy_class, cash=10000, commission=0.002)
            stats = bt.run()

            # 🚨 CRITICAL: Display COMPLETE backtesting.py stats - NEVER SUMMARIZE
            print(f"\n🎯 COMPLETE BACKTESTING RESULTS FOR {symbol}-{source}")
            print(f"{'='*80}")
            print("📊 FULL NATIVE BACKTESTING.PY OUTPUT:")
            print("-" * 80)
            print(stats)  # Complete stats output - DO NOT modify or summarize
            print("-" * 80)

            # 📈 Show plot
            try:
                bt.plot()
                print("📈 Strategy plot displayed")
            except Exception as e:
                print(f"⚠️ Plotting error: {e}")

            # 📝 Extract key metrics for ranking
            result_summary = {
                'symbol': symbol,
                'source': source,
                'name': f"{symbol}-{source}",
                'strategy': strategy_class.__name__,
                'total_return_pct': float(stats['Return [%]']),
                'sharpe_ratio': float(stats['Sharpe Ratio']),
                'max_drawdown_pct': float(stats['Max. Drawdown [%]']),
                'win_rate_pct': float(stats['Win Rate [%]']),
                'total_trades': int(stats['# Trades']),
                'profit_factor': float(stats.get('Profit Factor', 0)),
                'sortino_ratio': float(stats.get('Sortino Ratio', 0)),
                'calmar_ratio': float(stats.get('Calmar Ratio', 0)),
                'data_bars': len(data_df),
                'data_quality': validation_result.quality_score if TRADING_FUNCTIONS_AVAILABLE else 'N/A'
            }

            print(f"\n✅ {symbol}-{source} backtest completed successfully")
            return result_summary, stats

        except Exception as e:
            print(f"❌ Error in enhanced backtest for {symbol}-{source}: {e}")
            return None, None

    def test_strategy_on_all_assets(self, strategy_class):
        """
        🧪 Test strategy on ALL available assets with complete results display

        Args:
            strategy_class: Strategy class to test (SMAStrategy, RSIMeanReversionStrategy, etc.)
        """
        print(f"\n🚀 UNIVERSAL STRATEGY TESTING: {strategy_class.__name__}")
        print(f"{'='*80}")
        print(f"📊 Testing on {len(self.data_sources)} data sources")

        self.results = []
        successful_tests = 0

        for i, data_source in enumerate(self.data_sources, 1):
            try:
                print(f"\n📍 Progress: {i}/{len(self.data_sources)} - Testing {data_source['name']}")

                # 📊 Load data
                df = pd.read_csv(data_source['path'])

                # 🔧 Standardize data format
                df = self._prepare_data(df)

                if df is None or len(df) < 100:
                    print(f"⚠️ Insufficient data for {data_source['name']} (bars: {len(df) if df is not None else 0})")
                    continue

                # 🚀 Run enhanced backtest with complete stats display
                result_summary, full_stats = self.enhanced_backtest_runner(
                    df, strategy_class, data_source['symbol'], data_source['source']
                )

                if result_summary:
                    self.results.append(result_summary)
                    successful_tests += 1
                else:
                    print(f"❌ Failed to get results for {data_source['name']}")

            except Exception as e:
                print(f"❌ Error testing {data_source['name']}: {e}")

        print(f"\n🎯 TESTING SUMMARY")
        print(f"{'='*50}")
        print(f"✅ Successful tests: {successful_tests}/{len(self.data_sources)}")
        print(f"❌ Failed tests: {len(self.data_sources) - successful_tests}")

        # 🏆 Generate performance rankings
        if self.results:
            self._generate_performance_rankings(strategy_class.__name__)
            self._save_results_to_csv(strategy_class.__name__)

        return self.results

    def _prepare_data(self, df):
        """🔧 Standardize data format for backtesting"""
        try:
            # Handle different date column names
            date_columns = ['Datetime', 'Date', 'timestamp', 'time']
            date_col = None

            for col in date_columns:
                if col in df.columns:
                    date_col = col
                    break

            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
                df.set_index(date_col, inplace=True)

            # Ensure required OHLCV columns exist
            required_cols = ['Open', 'High', 'Low', 'Close']
            for col in required_cols:
                if col not in df.columns:
                    # Try different case variations
                    for alt_col in [col.lower(), col.upper()]:
                        if alt_col in df.columns:
                            df[col] = df[alt_col]
                            break
                    else:
                        raise ValueError(f"Missing required column: {col}")

            # Add Volume if missing
            if 'Volume' not in df.columns:
                df['Volume'] = 1000  # Default volume

            # Remove any non-numeric data
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Drop rows with NaN values
            df = df.dropna()

            return df

        except Exception as e:
            print(f"⚠️ Data preparation error: {e}")
            return None

    def _generate_performance_rankings(self, strategy_name):
        """🏆 Generate asset performance rankings"""
        if not self.results:
            return

        print(f"\n🏆 PERFORMANCE RANKINGS: {strategy_name}")
        print(f"{'='*80}")

        # Sort by Sharpe Ratio (best risk-adjusted returns)
        sorted_results = sorted(self.results, key=lambda x: x['sharpe_ratio'], reverse=True)

        print(f"📊 TOP PERFORMERS (by Sharpe Ratio):")
        print("-" * 80)
        print(f"{'Rank':<4} {'Asset':<15} {'Return%':<10} {'Sharpe':<8} {'MaxDD%':<8} {'Trades':<7} {'WinRate%':<9}")
        print("-" * 80)

        for i, result in enumerate(sorted_results[:10], 1):  # Top 10
            print(f"{i:<4} {result['name']:<15} "
                  f"{result['total_return_pct']:<10.1f} "
                  f"{result['sharpe_ratio']:<8.2f} "
                  f"{result['max_drawdown_pct']:<8.1f} "
                  f"{result['total_trades']:<7} "
                  f"{result['win_rate_pct']:<9.1f}")

        # 📊 Asset-specific analysis
        print(f"\n📈 CRYPTOCURRENCY PERFORMANCE ANALYSIS:")
        print("-" * 50)

        symbols = list(set([r['symbol'] for r in self.results]))
        for symbol in sorted(symbols):
            symbol_results = [r for r in self.results if r['symbol'] == symbol]
            if symbol_results:
                avg_sharpe = sum([r['sharpe_ratio'] for r in symbol_results]) / len(symbol_results)
                best_result = max(symbol_results, key=lambda x: x['sharpe_ratio'])
                print(f"{symbol:<6}: Avg Sharpe {avg_sharpe:.2f}, Best: {best_result['source']} ({best_result['sharpe_ratio']:.2f})")

    def _save_results_to_csv(self, strategy_name):
        """💾 Save results to CSV file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{strategy_name}_results_{timestamp}.csv"
            results_dir = Path(self.data_directory).parent / "strategies" / "results"
            results_dir.mkdir(exist_ok=True)

            filepath = results_dir / filename

            results_df = pd.DataFrame(self.results)
            results_df.to_csv(filepath, index=False)

            print(f"\n💾 Results saved to: {filepath}")
            return str(filepath)

        except Exception as e:
            print(f"⚠️ Error saving results: {e}")
            return None


def main():
    """🎯 Main testing function"""
    print("🚀 Universal Strategy Tester - Bobby's Minimal Trading Framework")
    print("=" * 80)

    # 🛡️ Production readiness check
    if TRADING_FUNCTIONS_AVAILABLE:
        try:
            readiness = production_readiness_check()
            if readiness.get('config_valid'):
                print("✅ Trading functions ready")
            else:
                print("⚠️ Some trading functions may not be available")
        except Exception as e:
            print(f"⚠️ Readiness check error: {e}")

    # Get strategy class from command line argument
    if len(sys.argv) < 2:
        print("\n📝 Usage: python universal_strategy_tester.py <StrategyClassName>")
        print("\n🎯 Available strategies:")
        print("   • SMAStrategy")
        print("   • RSIMeanReversionStrategy")
        print("   • BreakoutMomentumStrategy")
        return

    strategy_name = sys.argv[1]

    # Import the strategy class
    try:
        if strategy_name == "SMAStrategy":
            from sma_crossover_strategy import SMAStrategy
            strategy_class = SMAStrategy
        elif strategy_name == "RSIMeanReversionStrategy":
            from rsi_mean_reversion_strategy import RSIMeanReversionStrategy
            strategy_class = RSIMeanReversionStrategy
        elif strategy_name == "BreakoutMomentumStrategy":
            from breakout_momentum_strategy import BreakoutMomentumStrategy
            strategy_class = BreakoutMomentumStrategy
        else:
            print(f"❌ Unknown strategy: {strategy_name}")
            return

        print(f"✅ Strategy loaded: {strategy_class.__name__}")

    except ImportError as e:
        print(f"❌ Error importing strategy {strategy_name}: {e}")
        return

    # 🧪 Initialize tester and run tests
    tester = UniversalStrategyTester()
    results = tester.test_strategy_on_all_assets(strategy_class)

    print(f"\n🌙💫🚀 Universal testing complete for {strategy_class.__name__}!")
    print(f"📊 Total results: {len(results)}")


if __name__ == "__main__":
    main()