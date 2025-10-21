# comprehensive_multi_asset_tester.py
"""
🚀 Bobby's Comprehensive Multi-Asset Testing Framework
=======================================================
Auto-discovers ALL available data sources and tests strategies across:
- Multiple cryptocurrencies (BTC, ETH, CRO, HBAR, LINK, XRP, etc.)
- Multiple timeframes (1m, 5m, 1h, 6h, 1d)
- Multiple data providers (Coinbase, Yahoo, CoinGecko, CryptoCompare, Hyperliquid)
"""

import pandas as pd
import numpy as np
import os
import glob
from pathlib import Path
from backtesting import Backtest
import warnings
import traceback
from datetime import datetime
import sys

# Add path for universal display module
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import enhanced_backtest_runner, create_data_source_info

warnings.filterwarnings('ignore')

# Import our strategy
from volatility_multi_asset_fixed import VolatilityMultiAssetStrategy

print("🌍 COMPREHENSIVE MULTI-ASSET TESTING FRAMEWORK 🌍")
print("=" * 100)
print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)


class ComprehensiveDataDiscovery:
    """Auto-discover and categorize all available data sources"""

    def __init__(self):
        self.base_path = '/Users/bobbyyo/Projects/algo-fun/data'
        self.data_sources = []

    def discover_all_sources(self):
        """Scan all directories for CSV data files"""
        print("\n🔍 DISCOVERING ALL DATA SOURCES...")
        print("-" * 80)

        # Define search patterns
        search_patterns = [
            (self.base_path + '/coinbase/*.csv', 'Coinbase'),
            (self.base_path + '/yahoo/*.csv', 'Yahoo'),
            (self.base_path + '/coingecko/*.csv', 'CoinGecko'),
            (self.base_path + '/cryptocompare/*.csv', 'CryptoCompare'),
            (self.base_path + '/hyperliquid/*.csv', 'Hyperliquid'),
            (self.base_path + '/coinmarketcap/*.csv', 'CoinMarketCap'),
            (self.base_path + '/*.csv', 'Root'),
        ]

        for pattern, provider in search_patterns:
            files = glob.glob(pattern)
            for file in files:
                # Extract metadata from filename
                filename = os.path.basename(file)
                asset, timeframe = self._parse_filename(filename)

                if asset and asset not in ['stocks', 'watchlist', 'top_coins', 'global_metrics']:
                    self.data_sources.append({
                        'path': file,
                        'provider': provider,
                        'asset': asset,
                        'timeframe': timeframe,
                        'filename': filename
                    })

        # Sort by asset and provider
        self.data_sources.sort(key=lambda x: (x['asset'], x['provider'], x['timeframe'] or 'z'))

        # Print discovery summary
        unique_assets = set(ds['asset'] for ds in self.data_sources)
        unique_providers = set(ds['provider'] for ds in self.data_sources)

        print(f"✅ Discovered {len(self.data_sources)} data sources")
        print(f"📊 Assets: {', '.join(sorted(unique_assets))}")
        print(f"🏢 Providers: {', '.join(sorted(unique_providers))}")

        return self.data_sources

    def _parse_filename(self, filename):
        """Extract asset and timeframe from filename"""
        filename_upper = filename.upper()

        # Asset mapping
        asset = None
        if 'BTC' in filename_upper or 'BITCOIN' in filename_upper:
            asset = 'BTC'
        elif 'ETH' in filename_upper or 'ETHEREUM' in filename_upper:
            asset = 'ETH'
        elif 'XRP' in filename_upper or 'RIPPLE' in filename_upper:
            asset = 'XRP'
        elif 'LINK' in filename_upper:
            asset = 'LINK'
        elif 'CRO' in filename_upper:
            asset = 'CRO'
        elif 'HBAR' in filename_upper:
            asset = 'HBAR'

        # Timeframe mapping
        timeframe = None
        if '-1m-' in filename or '_1m_' in filename:
            timeframe = '1m'
        elif '-5m-' in filename or '_5m_' in filename:
            timeframe = '5m'
        elif '-1h-' in filename or '_1h_' in filename or 'hour' in filename.lower():
            timeframe = '1h'
        elif '-6h-' in filename or '_6h_' in filename:
            timeframe = '6h'
        elif '-1d-' in filename or '_1d_' in filename or 'day' in filename.lower() or 'daily' in filename.lower():
            timeframe = '1d'
        elif 'yr' in filename.lower() or 'year' in filename.lower():
            timeframe = '1d'  # Assume daily for yearly data

        return asset, timeframe

    def get_by_asset(self, asset):
        """Get all data sources for a specific asset"""
        return [ds for ds in self.data_sources if ds['asset'] == asset]

    def get_by_provider(self, provider):
        """Get all data sources from a specific provider"""
        return [ds for ds in self.data_sources if ds['provider'] == provider]


class MultiAssetBacktester:
    """Run backtests across multiple assets and data sources"""

    def __init__(self, strategy_class):
        self.strategy_class = strategy_class
        self.results = []
        self.failed_tests = []

    def clean_and_fix_data(self, data):
        """Clean and fix OHLC data issues"""
        # Ensure positive prices
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in data.columns:
                data[col] = data[col].abs()
                # Replace zeros with NaN and forward fill
                data[col] = data[col].replace(0, np.nan).fillna(method='ffill')

        # Fix OHLC relationships
        data['High'] = data[['Open', 'High', 'Low', 'Close']].max(axis=1)
        data['Low'] = data[['Open', 'High', 'Low', 'Close']].min(axis=1)

        # Ensure volume is positive
        if 'Volume' in data.columns:
            data['Volume'] = data['Volume'].abs()
            # Handle zero volume
            data['Volume'] = data['Volume'].replace(0, data['Volume'].mean() * 0.1)

        # Remove any remaining NaN rows
        data = data.dropna()

        return data

    def load_data(self, source):
        """Load and prepare data from various formats"""
        try:
            # Try different date column names
            date_columns = ['datetime', 'Date', 'date', 'timestamp', 'time']

            for date_col in date_columns:
                try:
                    data = pd.read_csv(source['path'], parse_dates=[date_col], index_col=date_col)
                    break
                except:
                    continue
            else:
                # If no date column worked, try without parsing dates
                data = pd.read_csv(source['path'])
                # Look for a date-like column
                for col in data.columns:
                    if 'date' in col.lower() or 'time' in col.lower():
                        data[col] = pd.to_datetime(data[col])
                        data.set_index(col, inplace=True)
                        break

            # Standardize column names
            column_mapping = {
                'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume',
                'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close', 'Volume': 'Volume',
                'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'
            }

            data.rename(columns=column_mapping, inplace=True)

            # Ensure we have the required columns
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in data.columns for col in required_cols):
                # Try to create missing volume if other columns exist
                if 'Volume' not in data.columns and all(col in data.columns for col in ['Open', 'High', 'Low', 'Close']):
                    data['Volume'] = 1000000  # Default volume

            # Select only OHLCV columns
            data = data[required_cols]

            # Clean and sort
            data = data.dropna()
            data = data.sort_index()
            data = self.clean_and_fix_data(data)

            return data

        except Exception as e:
            print(f"     ❌ Error loading {source['filename']}: {e}")
            return None

    def run_single_test(self, source):
        """Run backtest on a single data source"""
        test_id = f"{source['asset']}-{source['timeframe'] or 'unknown'}-{source['provider']}"

        print(f"\n  📊 Testing: {test_id}")
        print(f"     File: {source['filename']}")

        try:
            # Load data
            data = self.load_data(source)

            if data is None or len(data) < 100:
                print(f"     ⚠️ Insufficient data (only {len(data) if data is not None else 0} bars)")
                self.failed_tests.append({
                    'test_id': test_id,
                    'reason': 'Insufficient data',
                    'source': source
                })
                return None

            print(f"     ✅ Data loaded: {len(data)} bars")
            print(f"     📅 Period: {data.index[0]} to {data.index[-1]}")

            # Adjust cash based on timeframe (more granular = more cash for proper position sizing)
            if source['timeframe'] in ['1m', '5m']:
                cash = 10000000  # 10M for minute data
            elif source['timeframe'] in ['1h', '6h']:
                cash = 5000000   # 5M for hourly data
            else:
                cash = 1000000   # 1M for daily data

            # Create data source info for native display
            data_source_info = create_data_source_info(
                file_path=source['filepath'],
                symbol=source['asset'],
                timeframe=source['timeframe'],
                provider=source['provider']
            )

            # Run backtest with FULL NATIVE RESULTS DISPLAY
            summary_stats, stats = enhanced_backtest_runner(
                data=data,
                strategy_class=self.strategy_class,
                data_source_info=data_source_info,
                strategy_name=f"{self.strategy_class.__name__}",
                cash=cash,
                commission=0.001,
                margin=0.1,  # Allow 10:1 leverage
                trade_on_close=True,
                exclusive_orders=True
            )

            # Store results
            result = {
                'test_id': test_id,
                'asset': source['asset'],
                'timeframe': source['timeframe'] or 'unknown',
                'provider': source['provider'],
                'bars': len(data),
                'start': str(data.index[0]),
                'end': str(data.index[-1]),
                'return_%': round(stats['Return [%]'], 2),
                'annual_return_%': round(stats.get('Return (Ann.) [%]', 0), 2),
                'sharpe': round(stats['Sharpe Ratio'], 3) if not np.isnan(stats['Sharpe Ratio']) else 0,
                'sortino': round(stats.get('Sortino Ratio', 0), 3) if not np.isnan(stats.get('Sortino Ratio', 0)) else 0,
                'max_dd_%': round(stats['Max. Drawdown [%]'], 2),
                'trades': stats['# Trades'],
                'win_rate_%': round(stats['Win Rate [%]'], 2) if stats['# Trades'] > 0 else 0,
                'avg_trade_%': round(stats.get('Avg. Trade [%]', 0), 3),
                'profit_factor': round(stats.get('Profit Factor', 0), 3) if not np.isnan(stats.get('Profit Factor', 0)) else 0,
                'exposure_%': round(stats.get('Exposure Time [%]', 0), 2),
                'file': source['filename']
            }

            self.results.append(result)

            # Print key metrics
            print(f"     📈 Return: {stats['Return [%]']:.2f}%")
            print(f"     📊 Sharpe: {stats['Sharpe Ratio']:.3f}" if not np.isnan(stats['Sharpe Ratio']) else "     📊 Sharpe: N/A")
            print(f"     📉 Max DD: {stats['Max. Drawdown [%]']:.2f}%")
            print(f"     🎯 Trades: {stats['# Trades']}")
            if stats['# Trades'] > 0:
                print(f"     ✅ Win Rate: {stats['Win Rate [%]']:.1f}%")

            return result

        except Exception as e:
            print(f"     ❌ Test failed: {str(e)[:100]}")
            self.failed_tests.append({
                'test_id': test_id,
                'reason': str(e)[:200],
                'source': source
            })
            return None

    def run_all_tests(self, data_sources):
        """Run tests on all data sources"""
        print("\n" + "=" * 100)
        print("🚀 RUNNING COMPREHENSIVE BACKTESTS")
        print("=" * 100)

        # Group by asset
        assets = {}
        for source in data_sources:
            if source['asset'] not in assets:
                assets[source['asset']] = []
            assets[source['asset']].append(source)

        # Test each asset
        for asset in sorted(assets.keys()):
            print(f"\n{'='*60}")
            print(f"💰 TESTING {asset}")
            print(f"{'='*60}")

            for source in assets[asset]:
                self.run_single_test(source)

        return self.results


class PerformanceAnalyzer:
    """Analyze and compare performance across assets"""

    def __init__(self, results, failed_tests):
        self.results = pd.DataFrame(results) if results else pd.DataFrame()
        self.failed_tests = failed_tests

    def generate_comprehensive_analysis(self):
        """Generate comprehensive cross-asset analysis"""

        if self.results.empty:
            print("\n❌ No successful test results to analyze")
            return

        print("\n" + "=" * 100)
        print("📊 COMPREHENSIVE PERFORMANCE ANALYSIS")
        print("=" * 100)

        # 1. Overall Summary
        print("\n🌍 OVERALL TEST SUMMARY")
        print("-" * 80)
        print(f"✅ Successful Tests: {len(self.results)}")
        print(f"❌ Failed Tests: {len(self.failed_tests)}")
        print(f"📊 Total Data Sources Tested: {len(self.results) + len(self.failed_tests)}")

        # 2. Asset Performance Ranking
        print("\n🏆 ASSET PERFORMANCE RANKING (by Sharpe Ratio)")
        print("-" * 80)

        asset_performance = self.results.groupby('asset').agg({
            'sharpe': 'mean',
            'return_%': 'mean',
            'max_dd_%': 'mean',
            'win_rate_%': 'mean',
            'trades': 'sum'
        }).round(2)

        asset_performance = asset_performance.sort_values('sharpe', ascending=False)

        for idx, (asset, row) in enumerate(asset_performance.iterrows(), 1):
            quality = "⭐⭐⭐" if row['sharpe'] > 1.0 else "⭐⭐" if row['sharpe'] > 0.5 else "⭐"
            print(f"{idx}. {asset} {quality}")
            print(f"   • Avg Sharpe: {row['sharpe']:.3f}")
            print(f"   • Avg Return: {row['return_%']:.2f}%")
            print(f"   • Avg Max DD: {row['max_dd_%']:.2f}%")
            print(f"   • Avg Win Rate: {row['win_rate_%']:.1f}%")
            print(f"   • Total Trades: {int(row['trades'])}")

        # 3. Provider Comparison
        print("\n🏢 PROVIDER COMPARISON")
        print("-" * 80)

        provider_performance = self.results.groupby('provider').agg({
            'sharpe': 'mean',
            'return_%': 'mean',
            'test_id': 'count'
        }).round(2)

        provider_performance.columns = ['avg_sharpe', 'avg_return_%', 'tests_count']
        provider_performance = provider_performance.sort_values('avg_sharpe', ascending=False)

        for provider, row in provider_performance.iterrows():
            print(f"• {provider}: Sharpe {row['avg_sharpe']:.3f}, Return {row['avg_return_%']:.2f}%, Tests {int(row['tests_count'])}")

        # 4. Timeframe Analysis
        print("\n⏰ TIMEFRAME EFFECTIVENESS")
        print("-" * 80)

        timeframe_performance = self.results[self.results['timeframe'] != 'unknown'].groupby('timeframe').agg({
            'sharpe': 'mean',
            'return_%': 'mean',
            'trades': 'mean',
            'win_rate_%': 'mean'
        }).round(2)

        if not timeframe_performance.empty:
            timeframe_performance = timeframe_performance.sort_values('sharpe', ascending=False)

            for timeframe, row in timeframe_performance.iterrows():
                effectiveness = "EXCELLENT" if row['sharpe'] > 0.8 else "GOOD" if row['sharpe'] > 0.3 else "POOR"
                print(f"• {timeframe}: {effectiveness}")
                print(f"  Sharpe {row['sharpe']:.3f}, Return {row['return_%']:.2f}%, Trades {row['trades']:.0f}, Win {row['win_rate_%']:.1f}%")

        # 5. Top Performers
        print("\n🥇 TOP 10 INDIVIDUAL PERFORMERS")
        print("-" * 80)

        top_performers = self.results.nlargest(10, 'sharpe')[['test_id', 'sharpe', 'return_%', 'max_dd_%', 'trades']]

        for idx, row in top_performers.iterrows():
            print(f"• {row['test_id']}")
            print(f"  Sharpe: {row['sharpe']:.3f}, Return: {row['return_%']:.2f}%, Max DD: {row['max_dd_%']:.2f}%, Trades: {row['trades']}")

        # 6. Cross-Asset Insights
        print("\n💡 CROSS-ASSET INSIGHTS")
        print("-" * 80)

        # Best asset for the strategy
        best_asset = asset_performance.index[0]
        print(f"✅ Best Asset: {best_asset} performs best with this volatility strategy")

        # Asset volatility correlation
        high_sharpe_assets = asset_performance[asset_performance['sharpe'] > 0.5].index.tolist()
        if high_sharpe_assets:
            print(f"✅ High Performance Assets: {', '.join(high_sharpe_assets)} show consistent profitability")

        # Diversification potential
        if len(asset_performance) >= 3:
            top_3_assets = asset_performance.head(3).index.tolist()
            print(f"✅ Diversification: Consider portfolio with {', '.join(top_3_assets)} for balanced exposure")

        # 7. Failed Tests Analysis
        if self.failed_tests:
            print("\n⚠️ FAILED TESTS ANALYSIS")
            print("-" * 80)

            failure_reasons = {}
            for failed in self.failed_tests:
                reason = failed['reason'].split(':')[0] if ':' in failed['reason'] else failed['reason'][:50]
                if reason not in failure_reasons:
                    failure_reasons[reason] = []
                failure_reasons[reason].append(failed['test_id'])

            for reason, tests in failure_reasons.items():
                print(f"• {reason}: {len(tests)} tests")
                if len(tests) <= 3:
                    for test in tests:
                        print(f"  - {test}")

        # 8. Strategy Recommendations
        print("\n🎯 STRATEGY OPTIMIZATION RECOMMENDATIONS")
        print("-" * 80)

        avg_sharpe = self.results['sharpe'].mean()
        avg_win_rate = self.results['win_rate_%'].mean()
        avg_trades = self.results['trades'].mean()

        if avg_sharpe < 0.5:
            print("⚠️ Overall Sharpe ratio is low - consider:")
            print("   • Adjusting ATR periods for better volatility detection")
            print("   • Tightening entry filters")

        if avg_win_rate < 40:
            print("⚠️ Win rate is below 40% - consider:")
            print("   • Improving entry timing with additional confirmation")
            print("   • Adjusting stop loss levels")

        if avg_trades < 30:
            print("⚠️ Low trade frequency detected - consider:")
            print("   • Relaxing entry conditions slightly")
            print("   • Testing on shorter timeframes")

        # Asset-specific recommendations
        print("\n📊 ASSET-SPECIFIC RECOMMENDATIONS:")
        for asset in asset_performance.index[:5]:  # Top 5 assets
            asset_data = self.results[self.results['asset'] == asset]
            best_timeframe = asset_data.loc[asset_data['sharpe'].idxmax()]['timeframe'] if not asset_data.empty else 'unknown'
            best_provider = asset_data.loc[asset_data['sharpe'].idxmax()]['provider'] if not asset_data.empty else 'unknown'

            print(f"\n• {asset}:")
            print(f"  Best Timeframe: {best_timeframe}")
            print(f"  Best Provider: {best_provider}")
            print(f"  Avg Sharpe: {asset_performance.loc[asset, 'sharpe']:.3f}")

            if asset_performance.loc[asset, 'sharpe'] > 1.0:
                print(f"  ✅ PRODUCTION READY - Consider live deployment")
            elif asset_performance.loc[asset, 'sharpe'] > 0.5:
                print(f"  ⚠️ PROMISING - Needs parameter optimization")
            else:
                print(f"  ❌ NEEDS WORK - Requires strategy adjustment")

    def save_results(self):
        """Save comprehensive results to CSV"""
        if not self.results.empty:
            # Save detailed results
            results_path = '/Users/bobbyyo/Projects/algo-fun/strategies/analysis/results/comprehensive_multi_asset_results.csv'
            self.results.to_csv(results_path, index=False)
            print(f"\n📄 Detailed results saved to: {results_path}")

            # Save summary by asset
            asset_summary = self.results.groupby('asset').agg({
                'sharpe': ['mean', 'max'],
                'return_%': ['mean', 'max'],
                'max_dd_%': ['mean', 'min'],
                'trades': 'sum',
                'win_rate_%': 'mean'
            }).round(2)

            summary_path = '/Users/bobbyyo/Projects/algo-fun/strategies/analysis/results/asset_summary.csv'
            asset_summary.to_csv(summary_path)
            print(f"📄 Asset summary saved to: {summary_path}")

            # Save failed tests
            if self.failed_tests:
                failed_df = pd.DataFrame(self.failed_tests)
                failed_path = '/Users/bobbyyo/Projects/algo-fun/strategies/analysis/results/failed_tests.csv'
                failed_df.to_csv(failed_path, index=False)
                print(f"📄 Failed tests saved to: {failed_path}")


def main():
    """Main execution function"""

    # Step 1: Discover all data sources
    discoverer = ComprehensiveDataDiscovery()
    all_sources = discoverer.discover_all_sources()

    if not all_sources:
        print("\n❌ No data sources found!")
        return

    # Step 2: Run comprehensive backtests
    backtester = MultiAssetBacktester(VolatilityMultiAssetStrategy)
    results = backtester.run_all_tests(all_sources)

    # Step 3: Analyze and report results
    analyzer = PerformanceAnalyzer(results, backtester.failed_tests)
    analyzer.generate_comprehensive_analysis()
    analyzer.save_results()

    # Final summary
    print("\n" + "=" * 100)
    print("✅ COMPREHENSIVE MULTI-ASSET TESTING COMPLETE!")
    print("=" * 100)
    print(f"📊 Tested {len(results)} combinations successfully")
    print(f"🌍 Covered {len(set(r['asset'] for r in results))} different assets")
    print(f"⏰ Tested {len(set(r['timeframe'] for r in results if r['timeframe'] != 'unknown'))} different timeframes")
    print(f"🏢 Used {len(set(r['provider'] for r in results))} different data providers")

    return results, backtester.failed_tests


if __name__ == "__main__":
    results, failed = main()