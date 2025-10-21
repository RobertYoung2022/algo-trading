"""
🌍 Multi-Asset Fibonacci Scalping Strategy Tester 🌍
=====================================================
Comprehensive testing framework for the 1-minute Fibonacci Scalping Strategy
across all available cryptocurrencies and timeframes.

This script will:
1. Auto-discover all available data sources
2. Validate data quality before testing
3. Run strategy on multiple assets
4. Compare performance across different data providers
5. Generate comprehensive analysis and rankings

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from pathlib import Path
import glob
from datetime import datetime
from backtesting import Backtest
from fibonacci_scalping_1m_strategy import FibonacciScalpingStrategy
import sys
import os

# Add the analysis directory to path for universal display module
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import enhanced_backtest_runner, create_data_source_info

import warnings
warnings.filterwarnings('ignore')


class MultiAssetFibonacciTester:
    """
    🚀 Comprehensive Multi-Asset Testing Framework 🚀
    """

    def __init__(self, base_dir="/Users/bobbyyo/Projects/algo-fun"):
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / "data"
        self.results_dir = self.base_dir / "strategies" / "results"
        self.results_dir.mkdir(exist_ok=True)

        # Data quality threshold
        self.min_quality_score = 75

        # Strategy parameters
        self.initial_cash = 10000
        self.commission = 0.002

        # Results storage
        self.all_results = []

    def discover_data_sources(self):
        """
        🔍 Auto-discover all available 1-minute and 5-minute data sources 🔍
        """
        print("\n" + "="*80)
        print("🔍 DISCOVERING DATA SOURCES 🔍")
        print("="*80)

        data_sources = []

        # Search patterns for ALL timeframes including multi-year data
        patterns = [
            "**/*1m*.csv",
            "**/*5m*.csv",
            "**/*6h*.csv",
            "**/*1d*.csv",
            "**/*10yr*.csv",
            "**/*20yr*.csv",
            "**/*1min*.csv",
            "**/*5min*.csv",
            "**/*1000wks*.csv",  # Long term daily data
            "**/*200wks*.csv",   # Long term 6h data
            "**/*yahoo*.csv"     # Yahoo multi-year data
        ]

        for pattern in patterns:
            files = list(self.data_dir.glob(pattern))
            for file in files:
                # Extract metadata from filename
                filename = file.name
                parts = filename.split('-')

                if len(parts) >= 2:
                    symbol = parts[0]
                    timeframe = parts[1] if len(parts) > 1 else 'unknown'

                    # Determine provider from path
                    provider = 'unknown'
                    if 'coinbase' in str(file).lower():
                        provider = 'coinbase'
                    elif 'yahoo' in str(file).lower():
                        provider = 'yahoo'
                    elif 'coingecko' in str(file).lower():
                        provider = 'coingecko'
                    elif 'hyperliquid' in str(file).lower():
                        provider = 'hyperliquid'
                    elif 'cryptocompare' in str(file).lower():
                        provider = 'cryptocompare'

                    data_sources.append({
                        'path': str(file),
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'provider': provider,
                        'filename': filename
                    })

        print(f"✅ Found {len(data_sources)} potential data sources")

        # Group by symbol
        symbols = set([d['symbol'] for d in data_sources])
        print(f"\n📊 Available symbols: {', '.join(sorted(symbols))}")

        # Group by provider
        providers = set([d['provider'] for d in data_sources])
        print(f"🏢 Data providers: {', '.join(sorted(providers))}")

        return data_sources

    def validate_data_quality(self, df, source_info):
        """
        ✅ Validate data quality and calculate quality score ✅
        """
        quality_checks = {
            'has_required_columns': 0,
            'no_missing_values': 0,
            'sufficient_data': 0,
            'valid_ohlc': 0,
            'positive_volume': 0,
            'chronological': 0,
            'no_duplicates': 0,
            'reasonable_spreads': 0
        }

        # Check 1: Required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if all(col in df.columns for col in required_cols):
            quality_checks['has_required_columns'] = 100

        # Check 2: Missing values
        missing_pct = df[required_cols].isnull().sum().sum() / (len(df) * len(required_cols)) * 100
        quality_checks['no_missing_values'] = max(0, 100 - missing_pct * 10)

        # Check 3: Sufficient data (at least 1000 bars for 1m, 500 for 5m)
        min_bars = 1000 if '1m' in source_info['timeframe'] else 500
        if len(df) >= min_bars:
            quality_checks['sufficient_data'] = 100
        else:
            quality_checks['sufficient_data'] = (len(df) / min_bars) * 100

        # Check 4: Valid OHLC relationships
        valid_ohlc = ((df['High'] >= df['Low']) &
                      (df['High'] >= df['Open']) &
                      (df['High'] >= df['Close']) &
                      (df['Low'] <= df['Open']) &
                      (df['Low'] <= df['Close'])).mean() * 100
        quality_checks['valid_ohlc'] = valid_ohlc

        # Check 5: Positive volume
        quality_checks['positive_volume'] = (df['Volume'] > 0).mean() * 100

        # Check 6: Chronological order
        if df.index.is_monotonic_increasing:
            quality_checks['chronological'] = 100

        # Check 7: No duplicate timestamps
        duplicate_pct = df.index.duplicated().sum() / len(df) * 100
        quality_checks['no_duplicates'] = max(0, 100 - duplicate_pct * 10)

        # Check 8: Reasonable spreads
        spreads = (df['High'] - df['Low']) / df['Close'] * 100
        unreasonable_spreads = (spreads > 20).mean() * 100  # More than 20% spread is unusual
        quality_checks['reasonable_spreads'] = max(0, 100 - unreasonable_spreads)

        # Calculate overall quality score
        quality_score = np.mean(list(quality_checks.values()))

        return quality_score, quality_checks

    def load_and_prepare_data(self, data_path):
        """
        📂 Load and prepare data for backtesting 📂
        """
        try:
            # Load CSV
            df = pd.read_csv(data_path)

            # Handle different column naming conventions
            column_mapping = {
                'timestamp': 'timestamp',
                'Timestamp': 'timestamp',
                'Date': 'timestamp',
                'date': 'timestamp',
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }

            df.rename(columns=column_mapping, inplace=True)

            # Parse timestamp
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)

            # Ensure proper column names
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in required_cols:
                if col not in df.columns:
                    return None

            # Remove any rows with NaN values
            df = df.dropna()

            # Sort by index
            df = df.sort_index()

            return df

        except Exception as e:
            print(f"❌ Error loading {data_path}: {e}")
            return None

    def run_single_backtest(self, df, source_info):
        """
        🎯 Run backtest on a single data source with FULL NATIVE RESULTS DISPLAY 🎯
        """
        try:
            # Use the enhanced backtest runner with mandatory native results display
            summary_stats, full_stats = enhanced_backtest_runner(
                data=df,
                strategy_class=FibonacciScalpingStrategy,
                data_source_info=source_info,
                strategy_name="Fibonacci Scalping Strategy",
                cash=self.initial_cash,
                commission=self.commission,
                exclusive_orders=True,
                trade_on_close=False
            )

            # Convert summary_stats to legacy format for compatibility
            result = {
                'symbol': source_info['symbol'],
                'timeframe': source_info['timeframe'],
                'provider': source_info['provider'],
                'data_points': len(df),
                'date_start': df.index[0],
                'date_end': df.index[-1],
                'return_pct': summary_stats['Return_%'],
                'buy_hold_return': full_stats.get('Buy & Hold Return [%]', 0),
                'max_drawdown': summary_stats['Max_DD_%'],
                'num_trades': summary_stats['Trades'],
                'win_rate': summary_stats['Win_Rate_%'],
                'sharpe_ratio': summary_stats['Sharpe'],
                'sortino_ratio': summary_stats['Sortino'],
                'calmar_ratio': full_stats.get('Calmar Ratio', 0),
                'profit_factor': summary_stats['Profit_Factor'],
                'expectancy': full_stats.get('Expectancy [%]', 0),
                'sqn': full_stats.get('SQN', 0),
                'exposure_time': summary_stats['Exposure_%'],
                'avg_trade': summary_stats['Avg_Trade_%'],
                'best_trade': summary_stats['Best_Trade_%'],
                'worst_trade': summary_stats['Worst_Trade_%']
            }

            return result, full_stats

        except Exception as e:
            print(f"❌ Backtest error for {source_info['symbol']} ({source_info['provider']}): {e}")
            return None, None

    def run_comprehensive_test(self):
        """
        🌍 Run comprehensive multi-asset testing 🌍
        """
        print("\n" + "="*80)
        print("🚀 STARTING COMPREHENSIVE MULTI-ASSET TESTING 🚀")
        print("="*80)

        # Discover data sources
        data_sources = self.discover_data_sources()

        # Filter for 1m and 5m timeframes
        filtered_sources = [d for d in data_sources if d['timeframe'] in ['1m', '5m']]

        if not filtered_sources:
            print("❌ No suitable 1m or 5m data sources found!")
            return

        print(f"\n📊 Testing {len(filtered_sources)} data sources...")

        # Test each data source
        for i, source in enumerate(filtered_sources, 1):
            print(f"\n[{i}/{len(filtered_sources)}] Testing {source['symbol']} ({source['timeframe']}) from {source['provider']}...")

            # Load data
            df = self.load_and_prepare_data(source['path'])
            if df is None:
                print(f"   ⚠️ Failed to load data")
                continue

            # Validate data quality
            quality_score, quality_details = self.validate_data_quality(df, source)
            print(f"   📊 Data quality score: {quality_score:.1f}/100")

            if quality_score < self.min_quality_score:
                print(f"   ⚠️ Data quality below threshold ({self.min_quality_score}), skipping...")
                continue

            # Run backtest
            result, full_stats = self.run_single_backtest(df, source)

            if result:
                result['quality_score'] = quality_score
                self.all_results.append(result)
                print(f"   ✅ Backtest complete: Return={result['return_pct']:.2f}%, Sharpe={result['sharpe_ratio']:.2f}")
            else:
                print(f"   ❌ Backtest failed")

        # Generate comprehensive analysis
        self.generate_analysis()

    def generate_analysis(self):
        """
        📊 Generate comprehensive analysis and rankings 📊
        """
        if not self.all_results:
            print("\n❌ No successful backtests to analyze!")
            return

        print("\n" + "="*80)
        print("📊 COMPREHENSIVE ANALYSIS RESULTS 📊")
        print("="*80)

        # Convert results to DataFrame
        results_df = pd.DataFrame(self.all_results)

        # Save results to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.results_dir / f"fibonacci_scalping_results_{timestamp}.csv"
        results_df.to_csv(results_file, index=False)
        print(f"\n💾 Results saved to: {results_file}")

        # 1. Overall Performance Summary
        print("\n" + "-"*60)
        print("📈 OVERALL PERFORMANCE SUMMARY")
        print("-"*60)
        print(f"Total assets tested: {len(results_df)}")
        print(f"Average return: {results_df['return_pct'].mean():.2f}%")
        print(f"Average Sharpe ratio: {results_df['sharpe_ratio'].mean():.2f}")
        print(f"Average win rate: {results_df['win_rate'].mean():.2f}%")
        print(f"Average max drawdown: {results_df['max_drawdown'].mean():.2f}%")

        # 2. Top Performers by Return
        print("\n" + "-"*60)
        print("🏆 TOP 5 PERFORMERS BY RETURN")
        print("-"*60)
        top_return = results_df.nlargest(5, 'return_pct')[
            ['symbol', 'timeframe', 'provider', 'return_pct', 'sharpe_ratio', 'num_trades']
        ]
        for idx, row in top_return.iterrows():
            print(f"{row['symbol']} ({row['timeframe']}, {row['provider']}): "
                  f"Return={row['return_pct']:.2f}%, Sharpe={row['sharpe_ratio']:.2f}, Trades={row['num_trades']:.0f}")

        # 3. Top Performers by Sharpe Ratio
        print("\n" + "-"*60)
        print("🎯 TOP 5 PERFORMERS BY SHARPE RATIO")
        print("-"*60)
        top_sharpe = results_df.nlargest(5, 'sharpe_ratio')[
            ['symbol', 'timeframe', 'provider', 'sharpe_ratio', 'return_pct', 'win_rate']
        ]
        for idx, row in top_sharpe.iterrows():
            print(f"{row['symbol']} ({row['timeframe']}, {row['provider']}): "
                  f"Sharpe={row['sharpe_ratio']:.2f}, Return={row['return_pct']:.2f}%, Win Rate={row['win_rate']:.2f}%")

        # 4. Asset Performance Ranking
        print("\n" + "-"*60)
        print("📊 ASSET PERFORMANCE RANKING")
        print("-"*60)
        asset_summary = results_df.groupby('symbol').agg({
            'return_pct': 'mean',
            'sharpe_ratio': 'mean',
            'win_rate': 'mean',
            'num_trades': 'mean',
            'max_drawdown': 'mean'
        }).round(2).sort_values('sharpe_ratio', ascending=False)

        for symbol, metrics in asset_summary.iterrows():
            print(f"{symbol}: Sharpe={metrics['sharpe_ratio']:.2f}, "
                  f"Return={metrics['return_pct']:.2f}%, "
                  f"Win Rate={metrics['win_rate']:.2f}%, "
                  f"Avg Trades={metrics['num_trades']:.0f}")

        # 5. Timeframe Comparison
        print("\n" + "-"*60)
        print("⏰ TIMEFRAME COMPARISON")
        print("-"*60)
        timeframe_summary = results_df.groupby('timeframe').agg({
            'return_pct': 'mean',
            'sharpe_ratio': 'mean',
            'win_rate': 'mean',
            'num_trades': 'sum'
        }).round(2)

        for timeframe, metrics in timeframe_summary.iterrows():
            print(f"{timeframe}: Avg Return={metrics['return_pct']:.2f}%, "
                  f"Avg Sharpe={metrics['sharpe_ratio']:.2f}, "
                  f"Avg Win Rate={metrics['win_rate']:.2f}%, "
                  f"Total Trades={metrics['num_trades']:.0f}")

        # 6. Provider Reliability
        if len(results_df['provider'].unique()) > 1:
            print("\n" + "-"*60)
            print("🏢 PROVIDER COMPARISON")
            print("-"*60)
            provider_summary = results_df.groupby('provider').agg({
                'quality_score': 'mean',
                'return_pct': 'mean',
                'sharpe_ratio': 'mean'
            }).round(2).sort_values('quality_score', ascending=False)

            for provider, metrics in provider_summary.iterrows():
                print(f"{provider}: Quality={metrics['quality_score']:.1f}, "
                      f"Avg Return={metrics['return_pct']:.2f}%, "
                      f"Avg Sharpe={metrics['sharpe_ratio']:.2f}")

        # 7. Strategy Recommendations
        print("\n" + "-"*60)
        print("💡 STRATEGY RECOMMENDATIONS")
        print("-"*60)

        # Best overall asset
        best_asset = results_df.nlargest(1, 'sharpe_ratio').iloc[0]
        print(f"\n✅ BEST ASSET FOR THIS STRATEGY:")
        print(f"   {best_asset['symbol']} ({best_asset['timeframe']}) from {best_asset['provider']}")
        print(f"   Sharpe: {best_asset['sharpe_ratio']:.2f}, Return: {best_asset['return_pct']:.2f}%")

        # Assets to avoid
        worst_performers = results_df[results_df['sharpe_ratio'] < 0]
        if not worst_performers.empty:
            print(f"\n⚠️ ASSETS TO AVOID (Negative Sharpe):")
            for idx, row in worst_performers.iterrows():
                print(f"   {row['symbol']} ({row['timeframe']}, {row['provider']}): Sharpe={row['sharpe_ratio']:.2f}")

        # Optimization suggestions
        print("\n🔧 OPTIMIZATION SUGGESTIONS:")
        avg_trades = results_df['num_trades'].mean()
        if avg_trades < 10:
            print("   • Low trade frequency detected. Consider:")
            print("     - Reducing minimum impulse size requirement")
            print("     - Expanding trading session hours")
            print("     - Testing on higher frequency data")

        if results_df['win_rate'].mean() < 40:
            print("   • Low win rate detected. Consider:")
            print("     - Tightening entry criteria (stricter golden pocket validation)")
            print("     - Adding additional confluence indicators")
            print("     - Implementing better trend filters")

        if results_df['max_drawdown'].mean() > 20:
            print("   • High drawdown detected. Consider:")
            print("     - Reducing position sizing")
            print("     - Implementing trailing stops")
            print("     - Adding volatility-based position adjustment")

        print("\n" + "="*80)
        print("✅ ANALYSIS COMPLETE!")
        print("="*80)


if __name__ == "__main__":
    # Create tester instance
    tester = MultiAssetFibonacciTester()

    # Run comprehensive test
    tester.run_comprehensive_test()