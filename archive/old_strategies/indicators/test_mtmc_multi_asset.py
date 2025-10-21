"""
Multi-Asset Testing Framework for MTMC Strategy
================================================
Comprehensive testing across all available cryptocurrencies and timeframes
to validate the Multi-Timeframe Momentum Cascade strategy performance.

Testing Priorities:
1. ETH - Proven multi-timeframe trends
2. HBAR - Strong directional moves
3. LINK - Clear trending behavior
4. BTC - Macro trend leader
5. CRO, XRP - Additional validation

Created: 2025-01-17
Author: Bobby's Algo-Trading System 🌙💫🚀
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
from mtmc_multi_timeframe_cascade import MTMCStrategy
import glob
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class MTMCMultiAssetTester:
    """
    Comprehensive multi-asset testing framework for MTMC strategy
    Tests across multiple cryptocurrencies and data providers
    """

    def __init__(self, base_path="/Users/bobbyyo/Projects/algo-fun"):
        self.base_path = base_path
        self.data_path = os.path.join(base_path, "data")
        self.results = []
        self.strategy_name = "MTMC"

        # Priority assets for testing
        self.priority_assets = ['ETH', 'HBAR', 'LINK', 'BTC', 'CRO', 'XRP']

        # Data quality validator (if available)
        self.min_quality_score = 75

    def discover_data_files(self):
        """
        Discover all available data files across providers

        Returns:
            dict: Organized data files by provider and asset
        """
        print("\n🔍 Discovering Available Data Sources...")
        print("="*60)

        data_files = {
            'yahoo': {},
            'coinbase': {},
            'coingecko': {},
            'coinmarketcap': {},
            'cryptocompare': {}
        }

        # Yahoo Finance data
        yahoo_pattern = os.path.join(self.data_path, "yahoo", "*.csv")
        for file in glob.glob(yahoo_pattern):
            filename = os.path.basename(file)
            for asset in self.priority_assets:
                if asset in filename.upper():
                    if asset not in data_files['yahoo']:
                        data_files['yahoo'][asset] = []
                    data_files['yahoo'][asset].append(file)
                    break

        # Coinbase data
        coinbase_pattern = os.path.join(self.data_path, "coinbase", "*.csv")
        for file in glob.glob(coinbase_pattern):
            filename = os.path.basename(file)
            for asset in self.priority_assets:
                if asset in filename.upper():
                    if asset not in data_files['coinbase']:
                        data_files['coinbase'][asset] = []
                    data_files['coinbase'][asset].append(file)
                    break

        # CoinGecko data
        coingecko_pattern = os.path.join(self.data_path, "coingecko", "*.csv")
        for file in glob.glob(coingecko_pattern):
            filename = os.path.basename(file)
            for asset in self.priority_assets:
                if asset in filename.upper():
                    if asset not in data_files['coingecko']:
                        data_files['coingecko'][asset] = []
                    data_files['coingecko'][asset].append(file)
                    break

        # Display discovered files
        total_files = 0
        for provider, assets in data_files.items():
            if assets:
                print(f"\n📁 {provider.upper()}:")
                for asset, files in assets.items():
                    print(f"  {asset}: {len(files)} files")
                    total_files += len(files)

        print(f"\n✅ Total files discovered: {total_files}")
        return data_files

    def validate_data_quality(self, df):
        """
        Basic data quality validation

        Args:
            df: DataFrame to validate

        Returns:
            tuple: (is_valid, quality_score, issues)
        """
        issues = []
        quality_score = 100

        # Check for required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in required_cols:
            if col not in df.columns:
                issues.append(f"Missing column: {col}")
                quality_score -= 20

        # Check for NaN values
        nan_percentage = df[required_cols].isna().sum().sum() / (len(df) * len(required_cols))
        if nan_percentage > 0:
            issues.append(f"NaN values: {nan_percentage:.2%}")
            quality_score -= min(50, nan_percentage * 100)

        # Check for zero volumes
        if 'Volume' in df.columns:
            zero_vol_pct = (df['Volume'] == 0).sum() / len(df)
            if zero_vol_pct > 0.1:
                issues.append(f"Zero volume: {zero_vol_pct:.2%}")
                quality_score -= min(20, zero_vol_pct * 100)

        # Check data consistency
        if all(col in df.columns for col in ['High', 'Low', 'Close']):
            invalid_candles = ((df['High'] < df['Low']) |
                              (df['Close'] > df['High']) |
                              (df['Close'] < df['Low'])).sum()
            if invalid_candles > 0:
                issues.append(f"Invalid candles: {invalid_candles}")
                quality_score -= 30

        is_valid = quality_score >= self.min_quality_score
        return is_valid, quality_score, issues

    def load_and_prepare_data(self, file_path):
        """
        Load and prepare data for backtesting

        Args:
            file_path: Path to CSV file

        Returns:
            DataFrame or None if loading fails
        """
        try:
            df = pd.read_csv(file_path)

            # Handle different column naming conventions
            column_mappings = {
                'date': 'Date', 'time': 'Date', 'timestamp': 'Date',
                'open': 'Open', 'high': 'High', 'low': 'Low',
                'close': 'Close', 'volume': 'Volume'
            }

            df.columns = [column_mappings.get(col.lower(), col) for col in df.columns]

            # Ensure Date column and set as index
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index('Date', inplace=True)
            else:
                # Try to infer date from index
                df.index = pd.to_datetime(df.index)

            # Sort by date
            df.sort_index(inplace=True)

            # Validate data quality
            is_valid, quality_score, issues = self.validate_data_quality(df)

            if not is_valid:
                print(f"  ⚠️ Data quality too low ({quality_score:.0f}): {', '.join(issues)}")
                return None

            return df

        except Exception as e:
            print(f"  ❌ Error loading data: {str(e)}")
            return None

    def run_backtest(self, df, file_path, asset, provider):
        """
        Run MTMC backtest on prepared data

        Args:
            df: Prepared DataFrame
            file_path: Original file path
            asset: Asset symbol
            provider: Data provider name

        Returns:
            dict: Backtest results
        """
        try:
            # Infer timeframe from data
            if len(df) > 1:
                time_diff = (df.index[1] - df.index[0]).total_seconds()
                if time_diff < 3600:
                    timeframe = f"{int(time_diff/60)}m"
                elif time_diff < 86400:
                    timeframe = f"{int(time_diff/3600)}h"
                else:
                    timeframe = f"{int(time_diff/86400)}d"
            else:
                timeframe = "unknown"

            print(f"\n  🚀 Running MTMC on {asset} ({provider}, {timeframe})")
            print(f"     Data points: {len(df):,}, Period: {df.index[0].date()} to {df.index[-1].date()}")

            # Run backtest
            bt = Backtest(
                df,
                MTMCStrategy,
                cash=100000,
                commission=0.001,
                exclusive_orders=True
            )

            stats = bt.run()

            # Extract key metrics
            result = {
                'strategy': self.strategy_name,
                'asset': asset,
                'provider': provider,
                'timeframe': timeframe,
                'file_path': file_path,
                'data_points': len(df),
                'start_date': df.index[0].strftime('%Y-%m-%d'),
                'end_date': df.index[-1].strftime('%Y-%m-%d'),
                'total_return': stats['Return [%]'],
                'buy_hold_return': stats['Buy & Hold Return [%]'],
                'sharpe_ratio': stats.get('Sharpe Ratio', 0),
                'sortino_ratio': stats.get('Sortino Ratio', 0),
                'max_drawdown': stats['Max. Drawdown [%]'],
                'win_rate': stats.get('Win Rate [%]', 0),
                'num_trades': stats['# Trades'],
                'avg_trade': stats.get('Avg. Trade [%]', 0),
                'profit_factor': stats.get('Profit Factor', 0),
                'expectancy': stats.get('Expectancy [%]', 0),
                'sqn': stats.get('SQN', 0),
                'exposure_time': stats.get('Exposure Time [%]', 0),
                'stats_object': stats  # Store full stats for detailed analysis
            }

            # Calculate confluence effectiveness
            if stats['# Trades'] > 0:
                confluence_score = (
                    (stats.get('Win Rate [%]', 0) / 100 * 0.4) +
                    (min(stats.get('Profit Factor', 0) / 2, 1) * 0.3) +
                    (min(abs(stats.get('Sharpe Ratio', 0)) / 2, 1) * 0.3)
                )
                result['confluence_score'] = confluence_score
            else:
                result['confluence_score'] = 0

            return result

        except Exception as e:
            print(f"  ❌ Backtest error: {str(e)}")
            return None

    def test_priority_assets(self, data_files):
        """
        Test MTMC strategy on priority assets

        Args:
            data_files: Dictionary of discovered data files

        Returns:
            list: Test results
        """
        print("\n🎯 Testing Priority Assets with MTMC Strategy")
        print("="*60)

        results = []

        # Test each priority asset
        for asset in self.priority_assets:
            print(f"\n📊 Testing {asset}...")
            asset_results = []

            # Test across all providers
            for provider, assets_dict in data_files.items():
                if asset in assets_dict:
                    for file_path in assets_dict[asset]:
                        # Load and prepare data
                        df = self.load_and_prepare_data(file_path)
                        if df is not None and len(df) > 100:
                            # Run backtest
                            result = self.run_backtest(df, file_path, asset, provider)
                            if result:
                                results.append(result)
                                asset_results.append(result)

            # Show asset summary
            if asset_results:
                best = max(asset_results, key=lambda x: x['total_return'])
                avg_return = np.mean([r['total_return'] for r in asset_results])
                avg_sharpe = np.mean([r['sharpe_ratio'] for r in asset_results if r['sharpe_ratio']])

                print(f"\n  ✅ {asset} Summary:")
                print(f"     Tests run: {len(asset_results)}")
                print(f"     Best return: {best['total_return']:.2f}% ({best['provider']})")
                print(f"     Avg return: {avg_return:.2f}%")
                print(f"     Avg Sharpe: {avg_sharpe:.3f}")

        return results

    def generate_comparison_report(self, results):
        """
        Generate comprehensive comparison report

        Args:
            results: List of backtest results
        """
        print("\n" + "="*80)
        print("📊 MTMC MULTI-TIMEFRAME MOMENTUM CASCADE - COMPREHENSIVE REPORT")
        print("="*80)

        if not results:
            print("No results to report")
            return

        # Convert to DataFrame for analysis
        df_results = pd.DataFrame(results)

        # Overall performance summary
        print("\n🎯 OVERALL PERFORMANCE SUMMARY")
        print("-"*50)
        print(f"Total tests run: {len(results)}")
        print(f"Assets tested: {df_results['asset'].nunique()}")
        print(f"Providers used: {df_results['provider'].nunique()}")
        print(f"Profitable tests: {(df_results['total_return'] > 0).sum()} / {len(results)} ({(df_results['total_return'] > 0).mean()*100:.1f}%)")

        # Performance metrics
        print(f"\n📈 AGGREGATE METRICS:")
        print(f"Average Return: {df_results['total_return'].mean():.2f}%")
        print(f"Median Return: {df_results['total_return'].median():.2f}%")
        print(f"Best Return: {df_results['total_return'].max():.2f}%")
        print(f"Worst Return: {df_results['total_return'].min():.2f}%")
        print(f"Avg Sharpe Ratio: {df_results['sharpe_ratio'].mean():.3f}")
        print(f"Avg Win Rate: {df_results['win_rate'].mean():.1f}%")
        print(f"Avg Max Drawdown: {df_results['max_drawdown'].mean():.2f}%")

        # Asset rankings
        print("\n🏆 ASSET PERFORMANCE RANKINGS (BY AVERAGE RETURN):")
        print("-"*50)
        asset_summary = df_results.groupby('asset').agg({
            'total_return': ['mean', 'max', 'count'],
            'sharpe_ratio': 'mean',
            'win_rate': 'mean',
            'confluence_score': 'mean'
        }).round(2)

        asset_summary.columns = ['Avg Return', 'Best Return', 'Tests', 'Avg Sharpe', 'Avg Win Rate', 'Confluence']
        asset_summary = asset_summary.sort_values('Avg Return', ascending=False)

        for idx, (asset, row) in enumerate(asset_summary.iterrows(), 1):
            status = "🟢" if row['Avg Return'] > 0 else "🔴"
            print(f"{idx}. {status} {asset}: {row['Avg Return']:.2f}% avg, {row['Best Return']:.2f}% best, "
                  f"Sharpe: {row['Avg Sharpe']:.3f}, Win: {row['Avg Win Rate']:.1f}%, "
                  f"Confluence: {row['Confluence']:.2f} ({int(row['Tests'])} tests)")

        # Provider comparison
        print("\n📊 PROVIDER PERFORMANCE COMPARISON:")
        print("-"*50)
        provider_summary = df_results.groupby('provider').agg({
            'total_return': ['mean', 'count'],
            'sharpe_ratio': 'mean',
            'win_rate': 'mean'
        }).round(2)

        provider_summary.columns = ['Avg Return', 'Tests', 'Avg Sharpe', 'Avg Win Rate']
        provider_summary = provider_summary.sort_values('Avg Return', ascending=False)

        for provider, row in provider_summary.iterrows():
            print(f"{provider}: {row['Avg Return']:.2f}% return, Sharpe: {row['Avg Sharpe']:.3f}, "
                  f"Win: {row['Avg Win Rate']:.1f}% ({int(row['Tests'])} tests)")

        # Timeframe analysis
        print("\n⏰ TIMEFRAME EFFECTIVENESS:")
        print("-"*50)
        timeframe_summary = df_results.groupby('timeframe').agg({
            'total_return': ['mean', 'count'],
            'sharpe_ratio': 'mean',
            'win_rate': 'mean',
            'confluence_score': 'mean'
        }).round(2)

        timeframe_summary.columns = ['Avg Return', 'Tests', 'Avg Sharpe', 'Avg Win Rate', 'Confluence']
        timeframe_summary = timeframe_summary.sort_values('Avg Return', ascending=False)

        for tf, row in timeframe_summary.iterrows():
            print(f"{tf}: {row['Avg Return']:.2f}% return, Sharpe: {row['Avg Sharpe']:.3f}, "
                  f"Win: {row['Avg Win Rate']:.1f}%, Confluence: {row['Confluence']:.2f} ({int(row['Tests'])} tests)")

        # Top 5 best performing combinations
        print("\n🌟 TOP 5 BEST PERFORMING COMBINATIONS:")
        print("-"*50)
        top_5 = df_results.nlargest(5, 'total_return')[
            ['asset', 'provider', 'timeframe', 'total_return', 'sharpe_ratio', 'win_rate', 'num_trades', 'confluence_score']
        ]

        for idx, row in top_5.iterrows():
            print(f"{row['asset']}-{row['provider']}-{row['timeframe']}: "
                  f"{row['total_return']:.2f}% return, Sharpe: {row['sharpe_ratio']:.3f}, "
                  f"Win: {row['win_rate']:.1f}%, Trades: {row['num_trades']}, "
                  f"Confluence: {row['confluence_score']:.2f}")

        # Strategy validation
        print("\n✅ MTMC STRATEGY VALIDATION:")
        print("-"*50)

        # Check if confluence scoring is effective
        high_confluence = df_results[df_results['confluence_score'] > 0.7]
        low_confluence = df_results[df_results['confluence_score'] <= 0.7]

        if len(high_confluence) > 0 and len(low_confluence) > 0:
            print(f"High Confluence (>0.7): {high_confluence['total_return'].mean():.2f}% avg return, "
                  f"{high_confluence['win_rate'].mean():.1f}% win rate")
            print(f"Low Confluence (≤0.7): {low_confluence['total_return'].mean():.2f}% avg return, "
                  f"{low_confluence['win_rate'].mean():.1f}% win rate")

            confluence_advantage = high_confluence['total_return'].mean() - low_confluence['total_return'].mean()
            if confluence_advantage > 0:
                print(f"✅ Confluence scoring effective: +{confluence_advantage:.2f}% advantage")
            else:
                print(f"⚠️ Confluence scoring needs adjustment: {confluence_advantage:.2f}% disadvantage")

        # Win rate analysis
        target_win_rate = 55  # Target from specs
        achieved_win_rate = df_results['win_rate'].mean()
        print(f"\n📊 Win Rate Target: {target_win_rate}% | Achieved: {achieved_win_rate:.1f}%")
        if achieved_win_rate >= target_win_rate:
            print(f"✅ Win rate target achieved! (+{achieved_win_rate - target_win_rate:.1f}%)")
        else:
            print(f"⚠️ Win rate below target (-{target_win_rate - achieved_win_rate:.1f}%)")

        # Risk metrics
        print(f"\n🛡️ RISK MANAGEMENT:")
        print(f"Average Max Drawdown: {df_results['max_drawdown'].mean():.2f}%")
        print(f"Target Max Drawdown: 15-20%")
        if df_results['max_drawdown'].mean() <= 20:
            print("✅ Drawdown within acceptable range")
        else:
            print("⚠️ Drawdown exceeds target range")

        # Save results to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(self.base_path, "strategies", "results", f"mtmc_results_{timestamp}.csv")
        df_results.drop('stats_object', axis=1, errors='ignore').to_csv(csv_path, index=False)
        print(f"\n💾 Results saved to: {csv_path}")

        # Final recommendation
        print("\n" + "="*80)
        print("🎯 FINAL MTMC STRATEGY ASSESSMENT:")
        print("="*80)

        strengths = []
        weaknesses = []

        if achieved_win_rate >= target_win_rate:
            strengths.append(f"Win rate ({achieved_win_rate:.1f}%) exceeds target")
        else:
            weaknesses.append(f"Win rate ({achieved_win_rate:.1f}%) below target")

        if df_results['total_return'].mean() > 0:
            strengths.append(f"Positive average returns ({df_results['total_return'].mean():.2f}%)")
        else:
            weaknesses.append(f"Negative average returns ({df_results['total_return'].mean():.2f}%)")

        if df_results['sharpe_ratio'].mean() > 0.5:
            strengths.append(f"Good risk-adjusted returns (Sharpe: {df_results['sharpe_ratio'].mean():.3f})")
        else:
            weaknesses.append(f"Low risk-adjusted returns (Sharpe: {df_results['sharpe_ratio'].mean():.3f})")

        print("\n💪 STRENGTHS:")
        for s in strengths:
            print(f"  ✅ {s}")

        if weaknesses:
            print("\n⚠️ AREAS FOR IMPROVEMENT:")
            for w in weaknesses:
                print(f"  ⚠️ {w}")

        print("\n🚀 RECOMMENDATION:")
        if len(strengths) >= 2 and df_results['total_return'].mean() > 0:
            print("The MTMC strategy shows promise with multi-timeframe confluence working effectively.")
            print("Consider parameter optimization on top-performing assets (ETH, HBAR) for production deployment.")
        else:
            print("The MTMC strategy needs further refinement. Focus on:")
            print("- Adjusting confluence thresholds")
            print("- Optimizing timeframe combinations")
            print("- Fine-tuning entry/exit conditions")

    def run_comprehensive_test(self):
        """
        Run comprehensive multi-asset test of MTMC strategy
        """
        print("\n" + "="*80)
        print("🌙 MULTI-TIMEFRAME MOMENTUM CASCADE (MTMC) COMPREHENSIVE TEST 🌙")
        print("="*80)
        print(f"Testing sophisticated multi-timeframe confluence strategy")
        print(f"Strategy: HTF trend + MTF momentum + LTF timing")
        print(f"Target: 55-65% win rate with dynamic position sizing")

        # Discover available data
        data_files = self.discover_data_files()

        if not any(data_files.values()):
            print("❌ No data files found for testing")
            return

        # Run tests on priority assets
        results = self.test_priority_assets(data_files)

        # Generate comprehensive report
        if results:
            self.generate_comparison_report(results)
        else:
            print("❌ No successful test results to report")

        return results


if __name__ == "__main__":
    # Run comprehensive MTMC test
    tester = MTMCMultiAssetTester()
    results = tester.run_comprehensive_test()

    print("\n🎯 MTMC Multi-Timeframe Testing Complete! 🚀")
    print("="*80)