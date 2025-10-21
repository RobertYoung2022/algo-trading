"""
🚀 Comprehensive Multi-Asset Strategy Tester - December 2024
==============================================================
Tests all 4 fixed strategies across ALL available cryptocurrency data.
Generates comprehensive performance reports and deployment recommendations.

🌟 Strategies Tested:
    1. Dual Moving Average Crossover (DualMACrossover1224)
    2. Bollinger Bands Mean Reversion (BollingerMeanReversion1224)
    3. Simple SMA Crossover (SimpleSMACrossover1224)
    4. RSI + BB Mean Reversion (RSIBBMeanReversion1224)

💫 Data Sources:
    - BTC, ETH, XRP, LINK, CRO, HBAR
    - Multiple timeframes (1m, 5m, 1h, 6h, 1d)
    - Multiple providers (Coinbase, Hyperliquid)

🎯 Bot Deployment Criteria:
    - Return ≥ 20%
    - Sharpe Ratio ≥ 1.5
    - Max Drawdown ≥ -15%
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import glob
import warnings
warnings.filterwarnings('ignore')

# Add path for imports
sys.path.append('/Users/bobbyyo/Projects/algo-fun')
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/backtests/1224')

from backtesting import Backtest

# Import all strategies
from dual_ma_crossover_1224 import DualMACrossover1224
from bollinger_mean_reversion_1224 import BollingerMeanReversion1224
from simple_sma_crossover_1224 import SimpleSMACrossover1224
from rsi_bb_mean_reversion_1224 import RSIBBMeanReversion1224
from data_loader_1224 import load_and_validate_data


class ComprehensiveStrategyTester:
    """
    🎯 Comprehensive testing framework for all strategies
    """

    def __init__(self):
        """Initialize tester with all strategies and data sources"""
        self.strategies = {
            'DualMACrossover': DualMACrossover1224,
            'BollingerMeanReversion': BollingerMeanReversion1224,
            'SimpleSMACrossover': SimpleSMACrossover1224,
            'RSIBBMeanReversion': RSIBBMeanReversion1224
        }

        self.data_sources = []
        self.results = []
        self.deployment_candidates = []

        # Discover all data sources
        self._discover_data_sources()

    def _discover_data_sources(self):
        """🔍 Discover all available crypto data files"""
        base_path = "/Users/bobbyyo/Projects/algo-fun/dataset_files"

        # Find all CSV files
        patterns = [
            f"{base_path}/coinbase/*.csv",
            f"{base_path}/hyperliquid/*.csv",
            f"{base_path}/*.csv"
        ]

        for pattern in patterns:
            files = glob.glob(pattern)
            for file_path in files:
                # Extract metadata from filename
                filename = os.path.basename(file_path)
                provider = self._extract_provider(file_path)
                asset = self._extract_asset(filename)
                timeframe = self._extract_timeframe(filename)

                if asset and asset in ['BTC', 'ETH', 'XRP', 'LINK', 'CRO', 'HBAR']:
                    self.data_sources.append({
                        'file_path': file_path,
                        'filename': filename,
                        'provider': provider,
                        'asset': asset,
                        'timeframe': timeframe
                    })

        print(f"🔍 Discovered {len(self.data_sources)} data sources")
        print(f"   Assets: {sorted(set([d['asset'] for d in self.data_sources]))}")
        print(f"   Providers: {sorted(set([d['provider'] for d in self.data_sources]))}")

    def _extract_provider(self, file_path):
        """Extract data provider from path"""
        if 'coinbase' in file_path.lower():
            return 'Coinbase'
        elif 'hyperliquid' in file_path.lower():
            return 'Hyperliquid'
        elif 'cryptocompare' in file_path.lower():
            return 'CryptoCompare'
        else:
            return 'Unknown'

    def _extract_asset(self, filename):
        """Extract asset symbol from filename"""
        filename = filename.upper()
        for asset in ['BTC', 'ETH', 'XRP', 'LINK', 'CRO', 'HBAR']:
            if asset in filename:
                return asset
        return None

    def _extract_timeframe(self, filename):
        """Extract timeframe from filename"""
        filename = filename.lower()
        timeframes = ['1m', '5m', '15m', '1h', '4h', '6h', '1d', '1w']
        for tf in timeframes:
            if f'-{tf}-' in filename:
                return tf
        return 'unknown'

    # Use shared data loader
    def load_and_validate_data(self, file_path, min_quality_score=75):
        return load_and_validate_data(file_path, min_quality_score)

    def run_single_backtest(self, strategy_class, data, initial_cash=10000):
        """🚀 Run a single backtest"""
        try:
            bt = Backtest(
                data,
                strategy_class,
                cash=initial_cash,
                commission=0.002,
                exclusive_orders=True,
                hedging=False
            )

            stats = bt.run()
            return stats

        except Exception as e:
            print(f"   ❌ Backtest error: {e}")
            return None

    def test_strategy_on_all_data(self, strategy_name):
        """🎯 Test a single strategy on all data sources"""
        strategy_class = self.strategies[strategy_name]
        strategy_results = []

        print(f"\n{'='*80}")
        print(f"🚀 Testing {strategy_name}")
        print(f"{'='*80}")

        for data_source in self.data_sources:
            print(f"\n📊 {data_source['asset']}-{data_source['timeframe']} ({data_source['provider']})")

            # Load and validate data
            data, quality_score, valid = self.load_and_validate_data(data_source['file_path'])

            if not valid:
                print(f"   ⚠️ Data quality too low ({quality_score:.1f}/100)")
                continue

            if data is None or len(data) < 250:
                print(f"   ⚠️ Insufficient data ({len(data) if data is not None else 0} bars)")
                continue

            # Run backtest
            stats = self.run_single_backtest(strategy_class, data)

            if stats is not None:
                # Store results
                result = {
                    'strategy': strategy_name,
                    'asset': data_source['asset'],
                    'timeframe': data_source['timeframe'],
                    'provider': data_source['provider'],
                    'return_pct': stats['Return [%]'],
                    'buy_hold_return': stats['Buy & Hold Return [%]'],
                    'sharpe_ratio': stats['Sharpe Ratio'],
                    'sortino_ratio': stats['Sortino Ratio'],
                    'max_drawdown': stats['Max. Drawdown [%]'],
                    'win_rate': stats['Win Rate [%]'],
                    'num_trades': stats['# Trades'],
                    'profit_factor': stats.get('Profit Factor', np.nan)
                }

                strategy_results.append(result)

                # Print key metrics
                print(f"   Return: {result['return_pct']:.2f}%")
                print(f"   Sharpe: {result['sharpe_ratio']:.2f}")
                print(f"   Max DD: {result['max_drawdown']:.2f}%")
                print(f"   Trades: {result['num_trades']}")

                # Check deployment criteria
                if (result['return_pct'] >= 20 and
                    result['sharpe_ratio'] >= 1.5 and
                    result['max_drawdown'] >= -15):
                    print(f"   ✅ MEETS DEPLOYMENT CRITERIA!")
                    self.deployment_candidates.append(result)

        return strategy_results

    def run_comprehensive_test(self):
        """🚀 Run all strategies on all data"""
        print("\n" + "="*80)
        print("🚀 COMPREHENSIVE MULTI-ASSET STRATEGY TESTING")
        print("="*80)
        print(f"Strategies: {list(self.strategies.keys())}")
        print(f"Data Sources: {len(self.data_sources)}")

        # Test each strategy
        for strategy_name in self.strategies:
            results = self.test_strategy_on_all_data(strategy_name)
            self.results.extend(results)

        # Generate summary report
        self.generate_summary_report()

    def generate_summary_report(self):
        """📊 Generate comprehensive summary report"""
        if not self.results:
            print("\n❌ No results to report")
            return

        # Convert to DataFrame for analysis
        df = pd.DataFrame(self.results)

        print("\n" + "="*80)
        print("📊 COMPREHENSIVE TESTING SUMMARY")
        print("="*80)

        # Overall statistics
        print("\n🎯 Overall Statistics:")
        print(f"Total Tests Run: {len(df)}")
        print(f"Successful Tests: {df['return_pct'].notna().sum()}")
        print(f"Average Return: {df['return_pct'].mean():.2f}%")
        print(f"Average Sharpe: {df['sharpe_ratio'].mean():.2f}")
        print(f"Average Max DD: {df['max_drawdown'].mean():.2f}%")

        # Best performers by strategy
        print("\n🏆 Best Performance by Strategy:")
        for strategy in self.strategies:
            strategy_df = df[df['strategy'] == strategy]
            if not strategy_df.empty:
                best = strategy_df.nlargest(1, 'sharpe_ratio').iloc[0]
                print(f"\n{strategy}:")
                print(f"   Best Asset: {best['asset']}-{best['timeframe']}")
                print(f"   Return: {best['return_pct']:.2f}%")
                print(f"   Sharpe: {best['sharpe_ratio']:.2f}")
                print(f"   Max DD: {best['max_drawdown']:.2f}%")

        # Best performers by asset
        print("\n🏆 Best Strategy by Asset:")
        for asset in ['BTC', 'ETH', 'XRP', 'LINK', 'CRO', 'HBAR']:
            asset_df = df[df['asset'] == asset]
            if not asset_df.empty:
                best = asset_df.nlargest(1, 'sharpe_ratio').iloc[0]
                print(f"\n{asset}:")
                print(f"   Best Strategy: {best['strategy']}")
                print(f"   Timeframe: {best['timeframe']}")
                print(f"   Return: {best['return_pct']:.2f}%")
                print(f"   Sharpe: {best['sharpe_ratio']:.2f}")

        # Deployment candidates
        print("\n" + "="*80)
        print("🚀 BOT DEPLOYMENT CANDIDATES")
        print("="*80)

        if self.deployment_candidates:
            print(f"\n✅ {len(self.deployment_candidates)} strategies meet deployment criteria:\n")
            for candidate in self.deployment_candidates:
                print(f"Strategy: {candidate['strategy']}")
                print(f"   Asset: {candidate['asset']}-{candidate['timeframe']} ({candidate['provider']})")
                print(f"   Return: {candidate['return_pct']:.2f}%")
                print(f"   Sharpe: {candidate['sharpe_ratio']:.2f}")
                print(f"   Max DD: {candidate['max_drawdown']:.2f}%")
                print(f"   Win Rate: {candidate['win_rate']:.2f}%")
                print(f"   Trades: {candidate['num_trades']}")
                print()
        else:
            print("\n❌ No strategies currently meet the deployment criteria:")
            print("   - Return ≥ 20%")
            print("   - Sharpe Ratio ≥ 1.5")
            print("   - Max Drawdown ≥ -15%")

        # Save results to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = f"/Users/bobbyyo/Projects/algo-fun/strategies/backtests/1224/results_{timestamp}.csv"
        df.to_csv(csv_path, index=False)
        print(f"\n📁 Results saved to: {csv_path}")


if __name__ == "__main__":
    """
    🚀 Run comprehensive testing
    """
    tester = ComprehensiveStrategyTester()
    tester.run_comprehensive_test()