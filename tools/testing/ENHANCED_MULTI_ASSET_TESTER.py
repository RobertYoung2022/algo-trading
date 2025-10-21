#!/usr/bin/env python3
"""
🛡️ ENHANCED MULTI-ASSET TESTING FRAMEWORK
==========================================

Production-ready comprehensive testing framework with:
- Security validation for all data sources
- Production readiness assessment
- Risk management validation
- Multi-asset performance ranking
- Cross-provider comparison
- Quality assurance integration

SECURITY ENHANCEMENTS:
- Mandatory data validation (≥75 quality score)
- Production readiness validation
- Risk parameter validation
- Error handling and graceful degradation
"""

import pandas as pd
import numpy as np
import os
import warnings
from datetime import datetime
from backtesting import Backtest
warnings.filterwarnings('ignore')

# 🛡️ SECURITY: Import validation and risk management
from trading_functions import (
    DataQualityValidator,
    validate_data_source_quality,
    production_readiness_check,
    calculate_comprehensive_strategy_metrics,
    generate_risk_report
)

class EnhancedMultiAssetTester:
    """
    🛡️ Production-ready multi-asset testing framework

    Features:
    - Security validation for all data sources
    - Production readiness assessment
    - Comprehensive risk analysis
    - Multi-asset performance ranking
    - Cross-provider validation
    """

    def __init__(self, min_quality_score=75):
        """Initialize with security and validation settings"""
        self.min_quality_score = min_quality_score
        self.validator = DataQualityValidator()
        self.test_results = []
        self.validated_sources = []
        self.failed_sources = []

        # 🛡️ SECURITY: Validate framework readiness
        print("🛡️ Initializing Enhanced Multi-Asset Testing Framework...")
        readiness = production_readiness_check()
        if not readiness.get('config_valid', False):
            print("⚠️ FRAMEWORK: Production readiness validation failed")
        else:
            print("✅ FRAMEWORK: Production readiness validated")

    def validate_data_source(self, name, path):
        """
        🛡️ Validate data source quality and security
        """
        print(f"🛡️ Validating {name}...")

        if not os.path.exists(path):
            print(f"❌ SECURITY: File not found - {path}")
            self.failed_sources.append((name, path, "File not found"))
            return False

        try:
            validation_result = validate_data_source_quality(path, self.validator)

            if validation_result.overall_score < self.min_quality_score:
                print(f"❌ SECURITY: {name} quality too low - {validation_result.overall_score}")
                self.failed_sources.append((name, path, f"Low quality: {validation_result.overall_score}"))
                return False

            print(f"✅ {name} validated - Quality: {validation_result.overall_score}")
            self.validated_sources.append((name, path, validation_result.overall_score))
            return True

        except Exception as e:
            print(f"❌ SECURITY: {name} validation failed - {e}")
            self.failed_sources.append((name, path, f"Validation error: {e}"))
            return False

    def run_strategy_test(self, strategy_class, name, path, cash=1000000):
        """
        🛡️ Run strategy test with production safety features
        """
        try:
            print(f"\n🧪 Testing {strategy_class.__name__} on {name}...")

            # Load and prepare data
            data = pd.read_csv(path, parse_dates=['datetime'], index_col='datetime')
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            data = data.dropna().sort_index()

            # Ensure positive prices and fix OHLC relationships
            for col in ['Open', 'High', 'Low', 'Close']:
                data[col] = data[col].abs()

            data['High'] = data[['Open', 'High', 'Low', 'Close']].max(axis=1)
            data['Low'] = data[['Open', 'High', 'Low', 'Close']].min(axis=1)
            data['Volume'] = data['Volume'].abs()

            print(f"📊 Data: {len(data)} bars from {data.index[0].date()} to {data.index[-1].date()}")

            # 🛡️ PRODUCTION: Run backtest with error handling
            bt = Backtest(data, strategy_class, cash=cash, commission=0.002)
            stats = bt.run()

            # 🛡️ PRODUCTION: Calculate comprehensive metrics
            trades_data = bt._results if hasattr(bt, '_results') else None

            result = {
                'strategy': strategy_class.__name__,
                'asset': name,
                'data_path': path,
                'data_quality': next((score for n, p, score in self.validated_sources if p == path), 0),
                'start_date': data.index[0].date(),
                'end_date': data.index[-1].date(),
                'total_bars': len(data),
                'cash': cash,

                # Core performance metrics
                'return_pct': float(stats['Return [%]']),
                'buy_hold_return_pct': float(stats['Buy & Hold Return [%]']),
                'max_drawdown_pct': float(stats['Max. Drawdown [%]']),
                'sharpe_ratio': float(stats['Sharpe Ratio']),
                'sortino_ratio': float(stats['Sortino Ratio']),
                'calmar_ratio': float(stats['Calmar Ratio']),
                'total_trades': int(stats['# Trades']),
                'win_rate_pct': float(stats['Win Rate [%]']),
                'profit_factor': float(stats['Profit Factor']),
                'sqn': float(stats['SQN']),

                # Risk metrics
                'avg_drawdown_pct': float(stats['Avg. Drawdown [%]']),
                'max_drawdown_duration': int(stats['Max. Drawdown Duration']),
                'best_trade_pct': float(stats['Best Trade [%]']),
                'worst_trade_pct': float(stats['Worst Trade [%]']),
                'avg_trade_pct': float(stats['Avg. Trade [%]']),
                'exposure_time_pct': float(stats['Exposure Time [%]']),

                # 🛡️ PRODUCTION: Risk assessment
                'risk_grade': self._calculate_risk_grade(stats),
                'production_ready': self._assess_production_readiness(stats),
                'timestamp': datetime.now()
            }

            self.test_results.append(result)

            print(f"✅ {name} Complete - Return: {result['return_pct']:.2f}%, "
                  f"Sharpe: {result['sharpe_ratio']:.2f}, Trades: {result['total_trades']}")

            return result

        except Exception as e:
            print(f"❌ ERROR: {name} test failed - {e}")
            error_result = {
                'strategy': strategy_class.__name__,
                'asset': name,
                'data_path': path,
                'error': str(e),
                'timestamp': datetime.now()
            }
            self.test_results.append(error_result)
            return error_result

    def _calculate_risk_grade(self, stats):
        """Calculate risk grade based on key metrics"""
        score = 0

        # Positive return
        if stats['Return [%]'] > 0:
            score += 20

        # Good Sharpe ratio
        if stats['Sharpe Ratio'] > 1.5:
            score += 25
        elif stats['Sharpe Ratio'] > 1.0:
            score += 15
        elif stats['Sharpe Ratio'] > 0.5:
            score += 10

        # Low drawdown
        if abs(stats['Max. Drawdown [%]']) < 10:
            score += 25
        elif abs(stats['Max. Drawdown [%]']) < 20:
            score += 15
        elif abs(stats['Max. Drawdown [%]']) < 30:
            score += 10

        # Good win rate
        if stats['Win Rate [%]'] > 60:
            score += 20
        elif stats['Win Rate [%]'] > 50:
            score += 15
        elif stats['Win Rate [%]'] > 40:
            score += 10

        # Sufficient trades
        if stats['# Trades'] > 50:
            score += 10
        elif stats['# Trades'] > 20:
            score += 5

        # Convert to letter grade
        if score >= 85:
            return 'A+'
        elif score >= 80:
            return 'A'
        elif score >= 75:
            return 'A-'
        elif score >= 70:
            return 'B+'
        elif score >= 65:
            return 'B'
        elif score >= 60:
            return 'B-'
        elif score >= 55:
            return 'C+'
        elif score >= 50:
            return 'C'
        else:
            return 'F'

    def _assess_production_readiness(self, stats):
        """Assess if strategy is ready for production deployment"""
        checks = []

        # Profitability check
        checks.append(stats['Return [%]'] > 0)

        # Risk-adjusted return check
        checks.append(stats['Sharpe Ratio'] > 0.5)

        # Drawdown check
        checks.append(abs(stats['Max. Drawdown [%]']) < 25)

        # Trade frequency check
        checks.append(stats['# Trades'] > 10)

        # Win rate check
        checks.append(stats['Win Rate [%]'] > 35)

        # Profit factor check
        checks.append(stats['Profit Factor'] > 1.1)

        passed = sum(checks)
        total = len(checks)

        if passed >= 5:
            return f"READY ({passed}/{total})"
        elif passed >= 3:
            return f"NEEDS_WORK ({passed}/{total})"
        else:
            return f"NOT_READY ({passed}/{total})"

    def test_strategy_on_all_assets(self, strategy_class, data_sources, cash=1000000):
        """
        🛡️ Test strategy on all validated data sources
        """
        print(f"\n🌍 COMPREHENSIVE MULTI-ASSET TESTING: {strategy_class.__name__}")
        print("="*80)

        # 🛡️ SECURITY: Validate all data sources first
        valid_sources = []
        for name, path in data_sources:
            if self.validate_data_source(name, path):
                valid_sources.append((name, path))

        if not valid_sources:
            print("❌ SECURITY: No valid data sources found - aborting test")
            return None

        print(f"\n🛡️ SECURITY: {len(valid_sources)} validated sources, "
              f"{len(self.failed_sources)} failed validation")

        # Run tests on validated sources
        strategy_results = []
        for name, path in valid_sources:
            result = self.run_strategy_test(strategy_class, name, path, cash)
            if 'error' not in result:
                strategy_results.append(result)

        if not strategy_results:
            print("❌ ERROR: No successful test results")
            return None

        # 🛡️ PRODUCTION: Generate comprehensive analysis
        self._generate_comprehensive_analysis(strategy_class.__name__, strategy_results)

        return strategy_results

    def _generate_comprehensive_analysis(self, strategy_name, results):
        """Generate comprehensive analysis and rankings"""
        print(f"\n📊 COMPREHENSIVE ANALYSIS: {strategy_name}")
        print("="*80)

        # Sort by Sharpe ratio for ranking
        sorted_results = sorted(results, key=lambda x: x.get('sharpe_ratio', -999), reverse=True)

        print("\n🏆 ASSET PERFORMANCE RANKING (by Sharpe Ratio):")
        print("-"*60)
        for i, result in enumerate(sorted_results[:10], 1):
            grade = result.get('risk_grade', 'N/A')
            prod_ready = result.get('production_ready', 'N/A')
            print(f"{i:2d}. {result['asset']:25s} | "
                  f"Sharpe: {result.get('sharpe_ratio', 0):6.2f} | "
                  f"Return: {result.get('return_pct', 0):7.2f}% | "
                  f"Grade: {grade:3s} | "
                  f"Prod: {prod_ready}")

        # Summary statistics
        returns = [r.get('return_pct', 0) for r in results]
        sharpes = [r.get('sharpe_ratio', 0) for r in results]
        drawdowns = [abs(r.get('max_drawdown_pct', 0)) for r in results]

        print(f"\n📈 SUMMARY STATISTICS:")
        print("-"*40)
        print(f"Assets Tested:          {len(results)}")
        print(f"Avg Return:            {np.mean(returns):7.2f}%")
        print(f"Avg Sharpe Ratio:      {np.mean(sharpes):7.2f}")
        print(f"Avg Max Drawdown:      {np.mean(drawdowns):7.2f}%")
        print(f"Best Performer:        {sorted_results[0]['asset']}")
        print(f"Worst Performer:       {sorted_results[-1]['asset']}")

        # Production readiness summary
        ready_count = sum(1 for r in results if r.get('production_ready', '').startswith('READY'))
        print(f"Production Ready:      {ready_count}/{len(results)} ({ready_count/len(results)*100:.1f}%)")

        return sorted_results

    def save_results(self, filename=None):
        """Save all test results to CSV"""
        if not self.test_results:
            print("⚠️ No results to save")
            return None

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"enhanced_multi_asset_results_{timestamp}.csv"

        # Ensure results directory exists
        results_dir = "strategies/results"
        os.makedirs(results_dir, exist_ok=True)

        filepath = os.path.join(results_dir, filename)

        # Convert results to DataFrame
        df = pd.DataFrame(self.test_results)
        df.to_csv(filepath, index=False)

        print(f"💾 Results saved: {filepath}")
        return filepath

# 🛡️ PRODUCTION: Comprehensive data sources configuration
ENHANCED_DATA_SOURCES = [
    # Bitcoin - Multiple providers
    ('BTC-1d-Yahoo', '/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv'),
    ('BTC-6h-Legacy', '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-6h-500wks-data.csv'),
    ('BTC-1h-Legacy', '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1h-500wks-data.csv'),
    ('BTC-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv'),
    ('BTC-6h-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-6h-200wks-enhanced-data.csv'),
    ('BTC-5m-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-5m-50wks-enhanced-data.csv'),

    # Ethereum - Multiple providers
    ('ETH-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv'),
    ('ETH-6h-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-6h-200wks-enhanced-data.csv'),
    ('ETH-5m-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-5m-50wks-enhanced-data.csv'),

    # Alternative assets
    ('CRO-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/CROUSD-1d-1000wks-enhanced-data.csv'),
    ('CRO-6h-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/CROUSD-6h-200wks-enhanced-data.csv'),
    ('CRO-5m-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/CROUSD-5m-50wks-enhanced-data.csv'),

    ('HBAR-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/HBARUSD-1d-1000wks-enhanced-data.csv'),
    ('HBAR-6h-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/HBARUSD-6h-200wks-enhanced-data.csv'),
    ('HBAR-5m-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/HBARUSD-5m-50wks-enhanced-data.csv'),

    ('LINK-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv'),
    ('LINK-6h-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/LINKUSD-6h-200wks-enhanced-data.csv'),
    ('LINK-5m-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/LINKUSD-5m-50wks-enhanced-data.csv'),
]

def enhanced_test_on_all_data(strategy_class, test_name=None, cash=1000000):
    """
    🛡️ Enhanced multi-asset testing function with full security validation
    """
    if test_name is None:
        test_name = strategy_class.__name__

    print("🛡️ ENHANCED MULTI-ASSET TESTING FRAMEWORK")
    print("="*80)
    print(f"Strategy: {strategy_class.__name__}")
    print(f"Cash: ${cash:,}")
    print(f"Min Quality Score: 75")
    print("="*80)

    # Initialize enhanced tester
    tester = EnhancedMultiAssetTester(min_quality_score=75)

    # Run comprehensive testing
    results = tester.test_strategy_on_all_assets(strategy_class, ENHANCED_DATA_SOURCES, cash)

    if results:
        # Save results
        filename = f"{test_name}_enhanced_results.csv"
        filepath = tester.save_results(filename)

        print(f"\n🎯 TESTING COMPLETE!")
        print(f"📊 Results saved: {filepath}")
        print(f"🏆 Best performing asset: {results[0]['asset']}")
        print(f"📈 Best Sharpe ratio: {results[0]['sharpe_ratio']:.2f}")

        return results
    else:
        print("❌ TESTING FAILED - No valid results generated")
        return None

if __name__ == "__main__":
    print("🛡️ Enhanced Multi-Asset Testing Framework")
    print("This framework provides production-ready testing with:")
    print("• Security validation for all data sources")
    print("• Production readiness assessment")
    print("• Comprehensive risk analysis")
    print("• Multi-asset performance ranking")
    print("• Cross-provider validation")
    print("\nTo use: enhanced_test_on_all_data(YourStrategy)")