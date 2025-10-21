"""
🌙 Universal Strategy Testing Template 🌙
==========================================
Template for creating new strategy testing frameworks that comply with
Bobby's requirements for FULL NATIVE BACKTESTING.PY RESULTS DISPLAY.

This template MUST be used for ALL new strategy testing to ensure
complete native results display for every individual test.

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path
from datetime import datetime
import warnings

# 🚨 MANDATORY: Import universal native results display
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import enhanced_backtest_runner, create_data_source_info

# 🛡️ SECURITY: Import data validation for input security
try:
    from trading_functions import DataQualityValidator, validate_data_source_quality
except ImportError:
    print("⚠️ Trading functions not available - using basic validation")
    DataQualityValidator = None

warnings.filterwarnings('ignore')

# 🚨 REPLACE THIS: Import your strategy class
# from your_strategy_module import YourStrategyClass


class UniversalStrategyTester:
    """
    🚀 Universal Strategy Testing Framework with MANDATORY Native Results Display 🚀

    This class ensures ALL strategy testing shows complete backtesting.py native results
    for every individual test, following Bobby's requirements.
    """

    def __init__(self, strategy_class, strategy_name, base_dir="/Users/bobbyyo/Projects/algo-fun"):
        """
        Initialize the universal strategy tester.

        Args:
            strategy_class: The Strategy class to test
            strategy_name: Human-readable name for the strategy
            base_dir: Base directory for the project
        """
        self.strategy_class = strategy_class
        self.strategy_name = strategy_name
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / "data"
        self.results_dir = self.base_dir / "strategies" / "results"
        self.results_dir.mkdir(exist_ok=True)

        # Default backtest parameters
        self.default_backtest_params = {
            'cash': 10000,
            'commission': 0.002,
            'exclusive_orders': True,
            'trade_on_close': False
        }

        # Results storage
        self.all_results = []

    def discover_data_sources(self, patterns=None):
        """
        🔍 Auto-discover data sources for testing 🔍

        Args:
            patterns: List of file patterns to search for (default: common timeframes)
        """
        if patterns is None:
            patterns = [
                "**/*1m*.csv",
                "**/*5m*.csv",
                "**/*15m*.csv",
                "**/*1h*.csv",
                "**/*6h*.csv",
                "**/*1d*.csv"
            ]

        print(f"\n{'='*80}")
        print(f"🔍 DISCOVERING DATA SOURCES FOR {self.strategy_name}")
        print(f"{'='*80}")

        data_sources = []

        for pattern in patterns:
            files = list(self.data_dir.glob(pattern))
            for file in files:
                # Extract metadata from filename and path
                filename = file.name
                parts = filename.split('-')

                if len(parts) >= 2:
                    symbol = parts[0]
                    timeframe = parts[1] if len(parts) > 1 else 'unknown'

                    # Determine provider from path
                    provider = 'unknown'
                    file_str = str(file).lower()
                    for prov in ['coinbase', 'yahoo', 'coingecko', 'hyperliquid', 'cryptocompare', 'coinmarketcap']:
                        if prov in file_str:
                            provider = prov
                            break

                    data_sources.append({
                        'path': str(file),
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'provider': provider,
                        'filename': filename
                    })

        print(f"✅ Found {len(data_sources)} potential data sources")

        # Group by symbol and provider for overview
        symbols = set([d['symbol'] for d in data_sources])
        providers = set([d['provider'] for d in data_sources])

        print(f"\n📊 Available symbols: {', '.join(sorted(symbols))}")
        print(f"🏢 Data providers: {', '.join(sorted(providers))}")

        return data_sources

    def validate_data_quality(self, df, source_info, min_score=75):
        """
        ✅ Validate data quality using Bobby's validation system ✅
        """
        if DataQualityValidator is None:
            # Basic validation when trading_functions not available
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in df.columns for col in required_cols):
                return False, 0, "Missing required columns"

            if len(df) < 100:
                return False, 50, "Insufficient data"

            return True, 80, "Basic validation passed"

        try:
            # Use Bobby's validation system
            validator = DataQualityValidator(df)
            validation_result = validator.validate()

            if validation_result.overall_score < min_score:
                return False, validation_result.overall_score, f"Quality score too low: {validation_result.overall_score}"

            return True, validation_result.overall_score, "Validation passed"

        except Exception as e:
            print(f"⚠️ Validation error: {e}")
            return False, 0, f"Validation error: {e}"

    def load_and_prepare_data(self, data_path):
        """
        📂 Load and prepare data for backtesting 📂
        """
        try:
            # Load CSV with flexible column handling
            df = pd.read_csv(data_path)

            # Handle different datetime column names
            datetime_cols = ['datetime', 'timestamp', 'Date', 'date', 'Timestamp']
            datetime_col = None

            for col in datetime_cols:
                if col in df.columns:
                    datetime_col = col
                    break

            if datetime_col:
                df[datetime_col] = pd.to_datetime(df[datetime_col])
                df.set_index(datetime_col, inplace=True)

            # Standardize column names
            column_mapping = {
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }
            df.rename(columns=column_mapping, inplace=True)

            # Basic cleaning
            df = df.dropna()
            df = df.sort_index()

            return df

        except Exception as e:
            print(f"❌ Error loading {data_path}: {e}")
            return None

    def run_single_test(self, data_source):
        """
        🎯 Run single strategy test with MANDATORY NATIVE RESULTS DISPLAY 🎯
        """
        print(f"\n[{len(self.all_results) + 1}] Testing {data_source['symbol']} ({data_source['timeframe']}) from {data_source['provider']}...")

        # Load data
        df = self.load_and_prepare_data(data_source['path'])
        if df is None:
            return None

        # Validate data quality
        is_valid, quality_score, message = self.validate_data_quality(df, data_source)
        print(f"   📊 Data quality score: {quality_score:.1f}/100")

        if not is_valid:
            print(f"   ❌ {message}")
            return None

        print(f"   ✅ Data validated successfully")

        try:
            # 🚨 MANDATORY: Use enhanced_backtest_runner for FULL NATIVE RESULTS DISPLAY
            summary_stats, full_stats = enhanced_backtest_runner(
                data=df,
                strategy_class=self.strategy_class,
                data_source_info=data_source,
                strategy_name=self.strategy_name,
                **self.default_backtest_params
            )

            # Store results for CSV export
            self.all_results.append(summary_stats)

            return summary_stats, full_stats

        except Exception as e:
            print(f"   ❌ Backtest error: {e}")
            return None

    def run_comprehensive_test(self, data_sources=None, max_tests=None):
        """
        🌍 Run comprehensive multi-asset testing with FULL NATIVE RESULTS 🌍
        """
        if data_sources is None:
            data_sources = self.discover_data_sources()

        if max_tests:
            data_sources = data_sources[:max_tests]

        print(f"\n{'='*80}")
        print(f"🚀 STARTING COMPREHENSIVE TESTING: {self.strategy_name}")
        print(f"{'='*80}")
        print(f"📊 Testing {len(data_sources)} data sources...")

        successful_tests = 0
        failed_tests = 0

        for i, source in enumerate(data_sources, 1):
            result = self.run_single_test(source)

            if result:
                successful_tests += 1
            else:
                failed_tests += 1

        # Generate summary
        print(f"\n{'='*80}")
        print(f"📊 COMPREHENSIVE TESTING COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Successful tests: {successful_tests}")
        print(f"❌ Failed tests: {failed_tests}")
        print(f"📊 Total assets tested: {len(set(r['Symbol'] for r in self.all_results))}")

        # Save results to CSV
        if self.all_results:
            self.save_results_to_csv()
            self.display_performance_ranking()

        return self.all_results

    def save_results_to_csv(self):
        """
        💾 Save results to CSV file 💾
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.strategy_name.replace(' ', '_').lower()}_results_{timestamp}.csv"
        filepath = self.results_dir / filename

        df = pd.DataFrame(self.all_results)
        df.to_csv(filepath, index=False)

        print(f"\n💾 Results saved to: {filepath}")
        return filepath

    def display_performance_ranking(self):
        """
        🏆 Display performance ranking across all tested assets 🏆
        """
        if not self.all_results:
            return

        df = pd.DataFrame(self.all_results)
        df_sorted = df.sort_values('Return_%', ascending=False)

        print(f"\n{'='*80}")
        print(f"🏆 ASSET PERFORMANCE RANKING - {self.strategy_name}")
        print(f"{'='*80}")

        print(f"📊 TOP 5 PERFORMERS BY RETURN:")
        for i, row in df_sorted.head(5).iterrows():
            print(f"   {row['Symbol']} ({row['Timeframe']}, {row['Provider']}): "
                  f"Return={row['Return_%']:.2f}%, Sharpe={row['Sharpe']:.3f}, Trades={row['Trades']}")

        if len(df_sorted) > 5:
            print(f"\n📊 BOTTOM 3 PERFORMERS:")
            for i, row in df_sorted.tail(3).iterrows():
                print(f"   {row['Symbol']} ({row['Timeframe']}, {row['Provider']}): "
                      f"Return={row['Return_%']:.2f}%, Sharpe={row['Sharpe']:.3f}, Trades={row['Trades']}")

        # Overall statistics
        print(f"\n📊 OVERALL STATISTICS:")
        print(f"   Average Return: {df['Return_%'].mean():.2f}%")
        print(f"   Average Sharpe: {df['Sharpe'].mean():.3f}")
        print(f"   Average Max Drawdown: {df['Max_DD_%'].mean():.2f}%")
        print(f"   Total Trades: {df['Trades'].sum()}")


def main():
    """
    🌙 Main execution function - TEMPLATE EXAMPLE 🌙

    REPLACE THIS SECTION WITH YOUR STRATEGY-SPECIFIC CODE
    """
    print("🌙💫🚀 UNIVERSAL STRATEGY TESTING TEMPLATE 🚀💫🌙")
    print("=" * 80)
    print("⚠️ THIS IS A TEMPLATE - REPLACE WITH YOUR STRATEGY CLASS")
    print("=" * 80)

    # 🚨 REPLACE THIS: Import and use your actual strategy
    # from your_strategy_module import YourStrategyClass
    #
    # tester = UniversalStrategyTester(
    #     strategy_class=YourStrategyClass,
    #     strategy_name="Your Strategy Name"
    # )
    #
    # # Run comprehensive testing
    # results = tester.run_comprehensive_test()
    #
    # print("✅ Testing complete! Check the results CSV file.")


if __name__ == "__main__":
    main()


# 🌙💫🚀 TEMPLATE USAGE INSTRUCTIONS 🚀💫🌙
"""
TO USE THIS TEMPLATE FOR A NEW STRATEGY:

1. Copy this file to your strategy directory
2. Rename it appropriately (e.g., test_your_strategy_multi_asset.py)
3. Replace the import statement with your strategy class
4. Update the main() function to use your strategy
5. Customize backtest parameters if needed
6. Run the script

EXAMPLE:
```python
from my_awesome_strategy import MyAwesomeStrategy

tester = UniversalStrategyTester(
    strategy_class=MyAwesomeStrategy,
    strategy_name="My Awesome Strategy"
)

results = tester.run_comprehensive_test()
```

This template ensures:
- ✅ Full native backtesting.py results display for every test
- ✅ Comprehensive multi-asset testing
- ✅ Data quality validation
- ✅ Results saved to CSV
- ✅ Performance rankings
- ✅ Bobby's emoji style and documentation standards

🚨 NEVER use direct bt.run() calls - always use enhanced_backtest_runner()
"""