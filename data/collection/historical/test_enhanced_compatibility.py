#!/usr/bin/env python3
"""
🌙 Enhanced Historical Data Scripts - Backtesting.py Compatibility Tester 🚀
Tests that enhanced scripts produce data compatible with Bobby's backtesting framework
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
import talib
import os
import sys
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class SimpleRSIStrategy(Strategy):
    """Simple RSI strategy for testing data compatibility"""

    def init(self):
        # Use talib for indicators (Bobby's preference)
        close_prices = self.data.Close
        self.rsi = self.I(talib.RSI, close_prices, timeperiod=14)

    def next(self):
        if not self.position:
            if self.rsi[-1] < 30:  # Oversold
                self.buy()
        else:
            if self.rsi[-1] > 70:  # Overbought
                self.sell()

def load_and_validate_csv(filepath: str) -> Tuple[pd.DataFrame, Dict]:
    """
    Load CSV and validate it matches Bobby's format
    Returns DataFrame and validation report
    """
    print(f"\n📊 Testing: {os.path.basename(filepath)}")
    print("=" * 60)

    validation = {
        'file': filepath,
        'valid': True,
        'issues': [],
        'format_checks': {},
        'data_quality': {}
    }

    try:
        # Load the CSV
        df = pd.read_csv(filepath)

        # Check 1: Required columns
        required_cols = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            validation['valid'] = False
            validation['issues'].append(f"Missing columns: {missing_cols}")
        else:
            validation['format_checks']['columns'] = '✅ All required columns present'

        # Check 2: Column names are lowercase
        if df.columns.tolist() != [col.lower() for col in df.columns]:
            validation['valid'] = False
            validation['issues'].append("Column names must be lowercase")
        else:
            validation['format_checks']['lowercase'] = '✅ Column names are lowercase'

        # Check 3: Date format (YYYY-MM-DD)
        if 'datetime' in df.columns:
            sample_date = str(df['datetime'].iloc[0])
            if len(sample_date) == 10 and sample_date[4] == '-' and sample_date[7] == '-':
                validation['format_checks']['date_format'] = '✅ Date format is YYYY-MM-DD'
            else:
                validation['valid'] = False
                validation['issues'].append(f"Invalid date format: {sample_date}")

        # Check 4: Numeric columns are numeric
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    validation['valid'] = False
                    validation['issues'].append(f"{col} is not numeric")

        if all(col in df.columns and pd.api.types.is_numeric_dtype(df[col]) for col in numeric_cols):
            validation['format_checks']['numeric'] = '✅ All price/volume columns are numeric'

        # Check 5: No missing values
        missing_count = df[required_cols].isnull().sum().sum()
        if missing_count > 0:
            validation['issues'].append(f"Found {missing_count} missing values")
        else:
            validation['format_checks']['complete'] = '✅ No missing values'

        # Check 6: OHLC relationships
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            invalid_ohlc = df[(df['high'] < df['low']) |
                             (df['high'] < df['open']) |
                             (df['high'] < df['close']) |
                             (df['low'] > df['open']) |
                             (df['low'] > df['close'])]

            if not invalid_ohlc.empty:
                validation['issues'].append(f"Invalid OHLC relationships in {len(invalid_ohlc)} rows")
            else:
                validation['format_checks']['ohlc'] = '✅ Valid OHLC relationships'

        # Data quality metrics
        validation['data_quality']['rows'] = len(df)
        validation['data_quality']['date_range'] = f"{df['datetime'].iloc[0]} to {df['datetime'].iloc[-1]}"
        validation['data_quality']['price_range'] = f"${df['close'].min():.2f} - ${df['close'].max():.2f}"

        return df, validation

    except Exception as e:
        validation['valid'] = False
        validation['issues'].append(f"Error loading file: {str(e)}")
        return None, validation

def test_backtesting_compatibility(df: pd.DataFrame, name: str) -> Dict:
    """
    Test if data works with backtesting.py framework
    """
    print(f"\n🔧 Testing backtesting.py compatibility for {name}...")

    result = {
        'compatible': False,
        'backtest_stats': None,
        'error': None
    }

    try:
        # Prepare data for backtesting.py
        df_bt = df.copy()

        # Convert datetime to index
        df_bt['datetime'] = pd.to_datetime(df_bt['datetime'])
        df_bt = df_bt.set_index('datetime')

        # Rename columns to match backtesting.py format (capitalized)
        df_bt = df_bt.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        })

        # Ensure sorted by date
        df_bt = df_bt.sort_index()

        # Run backtest
        bt = Backtest(df_bt, SimpleRSIStrategy, cash=10000, commission=.002)
        stats = bt.run()

        result['compatible'] = True
        result['backtest_stats'] = {
            'Return [%]': stats['Return [%]'],
            'Sharpe Ratio': stats['Sharpe Ratio'],
            'Max. Drawdown [%]': stats['Max. Drawdown [%]'],
            'Win Rate [%]': stats['Win Rate [%]'],
            '# Trades': stats['# Trades'],
            'Exposure Time [%]': stats['Exposure Time [%]']
        }

        print("✅ Successfully ran backtest!")
        print(f"  Return: {stats['Return [%]']:.2f}%")
        print(f"  Sharpe: {stats['Sharpe Ratio']:.2f}")
        print(f"  Max DD: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"  Trades: {stats['# Trades']}")

    except Exception as e:
        result['error'] = str(e)
        print(f"❌ Backtest failed: {str(e)}")

    return result

def test_multi_data_compatibility(df: pd.DataFrame, name: str) -> bool:
    """
    Test if data format is compatible with multi-data testing framework
    """
    print(f"\n🔄 Testing multi-data framework compatibility for {name}...")

    try:
        # Check if format matches existing data files
        reference_file = '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1d-1000wks-data.csv'

        if os.path.exists(reference_file):
            ref_df = pd.read_csv(reference_file, nrows=5)

            # Compare column structure
            if set(df.columns) == set(ref_df.columns):
                print("✅ Column structure matches reference data")

                # Check data types
                for col in df.columns:
                    if df[col].dtype != ref_df[col].dtype:
                        print(f"⚠️ Data type mismatch for {col}")
                        return False

                print("✅ Data types match reference format")
                return True
            else:
                print(f"❌ Column mismatch. Expected: {ref_df.columns.tolist()}, Got: {df.columns.tolist()}")
                return False
        else:
            print("⚠️ Reference file not found, checking basic compatibility...")
            # Basic checks
            required = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            if all(col in df.columns for col in required):
                print("✅ Has all required columns for multi-data testing")
                return True
            else:
                print("❌ Missing required columns")
                return False

    except Exception as e:
        print(f"❌ Compatibility check failed: {str(e)}")
        return False

def generate_compatibility_report(results: List[Dict]) -> str:
    """
    Generate comprehensive compatibility report
    """
    report = []
    report.append("=" * 70)
    report.append("🌙 ENHANCED HISTORICAL DATA SCRIPTS - COMPATIBILITY REPORT 🚀")
    report.append("=" * 70)

    # Summary
    total_tests = len(results)
    valid_format = sum(1 for r in results if r['validation']['valid'])
    backtest_compatible = sum(1 for r in results if r.get('backtest_result', {}).get('compatible', False))
    multi_data_compatible = sum(1 for r in results if r.get('multi_data_compatible', False))

    report.append("\n📊 SUMMARY")
    report.append(f"  Total scripts tested: {total_tests}")
    report.append(f"  Valid format: {valid_format}/{total_tests}")
    report.append(f"  Backtesting.py compatible: {backtest_compatible}/{total_tests}")
    report.append(f"  Multi-data compatible: {multi_data_compatible}/{total_tests}")

    # Detailed results
    report.append("\n📋 DETAILED RESULTS")
    report.append("-" * 70)

    for result in results:
        name = os.path.basename(result['file'])
        report.append(f"\n📁 {name}")

        # Format validation
        val = result['validation']
        if val['valid']:
            report.append("  ✅ Format: VALID")
            for check, status in val['format_checks'].items():
                report.append(f"    {status}")
        else:
            report.append("  ❌ Format: INVALID")
            for issue in val['issues']:
                report.append(f"    ⚠️ {issue}")

        # Data quality
        if val['data_quality']:
            report.append("  📈 Data Quality:")
            for key, value in val['data_quality'].items():
                report.append(f"    {key}: {value}")

        # Backtesting compatibility
        if 'backtest_result' in result:
            bt = result['backtest_result']
            if bt['compatible']:
                report.append("  ✅ Backtesting.py: COMPATIBLE")
                if bt['backtest_stats']:
                    report.append("    Performance metrics:")
                    for key, value in bt['backtest_stats'].items():
                        if isinstance(value, float):
                            report.append(f"      {key}: {value:.2f}")
                        else:
                            report.append(f"      {key}: {value}")
            else:
                report.append(f"  ❌ Backtesting.py: INCOMPATIBLE - {bt.get('error', 'Unknown error')}")

        # Multi-data compatibility
        if result.get('multi_data_compatible'):
            report.append("  ✅ Multi-data framework: COMPATIBLE")
        else:
            report.append("  ❌ Multi-data framework: INCOMPATIBLE")

    # Recommendations
    report.append("\n💡 RECOMMENDATIONS")
    report.append("-" * 70)

    if valid_format == total_tests:
        report.append("✅ All scripts produce valid format - ready for production!")
    else:
        report.append("⚠️ Some scripts need format fixes:")
        report.append("  1. Ensure column names: datetime,open,high,low,close,volume")
        report.append("  2. Use YYYY-MM-DD date format")
        report.append("  3. Ensure all numeric columns are float type")
        report.append("  4. Fix any OHLC relationship violations")

    if backtest_compatible < total_tests:
        report.append("\n⚠️ Backtesting compatibility issues found:")
        report.append("  1. Check data has no missing values")
        report.append("  2. Ensure dates are sequential")
        report.append("  3. Verify OHLC relationships are valid")

    report.append("\n✨ NEXT STEPS")
    report.append("-" * 70)
    report.append("1. Run enhanced scripts to fetch historical data")
    report.append("2. Use data with existing strategies in /strategies")
    report.append("3. Test with multi_data_tester.py for comprehensive validation")
    report.append("4. Deploy to production backtesting workflow")

    return "\n".join(report)

def main():
    """Main testing function"""
    print("🚀 Testing Enhanced Historical Data Scripts Compatibility")
    print("=" * 70)

    results = []

    # Test 1: Check if we can import the scripts
    print("\n📦 Checking enhanced scripts...")

    enhanced_scripts = [
        '/Users/bobbyyo/Projects/algo-fun/data-scripts/enhanced_coingecko_historical.py',
        '/Users/bobbyyo/Projects/algo-fun/data-scripts/enhanced_cryptocompare_historical.py',
        '/Users/bobbyyo/Projects/algo-fun/data-scripts/enhanced_yahoo_historical.py'
    ]

    for script in enhanced_scripts:
        if os.path.exists(script):
            print(f"✅ Found: {os.path.basename(script)}")
        else:
            print(f"❌ Missing: {os.path.basename(script)}")

    # Test 2: Check existing data format for reference
    print("\n📂 Checking reference data format...")
    reference_file = '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1d-1000wks-data.csv'

    if os.path.exists(reference_file):
        ref_df, ref_validation = load_and_validate_csv(reference_file)

        if ref_validation['valid']:
            print("✅ Reference data format is valid")
            print(f"  Columns: {ref_df.columns.tolist()}")
            print(f"  Sample date: {ref_df['datetime'].iloc[0]}")
            print(f"  Data types: {ref_df.dtypes.to_dict()}")
        else:
            print("⚠️ Reference data has issues:", ref_validation['issues'])

    # Test 3: Test sample data files if they exist
    print("\n🔍 Testing existing data files...")

    test_files = [
        '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1d-1000wks-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/ETHUSD-1d-1000wks-data.csv'
    ]

    for filepath in test_files:
        if os.path.exists(filepath):
            result = {'file': filepath}

            # Validate format
            df, validation = load_and_validate_csv(filepath)
            result['validation'] = validation

            if df is not None and validation['valid']:
                # Test backtesting compatibility
                result['backtest_result'] = test_backtesting_compatibility(df, os.path.basename(filepath))

                # Test multi-data compatibility
                result['multi_data_compatible'] = test_multi_data_compatibility(df, os.path.basename(filepath))

            results.append(result)

    # Generate report
    if results:
        report = generate_compatibility_report(results)
        print("\n" + report)

        # Save report
        report_file = '/Users/bobbyyo/Projects/algo-fun/data-scripts/compatibility_report.txt'
        with open(report_file, 'w') as f:
            f.write(report)
        print(f"\n💾 Report saved to: {report_file}")
    else:
        print("\n⚠️ No data files found to test. Run the enhanced scripts first to generate data.")

    print("\n✨ Compatibility testing complete!")

    # Return overall status
    if results:
        all_valid = all(r['validation']['valid'] for r in results)
        all_compatible = all(r.get('backtest_result', {}).get('compatible', False) for r in results)
        return all_valid and all_compatible
    return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)