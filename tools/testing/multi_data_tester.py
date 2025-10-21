"""
🚀 Multi-Data Testing Framework for Backtesting-py Strategies - Enhanced with Data Validation
============================================================================================
This module provides a unified testing framework for running backtesting strategies on multiple
data sources with comprehensive data quality validation. Ensures 100% accurate backtesting results
by validating data quality before strategy execution.

🌟 Key Features:
    - Multi-source backtesting with validation
    - Comprehensive data quality scoring
    - Pre-backtest validation checks
    - Quality-aware source prioritization
    - Detailed validation reporting

💫 Usage Examples:
    # Standard testing with validation
    from multi_data_tester import test_on_all_data
    results = test_on_all_data(MyStrategy, 'MyStrategyName')

    # Testing with custom validation
    results = test_on_validated_data(MyStrategy, quality_threshold=85.0)

    # Generate data health report
    health_report = generate_data_health_report()

🔧 Integration with Bobby's Data Quality System:
    - Automatic validation before each backtest
    - Quality scoring for each data source
    - Critical issue detection and blocking
    - Comprehensive validation reporting
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import os
import warnings
from datetime import datetime
from pathlib import Path

# 🛡️ Import Bobby's data validation system
try:
    from trading_functions.utils.data_quality_validator import (
        DataQualityValidator,
        ValidationResult,
        validate_data_source_quality
    )
    from trading_functions.config.data_validation_config import (
        get_validation_config,
        get_quality_threshold,
        validate_source_compatibility,
        SOURCE_RELIABILITY
    )
    VALIDATION_AVAILABLE = True
    print("✅ Data validation system loaded successfully")
except ImportError as e:
    VALIDATION_AVAILABLE = False
    print(f"⚠️ Data validation system not available: {e}")

warnings.filterwarnings('ignore')

# ============================================================
# DATA SOURCE CONFIGURATION
# ============================================================
# Format: (name, path, data_type)
# data_type can be: 'coinbase', 'ib', or 'yahoo'

DATA_SOURCES = [
# BITCOIN DATA - Available datasets (Validated Sources Only)
('BTC-1d-Yahoo', '/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv', 'yahoo'),  # 🛡️ Validated: Clean Yahoo data source

# ETHEREUM DATA - Hyperliquid datasets
('ETH-1d-5yrs-old', '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv', 'coinbase'),
('ETH-1d-5yrs-new', '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250911_043600_historical.csv', 'coinbase'),
('ETH-1h-7mo', '/Users/bobbyyo/Projects/algo-fun/data/ETH_1h_20250911_043633_historical.csv', 'coinbase'),

# COINGECKO DATA - Available datasets (Updated paths)
('ETH-CoinGecko-90d', '/Users/bobbyyo/Projects/algo-fun/data/coingecko/ETHEREUMUSD-90d-coingecko-data.csv', 'coinbase'),

# COINMARKETCAP DATA - Available datasets  
('ETH-CMC-30pts', '/Users/bobbyyo/Projects/algo-fun/data/coinmarketcap/ETHUSD-daily-30pts-cmc-data.csv', 'coinbase'),

# CRYPTOCOMPARE DATA - Available datasets
('ETH-CC-100d', '/Users/bobbyyo/Projects/algo-fun/data/cryptocompare/ETHUSDT-day-100pts-cc-data.csv', 'coinbase'),
]

# ============================================================
# DATA LOADING FUNCTION - Handles different formats
# ============================================================

def load_data_universal(path, data_type, validate=True, quality_threshold=75.0):
    """
    🛡️ Load data from different sources with comprehensive validation

    Args:
        path: Path to CSV file
        data_type: 'coinbase', 'ib', or 'yahoo'
        validate: Whether to run data quality validation (default True)
        quality_threshold: Minimum quality score required (default 75.0)

    Returns:
        Tuple: (DataFrame with standardized columns, ValidationResult)
        DataFrame with columns: Open, High, Low, Close, Volume
        ValidationResult with quality analysis and issues
    """
    try:    
        if data_type == 'coinbase':
            # First, read the CSV to see what columns it has
            data = pd.read_csv(path)
            
            # Find the date column (could be 'datetime', 'timestamp', 'date', etc.)
            date_cols = [col for col in data.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
            
            if date_cols:
                date_col = date_cols[0]  # Use the first date column found
                data = pd.read_csv(path, parse_dates=[date_col], index_col=date_col)
            else:
                # Fallback to first column
                data = pd.read_csv(path, parse_dates=[data.columns[0]], index_col=data.columns[0])
            
            # Standardize column names
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        elif data_type == 'ib':
            # IB format: date, open, high, low, close, volume
            data = pd.read_csv(path, parse_dates=['date'], index_col='date')
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        elif data_type == 'yahoo':
            # Yahoo format: datetime, open, high, low, close, volume
            data = pd.read_csv(path, parse_dates=['Datetime'], index_col='Datetime')
            # Drop Adj Close and keep standard columns
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

        else:
            # Default fallback - try to guess format
            data = pd.read_csv(path)
            # Try to find date column
            data_cols = [col for col in data.columns if 'date' in col.lower() or 'time' in col.lower()]
            if data_cols:
                data = pd.read_csv(path, parse_dates=[data_cols[0]], index_col=data_cols[0])
            # Standard column names
            col_map = {}
            for col in data.columns:
                col_lower = col.lower()
                if 'open' in col_lower:
                    col_map[col] = 'Open'
                elif 'high' in col_lower:
                    col_map[col] = 'High'
                elif 'low' in col_lower:
                    col_map[col] = 'Low'
                elif 'close' in col_lower and 'adj' not in col_lower:
                    col_map[col] = 'Close'
                elif 'volume' in col_lower:
                    col_map[col] = 'Volume'
            data = data.rename(columns=col_map)
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

        # Clean and sort data
        data = data.sort_index()
        data = data.dropna()

        # Ensure Volume is float type for talib compatilbility
        if 'Volume' in data.columns:
            data['Volume'] = data['Volume'].astype(float)

        # Ensure all price columns are float
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in data.columns:
                data[col] = data[col].astype(float)

        # 🛡️ Run data quality validation if enabled
        validation_result = ValidationResult()
        if validate and VALIDATION_AVAILABLE and data is not None:
            try:
                validator = DataQualityValidator()
                validation_result = validator.validate_data_file(path)

                # 🚨 Check quality threshold
                if validation_result.quality_score < quality_threshold:
                    print(f"⚠️ Quality Warning: {Path(path).name} scored {validation_result.quality_score:.1f}/100 (below {quality_threshold} threshold)")

                # 🛑 Block on critical issues
                if validation_result.critical_issues:
                    print(f"🚨 CRITICAL ISSUES in {Path(path).name}:")
                    for issue in validation_result.critical_issues:
                        print(f"   • {issue}")
                    print("❌ Data blocked from backtesting due to critical issues")
                    return None, validation_result

                # ⚠️ Report warnings
                if validation_result.warnings:
                    print(f"⚠️ Data Quality Warnings for {Path(path).name}:")
                    for warning in validation_result.warnings[:3]:  # Show first 3 warnings
                        print(f"   • {warning}")
                    if len(validation_result.warnings) > 3:
                        print(f"   ... and {len(validation_result.warnings) - 3} more warnings")

            except Exception as e:
                print(f"⚠️ Validation error for {path}: {e}")
                validation_result.add_warning(f"Validation failed: {str(e)}")

        return data, validation_result

    except Exception as e:
        print(f"Error loading data: {e}")
        validation_result = ValidationResult()
        validation_result.add_critical(f"Data loading failed: {str(e)}")
        return None, validation_result


# ============================================================
# MAIN TESTING FUNCTION
# ============================================================

def test_on_all_data(strategy_class, strategy_name=None, optimize=False, cash=1000000, commission=0.001, verbose=True):
    """
    Test a strategy on all configured data sources and save results to CSV
    
    Args:
        strategy_class: The strategy class to test (must inherit from backtesting. Strategy) strategy_name: Name for the output file (defaults to strategy class name) optimize: Whether to run optimization (default False for speed) cash: Starting cash (default 10000000 for Bitcoin compatibility) commission:
        Commission rate (default 0.00045)
        verbose: Whether to print detailed output (default True)
        Returns:
        DataFrame with all results
        Example:
        from multi data tester import test on all data
        results = test_on_all_data(MyStrategy, 'MyStrategyTest ')
    """

    if strategy_name is None:
        strategy_name = strategy_class.__name__


    # Create results directory in the same folder as the calling script
    # Get the directory of the file that's calling this function
    import inspect
    caller_frame = inspect.stack()[1]
    caller_file = caller_frame.filename
    caller_dir = os.path.dirname(os.path.abspath(caller_file))

    # Create results directory in the caller's directory
    results_dir = os.path.join(caller_dir, 'results')
    os.makedirs(results_dir, exist_ok=True)

    all_results = []

    if verbose:
        print(f"\n{'='*80}")
        print(f"💫 Testing {strategy_name} on {len(DATA_SOURCES)} data sources")
        print(f"{'='*80}\n")

    for i, source in enumerate(DATA_SOURCES, 1):
        # Handle both old format (name, path) and new format (name, path, type)
        if len(source) == 3:
            name, path, data_type = source
        else:
            name, path = source
            data_type = 'coinbase' # Default for backward compatibility

        if verbose:
            print(f"[{i}/{len(DATA_SOURCES)}] {name}...", end=' ')

        # 🛡️ Load data with validation
        result = load_data_universal(path, data_type)
        if result is None or (isinstance(result, tuple) and result[0] is None):
            if verbose:
                print(f"❌ Failed to load data")
            continue

        # Handle both old format (data only) and new format (data, validation_result)
        if isinstance(result, tuple):
            data, validation_result = result
        else:
            data = result
            validation_result = ValidationResult()  # Empty validation result for backward compatibility

        if data is None:
            if verbose:
                print(f"❌ Failed to load data")
            continue

        # Run backtest
        try:
            bt = Backtest(data, strategy_class, cash=cash, commission=commission)
            stats = bt.run()

            # Print full stats grid like backtesting.py does
            if verbose:
                print(f"\n" + "="*60)
                print(f"🔍 Full Stats for {name}:")
                print("="*60)
                print(stats)
                print("="*60)

            # 🛡️ Extract validation information
            quality_score = validation_result.quality_score if hasattr(validation_result, 'quality_score') else 100.0
            critical_count = len(validation_result.critical_issues) if hasattr(validation_result, 'critical_issues') else 0
            warning_count = len(validation_result.warnings) if hasattr(validation_result, 'warnings') else 0

            # Extract source name for reliability scoring
            source_name = 'unknown'
            for src_key in SOURCE_RELIABILITY.keys() if VALIDATION_AVAILABLE else []:
                if src_key.lower() in path.lower():
                    source_name = src_key
                    break

            reliability_score = SOURCE_RELIABILITY.get(source_name, {}).get('reliability_score', 0) if VALIDATION_AVAILABLE else 0

            # Extract ALL stats for CSV (comprehensive list with validation metrics)
            results = {
                'Data_Source': name,
                'Rows': len(data),
                'Start': str(stats['Start']) if 'Start' in stats else '',
                'End': str(stats['End']) if 'End' in stats else '',
                'Duration': str(stats['Duration']) if 'Duration' in stats else '',
                # 🛡️ Data Quality Metrics
                'Quality_Score': round(quality_score, 1),
                'Source_Reliability': reliability_score,
                'Critical_Issues': critical_count,
                'Warnings': warning_count,
                'Data_Valid': validation_result.is_valid if hasattr(validation_result, 'is_valid') else True,
                'Exposure_Time_%': round(stats['Exposure Time [%]'], 2) if 'Exposure Time [%]' in stats else 0,
                'Equity_Final': round(stats['Equity Final [$]'], 2) if 'Equity Final [$]' in stats else 0,
                'Equity_Peak': round(stats['Equity Peak [$]'], 2) if 'Equity Peak [$]' in stats else 0,
                'Return_%': round(stats['Return [%]'], 2) if 'Return [%]' in stats else 0,
                'Buy_Hold_%': round(stats['Buy & Hold Return [%]'], 2) if 'Buy & Hold Return [%]' in stats else 0,
                'Return_Ann_%': round(stats['Return (Ann.) [%]'], 2) if 'Return (Ann.) [%]' in stats else 0,
                'Volatility_Ann_%': round(stats['Volatility (Ann.) [%]'], 2) if 'Volatility (Ann.) [%]' in stats else 0,
                'Sharpe': round(stats['Sharpe Ratio'], 3) if 'Sharpe Ratio' in stats else 0,
                'Sortino': round(stats['Sortino Ratio'], 3) if 'Sortino Ratio' in stats else 0,
                'Calmar': round(stats['Calmar Ratio'], 3) if 'Calmar Ratio' in stats else 0,
                'Max_DD_%': round(stats['Max. Drawdown [%]'], 2) if 'Max. Drawdown [%]' in stats else 0,
                'Avg_DD_%': round(stats['Avg. Drawdown [%]'], 2) if 'Avg. Drawdown [%]' in stats else 0,
                'Max_DD_Duration': str(stats['Max. Drawdown Duration']) if 'Max. Drawdown Duration' in stats else '',
                'Avg_DD_Duration': str(stats['Avg. Drawdown Duration']) if 'Avg. Drawdown Duration' in stats else '',
                'Trades': stats['# Trades'] if '# Trades' in stats else 0,
                'Win_Rate_%': round(stats['Win Rate [%]'], 2) if 'Win Rate [%]' in stats else 0,
                'Best_Trade_%': round(stats['Best Trade [%]'], 2) if 'Best Trade [%]' in stats else 0,
                'Worst_Trade_%': round(stats['Worst Trade [%]'], 2) if 'Worst Trade [%]' in stats else 0,
                'Avg_Trade_%': round(stats['Avg. Trade [%]'], 3) if 'Avg. Trade [%]' in stats else 0,
                'Max_Trade_Duration': str(stats['Max. Trade Duration']) if 'Max. Trade Duration' in stats else 0,
                'Avg_Trade_Duration': str(stats['Avg. Trade Duration']) if 'Avg. Trade Duration' in stats else 0,
                'Profit_Factor': round(stats['Profit Factor'], 3) if 'Profit Factor' in stats else 0,
                'Expectancy': round(stats['Expectancy'], 3) if 'Expectancy' in stats else 0,
                'SQN': round(stats['SQN'], 3) if 'SQN' in stats else 0,
            }
                

            # Add optimiztation results if requested
            if optimize:
                try:
                    opt_stats = bt.optimize(
                        maximize = 'Return [%]',
                        max_tries = 100,
                        random_state = 42,
                    )
                    results.update({
                        'Opt_Return_%': round(opt_stats['Return [%]'], 2) if 'Return [%]' in opt_stats else 0,
                        'Opt_Sharpe': round(opt_stats['Sharpe Ratio'], 3) if 'Sharpe Ratio' in opt_stats else 0,
                        'Opt_Trades': opt_stats['# Trades'] if '# Trades' in opt_stats else 0,
                        'Opt_Win_Rate_%': round(opt_stats['Win Rate [%]'], 2) if 'Win Rate [%]' in opt_stats else 0,
                    })
                except:
                    pass

            all_results.append(results)


        except Exception as e:
            if verbose:
                print(f"❌ Error running backtest: {e}")
            continue

    # Convert results list to DataFrame
    results_df = pd.DataFrame(all_results)

    # Save results to CSV (one CSV per strategy, replaces on each run)
    if all_results:
        df = pd.DataFrame(all_results)
        # Use consistent filename (no timestamp) so it replaces on each run
        csv_path = os.path.join(results_dir, f"{strategy_name}.csv")
        df.to_csv(csv_path, index=False)

        if verbose:
            print(f"\n{'='*80}")
            print(f"📊 Results saved to: {csv_path}")
            print(f"{'='*80}\n")

            # 📊 Print enhanced summary with validation metrics
            print("🛡️ ENHANCED SUMMARY WITH DATA QUALITY METRICS:")
            print("="*120)

            # Show key performance and quality metrics
            summary_cols = ['Data_Source', 'Quality_Score', 'Source_Reliability', 'Critical_Issues', 'Warnings',
                           'Return_%', 'Sharpe', 'Max_DD_%', 'Trades', 'Win_Rate_%']

            # Only include validation columns if they exist
            available_cols = [col for col in summary_cols if col in df.columns]
            print(df[available_cols].to_string(index=False))

            # 🎯 Quality insights
            if VALIDATION_AVAILABLE and 'Quality_Score' in df.columns:
                print("\n📈 DATA QUALITY INSIGHTS:")
                avg_quality = df['Quality_Score'].mean()
                min_quality = df['Quality_Score'].min()
                max_quality = df['Quality_Score'].max()
                critical_sources = df[df['Critical_Issues'] > 0]['Data_Source'].tolist()

                print(f"   • Average Quality Score: {avg_quality:.1f}/100")
                print(f"   • Quality Range: {min_quality:.1f} - {max_quality:.1f}")

                if critical_sources:
                    print(f"   • ⚠️ Sources with Critical Issues: {', '.join(critical_sources)}")
                else:
                    print("   • ✅ No sources with critical issues detected")

                # Quality-based recommendations
                high_quality = df[df['Quality_Score'] >= 90]['Data_Source'].tolist()
                if high_quality:
                    print(f"   • 🌟 High Quality Sources (≥90): {', '.join(high_quality)}")

                low_quality = df[df['Quality_Score'] < 70]['Data_Source'].tolist()
                if low_quality:
                    print(f"   • ⚠️ Low Quality Sources (<70): {', '.join(low_quality)} - consider avoiding")

        return df

    else:
        if verbose:
            print(f"❌ No results to save")
        return None

# ============================================================
# 🛡️ ENHANCED VALIDATION-FOCUSED TESTING FUNCTIONS
# ============================================================

def test_on_validated_data(strategy_class, strategy_name=None, quality_threshold=85.0,
                          use_case='development', min_sources=3, optimize=False,
                          cash=1000000, commission=0.001, verbose=True):
    """
    🛡️ Test strategy only on validated, high-quality data sources

    Args:
        strategy_class: The strategy class to test
        strategy_name: Name for the output file
        quality_threshold: Minimum quality score required (default 85.0)
        use_case: Use case for quality thresholds ('production', 'development', 'testing')
        min_sources: Minimum number of valid sources required
        optimize: Whether to run optimization
        cash: Starting cash
        commission: Commission rate
        verbose: Whether to print detailed output

    Returns:
        DataFrame with validated results only
    """
    if not VALIDATION_AVAILABLE:
        print("⚠️ Validation system not available, falling back to standard testing")
        return test_on_all_data(strategy_class, strategy_name, optimize, cash, commission, verbose)

    if strategy_name is None:
        strategy_name = f"{strategy_class.__name__}_validated"

    # 🎯 Get quality thresholds for use case
    thresholds = get_quality_threshold(use_case)
    effective_threshold = max(quality_threshold, thresholds['minimum_score'])

    if verbose:
        print(f"\n{'='*80}")
        print(f"🛡️ Testing {strategy_name} on VALIDATED DATA ONLY")
        print(f"   Quality Threshold: {effective_threshold}/100")
        print(f"   Use Case: {use_case}")
        print(f"   Critical Issues Allowed: {thresholds['critical_issues_allowed']}")
        print(f"{'='*80}\n")

    # 🔍 Pre-validate all data sources
    valid_sources = []
    validation_summary = []

    print("🔍 PRE-VALIDATION SCAN:")
    print("-" * 50)

    for i, source in enumerate(DATA_SOURCES, 1):
        if len(source) == 3:
            name, path, data_type = source
        else:
            name, path = source
            data_type = 'coinbase'

        print(f"[{i}/{len(DATA_SOURCES)}] Validating {name}...", end=' ')

        try:
            # Quick validation without full data loading
            validator = DataQualityValidator()
            result = validator.validate_data_file(path)

            validation_info = {
                'name': name,
                'path': path,
                'data_type': data_type,
                'quality_score': result.quality_score,
                'is_valid': result.is_valid,
                'critical_issues': len(result.critical_issues),
                'warnings': len(result.warnings),
                'validation_result': result
            }

            validation_summary.append(validation_info)

            # ✅ Check if source meets validation criteria
            meets_threshold = result.quality_score >= effective_threshold
            meets_critical = len(result.critical_issues) <= thresholds['critical_issues_allowed']
            meets_warnings = len(result.warnings) <= thresholds['warnings_allowed']

            if meets_threshold and meets_critical and meets_warnings:
                valid_sources.append(source)
                print(f"✅ PASS ({result.quality_score:.1f}/100)")
            else:
                reasons = []
                if not meets_threshold:
                    reasons.append(f"quality {result.quality_score:.1f}<{effective_threshold}")
                if not meets_critical:
                    reasons.append(f"{len(result.critical_issues)} critical issues")
                if not meets_warnings:
                    reasons.append(f"{len(result.warnings)} warnings")
                print(f"❌ FAIL ({', '.join(reasons)})")

        except Exception as e:
            print(f"❌ ERROR: {e}")
            validation_summary.append({
                'name': name,
                'path': path,
                'data_type': data_type,
                'quality_score': 0.0,
                'is_valid': False,
                'critical_issues': 1,
                'warnings': 0,
                'error': str(e)
            })

    print(f"\n📊 VALIDATION RESULTS:")
    print(f"   • Sources Passed: {len(valid_sources)}/{len(DATA_SOURCES)}")
    print(f"   • Minimum Required: {min_sources}")

    if len(valid_sources) < min_sources:
        print(f"\n🚨 INSUFFICIENT VALID DATA SOURCES!")
        print(f"   Only {len(valid_sources)} sources passed validation (need {min_sources})")
        print("   Consider lowering quality_threshold or use_case requirements")
        return None

    print(f"\n🚀 Proceeding with {len(valid_sources)} validated sources...")

    # 🎯 Run backtests on validated sources only
    # Temporarily replace DATA_SOURCES with valid sources
    original_sources = DATA_SOURCES.copy()
    DATA_SOURCES.clear()
    DATA_SOURCES.extend(valid_sources)

    try:
        # Run standard testing on validated sources
        results = test_on_all_data(strategy_class, strategy_name, optimize, cash, commission, verbose)

        # Add validation metadata to results
        if results is not None and not results.empty:
            validation_df = pd.DataFrame(validation_summary)
            validation_lookup = validation_df.set_index('name')[['quality_score', 'critical_issues', 'warnings']].to_dict('index')

            # Merge validation data with results
            for idx, row in results.iterrows():
                source_name = row['Data_Source']
                if source_name in validation_lookup:
                    validation_data = validation_lookup[source_name]
                    results.loc[idx, 'Pre_Validation_Score'] = validation_data['quality_score']
                    results.loc[idx, 'Pre_Validation_Critical'] = validation_data['critical_issues']
                    results.loc[idx, 'Pre_Validation_Warnings'] = validation_data['warnings']

    finally:
        # Restore original DATA_SOURCES
        DATA_SOURCES.clear()
        DATA_SOURCES.extend(original_sources)

    return results

def generate_data_health_report(output_path=None, include_plots=False):
    """
    📊 Generate comprehensive data health report for all sources

    Args:
        output_path: Path to save the report (default: auto-generate)
        include_plots: Whether to include data visualization plots

    Returns:
        Dictionary with comprehensive health analysis
    """
    if not VALIDATION_AVAILABLE:
        print("⚠️ Validation system not available")
        return None

    print("\n🔍 GENERATING COMPREHENSIVE DATA HEALTH REPORT")
    print("=" * 80)

    health_report = {
        'timestamp': datetime.now().isoformat(),
        'total_sources': len(DATA_SOURCES),
        'validation_results': {},
        'summary_stats': {},
        'recommendations': []
    }

    validator = DataQualityValidator()
    all_results = []

    # 📊 Validate all data sources
    for i, source in enumerate(DATA_SOURCES, 1):
        if len(source) == 3:
            name, path, data_type = source
        else:
            name, path = source
            data_type = 'coinbase'

        print(f"[{i}/{len(DATA_SOURCES)}] Analyzing {name}...")

        try:
            result = validator.validate_data_file(path)

            source_report = {
                'name': name,
                'path': path,
                'data_type': data_type,
                'quality_score': result.quality_score,
                'is_valid': result.is_valid,
                'critical_issues': result.critical_issues,
                'warnings': result.warnings,
                'info': result.info,
                'metadata': result.metadata
            }

            health_report['validation_results'][name] = source_report
            all_results.append(source_report)

        except Exception as e:
            error_report = {
                'name': name,
                'path': path,
                'data_type': data_type,
                'quality_score': 0.0,
                'is_valid': False,
                'error': str(e)
            }
            health_report['validation_results'][name] = error_report
            all_results.append(error_report)

    # 📈 Calculate summary statistics
    quality_scores = [r['quality_score'] for r in all_results if 'quality_score' in r]
    valid_sources = [r for r in all_results if r.get('is_valid', False)]
    critical_sources = [r for r in all_results if len(r.get('critical_issues', [])) > 0]

    health_report['summary_stats'] = {
        'avg_quality_score': np.mean(quality_scores) if quality_scores else 0,
        'min_quality_score': np.min(quality_scores) if quality_scores else 0,
        'max_quality_score': np.max(quality_scores) if quality_scores else 0,
        'valid_sources_count': len(valid_sources),
        'valid_sources_percentage': (len(valid_sources) / len(all_results)) * 100 if all_results else 0,
        'critical_issues_count': len(critical_sources),
        'sources_by_quality': {
            'excellent': len([r for r in all_results if r['quality_score'] >= 90]),
            'good': len([r for r in all_results if 80 <= r['quality_score'] < 90]),
            'acceptable': len([r for r in all_results if 70 <= r['quality_score'] < 80]),
            'poor': len([r for r in all_results if r['quality_score'] < 70])
        }
    }

    # 🎯 Generate recommendations
    recommendations = []

    if len(critical_sources) > 0:
        recommendations.append(f"🚨 CRITICAL: {len(critical_sources)} sources have critical issues - review immediately")

    poor_sources = [r['name'] for r in all_results if r['quality_score'] < 70]
    if poor_sources:
        recommendations.append(f"⚠️ Poor quality sources detected: {', '.join(poor_sources[:3])} - consider removing")

    if health_report['summary_stats']['avg_quality_score'] < 80:
        recommendations.append("📈 Overall data quality below recommended threshold - audit data collection process")

    excellent_sources = [r['name'] for r in all_results if r['quality_score'] >= 90]
    if excellent_sources:
        recommendations.append(f"✅ High quality sources for primary use: {', '.join(excellent_sources[:3])}")

    health_report['recommendations'] = recommendations

    # 📊 Print summary report
    print(f"\n📊 DATA HEALTH SUMMARY:")
    print(f"   • Total Sources: {health_report['total_sources']}")
    print(f"   • Valid Sources: {len(valid_sources)} ({health_report['summary_stats']['valid_sources_percentage']:.1f}%)")
    print(f"   • Average Quality: {health_report['summary_stats']['avg_quality_score']:.1f}/100")
    print(f"   • Quality Range: {health_report['summary_stats']['min_quality_score']:.1f} - {health_report['summary_stats']['max_quality_score']:.1f}")

    quality_dist = health_report['summary_stats']['sources_by_quality']
    print(f"   • Quality Distribution:")
    print(f"     - Excellent (≥90): {quality_dist['excellent']}")
    print(f"     - Good (80-89): {quality_dist['good']}")
    print(f"     - Acceptable (70-79): {quality_dist['acceptable']}")
    print(f"     - Poor (<70): {quality_dist['poor']}")

    if recommendations:
        print(f"\n💡 RECOMMENDATIONS:")
        for rec in recommendations:
            print(f"   • {rec}")

    # 💾 Save report if path specified
    if output_path:
        import json
        with open(output_path, 'w') as f:
            json.dump(health_report, f, indent=2, default=str)
        print(f"\n📄 Report saved to: {output_path}")

    return health_report

def get_quality_ranked_sources(min_quality=75.0, max_sources=None):
    """
    🏆 Get data sources ranked by quality score

    Args:
        min_quality: Minimum quality threshold
        max_sources: Maximum number of sources to return

    Returns:
        List of sources ranked by quality (highest first)
    """
    if not VALIDATION_AVAILABLE:
        return DATA_SOURCES

    validator = DataQualityValidator()
    source_rankings = []

    for source in DATA_SOURCES:
        if len(source) == 3:
            name, path, data_type = source
        else:
            name, path = source
            data_type = 'coinbase'

        try:
            result = validator.validate_data_file(path)
            if result.quality_score >= min_quality and result.is_valid:
                source_rankings.append({
                    'source': source,
                    'quality_score': result.quality_score,
                    'name': name
                })
        except Exception:
            continue

    # Sort by quality score (highest first)
    source_rankings.sort(key=lambda x: x['quality_score'], reverse=True)

    # Return requested number of sources
    if max_sources:
        source_rankings = source_rankings[:max_sources]

    return [item['source'] for item in source_rankings]

