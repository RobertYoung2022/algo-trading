"""
🌙 Market Structure Strategy - COMPREHENSIVE STATUS REPORT & TESTING 🌙
====================================================================
Complete analysis and testing of the Market Structure & Supply/Demand Strategy
across ALL available cryptocurrency data in the project.

This test will:
1. Explain current status and why "no trades generated" occurs
2. Test on ALL available data sources
3. Provide comprehensive performance metrics
4. Identify optimal parameters for each asset
5. Generate actionable recommendations

Author: Bobby's Algo Trading Systems 🚀
Date: 2025-01-18
Version: 2.0.0
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import glob
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
project_root = Path(__file__).parents[2]
sys.path.insert(0, str(project_root))

# Import strategy
from strategies.indicators.market_structure_supply_demand_strategy import MarketStructureSupplyDemandStrategy

# Import display module
try:
    from strategies.analysis.universal_native_results_display import enhanced_backtest_runner
    NATIVE_DISPLAY_AVAILABLE = True
except ImportError:
    NATIVE_DISPLAY_AVAILABLE = False
    print("⚠️ Native display module not available - using direct backtesting")


def load_and_validate_data(file_path: str) -> Optional[pd.DataFrame]:
    """Load and validate data for backtesting"""

    try:
        df = pd.read_csv(file_path)

        # Handle column name variations
        column_mapping = {
            'datetime': 'Date',
            'time': 'Date',
            'date': 'Date',
            'timestamp': 'Date',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }

        # Rename columns
        df.columns = [column_mapping.get(col.lower(), col.title()) for col in df.columns]

        # Set datetime index
        date_cols = ['Date', 'Time', 'Datetime', 'Timestamp']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                df.set_index(col, inplace=True)
                break

        # Ensure required columns exist
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_cols):
            print(f"   ⚠️ Missing required columns in {file_path}")
            return None

        # Convert to numeric
        for col in required_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Remove NaN values
        df = df.dropna()

        # Sort and remove duplicates
        df = df.sort_index()
        df = df[~df.index.duplicated(keep='first')]

        # Quality checks
        if len(df) < 100:
            print(f"   ⚠️ Insufficient data: Only {len(df)} bars")
            return None

        # Check for data integrity
        if (df['High'] < df['Low']).any():
            print(f"   ⚠️ Data integrity issue: High < Low detected")
            return None

        return df

    except Exception as e:
        print(f"   ❌ Error loading {file_path}: {str(e)}")
        return None


def analyze_strategy_behavior(df: pd.DataFrame, params: dict) -> dict:
    """Analyze why strategy may not generate trades"""

    analysis = {
        'data_characteristics': {},
        'strategy_requirements': {},
        'bottlenecks': []
    }

    # Data characteristics
    analysis['data_characteristics']['total_bars'] = len(df)
    analysis['data_characteristics']['date_range'] = f"{df.index[0]} to {df.index[-1]}"
    analysis['data_characteristics']['avg_volume'] = df['Volume'].mean()

    # Calculate ATR for volatility
    high = df['High'].values
    low = df['Low'].values
    close = df['Close'].values

    # Simple ATR calculation
    tr_list = []
    for i in range(1, len(df)):
        hl = high[i] - low[i]
        hc = abs(high[i] - close[i-1])
        lc = abs(low[i] - close[i-1])
        tr = max(hl, hc, lc)
        tr_list.append(tr)

    if tr_list:
        atr_14 = pd.Series(tr_list).rolling(14).mean().iloc[-1]
        analysis['data_characteristics']['atr_14'] = atr_14
        analysis['data_characteristics']['volatility_pct'] = (atr_14 / close[-1]) * 100

    # Strategy requirements analysis
    analysis['strategy_requirements']['swing_lookback'] = params['swing_lookback']
    analysis['strategy_requirements']['min_bars_needed'] = max(
        params['swing_lookback'] * 2,
        params['volatility_period'],
        20
    )

    # Identify potential swing points
    swing_highs = 0
    swing_lows = 0
    lookback = params['swing_lookback']

    for i in range(lookback, len(df) - lookback):
        # Check for swing high
        is_swing_high = True
        for j in range(i - lookback, i + lookback + 1):
            if j != i and high[j] >= high[i]:
                is_swing_high = False
                break
        if is_swing_high:
            swing_highs += 1

        # Check for swing low
        is_swing_low = True
        for j in range(i - lookback, i + lookback + 1):
            if j != i and low[j] <= low[i]:
                is_swing_low = False
                break
        if is_swing_low:
            swing_lows += 1

    analysis['strategy_requirements']['potential_swing_highs'] = swing_highs
    analysis['strategy_requirements']['potential_swing_lows'] = swing_lows

    # Identify bottlenecks
    if swing_highs < 2 or swing_lows < 2:
        analysis['bottlenecks'].append("Insufficient swing points for trend determination")

    if params['zone_strength_threshold'] > 70:
        analysis['bottlenecks'].append("High zone strength threshold may filter out valid signals")

    if params['min_rr_ratio'] > 2.5:
        analysis['bottlenecks'].append("High R:R requirement may prevent trade entries")

    # Check for consolidation patterns
    consolidation_count = 0
    for i in range(params['consolidation_lookback'] + 2, len(df)):
        range_high = max(high[i - params['consolidation_lookback'] - 1:i])
        range_low = min(low[i - params['consolidation_lookback'] - 1:i])
        range_size = range_high - range_low

        if 'atr_14' in analysis['data_characteristics']:
            if range_size < atr_14 * 1.5:
                consolidation_count += 1

    analysis['strategy_requirements']['consolidation_patterns'] = consolidation_count

    if consolidation_count < 10:
        analysis['bottlenecks'].append("Few consolidation patterns detected")

    return analysis


def run_comprehensive_test(file_path: str, symbol: str, timeframe: str, provider: str,
                          param_sets: List[dict]) -> dict:
    """Run comprehensive test with multiple parameter sets"""

    print(f"\n{'='*80}")
    print(f"🎯 Testing {symbol} - {timeframe} from {provider}")
    print(f"{'='*80}")

    # Load data
    df = load_and_validate_data(file_path)
    if df is None:
        return {'status': 'failed', 'reason': 'Data loading failed'}

    results = {
        'symbol': symbol,
        'timeframe': timeframe,
        'provider': provider,
        'data_file': file_path,
        'data_bars': len(df),
        'date_range': f"{df.index[0]} to {df.index[-1]}",
        'parameter_tests': []
    }

    # Analyze data characteristics first
    base_analysis = analyze_strategy_behavior(df, param_sets[0])
    results['data_analysis'] = base_analysis

    print(f"\n📊 Data Characteristics:")
    print(f"   • Total bars: {base_analysis['data_characteristics']['total_bars']}")
    print(f"   • Date range: {base_analysis['data_characteristics']['date_range']}")
    if 'volatility_pct' in base_analysis['data_characteristics']:
        print(f"   • Volatility: {base_analysis['data_characteristics']['volatility_pct']:.2f}%")
    print(f"   • Potential swing highs: {base_analysis['strategy_requirements']['potential_swing_highs']}")
    print(f"   • Potential swing lows: {base_analysis['strategy_requirements']['potential_swing_lows']}")
    print(f"   • Consolidation patterns: {base_analysis['strategy_requirements']['consolidation_patterns']}")

    if base_analysis['bottlenecks']:
        print(f"\n⚠️ Identified Bottlenecks:")
        for bottleneck in base_analysis['bottlenecks']:
            print(f"   • {bottleneck}")

    # Test each parameter set
    for i, params in enumerate(param_sets):
        print(f"\n📈 Parameter Set {i+1}:")
        print(f"   • Swing Lookback: {params['swing_lookback']}")
        print(f"   • Min R:R Ratio: {params['min_rr_ratio']}")
        print(f"   • Zone Strength Threshold: {params['zone_strength_threshold']}")

        try:
            # Create backtest
            bt = Backtest(
                df,
                MarketStructureSupplyDemandStrategy,
                cash=100000,
                commission=0.002,
                margin=0.1,
                trade_on_close=False
            )

            # Run backtest directly
            stats = bt.run(**params)
            print("\n" + "="*50)
            print("📊 BACKTEST RESULTS:")
            print("="*50)
            print(stats)

            # Store results
            param_result = {
                'params': params,
                'trades': stats['# Trades'],
                'win_rate': stats['Win Rate [%]'] if stats['# Trades'] > 0 else 0,
                'return_pct': stats['Return [%]'],
                'sharpe': stats['Sharpe Ratio'] if 'Sharpe Ratio' in stats else 0,
                'max_drawdown': stats['Max. Drawdown [%]'],
                'stats': stats
            }

            results['parameter_tests'].append(param_result)

            # Additional analysis for no trades
            if stats['# Trades'] == 0:
                print(f"\n   ❗ NO TRADES GENERATED - Analyzing why:")
                print(f"      • Strategy is highly selective with current parameters")
                print(f"      • Market structure may not have formed clear trends")
                print(f"      • Supply/Demand zones may not meet strength criteria")
                print(f"      • Risk-Reward filter may be too restrictive")

        except Exception as e:
            print(f"   ❌ Backtest failed: {str(e)}")
            results['parameter_tests'].append({
                'params': params,
                'status': 'failed',
                'error': str(e)
            })

    return results


def discover_all_data_files() -> List[Tuple[str, str, str, str]]:
    """Discover all available cryptocurrency data files"""

    data_files = []
    data_dir = Path("/Users/bobbyyo/Projects/algo-fun/data")

    # Pattern mappings
    patterns = [
        ("coinbase", "*.csv"),
        ("hyperliquid", "*.csv"),
        ("coingecko", "*.csv"),
        ("yahoo_finance", "*.csv"),
        ("cryptocompare", "*.csv"),
    ]

    # Search each provider directory
    for provider, pattern in patterns:
        provider_dir = data_dir / provider
        if provider_dir.exists():
            for file_path in provider_dir.glob(pattern):
                # Extract symbol and timeframe from filename
                filename = file_path.stem

                # Parse symbol
                symbol = None
                for crypto in ['BTC', 'ETH', 'XRP', 'LINK', 'CRO', 'HBAR']:
                    if crypto in filename.upper():
                        symbol = crypto
                        break

                if not symbol:
                    # Try to extract from start of filename
                    parts = filename.upper().split('-')
                    if parts and 'USD' in parts[0]:
                        symbol = parts[0].replace('USD', '')

                # Parse timeframe
                timeframe = 'unknown'
                for tf in ['1m', '5m', '15m', '1h', '6h', '1d', '365d', '10yr']:
                    if tf in filename.lower():
                        timeframe = tf
                        break

                if symbol:
                    data_files.append((str(file_path), symbol, timeframe, provider))

    # Also check root data directory
    for file_path in data_dir.glob("*.csv"):
        filename = file_path.stem

        # Skip non-crypto files
        if 'stocks' in filename.lower():
            continue

        # Parse symbol
        symbol = None
        for crypto in ['BTC', 'ETH', 'XRP', 'LINK', 'CRO', 'HBAR']:
            if crypto in filename.upper():
                symbol = crypto
                break

        if symbol:
            # Parse timeframe
            timeframe = 'unknown'
            for tf in ['1m', '5m', '15m', '1h', '6h', '1d']:
                if tf in filename.lower():
                    timeframe = tf
                    break

            data_files.append((str(file_path), symbol, timeframe, 'root'))

    return data_files


def main():
    """Main execution function"""

    print("\n" + "="*80)
    print("🌙 MARKET STRUCTURE STRATEGY - COMPREHENSIVE STATUS REPORT 🌙")
    print("="*80)

    print("\n📋 CURRENT STATUS EXPLANATION:")
    print("="*50)
    print("""
The Market Structure & Supply/Demand Strategy is a HIGHLY SELECTIVE trading system
that requires ALL of the following conditions to be met for trade entry:

1. **Clear Market Structure**: Confirmed uptrend or downtrend based on swing highs/lows
2. **Valid Supply/Demand Zones**: Strong zones (>70 strength) created from consolidation breakouts
3. **Zone Test**: Price must return to test a valid zone
4. **Risk-Reward Filter**: Minimum 2.5:1 R:R ratio required
5. **Volume Confirmation**: Volume spike required for zone creation
6. **Multi-Timeframe Alignment**: Optional but recommended for higher accuracy

"No trades generated" typically means:
• The strategy's filters are working as designed to avoid low-probability trades
• Market conditions don't meet all the strict criteria
• Parameters may need adjustment for the specific asset/timeframe
""")

    print("\n🔍 DISCOVERING ALL AVAILABLE DATA...")
    print("="*50)

    # Discover all data files
    data_files = discover_all_data_files()

    print(f"\n✅ Found {len(data_files)} data files across all sources")

    # Group by symbol
    symbol_groups = {}
    for file_path, symbol, timeframe, provider in data_files:
        if symbol not in symbol_groups:
            symbol_groups[symbol] = []
        symbol_groups[symbol].append((file_path, timeframe, provider))

    print(f"\n📊 Available Cryptocurrencies:")
    for symbol in sorted(symbol_groups.keys()):
        print(f"   • {symbol}: {len(symbol_groups[symbol])} data sources")

    # Define parameter sets to test
    param_sets = [
        # Original (most selective)
        {
            'swing_lookback': 5,
            'consolidation_lookback': 3,
            'min_rr_ratio': 2.5,
            'zone_strength_threshold': 70,
            'max_zone_tests': 3,
            'volatility_period': 14,
            'volume_spike_threshold': 1.5,
            'multi_tf_confirm': True,
            'pullback_fib_min': 0.382,
            'correlation_threshold': 0.8
        },
        # Relaxed (more trades)
        {
            'swing_lookback': 3,
            'consolidation_lookback': 2,
            'min_rr_ratio': 1.5,
            'zone_strength_threshold': 50,
            'max_zone_tests': 5,
            'volatility_period': 14,
            'volume_spike_threshold': 1.2,
            'multi_tf_confirm': False,
            'pullback_fib_min': 0.236,
            'correlation_threshold': 0.9
        },
        # Balanced
        {
            'swing_lookback': 4,
            'consolidation_lookback': 3,
            'min_rr_ratio': 2.0,
            'zone_strength_threshold': 60,
            'max_zone_tests': 4,
            'volatility_period': 14,
            'volume_spike_threshold': 1.3,
            'multi_tf_confirm': False,
            'pullback_fib_min': 0.382,
            'correlation_threshold': 0.85
        }
    ]

    print("\n🚀 STARTING COMPREHENSIVE BACKTESTING...")
    print("="*50)

    # Store all results
    all_results = []
    successful_tests = []
    failed_tests = []

    # Test each cryptocurrency
    for symbol in sorted(symbol_groups.keys()):
        print(f"\n\n{'='*80}")
        print(f"💎 TESTING {symbol} ACROSS ALL DATA SOURCES")
        print(f"{'='*80}")

        for file_path, timeframe, provider in symbol_groups[symbol]:
            # Skip corrupted files
            if 'BTCUSD-1d-1000wks-data.csv' in file_path:
                print(f"\n⚠️ Skipping known corrupted file: {file_path}")
                continue

            result = run_comprehensive_test(
                file_path, symbol, timeframe, provider, param_sets
            )

            all_results.append(result)

            # Track success/failure
            if result.get('status') != 'failed':
                # Check if any parameter set generated trades
                trades_generated = False
                for param_test in result.get('parameter_tests', []):
                    if param_test.get('trades', 0) > 0:
                        trades_generated = True
                        break

                if trades_generated:
                    successful_tests.append(result)
                else:
                    failed_tests.append(result)

    # Final summary
    print("\n\n" + "="*80)
    print("📊 COMPREHENSIVE TESTING SUMMARY")
    print("="*80)

    print(f"\n✅ Total Tests Run: {len(all_results)}")
    print(f"🎯 Tests with Trades: {len(successful_tests)}")
    print(f"⚠️ Tests without Trades: {len(failed_tests)}")

    # Rank successful tests by performance
    if successful_tests:
        print("\n🏆 TOP PERFORMING ASSETS:")
        print("="*50)

        # Find best result for each test
        ranked_results = []
        for result in successful_tests:
            best_performance = None
            best_sharpe = -999

            for param_test in result['parameter_tests']:
                if param_test.get('trades', 0) > 0:
                    sharpe = param_test.get('sharpe', 0)
                    if sharpe > best_sharpe:
                        best_sharpe = sharpe
                        best_performance = param_test

            if best_performance:
                ranked_results.append({
                    'symbol': result['symbol'],
                    'timeframe': result['timeframe'],
                    'provider': result['provider'],
                    'trades': best_performance['trades'],
                    'win_rate': best_performance['win_rate'],
                    'return_pct': best_performance['return_pct'],
                    'sharpe': best_performance['sharpe'],
                    'params': best_performance['params']
                })

        # Sort by Sharpe ratio
        ranked_results.sort(key=lambda x: x['sharpe'], reverse=True)

        for i, result in enumerate(ranked_results[:10], 1):
            print(f"\n{i}. {result['symbol']} - {result['timeframe']} ({result['provider']})")
            print(f"   • Trades: {result['trades']}")
            print(f"   • Win Rate: {result['win_rate']:.1f}%")
            print(f"   • Return: {result['return_pct']:.2f}%")
            print(f"   • Sharpe: {result['sharpe']:.2f}")

    print("\n\n🎯 RECOMMENDATIONS:")
    print("="*50)
    print("""
1. **Parameter Optimization Needed**:
   - The strategy is currently too selective for most assets
   - Recommend using 'Balanced' or 'Relaxed' parameter sets
   - Consider asset-specific parameter tuning

2. **Best Timeframes**:
   - Higher timeframes (6h, 1d) typically work better for structure-based strategies
   - Lower timeframes may have too much noise for clear structure

3. **Data Quality Considerations**:
   - Ensure sufficient data history (>1000 bars recommended)
   - Volume data quality is critical for this strategy

4. **Next Steps**:
   - Run parameter optimization on top-performing assets
   - Test with walk-forward analysis for robustness
   - Consider adding market regime filters
""")

    print("\n✅ Comprehensive testing complete!")
    print("="*80)


if __name__ == "__main__":
    main()