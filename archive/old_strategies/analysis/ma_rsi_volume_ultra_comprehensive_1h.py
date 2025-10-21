"""
🚀 ULTRA-COMPREHENSIVE MA-RSI-VOLUME HYBRID STRATEGY 1H TESTING 🚀
====================================================================
Maximum-depth analysis of MA-RSI-Volume Hybrid Strategy across ALL available
1-hour cryptocurrency data with extensive parameter optimization, market regime
analysis, and production readiness assessment.

This script provides:
- Complete 1H data discovery and validation
- Conservative & Adaptive strategy variations
- Parameter sensitivity analysis with multiple thresholds
- Market condition analysis per asset
- Signal quality assessment and optimization
- Production deployment recommendations
- Risk-adjusted performance rankings

Author: Bobby's Algo Trading System 🌙💫
Date: 2025-01-18
Version: 3.0.0 - Ultra-Comprehensive Edition
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.append('/Users/bobbyyo/Projects/algo-fun')

# 🚨 MANDATORY IMPORTS FOR NATIVE RESULTS DISPLAY
from strategies.analysis.universal_native_results_display import (
    enhanced_backtest_runner,
    create_data_source_info,
    display_full_native_results,
    display_trades_summary
)

# Import strategy classes
from strategies.indicators.ma_rsi_volume_hybrid_strategy import (
    MARSIVolumeHybridStrategy,
    validate_data_for_strategy,
    analyze_strategy_signals
)
from strategies.indicators.ma_rsi_volume_adaptive_strategy import MARSIVolumeAdaptiveStrategy

# Import validation and analysis functions
from trading_functions import (
    DataQualityValidator,
    validate_data_source_quality,
    calculate_comprehensive_strategy_metrics,
    calculate_risk_reward_ratio,
    generate_risk_report
)

from backtesting import Backtest
import matplotlib.pyplot as plt
# import seaborn as sns  # Optional - not critical for testing

# 🎯 Comprehensive Testing Parameters
PARAMETER_VARIATIONS = {
    'rsi_oversold': [30, 35, 40],
    'rsi_overbought': [60, 65, 70],
    'volume_spike': [1.0, 1.1, 1.2, 1.5],
    'ma_period': [10, 20, 30],
    'take_profit': [0.03, 0.05, 0.07],  # 3%, 5%, 7%
    'stop_loss': [0.015, 0.025, 0.035]  # 1.5%, 2.5%, 3.5%
}

# 📊 Data Quality Requirements
MIN_QUALITY_SCORE = 75
MIN_DATA_POINTS = 500
MAX_NULL_PERCENTAGE = 1.0

# 🌙 Initialize quality validator
validator = DataQualityValidator()

print("=" * 100)
print("🚀 ULTRA-COMPREHENSIVE MA-RSI-VOLUME HYBRID 1H TESTING FRAMEWORK 🚀")
print("=" * 100)
print(f"⏰ Execution Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)


def discover_all_crypto_data():
    """
    🔍 Discover ALL available cryptocurrency data files 🔍

    Returns comprehensive inventory of available datasets
    """
    print("\n📊 PHASE 1: Complete Data Discovery")
    print("=" * 80)

    data_inventory = {
        '1h': [],
        '1d': [],
        '6h': [],
        '5m': [],
        'other': []
    }

    # Scan all data directories
    data_paths = [
        '/Users/bobbyyo/Projects/algo-fun/data/',
        '/Users/bobbyyo/Projects/algo-fun/data/coinbase/',
        '/Users/bobbyyo/Projects/algo-fun/data/hyperliquid/',
        '/Users/bobbyyo/Projects/algo-fun/data/yahoo/',
        '/Users/bobbyyo/Projects/algo-fun/data/coingecko/',
        '/Users/bobbyyo/Projects/algo-fun/data/cryptocompare/',
        '/Users/bobbyyo/Projects/algo-fun/data/coinmarketcap/'
    ]

    for base_path in data_paths:
        if not os.path.exists(base_path):
            continue

        for file in os.listdir(base_path):
            if not file.endswith('.csv'):
                continue

            full_path = os.path.join(base_path, file)

            # Categorize by timeframe
            if '1h' in file.lower() or '1-hour' in file.lower():
                data_inventory['1h'].append(full_path)
            elif '1d' in file.lower() or 'daily' in file.lower():
                data_inventory['1d'].append(full_path)
            elif '6h' in file.lower() or '6-hour' in file.lower():
                data_inventory['6h'].append(full_path)
            elif '5m' in file.lower() or '5-min' in file.lower():
                data_inventory['5m'].append(full_path)
            else:
                # Check if it's a Yahoo/daily dataset
                if 'yahoo' in base_path.lower() or '20yr' in file.lower() or '10yr' in file.lower():
                    data_inventory['1d'].append(full_path)
                else:
                    data_inventory['other'].append(full_path)

    # Report findings
    print(f"🔍 Data Discovery Results:")
    print(f"   📈 1-Hour Data Files: {len(data_inventory['1h'])}")
    print(f"   📊 Daily Data Files: {len(data_inventory['1d'])}")
    print(f"   ⏰ 6-Hour Data Files: {len(data_inventory['6h'])}")
    print(f"   ⚡ 5-Minute Data Files: {len(data_inventory['5m'])}")
    print(f"   📁 Other Data Files: {len(data_inventory['other'])}")

    # List all 1h files
    if data_inventory['1h']:
        print(f"\n📊 Available 1-Hour Data Sources:")
        for i, path in enumerate(data_inventory['1h'], 1):
            filename = os.path.basename(path)
            print(f"   {i}. {filename}")

            # Extract asset info
            if 'BTC' in filename.upper():
                asset = 'BTC'
            elif 'ETH' in filename.upper():
                asset = 'ETH'
            elif 'XRP' in filename.upper():
                asset = 'XRP'
            elif 'CRO' in filename.upper():
                asset = 'CRO'
            elif 'HBAR' in filename.upper():
                asset = 'HBAR'
            elif 'LINK' in filename.upper():
                asset = 'LINK'
            else:
                asset = 'Unknown'

            # Extract provider info
            if 'coinbase' in path.lower():
                provider = 'Coinbase'
            elif 'hyperliquid' in path.lower():
                provider = 'Hyperliquid'
            elif 'yahoo' in path.lower():
                provider = 'Yahoo'
            else:
                provider = 'Direct'

            print(f"      → Asset: {asset} | Provider: {provider}")

    return data_inventory


def validate_and_load_data(file_path):
    """
    🛡️ Validate data quality and load if passes threshold 🛡️

    Returns: (dataframe, quality_score, validation_report) or (None, score, report) if fails
    """
    print(f"\n🔍 Validating: {os.path.basename(file_path)}")

    try:
        # Load data
        df = pd.read_csv(file_path)

        # Ensure datetime index
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
        elif 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df = df.set_index('Datetime')
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        else:
            # Try to parse index as datetime
            try:
                df.index = pd.to_datetime(df.index)
            except:
                print(f"   ❌ Cannot parse datetime index")
                return None, 0, "No datetime column found"

        # Standardize column names to uppercase
        df.columns = [col.capitalize() for col in df.columns]

        # Basic quality validation (simplified for now)
        quality_score = 100  # Start with perfect score

        # Check for required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in required_cols:
            if col not in df.columns:
                quality_score -= 20

        # Check for null values
        null_pct = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
        if null_pct > MAX_NULL_PERCENTAGE:
            quality_score -= 25

        # Check data length
        if len(df) < MIN_DATA_POINTS:
            quality_score -= 25

        print(f"   📊 Quality Score: {quality_score:.1f}/100")

        if quality_score < MIN_QUALITY_SCORE:
            print(f"   ❌ Below minimum quality threshold ({MIN_QUALITY_SCORE})")
            return None, quality_score, f"Quality score {quality_score} below threshold"

        # Additional strategy-specific validation
        is_valid, msg = validate_data_for_strategy(df)
        if not is_valid:
            print(f"   ❌ Strategy validation failed: {msg}")
            return None, quality_score, msg

        # Data characteristics
        print(f"   ✅ Data Validated Successfully")
        print(f"   📊 Rows: {len(df):,} | Date Range: {df.index.min()} to {df.index.max()}")
        print(f"   📊 Volume Present: {'Yes' if df['Volume'].sum() > 0 else 'No'}")

        return df, quality_score, {'quality_score': quality_score, 'status': 'passed'}

    except Exception as e:
        print(f"   ❌ Error loading data: {str(e)}")
        return None, 0, str(e)


def test_strategy_with_parameters(df, strategy_class, params, data_info):
    """
    🎯 Test strategy with specific parameter set 🎯

    Returns comprehensive results including all metrics
    """
    # Create custom strategy class with parameters
    class ParameterizedStrategy(strategy_class):
        ma_period = params.get('ma_period', 20)
        rsi_period = params.get('rsi_period', 14)
        rsi_oversold = params.get('rsi_oversold', 35)
        rsi_overbought = params.get('rsi_overbought', 65)
        volume_spike = params.get('volume_spike', 1.1)
        take_profit = params.get('take_profit', 0.05)
        stop_loss = params.get('stop_loss', 0.025)

        # For adaptive strategy
        if hasattr(strategy_class, 'mode'):
            mode = params.get('mode', 'AGGRESSIVE')
            rsi_buy = params.get('rsi_oversold', 40)
            rsi_sell = params.get('rsi_overbought', 60)
            volume_mult = params.get('volume_spike', 1.0)

    # Run backtest with enhanced runner
    summary_stats, full_stats = enhanced_backtest_runner(
        df,
        ParameterizedStrategy,
        data_info,
        strategy_name=f"{strategy_class.__name__} (Optimized)",
        cash=10000,
        commission=0.002
    )

    return summary_stats, full_stats


def analyze_market_conditions(df):
    """
    📈 Analyze market conditions for strategy suitability 📈

    Returns detailed market regime analysis
    """
    analysis = {}

    # Calculate returns
    df['returns'] = df['Close'].pct_change()

    # Volatility analysis
    analysis['volatility_daily'] = df['returns'].std() * 100
    analysis['volatility_annualized'] = analysis['volatility_daily'] * np.sqrt(365)

    # Trend analysis
    sma_20 = df['Close'].rolling(20).mean()
    sma_50 = df['Close'].rolling(50).mean()
    analysis['trend_strength'] = ((df['Close'] - sma_50) / sma_50 * 100).mean()
    analysis['trend_consistency'] = (df['Close'] > sma_20).mean() * 100

    # Volume analysis
    if 'Volume' in df.columns and df['Volume'].sum() > 0:
        volume_ma = df['Volume'].rolling(20).mean()
        analysis['volume_consistency'] = (df['Volume'] > 0).mean() * 100
        analysis['volume_spikes'] = (df['Volume'] > volume_ma * 1.5).sum()
        analysis['avg_volume'] = df['Volume'].mean()
    else:
        analysis['volume_consistency'] = 0
        analysis['volume_spikes'] = 0
        analysis['avg_volume'] = 0

    # Market regime classification
    if analysis['volatility_annualized'] < 50:
        regime = 'Low Volatility'
    elif analysis['volatility_annualized'] < 100:
        regime = 'Medium Volatility'
    else:
        regime = 'High Volatility'

    if abs(analysis['trend_strength']) < 5:
        regime += ' - Ranging'
    elif analysis['trend_strength'] > 5:
        regime += ' - Uptrend'
    else:
        regime += ' - Downtrend'

    analysis['market_regime'] = regime

    return analysis


def optimize_parameters_for_asset(df, strategy_class, asset_name, timeframe):
    """
    🎯 Optimize strategy parameters for specific asset 🎯

    Performs grid search optimization with multiple parameter combinations
    """
    print(f"\n🔧 Optimizing Parameters for {asset_name} ({timeframe})")
    print("=" * 60)

    best_sharpe = -999
    best_params = {}
    best_stats = None

    optimization_results = []

    # Grid search through parameter combinations
    for rsi_oversold in PARAMETER_VARIATIONS['rsi_oversold']:
        for volume_spike in PARAMETER_VARIATIONS['volume_spike']:
            for take_profit in PARAMETER_VARIATIONS['take_profit']:
                for stop_loss in PARAMETER_VARIATIONS['stop_loss']:

                    params = {
                        'rsi_oversold': rsi_oversold,
                        'rsi_overbought': 70 - (rsi_oversold - 30),  # Dynamic overbought
                        'volume_spike': volume_spike,
                        'take_profit': take_profit,
                        'stop_loss': stop_loss,
                        'ma_period': 20,  # Keep MA fixed for now
                        'rsi_period': 14   # Keep RSI period fixed
                    }

                    try:
                        # Create parameterized strategy
                        class OptimizedStrategy(strategy_class):
                            ma_period = params['ma_period']
                            rsi_period = params['rsi_period']
                            rsi_oversold = params['rsi_oversold']
                            rsi_overbought = params['rsi_overbought']
                            volume_spike = params['volume_spike']
                            take_profit = params['take_profit']
                            stop_loss = params['stop_loss']

                        # Run backtest
                        bt = Backtest(df, OptimizedStrategy, cash=10000, commission=0.002)
                        stats = bt.run()

                        # Track results
                        sharpe = stats['Sharpe Ratio']
                        result = {
                            'params': params.copy(),
                            'sharpe': sharpe,
                            'return': stats['Return [%]'],
                            'max_dd': stats['Max. Drawdown [%]'],
                            'win_rate': stats['Win Rate [%]'],
                            'trades': stats['# Trades']
                        }
                        optimization_results.append(result)

                        # Update best if better
                        if sharpe > best_sharpe and stats['# Trades'] > 10:  # Min trades requirement
                            best_sharpe = sharpe
                            best_params = params.copy()
                            best_stats = stats

                    except Exception as e:
                        continue

    # Display optimization results
    if best_stats:
        print(f"✅ Best Parameters Found:")
        print(f"   📊 RSI Oversold: {best_params['rsi_oversold']}")
        print(f"   📊 Volume Spike: {best_params['volume_spike']:.1f}x")
        print(f"   📊 Take Profit: {best_params['take_profit']*100:.1f}%")
        print(f"   📊 Stop Loss: {best_params['stop_loss']*100:.1f}%")
        print(f"   🎯 Best Sharpe: {best_sharpe:.2f}")
        print(f"   💰 Return: {best_stats['Return [%]']:.2f}%")
        print(f"   📉 Max DD: {best_stats['Max. Drawdown [%]']:.2f}%")
        print(f"   🎯 Win Rate: {best_stats['Win Rate [%]']:.1f}%")

    return best_params, best_stats, optimization_results


def create_comprehensive_report(all_results):
    """
    📊 Create comprehensive analysis report 📊

    Generates detailed performance analysis and rankings
    """
    print("\n" + "=" * 100)
    print("📊 COMPREHENSIVE PERFORMANCE ANALYSIS REPORT")
    print("=" * 100)

    if not all_results:
        print("❌ No results to analyze")
        return

    # Convert to DataFrame for analysis
    results_df = pd.DataFrame(all_results)

    # 🏆 Asset Performance Rankings
    print("\n🏆 ASSET PERFORMANCE RANKINGS (by Sharpe Ratio)")
    print("=" * 80)

    # Sort by Sharpe ratio
    rankings = results_df.sort_values('sharpe_ratio', ascending=False)

    for rank, (idx, row) in enumerate(rankings.iterrows(), 1):
        print(f"\n#{rank}. {row['asset']} ({row['timeframe']}, {row['provider']})")
        print(f"   📊 Sharpe Ratio: {row['sharpe_ratio']:.3f}")
        print(f"   💰 Total Return: {row['total_return']:.2f}%")
        print(f"   📉 Max Drawdown: {row['max_drawdown']:.2f}%")
        print(f"   🎯 Win Rate: {row['win_rate']:.1f}%")
        print(f"   📈 Trades: {row['trades']:.0f}")
        print(f"   🎯 Strategy Mode: {row['strategy_mode']}")

        # Quality indicators
        if row['sharpe_ratio'] > 1.5:
            print(f"   ✅ EXCELLENT - Ready for production")
        elif row['sharpe_ratio'] > 1.0:
            print(f"   ✅ GOOD - Consider for production with monitoring")
        elif row['sharpe_ratio'] > 0.5:
            print(f"   ⚠️ MODERATE - Needs optimization")
        else:
            print(f"   ❌ POOR - Not recommended")

    # 📊 Strategy Mode Comparison
    print("\n📊 STRATEGY MODE COMPARISON")
    print("=" * 80)

    conservative_results = results_df[results_df['strategy_mode'] == 'Conservative']
    adaptive_results = results_df[results_df['strategy_mode'] == 'Adaptive']

    if len(conservative_results) > 0:
        print("\n🛡️ Conservative Mode (All 3 Signals Required):")
        print(f"   Avg Sharpe: {conservative_results['sharpe_ratio'].mean():.3f}")
        print(f"   Avg Return: {conservative_results['total_return'].mean():.2f}%")
        print(f"   Avg Win Rate: {conservative_results['win_rate'].mean():.1f}%")
        print(f"   Avg Trades: {conservative_results['trades'].mean():.0f}")

    if len(adaptive_results) > 0:
        print("\n⚡ Adaptive Mode (2 of 3 Signals):")
        print(f"   Avg Sharpe: {adaptive_results['sharpe_ratio'].mean():.3f}")
        print(f"   Avg Return: {adaptive_results['total_return'].mean():.2f}%")
        print(f"   Avg Win Rate: {adaptive_results['win_rate'].mean():.1f}%")
        print(f"   Avg Trades: {adaptive_results['trades'].mean():.0f}")

    # 🎯 Parameter Optimization Insights
    print("\n🎯 OPTIMAL PARAMETER INSIGHTS")
    print("=" * 80)

    if 'best_params' in results_df.columns:
        # Analyze common optimal parameters
        all_params = [p for p in results_df['best_params'] if p]
        if all_params:
            avg_rsi = np.mean([p.get('rsi_oversold', 35) for p in all_params])
            avg_vol = np.mean([p.get('volume_spike', 1.1) for p in all_params])
            avg_tp = np.mean([p.get('take_profit', 0.05) for p in all_params])
            avg_sl = np.mean([p.get('stop_loss', 0.025) for p in all_params])

            print(f"📊 Average Optimal Parameters Across Assets:")
            print(f"   RSI Oversold: {avg_rsi:.1f}")
            print(f"   Volume Spike: {avg_vol:.2f}x")
            print(f"   Take Profit: {avg_tp*100:.1f}%")
            print(f"   Stop Loss: {avg_sl*100:.1f}%")

    # 🚀 Production Readiness Assessment
    print("\n🚀 PRODUCTION READINESS ASSESSMENT")
    print("=" * 80)

    production_ready = rankings[rankings['sharpe_ratio'] > 1.0]
    if len(production_ready) > 0:
        print(f"\n✅ {len(production_ready)} Assets Ready for Production:")
        for idx, row in production_ready.iterrows():
            print(f"   • {row['asset']} ({row['timeframe']}) - Sharpe: {row['sharpe_ratio']:.3f}")

    # 📈 Market Condition Analysis
    print("\n📈 MARKET CONDITION SUITABILITY")
    print("=" * 80)

    if 'market_regime' in results_df.columns:
        regime_performance = results_df.groupby('market_regime')['sharpe_ratio'].mean()
        print("\nStrategy Performance by Market Regime:")
        for regime, avg_sharpe in regime_performance.items():
            print(f"   {regime}: Avg Sharpe {avg_sharpe:.3f}")

    # 💡 Key Insights and Recommendations
    print("\n💡 KEY INSIGHTS AND RECOMMENDATIONS")
    print("=" * 80)

    # Best overall performer
    best_performer = rankings.iloc[0]
    print(f"\n🏆 Best Overall: {best_performer['asset']} with Sharpe {best_performer['sharpe_ratio']:.3f}")

    # Trade frequency analysis
    high_frequency = results_df[results_df['trades'] > 100]
    if len(high_frequency) > 0:
        print(f"\n📊 High-Frequency Trading Assets ({len(high_frequency)}):")
        for idx, row in high_frequency.iterrows():
            print(f"   • {row['asset']}: {row['trades']:.0f} trades")

    # Risk analysis
    low_risk = results_df[results_df['max_drawdown'] > -15]
    if len(low_risk) > 0:
        print(f"\n🛡️ Low-Risk Assets (Max DD < 15%): {len(low_risk)}")

    # Win rate analysis
    high_win_rate = results_df[results_df['win_rate'] > 60]
    if len(high_win_rate) > 0:
        print(f"\n🎯 High Win-Rate Assets (>60%): {len(high_win_rate)}")

    return results_df


def main():
    """
    🚀 Main execution function - Ultra-comprehensive testing 🚀
    """

    # Phase 1: Data Discovery
    data_inventory = discover_all_crypto_data()

    # Focus on 1H data as requested
    one_hour_files = data_inventory['1h']

    if not one_hour_files:
        print("\n❌ No 1-hour data files found!")
        print("🔄 Falling back to other timeframes for comprehensive testing...")

        # Use daily data as fallback
        test_files = data_inventory['1d'][:10]  # Limit to 10 files for testing
    else:
        test_files = one_hour_files

    # Phase 2: Comprehensive Testing
    print("\n" + "=" * 100)
    print("📊 PHASE 2: Comprehensive Strategy Testing")
    print("=" * 100)

    all_results = []

    for file_path in test_files:
        print("\n" + "=" * 80)
        print(f"📁 Processing: {os.path.basename(file_path)}")
        print("=" * 80)

        # Load and validate data
        df, quality_score, validation_report = validate_and_load_data(file_path)

        if df is None:
            print(f"⚠️ Skipping due to validation failure")
            continue

        # Extract metadata
        data_info = create_data_source_info(file_path)
        asset = data_info['symbol']
        timeframe = data_info['timeframe']
        provider = data_info['provider']

        # Analyze market conditions
        print(f"\n📈 Analyzing Market Conditions...")
        market_analysis = analyze_market_conditions(df)
        print(f"   Market Regime: {market_analysis['market_regime']}")
        print(f"   Volatility (Annual): {market_analysis['volatility_annualized']:.1f}%")
        print(f"   Trend Strength: {market_analysis['trend_strength']:.2f}%")

        # Test Conservative Strategy
        print(f"\n🛡️ Testing Conservative Strategy (All 3 Signals)...")
        try:
            summary_stats_cons, full_stats_cons = enhanced_backtest_runner(
                df,
                MARSIVolumeHybridStrategy,
                data_info,
                strategy_name="MA-RSI-Volume Conservative",
                cash=10000,
                commission=0.002
            )

            # Store results
            result = {
                'asset': asset,
                'timeframe': timeframe,
                'provider': provider,
                'strategy_mode': 'Conservative',
                'data_quality': quality_score,
                'data_points': len(df),
                'sharpe_ratio': full_stats_cons['Sharpe Ratio'],
                'sortino_ratio': full_stats_cons.get('Sortino Ratio', np.nan),
                'total_return': full_stats_cons['Return [%]'],
                'max_drawdown': full_stats_cons['Max. Drawdown [%]'],
                'win_rate': full_stats_cons['Win Rate [%]'],
                'trades': full_stats_cons['# Trades'],
                'profit_factor': full_stats_cons.get('Profit Factor', np.nan),
                'market_regime': market_analysis['market_regime'],
                'best_params': None  # Will be updated if optimized
            }
            all_results.append(result)

        except Exception as e:
            print(f"   ❌ Conservative strategy failed: {str(e)}")

        # Test Adaptive Strategy
        print(f"\n⚡ Testing Adaptive Strategy (2 of 3 Signals)...")
        try:
            summary_stats_adap, full_stats_adap = enhanced_backtest_runner(
                df,
                MARSIVolumeAdaptiveStrategy,
                data_info,
                strategy_name="MA-RSI-Volume Adaptive",
                cash=10000,
                commission=0.002
            )

            # Store results
            result = {
                'asset': asset,
                'timeframe': timeframe,
                'provider': provider,
                'strategy_mode': 'Adaptive',
                'data_quality': quality_score,
                'data_points': len(df),
                'sharpe_ratio': full_stats_adap['Sharpe Ratio'],
                'sortino_ratio': full_stats_adap.get('Sortino Ratio', np.nan),
                'total_return': full_stats_adap['Return [%]'],
                'max_drawdown': full_stats_adap['Max. Drawdown [%]'],
                'win_rate': full_stats_adap['Win Rate [%]'],
                'trades': full_stats_adap['# Trades'],
                'profit_factor': full_stats_adap.get('Profit Factor', np.nan),
                'market_regime': market_analysis['market_regime'],
                'best_params': None
            }
            all_results.append(result)

        except Exception as e:
            print(f"   ❌ Adaptive strategy failed: {str(e)}")

        # Perform parameter optimization (for 1H data only)
        if '1h' in file_path.lower():
            print(f"\n🔧 Performing Parameter Optimization...")
            best_params, best_stats, opt_results = optimize_parameters_for_asset(
                df, MARSIVolumeHybridStrategy, asset, timeframe
            )

            if best_stats:
                # Update the conservative result with optimization
                for r in all_results:
                    if r['asset'] == asset and r['strategy_mode'] == 'Conservative':
                        r['best_params'] = best_params
                        r['optimized_sharpe'] = best_stats['Sharpe Ratio']
                        r['optimized_return'] = best_stats['Return [%]']

    # Phase 3: Comprehensive Report Generation
    if all_results:
        results_df = create_comprehensive_report(all_results)

        # Save results to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'/Users/bobbyyo/Projects/algo-fun/strategies/results/ma_rsi_volume_ultra_comprehensive_{timestamp}.csv'
        results_df = pd.DataFrame(all_results)
        results_df.to_csv(output_file, index=False)
        print(f"\n💾 Results saved to: {output_file}")

        # Generate final summary
        print("\n" + "=" * 100)
        print("🎯 ULTRA-COMPREHENSIVE TESTING COMPLETE")
        print("=" * 100)
        print(f"✅ Total Assets Tested: {len(set([r['asset'] for r in all_results]))}")
        print(f"✅ Total Strategies Tested: {len(all_results)}")
        print(f"✅ Best Overall Sharpe: {max([r['sharpe_ratio'] for r in all_results]):.3f}")
        print(f"✅ Average Sharpe Ratio: {np.mean([r['sharpe_ratio'] for r in all_results]):.3f}")
        print(f"⏰ Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    else:
        print("\n❌ No successful test results generated")

    print("\n" + "=" * 100)
    print("🌙💫🚀 Ultra-Comprehensive MA-RSI-Volume Testing Complete! 🌙💫🚀")
    print("=" * 100)


if __name__ == "__main__":
    main()