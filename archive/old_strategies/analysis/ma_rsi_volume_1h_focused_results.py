"""
🚀 MA-RSI-VOLUME HYBRID 1H FOCUSED RESULTS 🚀
==============================================
Fast, focused testing of MA-RSI-Volume strategies on all available 1H data
with immediate comprehensive results display.

Author: Bobby's Algo Trading System 🌙💫
Date: 2025-01-18
Version: 1.0.0 - Focused Results Edition
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.append('/Users/bobbyyo/Projects/algo-fun')

# 🚨 MANDATORY IMPORTS FOR NATIVE RESULTS DISPLAY
from strategies.analysis.universal_native_results_display import (
    enhanced_backtest_runner,
    create_data_source_info,
    display_full_native_results
)

# Import strategies
from strategies.indicators.ma_rsi_volume_hybrid_strategy import MARSIVolumeHybridStrategy
from strategies.indicators.ma_rsi_volume_adaptive_strategy import MARSIVolumeAdaptiveStrategy

print("=" * 100)
print("🚀 MA-RSI-VOLUME HYBRID 1H FOCUSED RESULTS TESTING 🚀")
print("=" * 100)
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)


def load_and_prepare_data(file_path):
    """Load and prepare data for backtesting"""
    try:
        df = pd.read_csv(file_path)

        # Handle datetime column
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
        elif 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df = df.set_index('Datetime')
        elif 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime')
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        else:
            df.index = pd.to_datetime(df.index)

        # Standardize column names
        df.columns = [col.capitalize() for col in df.columns]

        # Validate required columns
        required = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required):
            return None

        # Ensure sufficient data
        if len(df) < 100:
            return None

        return df
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None


def analyze_strategy_performance(all_results):
    """Generate comprehensive performance analysis"""

    print("\n" + "=" * 100)
    print("📊 COMPREHENSIVE PERFORMANCE ANALYSIS")
    print("=" * 100)

    if not all_results:
        print("❌ No results to analyze")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_results)

    # 🏆 Rankings by Sharpe Ratio
    print("\n🏆 PERFORMANCE RANKINGS (BY SHARPE RATIO)")
    print("=" * 80)

    # Sort by Sharpe (handle NaN values)
    df['sharpe_clean'] = df['sharpe'].fillna(-999)
    ranked = df.sort_values('sharpe_clean', ascending=False)

    for idx, row in ranked.iterrows():
        print(f"\n📊 {row['asset']} - {row['strategy']}")
        print(f"   Provider: {row['provider']} | Timeframe: {row['timeframe']}")
        if row['sharpe_clean'] != -999:
            print(f"   Sharpe Ratio: {row['sharpe']:.3f}")
        else:
            print(f"   Sharpe Ratio: N/A (insufficient data)")
        print(f"   Total Return: {row['return']:.2f}%")
        print(f"   Max Drawdown: {row['max_dd']:.2f}%")
        print(f"   Win Rate: {row['win_rate']:.1f}%")
        print(f"   Trades: {row['trades']}")

        # Quality assessment
        if row['sharpe_clean'] > 1.5:
            print(f"   ✅ EXCELLENT - Production Ready")
        elif row['sharpe_clean'] > 1.0:
            print(f"   ✅ GOOD - Consider for Production")
        elif row['sharpe_clean'] > 0.5:
            print(f"   ⚠️ MODERATE - Needs Optimization")
        else:
            print(f"   ❌ POOR - Not Recommended")

    # 📈 Strategy Comparison
    print("\n📈 STRATEGY COMPARISON")
    print("=" * 80)

    conservative = df[df['strategy'] == 'Conservative']
    adaptive = df[df['strategy'] == 'Adaptive']

    if len(conservative) > 0:
        valid_sharpe = conservative[conservative['sharpe_clean'] != -999]['sharpe_clean']
        print("\n🛡️ Conservative Strategy (All 3 Signals):")
        if len(valid_sharpe) > 0:
            print(f"   Avg Sharpe: {valid_sharpe.mean():.3f}")
        print(f"   Avg Return: {conservative['return'].mean():.2f}%")
        print(f"   Avg Win Rate: {conservative['win_rate'].mean():.1f}%")
        print(f"   Avg Trades: {conservative['trades'].mean():.0f}")

    if len(adaptive) > 0:
        valid_sharpe = adaptive[adaptive['sharpe_clean'] != -999]['sharpe_clean']
        print("\n⚡ Adaptive Strategy (2 of 3 Signals):")
        if len(valid_sharpe) > 0:
            print(f"   Avg Sharpe: {valid_sharpe.mean():.3f}")
        print(f"   Avg Return: {adaptive['return'].mean():.2f}%")
        print(f"   Avg Win Rate: {adaptive['win_rate'].mean():.1f}%")
        print(f"   Avg Trades: {adaptive['trades'].mean():.0f}")

    # 🎯 Asset Performance Summary
    print("\n🎯 ASSET PERFORMANCE SUMMARY")
    print("=" * 80)

    asset_performance = df.groupby('asset').agg({
        'return': 'mean',
        'win_rate': 'mean',
        'trades': 'sum',
        'max_dd': 'mean'
    })

    for asset, metrics in asset_performance.iterrows():
        print(f"\n{asset}:")
        print(f"   Avg Return: {metrics['return']:.2f}%")
        print(f"   Avg Win Rate: {metrics['win_rate']:.1f}%")
        print(f"   Total Trades: {metrics['trades']:.0f}")
        print(f"   Avg Max DD: {metrics['max_dd']:.2f}%")

    # 💡 Key Insights
    print("\n💡 KEY INSIGHTS")
    print("=" * 80)

    # Best performer
    best = ranked.iloc[0] if len(ranked) > 0 else None
    if best is not None and best['sharpe_clean'] != -999:
        print(f"\n🏆 Best Performer: {best['asset']} - {best['strategy']}")
        print(f"   Sharpe: {best['sharpe']:.3f}, Return: {best['return']:.2f}%")

    # High frequency
    high_freq = df[df['trades'] > 100]
    if len(high_freq) > 0:
        print(f"\n📊 High-Frequency Assets: {len(high_freq)}")
        for _, row in high_freq.iterrows():
            print(f"   • {row['asset']}: {row['trades']} trades")

    # Low risk
    low_risk = df[df['max_dd'] > -20]
    if len(low_risk) > 0:
        print(f"\n🛡️ Low-Risk Assets (DD < 20%): {len(low_risk)}")

    # High win rate
    high_wr = df[df['win_rate'] > 55]
    if len(high_wr) > 0:
        print(f"\n🎯 High Win-Rate Assets (>55%): {len(high_wr)}")

    return df


def main():
    """Main execution - focused testing on all 1H data"""

    # Define all 1H data files
    one_hour_files = [
        '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1h-500wks-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/XRP_1h_20250914_210236_historical.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/coinbase/XRPUSD-1h-100wks-enhanced-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/hyperliquid/ETH-USD-1h-hyperliquid-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/hyperliquid/XRP-USD-1h-hyperliquid-data.csv'
    ]

    all_results = []

    print("\n📊 TESTING ALL AVAILABLE 1H DATA")
    print("=" * 100)

    for file_path in one_hour_files:
        if not os.path.exists(file_path):
            continue

        print(f"\n{'=' * 80}")
        print(f"📁 Processing: {os.path.basename(file_path)}")
        print(f"{'=' * 80}")

        # Load data
        df = load_and_prepare_data(file_path)
        if df is None:
            print("   ⚠️ Skipping - data loading failed")
            continue

        # Extract metadata
        data_info = create_data_source_info(file_path)

        print(f"   📊 Data loaded: {len(df)} rows")
        print(f"   📅 Range: {df.index.min()} to {df.index.max()}")

        # Test Conservative Strategy
        print(f"\n🛡️ Testing Conservative Strategy...")
        try:
            summary, stats = enhanced_backtest_runner(
                df,
                MARSIVolumeHybridStrategy,
                data_info,
                strategy_name="Conservative",
                cash=10000,
                commission=0.002
            )

            result = {
                'asset': data_info['symbol'],
                'timeframe': data_info['timeframe'],
                'provider': data_info['provider'],
                'strategy': 'Conservative',
                'return': stats['Return [%]'],
                'sharpe': stats['Sharpe Ratio'],
                'sortino': stats.get('Sortino Ratio', np.nan),
                'max_dd': stats['Max. Drawdown [%]'],
                'win_rate': stats['Win Rate [%]'],
                'trades': stats['# Trades'],
                'profit_factor': stats.get('Profit Factor', np.nan)
            }
            all_results.append(result)

        except Exception as e:
            print(f"   ❌ Error: {str(e)[:100]}")

        # Test Adaptive Strategy
        print(f"\n⚡ Testing Adaptive Strategy...")
        try:
            summary, stats = enhanced_backtest_runner(
                df,
                MARSIVolumeAdaptiveStrategy,
                data_info,
                strategy_name="Adaptive",
                cash=10000,
                commission=0.002
            )

            result = {
                'asset': data_info['symbol'],
                'timeframe': data_info['timeframe'],
                'provider': data_info['provider'],
                'strategy': 'Adaptive',
                'return': stats['Return [%]'],
                'sharpe': stats['Sharpe Ratio'],
                'sortino': stats.get('Sortino Ratio', np.nan),
                'max_dd': stats['Max. Drawdown [%]'],
                'win_rate': stats['Win Rate [%]'],
                'trades': stats['# Trades'],
                'profit_factor': stats.get('Profit Factor', np.nan)
            }
            all_results.append(result)

        except Exception as e:
            print(f"   ❌ Error: {str(e)[:100]}")

    # Generate comprehensive analysis
    if all_results:
        results_df = analyze_strategy_performance(all_results)

        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'/Users/bobbyyo/Projects/algo-fun/strategies/results/ma_rsi_1h_focused_{timestamp}.csv'
        results_df = pd.DataFrame(all_results)
        results_df.to_csv(output_file, index=False)
        print(f"\n💾 Results saved to: {output_file}")

    print("\n" + "=" * 100)
    print("🌙💫🚀 MA-RSI-Volume 1H Focused Testing Complete! 🌙💫🚀")
    print("=" * 100)
    print(f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)


if __name__ == "__main__":
    main()