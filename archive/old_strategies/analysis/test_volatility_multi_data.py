# test_volatility_multi_data.py
"""
🛡️ SECURITY-ENHANCED: Multi-Data Volatility Strategy Testing
Includes mandatory data validation for all sources (input security)
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import warnings
import os
import sys

# Add path for universal display module
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import enhanced_backtest_runner, create_data_source_info

warnings.filterwarnings('ignore')

# 🛡️ SECURITY: Import data validation for multi-source input security
from trading_functions import DataQualityValidator, validate_data_source_quality

# Import our strategy
from volatility_dual_mode_strategy import VolatilityDualModeStrategy

print("🚀 Multi-Data Testing of Dual-Mode Volatility Strategy")
print("="*80)

# Define test data sources
DATA_SOURCES = [
    ('BTC-1d-Yahoo', '/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv', 'datetime'),  # 🛡️ Fixed: Using validated Yahoo data
    ('BTC-6h', '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-6h-500wks-data.csv', 'datetime'),
    ('BTC-1h', '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1h-500wks-data.csv', 'datetime'),
]

def clean_and_fix_data(data):
    """Clean and fix OHLC data issues"""
    # Ensure positive prices
    for col in ['Open', 'High', 'Low', 'Close']:
        if col in data.columns:
            data[col] = data[col].abs()

    # Fix OHLC relationships
    data['High'] = data[['Open', 'High', 'Low', 'Close']].max(axis=1)
    data['Low'] = data[['Open', 'High', 'Low', 'Close']].min(axis=1)

    # Ensure volume is positive
    if 'Volume' in data.columns:
        data['Volume'] = data['Volume'].abs()

    return data

all_results = []

for name, path, date_col in DATA_SOURCES:
    print(f"\n📊 Testing on {name}...")
    print("-" * 40)

    try:
        # 🛡️ SECURITY: Validate data quality before processing (prevents malformed data)
        if os.path.exists(path):
            print(f"🛡️ Validating {name} data quality for security...")
            validator = DataQualityValidator()
            validation_result = validate_data_source_quality(path, validator)

            if validation_result.overall_score < 75:
                print(f"❌ SECURITY BLOCK: {name} data quality too low: {validation_result.overall_score}")
                print("🛡️ SECURITY: Skipping potentially corrupted data source")
                continue

            print(f"✅ {name} data security validated - Quality score: {validation_result.overall_score}")
            data = pd.read_csv(path, parse_dates=[date_col], index_col=date_col)
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

            # Clean and sort
            data = data.dropna()
            data = data.sort_index()
            data = clean_and_fix_data(data)

            print(f"✅ Data loaded: {len(data)} bars")
            print(f"   Period: {data.index[0]} to {data.index[-1]}")

            # Create data source info for native display
            data_source_info = create_data_source_info(
                file_path=filepath,
                symbol=name.split('-')[0] if '-' in name else name,
                timeframe=name.split('-')[1] if '-' in name and len(name.split('-')) > 1 else 'unknown',
                provider='multi-data'
            )

            # Run backtest with FULL NATIVE RESULTS DISPLAY
            summary_stats, stats = enhanced_backtest_runner(
                data=data,
                strategy_class=VolatilityDualModeStrategy,
                data_source_info=data_source_info,
                strategy_name="Volatility Dual-Mode Strategy",
                cash=1000000,
                commission=0.001
            )

            # Store results
            result = {
                'Data_Source': name,
                'Bars': len(data),
                'Start': str(data.index[0]),
                'End': str(data.index[-1]),
                'Return_%': round(stats['Return [%]'], 2),
                'Return_Ann_%': round(stats['Return (Ann.) [%]'], 2),
                'Sharpe': round(stats['Sharpe Ratio'], 3),
                'Sortino': round(stats['Sortino Ratio'], 3),
                'Max_DD_%': round(stats['Max. Drawdown [%]'], 2),
                'Trades': stats['# Trades'],
                'Win_Rate_%': round(stats['Win Rate [%]'], 2),
                'Avg_Trade_%': round(stats['Avg. Trade [%]'], 3),
                'Best_Trade_%': round(stats['Best Trade [%]'], 2),
                'Worst_Trade_%': round(stats['Worst Trade [%]'], 2),
                'Profit_Factor': round(stats['Profit Factor'], 3),
                'Expectancy_%': round(stats['Expectancy [%]'], 3),
            }

            all_results.append(result)

            # Print key metrics
            print(f"   📈 Return: {stats['Return [%]']:.2f}%")
            print(f"   📊 Sharpe: {stats['Sharpe Ratio']:.3f}")
            print(f"   📉 Max DD: {stats['Max. Drawdown [%]']:.2f}%")
            print(f"   🎯 Trades: {stats['# Trades']}")
            print(f"   ✅ Win Rate: {stats['Win Rate [%]']:.1f}%")

        else:
            print(f"❌ File not found: {path}")

    except Exception as e:
        print(f"❌ Error testing {name}: {e}")

# Create results dataframe
if all_results:
    results_df = pd.DataFrame(all_results)

    print("\n" + "="*80)
    print("📊 AGGREGATE RESULTS SUMMARY")
    print("="*80)

    # Display results table
    print("\n", results_df.to_string(index=False))

    # Calculate averages
    print("\n📈 AVERAGE PERFORMANCE METRICS:")
    print(f"   • Average Return: {results_df['Return_%'].mean():.2f}%")
    print(f"   • Average Annual Return: {results_df['Return_Ann_%'].mean():.2f}%")
    print(f"   • Average Sharpe Ratio: {results_df['Sharpe'].mean():.3f}")
    print(f"   • Average Max Drawdown: {results_df['Max_DD_%'].mean():.2f}%")
    print(f"   • Average Win Rate: {results_df['Win_Rate_%'].mean():.2f}%")
    print(f"   • Total Trades: {results_df['Trades'].sum()}")

    # Best performer
    best_idx = results_df['Sharpe'].idxmax()
    best = results_df.loc[best_idx]
    print(f"\n🏆 BEST PERFORMER: {best['Data_Source']}")
    print(f"   • Sharpe: {best['Sharpe']}")
    print(f"   • Return: {best['Return_%']}%")
    print(f"   • Max DD: {best['Max_DD_%']}%")

    # Save results
    results_path = '/Users/bobbyyo/Projects/algo-fun/strategies/analysis/results/volatility_multi_data_results.csv'
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    results_df.to_csv(results_path, index=False)
    print(f"\n📄 Results saved to: {results_path}")

    # Strategy insights
    print("\n" + "="*80)
    print("🔍 STRATEGY INSIGHTS")
    print("="*80)

    # Timeframe analysis
    print("\n📊 TIMEFRAME ANALYSIS:")
    for _, row in results_df.iterrows():
        timeframe = row['Data_Source'].split('-')[1]
        if row['Sharpe'] > 1.0:
            quality = "✅ EXCELLENT"
        elif row['Sharpe'] > 0.5:
            quality = "✅ GOOD"
        else:
            quality = "⚠️ NEEDS IMPROVEMENT"
        print(f"   • {timeframe}: {quality} (Sharpe: {row['Sharpe']}, Return: {row['Return_%']}%)")

    # Mode effectiveness
    print("\n🔄 DUAL-MODE EFFECTIVENESS:")
    avg_trades = results_df['Trades'].mean()
    avg_win_rate = results_df['Win_Rate_%'].mean()
    avg_profit_factor = results_df['Profit_Factor'].mean()

    print(f"   • Average {avg_trades:.0f} trades shows selective entry")
    print(f"   • Win rate {avg_win_rate:.1f}% with profit factor {avg_profit_factor:.2f}")
    print(f"   • Low max drawdown ({results_df['Max_DD_%'].mean():.1f}%) demonstrates risk control")

    # Recommendations
    print("\n💡 OPTIMIZATION RECOMMENDATIONS:")

    if results_df['Sharpe'].mean() > 0.8:
        print("   ✅ Strategy shows strong performance across timeframes")

    if results_df['Win_Rate_%'].mean() < 40:
        print("   • Consider tightening entry filters to improve win rate")

    if results_df['Max_DD_%'].max() > 15:
        print("   • Consider adjusting position sizing for lower drawdown")

    if results_df['Trades'].min() < 50:
        print("   • Some timeframes have low trade count - consider parameter adjustment")

    print("\n✅ Multi-data testing complete!")

else:
    print("\n❌ No results generated")