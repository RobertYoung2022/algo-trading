# test_fixed_multi_asset.py
"""
🛡️ SECURITY-ENHANCED: Multi-Asset Strategy Testing
Includes mandatory data validation for all asset sources (input security)
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import warnings
import sys

# Add path for universal display module
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import enhanced_backtest_runner, create_data_source_info

warnings.filterwarnings('ignore')

# 🛡️ SECURITY: Import data validation for multi-asset input security
from trading_functions import DataQualityValidator, validate_data_source_quality

from volatility_multi_asset_fixed import VolatilityMultiAssetStrategy

print("🌍 TESTING FIXED MULTI-ASSET STRATEGY")
print("=" * 80)

# Define key test sources across different assets
TEST_SOURCES = [
    # BTC - Different timeframes
    ('BTC-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv'),
    ('BTC-6h-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/BTCUSD-6h-200wks-enhanced-data.csv'),

    # ETH - Different timeframes
    ('ETH-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv'),
    ('ETH-6h-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-6h-200wks-enhanced-data.csv'),

    # CRO
    ('CRO-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/CROUSD-1d-1000wks-enhanced-data.csv'),

    # HBAR
    ('HBAR-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/HBARUSD-1d-1000wks-enhanced-data.csv'),

    # LINK
    ('LINK-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv'),

    # XRP
    ('XRP-1d-Coinbase', '/Users/bobbyyo/Projects/algo-fun/data/coinbase/XRPUSD-1d-500wks-enhanced-data.csv'),
]

def clean_data(data):
    """Clean and fix OHLC data"""
    for col in ['Open', 'High', 'Low', 'Close']:
        if col in data.columns:
            data[col] = data[col].abs()
            data[col] = data[col].replace(0, np.nan).fillna(method='ffill')

    data['High'] = data[['Open', 'High', 'Low', 'Close']].max(axis=1)
    data['Low'] = data[['Open', 'High', 'Low', 'Close']].min(axis=1)

    if 'Volume' in data.columns:
        data['Volume'] = data['Volume'].abs()
        data['Volume'] = data['Volume'].replace(0, data['Volume'].mean() * 0.1)

    return data.dropna()

results = []

for name, path in TEST_SOURCES:
    print(f"\n📊 Testing: {name}")
    print("-" * 40)

    try:
        # 🛡️ SECURITY: Validate data quality before processing (prevents malformed data)
        print(f"🛡️ Validating {name} data quality for security...")
        validator = DataQualityValidator()
        validation_result = validate_data_source_quality(path, validator)

        if validation_result.overall_score < 75:
            print(f"❌ SECURITY BLOCK: {name} data quality too low: {validation_result.overall_score}")
            print("🛡️ SECURITY: Skipping potentially corrupted data source")
            continue

        print(f"✅ {name} data security validated - Quality score: {validation_result.overall_score}")

        # Load data
        data = pd.read_csv(path, parse_dates=['datetime'], index_col='datetime')
        data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data = data.sort_index()
        data = clean_data(data)

        print(f"✅ Data: {len(data)} bars from {data.index[0].date()} to {data.index[-1].date()}")

        # Set cash based on timeframe
        if '6h' in name or '1h' in name:
            cash = 5000000
        else:
            cash = 1000000

        # Run backtest
        # Create data source info for native display
        data_source_info = create_data_source_info(
            file_path=filepath,
            symbol=name.split('-')[0],
            timeframe=name.split('-')[1],
            provider=name.split('-')[2]
        )

        # Run backtest with FULL NATIVE RESULTS DISPLAY
        summary_stats, stats = enhanced_backtest_runner(
            data=data,
            strategy_class=VolatilityMultiAssetStrategy,
            data_source_info=data_source_info,
            strategy_name="Volatility Multi-Asset Strategy",
            cash=cash,
            commission=0.001,
            exclusive_orders=True,
            trade_on_close=True
        )

        # Store results
        result = {
            'Asset': name.split('-')[0],
            'Timeframe': name.split('-')[1],
            'Provider': name.split('-')[2],
            'Bars': len(data),
            'Return_%': round(stats['Return [%]'], 2),
            'Sharpe': round(stats['Sharpe Ratio'], 3) if not np.isnan(stats['Sharpe Ratio']) else 0,
            'Max_DD_%': round(stats['Max. Drawdown [%]'], 2),
            'Trades': stats['# Trades'],
            'Win_Rate_%': round(stats['Win Rate [%]'], 2) if stats['# Trades'] > 0 else 0,
            'Profit_Factor': round(stats.get('Profit Factor', 0), 2) if not np.isnan(stats.get('Profit Factor', 0)) else 0
        }

        results.append(result)

        # Print results
        print(f"📈 Return: {stats['Return [%]']:.2f}%")
        print(f"📊 Sharpe: {stats['Sharpe Ratio']:.3f}" if not np.isnan(stats['Sharpe Ratio']) else "📊 Sharpe: N/A")
        print(f"📉 Max DD: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"🎯 Trades: {stats['# Trades']}")
        if stats['# Trades'] > 0:
            print(f"✅ Win Rate: {stats['Win Rate [%]']:.1f}%")

        # Print full stats for first test
        if name == TEST_SOURCES[0][0]:
            print("\n📊 FULL STATS FOR BTC-1D:")
            print(stats)

    except Exception as e:
        print(f"❌ Error: {str(e)[:100]}")

# Create summary DataFrame
if results:
    results_df = pd.DataFrame(results)

    print("\n" + "=" * 80)
    print("📊 COMPREHENSIVE MULTI-ASSET RESULTS")
    print("=" * 80)
    print("\n", results_df.to_string(index=False))

    # Asset performance summary
    print("\n🏆 ASSET PERFORMANCE RANKING")
    print("-" * 80)

    asset_summary = results_df.groupby('Asset').agg({
        'Sharpe': 'mean',
        'Return_%': 'mean',
        'Max_DD_%': 'mean',
        'Win_Rate_%': 'mean',
        'Trades': 'sum'
    }).round(2)

    asset_summary = asset_summary.sort_values('Sharpe', ascending=False)
    print(asset_summary)

    # Best performers
    print("\n🥇 TOP PERFORMERS")
    print("-" * 80)

    top_3 = results_df.nlargest(3, 'Sharpe')
    for idx, row in top_3.iterrows():
        print(f"• {row['Asset']}-{row['Timeframe']}: Sharpe {row['Sharpe']}, Return {row['Return_%']}%")

    # Strategy effectiveness
    print("\n💡 STRATEGY INSIGHTS")
    print("-" * 80)

    positive_return_pct = (results_df['Return_%'] > 0).mean() * 100
    avg_sharpe = results_df['Sharpe'].mean()
    avg_win_rate = results_df['Win_Rate_%'].mean()

    print(f"• Positive Returns: {positive_return_pct:.0f}% of tests")
    print(f"• Average Sharpe: {avg_sharpe:.3f}")
    print(f"• Average Win Rate: {avg_win_rate:.1f}%")

    if avg_sharpe > 0.5:
        print("✅ Strategy shows promise across multiple assets")
    elif avg_sharpe > 0:
        print("⚠️ Strategy needs parameter optimization")
    else:
        print("❌ Strategy requires significant adjustments")

    # Cross-asset comparison
    print("\n🌍 CROSS-ASSET INSIGHTS")
    print("-" * 80)

    best_asset = asset_summary.index[0]
    worst_asset = asset_summary.index[-1]

    print(f"• Best Asset: {best_asset} (Avg Sharpe: {asset_summary.loc[best_asset, 'Sharpe']})")
    print(f"• Worst Asset: {worst_asset} (Avg Sharpe: {asset_summary.loc[worst_asset, 'Sharpe']})")

    # Timeframe analysis
    timeframe_summary = results_df.groupby('Timeframe')['Sharpe'].mean()
    best_timeframe = timeframe_summary.idxmax()
    print(f"• Best Timeframe: {best_timeframe} (Avg Sharpe: {timeframe_summary[best_timeframe]:.3f})")

    # Save results
    results_df.to_csv('/Users/bobbyyo/Projects/algo-fun/strategies/analysis/results/fixed_strategy_results.csv', index=False)
    print(f"\n📄 Results saved to: fixed_strategy_results.csv")

print("\n✅ Multi-asset testing complete!")