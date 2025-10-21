# test_volatility_strategy.py
"""
🛡️ SECURITY-ENHANCED: Dual-Mode Volatility Strategy Testing
Includes mandatory data quality validation for input security
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import warnings
warnings.filterwarnings('ignore')

# 🛡️ SECURITY: Import data validation for input security
from trading_functions import DataQualityValidator, validate_data_source_quality

# Import our strategy
from volatility_dual_mode_strategy import VolatilityDualModeStrategy

print("🚀 Direct Testing of Dual-Mode Volatility Strategy")
print("="*80)

# Test on BTC daily data (most reliable)
data_path = '/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv'  # 🛡️ Fixed: Using validated Yahoo data instead of corrupted 1000wks file

try:
    # 🛡️ SECURITY: Validate data quality before processing (prevents malformed data injection)
    print("🛡️ Validating data quality for security...")
    validator = DataQualityValidator()
    validation_result = validate_data_source_quality(data_path, validator)

    if validation_result.overall_score < 75:
        print(f"❌ SECURITY BLOCK: Data quality too low: {validation_result.overall_score}")
        print("🛡️ SECURITY: Preventing processing of potentially corrupted data")
        exit(1)

    print(f"✅ Data security validated - Quality score: {validation_result.overall_score}")
    print("📊 Loading BTC-USD 1d data...")
    data = pd.read_csv(data_path, parse_dates=['datetime'], index_col='datetime')
    # The columns are already named correctly: open, high, low, close, volume
    data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # Clean data
    data = data.dropna()
    data = data.sort_index()

    # Ensure positive prices
    for col in ['Open', 'High', 'Low', 'Close']:
        data[col] = data[col].abs()

    # Fix OHLC relationships
    data['High'] = data[['Open', 'High', 'Low', 'Close']].max(axis=1)
    data['Low'] = data[['Open', 'High', 'Low', 'Close']].min(axis=1)

    print(f"✅ Data loaded: {len(data)} bars from {data.index[0]} to {data.index[-1]}")

    # Run backtest
    print("\n🎯 Running backtest...")
    bt = Backtest(
        data,
        VolatilityDualModeStrategy,
        cash=1000000,
        commission=0.001
    )

    stats = bt.run()

    # Print full statistics
    print("\n" + "="*60)
    print("📈 BACKTEST RESULTS")
    print("="*60)
    print(stats)
    print("="*60)

    # Extract key metrics
    print("\n🌟 KEY PERFORMANCE METRICS:")
    print(f"   • Total Return: {stats['Return [%]']:.2f}%")
    print(f"   • Annualized Return: {stats['Return (Ann.) [%]']:.2f}%")
    print(f"   • Sharpe Ratio: {stats['Sharpe Ratio']:.3f}")
    print(f"   • Sortino Ratio: {stats['Sortino Ratio']:.3f}")
    print(f"   • Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
    print(f"   • Number of Trades: {stats['# Trades']}")
    print(f"   • Win Rate: {stats['Win Rate [%]']:.2f}%")
    print(f"   • Avg Trade: {stats['Avg. Trade [%]']:.3f}%")
    print(f"   • Best Trade: {stats['Best Trade [%]']:.2f}%")
    print(f"   • Worst Trade: {stats['Worst Trade [%]']:.2f}%")

    # Test on shorter timeframe if available
    print("\n" + "="*80)
    print("📊 Testing on BTC 6h data...")

    try:
        data_6h = pd.read_csv('/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-6h-500wks-data.csv',
                             parse_dates=['datetime'], index_col='datetime')
        data_6h.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data_6h = data_6h.dropna().sort_index()

        # Fix OHLC
        for col in ['Open', 'High', 'Low', 'Close']:
            data_6h[col] = data_6h[col].abs()
        data_6h['High'] = data_6h[['Open', 'High', 'Low', 'Close']].max(axis=1)
        data_6h['Low'] = data_6h[['Open', 'High', 'Low', 'Close']].min(axis=1)

        bt_6h = Backtest(data_6h, VolatilityDualModeStrategy, cash=1000000, commission=0.001)
        stats_6h = bt_6h.run()

        print(f"✅ 6h Results: Return: {stats_6h['Return [%]']:.2f}%, Sharpe: {stats_6h['Sharpe Ratio']:.3f}, Trades: {stats_6h['# Trades']}")

    except Exception as e:
        print(f"⚠️ Could not test on 6h data: {e}")

    # Performance comparison
    print("\n" + "="*80)
    print("🔍 STRATEGY ANALYSIS")
    print("="*80)

    # Analyze performance
    if stats['Sharpe Ratio'] > 1.0:
        print("✅ EXCELLENT: Sharpe Ratio > 1.0 indicates strong risk-adjusted returns")
    elif stats['Sharpe Ratio'] > 0.5:
        print("✅ GOOD: Sharpe Ratio > 0.5 indicates positive risk-adjusted returns")
    else:
        print("⚠️ NEEDS IMPROVEMENT: Sharpe Ratio < 0.5 suggests optimization needed")

    if stats['Max. Drawdown [%]'] < 20:
        print("✅ EXCELLENT: Max drawdown < 20% shows good risk control")
    elif stats['Max. Drawdown [%]'] < 30:
        print("✅ ACCEPTABLE: Max drawdown < 30% is within tolerance")
    else:
        print("⚠️ HIGH RISK: Max drawdown > 30% needs risk management improvement")

    if stats['Win Rate [%]'] > 50:
        print(f"✅ POSITIVE WIN RATE: {stats['Win Rate [%]']:.1f}% winning trades")
    else:
        print(f"⚠️ LOW WIN RATE: {stats['Win Rate [%]']:.1f}% - relies on big winners")

    # Save results
    results_path = '/Users/bobbyyo/Projects/algo-fun/strategies/analysis/results/volatility_dual_mode_results.csv'
    import os
    os.makedirs(os.path.dirname(results_path), exist_ok=True)

    # Create results dataframe
    results_df = pd.DataFrame([{
        'Strategy': 'Volatility_Dual_Mode',
        'Data': 'BTC-USD-1d',
        'Return_%': stats['Return [%]'],
        'Sharpe': stats['Sharpe Ratio'],
        'Sortino': stats['Sortino Ratio'],
        'Max_DD_%': stats['Max. Drawdown [%]'],
        'Trades': stats['# Trades'],
        'Win_Rate_%': stats['Win Rate [%]'],
        'Avg_Trade_%': stats['Avg. Trade [%]'],
        'Best_Trade_%': stats['Best Trade [%]'],
        'Worst_Trade_%': stats['Worst Trade [%]']
    }])

    results_df.to_csv(results_path, index=False)
    print(f"\n📄 Results saved to: {results_path}")

    print("\n✅ Strategy testing complete!")

except Exception as e:
    print(f"❌ Error during testing: {e}")
    import traceback
    traceback.print_exc()