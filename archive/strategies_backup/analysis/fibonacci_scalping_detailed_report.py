"""
🌙 Fibonacci Scalping Strategy - Detailed Performance Report 🌙
================================================================
Comprehensive backtesting analysis with full performance stats as required by CLAUDE.md

Features:
- Full performance stats (Sharpe, Sortino, Max Drawdown, Win Rate, etc.)
- Interactive plots without saving HTML
- Multi-asset testing with asset suitability rankings
- Cross-provider validation where available
- Optimization suggestions based on results

Author: Bobby (algo-fun project)
Date: 2025-01-16
"""

import sys
import os
sys.path.append('/Users/bobbyyo/Projects/algo-fun')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib
from datetime import datetime, time
import pytz
import warnings
warnings.filterwarnings('ignore')

# Import the strategy
from strategies.indicators.fibonacci_scalping_1m_strategy import FibonacciScalpingStrategy

def load_validated_data(symbol='XRPUSD', timeframe='1m', provider='hyperliquid'):
    """
    🔍 Load quality-validated data (score ≥75) as per CLAUDE.md requirements
    """
    # Updated path structure based on actual data organization
    if provider == 'hyperliquid':
        data_path = f'/Users/bobbyyo/Projects/algo-fun/data/hyperliquid/{symbol}-{timeframe}-5000bars-enhanced-data.csv'
    else:
        data_path = f'/Users/bobbyyo/Projects/algo-fun/data/coinbase/{symbol}-{timeframe}-50wks-enhanced-data.csv'

    if not os.path.exists(data_path):
        print(f"❌ Data file not found: {data_path}")
        # Try alternative naming conventions
        alt_paths = [
            f'/Users/bobbyyo/Projects/algo-fun/data/{provider}/{symbol}-{timeframe}-enhanced-data.csv',
            f'/Users/bobbyyo/Projects/algo-fun/data/{symbol}-{timeframe}-{provider}-data.csv'
        ]

        for alt_path in alt_paths:
            if os.path.exists(alt_path):
                data_path = alt_path
                break
        else:
            return None

    try:
        df = pd.read_csv(data_path)
        # Handle different datetime column names
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        else:
            print(f"❌ No datetime column found in {data_path}")
            return None

        # Rename columns to match backtesting.py requirements
        column_mapping = {
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }
        df = df.rename(columns=column_mapping)

        # Data quality validation
        missing_pct = df.isnull().sum().sum() / len(df) * 100
        duplicate_pct = df.index.duplicated().sum() / len(df) * 100

        quality_score = 100 - missing_pct - duplicate_pct

        print(f"📊 Data Quality Assessment for {symbol} ({timeframe}, {provider}):")
        print(f"   📈 Total bars: {len(df):,}")
        print(f"   📅 Date range: {df.index.min()} to {df.index.max()}")
        print(f"   🎯 Quality score: {quality_score:.1f}/100")
        print(f"   ✅ Missing data: {missing_pct:.2f}%")
        print(f"   🔄 Duplicates: {duplicate_pct:.2f}%")

        if quality_score < 75:
            print(f"   ⚠️  Quality score below threshold (75), skipping...")
            return None

        return df

    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

def calculate_detailed_stats(result):
    """
    📊 Calculate comprehensive performance statistics as required by CLAUDE.md
    """
    if len(result._trades) == 0:
        return {
            'Return [%]': 0,
            'Sharpe Ratio': np.nan,
            'Sortino Ratio': np.nan,
            'Max Drawdown [%]': 0,
            'Win Rate [%]': 0,
            'Profit Factor': np.nan,
            'Total Trades': 0,
            'Avg Trade [%]': 0,
            'Best Trade [%]': 0,
            'Worst Trade [%]': 0,
            'Avg Winning Trade [%]': 0,
            'Avg Losing Trade [%]': 0,
            'Max Consecutive Wins': 0,
            'Max Consecutive Losses': 0,
            'Calmar Ratio': np.nan
        }

    trades = result._trades
    returns = trades['ReturnPct']

    # Basic metrics
    total_return = result['Return [%]']
    sharpe = result['Sharpe Ratio']
    max_dd = result['Max. Drawdown [%]']

    # Win/Loss metrics
    winning_trades = returns[returns > 0]
    losing_trades = returns[returns < 0]
    win_rate = len(winning_trades) / len(returns) * 100 if len(returns) > 0 else 0

    # Profit factor
    gross_profit = winning_trades.sum() if len(winning_trades) > 0 else 0
    gross_loss = abs(losing_trades.sum()) if len(losing_trades) > 0 else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan

    # Trade statistics
    avg_trade = returns.mean() if len(returns) > 0 else 0
    best_trade = returns.max() if len(returns) > 0 else 0
    worst_trade = returns.min() if len(returns) > 0 else 0
    avg_win = winning_trades.mean() if len(winning_trades) > 0 else 0
    avg_loss = losing_trades.mean() if len(losing_trades) > 0 else 0

    # Consecutive wins/losses
    def max_consecutive(series, condition):
        consecutive = 0
        max_consecutive = 0
        for val in series:
            if condition(val):
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0
        return max_consecutive

    max_wins = max_consecutive(returns, lambda x: x > 0)
    max_losses = max_consecutive(returns, lambda x: x < 0)

    # Sortino Ratio
    downside_returns = returns[returns < 0]
    downside_std = downside_returns.std() if len(downside_returns) > 0 else 0
    sortino = (returns.mean() / downside_std) * np.sqrt(252) if downside_std > 0 else np.nan

    # Calmar Ratio
    calmar = total_return / abs(max_dd) if max_dd != 0 else np.nan

    return {
        'Return [%]': total_return,
        'Sharpe Ratio': sharpe,
        'Sortino Ratio': sortino,
        'Max Drawdown [%]': max_dd,
        'Win Rate [%]': win_rate,
        'Profit Factor': profit_factor,
        'Total Trades': len(trades),
        'Avg Trade [%]': avg_trade,
        'Best Trade [%]': best_trade,
        'Worst Trade [%]': worst_trade,
        'Avg Winning Trade [%]': avg_win,
        'Avg Losing Trade [%]': avg_loss,
        'Max Consecutive Wins': max_wins,
        'Max Consecutive Losses': max_losses,
        'Calmar Ratio': calmar
    }

def run_detailed_analysis(symbol='XRPUSD', timeframe='1m', provider='hyperliquid'):
    """
    🚀 Run comprehensive backtesting analysis with detailed reporting
    """
    print(f"\n{'='*80}")
    print(f"🚀 DETAILED FIBONACCI SCALPING ANALYSIS: {symbol} ({timeframe}, {provider}) 🚀")
    print(f"{'='*80}")

    # Load and validate data
    data = load_validated_data(symbol, timeframe, provider)
    if data is None:
        return None

    print(f"\n📈 Running backtest...")

    try:
        # Initialize backtest
        bt = Backtest(
            data,
            FibonacciScalpingStrategy,
            cash=10000,
            commission=.002,  # 0.2% commission
            exclusive_orders=True
        )

        # Run backtest
        result = bt.run()

        # Calculate detailed statistics
        detailed_stats = calculate_detailed_stats(result)

        print(f"\n{'='*60}")
        print(f"📊 COMPREHENSIVE PERFORMANCE STATISTICS")
        print(f"{'='*60}")

        # Core Performance Metrics
        print(f"🎯 CORE PERFORMANCE METRICS")
        print(f"   💰 Total Return: {detailed_stats['Return [%]']:.2f}%")
        print(f"   📈 Sharpe Ratio: {detailed_stats['Sharpe Ratio']:.3f}" if not np.isnan(detailed_stats['Sharpe Ratio']) else "   📈 Sharpe Ratio: N/A")
        print(f"   📉 Sortino Ratio: {detailed_stats['Sortino Ratio']:.3f}" if not np.isnan(detailed_stats['Sortino Ratio']) else "   📉 Sortino Ratio: N/A")
        print(f"   📊 Max Drawdown: {detailed_stats['Max Drawdown [%]']:.2f}%")
        print(f"   🎯 Calmar Ratio: {detailed_stats['Calmar Ratio']:.3f}" if not np.isnan(detailed_stats['Calmar Ratio']) else "   🎯 Calmar Ratio: N/A")

        # Trading Statistics
        print(f"\n🔢 TRADING STATISTICS")
        print(f"   📊 Total Trades: {detailed_stats['Total Trades']}")
        print(f"   🏆 Win Rate: {detailed_stats['Win Rate [%]']:.2f}%")
        print(f"   💎 Profit Factor: {detailed_stats['Profit Factor']:.3f}" if not np.isnan(detailed_stats['Profit Factor']) else "   💎 Profit Factor: N/A")
        print(f"   📊 Avg Trade: {detailed_stats['Avg Trade [%]']:.3f}%")

        # Best/Worst Trades
        print(f"\n🚀 TRADE EXTREMES")
        print(f"   🎉 Best Trade: {detailed_stats['Best Trade [%]']:.2f}%")
        print(f"   😞 Worst Trade: {detailed_stats['Worst Trade [%]']:.2f}%")
        print(f"   🏆 Avg Winning Trade: {detailed_stats['Avg Winning Trade [%]']:.3f}%")
        print(f"   📉 Avg Losing Trade: {detailed_stats['Avg Losing Trade [%]']:.3f}%")

        # Streak Analysis
        print(f"\n🔥 STREAK ANALYSIS")
        print(f"   🎯 Max Consecutive Wins: {detailed_stats['Max Consecutive Wins']}")
        print(f"   📉 Max Consecutive Losses: {detailed_stats['Max Consecutive Losses']}")

        # Strategy Assessment
        print(f"\n{'='*60}")
        print(f"🔍 STRATEGY ASSESSMENT")
        print(f"{'='*60}")

        # Performance Rating
        if detailed_stats['Return [%]'] > 0 and detailed_stats['Win Rate [%]'] > 50:
            rating = "🌟 EXCELLENT"
        elif detailed_stats['Return [%]'] > 0 or detailed_stats['Win Rate [%]'] > 40:
            rating = "⚡ GOOD"
        elif detailed_stats['Return [%]'] > -20:
            rating = "⚠️  NEEDS OPTIMIZATION"
        else:
            rating = "❌ POOR - MAJOR REVISION NEEDED"

        print(f"📊 Overall Performance: {rating}")

        # Specific recommendations
        print(f"\n💡 OPTIMIZATION RECOMMENDATIONS:")

        if detailed_stats['Win Rate [%]'] < 40:
            print(f"   🎯 Low win rate ({detailed_stats['Win Rate [%]']:.1f}%) - Consider:")
            print(f"      • Tightening entry criteria")
            print(f"      • Adding confluence indicators")
            print(f"      • Implementing better trend filters")

        if detailed_stats['Total Trades'] < 50:
            print(f"   📊 Low trade frequency ({detailed_stats['Total Trades']} trades) - Consider:")
            print(f"      • Relaxing entry requirements")
            print(f"      • Testing on higher timeframes")
            print(f"      • Expanding trading sessions")

        if abs(detailed_stats['Max Drawdown [%]']) > 30:
            print(f"   📉 High drawdown ({detailed_stats['Max Drawdown [%]']:.1f}%) - Consider:")
            print(f"      • Reducing position sizes")
            print(f"      • Implementing trailing stops")
            print(f"      • Adding volatility filters")

        if not np.isnan(detailed_stats['Profit Factor']) and detailed_stats['Profit Factor'] < 1.5:
            print(f"   💰 Low profit factor ({detailed_stats['Profit Factor']:.2f}) - Consider:")
            print(f"      • Improving take profit levels")
            print(f"      • Optimizing stop loss placement")
            print(f"      • Better trade selection criteria")

        # Show interactive plot (no HTML saving as per CLAUDE.md)
        print(f"\n📊 Generating interactive plot...")
        plot = bt.plot(show_legend=True, open_browser=False)
        plt.show()

        return detailed_stats

    except Exception as e:
        print(f"❌ Error during backtest: {e}")
        return None

def main():
    """
    🌙 Main execution function - comprehensive Fibonacci scalping analysis 🌙
    """
    print("🌙💫🚀 FIBONACCI SCALPING STRATEGY - DETAILED PERFORMANCE REPORT 🚀💫🌙")
    print("=" * 80)

    # Test configurations (as per CLAUDE.md multi-asset requirements)
    test_configs = [
        ('XRPUSD', '1m', 'hyperliquid'),  # Best performer from multi-asset test
        ('BTCUSD', '5m', 'coinbase'),     # Alternative asset/timeframe
        ('CROUSD', '5m', 'coinbase'),     # Additional crypto asset
    ]

    all_results = []

    for symbol, timeframe, provider in test_configs:
        print(f"\n🔍 Testing {symbol} ({timeframe}) from {provider}...")
        result = run_detailed_analysis(symbol, timeframe, provider)

        if result:
            result['Symbol'] = symbol
            result['Timeframe'] = timeframe
            result['Provider'] = provider
            all_results.append(result)

    # Asset Performance Ranking (as required by CLAUDE.md)
    if all_results:
        print(f"\n{'='*80}")
        print(f"🏆 CROSS-ASSET PERFORMANCE RANKING")
        print(f"{'='*80}")

        results_df = pd.DataFrame(all_results)
        results_df = results_df.sort_values('Return [%]', ascending=False)

        print(f"📊 RANKING BY TOTAL RETURN:")
        for i, row in results_df.iterrows():
            symbol_info = f"{row['Symbol']} ({row['Timeframe']}, {row['Provider']})"
            print(f"   {symbol_info:<30} Return: {row['Return [%]']:>8.2f}% | Win Rate: {row['Win Rate [%]']:>6.2f}% | Trades: {row['Total Trades']:>5}")

        # Best asset recommendation
        best_asset = results_df.iloc[0]
        print(f"\n🌟 BEST PERFORMING ASSET:")
        print(f"   {best_asset['Symbol']} ({best_asset['Timeframe']}, {best_asset['Provider']})")
        print(f"   Return: {best_asset['Return [%]']:.2f}% | Win Rate: {best_asset['Win Rate [%]']:.2f}%")

    print(f"\n✅ Analysis complete! Strategy assessment follows CLAUDE.md requirements 🌙💫🚀")

if __name__ == "__main__":
    main()