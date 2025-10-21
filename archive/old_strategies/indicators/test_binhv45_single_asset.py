"""
🎯 BinHV45 Single Asset Testing Script 🎯
=========================================
Focused testing of BinHV45 mean-reversion strategy on XRP 1-minute data
to demonstrate full native backtesting.py results.

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 🚨 MANDATORY: Import universal native results display
from analysis.universal_native_results_display import enhanced_backtest_runner, create_data_source_info

# Import our strategy
from binhv45_mean_reversion_strategy import BinHV45Strategy


def test_binhv45_on_xrp():
    """
    🚀 Test BinHV45 strategy on XRP 1-minute data with full native results
    """
    print("="*100)
    print("🌙 BinHV45 Mean-Reversion Strategy Testing on XRP 1-Minute Data 🌙")
    print("="*100)

    # Define data source
    data_path = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/XRPUSD-1m-52wks-enhanced-data.csv"

    # Load data
    print(f"\n📊 Loading data from: {data_path}")
    data = pd.read_csv(data_path, index_col=0, parse_dates=True)

    # Rename columns to match backtesting.py requirements
    data.columns = [col.capitalize() for col in data.columns]

    print(f"✅ Data loaded: {len(data)} bars")
    print(f"📅 Date range: {data.index[0]} to {data.index[-1]}")

    # Create data source info
    data_source_info = create_data_source_info(
        data_path,
        symbol="XRP",
        timeframe="1m",
        provider="coinbase"
    )

    # 🚨 MANDATORY: Use enhanced_backtest_runner for native results
    print("\n" + "="*100)
    print("🎯 Running BinHV45 Strategy Backtest with FULL NATIVE RESULTS")
    print("="*100)

    summary_stats, full_stats = enhanced_backtest_runner(
        data=data,
        strategy_class=BinHV45Strategy,
        data_source_info=data_source_info,
        strategy_name="BinHV45 Mean-Reversion",
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    # Performance Analysis
    print("\n" + "="*100)
    print("📊 PERFORMANCE ANALYSIS")
    print("="*100)

    print("\n🎯 Key Strategy Characteristics:")
    print(f"  • Strategy Type: Mean-Reversion")
    print(f"  • Entry: Price below lower BB with multiple confirmations")
    print(f"  • Exit: Fixed SL (-5%) and TP (+1.25%)")
    print(f"  • Risk/Reward Ratio: 1:4")

    print("\n📈 Performance Metrics Interpretation:")

    # Sharpe Ratio Analysis
    sharpe = full_stats.get('Sharpe Ratio', np.nan)
    if not np.isnan(sharpe):
        print(f"\n  📊 Sharpe Ratio: {sharpe:.3f}")
        if sharpe > 1.5:
            print(f"     ✅ Excellent risk-adjusted returns")
        elif sharpe > 1.0:
            print(f"     ✅ Good risk-adjusted returns")
        elif sharpe > 0.5:
            print(f"     ⚠️ Moderate risk-adjusted returns")
        else:
            print(f"     ❌ Poor risk-adjusted returns")

    # Win Rate Analysis
    win_rate = full_stats.get('Win Rate [%]', 0)
    if win_rate > 0:
        print(f"\n  🎯 Win Rate: {win_rate:.1f}%")
        if win_rate > 80:
            print(f"     ✅ Excellent win rate for mean-reversion")
        elif win_rate > 60:
            print(f"     ✅ Good win rate")
        else:
            print(f"     ⚠️ Win rate needs improvement")

    # Trades Analysis
    num_trades = full_stats.get('# Trades', 0)
    print(f"\n  🔢 Total Trades: {num_trades}")
    if num_trades > 0:
        avg_trade = full_stats.get('Avg. Trade [%]', 0)
        print(f"     📊 Average Trade: {avg_trade:.4f}%")

        # Calculate expected value per trade
        if win_rate > 0:
            avg_win = abs(avg_trade) if avg_trade > 0 else 0
            avg_loss = -5.0  # Our stop loss
            expected_value = (win_rate/100 * avg_win) + ((100-win_rate)/100 * avg_loss)
            print(f"     💰 Expected Value per Trade: {expected_value:.4f}%")

    # Drawdown Analysis
    max_dd = full_stats.get('Max. Drawdown [%]', 0)
    print(f"\n  📉 Maximum Drawdown: {max_dd:.2f}%")
    if abs(max_dd) < 10:
        print(f"     ✅ Excellent drawdown control")
    elif abs(max_dd) < 20:
        print(f"     ⚠️ Moderate drawdown")
    else:
        print(f"     ❌ High drawdown - review risk management")

    # Return Analysis
    total_return = full_stats.get('Return [%]', 0)
    buy_hold = full_stats.get('Buy & Hold Return [%]', 0)
    print(f"\n  💰 Strategy Return: {total_return:.2f}%")
    print(f"  📊 Buy & Hold Return: {buy_hold:.2f}%")

    if total_return > buy_hold:
        print(f"     ✅ Outperformed buy & hold by {total_return - buy_hold:.2f}%")
    else:
        print(f"     ❌ Underperformed buy & hold by {buy_hold - total_return:.2f}%")

    # Strategy Assessment
    print("\n" + "="*100)
    print("🎯 STRATEGY ASSESSMENT")
    print("="*100)

    print("\n📊 Entry Conditions Analysis:")
    print("  1. ✅ Prior lower BB > 0 - Ensures valid BB calculation")
    print("  2. ✅ BB width is large - Filters for high volatility periods")
    print("  3. ✅ Close delta is large - Confirms significant price movement")
    print("  4. ✅ Close < prior lower BB - Entry on oversold condition")
    print("  5. ✅ Close <= prior close - Confirms downward momentum")
    print("  6. ✅ Small tail - Price closing near lows (selling pressure)")

    print("\n🛡️ Risk Management:")
    print("  • Stop Loss: -5% (protects capital)")
    print("  • Take Profit: +1.25% (4:1 risk/reward ratio)")
    print("  • Position Sizing: 95% of capital (aggressive for backtesting)")

    print("\n💡 Optimization Suggestions:")
    if win_rate < 80:
        print("  • Consider loosening entry conditions for more trades")
    if abs(max_dd) > 15:
        print("  • Tighten stop loss or reduce position size")
    if num_trades < 50:
        print("  • Review threshold parameters - may be too restrictive")
    if sharpe < 1.0:
        print("  • Adjust take profit target or entry thresholds")

    print("\n🚀 Production Readiness:")
    is_ready = (
        sharpe > 1.0 and
        win_rate > 70 and
        abs(max_dd) < 20 and
        total_return > 0
    )

    if is_ready:
        print("  ✅ Strategy shows promise for paper trading")
        print("  💡 Recommended next steps:")
        print("     1. Test with smaller position sizes (10-20%)")
        print("     2. Implement on paper trading account")
        print("     3. Monitor for 2-4 weeks before live deployment")
    else:
        print("  ⚠️ Strategy needs optimization before deployment")
        print("  💡 Focus on improving:")
        if sharpe <= 1.0:
            print("     • Risk-adjusted returns (Sharpe ratio)")
        if win_rate <= 70:
            print("     • Win rate through entry optimization")
        if abs(max_dd) >= 20:
            print("     • Drawdown control")

    print("\n" + "="*100)
    print("✅ BinHV45 Strategy Testing Complete")
    print("="*100)

    return summary_stats, full_stats


if __name__ == "__main__":
    # Run the test
    test_binhv45_on_xrp()