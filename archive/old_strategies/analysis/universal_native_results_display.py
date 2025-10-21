"""
🌙 Universal Native Backtesting.py Results Display Module 🌙
============================================================
Standardized display functions for showing full native backtesting.py results
across ALL strategies in Bobby's algo-fun project.

This module ensures consistent display of complete backtesting.py output
for every strategy test, replacing summarized metrics with full native results.

Author: Bobby (algo-fun project)
Date: 2025-01-16
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from datetime import datetime

# 🚨 ENSURE COMPLETE OUTPUT DISPLAY - NO TRUNCATION
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
np.set_printoptions(threshold=np.inf)


def display_full_native_results(stats, data_source_info, strategy_name="Strategy"):
    """
    🚀 Display complete native backtesting.py results with proper formatting 🚀

    This function shows the FULL backtesting.py output exactly as produced,
    with no summarization or truncation.

    Args:
        stats: Full backtesting.py results object
        data_source_info: Dict with source metadata (symbol, timeframe, provider, etc.)
        strategy_name: Name of the strategy being tested
    """

    # Extract metadata
    symbol = data_source_info.get('symbol', 'Unknown')
    timeframe = data_source_info.get('timeframe', 'Unknown')
    provider = data_source_info.get('provider', 'Unknown')
    file_path = data_source_info.get('path', 'Unknown')

    print(f"\n{'='*100}")
    print(f"📊 FULL BACKTESTING.PY NATIVE RESULTS - {strategy_name}")
    print(f"🎯 Asset: {symbol} | ⏰ Timeframe: {timeframe} | 🏢 Provider: {provider}")
    print(f"📁 Data Source: {file_path}")
    print(f"{'='*100}")

    # Display the COMPLETE native backtesting.py results - NEVER summarize
    print(stats)

    print(f"{'='*100}")
    print(f"✅ NATIVE RESULTS DISPLAY COMPLETE FOR {symbol} ({timeframe}, {provider})")
    print(f"{'='*100}\n")


def display_trades_summary(stats, max_trades_display=10):
    """
    📊 Display detailed trades summary from backtesting.py results 📊

    Shows first/last trades and statistics as supplement to native results.

    Args:
        stats: Full backtesting.py results object
        max_trades_display: Maximum number of trades to show from start/end
    """

    if hasattr(stats, '_trades') and len(stats._trades) > 0:
        trades = stats._trades

        print(f"📈 DETAILED TRADES ANALYSIS")
        print(f"{'='*60}")
        print(f"📊 Total Trades: {len(trades)}")

        if len(trades) > 0:
            print(f"\n📊 First {min(max_trades_display, len(trades))} trades:")
            print(trades.head(max_trades_display))

            if len(trades) > max_trades_display:
                print(f"\n📊 Last {min(max_trades_display, len(trades))} trades:")
                print(trades.tail(max_trades_display))

            # Trade statistics
            winning_trades = trades[trades['ReturnPct'] > 0]
            losing_trades = trades[trades['ReturnPct'] < 0]

            print(f"\n📊 Trade Statistics:")
            print(f"   🏆 Winning Trades: {len(winning_trades)}")
            print(f"   📉 Losing Trades: {len(losing_trades)}")
            print(f"   📊 Average Return per Trade: {trades['ReturnPct'].mean():.4f}%")

            if len(winning_trades) > 0:
                print(f"   🎉 Average Winning Trade: {winning_trades['ReturnPct'].mean():.4f}%")
                print(f"   🚀 Best Trade: {winning_trades['ReturnPct'].max():.4f}%")

            if len(losing_trades) > 0:
                print(f"   😞 Average Losing Trade: {losing_trades['ReturnPct'].mean():.4f}%")
                print(f"   📉 Worst Trade: {losing_trades['ReturnPct'].min():.4f}%")
    else:
        print("❌ No trades generated")

    print(f"{'='*60}\n")


def create_data_source_info(file_path, symbol=None, timeframe=None, provider=None):
    """
    🔍 Create standardized data source information dictionary 🔍

    Extracts metadata from file path and provided information for consistent display.

    Args:
        file_path: Full path to data file
        symbol: Asset symbol (auto-detected if None)
        timeframe: Data timeframe (auto-detected if None)
        provider: Data provider (auto-detected if None)

    Returns:
        Dict with standardized data source information
    """

    # Auto-detect from filename if not provided
    filename = file_path.split('/')[-1] if '/' in file_path else file_path
    parts = filename.split('-')

    if symbol is None and len(parts) >= 1:
        symbol = parts[0]

    if timeframe is None and len(parts) >= 2:
        timeframe = parts[1]

    if provider is None:
        if 'coinbase' in file_path.lower():
            provider = 'coinbase'
        elif 'yahoo' in file_path.lower():
            provider = 'yahoo'
        elif 'coingecko' in file_path.lower():
            provider = 'coingecko'
        elif 'hyperliquid' in file_path.lower():
            provider = 'hyperliquid'
        elif 'cryptocompare' in file_path.lower():
            provider = 'cryptocompare'
        else:
            provider = 'unknown'

    return {
        'path': file_path,
        'filename': filename,
        'symbol': symbol or 'Unknown',
        'timeframe': timeframe or 'Unknown',
        'provider': provider or 'Unknown'
    }


def enhanced_backtest_runner(data, strategy_class, data_source_info, strategy_name="Strategy", **backtest_kwargs):
    """
    🚀 Enhanced backtest runner with mandatory native results display 🚀

    Runs backtest and automatically displays full native results.
    This function should be used by ALL strategy testing frameworks.

    Args:
        data: DataFrame with OHLCV data
        strategy_class: Strategy class to test
        data_source_info: Dict or string with data source information
        strategy_name: Name of strategy being tested
        **backtest_kwargs: Additional arguments for Backtest()

    Returns:
        Tuple of (summary_stats_dict, full_stats_object)
    """

    from backtesting import Backtest

    # Ensure data_source_info is properly formatted
    if isinstance(data_source_info, str):
        data_source_info = create_data_source_info(data_source_info)
    elif not isinstance(data_source_info, dict):
        data_source_info = {'path': 'Unknown', 'symbol': 'Unknown', 'timeframe': 'Unknown', 'provider': 'Unknown'}

    # Set default backtest parameters
    default_kwargs = {
        'cash': 10000,
        'commission': 0.002,
        'exclusive_orders': True
    }
    default_kwargs.update(backtest_kwargs)

    # Run backtest
    bt = Backtest(data, strategy_class, **default_kwargs)
    stats = bt.run()

    # MANDATORY: Display full native backtesting.py results
    display_full_native_results(stats, data_source_info, strategy_name)

    # Optional: Display trades summary
    display_trades_summary(stats)

    # Create summary for CSV/comparison purposes
    summary_stats = {
        'Symbol': data_source_info.get('symbol', 'Unknown'),
        'Timeframe': data_source_info.get('timeframe', 'Unknown'),
        'Provider': data_source_info.get('provider', 'Unknown'),
        'Data_Source': data_source_info.get('path', 'Unknown'),
        'Return_%': stats.get('Return [%]', 0),
        'Sharpe': stats.get('Sharpe Ratio', np.nan),
        'Sortino': stats.get('Sortino Ratio', np.nan),
        'Max_DD_%': stats.get('Max. Drawdown [%]', 0),
        'Win_Rate_%': stats.get('Win Rate [%]', 0),
        'Trades': stats.get('# Trades', 0),
        'Profit_Factor': stats.get('Profit Factor', np.nan),
        'Best_Trade_%': stats.get('Best Trade [%]', 0),
        'Worst_Trade_%': stats.get('Worst Trade [%]', 0),
        'Avg_Trade_%': stats.get('Avg. Trade [%]', 0),
        'Exposure_%': stats.get('Exposure Time [%]', 0)
    }

    return summary_stats, stats


# 🌙💫🚀 Bobby's signature emoji style preserved throughout 🌙💫🚀