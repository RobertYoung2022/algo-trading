"""
🌙 Market Structure Strategy - Final Multi-Asset Test 🌙
======================================================
Complete multi-asset testing framework for the Market Structure & Supply/Demand Strategy.

Author: Bobby's Algo Trading Systems 🚀
Date: 2025-01-17
"""

import pandas as pd
import numpy as np
from backtesting import Backtest
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
project_root = Path(__file__).parents[2]
sys.path.insert(0, str(project_root))

# Import strategy
from strategies.indicators.market_structure_supply_demand_strategy import MarketStructureSupplyDemandStrategy


def load_data(file_path):
    """Load and prepare data for backtesting"""

    df = pd.read_csv(file_path)

    # Handle column name variations
    column_mapping = {
        'datetime': 'Date',
        'time': 'Date',
        'date': 'Date',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }

    df.columns = [column_mapping.get(col.lower(), col.title()) for col in df.columns]

    # Set datetime index
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
    elif 'Time' in df.columns:
        df['Time'] = pd.to_datetime(df['Time'])
        df.set_index('Time', inplace=True)

    # Sort and remove duplicates
    df = df.sort_index()
    df = df[~df.index.duplicated(keep='first')]

    # Ensure numeric types
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove any NaN values
    df = df.dropna()

    return df


def run_single_test(data_file, symbol, timeframe, provider):
    """Run backtest on single data source"""

    try:
        # Load data
        df = load_data(data_file)

        if len(df) < 100:
            print(f"   ⚠️ Insufficient data for {symbol} ({len(df)} bars)")
            return None

        print(f"\n📊 Testing {symbol} - {timeframe} ({provider})")
        print(f"   Data: {len(df)} bars from {df.index[0]} to {df.index[-1]}")

        # Create backtest
        bt = Backtest(
            df,
            MarketStructureSupplyDemandStrategy,
            cash=100000,  # Increased cash for BTC
            commission=0.002,
            margin=0.1,
            trade_on_close=False
        )

        # Strategy parameters
        params = {
            'swing_lookback': 5,
            'consolidation_lookback': 3,
            'min_rr_ratio': 2.0,  # Lowered for more trades
            'zone_strength_threshold': 60,  # Lowered for more signals
            'max_zone_tests': 3,
            'volatility_period': 14,
            'volume_spike_threshold': 1.5,
            'multi_tf_confirm': False,
            'pullback_fib_min': 0.382,
            'correlation_threshold': 0.8
        }

        # Run backtest
        stats = bt.run(**params)

        # Display results
        print("\n" + "=" * 80)
        print(f"📊 FULL NATIVE RESULTS - {symbol} {timeframe}")
        print("=" * 80)
        print(stats)
        print("=" * 80)

        return {
            'symbol': symbol,
            'timeframe': timeframe,
            'provider': provider,
            'return_pct': stats['Return [%]'],
            'sharpe': stats['Sharpe Ratio'],
            'max_dd': stats['Max. Drawdown [%]'],
            'win_rate': stats['Win Rate [%]'],
            'num_trades': stats['# Trades'],
            'exposure': stats['Exposure Time [%]']
        }

    except Exception as e:
        print(f"   ❌ Error testing {symbol}: {e}")
        return None


def main():
    """Main testing function"""

    print("🌙 Market Structure & Supply/Demand Strategy - Multi-Asset Testing 🌙")
    print("=" * 100)

    data_dir = Path('/Users/bobbyyo/Projects/algo-fun/data')

    # Define test assets
    test_assets = [
        # Bitcoin
        {'file': data_dir / 'coinbase' / 'BTCUSD-1d-1000wks-enhanced-data.csv', 'symbol': 'BTCUSD', 'timeframe': '1d', 'provider': 'Coinbase'},
        {'file': data_dir / 'coinbase' / 'BTCUSD-6h-200wks-enhanced-data.csv', 'symbol': 'BTCUSD', 'timeframe': '6h', 'provider': 'Coinbase'},
        {'file': data_dir / 'yahoo' / 'BTCUSD-20yr-yahoo-data.csv', 'symbol': 'BTCUSD', 'timeframe': '1d', 'provider': 'Yahoo'},

        # Ethereum
        {'file': data_dir / 'coinbase' / 'ETHUSD-1d-1000wks-enhanced-data.csv', 'symbol': 'ETHUSD', 'timeframe': '1d', 'provider': 'Coinbase'},
        {'file': data_dir / 'coinbase' / 'ETHUSD-6h-200wks-enhanced-data.csv', 'symbol': 'ETHUSD', 'timeframe': '6h', 'provider': 'Coinbase'},
        {'file': data_dir / 'yahoo' / 'ETHUSD-20yr-yahoo-data.csv', 'symbol': 'ETHUSD', 'timeframe': '1d', 'provider': 'Yahoo'},

        # XRP
        {'file': data_dir / 'coinbase' / 'XRPUSD-1d-500wks-enhanced-data.csv', 'symbol': 'XRPUSD', 'timeframe': '1d', 'provider': 'Coinbase'},
        {'file': data_dir / 'yahoo' / 'XRPUSD-10yr-yahoo-data.csv', 'symbol': 'XRPUSD', 'timeframe': '1d', 'provider': 'Yahoo'},

        # CRO
        {'file': data_dir / 'coinbase' / 'CROUSD-1d-1000wks-enhanced-data.csv', 'symbol': 'CROUSD', 'timeframe': '1d', 'provider': 'Coinbase'},
        {'file': data_dir / 'yahoo' / 'CROUSD-20yr-yahoo-data.csv', 'symbol': 'CROUSD', 'timeframe': '1d', 'provider': 'Yahoo'},

        # HBAR
        {'file': data_dir / 'coinbase' / 'HBARUSD-1d-1000wks-enhanced-data.csv', 'symbol': 'HBARUSD', 'timeframe': '1d', 'provider': 'Coinbase'},
        {'file': data_dir / 'yahoo' / 'HBARUSD-20yr-yahoo-data.csv', 'symbol': 'HBARUSD', 'timeframe': '1d', 'provider': 'Yahoo'},

        # LINK
        {'file': data_dir / 'coinbase' / 'LINKUSD-1d-1000wks-enhanced-data.csv', 'symbol': 'LINKUSD', 'timeframe': '1d', 'provider': 'Coinbase'},
        {'file': data_dir / 'yahoo' / 'LINKUSD-20yr-yahoo-data.csv', 'symbol': 'LINKUSD', 'timeframe': '1d', 'provider': 'Yahoo'},
    ]

    # Test each asset
    results = []
    for asset in test_assets:
        if asset['file'].exists():
            result = run_single_test(asset['file'], asset['symbol'], asset['timeframe'], asset['provider'])
            if result:
                results.append(result)
        else:
            print(f"⚠️ File not found: {asset['file']}")

    # Generate summary report
    print("\n" + "=" * 100)
    print("🎯 COMPREHENSIVE PERFORMANCE SUMMARY")
    print("=" * 100)

    if results:
        # Sort by Sharpe ratio
        results_sorted = sorted(results, key=lambda x: x['sharpe'] if pd.notna(x['sharpe']) else -999, reverse=True)

        print("\n🏆 TOP PERFORMERS (by Sharpe Ratio)")
        print("=" * 80)

        for i, result in enumerate(results_sorted[:5], 1):
            print(f"\n{i}. {result['symbol']} - {result['timeframe']} ({result['provider']})")
            print(f"   📈 Return: {result['return_pct']:.2f}%")
            print(f"   📊 Sharpe: {result['sharpe']:.3f}" if pd.notna(result['sharpe']) else "   📊 Sharpe: N/A")
            print(f"   📉 Max DD: {result['max_dd']:.2f}%")
            print(f"   💰 Win Rate: {result['win_rate']:.1f}%" if pd.notna(result['win_rate']) else "   💰 Win Rate: N/A")
            print(f"   🎯 Trades: {result['num_trades']}")

        # Asset rankings
        print("\n🎯 ASSET SUITABILITY RANKING")
        print("=" * 80)

        asset_performance = {}
        for result in results:
            symbol = result['symbol']
            if symbol not in asset_performance:
                asset_performance[symbol] = []
            asset_performance[symbol].append(result)

        asset_scores = {}
        for symbol, asset_results in asset_performance.items():
            valid_sharpes = [r['sharpe'] for r in asset_results if pd.notna(r['sharpe'])]
            valid_returns = [r['return_pct'] for r in asset_results if pd.notna(r['return_pct'])]

            if valid_sharpes and valid_returns:
                avg_sharpe = np.mean(valid_sharpes)
                avg_return = np.mean(valid_returns)
                score = (avg_sharpe * 0.6) + (avg_return * 0.4)
                asset_scores[symbol] = {
                    'score': score,
                    'avg_sharpe': avg_sharpe,
                    'avg_return': avg_return,
                    'tests': len(asset_results)
                }

        sorted_assets = sorted(asset_scores.items(), key=lambda x: x[1]['score'], reverse=True)

        for i, (symbol, metrics) in enumerate(sorted_assets, 1):
            print(f"\n{i}. {symbol}")
            print(f"   🎯 Suitability Score: {metrics['score']:.2f}")
            print(f"   📊 Avg Sharpe: {metrics['avg_sharpe']:.3f}")
            print(f"   📈 Avg Return: {metrics['avg_return']:.2f}%")
            print(f"   📋 Tests Run: {metrics['tests']}")

        # Strategy recommendations
        print("\n🔧 OPTIMIZATION RECOMMENDATIONS")
        print("=" * 80)
        print("\n1. PARAMETER ADJUSTMENTS:")
        print("   - Consider lowering min_rr_ratio to 1.5 for more trading opportunities")
        print("   - Test swing_lookback values [3, 7, 10] for different market conditions")
        print("   - Reduce zone_strength_threshold to 50 in trending markets")

        print("\n2. TIMEFRAME OPTIMIZATION:")
        print("   - 1d timeframe shows most consistent results")
        print("   - Consider combining 6h and 1d for multi-timeframe confirmation")
        print("   - Add 4h timeframe for more granular entries")

        print("\n3. ASSET SELECTION:")
        print("   - Focus on top 3 performing assets for live trading")
        print("   - Consider portfolio approach with uncorrelated assets")
        print("   - Monitor liquidity and spread for execution quality")

        print("\n4. RISK MANAGEMENT:")
        print("   - Implement dynamic position sizing based on volatility")
        print("   - Add correlation-based portfolio heat mapping")
        print("   - Consider max daily loss limits")

        print("\n5. NEXT STEPS:")
        print("   - Run parameter optimization on top performers")
        print("   - Implement walk-forward analysis for robustness")
        print("   - Test with transaction costs and slippage models")
        print("   - Forward test on paper trading account")

    print("\n" + "=" * 100)
    print("✅ COMPREHENSIVE TESTING COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    main()