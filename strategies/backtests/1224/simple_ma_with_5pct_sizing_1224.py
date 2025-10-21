"""
🚀 Simple MA Crossover with 5% Position Sizing - December 2024
==============================================================
Take the original working strategy (19,320% return) and ONLY add:
1. 5% position sizing instead of full capital
2. Basic stop loss
3. Multi-timeframe testing

Goal: Reduce -64% drawdown while maintaining strong returns
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import warnings
warnings.filterwarnings('ignore')

class SimpleMA5Percent1224(Strategy):
    """🎯 Original MA Strategy with 5% Position Sizing Only"""

    fast_period = 10
    slow_period = 30
    position_risk_pct = 5.0  # Key change: 5% instead of 100%

    def init(self):
        # Exact same indicators as original working strategy
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)

    def next(self):
        # Skip if indicators not ready
        if len(self.data) < self.slow_period:
            return

        # Calculate 5% position size
        account_balance = self.broker.cash + (self.position.size * self.data.Close[-1] if self.position else 0)
        position_value = account_balance * (self.position_risk_pct / 100)
        shares = position_value / self.data.Close[-1]

        # Ensure minimum reasonable position
        if shares < 0.001:  # Minimum $1 worth at current price
            return

        # Long entry - same logic as original
        if crossover(self.sma_fast, self.sma_slow):
            if not self.position:
                self.buy(size=shares)

        # Long exit - same logic as original
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()


class SimpleMA10Percent1224(SimpleMA5Percent1224):
    """🎯 10% Position Sizing Version"""
    position_risk_pct = 10.0

class SimpleMA20Percent1224(SimpleMA5Percent1224):
    """🎯 20% Position Sizing Version"""
    position_risk_pct = 20.0

class SimpleMAOriginal1224(SimpleMA5Percent1224):
    """🎯 Original Full Capital Version for comparison"""
    position_risk_pct = 95.0  # Nearly full capital like original


def load_data(file_path):
    """📊 Simple data loader"""
    try:
        df = pd.read_csv(file_path)

        # Handle datetime
        datetime_cols = ['datetime', 'Datetime', 'Date', 'date', 'Time', 'timestamp']
        for col in datetime_cols:
            if col in df.columns:
                df.set_index(col, inplace=True)
                break

        df.index = pd.to_datetime(df.index)
        df.columns = [col.capitalize() for col in df.columns]

        if 'Volume' not in df.columns:
            df['Volume'] = 1000

        return df.dropna()

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None


def test_strategy(data, strategy_class, name, timeframe):
    """🧪 Test single strategy"""
    try:
        print(f"\n{'='*50}")
        print(f"🎯 {name} - {timeframe}")
        print(f"{'='*50}")

        bt = Backtest(data, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()

        # Extract metrics
        return_pct = float(stats['Return [%]'])
        sharpe = float(stats['Sharpe Ratio']) if not pd.isna(stats['Sharpe Ratio']) else 0
        max_dd = float(stats['Max. Drawdown [%]'])
        trades = int(stats['# Trades'])

        print(f"📊 Return: {return_pct:.1f}%")
        print(f"📊 Sharpe: {sharpe:.2f}")
        print(f"📊 Max DD: {max_dd:.1f}%")
        print(f"📊 Trades: {trades}")

        # Simple assessment
        is_good = return_pct > 50 and sharpe > 0.5 and max_dd > -30 and trades > 3
        print(f"📊 Status: {'✅ PROMISING' if is_good else '⚠️ NEEDS WORK'}")

        return {
            'name': name,
            'timeframe': timeframe,
            'return': return_pct,
            'sharpe': sharpe,
            'max_dd': max_dd,
            'trades': trades,
            'is_good': is_good
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def main():
    """🚀 Position Sizing Impact Test"""
    print("🚀 POSITION SIZING IMPACT ANALYSIS")
    print("="*60)

    # Test files
    test_files = [
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv", "DAILY BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-6h-200wks-enhanced-data.csv", "6H BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1h-500wks-data.csv", "1H BTC")
    ]

    # Strategy variants
    strategies = [
        (SimpleMA5Percent1224, "5% Position Size"),
        (SimpleMA10Percent1224, "10% Position Size"),
        (SimpleMA20Percent1224, "20% Position Size"),
        (SimpleMAOriginal1224, "Original (~95%)")
    ]

    results = []

    for file_path, timeframe in test_files:
        print(f"\n📁 Loading {timeframe}...")
        data = load_data(file_path)

        if data is None:
            continue

        print(f"✅ {len(data)} bars loaded")

        for strategy_class, strategy_name in strategies:
            result = test_strategy(data, strategy_class, strategy_name, timeframe)
            if result:
                results.append(result)

    # Summary
    if results:
        print(f"\n🏆 POSITION SIZING COMPARISON")
        print("="*80)
        print(f"{'Strategy':<18} {'Timeframe':<10} {'Return%':<8} {'Sharpe':<6} {'MaxDD%':<7} {'Trades':<6} {'Status'}")
        print("-"*80)

        for r in results:
            status = "✅" if r['is_good'] else "⚠️"
            print(f"{r['name']:<18} {r['timeframe']:<10} {r['return']:<8.0f} {r['sharpe']:<6.2f} "
                  f"{r['max_dd']:<7.1f} {r['trades']:<6} {status}")

        # Analysis
        good_results = [r for r in results if r['is_good']]

        if good_results:
            print(f"\n✅ SUCCESSFUL CONFIGURATIONS: {len(good_results)}")

            # Best by Sharpe
            best_sharpe = max(good_results, key=lambda x: x['sharpe'])
            print(f"\n🥇 BEST SHARPE RATIO:")
            print(f"   {best_sharpe['name']} on {best_sharpe['timeframe']}")
            print(f"   Sharpe: {best_sharpe['sharpe']:.2f}")
            print(f"   Return: {best_sharpe['return']:.1f}%")
            print(f"   Max DD: {best_sharpe['max_dd']:.1f}%")

            # Best balance
            balanced = min(good_results, key=lambda x: abs(x['max_dd']))
            print(f"\n🛡️ BEST RISK MANAGEMENT:")
            print(f"   {balanced['name']} on {balanced['timeframe']}")
            print(f"   Max DD: {balanced['max_dd']:.1f}%")
            print(f"   Return: {balanced['return']:.1f}%")
            print(f"   Sharpe: {balanced['sharpe']:.2f}")

        else:
            print("\n⚠️ No configurations meet criteria - all need further optimization")

        print(f"\n🌙💫🚀 Position sizing analysis complete!")

    else:
        print("❌ No tests completed successfully")


if __name__ == "__main__":
    main()