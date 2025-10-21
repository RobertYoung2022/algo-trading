"""
🚀 MA Crossover Position Sizing Test - December 2024
===================================================
Take the EXACT working strategy and only change position sizing.
Based on simple_ma_fixed_1224.py but with size parameter in buy().
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import warnings
warnings.filterwarnings('ignore')

class MAWith5PercentSizing(Strategy):
    """🎯 MA Crossover with 5% position sizing"""

    fast_period = 10
    slow_period = 30

    def init(self):
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)

    def next(self):
        if len(self.data) < self.slow_period:
            return

        # Calculate 5% of current equity as position size
        current_price = self.data.Close[-1]
        equity = self.broker.equity
        position_value = equity * 0.05  # 5% of total equity
        shares = position_value / current_price

        if crossover(self.sma_fast, self.sma_slow):
            if not self.position:
                self.buy(size=shares)
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()

class MAWith10PercentSizing(Strategy):
    """🎯 MA Crossover with 10% position sizing"""

    fast_period = 10
    slow_period = 30

    def init(self):
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)

    def next(self):
        if len(self.data) < self.slow_period:
            return

        current_price = self.data.Close[-1]
        equity = self.broker.equity
        position_value = equity * 0.10  # 10% of total equity
        shares = position_value / current_price

        if crossover(self.sma_fast, self.sma_slow):
            if not self.position:
                self.buy(size=shares)
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()

class MAOriginal(Strategy):
    """🎯 Original MA strategy (full capital)"""

    fast_period = 10
    slow_period = 30

    def init(self):
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)

    def next(self):
        if len(self.data) < self.slow_period:
            return

        if crossover(self.sma_fast, self.sma_slow):
            if not self.position:
                self.buy()  # No size = full available capital
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()


def load_data(file_path):
    """📊 Load data"""
    try:
        df = pd.read_csv(file_path)

        # Handle datetime columns
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
        print(f"❌ Error loading data: {e}")
        return None


def test_strategy(data, strategy_class, name):
    """🧪 Test strategy and return results"""
    try:
        bt = Backtest(data, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()

        return_pct = float(stats['Return [%]'])
        sharpe = float(stats['Sharpe Ratio']) if not pd.isna(stats['Sharpe Ratio']) else 0
        max_dd = float(stats['Max. Drawdown [%]'])
        trades = int(stats['# Trades'])
        win_rate = float(stats['Win Rate [%]']) if trades > 0 else 0

        return {
            'name': name,
            'return': return_pct,
            'sharpe': sharpe,
            'max_dd': max_dd,
            'trades': trades,
            'win_rate': win_rate,
            'stats': stats
        }
    except Exception as e:
        print(f"❌ Error testing {name}: {e}")
        return None


def main():
    """🚀 Position Sizing Comparison"""
    print("🚀 MA CROSSOVER POSITION SIZING IMPACT")
    print("="*50)

    # Load daily BTC data (the one that worked before)
    file_path = "/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv"

    print("📊 Loading daily BTC data...")
    data = load_data(file_path)

    if data is None:
        print("❌ Failed to load data")
        return

    print(f"✅ Loaded {len(data)} bars")

    # Test different position sizing approaches
    strategies = [
        (MAOriginal, "Original (Full Capital)"),
        (MAWith10PercentSizing, "10% Position Sizing"),
        (MAWith5PercentSizing, "5% Position Sizing")
    ]

    results = []

    for strategy_class, name in strategies:
        print(f"\n🧪 Testing {name}...")

        result = test_strategy(data, strategy_class, name)
        if result:
            results.append(result)

            print(f"📊 Results:")
            print(f"   Return: {result['return']:.1f}%")
            print(f"   Sharpe: {result['sharpe']:.2f}")
            print(f"   Max DD: {result['max_dd']:.1f}%")
            print(f"   Trades: {result['trades']}")
            print(f"   Win Rate: {result['win_rate']:.1f}%")

    # Comparison
    if results:
        print(f"\n🏆 POSITION SIZING COMPARISON")
        print("="*60)
        print(f"{'Strategy':<20} {'Return%':<8} {'Sharpe':<6} {'MaxDD%':<7} {'Trades':<6}")
        print("-"*60)

        for r in results:
            print(f"{r['name']:<20} {r['return']:<8.0f} {r['sharpe']:<6.2f} {r['max_dd']:<7.1f} {r['trades']:<6}")

        print(f"\n💡 KEY INSIGHTS:")
        original = next((r for r in results if 'Original' in r['name']), None)
        sized_5 = next((r for r in results if '5%' in r['name']), None)

        if original and sized_5:
            dd_improvement = abs(sized_5['max_dd']) - abs(original['max_dd'])
            sharpe_change = sized_5['sharpe'] - original['sharpe']

            print(f"   📉 Drawdown change: {dd_improvement:.1f}% ({'better' if dd_improvement > 0 else 'worse'})")
            print(f"   📈 Sharpe change: {sharpe_change:+.2f}")
            print(f"   🎯 5% sizing {'IMPROVES' if sharpe_change > 0 and dd_improvement > 0 else 'needs tweaking'} the strategy")

        print(f"\n🌙💫🚀 Position sizing analysis complete!")

    else:
        print("❌ No successful tests")


if __name__ == "__main__":
    main()