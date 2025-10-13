"""
🚀 MA Crossover with Fixed Position Sizing - December 2024
=========================================================
Simple approach: Use fraction parameter in buy() call instead of calculating shares.
This should work with backtesting.py framework.
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import warnings
warnings.filterwarnings('ignore')

class MAWith5Percent(Strategy):
    """🎯 MA Crossover with 5% position sizing"""

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
                # Use fraction of available cash (5%)
                self.buy(size=0.05)  # This might work as a fraction
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()

class MAWith10Percent(Strategy):
    """🎯 MA Crossover with 10% position sizing"""

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
                self.buy(size=0.10)  # 10% of available cash
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()

class MAWith20Percent(Strategy):
    """🎯 MA Crossover with 20% position sizing"""

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
                self.buy(size=0.20)  # 20% of available cash
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
                self.buy()  # Full available capital
        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()


def load_data(file_path):
    """📊 Load data"""
    try:
        df = pd.read_csv(file_path)

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
        print(f"❌ Error: {e}")
        return None


def test_strategy(data, strategy_class, name):
    """🧪 Test strategy"""
    try:
        bt = Backtest(data, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()

        return_pct = float(stats['Return [%]'])
        sharpe = float(stats['Sharpe Ratio']) if not pd.isna(stats['Sharpe Ratio']) else 0
        max_dd = float(stats['Max. Drawdown [%]'])
        trades = int(stats['# Trades'])
        win_rate = float(stats['Win Rate [%]']) if trades > 0 else 0

        print(f"✅ {name}:")
        print(f"   Return: {return_pct:.1f}%")
        print(f"   Sharpe: {sharpe:.2f}")
        print(f"   Max DD: {max_dd:.1f}%")
        print(f"   Trades: {trades}")

        return {
            'name': name,
            'return': return_pct,
            'sharpe': sharpe,
            'max_dd': max_dd,
            'trades': trades
        }

    except Exception as e:
        print(f"❌ {name} failed: {e}")
        return None


def main():
    """🚀 Test position sizing variations"""
    print("🚀 MA CROSSOVER POSITION SIZING OPTIMIZATION")
    print("="*50)

    # Load data
    file_path = "/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv"
    data = load_data(file_path)

    if data is None:
        return

    print(f"📊 Testing on {len(data)} daily BTC bars")

    # Test strategies
    strategies = [
        (MAOriginal, "Original (100%)"),
        (MAWith20Percent, "20% Position Size"),
        (MAWith10Percent, "10% Position Size"),
        (MAWith5Percent, "5% Position Size")
    ]

    results = []

    for strategy_class, name in strategies:
        result = test_strategy(data, strategy_class, name)
        if result:
            results.append(result)

    # Analysis
    if len(results) > 1:
        print(f"\n🏆 POSITION SIZING IMPACT ANALYSIS")
        print("="*60)
        print(f"{'Strategy':<18} {'Return%':<8} {'Sharpe':<6} {'MaxDD%':<7} {'Trades'}")
        print("-"*60)

        for r in results:
            print(f"{r['name']:<18} {r['return']:<8.0f} {r['sharpe']:<6.2f} {r['max_dd']:<7.1f} {r['trades']}")

        # Find best improvements
        original = next((r for r in results if 'Original' in r['name']), None)

        if original:
            print(f"\n📊 OPTIMIZATION RESULTS:")
            print(f"   Original: {original['return']:.0f}% return, {original['sharpe']:.2f} Sharpe, {original['max_dd']:.1f}% DD")

            others = [r for r in results if 'Original' not in r['name']]
            for r in others:
                dd_improvement = abs(r['max_dd']) - abs(original['max_dd'])
                sharpe_improvement = r['sharpe'] - original['sharpe']

                print(f"   {r['name']}: DD {dd_improvement:+.1f}%, Sharpe {sharpe_improvement:+.2f}")

                if dd_improvement > 0 and sharpe_improvement > 0:
                    print(f"      ✅ IMPROVED both risk and risk-adjusted returns!")
                elif dd_improvement > 0:
                    print(f"      🛡️ Better risk management")
                elif sharpe_improvement > 0:
                    print(f"      📈 Better risk-adjusted returns")

        print(f"\n🌙💫🚀 Position sizing optimization complete!")

    else:
        print("❌ Insufficient results for comparison")


if __name__ == "__main__":
    main()