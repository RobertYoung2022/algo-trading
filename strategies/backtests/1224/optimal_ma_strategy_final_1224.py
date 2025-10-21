"""
🚀 Optimal MA Strategy - Final Configuration - December 2024
==========================================================
Based on testing results:
- 20% position sizing offers best balance (44% return, -8% DD vs original -64% DD)
- Test this across multiple timeframes to find the ultimate configuration
- Add minimal enhancements that don't break the core logic
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import warnings
warnings.filterwarnings('ignore')

class OptimalMACrossover1224(Strategy):
    """🎯 Optimal MA Crossover Configuration"""

    fast_period = 10
    slow_period = 30
    position_size_pct = 20  # Sweet spot from testing

    def init(self):
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)

    def next(self):
        if len(self.data) < self.slow_period:
            return

        if crossover(self.sma_fast, self.sma_slow):
            if not self.position:
                # Use 20% position sizing (best balance from testing)
                self.buy(size=self.position_size_pct / 100)

        elif crossover(self.sma_slow, self.sma_fast):
            if self.position:
                self.position.close()


class ConservativeMA1224(OptimalMACrossover1224):
    """🛡️ Conservative version (10% sizing)"""
    position_size_pct = 10

class AggressiveMA1224(OptimalMACrossover1224):
    """🚀 Aggressive version (30% sizing)"""
    position_size_pct = 30


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


def run_test(data, strategy_class, name, timeframe):
    """🧪 Run comprehensive test"""
    try:
        bt = Backtest(data, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()

        return_pct = float(stats['Return [%]'])
        sharpe = float(stats['Sharpe Ratio']) if not pd.isna(stats['Sharpe Ratio']) else 0
        max_dd = float(stats['Max. Drawdown [%]'])
        trades = int(stats['# Trades'])
        win_rate = float(stats['Win Rate [%]']) if trades > 0 else 0

        # Calculate deployment score
        # Sharpe ≥ 0.8, Return ≥ 20%, Max DD ≤ -25%
        deployment_score = 0
        if sharpe >= 0.8:
            deployment_score += 3
        elif sharpe >= 0.6:
            deployment_score += 2
        elif sharpe >= 0.4:
            deployment_score += 1

        if return_pct >= 50:
            deployment_score += 3
        elif return_pct >= 20:
            deployment_score += 2
        elif return_pct >= 10:
            deployment_score += 1

        if max_dd >= -15:
            deployment_score += 3
        elif max_dd >= -25:
            deployment_score += 2
        elif max_dd >= -35:
            deployment_score += 1

        is_deployable = deployment_score >= 6  # Minimum score for deployment

        return {
            'name': name,
            'timeframe': timeframe,
            'return': return_pct,
            'sharpe': sharpe,
            'max_dd': max_dd,
            'trades': trades,
            'win_rate': win_rate,
            'score': deployment_score,
            'deployable': is_deployable
        }

    except Exception as e:
        print(f"❌ {name} on {timeframe} failed: {e}")
        return None


def main():
    """🚀 Final Strategy Optimization"""
    print("🚀 FINAL MA CROSSOVER STRATEGY OPTIMIZATION")
    print("="*60)
    print("🎯 Testing optimal configurations across timeframes")

    # Test datasets
    datasets = [
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv", "DAILY BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-6h-200wks-enhanced-data.csv", "6H BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1h-500wks-data.csv", "1H BTC")
    ]

    # Strategy variants
    strategies = [
        (ConservativeMA1224, "Conservative (10%)"),
        (OptimalMACrossover1224, "Optimal (20%)"),
        (AggressiveMA1224, "Aggressive (30%)")
    ]

    all_results = []

    # Test all combinations
    for file_path, timeframe in datasets:
        print(f"\n📊 Testing {timeframe}...")

        data = load_data(file_path)
        if data is None:
            continue

        print(f"✅ Loaded {len(data)} bars")

        for strategy_class, strategy_name in strategies:
            result = run_test(data, strategy_class, strategy_name, timeframe)
            if result:
                all_results.append(result)

                print(f"  {strategy_name}: {result['return']:.0f}% return, "
                      f"{result['sharpe']:.2f} Sharpe, {result['max_dd']:.1f}% DD "
                      f"({'✅ DEPLOYABLE' if result['deployable'] else '⚠️ Needs work'})")

    # Final analysis
    if all_results:
        print(f"\n🏆 COMPREHENSIVE RESULTS")
        print("="*80)
        print(f"{'Strategy':<15} {'Timeframe':<10} {'Return%':<8} {'Sharpe':<6} {'MaxDD%':<7} {'Score':<5} {'Status'}")
        print("-"*80)

        for r in all_results:
            status = "✅ DEPLOY" if r['deployable'] else "⚠️ NEEDS WORK"
            print(f"{r['name']:<15} {r['timeframe']:<10} {r['return']:<8.0f} {r['sharpe']:<6.2f} "
                  f"{r['max_dd']:<7.1f} {r['score']:<5} {status}")

        # Find deployable strategies
        deployable = [r for r in all_results if r['deployable']]

        if deployable:
            print(f"\n🥇 DEPLOYMENT-READY STRATEGIES: {len(deployable)}")

            # Best overall
            best = max(deployable, key=lambda x: x['score'])
            print(f"\n🏆 BEST CONFIGURATION:")
            print(f"   {best['name']} on {best['timeframe']}")
            print(f"   Return: {best['return']:.1f}%")
            print(f"   Sharpe: {best['sharpe']:.2f}")
            print(f"   Max DD: {best['max_dd']:.1f}%")
            print(f"   Score: {best['score']}/9")

            # Conservative option
            conservative = min(deployable, key=lambda x: abs(x['max_dd']))
            if conservative != best:
                print(f"\n🛡️ MOST CONSERVATIVE:")
                print(f"   {conservative['name']} on {conservative['timeframe']}")
                print(f"   Max DD: {conservative['max_dd']:.1f}%")
                print(f"   Return: {conservative['return']:.1f}%")

        else:
            print(f"\n⚠️ NO STRATEGIES MEET DEPLOYMENT CRITERIA")
            print("   Best compromise strategies:")

            # Show best 3 even if not deployable
            sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)
            for i, r in enumerate(sorted_results[:3], 1):
                print(f"   {i}. {r['name']} on {r['timeframe']}: Score {r['score']}/9")

        # Summary recommendations
        print(f"\n💡 OPTIMIZATION INSIGHTS:")

        # Group by timeframe
        timeframes = {}
        for r in all_results:
            tf = r['timeframe']
            if tf not in timeframes:
                timeframes[tf] = []
            timeframes[tf].append(r)

        for tf, results in timeframes.items():
            best_tf = max(results, key=lambda x: x['score'])
            print(f"   {tf}: Best is {best_tf['name']} (Score: {best_tf['score']})")

        print(f"\n🌙💫🚀 Final optimization complete!")

    else:
        print("❌ No successful tests completed")


if __name__ == "__main__":
    main()