"""
🚀 Optimized MA Crossover - Realistic ICT-Inspired Enhancements - December 2024
==============================================================================
Simplified optimization of the 19,320% strategy with practical improvements:
- 5% position sizing (mandatory)
- ATR-based stop losses
- Volume confirmation (optional)
- Multi-timeframe testing
- Realistic risk management

Goal: Transform -64% drawdown into manageable risk while keeping strong returns
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import warnings
warnings.filterwarnings('ignore')

class OptimizedMACrossover1224(Strategy):
    """🎯 Simplified MA Crossover with Essential Risk Management"""

    # Core parameters
    fast_period = 10
    slow_period = 30
    position_risk_pct = 5.0  # 5% position sizing
    atr_period = 14
    atr_stop_multiplier = 1.5
    max_hold_days = 20

    # Optional filters (can be disabled)
    use_volume_filter = False  # Start simple
    volume_multiplier = 1.2

    def init(self):
        # Core indicators
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)

        # Volume filter
        if self.use_volume_filter:
            self.volume_ma = self.I(lambda x: pd.Series(x).rolling(20).mean(), self.data.Volume)

        # Position tracking
        self.entry_bar = None
        self.entry_price = None

    def calculate_position_size(self, stop_distance):
        """💰 5% Position Sizing - Core Risk Management"""
        try:
            # Risk 5% of account balance
            risk_amount = self.broker.cash * (self.position_risk_pct / 100)

            if stop_distance > 0:
                # Calculate shares based on stop distance
                shares = risk_amount / stop_distance

                # Don't exceed 50% of capital in one trade
                max_shares = (self.broker.cash * 0.5) / self.data.Close[-1]
                position_size = min(shares, max_shares)

                # Minimum position check
                if position_size * self.data.Close[-1] < 100:  # $100 minimum
                    return 0

                return position_size
            return 0
        except:
            return 0

    def volume_confirmed(self):
        """📊 Optional Volume Confirmation"""
        if not self.use_volume_filter:
            return True

        if len(self.data) < 20:
            return True

        return self.data.Volume[-1] > (self.volume_ma[-1] * self.volume_multiplier)

    def next(self):
        # Skip if insufficient data
        if len(self.data) < max(self.slow_period, self.atr_period):
            return

        # Time-based exit (prevent holding too long)
        if self.position and self.entry_bar is not None:
            if len(self.data) - self.entry_bar >= self.max_hold_days:
                self.position.close()
                self.entry_bar = None
                return

        # Long entry signal
        if crossover(self.sma_fast, self.sma_slow) and not self.position:
            # Volume filter (if enabled)
            if self.volume_confirmed():
                # Calculate stop loss
                atr_value = self.atr[-1] if not np.isnan(self.atr[-1]) else self.data.Close[-1] * 0.02
                stop_distance = atr_value * self.atr_stop_multiplier

                # Calculate position size
                position_size = self.calculate_position_size(stop_distance)

                if position_size > 0:
                    stop_loss = self.data.Close[-1] - stop_distance

                    # Enter position
                    self.buy(size=position_size, sl=stop_loss)
                    self.entry_bar = len(self.data)
                    self.entry_price = self.data.Close[-1]

        # Exit signal
        elif crossover(self.sma_slow, self.sma_fast) and self.position:
            self.position.close()
            self.entry_bar = None


# Create different strategy variants for testing
class ConservativeMACrossover(OptimizedMACrossover1224):
    """Conservative version with tighter risk management"""
    position_risk_pct = 3.0
    atr_stop_multiplier = 1.0
    max_hold_days = 15
    use_volume_filter = True

class AggressiveMACrossover(OptimizedMACrossover1224):
    """More aggressive version"""
    position_risk_pct = 7.0
    atr_stop_multiplier = 2.0
    max_hold_days = 30
    use_volume_filter = False

class BalancedMACrossover(OptimizedMACrossover1224):
    """Balanced approach - our main candidate"""
    position_risk_pct = 5.0
    atr_stop_multiplier = 1.5
    max_hold_days = 20
    use_volume_filter = True
    volume_multiplier = 1.3


def load_data(file_path):
    """📊 Simple data loader"""
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

        # Add volume if missing
        if 'Volume' not in df.columns:
            df['Volume'] = 1000

        return df.dropna()

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None


def run_strategy_test(data, strategy_class, strategy_name, timeframe):
    """🧪 Run single strategy test"""
    try:
        print(f"\n{'='*60}")
        print(f"🎯 {strategy_name} - {timeframe}")
        print(f"{'='*60}")

        bt = Backtest(data, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()

        # Key metrics
        return_pct = float(stats['Return [%]'])
        sharpe = float(stats['Sharpe Ratio']) if not pd.isna(stats['Sharpe Ratio']) else 0
        max_dd = float(stats['Max. Drawdown [%]'])
        num_trades = int(stats['# Trades'])
        win_rate = float(stats['Win Rate [%]']) if num_trades > 0 else 0

        print(f"📊 Results:")
        print(f"   Return: {return_pct:.1f}%")
        print(f"   Sharpe: {sharpe:.2f}")
        print(f"   Max DD: {max_dd:.1f}%")
        print(f"   Trades: {num_trades}")
        print(f"   Win Rate: {win_rate:.1f}%")

        # Assessment
        meets_criteria = (
            return_pct >= 20 and
            sharpe >= 0.8 and
            max_dd >= -25 and
            num_trades >= 3
        )

        print(f"   Status: {'✅ GOOD' if meets_criteria else '⚠️ NEEDS WORK'}")

        return {
            'strategy': strategy_name,
            'timeframe': timeframe,
            'return': return_pct,
            'sharpe': sharpe,
            'max_dd': max_dd,
            'trades': num_trades,
            'win_rate': win_rate,
            'meets_criteria': meets_criteria,
            'stats': stats
        }

    except Exception as e:
        print(f"❌ Error testing {strategy_name}: {e}")
        return None


def main():
    """🚀 Comprehensive Strategy Optimization"""
    print("🚀 OPTIMIZED MA CROSSOVER - REALISTIC ENHANCEMENTS")
    print("="*70)

    # Data sources
    datasets = [
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1h-500wks-data.csv", "1H BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-6h-200wks-enhanced-data.csv", "6H BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv", "DAILY BTC")
    ]

    # Strategy variants
    strategies = [
        (ConservativeMACrossover, "Conservative (3% risk)"),
        (BalancedMACrossover, "Balanced (5% risk)"),
        (AggressiveMACrossover, "Aggressive (7% risk)"),
        (OptimizedMACrossover1224, "Basic Optimized")
    ]

    all_results = []

    # Test each combination
    for file_path, timeframe in datasets:
        print(f"\n📊 Loading {timeframe} data...")
        data = load_data(file_path)

        if data is None:
            continue

        print(f"✅ Loaded {len(data)} bars")

        for strategy_class, strategy_name in strategies:
            result = run_strategy_test(data, strategy_class, strategy_name, timeframe)
            if result:
                all_results.append(result)

    # Analysis
    if all_results:
        print(f"\n🏆 COMPREHENSIVE RESULTS COMPARISON")
        print("="*100)
        print(f"{'Strategy':<20} {'Timeframe':<10} {'Return%':<8} {'Sharpe':<6} {'MaxDD%':<7} {'Trades':<6} {'Status'}")
        print("-"*100)

        for r in all_results:
            status = "✅" if r['meets_criteria'] else "⚠️"
            print(f"{r['strategy']:<20} {r['timeframe']:<10} {r['return']:<8.0f} {r['sharpe']:<6.2f} "
                  f"{r['max_dd']:<7.1f} {r['trades']:<6} {status}")

        # Find best configurations
        good_results = [r for r in all_results if r['meets_criteria']]

        if good_results:
            best = max(good_results, key=lambda x: x['sharpe'])
            print(f"\n🥇 BEST CONFIGURATION:")
            print(f"   Strategy: {best['strategy']}")
            print(f"   Timeframe: {best['timeframe']}")
            print(f"   Return: {best['return']:.1f}%")
            print(f"   Sharpe: {best['sharpe']:.2f}")
            print(f"   Max DD: {best['max_dd']:.1f}%")
        else:
            # Find best compromise
            best = max(all_results, key=lambda x: x['sharpe'] if x['trades'] > 0 else -999)
            print(f"\n🔧 BEST COMPROMISE (needs further optimization):")
            print(f"   Strategy: {best['strategy']}")
            print(f"   Timeframe: {best['timeframe']}")
            print(f"   Return: {best['return']:.1f}%")
            print(f"   Sharpe: {best['sharpe']:.2f}")
            print(f"   Max DD: {best['max_dd']:.1f}%")

        print(f"\n🌙💫🚀 Optimization testing complete!")

    else:
        print("❌ No successful tests completed")


if __name__ == "__main__":
    main()