"""
🚀 Multi-Timeframe MA Crossover with ICT Enhancements - December 2024
====================================================================
Enhanced version of Simple MA Crossover with:
- Multiple timeframe testing (1h, 6h, daily)
- ICT market structure concepts (no session restrictions)
- 5% position sizing
- Fair Value Gap detection
- Market structure break confirmation
- Proper risk management

🎯 Goal: Achieve Sharpe ≥0.8, Max DD ≤-25%, Returns 100-500%
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import warnings
warnings.filterwarnings('ignore')

class ICTEnhancedMACrossover1224(Strategy):
    """🎯 MA Crossover with ICT Market Structure Enhancement"""

    # Strategy parameters
    fast_period = 10
    slow_period = 30
    position_risk_pct = 5.0  # 5% position sizing
    atr_period = 14
    atr_stop_multiplier = 1.5
    max_hold_days = 20

    def init(self):
        # 🚀 Core indicators
        self.sma_fast = self.I(talib.SMA, self.data.Close, self.fast_period)
        self.sma_slow = self.I(talib.SMA, self.data.Close, self.slow_period)
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)

        # 🎯 ICT enhancements
        self.recent_high = self.I(lambda x: pd.Series(x).rolling(20).max(), self.data.High)
        self.recent_low = self.I(lambda x: pd.Series(x).rolling(20).min(), self.data.Low)

        # 📊 Volume confirmation
        self.volume_ma = self.I(lambda x: pd.Series(x).rolling(20).mean(), self.data.Volume)

        # 🏗️ Market structure tracking
        self.entry_bar = None

    def detect_fair_value_gap(self):
        """🔍 ICT Fair Value Gap Detection"""
        if len(self.data) < 3:
            return False

        # Look for gap between current and 2 bars ago
        current_low = self.data.Low[-1]
        prev_high = self.data.High[-3]

        # Bullish FVG: Current low > previous high (gap up)
        return current_low > prev_high

    def structure_break_confirmed(self, direction):
        """🏗️ ICT Market Structure Break Confirmation"""
        if len(self.data) < 21:
            return False

        if direction == 'bullish':
            # Bullish break: Price above recent high
            return self.data.Close[-1] > self.recent_high[-2]
        else:
            # Bearish break: Price below recent low
            return self.data.Close[-1] < self.recent_low[-2]

    def volume_confirmation(self):
        """📊 Volume Confirmation Filter"""
        if len(self.data) < 20:
            return True  # Default to true if insufficient data

        # Volume should be 1.5x average
        return self.data.Volume[-1] > (self.volume_ma[-1] * 1.5)

    def calculate_position_size(self, stop_distance):
        """💰 5% Position Sizing with ATR-based Stop"""
        try:
            risk_amount = self.broker.cash * (self.position_risk_pct / 100)
            if stop_distance > 0:
                shares = risk_amount / stop_distance
                # Cap at reasonable maximum
                max_shares = self.broker.cash * 0.95 / self.data.Close[-1]
                return min(shares, max_shares)
            return 0
        except:
            return 0

    def next(self):
        # 🛡️ Skip if insufficient data
        if len(self.data) < max(self.slow_period, 21):
            return

        # 🔄 Position management - Time-based exit
        if self.position and self.entry_bar is not None:
            if len(self.data) - self.entry_bar >= self.max_hold_days:
                self.position.close()
                return

        # 🎯 Long entry logic with ICT enhancements
        if crossover(self.sma_fast, self.sma_slow) and not self.position:
            # ICT filters
            structure_ok = self.structure_break_confirmed('bullish')
            volume_ok = self.volume_confirmation()
            fvg_present = self.detect_fair_value_gap()

            # Enhanced entry conditions
            if structure_ok and volume_ok:
                # Calculate stop loss and position size
                atr_value = self.atr[-1] if not np.isnan(self.atr[-1]) else self.data.Close[-1] * 0.02
                stop_distance = atr_value * self.atr_stop_multiplier
                position_size = self.calculate_position_size(stop_distance)

                if position_size > 0:
                    # Calculate stop loss level
                    stop_loss = self.data.Close[-1] - stop_distance

                    # Enter position
                    self.buy(size=position_size, sl=stop_loss)
                    self.entry_bar = len(self.data)

        # 🎯 Short entry logic (for completeness)
        elif crossover(self.sma_slow, self.sma_fast) and not self.position:
            structure_ok = self.structure_break_confirmed('bearish')
            volume_ok = self.volume_confirmation()

            if structure_ok and volume_ok:
                atr_value = self.atr[-1] if not np.isnan(self.atr[-1]) else self.data.Close[-1] * 0.02
                stop_distance = atr_value * self.atr_stop_multiplier
                position_size = self.calculate_position_size(stop_distance)

                if position_size > 0:
                    stop_loss = self.data.Close[-1] + stop_distance
                    self.sell(size=position_size, sl=stop_loss)
                    self.entry_bar = len(self.data)


def load_and_prepare_data(file_path):
    """📊 Load and prepare data for backtesting"""
    try:
        df = pd.read_csv(file_path)

        # Handle different datetime column names
        datetime_cols = ['datetime', 'Datetime', 'Date', 'date', 'Time', 'timestamp']
        for col in datetime_cols:
            if col in df.columns:
                df.set_index(col, inplace=True)
                break

        # Convert index to datetime
        df.index = pd.to_datetime(df.index)

        # Standardize column names
        df.columns = [col.capitalize() for col in df.columns]

        # Ensure required columns exist
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_cols):
            print(f"⚠️ Missing columns. Available: {df.columns.tolist()}")
            return None

        # Add default volume if missing
        if 'Volume' not in df.columns:
            df['Volume'] = 1000

        # Clean data
        df = df.dropna()

        return df

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None


def run_enhanced_backtest(data, timeframe_name, strategy_class=ICTEnhancedMACrossover1224):
    """🧪 Run backtest with comprehensive analysis"""
    try:
        print(f"\n{'='*80}")
        print(f"🎯 ICT-ENHANCED MA CROSSOVER - {timeframe_name}")
        print(f"{'='*80}")
        print(f"📊 Data: {len(data)} bars from {data.index[0]} to {data.index[-1]}")

        # Run backtest
        bt = Backtest(
            data,
            strategy_class,
            cash=10000,
            commission=0.002,
            exclusive_orders=True
        )

        stats = bt.run()

        # Display full results
        print("\n📈 COMPLETE BACKTESTING RESULTS:")
        print("-" * 80)
        print(stats)
        print("-" * 80)

        # Extract key metrics
        return_pct = float(stats['Return [%]'])
        sharpe = float(stats['Sharpe Ratio']) if not pd.isna(stats['Sharpe Ratio']) else 0
        max_dd = float(stats['Max. Drawdown [%]'])
        num_trades = int(stats['# Trades'])
        win_rate = float(stats['Win Rate [%]']) if '# Trades' in stats and stats['# Trades'] > 0 else 0

        # Performance assessment
        print(f"\n🎯 PERFORMANCE SUMMARY - {timeframe_name}:")
        print(f"   Return: {return_pct:.1f}%")
        print(f"   Sharpe: {sharpe:.2f}")
        print(f"   Max DD: {max_dd:.1f}%")
        print(f"   Trades: {num_trades}")
        print(f"   Win Rate: {win_rate:.1f}%")

        # Deployment criteria check
        meets_criteria = (
            return_pct >= 20 and
            sharpe >= 0.8 and
            max_dd >= -25 and
            num_trades >= 5
        )

        status = "✅ MEETS DEPLOYMENT CRITERIA" if meets_criteria else "⚠️ Needs optimization"
        print(f"   Status: {status}")

        return {
            'timeframe': timeframe_name,
            'return_pct': return_pct,
            'sharpe': sharpe,
            'max_dd': max_dd,
            'num_trades': num_trades,
            'win_rate': win_rate,
            'meets_criteria': meets_criteria,
            'stats': stats,
            'backtest': bt
        }

    except Exception as e:
        print(f"❌ Backtest error for {timeframe_name}: {e}")
        return None


def main():
    """🚀 Multi-Timeframe Strategy Testing"""
    print("🚀 ICT-ENHANCED MA CROSSOVER - MULTI-TIMEFRAME ANALYSIS")
    print("="*80)

    # Test configurations: (file_path, timeframe_name)
    test_configs = [
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1h-500wks-data.csv", "1-HOUR BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-6h-200wks-enhanced-data.csv", "6-HOUR BTC"),
        ("/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/BTCUSD-1d-1000wks-enhanced-data.csv", "DAILY BTC")
    ]

    results = []

    for file_path, timeframe_name in test_configs:
        print(f"\n🔍 Testing {timeframe_name}...")

        # Load data
        data = load_and_prepare_data(file_path)
        if data is None:
            print(f"❌ Failed to load {timeframe_name} data")
            continue

        # Run backtest
        result = run_enhanced_backtest(data, timeframe_name)
        if result:
            results.append(result)

    # Compare results
    if results:
        print(f"\n🏆 MULTI-TIMEFRAME COMPARISON")
        print("="*80)
        print(f"{'Timeframe':<12} {'Return%':<10} {'Sharpe':<8} {'MaxDD%':<8} {'Trades':<8} {'Status'}")
        print("-"*80)

        for r in results:
            status = "✅ GOOD" if r['meets_criteria'] else "⚠️ NEEDS WORK"
            print(f"{r['timeframe']:<12} {r['return_pct']:<10.1f} {r['sharpe']:<8.2f} "
                  f"{r['max_dd']:<8.1f} {r['num_trades']:<8} {status}")

        # Find best performer
        best_result = max(results, key=lambda x: x['sharpe'] if x['sharpe'] > 0 else -999)
        print(f"\n🥇 BEST PERFORMER: {best_result['timeframe']}")
        print(f"   Sharpe: {best_result['sharpe']:.2f}")
        print(f"   Return: {best_result['return_pct']:.1f}%")
        print(f"   Max DD: {best_result['max_dd']:.1f}%")

        # Recommendations
        print(f"\n💡 OPTIMIZATION RECOMMENDATIONS:")
        if best_result['sharpe'] < 0.8:
            print("   • Adjust MA periods for better signal quality")
            print("   • Tighten stop losses to reduce drawdown")
            print("   • Add more ICT filters for entry precision")
        if best_result['max_dd'] < -25:
            print("   • Implement tighter risk management")
            print("   • Consider smaller position sizes")
            print("   • Add profit-taking levels")

        print(f"\n🌙💫🚀 Multi-timeframe analysis complete!")

    else:
        print("❌ No successful backtests completed")


if __name__ == "__main__":
    main()