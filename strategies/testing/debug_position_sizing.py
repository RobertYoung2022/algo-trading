"""
🔍 DEBUG: Position Sizing Test
==============================
Test different position sizing approaches in backtesting.py

🎯 Hypothesis: size=0.015 is too small, causing framework to reject orders
"""

import sys
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/core_strategies')

from backtesting import Backtest, Strategy

# Simple test strategy to isolate position sizing issue
class SimpleRSITest(Strategy):
    """Minimal strategy to test position sizing"""

    def init(self):
        # Calculate RSI manually
        def rsi_calc(close, period=14):
            delta = pd.Series(close).diff()
            gain = delta.where(delta > 0, 0).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            return 100 - (100 / (1 + rs))

        self.rsi = self.I(rsi_calc, self.data.Close, 14)
        self.entry_count = 0
        print(f"✅ Strategy initialized with ${self.equity:,.2f} equity")

    def next(self):
        if len(self.data) < 15:
            return

        current_rsi = self.rsi[-1]
        if pd.isna(current_rsi):
            return

        # Try to enter on first oversold signal
        if not self.position and current_rsi < 30 and self.entry_count == 0:
            self.entry_count += 1
            current_price = self.data.Close[-1]

            print(f"\n🎯 ENTRY SIGNAL #{self.entry_count}")
            print(f"   Date: {self.data.index[-1]}")
            print(f"   RSI: {current_rsi:.2f}")
            print(f"   Price: ${current_price:.2f}")
            print(f"   Equity: ${self.equity:.2f}")

            # Test different position sizing approaches
            print(f"\n📊 TESTING POSITION SIZING:")

            # Approach 1: Larger fraction (0.95 = 95%)
            print(f"   Approach 1: size=0.95 (95% = ${self.equity * 0.95:.2f})")
            try:
                self.buy(size=0.95)
                print(f"      ✅ Order placed successfully")
            except Exception as e:
                print(f"      ❌ Order failed: {e}")

        # Exit when RSI returns to neutral (40+)
        elif self.position and current_rsi >= 40:
            print(f"\n📤 EXIT SIGNAL: RSI={current_rsi:.2f} (≥40)")
            self.position.close()

print("\n" + "="*70)
print("🔍 POSITION SIZING DEBUG TEST")
print("="*70)

# Load BTC data
df = pd.read_csv("/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv")
df.columns = df.columns.str.lower().str.strip()
df = df.rename(columns={'datetime': 'timestamp', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
df = df.set_index('timestamp')
df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df = df.dropna().sort_index()

print(f"📊 Loaded: {len(df)} bars of BTC data")
print(f"   Date range: {df.index[0]} to {df.index[-1]}")
print(f"   First price: ${df['Close'].iloc[0]:.2f}")

# Run backtest
bt = Backtest(df, SimpleRSITest, cash=10000, commission=.002)
stats = bt.run()

print(f"\n📊 RESULTS:")
print(f"   Trades: {stats['# Trades']}")
print(f"   Return: {stats['Return [%]']:.2f}%")
print(f"   Final Equity: ${stats['Equity Final [$]']:.2f}")

if stats['# Trades'] == 0:
    print(f"\n❌ CONFIRMED: Position sizing issue - no trades executed!")
    print(f"   🔍 Likely cause: size=0.015 too small for backtesting.py framework")
    print(f"   💡 Solution: Use larger position size or different sizing method")
else:
    print(f"\n✅ SUCCESS: {stats['# Trades']} trades executed!")

print("\n🌙💫🚀 Position sizing test complete!")
