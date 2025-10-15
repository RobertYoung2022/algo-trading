"""
🔍 DEBUG: BTC Trade Execution Issue
===================================
Investigate why BTC backtest shows position sizing but reports 0 trades

🎯 What we're checking:
1. Data loading (confirm proper format)
2. RSI calculation (confirm oversold signals)
3. Trade execution (confirm buy() calls)
4. Trade recording (confirm backtest framework records trades)
"""

import sys
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/core_strategies')

from backtesting import Backtest, Strategy
from rsi_mean_reversion_strategy import RSIMeanReversionStrategy

def load_data(file_path):
    """📊 Load and prepare data"""

    with open(file_path, 'r') as f:
        first_line = f.readline()
        has_url_header = 'http' in first_line.lower()

    if has_url_header:
        df = pd.read_csv(file_path, skiprows=1)
    else:
        df = pd.read_csv(file_path)

    df.columns = df.columns.str.lower().str.strip()

    column_mapping = {
        'datetime': 'timestamp',
        'date': 'timestamp',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }

    df = df.rename(columns=column_mapping)

    if 'Volume' not in df.columns:
        df['Volume'] = 1000.0

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.set_index('timestamp')
    elif not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index, errors='coerce')
        except:
            pass

    result = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
    result = result.dropna()

    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        result[col] = pd.to_numeric(result[col], errors='coerce')

    result = result.dropna().sort_index()

    return result

# Create a debug version of the strategy that prints trade attempts
class RSIMeanReversionDebug(RSIMeanReversionStrategy):
    """Debug version that logs trade attempts"""

    def __init__(self, broker, data, params):
        super().__init__(broker, data, params)
        self.buy_attempts = 0
        self.buy_executed = 0
        self.exit_attempts = 0
        self.position_checks = 0

    def next(self):
        """Override to track buy attempts and position status"""

        # Call parent's safety checks
        if len(self.data) < self.rsi_period + 1:
            return

        # Trade limit enforcement
        current_bar_year = self.data.index[-1].year
        if self.current_year is None:
            self.current_year = current_bar_year
            self.trades_this_year = 0
        elif current_bar_year != self.current_year:
            self.current_year = current_bar_year
            self.trades_this_year = 0

        if self.trades_this_year >= self.max_trades_per_year:
            return

        # Get current values
        current_price = self.data.Close[-1]
        current_rsi = self.rsi[-1]

        if pd.isna(current_rsi):
            return

        # 🔍 DEBUG: Check position status
        self.position_checks += 1
        has_position = bool(self.position)

        # Only print position status occasionally and when it changes
        if self.position_checks <= 5 or (has_position and self.position_checks % 50 == 0):
            print(f"🔍 Position check #{self.position_checks}: has_position={has_position}, RSI={current_rsi:.2f}")

        # Check for buy signal
        if not self.position:
            if current_rsi < self.rsi_oversold:
                self.buy_attempts += 1
                print(f"🎯 BUY ATTEMPT #{self.buy_attempts}: Bar {len(self.data)}, Date {self.data.index[-1]}, RSI={current_rsi:.2f}, Price=${current_price:.2f}")

                # Calculate position size (simplified for debug)
                size_fraction = 0.015

                # Try to buy
                try:
                    self.buy(size=size_fraction)
                    self.entry_bar = len(self.data)
                    self.trades_this_year += 1
                    self.buy_executed += 1
                    print(f"   ✅ Buy order placed: size={size_fraction}")
                except Exception as e:
                    print(f"   ❌ Buy failed: {e}")

        elif self.position and self.position.is_long:
            # Exit logic
            bars_held = len(self.data) - self.entry_bar if self.entry_bar else 0

            # 🔍 DEBUG: Print exit check details
            if bars_held <= 5 or bars_held % 10 == 0:
                print(f"   🔍 Exit check: RSI={current_rsi:.2f} (exit@{self.rsi_neutral_low}), bars_held={bars_held}/{self.max_hold_periods}")

            if current_rsi >= self.rsi_neutral_low:
                self.exit_attempts += 1
                print(f"📤 EXIT #{self.exit_attempts}: RSI neutral ({current_rsi:.2f}), held {bars_held} bars")
                self.position.close()
            elif bars_held >= self.max_hold_periods:
                self.exit_attempts += 1
                print(f"📤 EXIT #{self.exit_attempts}: Max hold period ({bars_held} bars)")
                self.position.close()
            elif current_rsi >= 85:
                self.exit_attempts += 1
                print(f"📤 EXIT #{self.exit_attempts}: Extreme overbought ({current_rsi:.2f})")
                self.position.close()

print("\n" + "="*70)
print("🔍 BTC TRADE EXECUTION DEBUG")
print("="*70)

# Load BTC data
print("\n📊 Step 1: Load BTC data")
df = load_data("/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv")
print(f"   ✅ Loaded {len(df)} bars")
print(f"   Date range: {df.index[0]} to {df.index[-1]}")
print(f"   Columns: {df.columns.tolist()}")
print(f"   First 3 rows:\n{df.head(3)}")

# Calculate RSI manually to confirm oversold signals
print("\n📊 Step 2: Calculate RSI and find oversold signals")
close_series = df['Close']
delta = close_series.diff()
gain = delta.where(delta > 0, 0).rolling(window=14).mean()
loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
rs = gain / loss
rsi = 100 - (100 / (1 + rs))

oversold_count = (rsi < 30).sum()
print(f"   ✅ RSI calculated")
print(f"   Oversold signals (RSI < 30): {oversold_count}")
print(f"   RSI range: {rsi.min():.2f} to {rsi.max():.2f}")

# Show some oversold dates
oversold_dates = df[rsi < 30].index
if len(oversold_dates) > 0:
    print(f"\n   First 5 oversold dates:")
    for i, date in enumerate(oversold_dates[:5]):
        print(f"      {i+1}. {date}: RSI={rsi.loc[date]:.2f}, Close=${close_series.loc[date]:.2f}")

# Run backtest with debug strategy
print("\n📊 Step 3: Run backtest with debug logging")
bt = Backtest(df, RSIMeanReversionDebug, cash=10000, commission=.002)
stats = bt.run()

print("\n📊 Step 4: Check results")
# Access strategy instance correctly
strategy_instance = bt._results
print(f"   Trades reported: {stats['# Trades']}")
print(f"   Return: {stats['Return [%]']:.2f}%")
print(f"   Final equity: ${stats['Equity Final [$]']:.2f}")

# Key insight check
print("\n🔍 KEY INSIGHT:")
if stats['# Trades'] == 0:
    print("   ❌ PROBLEM: 0 trades recorded despite buy attempts!")
    print("   💡 HYPOTHESIS: Positions never exit, so no complete trade cycle")
    print("   🎯 SOLUTION: Check exit logic in strategy")

print("\n" + "="*70)
print("🌙💫🚀 Debug complete!")
