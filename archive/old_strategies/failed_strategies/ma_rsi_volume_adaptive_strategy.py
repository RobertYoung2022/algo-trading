"""
🚀 MA-RSI-Volume Adaptive Strategy 🚀
======================================
Enhanced version with adaptive thresholds and alternative entry logic
for better signal generation in crypto markets.

Alternative Entry Logic:
- PRIMARY SIGNAL: Price > MA + RSI < 40 (trend + oversold)
- CONFIRMATION: Volume > average (any volume above average)
- AGGRESSIVE MODE: 2 of 3 conditions met (more signals)
- CONSERVATIVE MODE: All 3 conditions met (fewer, higher quality signals)

Exit Strategy:
- RSI > 60 (momentum exhaustion)
- Price < MA * 0.98 (trend break with 2% buffer)
- Stop Loss: -2%
- Take Profit: +4%

Author: Bobby's Algo Trading System 🌙💫
Date: 2025-01-18
Version: 2.0.0
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover

print("🚀 MA-RSI-Volume Adaptive Strategy Loading... 💫")

# 🎯 Strategy Parameters - Adaptive for crypto volatility
MA_PERIOD = 20              # Moving Average period
RSI_PERIOD = 14             # RSI period
RSI_BUY_THRESHOLD = 40      # More relaxed RSI threshold
RSI_SELL_THRESHOLD = 60     # Earlier exit on RSI
VOLUME_MULTIPLIER = 1.0     # Any volume above average
TAKE_PROFIT_PERCENT = 4.0   # 4% take profit
STOP_LOSS_PERCENT = 2.0     # 2% stop loss
MODE = 'AGGRESSIVE'         # 'AGGRESSIVE' or 'CONSERVATIVE'


class MARSIVolumeAdaptiveStrategy(Strategy):
    """
    🌙 Adaptive MA-RSI-Volume Strategy 🌙

    More flexible entry conditions for increased signal generation
    while maintaining risk control.
    """

    # Strategy parameters
    ma_period = MA_PERIOD
    rsi_period = RSI_PERIOD
    rsi_buy = RSI_BUY_THRESHOLD
    rsi_sell = RSI_SELL_THRESHOLD
    volume_mult = VOLUME_MULTIPLIER
    take_profit = TAKE_PROFIT_PERCENT / 100
    stop_loss = STOP_LOSS_PERCENT / 100
    mode = MODE  # 'AGGRESSIVE' or 'CONSERVATIVE'

    def init(self):
        """Initialize indicators"""
        print(f"🌙 Initializing Adaptive Strategy in {self.mode} mode...")

        # Indicators
        self.ma = self.I(talib.SMA, self.data.Close, self.ma_period)
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)
        self.volume_ma = self.I(talib.SMA, self.data.Volume, 20)

        # Entry tracking
        self.entry_price = None
        self.trade_count = 0

        print(f"✅ Adaptive strategy initialized")

    def next(self):
        """Trading logic with adaptive entry conditions"""

        # Skip if indicators not ready
        if len(self.data) < max(self.ma_period, self.rsi_period, 20):
            return

        # Skip NaN values
        if pd.isna(self.ma[-1]) or pd.isna(self.rsi[-1]) or pd.isna(self.volume_ma[-1]):
            return

        # Current values
        price = self.data.Close[-1]
        ma = self.ma[-1]
        rsi = self.rsi[-1]
        volume = self.data.Volume[-1]
        vol_ma = self.volume_ma[-1] if self.volume_ma[-1] > 0 else 1

        # Entry conditions
        price_above_ma = price > ma
        rsi_oversold = rsi < self.rsi_buy
        volume_spike = volume > (vol_ma * self.volume_mult)

        # Count met conditions
        conditions_met = sum([price_above_ma, rsi_oversold, volume_spike])

        # ENTRY LOGIC
        if not self.position:
            # Aggressive mode: 2 of 3 conditions
            # Conservative mode: all 3 conditions
            required_conditions = 3 if self.mode == 'CONSERVATIVE' else 2

            if conditions_met >= required_conditions:
                # Prioritize entries with trend confirmation
                if price_above_ma and (rsi_oversold or volume_spike):
                    self.buy(size=0.95)
                    self.entry_price = price
                    self.trade_count += 1

                    if self.trade_count <= 3:
                        condition_str = f"P>{price_above_ma} R<{rsi_oversold} V>{volume_spike}"
                        print(f"   🎯 ENTRY #{self.trade_count}: {condition_str} "
                              f"Price={price:.2f}, RSI={rsi:.1f}")

        # EXIT LOGIC
        elif self.position:
            pnl_pct = (price - self.entry_price) / self.entry_price if self.entry_price else 0

            # Exit conditions
            exit_rsi = rsi > self.rsi_sell
            exit_trend = price < ma * 0.98  # 2% buffer below MA
            exit_tp = pnl_pct >= self.take_profit
            exit_sl = pnl_pct <= -self.stop_loss

            if exit_rsi or exit_trend or exit_tp or exit_sl:
                if exit_sl:
                    reason = f"STOP LOSS"
                elif exit_tp:
                    reason = f"TAKE PROFIT"
                elif exit_rsi:
                    reason = f"RSI EXIT ({rsi:.1f})"
                else:
                    reason = f"TREND BREAK"

                self.position.close()

                if self.trade_count <= 3:
                    print(f"   📈 EXIT #{self.trade_count}: {reason} ({pnl_pct*100:.1f}%)")

                self.entry_price = None


def quick_test_strategy():
    """Quick test on sample data to verify strategy works"""

    # Find a good test file
    import glob
    test_files = [
        '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1h-500wks-data.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv'
    ]

    for test_file in test_files:
        try:
            print(f"\n🧪 Quick test on {test_file.split('/')[-1]}...")

            # Load data
            df = pd.read_csv(test_file)

            # Find date column
            date_col = None
            for col in df.columns:
                if col.lower() in ['date', 'datetime', 'time']:
                    date_col = col
                    break

            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
                df = df.set_index(date_col)

            # Standardize columns
            df.columns = [col.capitalize() for col in df.columns]

            # Quick signal check
            df['MA'] = talib.SMA(df['Close'], 20)
            df['RSI'] = talib.RSI(df['Close'], 14)
            df['Vol_MA'] = talib.SMA(df['Volume'], 20)

            # Check conditions with adaptive thresholds
            df['Price_Above'] = df['Close'] > df['MA']
            df['RSI_Buy'] = df['RSI'] < 40
            df['Volume_Above'] = df['Volume'] > df['Vol_MA']

            # Count potential signals (2 of 3 conditions)
            df['Signal_Score'] = df['Price_Above'].astype(int) + df['RSI_Buy'].astype(int) + df['Volume_Above'].astype(int)
            aggressive_signals = (df['Signal_Score'] >= 2).sum()
            conservative_signals = (df['Signal_Score'] == 3).sum()

            print(f"   📊 Potential Signals:")
            print(f"      - Aggressive mode (2/3): {aggressive_signals} signals")
            print(f"      - Conservative mode (3/3): {conservative_signals} signals")

            if aggressive_signals > 10:  # Run backtest if enough signals
                bt = Backtest(df, MARSIVolumeAdaptiveStrategy, cash=10000, commission=0.002)
                stats = bt.run()

                print(f"   📈 Quick Results:")
                print(f"      - Return: {stats['Return [%]']:.2f}%")
                print(f"      - Sharpe: {stats['Sharpe Ratio']:.3f}")
                print(f"      - Trades: {stats['# Trades']}")
                print(f"      - Win Rate: {stats['Win Rate [%]']:.1f}%")

                return True

        except Exception as e:
            print(f"   ❌ Test failed: {e}")
            continue

    return False


# Run quick test if executed directly
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🧪 RUNNING QUICK STRATEGY TEST")
    print("="*60)

    if quick_test_strategy():
        print("\n✅ Strategy validation successful!")
    else:
        print("\n⚠️ Strategy needs adjustment")

    print("\n🌙💫🚀 Adaptive strategy ready for comprehensive testing!")