# volatility_dual_mode_v2.py
"""
🚀 Bobby's Dual-Mode Volatility Strategy V2 - Enhanced Version
============================================================
Improved version with better SL/TP handling for all timeframes
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy

print("🚀 Bobby's Dual-Mode Volatility Strategy V2 Loading... ⚡")

# Strategy parameters
ATR_PERIOD = 14
ATR_MA_PERIOD = 20
VOLUME_MA_PERIOD = 20

# Breakout mode
BREAKOUT_LOOKBACK = 20
BREAKOUT_TAKE_PROFIT = 4.0  # %
BREAKOUT_STOP_LOSS = 2.0    # %
BREAKOUT_VOLUME_FACTOR = 1.2

# Mean reversion mode
REVERSION_BB_PERIOD = 20
REVERSION_BB_STD = 2.0
REVERSION_RSI_PERIOD = 14
REVERSION_RSI_OVERSOLD = 30
REVERSION_RSI_OVERBOUGHT = 70
REVERSION_TAKE_PROFIT = 2.5  # %
REVERSION_STOP_LOSS = 3.5    # %

class VolatilityDualModeStrategyV2(Strategy):
    """
    Enhanced Dual-Mode Strategy with improved SL/TP handling
    """

    def init(self):
        # ATR for volatility
        atr_values = talib.ATR(
            self.data.High,
            self.data.Low,
            self.data.Close,
            timeperiod=ATR_PERIOD
        )
        self.atr = self.I(lambda x: atr_values, self.data.Close, name='ATR')

        # ATR moving average
        atr_ma_values = talib.SMA(atr_values, timeperiod=ATR_MA_PERIOD)
        self.atr_ma = self.I(lambda x: atr_ma_values, self.data.Close, name='ATR_MA')

        # Volume MA
        volume_ma = talib.SMA(self.data.Volume, timeperiod=VOLUME_MA_PERIOD)
        self.volume_ma = self.I(lambda x: volume_ma, self.data.Volume, name='Volume_MA')

        # Breakout indicators
        high_values = pd.Series(self.data.High).rolling(BREAKOUT_LOOKBACK).max()
        low_values = pd.Series(self.data.Low).rolling(BREAKOUT_LOOKBACK).min()
        self.resistance = self.I(lambda x: high_values, self.data.High, name='Resistance')
        self.support = self.I(lambda x: low_values, self.data.Low, name='Support')

        # Mean reversion indicators
        bb_upper, bb_middle, bb_lower = talib.BBANDS(
            self.data.Close,
            timeperiod=REVERSION_BB_PERIOD,
            nbdevup=REVERSION_BB_STD,
            nbdevdn=REVERSION_BB_STD,
            matype=0
        )
        self.bb_upper = self.I(lambda x: bb_upper, self.data.Close, name='BB_Upper')
        self.bb_middle = self.I(lambda x: bb_middle, self.data.Close, name='BB_Middle')
        self.bb_lower = self.I(lambda x: bb_lower, self.data.Close, name='BB_Lower')

        # RSI
        self.rsi = self.I(talib.RSI, self.data.Close, REVERSION_RSI_PERIOD, name='RSI')

        self.current_mode = 'INITIALIZING'

    def next(self):
        # Ensure enough data
        if len(self.data) < max(ATR_MA_PERIOD + ATR_PERIOD, BREAKOUT_LOOKBACK, REVERSION_BB_PERIOD):
            return

        # Get current values
        current_atr = self.atr[-1]
        current_atr_ma = self.atr_ma[-1]

        if np.isnan(current_atr) or np.isnan(current_atr_ma):
            return

        # Determine volatility mode
        is_high_volatility = current_atr > current_atr_ma
        new_mode = 'BREAKOUT' if is_high_volatility else 'MEAN_REVERSION'

        # Close position on mode switch
        if self.current_mode != 'INITIALIZING' and new_mode != self.current_mode:
            if self.position:
                self.position.close()

        self.current_mode = new_mode

        # Market data
        current_price = self.data.Close[-1]
        current_volume = self.data.Volume[-1]
        current_high = self.data.High[-1]
        current_low = self.data.Low[-1]

        # Entry logic
        if not self.position:

            if self.current_mode == 'BREAKOUT':
                # Breakout entry
                resistance_level = self.resistance[-2] if len(self.resistance) > 1 else current_high
                avg_volume = self.volume_ma[-1]

                # Breakout conditions
                price_breaks_resistance = current_price > resistance_level
                volume_confirmed = current_volume > (avg_volume * BREAKOUT_VOLUME_FACTOR)
                close_near_high = (current_price - current_low) > 0.7 * (current_high - current_low)

                if price_breaks_resistance and volume_confirmed and close_near_high:
                    # Calculate SL and TP ensuring SL < current_price < TP
                    sl_price = current_price * (1 - BREAKOUT_STOP_LOSS/100)
                    tp_price = current_price * (1 + BREAKOUT_TAKE_PROFIT/100)

                    # Ensure valid order
                    if sl_price < current_price < tp_price:
                        self.buy(size=0.8, sl=sl_price, tp=tp_price)

            elif self.current_mode == 'MEAN_REVERSION':
                # Mean reversion entry
                current_rsi = self.rsi[-1]
                bb_lower_band = self.bb_lower[-1]
                bb_middle_band = self.bb_middle[-1]

                if np.isnan(current_rsi) or np.isnan(bb_lower_band):
                    return

                # Mean reversion conditions
                price_at_lower_band = current_price <= bb_lower_band * 1.01
                rsi_oversold = current_rsi < REVERSION_RSI_OVERSOLD
                price_below_middle = current_price < bb_middle_band
                sufficient_volume = current_volume > self.volume_ma[-1] * 0.5

                if price_at_lower_band and rsi_oversold and price_below_middle and sufficient_volume:
                    # Calculate SL and TP
                    sl_price = current_price * (1 - REVERSION_STOP_LOSS/100)
                    tp_price = min(
                        current_price * (1 + REVERSION_TAKE_PROFIT/100),
                        bb_middle_band * 0.99  # Target middle band if closer
                    )

                    # Ensure valid order
                    if sl_price < current_price < tp_price:
                        self.buy(size=0.95, sl=sl_price, tp=tp_price)

        else:
            # Position management - early exit for mean reversion
            if self.current_mode == 'MEAN_REVERSION' and self.position.is_long:
                bb_middle_band = self.bb_middle[-1]
                if current_price >= bb_middle_band * 0.99:
                    self.position.close()


# Test function
if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')

    print("\n" + "="*80)
    print("🧪 TESTING ENHANCED DUAL-MODE VOLATILITY STRATEGY V2")
    print("="*80)

    # Test on daily data
    data_path = '/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv'  # 🛡️ Fixed: Using validated Yahoo data

    try:
        # Load and clean data
        data = pd.read_csv(data_path, parse_dates=['datetime'], index_col='datetime')
        data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data = data.dropna().sort_index()

        # Fix OHLC
        for col in ['Open', 'High', 'Low', 'Close']:
            data[col] = data[col].abs()
        data['High'] = data[['Open', 'High', 'Low', 'Close']].max(axis=1)
        data['Low'] = data[['Open', 'High', 'Low', 'Close']].min(axis=1)

        print(f"📊 Testing on BTC-USD Daily")
        print(f"   Data: {len(data)} bars from {data.index[0]} to {data.index[-1]}")

        # Run backtest
        bt = Backtest(data, VolatilityDualModeStrategyV2, cash=1000000, commission=0.001)
        stats = bt.run()

        print(f"\n📈 RESULTS:")
        print(f"   • Return: {stats['Return [%]']:.2f}%")
        print(f"   • Sharpe: {stats['Sharpe Ratio']:.3f}")
        print(f"   • Max DD: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"   • Trades: {stats['# Trades']}")
        print(f"   • Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"   • Profit Factor: {stats['Profit Factor']:.2f}")

        # Test on 6h data
        print("\n" + "-"*40)
        data_6h_path = '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-6h-500wks-data.csv'
        data_6h = pd.read_csv(data_6h_path, parse_dates=['datetime'], index_col='datetime')
        data_6h.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data_6h = data_6h.dropna().sort_index()

        # Fix OHLC
        for col in ['Open', 'High', 'Low', 'Close']:
            data_6h[col] = data_6h[col].abs()
        data_6h['High'] = data_6h[['Open', 'High', 'Low', 'Close']].max(axis=1)
        data_6h['Low'] = data_6h[['Open', 'High', 'Low', 'Close']].min(axis=1)

        print(f"📊 Testing on BTC-USD 6-Hour")
        print(f"   Data: {len(data_6h)} bars")

        bt_6h = Backtest(data_6h, VolatilityDualModeStrategyV2, cash=1000000, commission=0.001)
        stats_6h = bt_6h.run()

        print(f"\n📈 RESULTS:")
        print(f"   • Return: {stats_6h['Return [%]']:.2f}%")
        print(f"   • Sharpe: {stats_6h['Sharpe Ratio']:.3f}")
        print(f"   • Max DD: {stats_6h['Max. Drawdown [%]']:.2f}%")
        print(f"   • Trades: {stats_6h['# Trades']}")
        print(f"   • Win Rate: {stats_6h['Win Rate [%]']:.2f}%")

        print("\n✅ Strategy V2 testing complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()