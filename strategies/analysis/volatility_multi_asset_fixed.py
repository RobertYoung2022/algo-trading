# volatility_multi_asset_fixed.py
"""
🚀 Bobby's Fixed Multi-Asset Volatility Strategy
=================================================
Enhanced version with proper order validation and multi-asset compatibility
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy

print("🚀 Bobby's Fixed Multi-Asset Volatility Strategy Loading... ⚡")

# Strategy parameters - optimized for multi-asset trading
ATR_PERIOD = 14
ATR_MA_PERIOD = 20
VOLUME_MA_PERIOD = 20

# Breakout mode - adjusted for better performance
BREAKOUT_LOOKBACK = 20
BREAKOUT_TAKE_PROFIT = 5.0  # % - increased for better risk/reward
BREAKOUT_STOP_LOSS = 2.5    # % - reasonable stop loss
BREAKOUT_VOLUME_FACTOR = 1.2  # Standard volume confirmation

# Mean reversion mode - adjusted for multi-asset
REVERSION_BB_PERIOD = 20
REVERSION_BB_STD = 2.0
REVERSION_RSI_PERIOD = 14
REVERSION_RSI_OVERSOLD = 35  # Less strict
REVERSION_RSI_OVERBOUGHT = 65  # Less strict
REVERSION_TAKE_PROFIT = 3.0  # % - better target
REVERSION_STOP_LOSS = 2.0    # % - tighter stop


class VolatilityMultiAssetStrategy(Strategy):
    """
    Fixed Multi-Asset Dual-Mode Strategy with improved order handling
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

        # Volume MA - handle zero/missing volume
        volume_data = self.data.Volume if self.data.Volume.sum() > 0 else pd.Series([1000000] * len(self.data))
        volume_ma = talib.SMA(volume_data, timeperiod=VOLUME_MA_PERIOD)
        self.volume_ma = self.I(lambda x: volume_ma, volume_data, name='Volume_MA')

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
        self.mode_switches = 0
        self.last_entry_price = None

    def next(self):
        # Ensure enough data
        min_bars = max(ATR_MA_PERIOD + ATR_PERIOD, BREAKOUT_LOOKBACK, REVERSION_BB_PERIOD)
        if len(self.data) < min_bars:
            return

        # Get current values
        current_atr = self.atr[-1]
        current_atr_ma = self.atr_ma[-1]

        if np.isnan(current_atr) or np.isnan(current_atr_ma) or current_atr_ma <= 0:
            return

        # Determine volatility mode
        is_high_volatility = current_atr > current_atr_ma
        new_mode = 'BREAKOUT' if is_high_volatility else 'MEAN_REVERSION'

        # Track mode switches but don't close position immediately
        if self.current_mode != 'INITIALIZING' and new_mode != self.current_mode:
            self.mode_switches += 1
            # Only close if we've had multiple switches or poor performance
            if self.position and self.mode_switches > 3:
                self.position.close()
                self.mode_switches = 0

        self.current_mode = new_mode

        # Market data
        current_price = self.data.Close[-1]
        current_volume = self.data.Volume[-1] if self.data.Volume.sum() > 0 else 1000000
        current_high = self.data.High[-1]
        current_low = self.data.Low[-1]

        # Skip if price data is invalid
        if current_price <= 0 or np.isnan(current_price):
            return

        # Entry logic
        if not self.position:

            if self.current_mode == 'BREAKOUT':
                # Breakout entry
                resistance_level = self.resistance[-2] if len(self.resistance) > 1 else current_high
                avg_volume = self.volume_ma[-1] if self.volume_ma[-1] > 0 else 1000000

                # Breakout conditions - more balanced
                price_breaks_resistance = current_price > resistance_level
                volume_confirmed = current_volume > (avg_volume * BREAKOUT_VOLUME_FACTOR)
                close_near_high = (current_price - current_low) > 0.65 * (current_high - current_low)

                # Add momentum filter
                if len(self.data) > 5:
                    momentum = (current_price / self.data.Close[-5] - 1) * 100  # 5-bar momentum
                    positive_momentum = momentum > 0.2  # At least 0.2% positive momentum
                else:
                    positive_momentum = True

                if price_breaks_resistance and volume_confirmed and close_near_high and positive_momentum:
                    # Calculate SL and TP with proper validation
                    sl_price = max(
                        current_price * (1 - BREAKOUT_STOP_LOSS/100),
                        current_low * 0.99  # At least 1% below recent low
                    )
                    tp_price = current_price * (1 + BREAKOUT_TAKE_PROFIT/100)

                    # Ensure valid order with minimum spread
                    if sl_price < current_price and tp_price > current_price:
                        try:
                            self.buy(size=0.95, sl=sl_price, tp=tp_price)
                            self.last_entry_price = current_price
                        except:
                            pass  # Silently skip invalid orders

            elif self.current_mode == 'MEAN_REVERSION':
                # Mean reversion entry
                current_rsi = self.rsi[-1]
                bb_lower_band = self.bb_lower[-1]
                bb_middle_band = self.bb_middle[-1]

                if np.isnan(current_rsi) or np.isnan(bb_lower_band) or bb_lower_band <= 0:
                    return

                # Mean reversion conditions - balanced
                price_at_lower_band = current_price <= bb_lower_band * 1.01  # Within 1% of lower band
                rsi_oversold = current_rsi < REVERSION_RSI_OVERSOLD
                price_below_middle = current_price < bb_middle_band

                # Add volume confirmation - less strict
                volume_spike = current_volume > self.volume_ma[-1] * 0.5  # At least 50% of average

                if price_at_lower_band and rsi_oversold and price_below_middle and volume_spike:
                    # Calculate SL and TP
                    sl_price = max(
                        current_price * (1 - REVERSION_STOP_LOSS/100),
                        bb_lower_band * 0.95  # 5% below lower band
                    )

                    # Target middle band or fixed percentage, whichever is closer
                    tp_price = min(
                        current_price * (1 + REVERSION_TAKE_PROFIT/100),
                        bb_middle_band * 0.98  # Target just below middle band
                    )

                    # Ensure valid order with minimum spread
                    if sl_price < current_price and tp_price > current_price:
                        try:
                            self.buy(size=0.95, sl=sl_price, tp=tp_price)
                            self.last_entry_price = current_price
                        except:
                            pass  # Silently skip invalid orders

        else:
            # Position management
            if self.position.is_long:
                # Trail stop loss in profit
                if self.last_entry_price and current_price > self.last_entry_price * 1.02:
                    # In profit by 2%, tighten stop
                    new_sl = current_price * 0.99  # Trail to 1% below current
                    if hasattr(self.position, 'sl') and new_sl > self.position.sl:
                        try:
                            self.position.close()
                            self.buy(size=0.95, sl=new_sl)
                        except:
                            pass

                # Exit mean reversion trades at middle band
                if self.current_mode == 'MEAN_REVERSION':
                    bb_middle_band = self.bb_middle[-1]
                    if current_price >= bb_middle_band * 0.98:
                        self.position.close()

                # Exit on RSI overbought in any mode
                if self.rsi[-1] > REVERSION_RSI_OVERBOUGHT:
                    self.position.close()


# Test function for single asset
if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')

    print("\n" + "="*80)
    print("🧪 TESTING FIXED MULTI-ASSET VOLATILITY STRATEGY")
    print("="*80)

    # Test on BTC daily data
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

        # Run backtest with proper settings
        bt = Backtest(
            data,
            VolatilityMultiAssetStrategy,
            cash=1000000,
            commission=0.001,
            exclusive_orders=True,
            trade_on_close=True
        )
        stats = bt.run()

        print(f"\n📈 RESULTS:")
        print(f"   • Return: {stats['Return [%]']:.2f}%")
        print(f"   • Sharpe: {stats['Sharpe Ratio']:.3f}")
        print(f"   • Max DD: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"   • Trades: {stats['# Trades']}")
        print(f"   • Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"   • Profit Factor: {stats.get('Profit Factor', 0):.2f}")

        # Test on ETH 6h data (which showed positive results)
        print("\n" + "-"*40)
        eth_path = '/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-6h-200wks-enhanced-data.csv'
        eth_data = pd.read_csv(eth_path, parse_dates=['datetime'], index_col='datetime')
        eth_data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        eth_data = eth_data.dropna().sort_index()

        # Fix OHLC
        for col in ['Open', 'High', 'Low', 'Close']:
            eth_data[col] = eth_data[col].abs()
        eth_data['High'] = eth_data[['Open', 'High', 'Low', 'Close']].max(axis=1)
        eth_data['Low'] = eth_data[['Open', 'High', 'Low', 'Close']].min(axis=1)

        print(f"📊 Testing on ETH-USD 6-Hour")
        print(f"   Data: {len(eth_data)} bars")

        bt_eth = Backtest(
            eth_data,
            VolatilityMultiAssetStrategy,
            cash=1000000,
            commission=0.001,
            exclusive_orders=True,
            trade_on_close=True
        )
        stats_eth = bt_eth.run()

        print(f"\n📈 RESULTS:")
        print(f"   • Return: {stats_eth['Return [%]']:.2f}%")
        print(f"   • Sharpe: {stats_eth['Sharpe Ratio']:.3f}")
        print(f"   • Max DD: {stats_eth['Max. Drawdown [%]']:.2f}%")
        print(f"   • Trades: {stats_eth['# Trades']}")
        print(f"   • Win Rate: {stats_eth['Win Rate [%]']:.2f}%")

        print("\n✅ Fixed strategy testing complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()