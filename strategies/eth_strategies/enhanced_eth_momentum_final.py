"""
Enhanced ETH Momentum Strategy - Final Working Version
=====================================================

This strategy implements optimized MACD parameters with proper position sizing:
- Faster MACD signals (8,21,5) for quicker entry/exit
- Balanced RSI filter (70) for reasonable entry frequency
- Simple but effective risk management
- Fixed position sizing to avoid margin issues
- Focus on trade generation and profitability

Based on comprehensive analysis showing original MACD strategy needs optimization.

Author: Bobby's Enhanced Strategy Framework - Final
Date: 2025-09-11
"""

import pandas as pd
import numpy as np
from backtesting import Strategy
import talib

class EnhancedETHMomentumFinal(Strategy):
    """
    Final working version of Enhanced ETH Momentum Strategy
    
    Key Features:
    1. Optimized MACD (8,21,5) for faster signals
    2. RSI filter (70) to avoid overbought entries
    3. Simple trend filter using 50-day MA
    4. Fixed position sizing to avoid margin issues
    5. ATR-based stop loss for dynamic risk management
    6. Clear entry/exit rules for consistent execution
    """
    
    # Strategy Parameters
    macd_fast = 8
    macd_slow = 21  
    macd_signal = 5
    rsi_period = 14
    rsi_threshold = 70
    ma_period = 50
    atr_period = 14
    atr_multiplier = 2.0
    position_size = 0.95  # Fixed 95% of equity
    
    def init(self):
        """Initialize indicators"""
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        
        # MACD with faster parameters
        self.macd_line, self.macd_signal_line, self.macd_histogram = self.I(
            talib.MACD, close,
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow,
            signalperiod=self.macd_signal
        )
        
        # RSI for momentum filter
        self.rsi = self.I(talib.RSI, close, timeperiod=self.rsi_period)
        
        # Moving average for trend filter
        self.ma = self.I(talib.SMA, close, timeperiod=self.ma_period)
        
        # ATR for stop loss
        self.atr = self.I(talib.ATR, high, low, close, timeperiod=self.atr_period)
        
        # Position tracking
        self.entry_price = None
        self.stop_loss = None
        
    def next(self):
        """Execute trading logic"""
        current_price = self.data.Close[-1]
        
        # Current values
        macd_now = self.macd_line[-1]
        macd_signal_now = self.macd_signal_line[-1]
        rsi_now = self.rsi[-1]
        ma_now = self.ma[-1]
        atr_now = self.atr[-1]
        
        # Previous values for crossover detection
        if len(self.macd_line) < 2:
            return
            
        macd_prev = self.macd_line[-2]
        macd_signal_prev = self.macd_signal_line[-2]
        
        # Skip if indicators not ready
        if pd.isna(macd_now) or pd.isna(rsi_now) or pd.isna(ma_now) or pd.isna(atr_now):
            return
        
        # Entry Logic
        if not self.position:
            # MACD bullish crossover
            macd_cross_up = (macd_prev <= macd_signal_prev and macd_now > macd_signal_now)
            
            # Filters
            rsi_ok = rsi_now < self.rsi_threshold  # Not overbought
            trend_ok = current_price > ma_now      # Above MA trend
            
            # Entry signal
            if macd_cross_up and rsi_ok and trend_ok:
                # Calculate stop loss
                stop_distance = atr_now * self.atr_multiplier
                self.stop_loss = current_price - stop_distance
                self.entry_price = current_price
                
                # Enter position with fixed size
                self.buy(size=self.position_size)
        
        # Exit Logic
        elif self.position:
            # Stop loss
            if current_price <= self.stop_loss:
                self.position.close()
                self.reset_position()
                return
            
            # Trailing stop
            new_stop = current_price - (atr_now * self.atr_multiplier)
            if new_stop > self.stop_loss:
                self.stop_loss = new_stop
            
            # MACD bearish crossover
            macd_cross_down = (macd_prev >= macd_signal_prev and macd_now < macd_signal_now)
            
            # RSI very overbought
            rsi_very_high = rsi_now > 80
            
            # Trend reversal
            trend_broken = current_price < ma_now
            
            # Exit conditions
            if macd_cross_down or rsi_very_high or trend_broken:
                self.position.close()
                self.reset_position()
    
    def reset_position(self):
        """Reset position tracking variables"""
        self.entry_price = None
        self.stop_loss = None

# Test function for multi-data testing
def create_enhanced_final_strategy():
    """Factory function for multi-data testing"""
    return EnhancedETHMomentumFinal

# Individual test
if __name__ == "__main__":
    """Test the strategy individually"""
    from backtesting import Backtest
    
    # Load ETH data
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    try:
        # Load data
        data = pd.read_csv(data_file)
        
        # Find date column
        date_cols = [col for col in data.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
        if date_cols:
            date_col = date_cols[0]
            data[date_col] = pd.to_datetime(data[date_col])
            data = data.set_index(date_col)
        
        # Standardize columns
        data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data = data.sort_index().dropna()
        
        print("🚀 Testing Enhanced ETH Momentum Strategy - Final Version")
        print("=" * 70)
        print(f"📊 Data: {len(data)} bars from {data.index[0]} to {data.index[-1]}")
        print(f"💰 Price range: ${data['Close'].min():.2f} - ${data['Close'].max():.2f}")
        print("\n🔧 Strategy Parameters:")
        print(f"   MACD: ({EnhancedETHMomentumFinal.macd_fast},{EnhancedETHMomentumFinal.macd_slow},{EnhancedETHMomentumFinal.macd_signal})")
        print(f"   RSI threshold: {EnhancedETHMomentumFinal.rsi_threshold}")
        print(f"   MA period: {EnhancedETHMomentumFinal.ma_period}")
        print(f"   ATR multiplier: {EnhancedETHMomentumFinal.atr_multiplier}")
        
        # Run backtest
        bt = Backtest(data, EnhancedETHMomentumFinal, cash=100000, commission=0.001)
        stats = bt.run()
        
        # Print key results
        print("\n📈 RESULTS SUMMARY")
        print("=" * 40)
        print(f"📊 Return: {stats['Return [%]']:.2f}%")
        print(f"📈 Buy & Hold: {stats['Buy & Hold Return [%]']:.2f}%")
        print(f"🔄 Trades: {stats['# Trades']}")
        
        if stats['# Trades'] > 0:
            print(f"🎯 Win Rate: {stats['Win Rate [%]']:.2f}%")
            print(f"📉 Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
            print(f"⚡ Sharpe Ratio: {stats['Sharpe Ratio']:.3f}")
            print(f"💰 Best Trade: {stats['Best Trade [%]']:.2f}%")
            print(f"💸 Worst Trade: {stats['Worst Trade [%]']:.2f}%")
            print(f"📊 Avg Trade: {stats['Avg. Trade [%]']:.3f}%")
            print(f"💪 Profit Factor: {stats['Profit Factor']:.3f}")
            
            print("\n✅ Strategy generating trades successfully!")
            
            # Performance assessment
            if stats['Return [%]'] > 0:
                print("🟢 Strategy is profitable!")
            else:
                print("🔴 Strategy needs further optimization")
                
            if stats['Sharpe Ratio'] > 1.0:
                print("🟢 Good risk-adjusted returns")
            elif stats['Sharpe Ratio'] > 0.5:
                print("🟡 Moderate risk-adjusted returns")
            else:
                print("🔴 Poor risk-adjusted returns")
        else:
            print("\n❌ No trades generated")
        
        print(f"\n📄 Full stats available in backtest object")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()