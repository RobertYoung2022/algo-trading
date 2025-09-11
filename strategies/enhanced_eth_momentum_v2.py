"""
Enhanced ETH Momentum Strategy V2 - Balanced Optimization
=========================================================

Based on debug analysis, this version provides a more balanced approach:
- Faster MACD signals (8,21,5) but with relaxed conditions
- RSI threshold increased to 70 for more entries  
- 100-day MA instead of 200-day for trend filter
- Volume filter made optional (used for confirmation only)
- Removed MACD > 0 requirement to catch more signals
- ATR-based dynamic stops maintained
- Proper position sizing (2% risk per trade)

Author: Bobby's Enhanced Strategy Framework V2
Date: 2025-09-11
"""

import pandas as pd
import numpy as np
from backtesting import Strategy
import talib

class EnhancedETHMomentumStrategyV2(Strategy):
    """
    Enhanced ETH Momentum Strategy V2 - More balanced and less restrictive
    
    Key Improvements from V1:
    1. Relaxed entry conditions for more trading opportunities
    2. 100-day MA trend filter instead of 200-day
    3. RSI threshold 70 instead of 65
    4. Removed MACD > 0 requirement
    5. Volume filter optional (confirmation only)
    6. Multiple entry signal types
    """
    
    # Balanced Parameters
    macd_fast = 8           # Faster signals
    macd_slow = 21          # Faster signals
    macd_signal = 5         # Faster signals
    rsi_period = 14
    rsi_threshold = 70      # Relaxed from 65
    ma_trend_period = 100   # Shorter trend filter (was 200)
    atr_period = 14
    atr_multiplier = 2.0
    volume_sma_period = 20
    risk_per_trade = 0.02
    
    def init(self):
        """Initialize all indicators"""
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        volume = self.data.Volume
        
        # MACD with optimized parameters
        self.macd_line, self.macd_signal_line, self.macd_histogram = self.I(
            talib.MACD, close,
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow,
            signalperiod=self.macd_signal
        )
        
        # RSI
        self.rsi = self.I(talib.RSI, close, timeperiod=self.rsi_period)
        
        # Moving averages (shorter trend filter)
        self.ma_trend = self.I(talib.SMA, close, timeperiod=self.ma_trend_period)
        self.ma_50 = self.I(talib.SMA, close, timeperiod=50)
        
        # ATR for dynamic stops
        self.atr = self.I(talib.ATR, high, low, close, timeperiod=self.atr_period)
        
        # Volume confirmation (optional)
        self.volume_sma = self.I(talib.SMA, volume, timeperiod=self.volume_sma_period)
        
        # Track position details
        self.entry_price = None
        self.stop_loss = None
        
    def next(self):
        """Execute trading logic with relaxed conditions"""
        current_price = self.data.Close[-1]
        
        # Current indicator values
        macd_current = self.macd_line[-1]
        macd_signal_current = self.macd_signal_line[-1]
        macd_prev = self.macd_line[-2] if len(self.macd_line) > 1 else 0
        macd_signal_prev = self.macd_signal_line[-2] if len(self.macd_signal_line) > 1 else 0
        
        rsi_current = self.rsi[-1]
        ma_trend_current = self.ma_trend[-1]
        ma_50_current = self.ma_50[-1]
        atr_current = self.atr[-1]
        volume_current = self.data.Volume[-1]
        volume_sma_current = self.volume_sma[-1]
        
        # Skip if indicators not ready
        if (pd.isna(macd_current) or pd.isna(rsi_current) or 
            pd.isna(ma_trend_current) or pd.isna(atr_current)):
            return
        
        # Entry Logic - Multiple signal types with relaxed conditions
        if not self.position:
            
            # Primary Signal: MACD Bullish Crossover
            macd_bullish_cross = (macd_prev <= macd_signal_prev and 
                                macd_current > macd_signal_current)
            
            # Alternative Signal: MACD histogram turning positive
            macd_hist_current = self.macd_histogram[-1] if len(self.macd_histogram) > 0 else 0
            macd_hist_prev = self.macd_histogram[-2] if len(self.macd_histogram) > 1 else 0
            macd_hist_bullish = (macd_hist_prev <= 0 and macd_hist_current > 0)
            
            # Core Filters (more relaxed)
            rsi_filter = rsi_current < self.rsi_threshold  # RSI < 70
            trend_filter = current_price > ma_trend_current  # Price > 100-day MA
            
            # Optional filters (for signal quality)
            momentum_filter = current_price > ma_50_current
            volume_confirmation = volume_current > volume_sma_current
            
            # Entry conditions - Multiple pathways
            primary_entry = (macd_bullish_cross and rsi_filter and trend_filter)
            
            alternative_entry = (macd_hist_bullish and rsi_filter and trend_filter and momentum_filter)
            
            strong_momentum_entry = (macd_bullish_cross and rsi_filter and momentum_filter and volume_confirmation)
            
            # Entry if any condition is met
            if primary_entry or alternative_entry or strong_momentum_entry:
                
                # Calculate position size based on ATR stop
                dynamic_stop_distance = atr_current * self.atr_multiplier
                dynamic_stop_price = current_price - dynamic_stop_distance
                
                # Position sizing
                risk_amount = self.equity * self.risk_per_trade
                price_risk = current_price - dynamic_stop_price
                
                if price_risk > 0:
                    position_size = risk_amount / price_risk
                    size_pct = min(position_size / self.equity, 0.90)  # Max 90% of equity
                    
                    # Enter position
                    self.buy(size=size_pct)
                    
                    # Store entry details
                    self.entry_price = current_price
                    self.stop_loss = dynamic_stop_price
        
        # Exit Logic - Keep strong risk management
        elif self.position:
            
            # 1. Stop Loss
            if current_price <= self.stop_loss:
                self.position.close()
                self.entry_price = None
                self.stop_loss = None
                return
            
            # 2. Trailing Stop
            if self.entry_price:
                new_stop = current_price - (atr_current * self.atr_multiplier)
                if new_stop > self.stop_loss:
                    self.stop_loss = new_stop
            
            # 3. MACD Bearish Signal
            macd_bearish_cross = (macd_prev >= macd_signal_prev and 
                                macd_current < macd_signal_current)
            
            # 4. RSI Overbought
            rsi_overbought = rsi_current > 85  # Very overbought
            
            # 5. Trend breakdown
            trend_breakdown = current_price < ma_trend_current
            
            # 6. Take profit on strong moves
            if self.entry_price:
                profit_pct = (current_price - self.entry_price) / self.entry_price
                take_profit_hit = profit_pct > 0.15  # 15% profit target
            else:
                take_profit_hit = False
            
            # Exit conditions
            if (macd_bearish_cross or rsi_overbought or trend_breakdown or take_profit_hit):
                self.position.close()
                self.entry_price = None
                self.stop_loss = None

# Test script
if __name__ == "__main__":
    """
    Individual test script for the V2 strategy
    """
    from backtesting import Backtest
    
    # Load ETH data
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    try:
        # Load and prepare data
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
        
        print("🚀 Testing Enhanced ETH Momentum Strategy V2")
        print("=" * 60)
        print(f"📊 Data: {len(data)} bars from {data.index[0]} to {data.index[-1]}")
        print(f"💰 Price range: ${data['Close'].min():.2f} - ${data['Close'].max():.2f}")
        
        # Run backtest
        bt = Backtest(data, EnhancedETHMomentumStrategyV2, cash=100000, commission=0.001)
        stats = bt.run()
        
        # Print results
        print("\n📈 ENHANCED ETH MOMENTUM STRATEGY V2 RESULTS")
        print("=" * 60)
        print(f"Return: {stats['Return [%]']:.2f}%")
        print(f"Buy & Hold: {stats['Buy & Hold Return [%]']:.2f}%")
        print(f"Trades: {stats['# Trades']}")
        print(f"Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"Sharpe Ratio: {stats['Sharpe Ratio']:.3f}")
        
        if stats['# Trades'] > 0:
            print("\n✅ Strategy generating trades!")
        else:
            print("\n❌ No trades generated - further optimization needed")
        
    except Exception as e:
        print(f"❌ Error running backtest: {e}")