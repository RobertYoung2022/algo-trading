"""
Enhanced ETH Momentum Strategy - Optimized from MACD Analysis
==========================================================

This strategy implements all the optimization recommendations from the MACD comprehensive analysis:
- Faster MACD signals (8,21,5) instead of (12,26,9) 
- Modified RSI filter (65 instead of 70) for more entries
- ATR-based dynamic stop losses instead of fixed 3%
- 200-day MA trend filter for market regime detection
- Volume confirmation for signal quality
- Proper position sizing (2% risk per trade)

Based on the comprehensive analysis of the original MACD strategy which showed:
- All datasets had negative returns (strategy needs optimization)
- Low trade frequency suggests over-filtering
- High drawdowns indicate need for better risk management
- ETH performance was particularly poor and needs focus

Author: Bobby's Enhanced Strategy Framework
Date: 2025-09-11
"""

import pandas as pd
import numpy as np
from backtesting import Strategy
import talib

class EnhancedETHMomentumStrategy(Strategy):
    """
    Enhanced ETH Momentum Strategy with optimized parameters based on MACD analysis insights
    
    Key Improvements:
    1. Faster MACD (8,21,5) for quicker signals
    2. Relaxed RSI filter (65) for more entries
    3. ATR-based dynamic stops for better risk management
    4. 200-day MA trend filter to avoid choppy markets
    5. Volume confirmation for signal quality
    6. Proper position sizing based on risk per trade
    """
    
    # Optimized Parameters (based on analysis recommendations)
    macd_fast = 8           # Faster than original 12
    macd_slow = 21          # Faster than original 26  
    macd_signal = 5         # Faster than original 9
    rsi_period = 14
    rsi_threshold = 65      # Lower than original 70 for more entries
    ma_trend_period = 200   # Long-term trend filter
    atr_period = 14         # For dynamic stop loss
    atr_multiplier = 2.0    # ATR multiplier for stop loss
    volume_sma_period = 20  # Volume confirmation
    risk_per_trade = 0.02   # 2% risk per trade
    
    def init(self):
        """Initialize all indicators"""
        # Price data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        volume = self.data.Volume
        
        # MACD with optimized parameters (8,21,5)
        self.macd_line, self.macd_signal_line, self.macd_histogram = self.I(
            talib.MACD, close, 
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow, 
            signalperiod=self.macd_signal
        )
        
        # RSI with relaxed threshold (65)
        self.rsi = self.I(talib.RSI, close, timeperiod=self.rsi_period)
        
        # 200-day MA for trend filter
        self.ma_trend = self.I(talib.SMA, close, timeperiod=self.ma_trend_period)
        
        # ATR for dynamic stop loss
        self.atr = self.I(talib.ATR, high, low, close, timeperiod=self.atr_period)
        
        # Volume confirmation
        self.volume_sma = self.I(talib.SMA, volume, timeperiod=self.volume_sma_period)
        
        # 50-day MA for additional confirmation
        self.ma_50 = self.I(talib.SMA, close, timeperiod=50)
        
        # Track entry price and stop loss for each position
        self.entry_price = None
        self.stop_loss = None
        
    def next(self):
        """Execute trading logic on each bar"""
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
        
        # Entry Logic - Multiple conditions for robust signal
        if not self.position:
            
            # 1. MACD Bullish Crossover (faster signals)
            macd_bullish_cross = (macd_prev <= macd_signal_prev and 
                                macd_current > macd_signal_current)
            
            # 2. RSI Filter (relaxed to 65 for more entries)
            rsi_filter = rsi_current < self.rsi_threshold
            
            # 3. Trend Filter - Price above 200-day MA (bull market only)
            trend_filter = current_price > ma_trend_current
            
            # 4. Additional momentum - Price above 50-day MA
            momentum_filter = current_price > ma_50_current
            
            # 5. Volume confirmation - Current volume above average
            volume_filter = volume_current > volume_sma_current
            
            # 6. MACD above zero line for additional confirmation
            macd_positive = macd_current > 0
            
            # Entry Signal: All conditions must be met
            if (macd_bullish_cross and rsi_filter and trend_filter and 
                momentum_filter and volume_filter and macd_positive):
                
                # Calculate position size based on 2% risk
                # Dynamic stop loss based on ATR
                dynamic_stop_distance = atr_current * self.atr_multiplier
                dynamic_stop_price = current_price - dynamic_stop_distance
                
                # Position size calculation
                # Risk = (Entry Price - Stop Price) * Position Size
                # Position Size = Risk Amount / (Entry Price - Stop Price)
                risk_amount = self.equity * self.risk_per_trade
                price_risk = current_price - dynamic_stop_price
                
                if price_risk > 0:
                    position_size = risk_amount / price_risk
                    # Convert to percentage of equity
                    size_pct = min(position_size / self.equity, 0.95)  # Max 95% of equity
                    
                    # Enter long position
                    self.buy(size=size_pct)
                    
                    # Store entry details
                    self.entry_price = current_price
                    self.stop_loss = dynamic_stop_price
        
        # Exit Logic - Multiple exit conditions
        elif self.position:
            
            # 1. Stop Loss - ATR-based dynamic stop
            if current_price <= self.stop_loss:
                self.position.close()
                self.entry_price = None
                self.stop_loss = None
                return
            
            # 2. Trailing Stop - Update stop loss if price moves favorably
            if self.entry_price:
                new_stop = current_price - (atr_current * self.atr_multiplier)
                if new_stop > self.stop_loss:
                    self.stop_loss = new_stop
            
            # 3. MACD Bearish Crossover - Exit signal
            macd_bearish_cross = (macd_prev >= macd_signal_prev and 
                                macd_current < macd_signal_current)
            
            # 4. RSI Overbought - Take profits
            rsi_overbought = rsi_current > 80
            
            # 5. Price below 50-day MA - Momentum lost
            momentum_lost = current_price < ma_50_current
            
            # 6. Volume divergence - Low volume on rise
            volume_divergence = (volume_current < volume_sma_current * 0.7 and 
                               current_price > self.entry_price * 1.05)  # Only if in profit
            
            # Exit conditions
            if (macd_bearish_cross or rsi_overbought or momentum_lost or volume_divergence):
                self.position.close()
                self.entry_price = None
                self.stop_loss = None

# Strategy factory function for multi-data testing
def create_enhanced_eth_strategy():
    """Factory function to create the strategy for testing"""
    return EnhancedETHMomentumStrategy

# Test script for individual backtesting
if __name__ == "__main__":
    """
    Individual test script - can be run standalone
    """
    from backtesting import Backtest
    import matplotlib.pyplot as plt
    
    # Load ETH data for testing
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    try:
        # Load and prepare data
        data = pd.read_csv(data_file)
        
        # Find date column dynamically
        date_cols = [col for col in data.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
        if date_cols:
            date_col = date_cols[0]
            data[date_col] = pd.to_datetime(data[date_col])
            data = data.set_index(date_col)
        
        # Standardize column names for backtesting.py
        data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data = data.sort_index().dropna()
        
        print("🚀 Testing Enhanced ETH Momentum Strategy")
        print("=" * 60)
        print(f"📊 Data: {len(data)} bars from {data.index[0]} to {data.index[-1]}")
        print(f"💰 Price range: ${data['Close'].min():.2f} - ${data['Close'].max():.2f}")
        
        # Run backtest
        bt = Backtest(data, EnhancedETHMomentumStrategy, cash=100000, commission=0.001)
        stats = bt.run()
        
        # Print results
        print("\n📈 ENHANCED ETH MOMENTUM STRATEGY RESULTS")
        print("=" * 60)
        print(stats)
        
        # Plot results
        bt.plot(filename='enhanced_eth_momentum_backtest.html')
        
        print("\n✅ Backtest completed! Check the HTML file for detailed charts.")
        
    except Exception as e:
        print(f"❌ Error running backtest: {e}")
        print("Make sure the data file exists and has the correct format.")