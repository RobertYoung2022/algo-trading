# eth_trend_converted_strategy.py
"""
🚀 Bobby's ETH Trend-Following Strategy - Converted to backtesting.py Framework
===============================================================================

This is the backtesting.py conversion of the original custom ETHTrendBacktester.
The strategy preserves all original logic while leveraging the standardized framework.

Original Strategy Logic (PRESERVED):
• 10-day and 50-day SMA crossovers for trend confirmation
• MACD momentum confirmation for entry signals  
• RSI filtering (RSI < 70 for entry, RSI > 80 for exit)
• 5% stop loss with 2:1 risk/reward ratio
• 3% trailing stop loss for risk management

🔄 Conversion Notes:
- Custom position tracking → backtesting.py position management
- Manual trade recording → Built-in trade tracking
- Custom signal loop → Event-driven next() method  
- Custom indicators → talib with self.I() wrapper
- Preserved trailing stop logic within framework constraints

📊 Performance Comparison:
The converted strategy should produce comparable results to the original
while benefiting from backtesting.py's robust infrastructure and multi-data testing.

Author: AI Assistant (Converted from Bobby's original strategy)
Date: 2025-09-11
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy

print("💫 Bobby's ETH Trend-Following Strategy Loading... 🌙")

# Strategy Parameters (Preserved from Original)
SMA_SHORT_PERIOD = 10           # Short-term SMA for trend detection
SMA_LONG_PERIOD = 50            # Long-term SMA for trend confirmation  
RSI_PERIOD = 14                 # RSI calculation period
RSI_OVERBOUGHT = 70             # RSI entry filter (avoid overbought entries)
RSI_EXIT_OVERBOUGHT = 80        # RSI exit trigger (take profits when very overbought)
MACD_FAST_PERIOD = 12           # MACD fast EMA period  
MACD_SLOW_PERIOD = 26           # MACD slow EMA period
MACD_SIGNAL_PERIOD = 9          # MACD signal line period
STOP_LOSS_PERCENT = 5.0         # Stop loss percentage (5%)
RISK_REWARD_RATIO = 2.0         # Risk/reward ratio (2:1)
TRAILING_STOP_PERCENT = 3.0     # Trailing stop percentage (3%)

class ETHTrendConvertedStrategy(Strategy):
    """
    🎯 ETH Trend-Following Strategy - Backtesting.py Framework Version
    
    Multi-Indicator Trend Strategy:
    ===============================
    1. 📈 SMA Crossover: 10-day crosses ABOVE 50-day SMA (bullish trend)
    2. ⚡ MACD Confirmation: MACD line crosses ABOVE signal line (momentum)  
    3. 🔍 RSI Filter: RSI < 70 (avoid overbought entries)
    4. 💰 Risk Management: 5% SL, 10% TP (2:1 ratio), 3% trailing stop
    
    Exit Conditions:
    ================
    • SMA bearish crossover (trend reversal)
    • RSI > 80 (very overbought, take profits)
    • Stop loss or take profit hit
    • Trailing stop triggered
    
    🧠 Strategy Logic:
    This is a comprehensive trend-following system that combines multiple
    timeframes and indicators for high-probability entries while maintaining
    strict risk management through multiple exit mechanisms.
    """
    
    # Strategy parameters (configurable for optimization)
    sma_short = SMA_SHORT_PERIOD
    sma_long = SMA_LONG_PERIOD  
    rsi_period = RSI_PERIOD
    rsi_overbought = RSI_OVERBOUGHT
    rsi_exit_overbought = RSI_EXIT_OVERBOUGHT
    macd_fast = MACD_FAST_PERIOD
    macd_slow = MACD_SLOW_PERIOD 
    macd_signal = MACD_SIGNAL_PERIOD
    stop_loss = STOP_LOSS_PERCENT / 100
    risk_reward_ratio = RISK_REWARD_RATIO
    trailing_stop = TRAILING_STOP_PERCENT / 100

    def init(self):
        """
        🔧 Initialize all technical indicators using self.I() wrapper
        
        Indicators Created:
        • SMA Short (10-day): Fast trend indicator
        • SMA Long (50-day): Slow trend confirmation  
        • MACD System: Trend momentum (12,26,9)
        • RSI (14): Overbought/oversold filter
        
        💡 Note: self.I() ensures proper integration with backtesting framework
        """
        # Initialize SMA indicators for trend detection
        self.sma_short_line = self.I(talib.SMA, self.data.Close, self.sma_short)
        self.sma_long_line = self.I(talib.SMA, self.data.Close, self.sma_long)
        
        # Initialize MACD system for momentum confirmation
        # MACD returns tuple: (macd, signal, histogram)
        macd_data = talib.MACD(
            self.data.Close,
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow,
            signalperiod=self.macd_signal
        )
        
        self.macd_line = self.I(lambda x: macd_data[0], self.data.Close, name='MACD')
        self.macd_signal_line = self.I(lambda x: macd_data[1], self.data.Close, name='MACD_Signal')  
        self.macd_histogram = self.I(lambda x: macd_data[2], self.data.Close, name='MACD_Histogram')
        
        # Initialize RSI for overbought/oversold filtering
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)
        
        # Initialize position tracking for trailing stop
        self.highest_price = 0
        self.trailing_stop_price = 0

    def next(self):
        """
        🎯 Main strategy logic executed on each bar
        
        Entry Logic:
        ============
        1. Check for SMA bullish crossover (short > long)
        2. Confirm MACD bullish crossover (macd > signal)  
        3. Filter with RSI (< 70, not overbought)
        4. Enter long with stop loss and take profit
        
        Exit Logic:
        ===========
        1. SMA bearish crossover (trend reversal)
        2. RSI very overbought (> 80)
        3. Trailing stop hit (3% below highest price)
        4. Regular stop/take profit levels
        
        💡 Conversion Notes:
        - Original loop-based logic → Event-driven next() method
        - Manual position tracking → Built-in self.position
        - Custom trade recording → Automatic trade tracking
        """
        # 🔍 Data Validation: Need enough bars for all indicators
        if (len(self.sma_long_line) < self.sma_long or 
            len(self.macd_line) < self.macd_slow or 
            len(self.rsi) < self.rsi_period):
            return
            
        # 📊 Current indicator values
        current_price = self.data.Close[-1]
        current_sma_short = self.sma_short_line[-1] 
        current_sma_long = self.sma_long_line[-1]
        current_macd = self.macd_line[-1]
        current_macd_signal = self.macd_signal_line[-1]
        current_rsi = self.rsi[-1]
        
        # 📈 Previous values for crossover detection
        prev_sma_short = self.sma_short_line[-2]
        prev_sma_long = self.sma_long_line[-2] 
        prev_macd = self.macd_line[-2]
        prev_macd_signal = self.macd_signal_line[-2]
        
        # 🚀 ENTRY LOGIC - Long Only Strategy
        if not self.position:
            # 1️⃣ SMA Bullish Crossover: Short SMA crosses above Long SMA
            sma_bullish_crossover = (prev_sma_short <= prev_sma_long and 
                                   current_sma_short > current_sma_long)
            
            # 2️⃣ MACD Momentum Confirmation: MACD crosses above signal line  
            macd_bullish_crossover = (prev_macd <= prev_macd_signal and
                                    current_macd > current_macd_signal)
            
            # 3️⃣ RSI Filter: Avoid overbought entries
            rsi_filter = current_rsi < self.rsi_overbought
            
            # 🎯 Enter Long Position with Risk Management
            if sma_bullish_crossover and macd_bullish_crossover and rsi_filter:
                # Calculate stop loss and take profit levels
                stop_loss_price = current_price * (1 - self.stop_loss)
                take_profit_price = current_price * (1 + (self.stop_loss * self.risk_reward_ratio))
                
                # Initialize trailing stop tracking
                self.highest_price = current_price
                self.trailing_stop_price = current_price * (1 - self.trailing_stop)
                
                # Place order with stop loss and take profit
                self.buy(sl=stop_loss_price, tp=take_profit_price)
        
        # 🛑 EXIT LOGIC - Multiple Exit Conditions
        else:
            # Update trailing stop (track highest price since entry)
            if current_price > self.highest_price:
                self.highest_price = current_price
                self.trailing_stop_price = current_price * (1 - self.trailing_stop)
            
            # 1️⃣ SMA Bearish Crossover: Trend reversal signal
            sma_bearish_crossover = (prev_sma_short >= prev_sma_long and
                                   current_sma_short < current_sma_long)
            
            # 2️⃣ RSI Very Overbought: Take profits at extreme levels  
            rsi_very_overbought = current_rsi > self.rsi_exit_overbought
            
            # 3️⃣ Trailing Stop Hit: Price dropped 3% from recent high
            trailing_stop_hit = current_price <= self.trailing_stop_price
            
            # 🚪 Execute Exit on Any Condition
            if sma_bearish_crossover or rsi_very_overbought or trailing_stop_hit:
                self.sell()
                
                # Reset trailing stop tracking
                self.highest_price = 0
                self.trailing_stop_price = 0

# 🧪 MULTI-DATA TESTING INTEGRATION
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🧪 TESTING ETH TREND-FOLLOWING STRATEGY ON ALL DATA SOURCES")  
    print("="*80)
    print("📊 Strategy Details:")
    print(f"   • SMA Crossover: {SMA_SHORT_PERIOD}-day x {SMA_LONG_PERIOD}-day")
    print(f"   • MACD Momentum: ({MACD_FAST_PERIOD}, {MACD_SLOW_PERIOD}, {MACD_SIGNAL_PERIOD})")
    print(f"   • RSI Filter: Entry < {RSI_OVERBOUGHT}, Exit > {RSI_EXIT_OVERBOUGHT}")
    print(f"   • Risk Management: {STOP_LOSS_PERCENT}% SL, {STOP_LOSS_PERCENT * RISK_REWARD_RATIO}% TP, {TRAILING_STOP_PERCENT}% Trailing")
    print(f"   • Entry: SMA crossover + MACD momentum + RSI filter")
    print(f"   • Exit: SMA bearish OR RSI >80 OR trailing stop OR SL/TP")
    print("="*80)
    
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from multi_data_tester import test_on_all_data
    
    # 🚀 Test strategy on all configured data sources
    results = test_on_all_data(ETHTrendConvertedStrategy, 'ETH_Trend_Converted_Strategy')
    
    if results is not None:
        print("\n✅ Testing complete! Results saved in: ./results/ETH_Trend_Converted_Strategy.csv")
        print("\n🎯 Key Insights:")
        print("   • Multi-indicator approach reduces false signals")
        print("   • SMA crossover captures major trend changes")  
        print("   • MACD adds momentum confirmation for better timing")
        print("   • RSI filter avoids poor entry points at extremes")
        print("   • Trailing stop locks in profits during strong moves")
        print("   • 2:1 risk/reward provides positive expectancy over time")
        print("\n📈 Conversion Benefits:")
        print("   • Standardized backtesting framework")
        print("   • Automatic trade tracking and performance metrics")
        print("   • Multi-data source testing capability")  
        print("   • Consistent results comparison with other strategies")
        print("   • Built-in risk management and position sizing")
        
        print("\n💡 Strategy Performance Notes:")
        print("   • Best performance expected in trending markets")
        print("   • May struggle in sideways/choppy conditions")
        print("   • Multiple confirmations reduce trade frequency but improve quality")
        print("   • Trailing stop helps capture extended moves")
        print("   • Compare results with original custom backtester for validation")
    else:
        print("❌ Testing failed. Please check data sources and strategy implementation.")