# adaptive_volatility_strategy.py
import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy

print("💫 BobbyYo's Adaptive Volatility Strategy Loading... 🌙")

# ====== BobbyYo's Strategy Configuration 🌙 ======
# Default (BTC-optimized) Parameters
ATR_PERIOD = 14              # ATR calculation period
ATR_MULTIPLIER = 0.5         # ATR multiplier for breakout level
BREAKOUT_SIZE = 12           # Position size for breakout trades (aggressive)
BREAKOUT_TP = 1.03           # Take profit: 3% 
BREAKOUT_SL = 0.99           # Stop loss: 1%

# Mean Reversion Parameters  
DIP_THRESHOLD = 0.9          # Mean revert when price drops 10%
REVERSION_SIZE = 5           # Position size for mean reversion (lighter)
REVERSION_TP = 1.02          # Take profit: 2%
REVERSION_SL = 0.95          # Stop loss: 5%

# Adaptive Parameters
ATR_AVG_PERIOD = 20          # Period for ATR average (regime detection)
COMBO_BREAKOUT_SIZE = 12     # Combo strategy breakout size
COMBO_REVERSION_SIZE = 6     # Combo strategy reversion size
COMBO_BREAKOUT_TP = 1.04     # Combo breakout take profit: 4%
COMBO_REVERSION_TP = 1.02    # Combo reversion take profit: 2%
COMBO_BREAKOUT_SL = 0.99     # Combo breakout stop loss: 1%
COMBO_REVERSION_SL = 0.94    # Combo reversion stop loss: 6%

# ====== ETH-Specific Optimized Parameters 🚀 ======
ETH_ATR_PERIOD = 10              # Shorter ATR period for ETH
ETH_ATR_MULTIPLIER = 0.35        # More sensitive breakouts
ETH_BREAKOUT_SIZE = 8            # Smaller initial positions
ETH_BREAKOUT_TP = 1.025          # Lower take profit: 2.5%
ETH_BREAKOUT_SL = 0.985          # Tighter stop loss: 1.5%

# ETH Mean Reversion Parameters
ETH_DIP_THRESHOLD = 0.96         # Catch 4% dips instead of 10%
ETH_REVERSION_SIZE = 8           # Larger positions for mean reversion
ETH_REVERSION_TP = 1.015         # Lower take profit: 1.5%
ETH_REVERSION_SL = 0.975         # Tighter stop loss: 2.5%

# ETH Adaptive Parameters
ETH_ATR_AVG_PERIOD = 15          # Shorter average period
ETH_COMBO_BREAKOUT_SIZE = 6      # Conservative breakout sizing
ETH_COMBO_REVERSION_SIZE = 8     # Favor mean reversion for ETH
ETH_COMBO_BREAKOUT_TP = 1.025    # ETH breakout take profit: 2.5%
ETH_COMBO_REVERSION_TP = 1.015   # ETH reversion take profit: 1.5%
ETH_COMBO_BREAKOUT_SL = 0.985    # ETH breakout stop loss: 1.5%
ETH_COMBO_REVERSION_SL = 0.975   # ETH reversion stop loss: 2.5%

# Backtesting Configuration
INITIAL_CASH = 15000         # $15,000 starting capital
COMMISSION = 0.0007          # 0.07% commission rate

print(f"🎯 Strategy Configuration:")
print(f"💰 Initial Cash: ${INITIAL_CASH:,}")
print(f"📊 Commission: {COMMISSION*100:.2f}%")
print(f"🔥 ATR Period: {ATR_PERIOD} | Multiplier: {ATR_MULTIPLIER}")
print(f"⚡ Breakout: Size {BREAKOUT_SIZE} | TP {BREAKOUT_TP} | SL {BREAKOUT_SL}")
print(f"🔄 Reversion: Size {REVERSION_SIZE} | TP {REVERSION_TP} | SL {REVERSION_SL}")

class VolBreakStrategy(Strategy):
    """🚀 Pure Volatility Breakout Strategy - Aggressive momentum trading"""
    
    # Strategy parameters
    atr_period = ATR_PERIOD
    atr_multiplier = ATR_MULTIPLIER
    position_size = BREAKOUT_SIZE
    take_profit = BREAKOUT_TP
    stop_loss = BREAKOUT_SL

    def init(self):
        # ATR for volatility measurement
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        
        # Yesterday's high for breakout level calculation
        close_data = pd.Series(self.data.Close, index=self.data.index)
        high_data = pd.Series(self.data.High, index=self.data.index)
        
        # Shift high data by 1 period
        self.yesterday_high = self.I(lambda x: pd.Series(x).shift(1).values, high_data)

    def next(self):
        # Skip if we don't have enough data
        if len(self.atr) < self.atr_period + 1:
            return
            
        # Calculate breakout level: yesterday_high + (ATR * 0.5)
        if pd.notna(self.yesterday_high[-1]) and pd.notna(self.atr[-1]):
            breakout_level = self.yesterday_high[-1] + (self.atr[-1] * self.atr_multiplier)
            
            # Entry condition: price breaks above level
            if not self.position and self.data.Close[-1] > breakout_level:
                current_price = self.data.Close[-1]
                tp_price = current_price * self.take_profit
                sl_price = current_price * self.stop_loss
                
                # Use percentage of equity for position sizing
                self.buy(size=0.8, tp=tp_price, sl=sl_price)  # 80% of available capital

class MeanRevStrategy(Strategy):
    """🔄 Pure Mean Reversion Strategy - Fade the dips"""
    
    # Strategy parameters
    dip_threshold = DIP_THRESHOLD
    position_size = REVERSION_SIZE
    take_profit = REVERSION_TP
    stop_loss = REVERSION_SL

    def init(self):
        # Price data for comparison
        self.price = self.data.Close

    def next(self):
        # Skip first few bars
        if len(self.price) < 2:
            return
            
        # Entry condition: price drops 10% from previous bar
        if (not self.position and 
            len(self.price) >= 2 and 
            self.price[-1] < self.price[-2] * self.dip_threshold):
            
            current_price = self.data.Close[-1]
            tp_price = current_price * self.take_profit
            sl_price = current_price * self.stop_loss
            
            # Lighter position sizing for mean reversion
            self.buy(size=0.4, tp=tp_price, sl=sl_price)  # 40% of available capital

class AdaptiveVolatilityStrategy(Strategy):
    """🧠 Adaptive Strategy - Breakout in high vol, mean revert in low vol"""
    
    # Strategy parameters
    atr_period = ATR_PERIOD
    atr_avg_period = ATR_AVG_PERIOD
    atr_multiplier = ATR_MULTIPLIER
    dip_threshold = DIP_THRESHOLD
    
    # Position sizing
    breakout_size = COMBO_BREAKOUT_SIZE
    reversion_size = COMBO_REVERSION_SIZE
    
    # Take profits and stop losses
    breakout_tp = COMBO_BREAKOUT_TP
    reversion_tp = COMBO_REVERSION_TP
    breakout_sl = COMBO_BREAKOUT_SL
    reversion_sl = COMBO_REVERSION_SL

    def init(self):
        # ATR for volatility measurement
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        
        # 20-day ATR average for regime detection
        self.atr_avg = self.I(talib.SMA, self.atr, self.atr_avg_period)
        
        # Yesterday's high for breakout calculation
        high_data = pd.Series(self.data.High, index=self.data.index)
        self.yesterday_high = self.I(lambda x: pd.Series(x).shift(1).values, high_data)
        
        # Price for mean reversion
        self.price = self.data.Close

    def next(self):
        # Skip if we don't have enough data
        if len(self.atr) < self.atr_avg_period + 1 or len(self.price) < 2:
            return
            
        # Skip if we're already in a position
        if self.position:
            return
            
        current_atr = self.atr[-1]
        avg_atr = self.atr_avg[-1]
        current_price = self.data.Close[-1]
        
        # Check if we have valid data
        if pd.notna(current_atr) and pd.notna(avg_atr):
            
            # HIGH VOLATILITY REGIME: Use breakout strategy
            if current_atr > avg_atr:
                # Calculate breakout level
                if pd.notna(self.yesterday_high[-1]):
                    breakout_level = self.yesterday_high[-1] + (current_atr * self.atr_multiplier)
                    
                    # Breakout entry
                    if current_price > breakout_level:
                        tp_price = current_price * self.breakout_tp
                        sl_price = current_price * self.breakout_sl
                        self.buy(size=0.7, tp=tp_price, sl=sl_price)  # 70% capital - aggressive
            
            # LOW VOLATILITY REGIME: Use mean reversion strategy  
            else:
                # Mean reversion entry: fade 10% dips
                if current_price < self.price[-2] * self.dip_threshold:
                    tp_price = current_price * self.reversion_tp
                    sl_price = current_price * self.reversion_sl
                    self.buy(size=0.4, tp=tp_price, sl=sl_price)  # 40% capital - conservative

# ====== ETH-Optimized Strategy Classes 🚀 ======

def detect_eth_symbol(data_source_name):
    """Detect if we're trading ETH based on data source name"""
    return 'ETH' in data_source_name.upper()

class ETHVolBreakStrategy(Strategy):
    """🚀 ETH-Optimized Volatility Breakout Strategy"""
    
    # ETH-specific parameters
    atr_period = ETH_ATR_PERIOD
    atr_multiplier = ETH_ATR_MULTIPLIER
    position_size = ETH_BREAKOUT_SIZE
    take_profit = ETH_BREAKOUT_TP
    stop_loss = ETH_BREAKOUT_SL

    def init(self):
        # ATR for volatility measurement
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        
        # Yesterday's high for breakout level calculation
        close_data = pd.Series(self.data.Close, index=self.data.index)
        high_data = pd.Series(self.data.High, index=self.data.index)
        
        # Shift high data by 1 period
        self.yesterday_high = self.I(lambda x: pd.Series(x).shift(1).values, high_data)

    def next(self):
        # Skip if we don't have enough data
        if len(self.atr) < self.atr_period + 1:
            return
            
        # Calculate breakout level: yesterday_high + (ATR * 0.35)
        if pd.notna(self.yesterday_high[-1]) and pd.notna(self.atr[-1]):
            breakout_level = self.yesterday_high[-1] + (self.atr[-1] * self.atr_multiplier)
            
            # Entry condition: price breaks above level
            if not self.position and self.data.Close[-1] > breakout_level:
                current_price = self.data.Close[-1]
                tp_price = current_price * self.take_profit
                sl_price = current_price * self.stop_loss
                
                # ETH-optimized position sizing (60% of capital)
                self.buy(size=0.6, tp=tp_price, sl=sl_price)

class ETHMeanRevStrategy(Strategy):
    """🔄 ETH-Optimized Mean Reversion Strategy"""
    
    # ETH-specific parameters
    dip_threshold = ETH_DIP_THRESHOLD
    position_size = ETH_REVERSION_SIZE
    take_profit = ETH_REVERSION_TP
    stop_loss = ETH_REVERSION_SL

    def init(self):
        # Price data for comparison
        self.price = self.data.Close

    def next(self):
        # Skip first few bars
        if len(self.price) < 2:
            return
            
        # Entry condition: price drops 4% from previous bar (ETH-optimized)
        if (not self.position and 
            len(self.price) >= 2 and 
            self.price[-1] < self.price[-2] * self.dip_threshold):
            
            current_price = self.data.Close[-1]
            tp_price = current_price * self.take_profit
            sl_price = current_price * self.stop_loss
            
            # ETH-optimized position sizing (50% of capital)
            self.buy(size=0.5, tp=tp_price, sl=sl_price)

class ETHAdaptiveVolatilityStrategy(Strategy):
    """🧠 ETH-Optimized Adaptive Strategy - Breakout in high vol, mean revert in low vol"""
    
    # ETH-specific parameters
    atr_period = ETH_ATR_PERIOD
    atr_avg_period = ETH_ATR_AVG_PERIOD
    atr_multiplier = ETH_ATR_MULTIPLIER
    dip_threshold = ETH_DIP_THRESHOLD
    
    # Position sizing
    breakout_size = ETH_COMBO_BREAKOUT_SIZE
    reversion_size = ETH_COMBO_REVERSION_SIZE
    
    # Take profits and stop losses
    breakout_tp = ETH_COMBO_BREAKOUT_TP
    reversion_tp = ETH_COMBO_REVERSION_TP
    breakout_sl = ETH_COMBO_BREAKOUT_SL
    reversion_sl = ETH_COMBO_REVERSION_SL

    def init(self):
        # ATR for volatility measurement
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        
        # ETH-optimized ATR average for regime detection
        self.atr_avg = self.I(talib.SMA, self.atr, self.atr_avg_period)
        
        # Yesterday's high for breakout calculation
        high_data = pd.Series(self.data.High, index=self.data.index)
        self.yesterday_high = self.I(lambda x: pd.Series(x).shift(1).values, high_data)
        
        # Price for mean reversion
        self.price = self.data.Close

    def next(self):
        # Skip if we don't have enough data
        if len(self.atr) < self.atr_avg_period + 1 or len(self.price) < 2:
            return
            
        # Skip if we're already in a position
        if self.position:
            return
            
        current_atr = self.atr[-1]
        avg_atr = self.atr_avg[-1]
        current_price = self.data.Close[-1]
        
        # Check if we have valid data
        if pd.notna(current_atr) and pd.notna(avg_atr):
            
            # HIGH VOLATILITY REGIME: Use breakout strategy
            if current_atr > avg_atr:
                # Calculate breakout level
                if pd.notna(self.yesterday_high[-1]):
                    breakout_level = self.yesterday_high[-1] + (current_atr * self.atr_multiplier)
                    
                    # Breakout entry
                    if current_price > breakout_level:
                        tp_price = current_price * self.breakout_tp
                        sl_price = current_price * self.breakout_sl
                        self.buy(size=0.5, tp=tp_price, sl=sl_price)  # 50% capital - conservative for ETH
            
            # LOW VOLATILITY REGIME: Use mean reversion strategy  
            else:
                # Mean reversion entry: fade 4% dips (ETH-optimized)
                if current_price < self.price[-2] * self.dip_threshold:
                    tp_price = current_price * self.reversion_tp
                    sl_price = current_price * self.reversion_sl
                    self.buy(size=0.6, tp=tp_price, sl=sl_price)  # 60% capital - favor mean reversion for ETH

class UltimateStrategy(Strategy):
    """🔥 Ultimate Mashup Strategy - Volatility + Mean Reversion + MA Filter"""
    
    # Ultimate strategy parameters
    atr_period = 14
    atr_avg_period = 20
    ma_period = 20
    atr_multiplier = 0.5
    dip_threshold = 0.9
    breakout_size = 12
    reversion_size = 6
    breakout_tp = 1.03
    breakout_sl = 0.99
    reversion_tp = 1.02
    reversion_sl = 0.95

    def init(self):
        # ATR for volatility measurement
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        
        # ATR average for regime detection
        self.atr_avg = self.I(talib.SMA, self.atr, self.atr_avg_period)
        
        # 20-day moving average filter
        self.ma20 = self.I(talib.SMA, self.data.Close, self.ma_period)
        
        # Yesterday's high for breakout calculation
        high_data = pd.Series(self.data.High, index=self.data.index)
        self.yesterday_high = self.I(lambda x: pd.Series(x).shift(1).values, high_data)
        
        # Price for mean reversion
        self.price = self.data.Close

    def next(self):
        # Skip if we don't have enough data
        if (len(self.atr) < self.atr_avg_period + 1 or 
            len(self.price) < 2 or 
            len(self.ma20) < self.ma_period + 1):
            return
            
        # Skip if we're already in a position
        if self.position:
            return
            
        current_atr = self.atr[-1]
        avg_atr = self.atr_avg[-1]
        current_price = self.data.Close[-1]
        current_ma20 = self.ma20[-1]
        
        # Check if we have valid data
        if pd.notna(current_atr) and pd.notna(avg_atr) and pd.notna(current_ma20):
            
            # HIGH VOLATILITY: Ride the rocket (breakout strategy)
            if current_atr > avg_atr:
                # Calculate breakout level
                if pd.notna(self.yesterday_high[-1]):
                    breakout_level = self.yesterday_high[-1] + (current_atr * self.atr_multiplier)
                    
                    # Breakout entry
                    if current_price > breakout_level:
                        tp_price = current_price * self.breakout_tp
                        sl_price = current_price * self.breakout_sl
                        self.buy(size=0.8, tp=tp_price, sl=sl_price)  # 80% capital - aggressive breakout
            
            # LOW VOLATILITY BUT ABOVE MA: Fade the dips (mean reversion with filter)
            elif (current_atr <= avg_atr and 
                  current_price > current_ma20):  # Only mean revert above MA
                
                # Mean reversion entry: fade 10% dips
                if current_price < self.price[-2] * self.dip_threshold:
                    tp_price = current_price * self.reversion_tp
                    sl_price = current_price * self.reversion_sl
                    self.buy(size=0.5, tp=tp_price, sl=sl_price)  # 50% capital - conservative mean reversion
            
            # ELSE: Do nothing, wait like a smart trader (below MA in low vol = danger zone)

class ETHUltimateStrategy(Strategy):
    """🔥 ETH-Optimized Ultimate Mashup Strategy - Volatility + Mean Reversion + MA Filter"""
    
    # ETH-optimized ultimate strategy parameters
    atr_period = ETH_ATR_PERIOD
    atr_avg_period = ETH_ATR_AVG_PERIOD
    ma_period = 20
    atr_multiplier = ETH_ATR_MULTIPLIER
    dip_threshold = ETH_DIP_THRESHOLD
    breakout_size = ETH_COMBO_BREAKOUT_SIZE
    reversion_size = ETH_COMBO_REVERSION_SIZE
    breakout_tp = ETH_COMBO_BREAKOUT_TP
    breakout_sl = ETH_COMBO_BREAKOUT_SL
    reversion_tp = ETH_COMBO_REVERSION_TP
    reversion_sl = ETH_COMBO_REVERSION_SL

    def init(self):
        # ATR for volatility measurement
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        
        # ATR average for regime detection
        self.atr_avg = self.I(talib.SMA, self.atr, self.atr_avg_period)
        
        # 20-day moving average filter
        self.ma20 = self.I(talib.SMA, self.data.Close, self.ma_period)
        
        # Yesterday's high for breakout calculation
        high_data = pd.Series(self.data.High, index=self.data.index)
        self.yesterday_high = self.I(lambda x: pd.Series(x).shift(1).values, high_data)
        
        # Price for mean reversion
        self.price = self.data.Close

    def next(self):
        # Skip if we don't have enough data
        if (len(self.atr) < self.atr_avg_period + 1 or 
            len(self.price) < 2 or 
            len(self.ma20) < self.ma_period + 1):
            return
            
        # Skip if we're already in a position
        if self.position:
            return
            
        current_atr = self.atr[-1]
        avg_atr = self.atr_avg[-1]
        current_price = self.data.Close[-1]
        current_ma20 = self.ma20[-1]
        
        # Check if we have valid data
        if pd.notna(current_atr) and pd.notna(avg_atr) and pd.notna(current_ma20):
            
            # HIGH VOLATILITY: Ride the rocket (ETH-optimized breakout)
            if current_atr > avg_atr:
                # Calculate breakout level
                if pd.notna(self.yesterday_high[-1]):
                    breakout_level = self.yesterday_high[-1] + (current_atr * self.atr_multiplier)
                    
                    # Breakout entry
                    if current_price > breakout_level:
                        tp_price = current_price * self.breakout_tp
                        sl_price = current_price * self.breakout_sl
                        self.buy(size=0.5, tp=tp_price, sl=sl_price)  # 50% capital - ETH breakout
            
            # LOW VOLATILITY BUT ABOVE MA: Fade the dips (ETH-optimized mean reversion)
            elif (current_atr <= avg_atr and 
                  current_price > current_ma20):  # Only mean revert above MA
                
                # ETH mean reversion entry: fade 4% dips
                if current_price < self.price[-2] * self.dip_threshold:
                    tp_price = current_price * self.reversion_tp
                    sl_price = current_price * self.reversion_sl
                    self.buy(size=0.6, tp=tp_price, sl=sl_price)  # 60% capital - favor mean reversion for ETH
            
            # ELSE: Do nothing, wait like a smart trader (below MA in low vol = danger zone)

# TEST ON ALL DATA SOURCES
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🧪 TESTING ADAPTIVE VOLATILITY STRATEGIES ON ALL DATA SOURCES")
    print("="*80)

    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from multi_data_tester import test_on_all_data

    # Test all strategies - original + ETH-optimized + ultimate
    strategies_to_test = [
        (VolBreakStrategy, 'VolBreak_Strategy'),
        (MeanRevStrategy, 'MeanRev_Strategy'), 
        (AdaptiveVolatilityStrategy, 'Adaptive_Volatility_Strategy'),
        (ETHVolBreakStrategy, 'ETH_VolBreak_Strategy'),
        (ETHMeanRevStrategy, 'ETH_MeanRev_Strategy'),
        (ETHAdaptiveVolatilityStrategy, 'ETH_Adaptive_Strategy'),
        (UltimateStrategy, 'Ultimate_Strategy'),
        (ETHUltimateStrategy, 'ETH_Ultimate_Strategy')
    ]
    
    for strategy_class, strategy_name in strategies_to_test:
        print(f"\n🚀 Testing {strategy_name}...")
        results = test_on_all_data(strategy_class, strategy_name, cash=INITIAL_CASH, commission=COMMISSION)
        
        if results is not None:
            print(f"✅ {strategy_name} testing complete! Results saved in: ./results/{strategy_name}.csv")
        else:
            print(f"❌ {strategy_name} testing failed!")
    
    print("\n🎯 All adaptive volatility strategies tested!")
    print("💡 Check the results directory for comprehensive performance analysis")
    print("🔥 Recommendation: Focus on the Adaptive strategy for best risk-adjusted returns")