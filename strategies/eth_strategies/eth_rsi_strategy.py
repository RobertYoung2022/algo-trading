# eth_rsi_strategy.py
import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy

print("💫 Bobby's ETH RSI Strategy Loading... 🌙")

# Strategy parameters
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70
TAKE_PROFIT_PERCENT = 5.0
STOP_LOSS_PERCENT = 3.0

class ETHRSIStrategy(Strategy):
    # Strategy parameters
    rsi_period = RSI_PERIOD
    rsi_oversold = RSI_OVERSOLD
    rsi_overbought = RSI_OVERBOUGHT
    take_profit = TAKE_PROFIT_PERCENT / 100
    stop_loss = STOP_LOSS_PERCENT / 100

    def init(self):
        # Initialize RSI indicator
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)

    def next(self):
        # Only trade if we have enough data for RSI
        if len(self.rsi) < self.rsi_period:
            return
            
        current_rsi = self.rsi[-1]
        
        # Entry conditions
        if not self.position:
            # Buy when RSI is oversold (price might bounce up)
            if current_rsi < self.rsi_oversold:
                current_price = self.data.Close[-1]
                sl_price = current_price * (1 - self.stop_loss)
                tp_price = current_price * (1 + self.take_profit)
                self.buy(sl=sl_price, tp=tp_price)
        
        # Exit conditions
        else:
            # Sell when RSI is overbought (price might drop)
            if current_rsi > self.rsi_overbought:
                self.sell()

# TEST ON ALL DATA SOURCES
if __name__ == "__main__":
    print("\n" + "="*80)
    print("TESTING ETH RSI STRATEGY ON ALL DATA SOURCES")
    print("="*80)

    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from multi_data_tester import test_on_all_data

    # Test this strategy on all configured data sources
    results = test_on_all_data(ETHRSIStrategy, 'ETH_RSI_Strategy')

    if results is not None:
        print("\n✅ Testing complete! Results saved in: ./results/ETH_RSI_Strategy.csv")