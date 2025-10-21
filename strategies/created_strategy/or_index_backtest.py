Below is the backtest code of the provided trading strategy using the backtesting.py library in Python. It includes the entry and exit criteria, position sizing, and risk management as described.

```python
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import SMA, GOOG # Replace GOOG with the actual data
import pandas as pd
import numpy as np

# Set up the strategy
class MeanReversionBollingerBands(Strategy):
    n1 = 20  # SMA window
    n_dev = 2  # Number of standard deviations for Bollinger Bands
    stop_loss_percent = 0.03  # Stop-loss percentage
    risk_per_trade = 0.01  # Risk per trade
    
    def init(self):
        # Initialize indicators
        close = self.data.Close
        self.sma = self.I(SMA, close, self.n1)
        self.upper_band = self.I(lambda s: s + self.n_dev * np.std(s[-self.n1:]), self.sma)
        self.lower_band = self.I(lambda s: s - self.n_dev * np.std(s[-self.n1:]), self.sma)
        
        # Variables to manage stop-loss and time exit
        self.stop_loss = pd.Series(np.nan, index=self.data.index)
        self.entry_price = 0
        self.entry_time = None
        
    def next(self):
        # Get current position and bar
        current_position = self.position
        last_bar = len(self.data) - 1
        
        # Check conditions for entry
        if not current_position:
            if self.data.Close[-1] < self.lower_band[-1]:
                # Calculate position size and stop-loss
                risk_amount = self.broker.cash * self.risk_per_trade
                distance_to_stop = self.data.Close[-1] * (1 - self.stop_loss_percent)
                position_size = risk_amount / distance_to_stop
                # Enter long
                self.buy(size=position_size)
                self.entry_price = self.data.Open[-1]
                self.entry_time = last_bar
                self.stop_loss[-1] = self.entry_price * (1 - self.stop_loss_percent)
                
            elif self.data.Close[-1] > self.upper_band[-1]:
                # Calculate position size and stop-loss
                risk_amount = self.broker.cash * self.risk_per_trade
                distance_to_stop = self.data.Close[-1] * (1 + self.stop_loss_percent)
                position_size = risk_amount / distance_to_stop
                # Enter short
                self.sell(size=position_size)
                self.entry_price = self.data.Open[-1]
                self.entry_time = last_bar
                self.stop_loss[-1] = self.entry_price * (1 + self.stop_loss_percent)
        
        # Update stop-loss for open position based on the closing price
        elif current_position.is_long:
            self.stop_loss[-1] = self.data.Close[-1] * (1 - self.stop_loss_percent)
            # Exit conditions for long position
            if (self.data.Close[-1] >= self.sma[-1] or
                self.data.Close[-1] <= self.stop_loss[-2] or
                last_bar - self.entry_time >= 10):
                self.position.close()
        elif current_position.is_short:
            self.stop_loss[-1] = self.data.Close[-1] * (1 + self.stop_loss_percent)
            # Exit conditions for short position
            if (self.data.Close[-1] <= self.sma[-1] or
                self.data.Close[-1] >= self.stop_loss[-2] or
                last_bar - self.entry_time >= 10):
                self.position.close()

# Backtest settings

backtest = Backtest(GOOG, MeanReversionBollingerBands, cash=100000, commission=.01, trade_on_close=True)
stats = backtest.run()
print(stats)
backtest.plot()
```

Note that you will need actual stock/index OHLC data instead of the placeholder `GOOG`, and you should customize the details (e.g., commission, slippage) according to the requirements of the strategy. Additionally, ensure that `backtesting.py` is installed in your Python environment and that you have the historical daily price data for the stock/index you want to backtest.