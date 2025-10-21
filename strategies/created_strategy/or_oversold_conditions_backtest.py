To implement the given Moving Average Crossover with RSI Filter trading strategy using the backtesting.py framework, you would write a script as follows:

```python
from backtesting import Strategy, Backtest
from backtesting.lib import crossover
from backtesting.test import SMA, GOOG

# Import additional necessary packages
import pandas as pd
import numpy as np

# Define the strategy class inheriting from Strategy
class SmaCrossRsi(Strategy):
    # Define the parameters (by default: SMA 50, SMA 200, RSI Period 14)
    sma_short = 50
    sma_long = 200
    rsi_period = 14
    
    def init(self):
        # Precompute the two simple moving averages
        self.sma1 = self.I(SMA, self.data.Close, self.sma_short)
        self.sma2 = self.I(SMA, self.data.Close, self.sma_long)
        
        # Compute Relative Strength Index (RSI)
        delta = self.data.Close.diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0
        down[down > 0] = 0
        
        # Calculate the Exponential Moving Average (EMA) of up and down moves
        roll_up = up.rolling(self.rsi_period).mean()
        roll_down = down.abs().rolling(self.rsi_period).mean()

        roll_up_ewm = up.ewm(span=self.rsi_period).mean()
        roll_down_ewm = down.abs().ewm(span=self.rsi_period).mean()
        
        RS = roll_up_ewm / roll_down_ewm
        self.rsi = 100.0 - (100.0 / (1.0 + RS))
    
    def next(self):
        # If the short SMA crosses above the long SMA
        if crossover(self.sma1, self.sma2):
            # Check if RSI is between 30 and 70
            if 30 < self.rsi[-1] < 70:
                # Buy at the opening of next trading day
                self.buy()
        
        # If the short SMA crosses below the long SMA
        elif crossover(self.sma2, self.sma1):
            # Check if RSI is between 30 and 70
            if 30 < self.rsi[-1] < 70:
                # Sell at the opening of next trading day
                self.sell()

# Example data loading and running the backtest
if __name__ == '__main__':
    # Load some data, e.g., Google stock data included in backtesting.py
    data = GOOG.copy()
    
    # Instantiate the backtest with your data and strategy
    bt = Backtest(data, SmaCrossRsi, cash=10000, commission=.002, exclusive_orders=True)
    
    # Run the backtest
    output = bt.run()

    # Optionally plot the trades and equity curve
    bt.plot()
```

Note that the Google data (`GOOG`) provided by `backtesting.test` is just for example purposes. In practice, you should replace the data source with the actual historical daily price data for the financial instrument you want to backtest the strategy on.

This backtest code is made to represent the strategy described. However, this code will not run in a restricted environment where external libraries such as `backtesting.py` are not installed. You'd need to install the `backtesting.py` package (`pip install backtesting`) and ensure that it can access the necessary data and run in your local environment.