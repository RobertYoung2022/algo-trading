```python
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import pandas_ta as ta

class MovingAverageCrossoverStrategy(Strategy):
    n1 = 50
    n2 = 200
    
    def init(self):
        close = self.data.Close
        self.sma1 = self.I(ta.sma, close, self.n1)
        self.sma2 = self.I(ta.sma, close, self.n2)

    def next(self):
        if not self.position:
            if crossover(self.sma1, self.sma2):
                self.buy(slippage=0.01, size=self.broker.cash * 0.01 / self.data.Open[1])
        else:
            if crossover(self.sma2, self.sma1):
                self.position.close(slippage=0.01)

# Example usage:
# import pandas as pd

# Load 5 years of historical data into a DataFrame 'data'
# data = pd.read_csv('path_to_historical_data.csv', index_col=0, parse_dates=True)

# bt = Backtest(data, MovingAverageCrossoverStrategy,
#               cash=100000, commission=.002,
#               exclusive_orders=True)

# output = bt.run()
# bt.plot()
```

This backtest code uses the `backtesting.py` library to implement the Moving Average Crossover Strategy with the detailed requirements specified in the strategy instructions provided. The code does not include data loading, and assumes that historical data in a DataFrame named 'data' is available.

Please note that the slippage parameter can be a fixed value in the `self.buy()` and `self.position.close()` methods to account for potential slippage. The example transaction fee is set to 0.2%, so adjust it to your actual needs.

The code is structured such that it can be easily extended or modified to include other features or alter the strategy parameters. Also, backtesting results and plots can be obtained by uncommenting the last few lines and providing the correct path to the historical CSV data file.