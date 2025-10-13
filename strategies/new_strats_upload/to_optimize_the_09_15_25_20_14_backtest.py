Below is the backtest code for the Dual Moving Average Crossover Strategy with Risk Management using `backtesting.py` library:

```python
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import GOOG  # Replace with your own historical data source
import pandas as pd

class DualMovingAverageCrossover(Strategy):
    n1 = 50
    n2 = 200
    
    def init(self):
        self.sma1 = self.I(pd.Series.rolling, self.data.Close, self.n1).mean()
        self.sma2 = self.I(pd.Series.rolling, self.data.Close, self.n2).mean()
        self.set_atr = self.I(pd.Series.rolling, self.data.High - self.data.Low, self.n1).mean()
        
    def next(self):
        # Define the position size and risk management parameters
        max_risk = 0.01  # Maximum risk per trade (1%)
        account_balance = self.broker.cash
        stop_loss_atr_multiplier = 2

        if crossover(self.sma1, self.sma2):
            # Calculate position size based on stop loss and max risk
            atr = self.set_atr[-1]
            stop_loss = atr * stop_loss_atr_multiplier
            position_size = account_balance * max_risk / stop_loss
            self.buy(size=position_size)
            
        elif crossover(self.sma2, self.sma1):
            # Sell conditions, closing all positions
            for trade in self.trades:
                self.sell(trade.size)
      
# Prepare and configure the backtest
bt = Backtest(GOOG,  # Replace with your data
              DualMovingAverageCrossover,
              cash=10000,  # Starting capital
              commission=.002,
              exclusive_orders=True)
           
# Run the backtest
stats = bt.run()

# Print out the strategy statistics
print(stats)

# Plot the strategy
bt.plot()
```

In this code, `GOOG` is used as a placeholder. You would have to replace the `GOOG` variable with your own historical data source, preferably as a Pandas DataFrame that contains the OHLC (Open, High, Low, and Close) data.

The `DualMovingAverageCrossover` class inherits from `Strategy` and implements the logic for the strategy, including setup (`init` method), trade execution (`next` method), and position sizing based on the maximum allowable risk.

After defining the strategy, we instantiate a `Backtest` object, passing in the data, strategy, initial capital, and commission for trades. We run the backtest and then output the statistics to review its performance. The `bt.plot()` call will generate a plot for visual analysis.

Before running this code, make sure you have `backtesting.py` installed and that you have a historical data set in the format expected by the backtesting library. This data set needs to contain daily price data for at least five years to comply with the strategy's requirement.