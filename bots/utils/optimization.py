import pandas as pd
from backtesting import Strategy, Backtest
from backtesting.test import SMA
import warnings
import matplotlib.pyplot as plt
import seaborn as sns

# Hide all warnings
warnings.filterwarnings("ignore")

# Load daily data
daily_data_path = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
daily_data = pd.read_csv(daily_data_path, parse_dates = ['timestamp'])

# Load hourly data
hourly_data_path = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
hourly_data = pd.read_csv(hourly_data_path, parse_dates = ['timestamp'])

# Ensure the columns are correctly loaded
print(daily_data.columns)
print(hourly_data.columns)

# Set indices for daily and hourly data
daily_data.set_index('timestamp', inplace = True)
hourly_data.set_index('timestamp', inplace = True)

# Rename columns to match expected format for backtesting.py
hourly_data.rename(columns = {
    'open': 'Open',
    'high': 'High',
    'low': 'Low',
    'close': 'Close',
    'volume': 'Volume'
}, inplace = True)

class BreakoutStrategy(Strategy):
    tp_percent = 7 # Default take profit at 20%
    sl_percent = 16 # Default stop loss at 19%

    def init(self):
        self.daily_resistance = daily_data['resis']

    def next(self):
        # get the most recent daily resistance level for the current timestamp
        current_time = self.data.index[-1]
        daily_resistance = self.daily_resistance[self.daily_resistance.index <= current_time].iloc[-1]
        current_close = self.data.Close[-1]

        # Check for breakout on the hourly data
        if current_close > daily_resistance:
            entry_price = current_close # Use current close price as entry price
            stop_loss = entry_price * (1 - self.sl_percent / 100) # Stop loss based on sl_percent
            take_profit = entry_price * (1 + self.tp_percent / 100) # Take profit based on tp_percent

            # Check if SL < entry price < TP
            if stop_loss < entry_price < take_profit:
                self.buy(sl = stop_loss, tp = take_profit)

# Ensure the renamed data is correct
print(hourly_data.head())

# Run the backtest
bt = Backtest(hourly_data, BreakoutStrategy, cash = 100000, commission = 0.002)

# Optimize the strategy
opt_stats, heatmap = bt.optimize(
    tp_percent = range(3, 21, 1), # Test take profit levels from 3% to 20%
    sl_percent = range(3, 21, 1), # Test stop loss levels from 3% to 20%
    maximize = ('Equity Final [$]'),
    method = 'grid',
    return_heatmap = True
)

# Print the optimization results
print(opt_stats)

# Convert heatmap to a 2D DataFrame for plotting
heatmap_df = heatmap.unstack(level = 'tp_percent').T

# Plot the heatmap for the optimization results
plt.figure(figsize = (10,8))
sns.heatmap(heatmap_df, annot = True, fmt = " .2f", cmap = 'viridis')
plt.title("Optimization Heatmap")
plt.xlabel("Stop Loss (%)")
plt.ylabel("Take Profit (%)")
plt.show()

# Run the backtest with the best parameters
results = bt.run(tp_percent = opt_stats.tp_percent, sl_percent = opt_stats.sl_percent)
print(results)

# Plot the performance
bt.plot()
