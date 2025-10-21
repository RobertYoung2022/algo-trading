"""
Quick debug test for TEMS strategy
"""

import pandas as pd
from backtesting import Backtest
from debug_tems_strategy import DebugTEMSStrategy


def main():
    # Load ETH 6h data
    file_path = "/Users/bobbyyo/Projects/algo-fun/data/coinbase/ETHUSD-6h-200wks-enhanced-data.csv"

    # Load data
    df = pd.read_csv(file_path)

    # Handle datetime column
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    # Standardize column names
    df.columns = [col.capitalize() for col in df.columns]

    print(f"Data loaded: {len(df)} bars")
    print(f"Date range: {df.index[0]} to {df.index[-1]}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"\nFirst few rows:")
    print(df.head())

    # Run backtest with debug strategy
    bt = Backtest(
        df,
        DebugTEMSStrategy,
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    print("\n" + "="*80)
    print("Running DEBUG TEMS Strategy...")
    print("="*80)

    stats = bt.run()

    print("\n" + "="*80)
    print("DEBUG TEMS RESULTS")
    print("="*80)
    print(stats)

    # Show plot
    try:
        bt.plot()
    except:
        pass


if __name__ == "__main__":
    main()