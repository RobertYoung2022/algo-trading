"""
Multi-Data Testing Framework for Backtesting-py Strategies
=========================================================
This module provides a unified testing framework for running backtesting strategies on multiple data sources with different formats (Coinbase, Interactive Brokers, Yahoo).
Usage:
    from multi_data_tester import test_on_all_data
    results = test_on_all_data(MyStrategy, 'MyStrategyName')
"""

import pandas as pd 
import numpy as np 
from backtesting import Backtest 
import os 
import warnings
warnings. filterwarnings ('ignore')

# ============================================================
# DATA SOURCE CONFIGURATION
# ============================================================
# Format: (name, path, data_type)
# data_type can be: 'coinbase', 'ib', or 'yahoo'

DATA_SOURCES = [
# BITCOIN DATA - Available datasets
('BTC-1d-1000wks', '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1d-1000wks-data.csv', 'coinbase'),
('BTC-1h-500wks', '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-1h-500wks-data.csv', 'coinbase'),
('BTC-6h-500wks', '/Users/bobbyyo/Projects/algo-fun/data/BTCUSD-6h-500wks-data.csv', 'coinbase'),

# ETHEREUM DATA - Hyperliquid datasets
('ETH-1d-5yrs-old', '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv', 'coinbase'),
('ETH-1d-5yrs-new', '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250911_043600_historical.csv', 'coinbase'),
('ETH-1h-7mo-old', '/Users/bobbyyo/Projects/algo-fun/data/ETH_1h_20250911_043633_historical.csv', 'coinbase'),
('ETH-1h-7mo-new', '/Users/bobbyyo/Projects/algo-fun/data/hyperliquid/ETH-USD-1h-hyperliquid-data.csv', 'coinbase'),

# COINGECKO DATA - Available datasets (Updated paths)
('ETH-CoinGecko-90d', '/Users/bobbyyo/Projects/algo-fun/data/coingecko/ETHEREUMUSD-90d-coingecko-data.csv', 'coinbase'),

# COINMARKETCAP DATA - Available datasets  
('ETH-CMC-30pts', '/Users/bobbyyo/Projects/algo-fun/data/coinmarketcap/ETHUSD-daily-30pts-cmc-data.csv', 'coinbase'),

# CRYPTOCOMPARE DATA - Available datasets
('ETH-CC-100d', '/Users/bobbyyo/Projects/algo-fun/data/cryptocompare/ETHUSDT-day-100pts-cc-data.csv', 'coinbase'),
]

# ============================================================
# DATA LOADING FUNCTION - Handles different formats
# ============================================================

def load_data_universal(path, data_type):
    """
    Load data from different sources with different formats

    Args:
        path: Path to CSV file 
        data_type: 'coinbase', 'ib', or 'yahoo'

    Returns: 
        DataFrame with standardized columns: Open, High, Low, Close, Volume
    """
    try:    
        if data_type == 'coinbase':
            # First, read the CSV to see what columns it has
            data = pd.read_csv(path)
            
            # Find the date column (could be 'datetime', 'timestamp', 'date', etc.)
            date_cols = [col for col in data.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
            
            if date_cols:
                date_col = date_cols[0]  # Use the first date column found
                data = pd.read_csv(path, parse_dates=[date_col], index_col=date_col)
            else:
                # Fallback to first column
                data = pd.read_csv(path, parse_dates=[data.columns[0]], index_col=data.columns[0])
            
            # Standardize column names
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        elif data_type == 'ib':
            # IB format: date, open, high, low, close, volume
            data = pd.read_csv(path, parse_dates=['date'], index_col='date')
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        elif data_type == 'yahoo':
            # Yahoo format: datetime, open, high, low, close, volume
            data = pd.read_csv(path, parse_dates=['Datetime'], index_col='Datetime')
            # Drop Adj Close and keep standard columns
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

        else:
            # Default fallback - try to guess format
            data = pd.read_csv(path)
            # Try to find date column
            data_cols = [col for col in data.columns if 'date' in col.lower() or 'time' in col.lower()]
            if data_cols:
                data = pd.read_csv(path, parse_dates=[data_cols[0]], index_col=data_cols[0])
            # Standard column names
            col_map = {}
            for col in data.columns:
                col_lower = col.lower()
                if 'open' in col_lower:
                    col_map[col] = 'Open'
                elif 'high' in col_lower:
                    col_map[col] = 'High'
                elif 'low' in col_lower:
                    col_map[col] = 'Low'
                elif 'close' in col_lower and 'adj' not in col_lower:
                    col_map[col] = 'Close'
                elif 'volume' in col_lower:
                    col_map[col] = 'Volume'
            data = data.rename(columns=col_map)
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

        # Clean and sort data
        data = data.sort_index()
        data = data.dropna()

        # Ensure Volume is float type for talib compatilbility
        if 'Volume' in data.columns:
            data['Volume'] = data['Volume'].astype(float)

        # Ensure all price columns are float
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in data.columns:
                data[col] = data[col].astype(float)

        return data

    except Exception as e:
        print(f"Error loading data: {e}")
        return None


# ============================================================
# MAIN TESTING FUNCTION
# ============================================================

def test_on_all_data(strategy_class, strategy_name=None, optimize=False, cash=1000000, commission=0.001, verbose=True):
    """
    Test a strategy on all configured data sources and save results to CSV
    
    Args:
        strategy_class: The strategy class to test (must inherit from backtesting. Strategy) strategy_name: Name for the output file (defaults to strategy class name) optimize: Whether to run optimization (default False for speed) cash: Starting cash (default 10000000 for Bitcoin compatibility) commission:
        Commission rate (default 0.00045)
        verbose: Whether to print detailed output (default True)
        Returns:
        DataFrame with all results
        Example:
        from multi data tester import test on all data
        results = test_on_all_data(MyStrategy, 'MyStrategyTest ')
    """

    if strategy_name is None:
        strategy_name = strategy_class.__name__


    # Create results directory in the same folder as the calling script
    # Get the directory of the file that's calling this function
    import inspect
    caller_frame = inspect.stack()[1]
    caller_file = caller_frame.filename
    caller_dir = os.path.dirname(os.path.abspath(caller_file))

    # Create results directory in the caller's directory
    results_dir = os.path.join(caller_dir, 'results')
    os.makedirs(results_dir, exist_ok=True)

    all_results = []

    if verbose:
        print(f"\n{'='*80}")
        print(f"💫 Testing {strategy_name} on {len(DATA_SOURCES)} data sources")
        print(f"{'='*80}\n")

    for i, source in enumerate(DATA_SOURCES, 1):
        # Handle both old format (name, path) and new format (name, path, type)
        if len(source) == 3:
            name, path, data_type = source
        else:
            name, path = source
            data_type = 'coinbase' # Default for backward compatibility

        if verbose:
            print(f"[{i}/{len(DATA_SOURCES)}] {name}...", end=' ')

        # Load data with appropriate format handler
        data = load_data_universal(path, data_type)
        if data is None:
            if verbose:
                print(f"❌ Failed to load data")
            continue

        # Run backtest
        try:
            bt = Backtest(data, strategy_class, cash=cash, commission=commission)
            stats = bt.run()

            # Print full stats grid like backtesting.py does
            if verbose:
                print(f"\n" + "="*60)
                print(f"🔍 Full Stats for {name}:")
                print("="*60)
                print(stats)
                print("="*60)

            # Extract ALL stats for CSV (comprehensive list)
            results = {
                'Data_Source': name,
                'Rows': len(data),
                'Start': str(stats['Start']) if 'Start' in stats else '',
                'End': str(stats['End']) if 'End' in stats else '',
                'Duration': str(stats['Duration']) if 'Duration' in stats else '',
                'Exposure_Time_%': round(stats['Exposure Time [%]'], 2) if 'Exposure Time [%]' in stats else 0,
                'Equity_Final': round(stats['Equity Final [$]'], 2) if 'Equity Final [$]' in stats else 0,
                'Equity_Peak': round(stats['Equity Peak [$]'], 2) if 'Equity Peak [$]' in stats else 0,
                'Return_%': round(stats['Return [%]'], 2) if 'Return [%]' in stats else 0,
                'Buy_Hold_%': round(stats['Buy & Hold Return [%]'], 2) if 'Buy & Hold Return [%]' in stats else 0,
                'Return_Ann_%': round(stats['Return (Ann.) [%]'], 2) if 'Return (Ann.) [%]' in stats else 0,
                'Volatility_Ann_%': round(stats['Volatility (Ann.) [%]'], 2) if 'Volatility (Ann.) [%]' in stats else 0,
                'Sharpe': round(stats['Sharpe Ratio'], 3) if 'Sharpe Ratio' in stats else 0,
                'Sortino': round(stats['Sortino Ratio'], 3) if 'Sortino Ratio' in stats else 0,
                'Calmar': round(stats['Calmar Ratio'], 3) if 'Calmar Ratio' in stats else 0,
                'Max_DD_%': round(stats['Max. Drawdown [%]'], 2) if 'Max. Drawdown [%]' in stats else 0,
                'Avg_DD_%': round(stats['Avg. Drawdown [%]'], 2) if 'Avg. Drawdown [%]' in stats else 0,
                'Max_DD_Duration': str(stats['Max. Drawdown Duration']) if 'Max. Drawdown Duration' in stats else '',
                'Avg_DD_Duration': str(stats['Avg. Drawdown Duration']) if 'Avg. Drawdown Duration' in stats else '',
                'Trades': stats['# Trades'] if '# Trades' in stats else 0,
                'Win_Rate_%': round(stats['Win Rate [%]'], 2) if 'Win Rate [%]' in stats else 0,
                'Best_Trade_%': round(stats['Best Trade [%]'], 2) if 'Best Trade [%]' in stats else 0,
                'Worst_Trade_%': round(stats['Worst Trade [%]'], 2) if 'Worst Trade [%]' in stats else 0,
                'Avg_Trade_%': round(stats['Avg. Trade [%]'], 3) if 'Avg. Trade [%]' in stats else 0,
                'Max_Trade_Duration': str(stats['Max. Trade Duration']) if 'Max. Trade Duration' in stats else 0,
                'Avg_Trade_Duration': str(stats['Avg. Trade Duration']) if 'Avg. Trade Duration' in stats else 0,
                'Profit_Factor': round(stats['Profit Factor'], 3) if 'Profit Factor' in stats else 0,
                'Expectancy': round(stats['Expectancy'], 3) if 'Expectancy' in stats else 0,
                'SQN': round(stats['SQN'], 3) if 'SQN' in stats else 0,
            }
                

            # Add optimiztation results if requested
            if optimize:
                try:
                    opt_stats = bt.optimize(
                        maximize = 'Return [%]',
                        max_tries = 100,
                        random_state = 42,
                    )
                    results.update({
                        'Opt_Return_%': round(opt_stats['Return [%]'], 2) if 'Return [%]' in opt_stats else 0,
                        'Opt_Sharpe': round(opt_stats['Sharpe Ratio'], 3) if 'Sharpe Ratio' in opt_stats else 0,
                        'Opt_Trades': opt_stats['# Trades'] if '# Trades' in opt_stats else 0,
                        'Opt_Win_Rate_%': round(opt_stats['Win Rate [%]'], 2) if 'Win Rate [%]' in opt_stats else 0,
                    })
                except:
                    pass

            all_results.append(results)


        except Exception as e:
            if verbose:
                print(f"❌ Error running backtest: {e}")
            continue

    # Convert results list to DataFrame
    results_df = pd.DataFrame(all_results)

    # Save results to CSV (one CSV per strategy, replaces on each run)
    if all_results:
        df = pd.DataFrame(all_results)
        # Use consistent filename (no timestamp) so it replaces on each run
        csv_path = os.path.join(results_dir, f"{strategy_name}.csv")
        df.to_csv(csv_path, index=False)

        if verbose:
            print(f"\n{'='*80}")
            print(f"📊 Results saved to: {csv_path}")
            print(f"{'='*80}\n")

            # Print summary with key metrics
            print("SUMMARY:")
            print(df[['Data_Source', 'Return_%', 'Buy_Hold_%', 'Sharpe', 'Sortino', 'Max_DD_%', 'Profit_Factor', 'Expectancy', 'Trades', 'Win_Rate_%']].to_string())

        return df

    else:
        if verbose:
            print(f"❌ No results to save")
        return None

