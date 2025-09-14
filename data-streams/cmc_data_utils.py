"""
CMC Data Utils - Helper functions for loading and analyzing daily CMC data files
===============================================================================
This module provides utilities for working with daily CMC data files created by
the cmc_real_time_monitor.py script.

Usage Examples:
    # Load today's watchlist data
    df = load_cmc_data('watchlist')

    # Load last 7 days of global metrics
    df = load_cmc_data('global', days_back=7)

    # Load specific date range
    df = load_cmc_data('top_coins', start_date='2024-01-15', end_date='2024-01-20')

    # Get summary of available data
    summary = get_data_summary()
"""

import pandas as pd
import datetime
import os
import glob
from pathlib import Path
from typing import Optional, Union, List, Dict
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default data directory (relative to this script)
DEFAULT_DATA_DIR = '../data/cmc_monitor'

# Available data types
VALID_DATA_TYPES = [
    'global',           # Global market metrics
    'top_coins',        # Top cryptocurrencies by market cap
    'watchlist',        # Personal watchlist data
    'fear_greed',       # Fear & Greed Index
    'market_sentiment', # Market sentiment analysis
    'social_sentiment'  # Social media sentiment
]

def load_cmc_data(
    data_type: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: Optional[int] = None,
    data_dir: Optional[str] = None
) -> pd.DataFrame:
    """
    Load CMC data from daily files

    Args:
        data_type: Type of data ('global', 'top_coins', 'watchlist', etc.)
        start_date: Start date in 'YYYY-MM-DD' format (optional)
        end_date: End date in 'YYYY-MM-DD' format (optional)
        days_back: Number of days back from today (optional)
        data_dir: Custom data directory path (optional)

    Returns:
        pandas.DataFrame with combined data from specified date range

    Examples:
        # Load today's data
        df = load_cmc_data('watchlist')

        # Load last week
        df = load_cmc_data('global', days_back=7)

        # Load specific range
        df = load_cmc_data('top_coins', start_date='2024-01-15', end_date='2024-01-20')
    """
    if data_type not in VALID_DATA_TYPES:
        raise ValueError(f"Invalid data_type. Must be one of: {VALID_DATA_TYPES}")

    data_dir = data_dir or DEFAULT_DATA_DIR

    # Determine date range
    if days_back:
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=days_back-1)
    elif start_date and end_date:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    elif start_date:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = start_date
    elif end_date:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        start_date = end_date
    else:
        # Default to today only
        start_date = end_date = datetime.date.today()

    # Collect files for date range
    files_to_load = []
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')

        # Different filename patterns based on data type
        if data_type == 'global':
            filename = f"{data_dir}/global_metrics_{date_str}.csv"
        else:
            filename = f"{data_dir}/{data_type}_{date_str}.csv"

        if os.path.exists(filename):
            files_to_load.append(filename)
            logger.debug(f"Found data file: {filename}")
        else:
            logger.debug(f"No data file for {current_date}: {filename}")

        current_date += datetime.timedelta(days=1)

    if not files_to_load:
        logger.warning(f"No {data_type} data files found for date range {start_date} to {end_date}")
        return pd.DataFrame()

    # Load and combine files
    try:
        dataframes = []
        for file_path in files_to_load:
            df = pd.read_csv(file_path)
            logger.debug(f"Loaded {len(df)} records from {os.path.basename(file_path)}")
            dataframes.append(df)

        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Loaded {len(combined_df)} total records for {data_type} from {len(files_to_load)} files")

        # Add date column if timestamp exists
        if 'timestamp' in combined_df.columns:
            combined_df['date'] = pd.to_datetime(combined_df['timestamp']).dt.date

        # Sort by timestamp if available
        if 'timestamp' in combined_df.columns:
            combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)

        return combined_df

    except Exception as e:
        logger.error(f"Error loading {data_type} data: {e}")
        return pd.DataFrame()

def get_latest_data(data_type: str, data_dir: Optional[str] = None) -> pd.DataFrame:
    """
    Get the most recent data for a specific type

    Args:
        data_type: Type of data to load
        data_dir: Custom data directory path (optional)

    Returns:
        pandas.DataFrame with today's data
    """
    return load_cmc_data(data_type, days_back=1, data_dir=data_dir)

def get_data_summary(data_dir: Optional[str] = None) -> Dict:
    """
    Get summary of available CMC data files

    Args:
        data_dir: Custom data directory path (optional)

    Returns:
        Dictionary with summary statistics for each data type
    """
    data_dir = data_dir or DEFAULT_DATA_DIR

    if not os.path.exists(data_dir):
        logger.warning(f"Data directory does not exist: {data_dir}")
        return {}

    summary = {}

    for data_type in VALID_DATA_TYPES:
        # Find all files for this data type
        if data_type == 'global':
            pattern = f"{data_dir}/global_metrics_*.csv"
        else:
            pattern = f"{data_dir}/{data_type}_*.csv"

        files = glob.glob(pattern)

        if files:
            # Extract dates from filenames
            dates = []
            total_records = 0

            for file_path in files:
                try:
                    filename = os.path.basename(file_path)
                    # Extract date from filename (last 8 characters before .csv)
                    date_str = filename.split('_')[-1].replace('.csv', '')
                    if len(date_str) == 8 and date_str.isdigit():
                        date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
                        dates.append(date)

                        # Count records in file
                        df = pd.read_csv(file_path)
                        total_records += len(df)
                except Exception as e:
                    logger.debug(f"Error processing file {file_path}: {e}")

            if dates:
                summary[data_type] = {
                    'files_count': len(files),
                    'total_records': total_records,
                    'date_range': {
                        'start': min(dates).strftime('%Y-%m-%d'),
                        'end': max(dates).strftime('%Y-%m-%d')
                    },
                    'latest_file': max(files, key=os.path.getmtime),
                    'total_size_mb': sum(os.path.getsize(f) for f in files) / (1024*1024)
                }

    return summary

def analyze_watchlist_performance(days_back: int = 7, data_dir: Optional[str] = None) -> Dict:
    """
    Analyze watchlist performance over specified period

    Args:
        days_back: Number of days to analyze
        data_dir: Custom data directory path (optional)

    Returns:
        Dictionary with performance analysis for each coin
    """
    df = load_cmc_data('watchlist', days_back=days_back, data_dir=data_dir)

    if df.empty:
        logger.warning("No watchlist data available for analysis")
        return {}

    # Group by symbol and calculate performance metrics
    performance = {}

    for symbol in df['symbol'].unique():
        symbol_data = df[df['symbol'] == symbol].copy()

        if len(symbol_data) > 1:
            # Sort by timestamp
            symbol_data = symbol_data.sort_values('timestamp')

            # Calculate performance metrics
            first_price = symbol_data.iloc[0]['price']
            last_price = symbol_data.iloc[-1]['price']
            price_change = ((last_price - first_price) / first_price) * 100

            # Get price statistics
            min_price = symbol_data['price'].min()
            max_price = symbol_data['price'].max()
            avg_price = symbol_data['price'].mean()

            # Calculate volatility (standard deviation of daily changes)
            symbol_data['price_change'] = symbol_data['price'].pct_change()
            volatility = symbol_data['price_change'].std() * 100

            performance[symbol] = {
                'price_change_percent': round(price_change, 2),
                'start_price': round(first_price, 6),
                'end_price': round(last_price, 6),
                'min_price': round(min_price, 6),
                'max_price': round(max_price, 6),
                'avg_price': round(avg_price, 6),
                'volatility_percent': round(volatility, 2),
                'data_points': len(symbol_data)
            }

    return performance

def export_daily_summary(date: str, data_dir: Optional[str] = None, output_dir: Optional[str] = None) -> str:
    """
    Export a daily summary combining all data types for a specific date

    Args:
        date: Date in 'YYYY-MM-DD' format
        data_dir: Custom data directory path (optional)
        output_dir: Output directory for summary file (optional)

    Returns:
        Path to created summary file
    """
    date_obj = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    date_str = date_obj.strftime('%Y%m%d')

    data_dir = data_dir or DEFAULT_DATA_DIR
    output_dir = output_dir or data_dir

    summary_file = f"{output_dir}/daily_summary_{date_str}.xlsx"

    with pd.ExcelWriter(summary_file, engine='openpyxl') as writer:
        for data_type in VALID_DATA_TYPES:
            df = load_cmc_data(data_type, start_date=date, end_date=date, data_dir=data_dir)
            if not df.empty:
                df.to_excel(writer, sheet_name=data_type, index=False)
                logger.info(f"Added {len(df)} {data_type} records to summary")

    logger.info(f"Daily summary exported to: {summary_file}")
    return summary_file

if __name__ == "__main__":
    # Example usage and testing
    print("CMC Data Utils - Example Usage")
    print("=" * 50)

    # Show data summary
    print("\n1. Data Summary:")
    summary = get_data_summary()
    for data_type, info in summary.items():
        print(f"   {data_type}: {info['files_count']} files, "
              f"{info['total_records']} records, "
              f"{info['date_range']['start']} to {info['date_range']['end']}")

    # Load today's watchlist
    print("\n2. Today's Watchlist:")
    df = get_latest_data('watchlist')
    if not df.empty:
        print(f"   Found {len(df)} coins in today's watchlist")
        print("   Symbols:", df['symbol'].tolist() if 'symbol' in df.columns else "No symbol column")
    else:
        print("   No watchlist data available for today")

    # Analyze performance
    print("\n3. Weekly Performance Analysis:")
    performance = analyze_watchlist_performance(days_back=7)
    for symbol, metrics in performance.items():
        print(f"   {symbol}: {metrics['price_change_percent']}% change, "
              f"{metrics['volatility_percent']}% volatility")