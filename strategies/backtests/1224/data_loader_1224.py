"""
🔧 Data Loading and Validation Module - December 2024
======================================================
Unified data loading and validation for all strategies.
Handles different column naming conventions and validates data quality.
"""

import pandas as pd
import numpy as np


def load_and_validate_data(file_path, min_quality_score=75):
    """
    🔍 Load and validate data quality
    Returns: (DataFrame, quality_score, validation_passed)
    """
    try:
        # Load data
        df = pd.read_csv(file_path)

        # Handle different column naming conventions for datetime
        datetime_cols = ['datetime', 'Datetime', 'Date', 'date', 'Time', 'time', 'timestamp']
        for col in datetime_cols:
            if col in df.columns:
                df.set_index(col, inplace=True)
                break

        # Convert index to datetime
        df.index = pd.to_datetime(df.index)

        # Standardize column names to uppercase
        df.columns = [col.capitalize() for col in df.columns]

        # Ensure required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_cols):
            print(f"⚠️ Missing required columns in {file_path}")
            print(f"   Available columns: {df.columns.tolist()}")
            return None, 0, False

        # Basic quality checks
        quality_score = 100

        # Check for missing values
        missing_pct = (df[required_cols].isna().sum().sum() / (len(df) * len(required_cols))) * 100
        quality_score -= missing_pct * 2

        # Check for zero prices
        zero_prices = (df[['Open', 'High', 'Low', 'Close']] == 0).sum().sum()
        if zero_prices > 0:
            quality_score -= 10

        # Check for negative prices
        negative_prices = (df[['Open', 'High', 'Low', 'Close']] < 0).sum().sum()
        if negative_prices > 0:
            quality_score -= 20

        # Check OHLC consistency
        invalid_candles = ((df['High'] < df['Low']) |
                          (df['High'] < df['Open']) |
                          (df['High'] < df['Close']) |
                          (df['Low'] > df['Open']) |
                          (df['Low'] > df['Close'])).sum()
        if invalid_candles > 0:
            quality_score -= (invalid_candles / len(df)) * 50

        validation_passed = quality_score >= min_quality_score

        return df, quality_score, validation_passed

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None, 0, False