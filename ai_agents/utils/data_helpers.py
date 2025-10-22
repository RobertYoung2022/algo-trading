"""
Data Helper Functions
Utilities for preparing and formatting data for AI agents
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np


def prepare_market_data(df: pd.DataFrame, columns: List[str] = None) -> Dict[str, Any]:
    """
    Prepare market data for AI analysis

    Args:
        df: Market data DataFrame
        columns: Specific columns to include (None = all)

    Returns:
        Dict with formatted market data
    """
    if df.empty:
        return {"error": "Empty DataFrame"}

    # Select columns
    if columns:
        df = df[columns]

    # Get recent data summary
    recent = df.tail(20)  # Last 20 periods

    summary = {
        "latest": {
            col: float(recent[col].iloc[-1]) if pd.api.types.is_numeric_dtype(recent[col]) else str(recent[col].iloc[-1])
            for col in recent.columns
        },
        "statistics": {
            col: {
                "mean": float(recent[col].mean()),
                "std": float(recent[col].std()),
                "min": float(recent[col].min()),
                "max": float(recent[col].max())
            }
            for col in recent.columns if pd.api.types.is_numeric_dtype(recent[col])
        },
        "trends": {
            col: "increasing" if recent[col].iloc[-1] > recent[col].iloc[0] else "decreasing"
            for col in recent.columns if pd.api.types.is_numeric_dtype(recent[col])
        },
        "total_periods": len(df),
        "recent_periods": len(recent)
    }

    return summary


def format_backtest_results(results: Dict[str, Any]) -> str:
    """
    Format backtest results for AI analysis

    Args:
        results: Backtest results dictionary

    Returns:
        Formatted string representation
    """
    if not results:
        return "No results available"

    formatted = "=== BACKTEST RESULTS ===\n\n"

    # Performance metrics
    if "return" in results:
        formatted += f"Total Return: {results['return']:.2%}\n"
    if "sharpe_ratio" in results:
        formatted += f"Sharpe Ratio: {results['sharpe_ratio']:.2f}\n"
    if "max_drawdown" in results:
        formatted += f"Max Drawdown: {results['max_drawdown']:.2%}\n"
    if "win_rate" in results:
        formatted += f"Win Rate: {results['win_rate']:.2%}\n"

    # Trade statistics
    if "num_trades" in results:
        formatted += f"\nTotal Trades: {results['num_trades']}\n"
    if "avg_trade_return" in results:
        formatted += f"Avg Trade Return: {results['avg_trade_return']:.2%}\n"

    # Risk metrics
    if "volatility" in results:
        formatted += f"\nVolatility: {results['volatility']:.2%}\n"
    if "calmar_ratio" in results:
        formatted += f"Calmar Ratio: {results['calmar_ratio']:.2f}\n"

    return formatted


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate common technical indicators

    Args:
        df: OHLCV DataFrame

    Returns:
        DataFrame with added indicators
    """
    df = df.copy()

    # Simple Moving Averages
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()

    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # Bollinger Bands
    sma = df['close'].rolling(window=20).mean()
    std = df['close'].rolling(window=20).std()
    df['bb_upper'] = sma + (std * 2)
    df['bb_lower'] = sma - (std * 2)

    # Volume indicators
    df['volume_sma'] = df['volume'].rolling(window=20).mean()
    df['volume_ratio'] = df['volume'] / df['volume_sma']

    return df
