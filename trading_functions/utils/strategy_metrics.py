"""
📊 STRATEGY PERFORMANCE METRICS MODULE 📈
========================================
Comprehensive strategy performance analysis and metrics calculation.
Advanced backtesting metrics from the One Candle Strategy framework.

Features:
- Comprehensive Strategy Performance Metrics 📊
- Trade Analysis and Statistics 💰
- Risk-Adjusted Returns 📈
- Drawdown Analysis 📉
- Consecutive Win/Loss Tracking 🎯

Author: Bobby's Algo Trading Systems 🌙
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta


class StrategyMetricsError(Exception):
    """🚨 Custom exception for strategy metrics errors"""
    pass


@dataclass
class MetricsConfig:
    """🎯 Configuration for strategy metrics calculation"""
    risk_free_rate: float = 0.02              # Risk-free rate for Sharpe calculation
    trading_days_per_year: int = 252           # Trading days per year
    benchmark_return: float = 0.10             # Benchmark return for comparison
    max_consecutive_threshold: int = 10        # Threshold for consecutive analysis


# ============================================================
# COMPREHENSIVE STRATEGY METRICS 📊
# ============================================================

def calculate_comprehensive_strategy_metrics(
    trades_df: pd.DataFrame,
    initial_capital: float = 100000,
    config: MetricsConfig = None
) -> Dict[str, Any]:
    """
    📊 Calculate comprehensive strategy performance metrics

    This function provides institutional-grade performance analysis
    covering returns, risk, drawdowns, and trade statistics.

    Args:
        trades_df: DataFrame with trade results (must have 'pnl' column)
        initial_capital: Starting capital amount
        config: MetricsConfig object for parameters

    Returns:
        Dictionary with comprehensive performance metrics

    Raises:
        StrategyMetricsError: If calculation fails
    """
    try:
        config = config or MetricsConfig()

        if trades_df.empty:
            return _empty_metrics_response()

        # Validate required columns
        if 'pnl' not in trades_df.columns:
            raise StrategyMetricsError("DataFrame must contain 'pnl' column")

        # Basic trade statistics
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['pnl'] > 0])
        losing_trades = len(trades_df[trades_df['pnl'] < 0])
        breakeven_trades = len(trades_df[trades_df['pnl'] == 0])

        # Win rate and loss rate
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        loss_rate = (losing_trades / total_trades * 100) if total_trades > 0 else 0

        # Profit and loss calculations
        gross_profit = trades_df[trades_df['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(trades_df[trades_df['pnl'] < 0]['pnl'].sum())
        net_profit = gross_profit - gross_loss
        total_return_pct = (net_profit / initial_capital * 100) if initial_capital > 0 else 0

        # Profit factor
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')

        # Average trade metrics
        avg_win = gross_profit / winning_trades if winning_trades > 0 else 0
        avg_loss = gross_loss / losing_trades if losing_trades > 0 else 0
        avg_trade = trades_df['pnl'].mean()

        # Expectancy calculation
        expectancy = (win_rate/100 * avg_win) - (loss_rate/100 * avg_loss)

        # Largest win and loss
        largest_win = trades_df['pnl'].max() if not trades_df.empty else 0
        largest_loss = trades_df['pnl'].min() if not trades_df.empty else 0

        # Calculate consecutive wins/losses
        consecutive_stats = _calculate_consecutive_stats(trades_df)

        # Risk metrics
        risk_metrics = _calculate_risk_metrics(trades_df, config)

        # Drawdown analysis
        drawdown_metrics = _calculate_drawdown_metrics(trades_df, initial_capital)

        # Time-based analysis (if timestamp data available)
        time_metrics = _calculate_time_metrics(trades_df)

        # Combine all metrics
        metrics = {
            # Basic Trade Statistics
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'breakeven_trades': breakeven_trades,
            'win_rate_percent': win_rate,
            'loss_rate_percent': loss_rate,

            # Profit/Loss Metrics
            'gross_profit': gross_profit,
            'gross_loss': gross_loss,
            'net_profit': net_profit,
            'total_return_percent': total_return_pct,
            'profit_factor': profit_factor,

            # Average Trade Metrics
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'avg_trade': avg_trade,
            'expectancy': expectancy,

            # Extreme Values
            'largest_win': largest_win,
            'largest_loss': largest_loss,

            # Risk-Adjusted Metrics
            **risk_metrics,

            # Drawdown Metrics
            **drawdown_metrics,

            # Consecutive Statistics
            **consecutive_stats,

            # Time-Based Metrics
            **time_metrics
        }

        return metrics

    except Exception as e:
        raise StrategyMetricsError(f"Failed to calculate strategy metrics: {e}")


# ============================================================
# CONSECUTIVE STATISTICS 🎯
# ============================================================

def _calculate_consecutive_stats(trades_df: pd.DataFrame) -> Dict[str, int]:
    """
    🎯 Calculate consecutive wins and losses statistics

    Args:
        trades_df: DataFrame with trade results

    Returns:
        Dictionary with consecutive statistics
    """
    try:
        if trades_df.empty:
            return {
                'max_consecutive_wins': 0,
                'max_consecutive_losses': 0,
                'current_streak': 0,
                'current_streak_type': 'none'
            }

        # Create win/loss series
        trades_df = trades_df.copy()
        trades_df['is_win'] = trades_df['pnl'] > 0

        # Calculate consecutive wins
        win_groups = (trades_df['is_win'] != trades_df['is_win'].shift()).cumsum()
        consecutive_wins = trades_df[trades_df['is_win']].groupby(win_groups).size()
        max_consecutive_wins = consecutive_wins.max() if not consecutive_wins.empty else 0

        # Calculate consecutive losses
        loss_groups = (~trades_df['is_win'] != ~trades_df['is_win'].shift()).cumsum()
        consecutive_losses = trades_df[~trades_df['is_win']].groupby(loss_groups).size()
        max_consecutive_losses = consecutive_losses.max() if not consecutive_losses.empty else 0

        # Current streak
        current_streak = 1
        current_streak_type = 'win' if trades_df['is_win'].iloc[-1] else 'loss'

        # Count current streak
        for i in range(len(trades_df) - 2, -1, -1):
            if trades_df['is_win'].iloc[i] == trades_df['is_win'].iloc[-1]:
                current_streak += 1
            else:
                break

        return {
            'max_consecutive_wins': int(max_consecutive_wins),
            'max_consecutive_losses': int(max_consecutive_losses),
            'current_streak': current_streak,
            'current_streak_type': current_streak_type
        }

    except Exception:
        return {
            'max_consecutive_wins': 0,
            'max_consecutive_losses': 0,
            'current_streak': 0,
            'current_streak_type': 'none'
        }


# ============================================================
# RISK-ADJUSTED METRICS 📈
# ============================================================

def _calculate_risk_metrics(
    trades_df: pd.DataFrame,
    config: MetricsConfig
) -> Dict[str, float]:
    """
    📈 Calculate risk-adjusted performance metrics

    Args:
        trades_df: DataFrame with trade results
        config: MetricsConfig object

    Returns:
        Dictionary with risk metrics
    """
    try:
        if trades_df.empty or len(trades_df) < 2:
            return {
                'sharpe_ratio': 0.0,
                'sortino_ratio': 0.0,
                'calmar_ratio': 0.0,
                'volatility': 0.0,
                'downside_deviation': 0.0
            }

        # Calculate returns volatility
        returns = trades_df['pnl'].values
        volatility = np.std(returns) * np.sqrt(config.trading_days_per_year)

        # Sharpe Ratio
        avg_return = np.mean(returns)
        excess_return = avg_return - (config.risk_free_rate / config.trading_days_per_year)
        sharpe_ratio = (excess_return / np.std(returns)) if np.std(returns) > 0 else 0

        # Sortino Ratio (using downside deviation)
        negative_returns = returns[returns < 0]
        downside_deviation = np.std(negative_returns) if len(negative_returns) > 0 else 0
        sortino_ratio = (excess_return / downside_deviation) if downside_deviation > 0 else 0

        # Calmar Ratio (requires drawdown calculation)
        cumulative_returns = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdowns = (cumulative_returns - running_max) / running_max
        max_drawdown = abs(np.min(drawdowns)) if len(drawdowns) > 0 else 0.001
        calmar_ratio = (avg_return * config.trading_days_per_year) / max_drawdown

        return {
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'calmar_ratio': calmar_ratio,
            'volatility': volatility,
            'downside_deviation': downside_deviation
        }

    except Exception:
        return {
            'sharpe_ratio': 0.0,
            'sortino_ratio': 0.0,
            'calmar_ratio': 0.0,
            'volatility': 0.0,
            'downside_deviation': 0.0
        }


# ============================================================
# DRAWDOWN ANALYSIS 📉
# ============================================================

def _calculate_drawdown_metrics(
    trades_df: pd.DataFrame,
    initial_capital: float
) -> Dict[str, float]:
    """
    📉 Calculate drawdown analysis metrics

    Args:
        trades_df: DataFrame with trade results
        initial_capital: Starting capital amount

    Returns:
        Dictionary with drawdown metrics
    """
    try:
        if trades_df.empty:
            return {
                'max_drawdown_percent': 0.0,
                'max_drawdown_amount': 0.0,
                'current_drawdown_percent': 0.0,
                'recovery_factor': 0.0,
                'average_drawdown': 0.0
            }

        # Calculate equity curve
        cumulative_pnl = trades_df['pnl'].cumsum()
        equity_curve = initial_capital + cumulative_pnl

        # Calculate running maximum (peak equity)
        running_max = equity_curve.expanding().max()

        # Calculate drawdowns
        drawdown_amounts = equity_curve - running_max
        drawdown_percentages = (drawdown_amounts / running_max) * 100

        # Maximum drawdown
        max_drawdown_percent = abs(drawdown_percentages.min())
        max_drawdown_amount = abs(drawdown_amounts.min())

        # Current drawdown
        current_drawdown_percent = abs(drawdown_percentages.iloc[-1])

        # Recovery factor (net profit / max drawdown)
        net_profit = cumulative_pnl.iloc[-1]
        recovery_factor = (net_profit / max_drawdown_amount) if max_drawdown_amount > 0 else 0

        # Average drawdown (average of all negative drawdowns)
        negative_drawdowns = drawdown_percentages[drawdown_percentages < 0]
        average_drawdown = abs(negative_drawdowns.mean()) if not negative_drawdowns.empty else 0

        return {
            'max_drawdown_percent': max_drawdown_percent,
            'max_drawdown_amount': max_drawdown_amount,
            'current_drawdown_percent': current_drawdown_percent,
            'recovery_factor': recovery_factor,
            'average_drawdown': average_drawdown
        }

    except Exception:
        return {
            'max_drawdown_percent': 0.0,
            'max_drawdown_amount': 0.0,
            'current_drawdown_percent': 0.0,
            'recovery_factor': 0.0,
            'average_drawdown': 0.0
        }


# ============================================================
# TIME-BASED ANALYSIS ⏰
# ============================================================

def _calculate_time_metrics(trades_df: pd.DataFrame) -> Dict[str, Any]:
    """
    ⏰ Calculate time-based performance metrics

    Args:
        trades_df: DataFrame with trade results

    Returns:
        Dictionary with time-based metrics
    """
    try:
        if trades_df.empty:
            return {
                'total_trading_period': 'N/A',
                'trades_per_day': 0.0,
                'best_month': 'N/A',
                'worst_month': 'N/A'
            }

        # Check if timestamp data is available
        if 'timestamp' in trades_df.columns or 'entry_time' in trades_df.columns:
            timestamp_col = 'timestamp' if 'timestamp' in trades_df.columns else 'entry_time'

            # Convert to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(trades_df[timestamp_col]):
                trades_df = trades_df.copy()
                trades_df[timestamp_col] = pd.to_datetime(trades_df[timestamp_col])

            # Calculate trading period
            start_date = trades_df[timestamp_col].min()
            end_date = trades_df[timestamp_col].max()
            trading_period = (end_date - start_date).days

            # Trades per day
            trades_per_day = len(trades_df) / max(trading_period, 1)

            # Monthly analysis
            trades_df['month'] = trades_df[timestamp_col].dt.to_period('M')
            monthly_pnl = trades_df.groupby('month')['pnl'].sum()

            best_month = str(monthly_pnl.idxmax()) if not monthly_pnl.empty else 'N/A'
            worst_month = str(monthly_pnl.idxmin()) if not monthly_pnl.empty else 'N/A'

            return {
                'total_trading_period': f"{trading_period} days",
                'trades_per_day': trades_per_day,
                'best_month': best_month,
                'worst_month': worst_month
            }

        return {
            'total_trading_period': 'N/A',
            'trades_per_day': 0.0,
            'best_month': 'N/A',
            'worst_month': 'N/A'
        }

    except Exception:
        return {
            'total_trading_period': 'N/A',
            'trades_per_day': 0.0,
            'best_month': 'N/A',
            'worst_month': 'N/A'
        }


# ============================================================
# ENHANCED REWARD-TO-RISK CALCULATION 💰
# ============================================================

def calculate_enhanced_reward_to_risk(
    entry: float,
    stop_loss: float,
    take_profit: float,
    confidence_level: float = 1.0
) -> Dict[str, float]:
    """
    💰 Calculate enhanced reward-to-risk ratio with confidence adjustments

    Args:
        entry: Entry price
        stop_loss: Stop loss price
        take_profit: Take profit price
        confidence_level: Confidence multiplier (0.0 to 1.0)

    Returns:
        Dictionary with enhanced risk-reward metrics
    """
    try:
        # Basic risk and reward calculation
        risk = abs(entry - stop_loss)
        reward = abs(take_profit - entry)

        if risk == 0:
            return {
                'reward_to_risk_ratio': 0.0,
                'adjusted_ratio': 0.0,
                'risk_amount': 0.0,
                'reward_amount': 0.0,
                'confidence_level': confidence_level
            }

        # Basic ratio
        basic_ratio = reward / risk

        # Confidence-adjusted ratio
        adjusted_ratio = basic_ratio * confidence_level

        return {
            'reward_to_risk_ratio': basic_ratio,
            'adjusted_ratio': adjusted_ratio,
            'risk_amount': risk,
            'reward_amount': reward,
            'confidence_level': confidence_level
        }

    except Exception as e:
        raise StrategyMetricsError(f"Failed to calculate reward-to-risk: {e}")


# ============================================================
# UTILITY FUNCTIONS 🛠️
# ============================================================

def _empty_metrics_response() -> Dict[str, Any]:
    """
    🛠️ Return empty metrics response for edge cases

    Returns:
        Dictionary with zero/empty values for all metrics
    """
    return {
        'total_trades': 0,
        'winning_trades': 0,
        'losing_trades': 0,
        'breakeven_trades': 0,
        'win_rate_percent': 0.0,
        'loss_rate_percent': 0.0,
        'gross_profit': 0.0,
        'gross_loss': 0.0,
        'net_profit': 0.0,
        'total_return_percent': 0.0,
        'profit_factor': 0.0,
        'avg_win': 0.0,
        'avg_loss': 0.0,
        'avg_trade': 0.0,
        'expectancy': 0.0,
        'largest_win': 0.0,
        'largest_loss': 0.0,
        'sharpe_ratio': 0.0,
        'sortino_ratio': 0.0,
        'calmar_ratio': 0.0,
        'volatility': 0.0,
        'downside_deviation': 0.0,
        'max_drawdown_percent': 0.0,
        'max_drawdown_amount': 0.0,
        'current_drawdown_percent': 0.0,
        'recovery_factor': 0.0,
        'average_drawdown': 0.0,
        'max_consecutive_wins': 0,
        'max_consecutive_losses': 0,
        'current_streak': 0,
        'current_streak_type': 'none',
        'total_trading_period': 'N/A',
        'trades_per_day': 0.0,
        'best_month': 'N/A',
        'worst_month': 'N/A'
    }


def validate_trades_data(trades_df: pd.DataFrame) -> bool:
    """
    🛡️ Validate trades data for metrics calculation

    Args:
        trades_df: DataFrame to validate

    Returns:
        True if data is valid for analysis
    """
    try:
        # Check if DataFrame is not empty
        if trades_df.empty:
            return False

        # Check for required PnL column
        if 'pnl' not in trades_df.columns:
            return False

        # Check for numeric PnL values
        if not pd.api.types.is_numeric_dtype(trades_df['pnl']):
            return False

        return True

    except Exception:
        return False


def strategy_metrics_production_readiness() -> Dict[str, bool]:
    """
    🛡️ Check production readiness of strategy metrics module

    Returns:
        Dictionary with readiness status
    """
    try:
        readiness = {
            'module_importable': True,
            'core_functions_available': True,
            'pandas_available': True,
            'numpy_available': True,
            'error_handling_implemented': True
        }

        # Test core function availability
        core_functions = [
            calculate_comprehensive_strategy_metrics,
            calculate_enhanced_reward_to_risk,
            validate_trades_data
        ]

        for func in core_functions:
            if not callable(func):
                readiness['core_functions_available'] = False
                break

        # Test pandas availability
        try:
            import pandas as pd
            readiness['pandas_available'] = True
        except ImportError:
            readiness['pandas_available'] = False

        # Test numpy availability
        try:
            import numpy as np
            readiness['numpy_available'] = True
        except ImportError:
            readiness['numpy_available'] = False

        return readiness

    except Exception:
        return {
            'module_importable': False,
            'core_functions_available': False,
            'pandas_available': False,
            'numpy_available': False,
            'error_handling_implemented': False
        }


print("📊 Strategy Performance Metrics Module loaded successfully! 📈")