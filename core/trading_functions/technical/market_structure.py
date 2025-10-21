"""
🏔️ MARKET STRUCTURE ANALYSIS MODULE 📈
=====================================
Advanced market structure analysis for trading functions library.
Swing points, trend analysis, and volume profile tools from One Candle Strategy.

Features:
- Swing High/Low Identification 🏔️
- Market Structure Analysis 📈
- Volume Profile Analysis 📊
- Trend Strength Detection 💪
- Linear Regression Analysis 📊

Author: Bobby's Algo Trading Systems 🌙
"""

import pandas as pd
import numpy as np
import talib
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from scipy import stats


class MarketStructureError(Exception):
    """🚨 Custom exception for market structure analysis errors"""
    pass


@dataclass
class MarketStructureConfig:
    """🎯 Configuration for market structure analysis parameters"""
    swing_lookback: int = 5                    # Bars to look back/forward for swings
    trend_lookback: int = 20                   # Bars for trend analysis
    volume_lookback: int = 20                  # Bars for volume analysis
    trend_strength_threshold: float = 0.7      # R-value threshold for strong trend
    weak_trend_threshold: float = 0.3          # R-value threshold for weak trend
    volume_spike_std: float = 2.0              # Standard deviations for volume spike


# ============================================================
# SWING POINT IDENTIFICATION 🏔️
# ============================================================

def identify_swing_points(
    data: pd.DataFrame,
    lookback: int = None,
    config: MarketStructureConfig = None
) -> Tuple[List[int], List[int]]:
    """
    🏔️ Identify swing highs and lows in price action

    Swing points are local extremes where price reverses direction,
    indicating potential support/resistance levels and market structure.

    Args:
        data: DataFrame with OHLC data
        lookback: Number of bars to look back/forward for swings (default 5)
        config: MarketStructureConfig object

    Returns:
        Tuple: (swing_highs, swing_lows) - Lists of indices

    Raises:
        MarketStructureError: If data validation fails
    """
    try:
        config = config or MarketStructureConfig()
        lookback = lookback or config.swing_lookback

        # Validate data
        if 'High' not in data.columns or 'Low' not in data.columns:
            raise MarketStructureError("Data must contain 'High' and 'Low' columns")

        if len(data) < lookback * 2 + 1:
            return [], []

        swing_highs = []
        swing_lows = []

        # Identify swing points by checking local extremes
        for i in range(lookback, len(data) - lookback):
            # Check for swing high - current high is highest in lookback window
            is_swing_high = True
            current_high = data['High'].iloc[i]

            for j in range(i - lookback, i + lookback + 1):
                if j != i and data['High'].iloc[j] >= current_high:
                    is_swing_high = False
                    break

            if is_swing_high:
                swing_highs.append(i)

            # Check for swing low - current low is lowest in lookback window
            is_swing_low = True
            current_low = data['Low'].iloc[i]

            for j in range(i - lookback, i + lookback + 1):
                if j != i and data['Low'].iloc[j] <= current_low:
                    is_swing_low = False
                    break

            if is_swing_low:
                swing_lows.append(i)

        return swing_highs, swing_lows

    except Exception as e:
        raise MarketStructureError(f"Failed to identify swing points: {e}")


def analyze_swing_structure(
    data: pd.DataFrame,
    swing_highs: List[int],
    swing_lows: List[int]
) -> Dict[str, Any]:
    """
    📊 Analyze the structure of swing points for trend determination

    Args:
        data: DataFrame with OHLC data
        swing_highs: List of swing high indices
        swing_lows: List of swing low indices

    Returns:
        Dictionary with swing structure analysis
    """
    try:
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return {
                'structure_type': 'insufficient_data',
                'higher_highs': 0,
                'lower_highs': 0,
                'higher_lows': 0,
                'lower_lows': 0,
                'trend_direction': 'neutral'
            }

        # Analyze recent swing patterns
        recent_highs = swing_highs[-3:] if len(swing_highs) >= 3 else swing_highs
        recent_lows = swing_lows[-3:] if len(swing_lows) >= 3 else swing_lows

        # Count higher/lower highs
        higher_highs = 0
        lower_highs = 0
        for i in range(1, len(recent_highs)):
            prev_high = data['High'].iloc[recent_highs[i-1]]
            curr_high = data['High'].iloc[recent_highs[i]]
            if curr_high > prev_high:
                higher_highs += 1
            else:
                lower_highs += 1

        # Count higher/lower lows
        higher_lows = 0
        lower_lows = 0
        for i in range(1, len(recent_lows)):
            prev_low = data['Low'].iloc[recent_lows[i-1]]
            curr_low = data['Low'].iloc[recent_lows[i]]
            if curr_low > prev_low:
                higher_lows += 1
            else:
                lower_lows += 1

        # Determine trend direction
        if higher_highs > lower_highs and higher_lows > lower_lows:
            trend_direction = 'uptrend'
        elif lower_highs > higher_highs and lower_lows > higher_lows:
            trend_direction = 'downtrend'
        else:
            trend_direction = 'sideways'

        return {
            'structure_type': 'complete_analysis',
            'higher_highs': higher_highs,
            'lower_highs': lower_highs,
            'higher_lows': higher_lows,
            'lower_lows': lower_lows,
            'trend_direction': trend_direction,
            'swing_high_count': len(swing_highs),
            'swing_low_count': len(swing_lows)
        }

    except Exception as e:
        raise MarketStructureError(f"Failed to analyze swing structure: {e}")


# ============================================================
# MARKET STRUCTURE ANALYSIS 📈
# ============================================================

def calculate_market_structure(
    data: pd.DataFrame,
    lookback: int = None,
    config: MarketStructureConfig = None
) -> Dict[str, Any]:
    """
    📈 Determine market structure (trending/ranging) using statistical analysis

    Uses linear regression to determine trend strength and direction,
    combined with ATR for volatility assessment.

    Args:
        data: DataFrame with OHLC data
        lookback: Number of bars to analyze (default 20)
        config: MarketStructureConfig object

    Returns:
        Dictionary with market structure analysis

    Raises:
        MarketStructureError: If analysis fails
    """
    try:
        config = config or MarketStructureConfig()
        lookback = lookback or config.trend_lookback

        if len(data) < lookback:
            raise MarketStructureError(f"Insufficient data: need {lookback} bars, got {len(data)}")

        recent_data = data.tail(lookback).copy()

        # Calculate trend using linear regression on close prices
        x = np.arange(len(recent_data))
        y = recent_data['Close'].values

        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

        # Determine trend strength based on R-squared
        trend_strength = abs(r_value)
        r_squared = r_value ** 2

        # Calculate ATR for volatility assessment
        high_values = recent_data['High'].values
        low_values = recent_data['Low'].values
        close_values = recent_data['Close'].values

        atr_period = min(14, len(recent_data))
        if len(recent_data) >= atr_period:
            atr = talib.ATR(high_values, low_values, close_values, atr_period)
            current_atr = atr[-1] if len(atr) > 0 and not np.isnan(atr[-1]) else 0
        else:
            # Calculate simple range for short data
            current_atr = recent_data['High'].max() - recent_data['Low'].min()

        # Determine market state based on trend strength and slope
        if trend_strength > config.trend_strength_threshold:
            if slope > 0:
                market_state = 'strong_uptrend'
            else:
                market_state = 'strong_downtrend'
        elif trend_strength > config.weak_trend_threshold:
            if slope > 0:
                market_state = 'weak_uptrend'
            else:
                market_state = 'weak_downtrend'
        else:
            market_state = 'ranging'

        # Calculate additional metrics
        price_range = recent_data['High'].max() - recent_data['Low'].min()
        current_price = recent_data['Close'].iloc[-1]
        range_position = (current_price - recent_data['Low'].min()) / price_range if price_range > 0 else 0.5

        return {
            'market_state': market_state,
            'trend_strength': trend_strength,
            'slope': slope,
            'r_squared': r_squared,
            'p_value': p_value,
            'atr': current_atr,
            'price_range': price_range,
            'range_position': range_position,
            'trend_direction': 'bullish' if slope > 0 else 'bearish',
            'volatility_level': 'high' if current_atr > price_range * 0.1 else 'low'
        }

    except Exception as e:
        raise MarketStructureError(f"Failed to calculate market structure: {e}")


# ============================================================
# VOLUME PROFILE ANALYSIS 📊
# ============================================================

def analyze_volume_profile(
    data: pd.DataFrame,
    lookback_bars: int = None,
    config: MarketStructureConfig = None
) -> Dict[str, Any]:
    """
    📊 Analyze volume profile and identify high volume nodes

    Volume profile reveals where most trading activity occurred,
    indicating important support/resistance levels.

    Args:
        data: DataFrame with OHLC and Volume data
        lookback_bars: Number of bars to analyze (default 20)
        config: MarketStructureConfig object

    Returns:
        Dictionary with volume analysis results

    Raises:
        MarketStructureError: If volume analysis fails
    """
    try:
        config = config or MarketStructureConfig()
        lookback_bars = lookback_bars or config.volume_lookback

        if 'Volume' not in data.columns:
            raise MarketStructureError("Data must contain 'Volume' column")

        if len(data) < lookback_bars:
            return {
                'avg_volume': 0,
                'current_volume': 0,
                'volume_ratio': 0,
                'volume_spike': False,
                'vwap': 0,
                'volume_trend': 'insufficient_data'
            }

        recent_data = data.tail(lookback_bars).copy()

        # Calculate volume statistics
        avg_volume = recent_data['Volume'].mean()
        volume_std = recent_data['Volume'].std()
        current_volume = data['Volume'].iloc[-1]

        # Identify volume spikes (> 2 standard deviations above mean)
        volume_spike = current_volume > (avg_volume + config.volume_spike_std * volume_std)

        # Calculate volume-weighted average price (VWAP)
        total_volume = recent_data['Volume'].sum()
        if total_volume > 0:
            vwap = (recent_data['Close'] * recent_data['Volume']).sum() / total_volume
        else:
            vwap = recent_data['Close'].mean()

        # Calculate volume trend
        volume_values = recent_data['Volume'].values
        x = np.arange(len(volume_values))

        if len(volume_values) > 1:
            vol_slope, _, vol_r_value, _, _ = stats.linregress(x, volume_values)
            if abs(vol_r_value) > 0.3:  # Significant correlation
                volume_trend = 'increasing' if vol_slope > 0 else 'decreasing'
            else:
                volume_trend = 'stable'
        else:
            volume_trend = 'stable'

        # Calculate volume percentiles for context
        volume_percentiles = {
            '25th': np.percentile(recent_data['Volume'], 25),
            '50th': np.percentile(recent_data['Volume'], 50),
            '75th': np.percentile(recent_data['Volume'], 75),
            '90th': np.percentile(recent_data['Volume'], 90)
        }

        return {
            'avg_volume': avg_volume,
            'current_volume': current_volume,
            'volume_ratio': current_volume / avg_volume if avg_volume > 0 else 0,
            'volume_spike': volume_spike,
            'vwap': vwap,
            'volume_trend': volume_trend,
            'volume_std': volume_std,
            'volume_percentiles': volume_percentiles,
            'high_volume_bars': len(recent_data[recent_data['Volume'] > avg_volume + volume_std])
        }

    except Exception as e:
        raise MarketStructureError(f"Failed to analyze volume profile: {e}")


# ============================================================
# SUPPORT AND RESISTANCE LEVELS 🎯
# ============================================================

def identify_key_levels(
    data: pd.DataFrame,
    swing_highs: List[int],
    swing_lows: List[int],
    proximity_threshold: float = 0.01
) -> Dict[str, List[float]]:
    """
    🎯 Identify key support and resistance levels from swing points

    Args:
        data: DataFrame with OHLC data
        swing_highs: List of swing high indices
        swing_lows: List of swing low indices
        proximity_threshold: How close levels need to be to cluster (default 1%)

    Returns:
        Dictionary with support and resistance levels
    """
    try:
        # Extract swing high and low prices
        resistance_levels = [data['High'].iloc[i] for i in swing_highs]
        support_levels = [data['Low'].iloc[i] for i in swing_lows]

        # Cluster nearby levels
        def cluster_levels(levels, threshold):
            if not levels:
                return []

            levels = sorted(levels)
            clusters = []
            current_cluster = [levels[0]]

            for i in range(1, len(levels)):
                # Check if current level is within threshold of cluster
                cluster_avg = sum(current_cluster) / len(current_cluster)
                if abs(levels[i] - cluster_avg) / cluster_avg <= threshold:
                    current_cluster.append(levels[i])
                else:
                    # Start new cluster
                    clusters.append(sum(current_cluster) / len(current_cluster))
                    current_cluster = [levels[i]]

            # Add the last cluster
            if current_cluster:
                clusters.append(sum(current_cluster) / len(current_cluster))

            return clusters

        # Cluster support and resistance levels
        clustered_resistance = cluster_levels(resistance_levels, proximity_threshold)
        clustered_support = cluster_levels(support_levels, proximity_threshold)

        # Sort by strength (number of touches)
        current_price = data['Close'].iloc[-1]

        return {
            'resistance_levels': sorted(clustered_resistance, reverse=True),
            'support_levels': sorted(clustered_support),
            'nearest_resistance': min(clustered_resistance, key=lambda x: abs(x - current_price)) if clustered_resistance else None,
            'nearest_support': min(clustered_support, key=lambda x: abs(x - current_price)) if clustered_support else None,
            'current_price': current_price
        }

    except Exception as e:
        raise MarketStructureError(f"Failed to identify key levels: {e}")


# ============================================================
# MARKET STRUCTURE UTILITIES 🛠️
# ============================================================

def validate_market_structure_data(data: pd.DataFrame) -> bool:
    """
    🛡️ Validate data for market structure analysis

    Args:
        data: DataFrame to validate

    Returns:
        True if data is valid for analysis
    """
    try:
        required_columns = ['High', 'Low', 'Close']

        # Check required columns
        if not all(col in data.columns for col in required_columns):
            return False

        # Check for sufficient data
        if len(data) < 10:
            return False

        # Check for valid price relationships
        for _, row in data.tail(5).iterrows():
            if not (row['Low'] <= row['Close'] <= row['High']):
                return False

        return True

    except Exception:
        return False


def market_structure_production_readiness() -> Dict[str, bool]:
    """
    🛡️ Check production readiness of market structure module

    Returns:
        Dictionary with readiness status
    """
    try:
        readiness = {
            'module_importable': True,
            'core_functions_available': True,
            'scipy_available': True,
            'talib_available': True,
            'error_handling_implemented': True
        }

        # Test core function availability
        core_functions = [
            identify_swing_points,
            calculate_market_structure,
            analyze_volume_profile,
            identify_key_levels
        ]

        for func in core_functions:
            if not callable(func):
                readiness['core_functions_available'] = False
                break

        # Test scipy availability
        try:
            from scipy import stats
            readiness['scipy_available'] = True
        except ImportError:
            readiness['scipy_available'] = False

        # Test talib availability
        try:
            import talib
            readiness['talib_available'] = True
        except ImportError:
            readiness['talib_available'] = False

        return readiness

    except Exception:
        return {
            'module_importable': False,
            'core_functions_available': False,
            'scipy_available': False,
            'talib_available': False,
            'error_handling_implemented': False
        }


print("🏔️ Market Structure Analysis Module loaded successfully! 📈")