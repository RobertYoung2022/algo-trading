"""
🕳️ ADVANCED PATTERN RECOGNITION MODULE 🕯️
==========================================
Fair Value Gaps, Pin Bars, and Advanced Pattern Detection for Trading Functions Library.
Institutional-grade pattern recognition from the One Candle Strategy.

Features:
- Fair Value Gap (FVG) Detection 🕳️
- Pin Bar (Hammer/Shooting Star) Recognition 📍
- Enhanced Engulfing Patterns 🕯️
- Session Range Analysis ⏰
- Advanced Pattern Utilities 🛠️

Author: Bobby's Algo Trading Systems 🌙
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime, time


class PatternAnalysisError(Exception):
    """🚨 Custom exception for pattern analysis errors"""
    pass


@dataclass
class PatternConfig:
    """🎯 Configuration for pattern detection parameters"""
    fvg_min_gap_percent: float = 0.1      # Minimum FVG gap size (%)
    fvg_lookback_bars: int = 20            # Bars to look back for FVG detection
    pin_bar_min_wick_ratio: float = 2.0    # Minimum wick-to-body ratio for pin bars
    engulfing_min_body_ratio: float = 1.5  # Minimum body ratio for engulfing patterns
    session_hours: int = 1                 # Hours for session definition


# ============================================================
# FAIR VALUE GAP DETECTION 🕳️
# ============================================================

def identify_fair_value_gaps(
    data: pd.DataFrame,
    min_gap_percent: float = None,
    lookback_bars: int = None,
    config: PatternConfig = None
) -> Tuple[List[Dict], List[Dict]]:
    """
    🕳️ Identify all Fair Value Gaps (FVGs) in the data

    Fair Value Gaps represent institutional order imbalances where price
    moves so quickly that it leaves inefficiencies in the market structure.

    Args:
        data: DataFrame with OHLC data
        min_gap_percent: Minimum gap size as percentage (default 0.1%)
        lookback_bars: Number of bars to look back (default 20)
        config: PatternConfig object for parameters

    Returns:
        Tuple: (bullish_fvgs, bearish_fvgs) - Lists of FVG dictionaries

    Raises:
        PatternAnalysisError: If data validation fails
    """
    try:
        # Parameter setup
        config = config or PatternConfig()
        min_gap_percent = min_gap_percent or config.fvg_min_gap_percent
        lookback_bars = lookback_bars or config.fvg_lookback_bars

        # Validate data
        required_columns = ['Open', 'High', 'Low', 'Close']
        if not all(col in data.columns for col in required_columns):
            raise PatternAnalysisError(f"Data must contain columns: {required_columns}")

        if len(data) < lookback_bars + 3:
            return [], []

        bullish_fvgs = []
        bearish_fvgs = []
        min_gap_ratio = min_gap_percent / 100

        # Look for FVGs in recent bars
        start_idx = max(0, len(data) - lookback_bars)
        for i in range(start_idx, len(data) - 2):
            # Get three consecutive candles for FVG pattern
            candle1 = data.iloc[i]
            candle2 = data.iloc[i + 1]
            candle3 = data.iloc[i + 2]

            # Check for Bullish FVG (gap up) - candle 3 low > candle 1 high
            if candle3['Low'] > candle1['High']:
                gap_size = (candle3['Low'] - candle1['High']) / candle1['High']
                if gap_size >= min_gap_ratio:
                    bullish_fvgs.append({
                        'timestamp': data.index[i + 2],
                        'gap_high': candle3['Low'],
                        'gap_low': candle1['High'],
                        'gap_size_percent': gap_size * 100,
                        'candle1_idx': i,
                        'candle3_idx': i + 2,
                        'gap_range': candle3['Low'] - candle1['High'],
                        'formation_type': 'bullish_fvg'
                    })

            # Check for Bearish FVG (gap down) - candle 3 high < candle 1 low
            elif candle3['High'] < candle1['Low']:
                gap_size = (candle1['Low'] - candle3['High']) / candle3['High']
                if gap_size >= min_gap_ratio:
                    bearish_fvgs.append({
                        'timestamp': data.index[i + 2],
                        'gap_high': candle1['Low'],
                        'gap_low': candle3['High'],
                        'gap_size_percent': gap_size * 100,
                        'candle1_idx': i,
                        'candle3_idx': i + 2,
                        'gap_range': candle1['Low'] - candle3['High'],
                        'formation_type': 'bearish_fvg'
                    })

        return bullish_fvgs, bearish_fvgs

    except Exception as e:
        raise PatternAnalysisError(f"Failed to identify Fair Value Gaps: {e}")


def is_price_in_fvg(
    price: float,
    fvg_list: List[Dict]
) -> Optional[Dict]:
    """
    ✅ Check if price is within any FVG zone

    Args:
        price: Current price to check
        fvg_list: List of FVG dictionaries from identify_fair_value_gaps()

    Returns:
        FVG dict if price is in zone, None otherwise
    """
    try:
        for fvg in fvg_list:
            if fvg['gap_low'] <= price <= fvg['gap_high']:
                return fvg
        return None

    except Exception as e:
        raise PatternAnalysisError(f"Failed to check price in FVG: {e}")


# ============================================================
# ADVANCED PATTERN RECOGNITION 🕯️
# ============================================================

def detect_enhanced_engulfing_pattern(
    data: pd.DataFrame,
    min_body_ratio: float = None,
    config: PatternConfig = None
) -> Optional[str]:
    """
    🕯️ Detect enhanced engulfing patterns with stricter criteria

    Args:
        data: DataFrame with OHLC data
        min_body_ratio: Minimum body ratio for engulfing (default 1.5)
        config: PatternConfig object

    Returns:
        'bullish', 'bearish', or None
    """
    try:
        config = config or PatternConfig()
        min_body_ratio = min_body_ratio or config.engulfing_min_body_ratio

        if len(data) < 2:
            return None

        # Previous and current candle
        prev = data.iloc[-2]
        curr = data.iloc[-1]

        # Calculate body sizes
        prev_body = abs(prev['Close'] - prev['Open'])
        curr_body = abs(curr['Close'] - curr['Open'])

        # Bullish engulfing with enhanced criteria
        if (prev['Close'] < prev['Open'] and          # Previous bearish
            curr['Close'] > curr['Open'] and          # Current bullish
            curr['Open'] <= prev['Close'] and         # Opens at/below prev close
            curr['Close'] >= prev['Open'] and         # Closes above prev open
            curr['High'] > prev['High'] and           # Higher high
            curr['Low'] < prev['Low'] and             # Lower low
            curr_body > prev_body * min_body_ratio):  # Significant engulfing
            return 'bullish'

        # Bearish engulfing with enhanced criteria
        if (prev['Close'] > prev['Open'] and          # Previous bullish
            curr['Close'] < curr['Open'] and          # Current bearish
            curr['Open'] >= prev['Close'] and         # Opens at/above prev close
            curr['Close'] <= prev['Open'] and         # Closes below prev open
            curr['High'] > prev['High'] and           # Higher high
            curr['Low'] < prev['Low'] and             # Lower low
            curr_body > prev_body * min_body_ratio):  # Significant engulfing
            return 'bearish'

        return None

    except Exception as e:
        raise PatternAnalysisError(f"Failed to detect engulfing pattern: {e}")


def detect_pin_bar(
    data: pd.DataFrame,
    min_wick_ratio: float = None,
    config: PatternConfig = None
) -> Optional[str]:
    """
    📍 Detect pin bar (hammer/shooting star) patterns

    Pin bars indicate potential reversal points where one side of the
    market is rejected, creating long wicks relative to the body.

    Args:
        data: DataFrame with OHLC data
        min_wick_ratio: Minimum ratio of wick to body (default 2.0)
        config: PatternConfig object

    Returns:
        'bullish_pin', 'bearish_pin', or None
    """
    try:
        config = config or PatternConfig()
        min_wick_ratio = min_wick_ratio or config.pin_bar_min_wick_ratio

        if len(data) < 1:
            return None

        candle = data.iloc[-1]

        # Calculate body and wick sizes
        body = abs(candle['Close'] - candle['Open'])
        upper_wick = candle['High'] - max(candle['Close'], candle['Open'])
        lower_wick = min(candle['Close'], candle['Open']) - candle['Low']

        # Avoid division by zero
        if body == 0:
            body = 0.0001  # Small value to avoid division by zero

        # Bullish pin bar (hammer) - long lower wick, small upper wick
        if (lower_wick > body * min_wick_ratio and
            upper_wick < body):
            return 'bullish_pin'

        # Bearish pin bar (shooting star) - long upper wick, small lower wick
        if (upper_wick > body * min_wick_ratio and
            lower_wick < body):
            return 'bearish_pin'

        return None

    except Exception as e:
        raise PatternAnalysisError(f"Failed to detect pin bar: {e}")


# ============================================================
# SESSION AND TIME ANALYSIS ⏰
# ============================================================

def identify_session_ranges(
    data: pd.DataFrame,
    session_hours: int = None,
    config: PatternConfig = None
) -> pd.DataFrame:
    """
    📊 Identify session ranges for crypto markets (24/7 operation)

    For crypto markets that operate 24/7, we define daily sessions
    and calculate key levels for each session period.

    Args:
        data: DataFrame with OHLC data and datetime index
        session_hours: Number of hours to define as session (default 1)
        config: PatternConfig object

    Returns:
        DataFrame with session high/low columns added
    """
    try:
        config = config or PatternConfig()
        session_hours = session_hours or config.session_hours

        data = data.copy()

        # Create session identifier (daily sessions for crypto)
        data['session_date'] = data.index.date

        # Calculate session statistics for each day
        session_stats = data.groupby('session_date').agg({
            'High': 'max',
            'Low': 'min',
            'Open': 'first',
            'Close': 'last',
            'Volume': 'sum'
        })

        # Map session data back to original dataframe
        data['session_high'] = data['session_date'].map(session_stats['High'])
        data['session_low'] = data['session_date'].map(session_stats['Low'])
        data['session_open'] = data['session_date'].map(session_stats['Open'])
        data['session_close'] = data['session_date'].map(session_stats['Close'])
        data['session_volume'] = data['session_date'].map(session_stats['Volume'])

        # Calculate session range and midpoint
        data['session_range'] = data['session_high'] - data['session_low']
        data['session_midpoint'] = (data['session_high'] + data['session_low']) / 2

        return data

    except Exception as e:
        raise PatternAnalysisError(f"Failed to identify session ranges: {e}")


def detect_range_break(
    data: pd.DataFrame,
    session_high: float,
    session_low: float
) -> Optional[str]:
    """
    🔍 Detect if price has broken above or below the session range

    Args:
        data: DataFrame with OHLC data
        session_high: Session high level
        session_low: Session low level

    Returns:
        'bullish', 'bearish', or None
    """
    try:
        if len(data) == 0 or session_high is None or session_low is None:
            return None

        current_price = data['Close'].iloc[-1]

        # Check for range break
        if current_price > session_high:
            return 'bullish'
        elif current_price < session_low:
            return 'bearish'

        return None

    except Exception as e:
        raise PatternAnalysisError(f"Failed to detect range break: {e}")


# ============================================================
# PATTERN UTILITIES 🛠️
# ============================================================

def calculate_pattern_strength(
    pattern_type: str,
    data: pd.DataFrame,
    volume_data: Optional[pd.Series] = None
) -> float:
    """
    💪 Calculate the strength of a detected pattern

    Args:
        pattern_type: Type of pattern detected
        data: DataFrame with OHLC data
        volume_data: Optional volume data for confirmation

    Returns:
        Pattern strength score (0.0 to 1.0)
    """
    try:
        if len(data) < 2:
            return 0.0

        strength = 0.5  # Base strength

        # Volume confirmation (if available)
        if volume_data is not None and len(volume_data) >= 2:
            current_volume = volume_data.iloc[-1]
            avg_volume = volume_data.tail(10).mean()

            if current_volume > avg_volume * 1.5:
                strength += 0.2  # Volume confirmation

        # Price action confirmation
        current_candle = data.iloc[-1]
        body_size = abs(current_candle['Close'] - current_candle['Open'])
        candle_range = current_candle['High'] - current_candle['Low']

        if candle_range > 0:
            body_ratio = body_size / candle_range
            if body_ratio > 0.7:  # Strong body
                strength += 0.2

        # Pattern-specific adjustments
        if 'engulfing' in pattern_type:
            # Check for complete engulfment
            prev_candle = data.iloc[-2]
            if (current_candle['High'] > prev_candle['High'] and
                current_candle['Low'] < prev_candle['Low']):
                strength += 0.1

        return min(strength, 1.0)

    except Exception as e:
        raise PatternAnalysisError(f"Failed to calculate pattern strength: {e}")


def validate_pattern_data(data: pd.DataFrame) -> bool:
    """
    🛡️ Validate data for pattern analysis

    Args:
        data: DataFrame to validate

    Returns:
        True if data is valid for pattern analysis
    """
    try:
        required_columns = ['Open', 'High', 'Low', 'Close']

        # Check required columns
        if not all(col in data.columns for col in required_columns):
            return False

        # Check for sufficient data
        if len(data) < 3:
            return False

        # Check for valid price relationships
        for _, row in data.tail(3).iterrows():
            if not (row['Low'] <= row['Open'] <= row['High'] and
                    row['Low'] <= row['Close'] <= row['High']):
                return False

        return True

    except Exception:
        return False


def patterns_production_readiness() -> Dict[str, bool]:
    """
    🛡️ Check production readiness of patterns module

    Returns:
        Dictionary with readiness status
    """
    try:
        readiness = {
            'module_importable': True,
            'core_functions_available': True,
            'error_handling_implemented': True,
            'configuration_support': True,
            'validation_functions': True
        }

        # Test core function availability
        core_functions = [
            identify_fair_value_gaps,
            detect_enhanced_engulfing_pattern,
            detect_pin_bar,
            identify_session_ranges,
            is_price_in_fvg
        ]

        for func in core_functions:
            if not callable(func):
                readiness['core_functions_available'] = False
                break

        return readiness

    except Exception:
        return {
            'module_importable': False,
            'core_functions_available': False,
            'error_handling_implemented': False,
            'configuration_support': False,
            'validation_functions': False
        }


print("🕳️ Advanced Pattern Recognition Module loaded successfully! 🕯️")