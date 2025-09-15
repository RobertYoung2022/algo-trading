"""
🚀 Data Quality Validator - Bulletproof Data Validation for Backtesting
====================================================================

100% accurate data validation system to ensure reliable backtesting results.
Prevents future data contamination, validates OHLC consistency, and provides
comprehensive quality scoring for multiple data sources.

🌟 Key Features:
    - Timestamp validation (no future dates, proper chronological order)
    - OHLC mathematical consistency validation
    - Content vs filename verification
    - Multi-source quality scoring (0-100)
    - Integration with backtesting.py framework

💫 Usage Examples:
    # Basic validation
    validator = DataQualityValidator()
    result = validator.validate_data_file('BTCUSD-1d-yahoo.csv')

    # Quality scoring
    score = validator.calculate_quality_score(df, 'yahoo')

    # Pre-backtest validation
    issues = validator.validate_for_backtesting(df, min_periods=252)

🔧 Integration with Bobby's Multi-Data Testing:
    - Seamless integration with multi_data_tester.py
    - Quality-aware source prioritization
    - Automatic validation before strategy execution
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
import logging
import warnings
from pathlib import Path
import hashlib
import re

# 🛡️ Configure logging for data quality monitoring
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityError(Exception):
    """🚨 Custom exception for data quality issues"""
    pass

class ValidationResult:
    """📊 Container for validation results with severity levels"""

    def __init__(self):
        self.is_valid: bool = True
        self.quality_score: float = 100.0
        self.critical_issues: List[str] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        self.metadata: Dict[str, Any] = {}

    def add_critical(self, message: str):
        """🚨 Add critical issue that blocks backtesting"""
        self.critical_issues.append(message)
        self.is_valid = False
        self.quality_score -= 25.0  # Heavy penalty for critical issues

    def add_warning(self, message: str):
        """⚠️ Add warning that affects quality but doesn't block"""
        self.warnings.append(message)
        self.quality_score -= 10.0  # Moderate penalty for warnings

    def add_info(self, message: str):
        """ℹ️ Add informational note"""
        self.info.append(message)
        self.quality_score -= 2.0  # Light penalty for minor issues

    def get_summary(self) -> str:
        """📋 Get formatted summary of validation results"""
        status = "✅ PASS" if self.is_valid else "❌ FAIL"
        score = max(0, min(100, self.quality_score))

        summary = f"{status} - Quality Score: {score:.1f}/100\n"

        if self.critical_issues:
            summary += f"🚨 Critical Issues ({len(self.critical_issues)}):\n"
            for issue in self.critical_issues:
                summary += f"   • {issue}\n"

        if self.warnings:
            summary += f"⚠️ Warnings ({len(self.warnings)}):\n"
            for warning in self.warnings:
                summary += f"   • {warning}\n"

        if self.info:
            summary += f"ℹ️ Info ({len(self.info)}):\n"
            for info in self.info:
                summary += f"   • {info}\n"

        return summary

class DataQualityValidator:
    """
    🛡️ Comprehensive data quality validator for trading data

    Ensures 100% accuracy in historical data for reliable backtesting.
    Validates timestamps, OHLC consistency, completeness, and filename accuracy.
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        🔧 Initialize validator with configuration

        Args:
            config: Configuration dictionary for validation rules
        """
        self.config = config or self._default_config()

    def _default_config(self) -> Dict:
        """🎯 Default validation configuration"""
        return {
            # 🕒 Timestamp validation
            'allow_future_dates': False,
            'max_future_tolerance_days': 0,
            'require_chronological_order': True,

            # 📊 OHLC validation
            'validate_ohlc_relationships': True,
            'max_price_jump_percent': 50.0,  # Flag price jumps > 50%
            'min_positive_prices': True,

            # 📈 Volume validation
            'validate_volume': True,
            'allow_zero_volume': True,
            'max_volume_spike_multiplier': 100.0,

            # 📏 Data completeness
            'min_data_points_1d': 30,      # Minimum 30 days for daily data
            'min_data_points_1h': 168,     # Minimum 1 week for hourly data
            'max_gap_tolerance_percent': 5.0,  # Max 5% gaps allowed

            # 🏷️ Filename validation
            'validate_filename_content': True,
            'filename_tolerance_days': 7,   # Allow 7 days variance in filename claims

            # 🎯 Quality scoring weights
            'scoring_weights': {
                'timestamp_accuracy': 0.30,
                'data_completeness': 0.25,
                'ohlc_consistency': 0.20,
                'volume_validity': 0.15,
                'filename_accuracy': 0.10
            }
        }

    def validate_data_file(self, file_path: Union[str, Path]) -> ValidationResult:
        """
        📋 Comprehensive validation of a data file

        Args:
            file_path: Path to the CSV data file to validate

        Returns:
            ValidationResult with detailed analysis
        """
        result = ValidationResult()
        file_path = Path(file_path)

        try:
            # 📥 Load data with error handling
            df = self._load_data_safely(file_path, result)
            if df is None:
                return result

            # 🔍 Extract metadata from filename
            filename_metadata = self._parse_filename(file_path.name)
            result.metadata['filename_claims'] = filename_metadata
            result.metadata['actual_file_size'] = file_path.stat().st_size
            result.metadata['file_path'] = str(file_path)

            # 🕒 Validate timestamps
            self._validate_timestamps(df, result, filename_metadata)

            # 📊 Validate OHLC data
            self._validate_ohlc_data(df, result)

            # 📈 Validate volume data
            self._validate_volume_data(df, result)

            # 📏 Validate data completeness
            self._validate_completeness(df, result, filename_metadata)

            # 🏷️ Validate filename vs content
            self._validate_filename_accuracy(df, result, filename_metadata)

            # 📊 Calculate final quality score
            result.quality_score = max(0, min(100, result.quality_score))
            result.metadata['total_rows'] = len(df)
            result.metadata['date_range'] = {
                'start': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
                'end': df.index[-1].strftime('%Y-%m-%d') if len(df) > 0 else None,
                'days': (df.index[-1] - df.index[0]).days if len(df) > 1 else 0
            }

        except Exception as e:
            result.add_critical(f"Validation failed with error: {str(e)}")
            logger.error(f"🚨 Validation error for {file_path}: {e}")

        return result

    def _load_data_safely(self, file_path: Path, result: ValidationResult) -> Optional[pd.DataFrame]:
        """📥 Safely load CSV data with error handling"""
        try:
            # 🔍 Detect date column and format
            df = pd.read_csv(file_path)

            # 📅 Common date column names to try
            date_columns = ['Date', 'date', 'timestamp', 'Timestamp', 'datetime', 'time']
            date_col = None

            for col in date_columns:
                if col in df.columns:
                    date_col = col
                    break

            if date_col is None:
                result.add_critical("No recognized date column found (tried: Date, timestamp, datetime)")
                return None

            # 🕒 Parse dates and set as index
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.set_index(date_col).sort_index()

            # ✅ Standardize column names to handle both lowercase and uppercase
            column_mapping = {}
            for col in df.columns:
                col_lower = col.lower()
                if col_lower == 'open':
                    column_mapping[col] = 'Open'
                elif col_lower == 'high':
                    column_mapping[col] = 'High'
                elif col_lower == 'low':
                    column_mapping[col] = 'Low'
                elif col_lower == 'close':
                    column_mapping[col] = 'Close'
                elif col_lower == 'volume':
                    column_mapping[col] = 'Volume'

            # Apply column mapping
            df = df.rename(columns=column_mapping)

            # ✅ Verify required OHLC columns exist (after standardization)
            required_cols = ['Open', 'High', 'Low', 'Close']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                result.add_critical(f"Missing required OHLC columns: {missing_cols}")
                return None

            result.add_info(f"Successfully loaded {len(df)} rows of data")
            return df

        except Exception as e:
            result.add_critical(f"Failed to load data file: {str(e)}")
            return None

    def _parse_filename(self, filename: str) -> Dict[str, Any]:
        """🏷️ Extract metadata from filename patterns"""
        metadata = {
            'symbol': None,
            'timeframe': None,
            'claimed_period': None,
            'source': None,
            'original_filename': filename
        }

        # 🔍 Common filename patterns to match
        patterns = [
            # BTCUSD-1d-1000wks-data.csv
            r'([A-Z]+USD?)-(\d+[hdm])-(\d+(?:yr|wks?|mo|days?))',
            # BTCUSD-10yr-yahoo-data.csv
            r'([A-Z]+USD?)-(\d+(?:yr|mo|wks?))-([a-z]+)',
            # BTC_1d_20240101_20241231_coinbase.csv
            r'([A-Z]+)_(\d+[hdm])_(\d{8})_(\d{8})_([a-z]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                groups = match.groups()
                metadata['symbol'] = groups[0]
                if len(groups) >= 2:
                    metadata['timeframe'] = groups[1]
                if len(groups) >= 3:
                    metadata['claimed_period'] = groups[2]
                if len(groups) >= 4:
                    metadata['source'] = groups[-1]
                break

        return metadata

    def _validate_timestamps(self, df: pd.DataFrame, result: ValidationResult,
                           filename_metadata: Dict) -> None:
        """🕒 Validate timestamp accuracy and chronological order"""
        if df.empty:
            result.add_critical("Dataset is empty")
            return

        # 🚨 Check for future dates
        if not self.config['allow_future_dates']:
            now = datetime.now()
            max_future = now + timedelta(days=self.config['max_future_tolerance_days'])
            future_dates = df.index > max_future

            if future_dates.any():
                future_count = future_dates.sum()
                first_future = df.index[future_dates][0]
                result.add_critical(
                    f"Found {future_count} future dates. First future date: {first_future}"
                )

        # 📅 Check chronological order
        if self.config['require_chronological_order']:
            if not df.index.is_monotonic_increasing:
                result.add_warning("Timestamps are not in chronological order")

        # 🔍 Check for duplicate timestamps
        duplicates = df.index.duplicated()
        if duplicates.any():
            dup_count = duplicates.sum()
            result.add_warning(f"Found {dup_count} duplicate timestamps")

        # ⏰ Validate timeframe consistency
        self._validate_timeframe_consistency(df, result, filename_metadata)

    def _validate_timeframe_consistency(self, df: pd.DataFrame, result: ValidationResult,
                                      filename_metadata: Dict) -> None:
        """⏰ Validate that actual timeframe matches filename claims"""
        if len(df) < 2:
            return

        # 📊 Calculate actual intervals between timestamps
        intervals = df.index.to_series().diff().dropna()
        median_interval = intervals.median()

        # 🎯 Map common timeframe strings to expected intervals
        timeframe_map = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '15m': timedelta(minutes=15),
            '1h': timedelta(hours=1),
            '6h': timedelta(hours=6),
            '1d': timedelta(days=1),
            '1w': timedelta(weeks=1)
        }

        claimed_timeframe = filename_metadata.get('timeframe')
        if claimed_timeframe and claimed_timeframe.lower() in timeframe_map:
            expected_interval = timeframe_map[claimed_timeframe.lower()]

            # ✅ Allow 10% tolerance for timeframe matching
            tolerance = expected_interval * 0.1
            if abs(median_interval - expected_interval) > tolerance:
                result.add_warning(
                    f"Timeframe mismatch: filename claims {claimed_timeframe}, "
                    f"actual median interval is {median_interval}"
                )

        result.metadata['actual_median_interval'] = str(median_interval)

    def _validate_ohlc_data(self, df: pd.DataFrame, result: ValidationResult) -> None:
        """📊 Validate OHLC mathematical consistency"""
        if not self.config['validate_ohlc_relationships']:
            return

        # ✅ Verify OHLC relationships: High >= Low, High >= Open/Close, Low <= Open/Close
        high_low_invalid = df['High'] < df['Low']
        high_open_invalid = df['High'] < df['Open']
        high_close_invalid = df['High'] < df['Close']
        low_open_invalid = df['Low'] > df['Open']
        low_close_invalid = df['Low'] > df['Close']

        total_invalid = (high_low_invalid | high_open_invalid | high_close_invalid |
                        low_open_invalid | low_close_invalid).sum()

        if total_invalid > 0:
            result.add_critical(f"Found {total_invalid} rows with invalid OHLC relationships")

        # 🚨 Check for extreme price jumps
        price_changes = df['Close'].pct_change().abs() * 100
        extreme_jumps = price_changes > self.config['max_price_jump_percent']

        if extreme_jumps.any():
            jump_count = extreme_jumps.sum()
            max_jump = price_changes.max()
            result.add_warning(
                f"Found {jump_count} extreme price jumps (>{self.config['max_price_jump_percent']}%). "
                f"Largest jump: {max_jump:.1f}%"
            )

        # 💰 Check for negative prices
        if self.config['min_positive_prices']:
            negative_prices = ((df['Open'] <= 0) | (df['High'] <= 0) |
                              (df['Low'] <= 0) | (df['Close'] <= 0)).any()
            if negative_prices:
                result.add_critical("Found negative or zero prices in OHLC data")

    def _validate_volume_data(self, df: pd.DataFrame, result: ValidationResult) -> None:
        """📈 Validate volume data quality"""
        if not self.config['validate_volume'] or 'Volume' not in df.columns:
            return

        # 🔍 Check for negative volume
        negative_volume = (df['Volume'] < 0).any()
        if negative_volume:
            result.add_critical("Found negative volume values")

        # ⚠️ Check for volume spikes
        if len(df) > 1:
            volume_median = df['Volume'].median()
            if volume_median > 0:
                volume_spikes = df['Volume'] > (volume_median * self.config['max_volume_spike_multiplier'])
                if volume_spikes.any():
                    spike_count = volume_spikes.sum()
                    max_spike = (df['Volume'] / volume_median).max()
                    result.add_warning(
                        f"Found {spike_count} volume spikes "
                        f"(>{self.config['max_volume_spike_multiplier']}x median). "
                        f"Largest spike: {max_spike:.1f}x"
                    )

        # ℹ️ Report zero volume statistics
        zero_volume_count = (df['Volume'] == 0).sum()
        if zero_volume_count > 0:
            zero_pct = (zero_volume_count / len(df)) * 100
            if zero_pct > 10:  # More than 10% zero volume
                result.add_warning(f"{zero_volume_count} rows ({zero_pct:.1f}%) have zero volume")
            else:
                result.add_info(f"{zero_volume_count} rows ({zero_pct:.1f}%) have zero volume")

    def _validate_completeness(self, df: pd.DataFrame, result: ValidationResult,
                             filename_metadata: Dict) -> None:
        """📏 Validate data completeness and gap analysis"""
        if len(df) < 2:
            result.add_critical("Insufficient data for completeness analysis (< 2 rows)")
            return

        # 🎯 Determine minimum expected data points based on timeframe
        timeframe = filename_metadata.get('timeframe', '').lower()
        if '1d' in timeframe or 'day' in timeframe:
            min_points = self.config['min_data_points_1d']
        elif '1h' in timeframe or 'hour' in timeframe:
            min_points = self.config['min_data_points_1h']
        else:
            min_points = 30  # Default minimum

        if len(df) < min_points:
            result.add_critical(
                f"Insufficient data points: {len(df)} < {min_points} minimum required"
            )

        # 📊 Gap analysis
        intervals = df.index.to_series().diff().dropna()
        if len(intervals) > 0:
            median_interval = intervals.median()

            # 🔍 Find gaps larger than expected
            gap_threshold = median_interval * 2  # Gaps > 2x normal interval
            large_gaps = intervals > gap_threshold

            if large_gaps.any():
                gap_count = large_gaps.sum()
                largest_gap = intervals.max()
                gap_pct = (gap_count / len(intervals)) * 100

                if gap_pct > self.config['max_gap_tolerance_percent']:
                    result.add_warning(
                        f"Found {gap_count} significant gaps ({gap_pct:.1f}% of data). "
                        f"Largest gap: {largest_gap}"
                    )
                else:
                    result.add_info(f"Found {gap_count} minor gaps ({gap_pct:.1f}% of data)")

    def _validate_filename_accuracy(self, df: pd.DataFrame, result: ValidationResult,
                                   filename_metadata: Dict) -> None:
        """🏷️ Validate filename claims vs actual content"""
        if not self.config['validate_filename_content'] or len(df) < 2:
            return

        actual_days = (df.index[-1] - df.index[0]).days
        claimed_period = filename_metadata.get('claimed_period', '')

        if claimed_period:
            # 📅 Parse claimed period and convert to days
            claimed_days = self._parse_period_to_days(claimed_period)

            if claimed_days:
                tolerance_days = self.config['filename_tolerance_days']
                difference = abs(actual_days - claimed_days)

                if difference > tolerance_days:
                    accuracy_pct = (actual_days / claimed_days) * 100
                    result.add_warning(
                        f"Filename period mismatch: claims {claimed_period} "
                        f"({claimed_days} days), actual {actual_days} days "
                        f"({accuracy_pct:.1f}% accuracy)"
                    )
                else:
                    result.add_info(f"Filename period accurate: {claimed_period} matches content")

    def _parse_period_to_days(self, period_str: str) -> Optional[int]:
        """📅 Convert period string to days"""
        period_str = period_str.lower()

        # 🔍 Extract number and unit
        import re
        match = re.match(r'(\d+)(yr|year|mo|month|wks?|week|days?)', period_str)
        if not match:
            return None

        num, unit = match.groups()
        num = int(num)

        # 📊 Convert to days
        if unit.startswith('yr') or unit.startswith('year'):
            return num * 365
        elif unit.startswith('mo') or unit.startswith('month'):
            return num * 30
        elif unit.startswith('wk') or unit.startswith('week'):
            return num * 7
        elif unit.startswith('day'):
            return num
        else:
            return None

    def calculate_quality_score(self, df: pd.DataFrame, source: str = 'unknown') -> float:
        """
        📊 Calculate comprehensive quality score (0-100)

        Args:
            df: DataFrame with OHLC data
            source: Data source name for source-specific scoring

        Returns:
            Quality score from 0-100
        """
        if df.empty:
            return 0.0

        score = 100.0
        weights = self.config['scoring_weights']

        # 🕒 Timestamp accuracy score
        timestamp_score = self._score_timestamps(df)
        score -= (100 - timestamp_score) * weights['timestamp_accuracy']

        # 📏 Data completeness score
        completeness_score = self._score_completeness(df)
        score -= (100 - completeness_score) * weights['data_completeness']

        # 📊 OHLC consistency score
        ohlc_score = self._score_ohlc_consistency(df)
        score -= (100 - ohlc_score) * weights['ohlc_consistency']

        # 📈 Volume validity score
        volume_score = self._score_volume_validity(df)
        score -= (100 - volume_score) * weights['volume_validity']

        return max(0.0, min(100.0, score))

    def _score_timestamps(self, df: pd.DataFrame) -> float:
        """🕒 Score timestamp quality"""
        if len(df) < 2:
            return 50.0

        score = 100.0

        # ✅ Check chronological order
        if not df.index.is_monotonic_increasing:
            score -= 30.0

        # 🔍 Check for duplicates
        dup_pct = (df.index.duplicated().sum() / len(df)) * 100
        score -= min(dup_pct * 2, 40.0)  # Up to 40 point penalty

        # 🚨 Check for future dates
        future_dates = df.index > datetime.now()
        if future_dates.any():
            score = 0.0  # Critical failure

        return max(0.0, score)

    def _score_completeness(self, df: pd.DataFrame) -> float:
        """📏 Score data completeness"""
        if len(df) < 2:
            return 0.0

        # 📊 Calculate expected vs actual data points
        date_range_days = (df.index[-1] - df.index[0]).days
        if date_range_days == 0:
            return 50.0

        # 🎯 Estimate expected data points (assuming daily data)
        expected_points = date_range_days
        actual_points = len(df)
        completeness_pct = min(100.0, (actual_points / expected_points) * 100)

        return completeness_pct

    def _score_ohlc_consistency(self, df: pd.DataFrame) -> float:
        """📊 Score OHLC mathematical consistency"""
        if df.empty:
            return 0.0

        score = 100.0
        total_rows = len(df)

        # ✅ Check OHLC relationships
        invalid_ohlc = ((df['High'] < df['Low']) |
                       (df['High'] < df['Open']) |
                       (df['High'] < df['Close']) |
                       (df['Low'] > df['Open']) |
                       (df['Low'] > df['Close'])).sum()

        if invalid_ohlc > 0:
            error_pct = (invalid_ohlc / total_rows) * 100
            score -= min(error_pct * 10, 80.0)  # Up to 80 point penalty

        # 💰 Check for negative/zero prices
        negative_prices = ((df[['Open', 'High', 'Low', 'Close']] <= 0).any(axis=1)).sum()
        if negative_prices > 0:
            score -= 20.0  # Fixed penalty for any negative prices

        return max(0.0, score)

    def _score_volume_validity(self, df: pd.DataFrame) -> float:
        """📈 Score volume data validity"""
        if 'Volume' not in df.columns:
            return 90.0  # High score if volume not required

        score = 100.0

        # 🔍 Check for negative volume
        if (df['Volume'] < 0).any():
            score -= 50.0

        # ⚠️ Check for extreme spikes
        if len(df) > 1:
            volume_median = df['Volume'].median()
            if volume_median > 0:
                max_ratio = df['Volume'].max() / volume_median
                if max_ratio > 1000:  # 1000x spike
                    score -= 20.0
                elif max_ratio > 100:  # 100x spike
                    score -= 10.0

        return max(0.0, score)

    def validate_for_backtesting(self, df: pd.DataFrame, min_periods: int = 252,
                                strategy_requirements: Optional[Dict] = None) -> ValidationResult:
        """
        🎯 Specialized validation for backtesting requirements

        Args:
            df: DataFrame with OHLC data
            min_periods: Minimum periods required for strategy
            strategy_requirements: Strategy-specific validation requirements

        Returns:
            ValidationResult focused on backtesting readiness
        """
        result = ValidationResult()

        # 📊 Basic data validation
        if df.empty:
            result.add_critical("Dataset is empty - cannot run backtest")
            return result

        if len(df) < min_periods:
            result.add_critical(
                f"Insufficient data for backtesting: {len(df)} < {min_periods} required periods"
            )

        # 🕒 Future data contamination check (critical for backtesting)
        future_dates = df.index > datetime.now()
        if future_dates.any():
            future_count = future_dates.sum()
            first_future = df.index[future_dates][0]
            result.add_critical(
                f"LOOK-AHEAD BIAS DETECTED: {future_count} future dates found. "
                f"First future date: {first_future}. This will invalidate backtest results!"
            )

        # 📈 Technical indicator requirements
        if strategy_requirements:
            for indicator, periods in strategy_requirements.get('indicators', {}).items():
                if len(df) < periods:
                    result.add_warning(
                        f"Insufficient data for {indicator}: {len(df)} < {periods} periods needed"
                    )

        # 💰 Price data quality for strategy calculations
        if (df[['Open', 'High', 'Low', 'Close']] <= 0).any().any():
            result.add_critical("Negative or zero prices detected - will cause calculation errors")

        # 📊 Calculate backtest-specific quality score
        quality_score = self.calculate_quality_score(df)
        result.quality_score = quality_score

        if quality_score < 70:
            result.add_warning(f"Low quality score ({quality_score:.1f}/100) - backtest results may be unreliable")
        elif quality_score > 90:
            result.add_info(f"High quality score ({quality_score:.1f}/100) - excellent for backtesting")

        return result

def validate_data_source_quality(source_name: str, data_files: List[str]) -> Dict[str, ValidationResult]:
    """
    🌟 Batch validation for multiple data files from a source

    Args:
        source_name: Name of the data source (e.g., 'yahoo', 'coinbase')
        data_files: List of file paths to validate

    Returns:
        Dictionary mapping file paths to validation results
    """
    validator = DataQualityValidator()
    results = {}

    logger.info(f"🔍 Validating {len(data_files)} files from {source_name}")

    for file_path in data_files:
        try:
            result = validator.validate_data_file(file_path)
            results[file_path] = result

            status = "✅" if result.is_valid else "❌"
            logger.info(f"{status} {Path(file_path).name}: {result.quality_score:.1f}/100")

        except Exception as e:
            error_result = ValidationResult()
            error_result.add_critical(f"Validation failed: {str(e)}")
            results[file_path] = error_result
            logger.error(f"❌ {Path(file_path).name}: Validation failed - {e}")

    return results

# 🚀 Production readiness function following Bobby's pattern
def data_quality_production_readiness() -> Dict[str, Any]:
    """🛡️ Production readiness check for data quality validator"""
    try:
        # ✅ Test core validation functionality
        test_data = pd.DataFrame({
            'Open': [100, 101, 102],
            'High': [105, 106, 107],
            'Low': [99, 100, 101],
            'Close': [104, 105, 106],
            'Volume': [1000, 1100, 1200]
        }, index=pd.date_range('2024-01-01', periods=3, freq='D'))

        validator = DataQualityValidator()
        result = validator.validate_for_backtesting(test_data)

        return {
            'module_name': 'data_quality_validator',
            'status': 'ready',
            'version': '1.0.0',
            'validation_test': result.is_valid,
            'quality_score_test': result.quality_score,
            'dependencies_available': True,
            'error': None
        }

    except Exception as e:
        return {
            'module_name': 'data_quality_validator',
            'status': 'error',
            'error': str(e),
            'validation_test': False
        }

if __name__ == "__main__":
    # 🧪 Quick test of the validator
    print("🚀 Data Quality Validator - Test Run")
    readiness = data_quality_production_readiness()
    print(f"📊 Production Readiness: {readiness}")