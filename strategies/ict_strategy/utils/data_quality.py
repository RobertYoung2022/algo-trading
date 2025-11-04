from dataclasses import dataclass
from typing import List
import pandas as pd
from core.trading_functions.utils.data_quality_validator import DataQualityValidator

@dataclass
class ValidationResult:
    is_valid: bool
    quality_score: float
    timeframe: str
    issues: List[str]

class ICTDataValidator:
    """
    Wrapper around DataQualityValidator specialized for ICT strategy requirements.

    ICT strategies require:
    - No gaps in data (FVG detection needs consecutive candles)
    - No price anomalies (affects liquidity pool identification)
    - Sufficient volume (confirms real market moves vs artifacts)
    - Consistent timeframe (multi-TF analysis requires aligned data)
    """

    def __init__(self, min_quality_score: float = 80.0):
        self.validator = DataQualityValidator()
        self.min_quality_score = min_quality_score

    def validate_for_fvg_detection(self, data: pd.DataFrame, timeframe: str) -> ValidationResult:
        """
        Validate data quality for FVG detection.

        Args:
            data: OHLCV DataFrame with datetime index
            timeframe: Timeframe string (e.g., '1H', '15m')

        Returns:
            ValidationResult with pass/fail and details
        """
        issues = []

        # Check for required columns (lowercase as provided in test data)
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                timeframe=timeframe,
                issues=issues
            )

        # Check for gaps (missing timestamps)
        if len(data) < 3:
            issues.append("Insufficient data: need at least 3 candles for FVG detection")

        # Check for NaN values
        nan_count = data[required_cols].isna().sum().sum()
        if nan_count > 0:
            issues.append(f"Data contains {nan_count} missing values")

        # Check for anomalies (prices outside 3 std devs)
        for col in ['open', 'high', 'low', 'close']:
            if col in data.columns:
                # Only check non-NaN values
                col_data = data[col].dropna()
                if len(col_data) > 0:
                    mean = col_data.mean()
                    std = col_data.std()
                    if std > 0:  # Avoid division by zero
                        anomalies = col_data[(col_data > mean + 3*std) | (col_data < mean - 3*std)]
                        if len(anomalies) > 0:
                            issues.append(f"Found {len(anomalies)} price anomalies in {col}")

        # Create a copy with standardized column names for the core validator
        data_copy = data.copy()

        # Standardize column names to match what the core validator expects
        column_mapping = {
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }
        data_copy = data_copy.rename(columns=column_mapping)

        # Use DataQualityValidator for comprehensive checks
        core_result = self.validator.validate_for_backtesting(data_copy, min_periods=3)
        quality_score = core_result.quality_score

        # Add any issues from core validator
        if core_result.critical_issues:
            issues.extend(core_result.critical_issues)
        if core_result.warnings:
            issues.extend(core_result.warnings)

        # Apply penalties for ICT-specific issues
        # Penalize for missing data heavily (ICT needs complete candle sequences)
        if nan_count > 0:
            missing_pct = (nan_count / (len(data) * len(required_cols))) * 100
            quality_score -= min(missing_pct * 3, 50.0)  # Up to 50 point penalty

        # Penalize for anomalies (critical for ICT liquidity detection)
        for col in ['open', 'high', 'low', 'close']:
            if col in data.columns:
                col_data = data[col].dropna()
                if len(col_data) > 0:
                    mean = col_data.mean()
                    std = col_data.std()
                    if std > 0:
                        anomalies = col_data[(col_data > mean + 3*std) | (col_data < mean - 3*std)]
                        if len(anomalies) > 0:
                            anomaly_pct = (len(anomalies) / len(col_data)) * 100
                            quality_score -= min(anomaly_pct * 10, 30.0)  # Up to 30 points per column

        quality_score = max(0.0, min(100.0, quality_score))

        is_valid = quality_score >= self.min_quality_score and len(issues) == 0

        # Convert to Python bool to ensure identity checks work in tests
        is_valid = bool(is_valid)

        return ValidationResult(
            is_valid=is_valid,
            quality_score=quality_score,
            timeframe=timeframe,
            issues=issues
        )
