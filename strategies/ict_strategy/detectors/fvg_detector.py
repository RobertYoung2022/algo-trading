from typing import List
import pandas as pd
from strategies.ict_strategy.models.fvg import FVG, FVGType
from strategies.ict_strategy.utils.data_quality import ICTDataValidator

class FVGDetector:
    """
    Detects Fair Value Gaps (FVGs) in OHLCV data.

    FVG Definition:
    - Bullish FVG: 3-candle pattern where candle[0].high < candle[2].low
    - Bearish FVG: 3-candle pattern where candle[0].low > candle[2].high

    The "gap" is the space between candle 0 and candle 2 that candle 1 creates.
    """

    def __init__(self, timeframe: str, min_gap_size: float = 0.0,
                 validate_quality: bool = True):
        self.timeframe = timeframe
        self.min_gap_size = min_gap_size
        self.validate_quality = validate_quality
        self.validator = ICTDataValidator() if validate_quality else None

    def detect(self, data: pd.DataFrame) -> List[FVG]:
        """
        Detect all FVGs in the provided data.

        Args:
            data: OHLCV DataFrame with datetime index

        Returns:
            List of FVG objects
        """
        # Validate data quality
        if self.validate_quality:
            validation = self.validator.validate_for_fvg_detection(data, self.timeframe)
            if not validation.is_valid:
                raise ValueError(f"Data quality too low: {validation.issues}")
            quality_score = validation.quality_score
        else:
            quality_score = 100.0

        fvgs = []

        # Need at least 3 candles to detect FVG
        if len(data) < 3:
            return fvgs

        # Scan through data with 3-candle window
        for i in range(len(data) - 2):
            candle_0 = data.iloc[i]
            candle_1 = data.iloc[i + 1]
            candle_2 = data.iloc[i + 2]

            # Check for bullish FVG (gap up)
            if candle_0['high'] < candle_2['low']:
                gap_size = candle_2['low'] - candle_0['high']
                if gap_size >= self.min_gap_size:
                    fvg = FVG(
                        type=FVGType.BULLISH,
                        high=max(candle_0['high'], candle_1['high'], candle_2['high']),
                        low=min(candle_0['low'], candle_1['low'], candle_2['low']),
                        gap_high=candle_2['low'],
                        gap_low=candle_0['high'],
                        timestamp=data.index[i + 2],  # FVG confirmed on candle 2
                        timeframe=self.timeframe,
                        quality_score=quality_score
                    )
                    fvgs.append(fvg)

            # Check for bearish FVG (gap down)
            elif candle_0['low'] > candle_2['high']:
                gap_size = candle_0['low'] - candle_2['high']
                if gap_size >= self.min_gap_size:
                    fvg = FVG(
                        type=FVGType.BEARISH,
                        high=max(candle_0['high'], candle_1['high'], candle_2['high']),
                        low=min(candle_0['low'], candle_1['low'], candle_2['low']),
                        gap_high=candle_0['low'],
                        gap_low=candle_2['high'],
                        timestamp=data.index[i + 2],
                        timeframe=self.timeframe,
                        quality_score=quality_score
                    )
                    fvgs.append(fvg)

        return fvgs
