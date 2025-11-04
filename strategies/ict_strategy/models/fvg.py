from dataclasses import dataclass
from datetime import datetime
from enum import Enum

class FVGType(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"

@dataclass
class FVG:
    """
    Fair Value Gap (FVG) - A 3-candle formation where candle 2 has a gap
    that candle 3 doesn't fill.

    Bullish FVG: Candle 1 high < Candle 3 low (gap between them)
    Bearish FVG: Candle 1 low > Candle 3 high (gap between them)
    """
    type: FVGType
    high: float  # Highest point of 3-candle formation
    low: float   # Lowest point of 3-candle formation
    gap_high: float  # Top of the gap
    gap_low: float   # Bottom of the gap
    timestamp: datetime
    timeframe: str
    quality_score: float  # From DataQualityValidator (0-100)
    fill_percentage: float = 0.0
    invalidated: bool = False
    aligned_with_htf: bool = False  # Whether FVG aligns with higher timeframe bias

    def __post_init__(self):
        """Validate edge cases on initialization"""
        if self.gap_high <= self.gap_low:
            raise ValueError(f"gap_high ({self.gap_high}) must be > gap_low ({self.gap_low})")
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) must be >= low ({self.low})")
        if not (0 <= self.quality_score <= 100):
            raise ValueError(f"quality_score ({self.quality_score}) must be between 0 and 100")
        if not (0 <= self.fill_percentage <= 100):
            raise ValueError(f"fill_percentage ({self.fill_percentage}) must be between 0 and 100")

    @property
    def gap_size(self) -> float:
        """Size of the gap in price units"""
        return self.gap_high - self.gap_low

    def is_valid(self) -> bool:
        """FVG is valid if not invalidated and fill < 100%"""
        return not self.invalidated and self.fill_percentage < 100.0

    def update_fill_percentage(self, current_price: float):
        """Update how much of the gap has been filled"""
        if self.type == FVGType.BULLISH:
            # Bullish FVG: price coming down into gap
            if current_price <= self.gap_low:
                self.fill_percentage = 100.0
                self.invalidated = True
            elif current_price < self.gap_high:
                filled = self.gap_high - current_price
                self.fill_percentage = (filled / self.gap_size) * 100.0
        else:  # BEARISH
            # Bearish FVG: price coming up into gap
            if current_price >= self.gap_high:
                self.fill_percentage = 100.0
                self.invalidated = True
            elif current_price > self.gap_low:
                filled = current_price - self.gap_low
                self.fill_percentage = (filled / self.gap_size) * 100.0
