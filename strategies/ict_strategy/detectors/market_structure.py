from dataclasses import dataclass
from typing import Optional
import pandas as pd
from strategies.ict_strategy.detectors.liquidity_detector import LiquidityDetector

@dataclass
class MarketStructure:
    trend: str  # 'bullish', 'bearish', 'ranging'
    last_higher_high: Optional[float] = None
    last_higher_low: Optional[float] = None
    last_lower_high: Optional[float] = None
    last_lower_low: Optional[float] = None

class MarketStructureDetector:
    """
    Detects market structure: higher highs/lows (bullish) or lower highs/lows (bearish).

    ICT Theory: Market structure shifts signal trend changes. A break of structure
    (BOS) confirms continuation, while change of character (ChoCh) signals reversal.
    """

    def __init__(self, lookback: int = 5):
        self.lookback = lookback
        self.liquidity_detector = LiquidityDetector(lookback=lookback)

    def analyze(self, data: pd.DataFrame) -> MarketStructure:
        """
        Analyze market structure from OHLCV data.

        Args:
            data: OHLCV DataFrame

        Returns:
            MarketStructure object with trend and key levels
        """
        # Get swing highs and lows using liquidity detector
        pools = self.liquidity_detector.detect(data)

        highs = sorted([p for p in pools if p.type.value == 'high'],
                      key=lambda p: p.timestamp)
        lows = sorted([p for p in pools if p.type.value == 'low'],
                     key=lambda p: p.timestamp)

        if len(highs) < 2 or len(lows) < 2:
            return MarketStructure(trend='ranging')

        # Check for higher highs and higher lows (bullish)
        recent_highs = highs[-2:]
        recent_lows = lows[-2:]

        has_higher_high = recent_highs[-1].price > recent_highs[-2].price
        has_higher_low = recent_lows[-1].price > recent_lows[-2].price

        # Check for lower highs and lower lows (bearish)
        has_lower_high = recent_highs[-1].price < recent_highs[-2].price
        has_lower_low = recent_lows[-1].price < recent_lows[-2].price

        if has_higher_high and has_higher_low:
            return MarketStructure(
                trend='bullish',
                last_higher_high=recent_highs[-1].price,
                last_higher_low=recent_lows[-1].price
            )
        elif has_lower_high and has_lower_low:
            return MarketStructure(
                trend='bearish',
                last_lower_high=recent_highs[-1].price,
                last_lower_low=recent_lows[-1].price
            )
        else:
            return MarketStructure(trend='ranging')
