from typing import List
import pandas as pd
from strategies.ict_strategy.models.liquidity_pool import LiquidityPool, LiquidityType

class LiquidityDetector:
    """
    Detects liquidity pools at swing highs and lows.

    ICT Theory: Smart money targets liquidity pools (clusters of stops)
    before true moves. A "liquidity sweep" is when price briefly takes
    out stops then reverses.
    """

    def __init__(self, lookback: int = 5):
        """
        Args:
            lookback: Number of candles on each side to confirm swing point
        """
        self.lookback = lookback

    def detect(self, data: pd.DataFrame) -> List[LiquidityPool]:
        """
        Detect liquidity pools at swing highs and lows.

        Args:
            data: OHLCV DataFrame

        Returns:
            List of LiquidityPool objects
        """
        pools = []

        if len(data) < self.lookback * 2 + 1:
            return pools

        # Scan for swing highs
        for i in range(self.lookback, len(data) - self.lookback):
            current_high = data.iloc[i]['high']

            # Check if this is highest point in lookback window
            is_swing_high = True
            for j in range(i - self.lookback, i + self.lookback + 1):
                if j != i and data.iloc[j]['high'] > current_high:
                    is_swing_high = False
                    break

            if is_swing_high:
                pool = LiquidityPool(
                    type=LiquidityType.HIGH,
                    price=current_high,
                    timestamp=data.index[i],
                    timeframe='unknown'  # Set by caller
                )
                pools.append(pool)

        # Scan for swing lows
        for i in range(self.lookback, len(data) - self.lookback):
            current_low = data.iloc[i]['low']

            # Check if this is lowest point in lookback window
            is_swing_low = True
            for j in range(i - self.lookback, i + self.lookback + 1):
                if j != i and data.iloc[j]['low'] < current_low:
                    is_swing_low = False
                    break

            if is_swing_low:
                pool = LiquidityPool(
                    type=LiquidityType.LOW,
                    price=current_low,
                    timestamp=data.index[i],
                    timeframe='unknown'
                )
                pools.append(pool)

        return pools

    def check_sweep(self, pool: LiquidityPool, current_price: float,
                    then_reversed_to: float) -> bool:
        """
        Check if a liquidity pool was swept (taken out then reversed).

        Args:
            pool: The liquidity pool
            current_price: Price when sweep occurred
            then_reversed_to: Price after reversal

        Returns:
            True if this was a sweep (false breakout)
        """
        if pool.type == LiquidityType.HIGH:
            # Price went above pool then came back below
            if current_price > pool.price and then_reversed_to < pool.price:
                return True
        else:  # LOW
            # Price went below pool then came back above
            if current_price < pool.price and then_reversed_to > pool.price:
                return True

        return False
