from dataclasses import dataclass
from datetime import datetime
from enum import Enum

class LiquidityType(Enum):
    HIGH = "high"  # Liquidity resting above (stops above swing high)
    LOW = "low"    # Liquidity resting below (stops below swing low)

@dataclass
class LiquidityPool:
    """
    Liquidity pools are price levels where stops cluster.

    Typically at:
    - Previous swing highs (buy stops above)
    - Previous swing lows (sell stops below)
    - Round numbers (psychological levels)
    - Key support/resistance
    """
    type: LiquidityType
    price: float
    timestamp: datetime
    timeframe: str
    swept: bool = False  # Has price swept through this level?
    sweep_timestamp: datetime = None
