from typing import List, Dict
import pandas as pd
from strategies.ict_strategy.detectors.correlation import CorrelationAnalyzer

class SMTDetector:
    """
    Smart Money Technique (SMT) Divergence Detector.

    ICT Theory: When correlated assets diverge (one makes new high/low,
    the other doesn't), the weaker asset shows where smart money expects
    the market to go.

    Example:
    - BTC makes new high, ETH fails to = Bearish divergence
    - ETH makes new low, BTC fails to = Bullish divergence
    """

    def __init__(self, asset1: str, asset2: str, timeframe: str,
                 lookback: int = 50):
        self.asset1 = asset1
        self.asset2 = asset2
        self.timeframe = timeframe
        self.lookback = lookback
        self.correlation_analyzer = CorrelationAnalyzer()

    def _fetch_data(self, asset: str) -> pd.DataFrame:
        """Fetch data for asset - to be implemented with @trading_functions"""
        raise NotImplementedError("Implement with @trading_functions")

    def detect(self) -> List[Dict]:
        """
        Detect SMT divergences between the two assets.

        Returns:
            List of divergence events with type (bullish/bearish)
        """
        data1 = self._fetch_data(self.asset1)
        data2 = self._fetch_data(self.asset2)

        divergences = []

        # Get recent highs for both assets
        recent_window = min(20, len(data1))

        asset1_recent = data1['close'].tail(recent_window)
        asset2_recent = data2['close'].tail(recent_window)

        asset1_high = asset1_recent.max()
        asset2_high = asset2_recent.max()

        asset1_low = asset1_recent.min()
        asset2_low = asset2_recent.min()

        # Check for divergence at highs (bearish signal)
        # Asset 1 makes new high relative to lookback, but asset 2 doesn't
        lookback_data1 = data1['close'].tail(self.lookback)
        lookback_data2 = data2['close'].tail(self.lookback)

        if recent_window < len(data1):
            prev_high1 = lookback_data1.iloc[:-recent_window].max()
            prev_high2 = lookback_data2.iloc[:-recent_window].max()

            # Asset1 makes new high, Asset2 doesn't
            if asset1_high > prev_high1 and asset2_high <= prev_high2:
                divergences.append({
                    'type': 'bearish',
                    'timestamp': data1.index[-1],
                    'asset1': self.asset1,
                    'asset2': self.asset2,
                    'description': f'{self.asset1} made new high, {self.asset2} failed'
                })

            # Check for divergence at lows (bullish signal)
            prev_low1 = lookback_data1.iloc[:-recent_window].min()
            prev_low2 = lookback_data2.iloc[:-recent_window].min()

            # Asset2 makes new low, Asset1 doesn't
            if asset2_low < prev_low2 and asset1_low >= prev_low1:
                divergences.append({
                    'type': 'bullish',
                    'timestamp': data1.index[-1],
                    'asset1': self.asset1,
                    'asset2': self.asset2,
                    'description': f'{self.asset2} made new low, {self.asset1} held'
                })

        return divergences
