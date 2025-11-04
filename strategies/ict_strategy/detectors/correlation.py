from typing import List, Dict
import pandas as pd
import numpy as np

class CorrelationAnalyzer:
    """
    Analyzes correlation between two assets and detects divergences.

    ICT SMT (Smart Money Technique): When two correlated assets (like BTC/ETH
    or NQ/ES futures) diverge, it signals smart money positioning. The weaker
    asset shows where smart money expects the stronger asset to follow.
    """

    def __init__(self, correlation_threshold: float = 0.8):
        """
        Args:
            correlation_threshold: Correlation below this is considered divergence
        """
        self.correlation_threshold = correlation_threshold

    def calculate_correlation(self, data1: pd.DataFrame, data2: pd.DataFrame,
                            window: int = 20) -> float:
        """
        Calculate rolling correlation between two assets.

        Args:
            data1: First asset OHLCV data with 'close' column
            data2: Second asset OHLCV data with 'close' column
            window: Rolling window size

        Returns:
            Most recent correlation value
        """
        # Align the two dataframes by index
        df1 = data1['close'].to_frame(name='close1')
        df2 = data2['close'].to_frame(name='close2')

        combined = df1.join(df2, how='inner')

        if len(combined) < window:
            return 0.0

        # Calculate rolling correlation
        rolling_corr = combined['close1'].rolling(window=window).corr(
            combined['close2']
        )

        return rolling_corr.iloc[-1]

    def detect_divergence(self, data1: pd.DataFrame, data2: pd.DataFrame,
                         window: int = 20) -> List[Dict]:
        """
        Detect SMT divergences between two assets.

        Returns:
            List of divergence events with timestamp and correlation
        """
        divergences = []

        # Align dataframes
        df1 = data1['close'].to_frame(name='close1')
        df2 = data2['close'].to_frame(name='close2')
        combined = df1.join(df2, how='inner')

        if len(combined) < window:
            return divergences

        # Calculate rolling correlation
        combined['correlation'] = combined['close1'].rolling(window=window).corr(
            combined['close2']
        )

        # Detect where correlation breaks threshold
        for i in range(window, len(combined)):
            corr = combined['correlation'].iloc[i]

            if corr < self.correlation_threshold:
                # Check if this is a new divergence (not continuation)
                if i > 0 and combined['correlation'].iloc[i-1] >= self.correlation_threshold:
                    divergences.append({
                        'timestamp': combined.index[i],
                        'correlation': corr,
                        'asset1_price': combined['close1'].iloc[i],
                        'asset2_price': combined['close2'].iloc[i]
                    })

        return divergences
