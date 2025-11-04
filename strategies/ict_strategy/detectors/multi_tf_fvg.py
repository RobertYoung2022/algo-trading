from typing import Dict, List
import pandas as pd
from strategies.ict_strategy.detectors.fvg_detector import FVGDetector
from strategies.ict_strategy.models.fvg import FVG

class MultiTimeframeFVGDetector:
    """
    Detects FVGs across multiple timeframes and identifies alignment.

    Higher timeframe FVGs are more significant than lower timeframe FVGs.
    When LTF FVGs align with HTF FVGs, they are higher probability setups.
    """

    def __init__(self, symbol: str, timeframes: List[str], exchange: str = 'hyperliquid',
                 validate_quality: bool = True):
        self.symbol = symbol
        self.timeframes = timeframes
        self.exchange = exchange
        self.validate_quality = validate_quality
        self.detectors = {
            tf: FVGDetector(timeframe=tf, min_gap_size=0.0, validate_quality=validate_quality)
            for tf in timeframes
        }

    def _fetch_data(self, timeframe: str, limit: int = 500) -> pd.DataFrame:
        """
        Fetch OHLCV data using @trading_functions.

        This is a placeholder - actual implementation depends on your
        @trading_functions API.

        Based on exploration of core/trading_functions/, available functions are:
        - get_ohlcv_data_phemex(client, symbol, timeframe, limit) -> pd.DataFrame
        - get_ohlcv_hyperliquid(client, symbol, interval, lookback_days) -> List[Dict]

        Implementation notes:
        1. Both require a client instance (PhemexClient or HyperliquidClient)
        2. Phemex returns a DataFrame directly
        3. Hyperliquid returns List[Dict] that needs conversion to DataFrame
        4. Timeframe formats differ between exchanges
        5. Need to handle client initialization and exchange selection

        For production use, this should:
        - Initialize appropriate client based on self.exchange
        - Convert timeframe format if needed
        - Handle data conversion to standard DataFrame format
        - Add error handling and retries
        """
        # TODO: Replace with actual @trading_functions call
        # Example implementation:
        # if self.exchange == 'phemex':
        #     client = create_phemex_client()
        #     return get_ohlcv_data_phemex(client, self.symbol, timeframe, limit)
        # elif self.exchange == 'hyperliquid':
        #     client = create_hyperliquid_client()
        #     data = get_ohlcv_hyperliquid(client, self.symbol, timeframe, lookback_days=30)
        #     return _convert_hyperliquid_to_df(data)

        # For now, raise NotImplementedError to force proper implementation
        raise NotImplementedError(
            "Must implement _fetch_data using @trading_functions. "
            "Available functions: get_ohlcv_data_phemex, get_ohlcv_hyperliquid. "
            "Both require client initialization. See core/trading_functions/ for API details."
        )

    def detect_all(self) -> Dict[str, List[FVG]]:
        """
        Detect FVGs on all configured timeframes.

        Returns:
            Dict mapping timeframe -> list of FVGs
        """
        results = {}

        for tf in self.timeframes:
            data = self._fetch_data(tf)
            detector = self.detectors[tf]
            fvgs = detector.detect(data)
            results[tf] = fvgs

        return results

    def detect_aligned_fvgs(self, higher_tf: str, lower_tf: str) -> List[FVG]:
        """
        Find lower timeframe FVGs that align with higher timeframe FVGs.

        Alignment means:
        - LTF FVG falls within the price range of HTF FVG
        - LTF FVG is same direction as HTF FVG

        Args:
            higher_tf: Higher timeframe (e.g., '4H')
            lower_tf: Lower timeframe (e.g., '1H')

        Returns:
            List of aligned LTF FVGs with aligned_with_htf flag set
        """
        htf_data = self._fetch_data(higher_tf)
        ltf_data = self._fetch_data(lower_tf)

        htf_fvgs = self.detectors[higher_tf].detect(htf_data)
        ltf_fvgs = self.detectors[lower_tf].detect(ltf_data)

        aligned = []

        for ltf_fvg in ltf_fvgs:
            for htf_fvg in htf_fvgs:
                # Check if LTF FVG is within HTF FVG price range
                if (ltf_fvg.type == htf_fvg.type and
                    ltf_fvg.gap_low >= htf_fvg.low and
                    ltf_fvg.gap_high <= htf_fvg.high):

                    # Mark as aligned
                    ltf_fvg.aligned_with_htf = True
                    aligned.append(ltf_fvg)
                    break

        return aligned
