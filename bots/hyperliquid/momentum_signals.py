"""
📈 MOMENTUM SIGNAL DETECTION MODULE 📈
=======================================
Advanced signal detection for Crypto Momentum Trading Bot
with fake pump detection and multi-timeframe analysis.

SIGNAL COMPONENTS:
- MACD crossover detection
- RSI momentum validation
- Rate of Change surge detection
- Volume spike confirmation
- OBV momentum analysis
- Fake pump filtering

Author: Bobby's Algo Trading Systems 🌙
Date: 2025-01-18
Version: 1.0.0
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

# 🛡️ MODERN: Import technical analysis from @trading_functions
from trading_functions import (
    calculate_macd,
    calculate_rsi,
    calculate_bollinger_bands,
    calculate_vwap,
    analyze_volume_pattern,
    identify_swing_points,
    calculate_pattern_strength
)

logger = logging.getLogger(__name__)


# ============================================================
# 🎯 SIGNAL CONFIGURATION
# ============================================================

@dataclass
class SignalParameters:
    """Parameters for signal detection"""
    # MACD Parameters
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9

    # RSI Parameters
    rsi_period: int = 14
    rsi_buy_threshold: float = 50
    rsi_sell_threshold: float = 70
    rsi_oversold: float = 30
    rsi_overbought: float = 70

    # Rate of Change Parameters
    roc_periods: List[int] = None
    roc_surge_thresholds: Dict[int, float] = None

    # Volume Parameters
    volume_ma_period: int = 20
    volume_spike_multiplier: float = 1.8
    volume_fade_multiplier: float = 0.8
    min_volume: float = 100

    # OBV Parameters
    obv_ma_period: int = 10

    # Signal Strength Thresholds
    min_signal_strength: float = 0.5
    strong_signal_threshold: float = 0.7

    # Fake Pump Detection
    fake_pump_volume_threshold: float = 1.2
    fake_pump_lookback: int = 5

    def __post_init__(self):
        """Initialize default values"""
        if self.roc_periods is None:
            self.roc_periods = [1, 3, 5, 10]

        if self.roc_surge_thresholds is None:
            self.roc_surge_thresholds = {
                1: 1.0,   # 1% in 1 period
                3: 2.0,   # 2% in 3 periods
                5: 3.0,   # 3% in 5 periods
                10: 5.0   # 5% in 10 periods
            }


@dataclass
class MomentumSignal:
    """Momentum signal data structure"""
    symbol: str
    timestamp: datetime
    has_signal: bool
    signal_type: str  # 'buy', 'sell', 'hold'
    strength: float  # 0-1 signal strength
    components: Dict[str, bool]  # Individual signal components
    metrics: Dict[str, float]  # Signal metrics
    confidence: float  # 0-1 confidence score
    fake_pump_risk: float  # 0-1 fake pump probability


# ============================================================
# 📈 MOMENTUM SIGNAL DETECTOR
# ============================================================

class MomentumSignalDetector:
    """
    🔍 Advanced Momentum Signal Detection 🔍

    Detects high-probability momentum surges using:
    - Multi-indicator confluence
    - Volume confirmation
    - Fake pump filtering
    - Market structure validation
    """

    def __init__(self, params: Optional[SignalParameters] = None):
        """Initialize signal detector"""
        self.params = params or SignalParameters()

        # Signal history for pattern analysis
        self.signal_history: List[MomentumSignal] = []
        self.false_signals: List[MomentumSignal] = []

        # Performance tracking
        self.signal_accuracy = 0.0
        self.total_signals = 0
        self.successful_signals = 0

        logger.info("✅ Signal Detector initialized")

    # ============================================================
    # 🚀 MAIN SIGNAL DETECTION
    # ============================================================

    def detect_momentum_surge(
        self,
        data: pd.DataFrame,
        symbol: str,
        asset_config: Optional[Dict] = None
    ) -> MomentumSignal:
        """
        Detect momentum surge signals

        Args:
            data: OHLCV DataFrame
            symbol: Trading symbol
            asset_config: Asset-specific configuration

        Returns:
            MomentumSignal object
        """
        # Validate data
        if data is None or len(data) < 50:
            return self._create_empty_signal(symbol)

        # Calculate all indicators
        indicators = self._calculate_indicators(data)

        # Detect individual signal components
        components = self._detect_signal_components(indicators, data)

        # Check for fake pump
        fake_pump_risk = self._detect_fake_pump(indicators, data)

        # Calculate signal strength
        strength = self._calculate_signal_strength(components, indicators)

        # Apply asset-specific adjustments
        if asset_config:
            strength = self._apply_asset_adjustments(strength, asset_config)

        # Determine if we have a valid signal
        has_signal = (
            strength >= self.params.min_signal_strength and
            fake_pump_risk < 0.5 and
            components.get('volume_confirms', False)
        )

        # Calculate confidence
        confidence = self._calculate_confidence(
            components,
            indicators,
            fake_pump_risk
        )

        # Create signal object
        signal = MomentumSignal(
            symbol=symbol,
            timestamp=datetime.now(),
            has_signal=has_signal,
            signal_type='buy' if has_signal else 'hold',
            strength=strength,
            components=components,
            metrics=self._extract_metrics(indicators),
            confidence=confidence,
            fake_pump_risk=fake_pump_risk
        )

        # Record signal
        self._record_signal(signal)

        return signal

    def _calculate_indicators(self, data: pd.DataFrame) -> Dict:
        """Calculate all technical indicators"""
        indicators = {}

        try:
            # MACD
            macd_result = calculate_macd(
                data,
                fast_period=self.params.macd_fast,
                slow_period=self.params.macd_slow,
                signal_period=self.params.macd_signal
            )
            indicators['macd'] = macd_result.get('macd', [])
            indicators['macd_signal'] = macd_result.get('signal', [])
            indicators['macd_histogram'] = macd_result.get('histogram', [])

            # RSI
            indicators['rsi'] = calculate_rsi(data, period=self.params.rsi_period)

            # Rate of Change
            indicators['roc'] = {}
            for period in self.params.roc_periods:
                roc = data['close'].pct_change(period) * 100
                indicators['roc'][period] = roc.values

            # Volume Analysis
            indicators['volume_ma'] = data['volume'].rolling(
                self.params.volume_ma_period
            ).mean().values

            indicators['volume_ratio'] = (
                data['volume'] / indicators['volume_ma']
            ).fillna(1).values

            # OBV
            obv = (np.sign(data['close'].diff()) * data['volume']).cumsum()
            indicators['obv'] = obv.values
            indicators['obv_ma'] = obv.rolling(
                self.params.obv_ma_period
            ).mean().values

            # Momentum
            indicators['momentum'] = data['close'].diff(10).values

            # Bollinger Bands
            bb_result = calculate_bollinger_bands(data)
            indicators['bb_upper'] = bb_result['upper']
            indicators['bb_lower'] = bb_result['lower']
            indicators['bb_middle'] = bb_result['middle']

            # VWAP
            indicators['vwap'] = calculate_vwap(data)

            # ATR for volatility
            high_low = data['high'] - data['low']
            high_close = abs(data['high'] - data['close'].shift())
            low_close = abs(data['low'] - data['close'].shift())
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            indicators['atr'] = true_range.rolling(14).mean().values

        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")

        return indicators

    def _detect_signal_components(
        self,
        indicators: Dict,
        data: pd.DataFrame
    ) -> Dict[str, bool]:
        """Detect individual signal components"""
        components = {}

        try:
            # MACD Crossover
            if len(indicators.get('macd', [])) > 1:
                macd_cross = (
                    indicators['macd'][-1] > indicators['macd_signal'][-1] and
                    indicators['macd'][-2] <= indicators['macd_signal'][-2]
                )
                components['macd_bullish'] = macd_cross
            else:
                components['macd_bullish'] = False

            # RSI Momentum
            if len(indicators.get('rsi', [])) > 1:
                components['rsi_climbing'] = (
                    indicators['rsi'][-1] > indicators['rsi'][-2] and
                    indicators['rsi'][-1] > self.params.rsi_buy_threshold
                )
                components['rsi_oversold_bounce'] = (
                    indicators['rsi'][-2] < self.params.rsi_oversold and
                    indicators['rsi'][-1] > self.params.rsi_oversold
                )
            else:
                components['rsi_climbing'] = False
                components['rsi_oversold_bounce'] = False

            # Rate of Change Surge
            surge_detected = False
            for period, threshold in self.params.roc_surge_thresholds.items():
                if period in indicators.get('roc', {}):
                    roc_values = indicators['roc'][period]
                    if len(roc_values) > 0 and roc_values[-1] > threshold:
                        surge_detected = True
                        break
            components['roc_surge'] = surge_detected

            # Volume Confirmation
            if len(indicators.get('volume_ratio', [])) > 0:
                components['volume_confirms'] = (
                    indicators['volume_ratio'][-1] > self.params.volume_spike_multiplier
                )
            else:
                components['volume_confirms'] = False

            # OBV Rising
            if len(indicators.get('obv', [])) > 1:
                components['obv_rising'] = (
                    indicators['obv'][-1] > indicators['obv'][-2]
                )
                if len(indicators.get('obv_ma', [])) > 0:
                    components['obv_above_ma'] = (
                        indicators['obv'][-1] > indicators['obv_ma'][-1]
                    )
                else:
                    components['obv_above_ma'] = False
            else:
                components['obv_rising'] = False
                components['obv_above_ma'] = False

            # Price Action
            if len(data) > 1:
                components['price_rising'] = data['close'].iloc[-1] > data['close'].iloc[-2]
                components['higher_high'] = (
                    data['high'].iloc[-1] > data['high'].iloc[-5:].max()
                    if len(data) >= 5 else False
                )
            else:
                components['price_rising'] = False
                components['higher_high'] = False

            # Bollinger Band Position
            if (len(indicators.get('bb_upper', [])) > 0 and
                len(data) > 0):
                price = data['close'].iloc[-1]
                components['bb_breakout'] = price > indicators['bb_upper'][-1]
                components['bb_squeeze'] = (
                    (indicators['bb_upper'][-1] - indicators['bb_lower'][-1]) <
                    (indicators['bb_upper'][-5] - indicators['bb_lower'][-5])
                    if len(indicators['bb_upper']) >= 5 else False
                )
            else:
                components['bb_breakout'] = False
                components['bb_squeeze'] = False

        except Exception as e:
            logger.error(f"Error detecting signal components: {e}")

        return components

    # ============================================================
    # 🚨 FAKE PUMP DETECTION
    # ============================================================

    def _detect_fake_pump(
        self,
        indicators: Dict,
        data: pd.DataFrame
    ) -> float:
        """
        Detect probability of fake pump

        Returns:
            Float between 0-1 indicating fake pump probability
        """
        fake_pump_score = 0.0
        factors = 0

        try:
            # Volume too low for authentic move
            if len(indicators.get('volume_ratio', [])) > 0:
                if indicators['volume_ratio'][-1] < self.params.fake_pump_volume_threshold:
                    fake_pump_score += 0.3
                factors += 1

            # No momentum follow-through
            if 'roc' in indicators and 5 in indicators['roc']:
                roc_values = indicators['roc'][5]
                if len(roc_values) >= 3:
                    if roc_values[-1] < roc_values[-3]:
                        fake_pump_score += 0.2
                    factors += 1

            # RSI divergence
            if len(indicators.get('rsi', [])) >= 3 and len(data) >= 3:
                price_rising = data['close'].iloc[-1] > data['close'].iloc[-3]
                rsi_falling = indicators['rsi'][-1] < indicators['rsi'][-3]
                if price_rising and rsi_falling:
                    fake_pump_score += 0.3
                factors += 1

            # OBV divergence
            if len(indicators.get('obv', [])) >= 3 and len(data) >= 3:
                price_rising = data['close'].iloc[-1] > data['close'].iloc[-3]
                obv_falling = indicators['obv'][-1] < indicators['obv'][-3]
                if price_rising and obv_falling:
                    fake_pump_score += 0.2
                factors += 1

            # Sudden spike without buildup
            if len(data) >= self.params.fake_pump_lookback:
                recent_range = data['close'].iloc[-self.params.fake_pump_lookback:-1]
                current_price = data['close'].iloc[-1]
                avg_price = recent_range.mean()

                spike_pct = ((current_price - avg_price) / avg_price) * 100
                if spike_pct > 5:  # 5% spike
                    # Check if volume preceded price
                    vol_preceded = indicators.get('volume_ratio', [1])[-2] > 1.5
                    if not vol_preceded:
                        fake_pump_score += 0.3
                    factors += 1

            # No support from major indicators
            macd_bearish = (
                indicators.get('macd', [0])[-1] < indicators.get('macd_signal', [0])[-1]
                if len(indicators.get('macd', [])) > 0 else False
            )
            if macd_bearish:
                fake_pump_score += 0.2
                factors += 1

        except Exception as e:
            logger.error(f"Error in fake pump detection: {e}")

        # Normalize score
        if factors > 0:
            fake_pump_score = min(fake_pump_score, 1.0)

        return fake_pump_score

    def detect_momentum_fade(self, data: pd.DataFrame) -> bool:
        """
        Detect if momentum is fading

        Returns:
            True if momentum is fading
        """
        if data is None or len(data) < 20:
            return False

        try:
            indicators = self._calculate_indicators(data)

            fade_signals = 0

            # RSI declining from overbought
            if len(indicators.get('rsi', [])) > 1:
                if (indicators['rsi'][-2] > self.params.rsi_overbought and
                    indicators['rsi'][-1] < indicators['rsi'][-2]):
                    fade_signals += 1

            # Volume declining
            if len(indicators.get('volume_ratio', [])) > 0:
                if indicators['volume_ratio'][-1] < self.params.volume_fade_multiplier:
                    fade_signals += 1

            # ROC turning negative
            if 5 in indicators.get('roc', {}):
                if indicators['roc'][5][-1] < 0.5:
                    fade_signals += 1

            # OBV declining
            if len(indicators.get('obv', [])) > 1:
                if indicators['obv'][-1] < indicators['obv'][-2]:
                    fade_signals += 1

            # MACD bearish crossover
            if len(indicators.get('macd', [])) > 1:
                macd_bearish = (
                    indicators['macd'][-1] < indicators['macd_signal'][-1] and
                    indicators['macd'][-2] >= indicators['macd_signal'][-2]
                )
                if macd_bearish:
                    fade_signals += 1

            return fade_signals >= 3

        except Exception as e:
            logger.error(f"Error detecting momentum fade: {e}")
            return False

    # ============================================================
    # 📊 SIGNAL STRENGTH & CONFIDENCE
    # ============================================================

    def _calculate_signal_strength(
        self,
        components: Dict[str, bool],
        indicators: Dict
    ) -> float:
        """
        Calculate overall signal strength

        Returns:
            Float between 0-1 indicating signal strength
        """
        strength = 0.0
        weights = {
            'macd_bullish': 0.15,
            'rsi_climbing': 0.15,
            'rsi_oversold_bounce': 0.10,
            'roc_surge': 0.20,
            'volume_confirms': 0.15,
            'obv_rising': 0.10,
            'obv_above_ma': 0.05,
            'price_rising': 0.05,
            'higher_high': 0.05
        }

        for component, weight in weights.items():
            if components.get(component, False):
                strength += weight

        # Bonus for multiple confirmations
        confirmed_count = sum(1 for v in components.values() if v)
        if confirmed_count >= 6:
            strength *= 1.2
        elif confirmed_count >= 4:
            strength *= 1.1

        return min(strength, 1.0)

    def _calculate_confidence(
        self,
        components: Dict[str, bool],
        indicators: Dict,
        fake_pump_risk: float
    ) -> float:
        """
        Calculate signal confidence

        Returns:
            Float between 0-1 indicating confidence
        """
        confidence = 1.0

        # Reduce confidence for fake pump risk
        confidence -= fake_pump_risk * 0.5

        # Reduce confidence for weak volume
        if not components.get('volume_confirms', False):
            confidence -= 0.2

        # Reduce confidence for divergences
        if not components.get('obv_rising', False):
            confidence -= 0.1

        # Boost confidence for strong confluence
        confirmed_count = sum(1 for v in components.values() if v)
        if confirmed_count >= 7:
            confidence += 0.2

        return max(0, min(confidence, 1.0))

    def _apply_asset_adjustments(
        self,
        strength: float,
        asset_config: Dict
    ) -> float:
        """Apply asset-specific signal adjustments"""
        # Adjust based on asset volatility profile
        volatility_adj = asset_config.get('signal_volatility_adj', 1.0)
        strength *= volatility_adj

        # Adjust based on asset liquidity
        liquidity_adj = asset_config.get('signal_liquidity_adj', 1.0)
        strength *= liquidity_adj

        return min(strength, 1.0)

    # ============================================================
    # 📈 METRICS & REPORTING
    # ============================================================

    def _extract_metrics(self, indicators: Dict) -> Dict[str, float]:
        """Extract key metrics from indicators"""
        metrics = {}

        try:
            # Latest indicator values
            if len(indicators.get('rsi', [])) > 0:
                metrics['rsi'] = indicators['rsi'][-1]

            if len(indicators.get('macd', [])) > 0:
                metrics['macd'] = indicators['macd'][-1]

            if 5 in indicators.get('roc', {}):
                metrics['roc_5'] = indicators['roc'][5][-1]

            if len(indicators.get('volume_ratio', [])) > 0:
                metrics['volume_ratio'] = indicators['volume_ratio'][-1]

            if len(indicators.get('atr', [])) > 0:
                metrics['atr'] = indicators['atr'][-1]

            # Calculate volatility
            if len(indicators.get('atr', [])) > 0:
                metrics['volatility'] = indicators['atr'][-1] / indicators.get('vwap', [1])[-1]

        except Exception as e:
            logger.error(f"Error extracting metrics: {e}")

        return metrics

    def _create_empty_signal(self, symbol: str) -> MomentumSignal:
        """Create empty signal when no data available"""
        return MomentumSignal(
            symbol=symbol,
            timestamp=datetime.now(),
            has_signal=False,
            signal_type='hold',
            strength=0.0,
            components={},
            metrics={},
            confidence=0.0,
            fake_pump_risk=0.0
        )

    def _record_signal(self, signal: MomentumSignal):
        """Record signal for analysis"""
        self.signal_history.append(signal)
        if signal.has_signal:
            self.total_signals += 1

        # Keep only recent history
        if len(self.signal_history) > 1000:
            self.signal_history = self.signal_history[-500:]

    def update_signal_performance(self, signal: MomentumSignal, was_successful: bool):
        """Update signal performance metrics"""
        if was_successful:
            self.successful_signals += 1
        else:
            self.false_signals.append(signal)

        if self.total_signals > 0:
            self.signal_accuracy = self.successful_signals / self.total_signals

    def get_signal_stats(self) -> Dict:
        """Get signal detection statistics"""
        return {
            'total_signals': self.total_signals,
            'successful_signals': self.successful_signals,
            'accuracy': self.signal_accuracy,
            'false_signals': len(self.false_signals),
            'recent_signals': len([s for s in self.signal_history[-100:]
                                  if s.has_signal])
        }

# 🌙💫🚀 Signal Detection Ready for Production! 🌙💫🚀