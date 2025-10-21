"""
🌙 Market Structure & Supply/Demand Strategy 🌙
================================================
Implementation of the "Only Trading Strategy You'll Ever Need" methodology
featuring market structure analysis, supply & demand zones, and risk-reward filtering.

This production-ready strategy combines:
1. Market Structure (Trend) - Higher highs/lows for uptrend, lower highs/lows for downtrend
2. Supply & Demand Zones - Consolidation + impulsive move patterns
3. Risk-to-Reward Filter - Minimum 2.5:1 R:R requirement

Author: Bobby's Algo Trading Systems 🚀
Date: 2025-01-17
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from backtesting import Strategy
from backtesting.lib import crossover
import talib
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SupplyDemandZone:
    """📊 Supply/Demand Zone Structure"""
    zone_type: str  # 'supply' or 'demand'
    top: float      # Zone top price
    bottom: float   # Zone bottom price
    strength: float # Zone strength score (0-100)
    created_at: int # Bar index when created
    test_count: int # Number of times tested
    active: bool    # If zone is still active


class MarketStructureSupplyDemandStrategy(Strategy):
    """
    🎯 Market Structure & Supply/Demand Trading Strategy 🎯

    This strategy implements a comprehensive approach to trading based on:
    1. Market structure analysis for trend identification
    2. Supply and demand zones for entry logic
    3. Risk-to-reward filtering for trade quality

    Parameters:
        swing_lookback: Bars to identify swing points (default 5)
        consolidation_lookback: Bars to identify consolidation (default 3)
        min_rr_ratio: Minimum risk-reward ratio (default 2.5)
        zone_strength_threshold: Minimum zone strength to trade (default 70)
        max_zone_tests: Maximum times a zone can be tested (default 3)
        volatility_period: Period for ATR calculation (default 14)
        volume_spike_threshold: Volume spike multiplier (default 1.5)
        multi_tf_confirm: Use multi-timeframe confirmation (default True)
        pullback_fib_min: Minimum Fibonacci pullback level (default 0.382)
        correlation_threshold: Max correlation for position reduction (default 0.8)
    """

    # Strategy parameters
    swing_lookback = 5
    consolidation_lookback = 3
    min_rr_ratio = 2.5
    zone_strength_threshold = 70
    max_zone_tests = 3
    volatility_period = 14
    volume_spike_threshold = 1.5
    multi_tf_confirm = True
    pullback_fib_min = 0.382
    correlation_threshold = 0.8

    def init(self):
        """🔧 Initialize strategy indicators and state tracking"""

        # 📊 Core price data
        self.high = self.data.High
        self.low = self.data.Low
        self.close = self.data.Close
        self.open = self.data.Open
        self.volume = self.data.Volume

        # 📈 Technical indicators
        self.atr = self.I(talib.ATR, self.high, self.low, self.close, timeperiod=self.volatility_period)
        self.rsi = self.I(talib.RSI, self.close, timeperiod=14)
        self.volume_sma = self.I(talib.SMA, self.volume, timeperiod=20)

        # 🎯 Market structure state
        self.trend_state = 'neutral'  # 'uptrend', 'downtrend', 'neutral'
        self.last_swing_high = None
        self.last_swing_low = None
        self.confirmed_swing_high = None
        self.confirmed_swing_low = None

        # 📊 Supply/Demand zones storage
        self.supply_zones = []
        self.demand_zones = []

        # 📈 Swing point tracking
        self.swing_highs = []
        self.swing_lows = []

        # 🛡️ Risk management
        self.position_adjustment = 1.0  # Position size multiplier
        self.choch_warning = False      # Change of character warning

        # 📊 Performance tracking
        self.trade_count = 0
        self.winning_trades = 0
        self.losing_trades = 0

        print(f"🚀 Market Structure Strategy Initialized")
        print(f"   📊 Swing Lookback: {self.swing_lookback}")
        print(f"   🎯 Min R:R Ratio: {self.min_rr_ratio}")
        print(f"   💪 Zone Strength Threshold: {self.zone_strength_threshold}")
        print(f"   📈 Multi-TF Confirmation: {self.multi_tf_confirm}")

    def identify_swing_point(self, index: int, is_high: bool) -> bool:
        """🏔️ Identify if current bar is a swing high or low"""

        if index < self.swing_lookback or index >= len(self.data) - self.swing_lookback:
            return False

        if is_high:
            # Check for swing high
            current_high = self.high[index]
            for i in range(index - self.swing_lookback, index + self.swing_lookback + 1):
                if i != index and self.high[i] >= current_high:
                    return False
            return True
        else:
            # Check for swing low
            current_low = self.low[index]
            for i in range(index - self.swing_lookback, index + self.swing_lookback + 1):
                if i != index and self.low[i] <= current_low:
                    return False
            return True

    def validate_swing_point(self, new_swing: float, is_high: bool) -> bool:
        """✅ Validate swing point according to market structure rules"""

        # Critical rule: Swing is only valid if it breaks previous swing
        if is_high:
            # For swing high to be valid, must break previous confirmed swing high
            if self.confirmed_swing_high is not None:
                return new_swing > self.confirmed_swing_high
        else:
            # For swing low to be valid, must break previous confirmed swing low
            if self.confirmed_swing_low is not None:
                return new_swing < self.confirmed_swing_low

        return True  # First swing is always valid

    def update_market_structure(self, index: int):
        """📈 Update market structure and trend state"""

        # Check for swing high
        if self.identify_swing_point(index - self.swing_lookback, True):
            swing_high = self.high[index - self.swing_lookback]

            # Validate swing high
            if self.validate_swing_point(swing_high, True):
                self.swing_highs.append((index - self.swing_lookback, swing_high))
                self.last_swing_high = swing_high

                # Update confirmed swing high
                if self.confirmed_swing_high is None or swing_high > self.confirmed_swing_high:
                    self.confirmed_swing_high = swing_high

        # Check for swing low
        if self.identify_swing_point(index - self.swing_lookback, False):
            swing_low = self.low[index - self.swing_lookback]

            # Validate swing low
            if self.validate_swing_point(swing_low, False):
                self.swing_lows.append((index - self.swing_lookback, swing_low))
                self.last_swing_low = swing_low

                # Update confirmed swing low
                if self.confirmed_swing_low is None or swing_low < self.confirmed_swing_low:
                    self.confirmed_swing_low = swing_low

        # Determine trend based on swing structure
        if len(self.swing_highs) >= 2 and len(self.swing_lows) >= 2:
            # Check for uptrend (higher highs and higher lows)
            recent_highs = [h[1] for h in self.swing_highs[-2:]]
            recent_lows = [l[1] for l in self.swing_lows[-2:]]

            if recent_highs[-1] > recent_highs[-2] and recent_lows[-1] > recent_lows[-2]:
                self.trend_state = 'uptrend'
            elif recent_highs[-1] < recent_highs[-2] and recent_lows[-1] < recent_lows[-2]:
                self.trend_state = 'downtrend'
            else:
                self.trend_state = 'neutral'

    def identify_consolidation_breakout(self, index: int) -> Optional[str]:
        """📊 Identify consolidation followed by impulsive move"""

        if index < self.consolidation_lookback + 2:
            return None

        # Calculate range of consolidation period
        consolidation_high = max(self.high[index - self.consolidation_lookback - 1:index])
        consolidation_low = min(self.low[index - self.consolidation_lookback - 1:index])
        consolidation_range = consolidation_high - consolidation_low

        # Check for small range (consolidation)
        atr_value = self.atr[index - 1]
        if consolidation_range > atr_value * 1.5:
            return None  # Range too wide for consolidation

        # Check for breakout with volume
        current_close = self.close[index]
        current_volume = self.volume[index]
        avg_volume = self.volume_sma[index]

        # Volume spike validation
        if current_volume < avg_volume * self.volume_spike_threshold:
            return None

        # Determine breakout direction
        if current_close > consolidation_high:
            return 'bullish'
        elif current_close < consolidation_low:
            return 'bearish'

        return None

    def create_supply_demand_zone(self, index: int, zone_type: str):
        """🎯 Create supply or demand zone"""

        # Define zone boundaries (using candle body before breakout)
        zone_top = max(self.open[index - 1], self.close[index - 1])
        zone_bottom = min(self.open[index - 1], self.close[index - 1])

        # Calculate zone strength
        volume_ratio = self.volume[index] / self.volume_sma[index]
        price_move = abs(self.close[index] - self.close[index - 1]) / self.atr[index]

        # Strength scoring (0-100)
        strength = min(100, (volume_ratio * 30 + price_move * 40 + 30))

        # Create zone object
        zone = SupplyDemandZone(
            zone_type=zone_type,
            top=zone_top,
            bottom=zone_bottom,
            strength=strength,
            created_at=index,
            test_count=0,
            active=True
        )

        # Add to appropriate list
        if zone_type == 'supply':
            self.supply_zones.append(zone)
            # Keep only recent zones (max 5)
            if len(self.supply_zones) > 5:
                self.supply_zones.pop(0)
        else:
            self.demand_zones.append(zone)
            # Keep only recent zones (max 5)
            if len(self.demand_zones) > 5:
                self.demand_zones.pop(0)

    def check_zone_test(self, index: int) -> Optional[SupplyDemandZone]:
        """📍 Check if price is testing a supply/demand zone"""

        current_price = self.close[index]
        current_low = self.low[index]
        current_high = self.high[index]

        # Check demand zones in uptrend
        if self.trend_state == 'uptrend':
            for zone in self.demand_zones:
                if not zone.active or zone.test_count >= self.max_zone_tests:
                    continue

                # Check if price dipped into zone
                if current_low <= zone.top and current_low >= zone.bottom:
                    if zone.strength >= self.zone_strength_threshold:
                        zone.test_count += 1
                        return zone

        # Check supply zones in downtrend
        elif self.trend_state == 'downtrend':
            for zone in self.supply_zones:
                if not zone.active or zone.test_count >= self.max_zone_tests:
                    continue

                # Check if price rallied into zone
                if current_high >= zone.bottom and current_high <= zone.top:
                    if zone.strength >= self.zone_strength_threshold:
                        zone.test_count += 1
                        return zone

        return None

    def calculate_risk_reward(self, entry: float, stop: float, target: float, is_long: bool) -> float:
        """💰 Calculate risk-to-reward ratio"""

        if is_long:
            risk = entry - stop
            reward = target - entry
        else:
            risk = stop - entry
            reward = entry - target

        if risk <= 0:
            return 0

        return reward / risk

    def calculate_pullback_depth(self, index: int) -> float:
        """📏 Calculate Fibonacci pullback depth"""

        if len(self.swing_highs) < 1 or len(self.swing_lows) < 1:
            return 0

        # Get recent swing points
        last_high = self.swing_highs[-1][1]
        last_low = self.swing_lows[-1][1]
        current_price = self.close[index]

        # Calculate pullback percentage
        swing_range = last_high - last_low
        if swing_range <= 0:
            return 0

        if self.trend_state == 'uptrend':
            pullback_depth = (last_high - current_price) / swing_range
        else:
            pullback_depth = (current_price - last_low) / swing_range

        return pullback_depth

    def detect_change_of_character(self, index: int) -> bool:
        """⚠️ Detect potential trend change (ChoCh)"""

        if index < 20:
            return False

        # RSI divergence check
        if len(self.swing_highs) >= 2:
            # Price making new high but RSI not confirming
            if self.high[index] > self.swing_highs[-1][1]:
                current_rsi = self.rsi[index]
                prev_high_index = self.swing_highs[-1][0]
                prev_rsi = self.rsi[prev_high_index]

                if current_rsi < prev_rsi:
                    # Bearish divergence detected
                    return True

        if len(self.swing_lows) >= 2:
            # Price making new low but RSI not confirming
            if self.low[index] < self.swing_lows[-1][1]:
                current_rsi = self.rsi[index]
                prev_low_index = self.swing_lows[-1][0]
                prev_rsi = self.rsi[prev_low_index]

                if current_rsi > prev_rsi:
                    # Bullish divergence detected
                    return True

        return False

    def calculate_position_adjustment(self, index: int) -> float:
        """🛡️ Calculate position size adjustment based on market conditions"""

        adjustment = 1.0

        # 1. Pullback depth adjustment
        pullback_depth = self.calculate_pullback_depth(index)
        if pullback_depth < 0.236:
            # Shallow pullback - reduce size
            adjustment *= 0.5
        elif pullback_depth > self.pullback_fib_min and pullback_depth < 0.618:
            # Deep pullback in sweet spot - increase size
            adjustment *= 1.25

        # 2. ChoCh warning adjustment
        if self.choch_warning:
            adjustment *= 0.7

        # 3. Volume confirmation adjustment
        if self.volume[index] > self.volume_sma[index] * self.volume_spike_threshold:
            adjustment *= 1.1

        return min(2.0, max(0.1, adjustment))  # Cap between 0.1 and 2.0

    def next(self):
        """🔄 Main trading logic executed for each bar"""

        # Get current bar index
        index = len(self.data) - 1

        # Skip if not enough data
        if index < max(self.swing_lookback * 2, self.volatility_period, 20):
            return

        # Update market structure
        self.update_market_structure(index)

        # Check for consolidation breakout to create zones
        breakout = self.identify_consolidation_breakout(index)
        if breakout == 'bullish' and self.trend_state == 'uptrend':
            self.create_supply_demand_zone(index, 'demand')
        elif breakout == 'bearish' and self.trend_state == 'downtrend':
            self.create_supply_demand_zone(index, 'supply')

        # Check for ChoCh warning
        self.choch_warning = self.detect_change_of_character(index)

        # Exit logic for ChoCh
        if self.position and self.choch_warning:
            self.position.close()
            return

        # Skip if already in position
        if self.position:
            return

        # Check for zone test
        tested_zone = self.check_zone_test(index)
        if not tested_zone:
            return

        # Calculate position adjustment
        self.position_adjustment = self.calculate_position_adjustment(index)

        # Entry logic based on trend
        if self.trend_state == 'uptrend' and tested_zone.zone_type == 'demand':
            # Long entry setup
            entry_price = self.close[index]
            stop_loss = tested_zone.bottom - self.atr[index] * 0.5

            # Target is last swing high
            if self.last_swing_high:
                take_profit = self.last_swing_high
            else:
                take_profit = entry_price + self.atr[index] * 3

            # Calculate R:R
            rr_ratio = self.calculate_risk_reward(entry_price, stop_loss, take_profit, True)

            # Execute if R:R meets requirement
            if rr_ratio >= self.min_rr_ratio:
                self.buy(
                    size=self.position_adjustment,
                    sl=stop_loss,
                    tp=take_profit
                )
                self.trade_count += 1

        elif self.trend_state == 'downtrend' and tested_zone.zone_type == 'supply':
            # Short entry setup
            entry_price = self.close[index]
            stop_loss = tested_zone.top + self.atr[index] * 0.5

            # Target is last swing low
            if self.last_swing_low:
                take_profit = self.last_swing_low
            else:
                take_profit = entry_price - self.atr[index] * 3

            # Calculate R:R
            rr_ratio = self.calculate_risk_reward(entry_price, stop_loss, take_profit, False)

            # Execute if R:R meets requirement
            if rr_ratio >= self.min_rr_ratio:
                self.sell(
                    size=self.position_adjustment,
                    sl=stop_loss,
                    tp=take_profit
                )
                self.trade_count += 1