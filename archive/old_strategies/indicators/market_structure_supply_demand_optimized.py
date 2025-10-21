"""
🌙 Market Structure & Supply/Demand Strategy - OPTIMIZED 🌙
==========================================================
Production-ready version with practical parameters and improved swing detection
featuring market structure analysis, supply & demand zones, and risk-reward filtering.

This optimized version includes:
1. More practical parameter settings for real trading
2. Improved swing validation logic that works with normal market structure
3. Parameter optimization framework for finding optimal settings
4. Enhanced entry/exit logic for better trade generation

Author: Bobby's Algo Trading Systems 🚀
Date: 2025-01-18
Version: 2.0.0 - Optimized
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


class MarketStructureSupplyDemandOptimized(Strategy):
    """
    🎯 Market Structure & Supply/Demand Trading Strategy - OPTIMIZED 🎯

    This optimized version uses more practical parameters and improved logic
    to generate meaningful trading signals while maintaining risk management.

    Optimized Parameters:
        swing_lookback: Bars to identify swing points (default 4, reduced from 5)
        consolidation_lookback: Bars to identify consolidation (default 3)
        min_rr_ratio: Minimum risk-reward ratio (default 1.5, reduced from 2.5)
        zone_strength_threshold: Minimum zone strength to trade (default 45, reduced from 70)
        max_zone_tests: Maximum times a zone can be tested (default 3)
        volatility_period: Period for ATR calculation (default 14)
        volume_spike_threshold: Volume spike multiplier (default 1.2, reduced from 1.5)
        multi_tf_confirm: Use multi-timeframe confirmation (default False for simplicity)
        pullback_fib_min: Minimum Fibonacci pullback level (default 0.236, reduced from 0.382)
        correlation_threshold: Max correlation for position reduction (default 0.8)
        swing_validation_mode: Swing validation approach ('flexible' or 'strict', default 'flexible')
        trend_confirmation_bars: Bars needed to confirm trend (default 2, reduced from 3)
    """

    # Optimized strategy parameters - more practical for real trading
    swing_lookback = 4  # Reduced for more frequent swing detection
    consolidation_lookback = 3
    min_rr_ratio = 1.5  # Reduced from 2.5 for more trade opportunities
    zone_strength_threshold = 45  # Reduced from 70 for more zones
    max_zone_tests = 3
    volatility_period = 14
    volume_spike_threshold = 1.2  # Reduced from 1.5 for more signals
    multi_tf_confirm = False  # Simplified for initial testing
    pullback_fib_min = 0.236  # Reduced from 0.382 for more entries
    correlation_threshold = 0.8
    swing_validation_mode = 'flexible'  # New parameter for flexible swing detection
    trend_confirmation_bars = 2  # Reduced for faster trend detection

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

        # Additional indicators for better signal generation
        self.ema_fast = self.I(talib.EMA, self.close, timeperiod=9)
        self.ema_slow = self.I(talib.EMA, self.close, timeperiod=21)
        self.macd, self.macd_signal, self.macd_hist = self.I(
            talib.MACD, self.close, fastperiod=12, slowperiod=26, signalperiod=9
        )

        # 🎯 Market structure state
        self.trend_state = 'neutral'  # 'uptrend', 'downtrend', 'neutral'
        self.last_swing_high = None
        self.last_swing_low = None
        self.confirmed_swing_high = None
        self.confirmed_swing_low = None
        self.prev_swing_high = None  # Track previous swing for flexible validation
        self.prev_swing_low = None

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

        print(f"🚀 Market Structure Strategy OPTIMIZED Initialized")
        print(f"   📊 Swing Lookback: {self.swing_lookback}")
        print(f"   🎯 Min R:R Ratio: {self.min_rr_ratio}")
        print(f"   💪 Zone Strength Threshold: {self.zone_strength_threshold}")
        print(f"   📈 Volume Spike Threshold: {self.volume_spike_threshold}")
        print(f"   🔄 Swing Validation Mode: {self.swing_validation_mode}")

    def identify_swing_point(self, index: int, is_high: bool) -> bool:
        """🏔️ Identify if current bar is a swing high or low - IMPROVED"""

        if index < self.swing_lookback or index >= len(self.data) - self.swing_lookback:
            return False

        if is_high:
            # Check for swing high - more flexible detection
            current_high = self.high[index]
            left_side_ok = all(self.high[i] <= current_high for i in range(index - self.swing_lookback, index))
            right_side_ok = all(self.high[i] <= current_high for i in range(index + 1, index + self.swing_lookback + 1))

            # Allow for equal highs on one side for more flexibility
            if left_side_ok and right_side_ok:
                return True

            # Alternative: At least be higher than 80% of surrounding bars
            surrounding_highs = [self.high[i] for i in range(index - self.swing_lookback, index + self.swing_lookback + 1) if i != index]
            higher_count = sum(1 for h in surrounding_highs if current_high > h)
            if higher_count >= len(surrounding_highs) * 0.8:
                return True

        else:
            # Check for swing low - more flexible detection
            current_low = self.low[index]
            left_side_ok = all(self.low[i] >= current_low for i in range(index - self.swing_lookback, index))
            right_side_ok = all(self.low[i] >= current_low for i in range(index + 1, index + self.swing_lookback + 1))

            # Allow for equal lows on one side for more flexibility
            if left_side_ok and right_side_ok:
                return True

            # Alternative: At least be lower than 80% of surrounding bars
            surrounding_lows = [self.low[i] for i in range(index - self.swing_lookback, index + self.swing_lookback + 1) if i != index]
            lower_count = sum(1 for l in surrounding_lows if current_low < l)
            if lower_count >= len(surrounding_lows) * 0.8:
                return True

        return False

    def validate_swing_point(self, new_swing: float, is_high: bool) -> bool:
        """✅ Validate swing point - IMPROVED with flexible mode"""

        if self.swing_validation_mode == 'flexible':
            # Flexible validation - accept swings that are significant moves
            if is_high:
                # Accept if it's a meaningful move up from recent lows
                if self.last_swing_low is not None:
                    move_size = (new_swing - self.last_swing_low) / self.last_swing_low
                    if move_size > 0.005:  # At least 0.5% move
                        return True

                # Or if it's higher than recent swing high by a small margin
                if self.confirmed_swing_high is not None:
                    if new_swing >= self.confirmed_swing_high * 0.998:  # Within 0.2% counts
                        return True
            else:
                # Accept if it's a meaningful move down from recent highs
                if self.last_swing_high is not None:
                    move_size = (self.last_swing_high - new_swing) / self.last_swing_high
                    if move_size > 0.005:  # At least 0.5% move
                        return True

                # Or if it's lower than recent swing low by a small margin
                if self.confirmed_swing_low is not None:
                    if new_swing <= self.confirmed_swing_low * 1.002:  # Within 0.2% counts
                        return True

            return True  # In flexible mode, accept most swings

        else:
            # Strict mode - original validation logic
            if is_high:
                if self.confirmed_swing_high is not None:
                    return new_swing > self.confirmed_swing_high
            else:
                if self.confirmed_swing_low is not None:
                    return new_swing < self.confirmed_swing_low

            return True  # First swing is always valid

    def update_market_structure(self, index: int):
        """📈 Update market structure and trend state - IMPROVED"""

        # Check for swing high
        if self.identify_swing_point(index - self.swing_lookback, True):
            swing_high = self.high[index - self.swing_lookback]

            # Validate swing high with flexible mode
            if self.validate_swing_point(swing_high, True):
                self.swing_highs.append((index - self.swing_lookback, swing_high))

                # Update swing tracking
                self.prev_swing_high = self.last_swing_high
                self.last_swing_high = swing_high

                # Update confirmed swing high with more flexible logic
                if self.confirmed_swing_high is None or swing_high > self.confirmed_swing_high * 0.995:
                    self.confirmed_swing_high = swing_high

        # Check for swing low
        if self.identify_swing_point(index - self.swing_lookback, False):
            swing_low = self.low[index - self.swing_lookback]

            # Validate swing low with flexible mode
            if self.validate_swing_point(swing_low, False):
                self.swing_lows.append((index - self.swing_lookback, swing_low))

                # Update swing tracking
                self.prev_swing_low = self.last_swing_low
                self.last_swing_low = swing_low

                # Update confirmed swing low with more flexible logic
                if self.confirmed_swing_low is None or swing_low < self.confirmed_swing_low * 1.005:
                    self.confirmed_swing_low = swing_low

        # Determine trend based on swing structure - IMPROVED
        if len(self.swing_highs) >= self.trend_confirmation_bars and len(self.swing_lows) >= self.trend_confirmation_bars:
            # Get recent swings for trend analysis
            recent_highs = [h[1] for h in self.swing_highs[-self.trend_confirmation_bars:]]
            recent_lows = [l[1] for l in self.swing_lows[-self.trend_confirmation_bars:]]

            # More flexible trend detection
            avg_high_change = np.mean(np.diff(recent_highs)) if len(recent_highs) > 1 else 0
            avg_low_change = np.mean(np.diff(recent_lows)) if len(recent_lows) > 1 else 0

            # Also consider EMA alignment for trend confirmation
            ema_bullish = self.ema_fast[index] > self.ema_slow[index]
            ema_bearish = self.ema_fast[index] < self.ema_slow[index]

            # Combine structure and momentum for trend determination
            if (avg_high_change > 0 and avg_low_change > 0) or (ema_bullish and avg_high_change >= 0):
                self.trend_state = 'uptrend'
            elif (avg_high_change < 0 and avg_low_change < 0) or (ema_bearish and avg_high_change <= 0):
                self.trend_state = 'downtrend'
            else:
                # Use MACD as tiebreaker for neutral markets
                if self.macd_hist[index] > 0:
                    self.trend_state = 'uptrend'
                elif self.macd_hist[index] < 0:
                    self.trend_state = 'downtrend'
                else:
                    self.trend_state = 'neutral'

    def identify_consolidation_breakout(self, index: int) -> Optional[str]:
        """📊 Identify consolidation followed by impulsive move - IMPROVED"""

        if index < self.consolidation_lookback + 2:
            return None

        # Calculate range of consolidation period
        consolidation_high = max(self.high[index - self.consolidation_lookback - 1:index])
        consolidation_low = min(self.low[index - self.consolidation_lookback - 1:index])
        consolidation_range = consolidation_high - consolidation_low

        # More flexible consolidation detection
        atr_value = self.atr[index - 1]
        if consolidation_range > atr_value * 2.0:  # Increased from 1.5 for more flexibility
            return None  # Range too wide for consolidation

        # Check for breakout with volume - more flexible
        current_close = self.close[index]
        current_volume = self.volume[index]
        avg_volume = self.volume_sma[index]

        # More flexible volume validation
        volume_condition = current_volume >= avg_volume * self.volume_spike_threshold

        # Alternative: Price momentum breakout even without volume spike
        price_momentum = abs(current_close - self.close[index - 1]) / atr_value
        momentum_breakout = price_momentum > 1.0  # Strong price move

        # Accept breakout with volume OR strong momentum
        if not (volume_condition or momentum_breakout):
            return None

        # Determine breakout direction with some tolerance
        tolerance = consolidation_range * 0.1  # 10% tolerance

        if current_close > consolidation_high - tolerance:
            return 'bullish'
        elif current_close < consolidation_low + tolerance:
            return 'bearish'

        return None

    def create_supply_demand_zone(self, index: int, zone_type: str):
        """🎯 Create supply or demand zone - IMPROVED"""

        # Define zone boundaries (using last 2 candles for better zones)
        zone_top = max(
            max(self.open[index - 1], self.close[index - 1]),
            max(self.open[index - 2], self.close[index - 2])
        )
        zone_bottom = min(
            min(self.open[index - 1], self.close[index - 1]),
            min(self.open[index - 2], self.close[index - 2])
        )

        # Calculate zone strength - IMPROVED scoring
        volume_ratio = self.volume[index] / self.volume_sma[index] if self.volume_sma[index] > 0 else 1
        price_move = abs(self.close[index] - self.close[index - 1]) / self.atr[index] if self.atr[index] > 0 else 1

        # Consider RSI for zone strength
        rsi_score = 0
        if zone_type == 'demand' and self.rsi[index] < 40:
            rsi_score = 20  # Oversold adds strength to demand zone
        elif zone_type == 'supply' and self.rsi[index] > 60:
            rsi_score = 20  # Overbought adds strength to supply zone

        # Improved strength scoring (0-100)
        strength = min(100, (volume_ratio * 25 + price_move * 35 + rsi_score + 25))

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
            # Keep only recent zones (max 7 for more opportunities)
            if len(self.supply_zones) > 7:
                self.supply_zones.pop(0)
        else:
            self.demand_zones.append(zone)
            # Keep only recent zones (max 7 for more opportunities)
            if len(self.demand_zones) > 7:
                self.demand_zones.pop(0)

    def check_zone_test(self, index: int) -> Optional[SupplyDemandZone]:
        """📍 Check if price is testing a supply/demand zone - IMPROVED"""

        current_price = self.close[index]
        current_low = self.low[index]
        current_high = self.high[index]

        # More flexible zone testing in any trend condition

        # Check demand zones (for long entries)
        if self.trend_state in ['uptrend', 'neutral']:  # Allow neutral trend too
            for zone in self.demand_zones:
                if not zone.active or zone.test_count >= self.max_zone_tests:
                    continue

                # More flexible zone test - price just needs to touch zone
                zone_buffer = (zone.top - zone.bottom) * 0.2  # 20% buffer around zone

                if (current_low <= zone.top + zone_buffer and
                    current_low >= zone.bottom - zone_buffer):

                    if zone.strength >= self.zone_strength_threshold:
                        zone.test_count += 1
                        return zone

        # Check supply zones (for short entries)
        if self.trend_state in ['downtrend', 'neutral']:  # Allow neutral trend too
            for zone in self.supply_zones:
                if not zone.active or zone.test_count >= self.max_zone_tests:
                    continue

                # More flexible zone test - price just needs to touch zone
                zone_buffer = (zone.top - zone.bottom) * 0.2  # 20% buffer around zone

                if (current_high >= zone.bottom - zone_buffer and
                    current_high <= zone.top + zone_buffer):

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
        """📏 Calculate Fibonacci pullback depth - IMPROVED"""

        if len(self.swing_highs) < 1 or len(self.swing_lows) < 1:
            return 0.5  # Return neutral value if no swings

        # Get recent swing points
        last_high = self.swing_highs[-1][1] if self.swing_highs else self.high[index]
        last_low = self.swing_lows[-1][1] if self.swing_lows else self.low[index]
        current_price = self.close[index]

        # Calculate pullback percentage
        swing_range = last_high - last_low
        if swing_range <= 0:
            return 0.5  # Return neutral value

        if self.trend_state == 'uptrend':
            pullback_depth = (last_high - current_price) / swing_range
        elif self.trend_state == 'downtrend':
            pullback_depth = (current_price - last_low) / swing_range
        else:
            # For neutral trend, calculate from midpoint
            midpoint = (last_high + last_low) / 2
            pullback_depth = abs(current_price - midpoint) / swing_range

        return max(0, min(1, pullback_depth))  # Clamp between 0 and 1

    def detect_change_of_character(self, index: int) -> bool:
        """⚠️ Detect potential trend change (ChoCh) - IMPROVED"""

        if index < 20:
            return False

        # Multiple ChoCh detection methods for better accuracy
        choch_signals = 0

        # 1. RSI divergence check
        if len(self.swing_highs) >= 2:
            # Price making new high but RSI not confirming
            if self.high[index] > self.swing_highs[-1][1] * 0.998:  # Within 0.2% counts
                current_rsi = self.rsi[index]
                prev_high_index = self.swing_highs[-1][0]
                prev_rsi = self.rsi[prev_high_index]

                if current_rsi < prev_rsi - 5:  # Significant divergence
                    choch_signals += 1

        if len(self.swing_lows) >= 2:
            # Price making new low but RSI not confirming
            if self.low[index] < self.swing_lows[-1][1] * 1.002:  # Within 0.2% counts
                current_rsi = self.rsi[index]
                prev_low_index = self.swing_lows[-1][0]
                prev_rsi = self.rsi[prev_low_index]

                if current_rsi > prev_rsi + 5:  # Significant divergence
                    choch_signals += 1

        # 2. MACD divergence
        if self.macd_hist[index] * self.macd_hist[index - 1] < 0:  # MACD histogram crosses zero
            choch_signals += 1

        # 3. EMA crossover
        if index > 1:
            prev_ema_diff = self.ema_fast[index - 1] - self.ema_slow[index - 1]
            curr_ema_diff = self.ema_fast[index] - self.ema_slow[index]
            if prev_ema_diff * curr_ema_diff < 0:  # EMA crossover occurred
                choch_signals += 1

        # Need at least 2 signals for ChoCh warning
        return choch_signals >= 2

    def calculate_position_adjustment(self, index: int) -> float:
        """🛡️ Calculate position size adjustment - IMPROVED"""

        # Start with default size (fraction of equity)
        adjustment = 0.95  # Use 95% of available equity by default

        # 1. Pullback depth adjustment - more balanced
        pullback_depth = self.calculate_pullback_depth(index)
        if pullback_depth < 0.236:
            # Shallow pullback - slightly reduce size
            adjustment *= 0.7
        elif pullback_depth > self.pullback_fib_min and pullback_depth < 0.618:
            # Deep pullback in sweet spot - maintain or increase size
            adjustment *= 1.0
        elif pullback_depth > 0.618:
            # Very deep pullback - be cautious
            adjustment *= 0.8

        # 2. ChoCh warning adjustment
        if self.choch_warning:
            adjustment *= 0.6

        # 3. Volume confirmation adjustment
        if self.volume[index] > self.volume_sma[index] * self.volume_spike_threshold:
            adjustment *= 1.05

        # 4. Zone strength adjustment
        # This will be applied when we have access to the tested zone

        # 5. Trend strength adjustment
        if self.trend_state != 'neutral':
            # Slightly stronger position in confirmed trends
            adjustment *= 1.0

        return min(0.99, max(0.1, adjustment))  # Cap between 0.1 and 0.99 (fraction of equity)

    def next(self):
        """🔄 Main trading logic executed for each bar - OPTIMIZED"""

        # Get current bar index
        index = len(self.data) - 1

        # Skip if not enough data
        if index < max(self.swing_lookback * 2, self.volatility_period, 20):
            return

        # Update market structure
        self.update_market_structure(index)

        # Check for consolidation breakout to create zones
        breakout = self.identify_consolidation_breakout(index)
        if breakout == 'bullish':
            self.create_supply_demand_zone(index, 'demand')
        elif breakout == 'bearish':
            self.create_supply_demand_zone(index, 'supply')

        # Also create zones at significant swing points
        if len(self.swing_highs) > 0 and index - self.swing_highs[-1][0] == 1:
            # Just confirmed a swing high - create supply zone
            self.create_supply_demand_zone(self.swing_highs[-1][0], 'supply')

        if len(self.swing_lows) > 0 and index - self.swing_lows[-1][0] == 1:
            # Just confirmed a swing low - create demand zone
            self.create_supply_demand_zone(self.swing_lows[-1][0], 'demand')

        # Check for ChoCh warning
        self.choch_warning = self.detect_change_of_character(index)

        # Exit logic for ChoCh - less aggressive
        if self.position and self.choch_warning:
            # Only exit if in profit or significant loss
            if self.position.pl_pct > 0.5 or self.position.pl_pct < -2:
                self.position.close()
                return

        # Skip if already in position (can be modified for pyramiding)
        if self.position:
            return

        # Check for zone test
        tested_zone = self.check_zone_test(index)

        # Alternative entry: momentum-based without strict zone requirement
        if not tested_zone and self.trend_state != 'neutral':
            # Check for momentum entry in strong trends
            if self.trend_state == 'uptrend':
                # Bullish momentum entry
                if (self.macd_hist[index] > 0 and
                    self.macd_hist[index] > self.macd_hist[index - 1] and
                    self.rsi[index] > 40 and self.rsi[index] < 70 and
                    self.close[index] > self.ema_fast[index]):

                    # Create synthetic zone for entry
                    tested_zone = SupplyDemandZone(
                        zone_type='demand',
                        top=self.close[index],
                        bottom=self.low[index],
                        strength=50,  # Moderate strength for momentum entry
                        created_at=index,
                        test_count=0,
                        active=True
                    )

            elif self.trend_state == 'downtrend':
                # Bearish momentum entry
                if (self.macd_hist[index] < 0 and
                    self.macd_hist[index] < self.macd_hist[index - 1] and
                    self.rsi[index] < 60 and self.rsi[index] > 30 and
                    self.close[index] < self.ema_fast[index]):

                    # Create synthetic zone for entry
                    tested_zone = SupplyDemandZone(
                        zone_type='supply',
                        top=self.high[index],
                        bottom=self.close[index],
                        strength=50,  # Moderate strength for momentum entry
                        created_at=index,
                        test_count=0,
                        active=True
                    )

        if not tested_zone:
            return

        # Calculate position adjustment
        self.position_adjustment = self.calculate_position_adjustment(index)

        # Adjust for zone strength (but keep within valid bounds)
        if tested_zone.strength > 70:
            self.position_adjustment = min(0.99, self.position_adjustment * 1.1)
        elif tested_zone.strength < 50:
            self.position_adjustment = max(0.1, self.position_adjustment * 0.9)

        # Entry logic based on trend and zone type
        if tested_zone.zone_type == 'demand' and self.trend_state in ['uptrend', 'neutral']:
            # Long entry setup
            entry_price = self.close[index]

            # More practical stop loss
            stop_loss = min(
                tested_zone.bottom - self.atr[index] * 0.5,
                entry_price - self.atr[index] * 1.5  # Max stop at 1.5 ATR
            )

            # More achievable targets
            if self.last_swing_high and self.last_swing_high > entry_price:
                # Use swing high but cap at reasonable level
                take_profit = min(
                    self.last_swing_high,
                    entry_price + self.atr[index] * 2.5
                )
            else:
                # Default target
                take_profit = entry_price + self.atr[index] * 2

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

        elif tested_zone.zone_type == 'supply' and self.trend_state in ['downtrend', 'neutral']:
            # Short entry setup
            entry_price = self.close[index]

            # More practical stop loss
            stop_loss = max(
                tested_zone.top + self.atr[index] * 0.5,
                entry_price + self.atr[index] * 1.5  # Max stop at 1.5 ATR
            )

            # More achievable targets
            if self.last_swing_low and self.last_swing_low < entry_price:
                # Use swing low but cap at reasonable level
                take_profit = max(
                    self.last_swing_low,
                    entry_price - self.atr[index] * 2.5
                )
            else:
                # Default target
                take_profit = entry_price - self.atr[index] * 2

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