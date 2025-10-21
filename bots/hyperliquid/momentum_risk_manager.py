"""
🛡️ MOMENTUM STRATEGY RISK MANAGEMENT MODULE 🛡️
================================================
Comprehensive risk management for the Crypto Momentum Bot
with dynamic position sizing and portfolio protection.

FEATURES:
- Dynamic position sizing based on signal strength
- Volatility-adjusted risk parameters
- Correlation-based portfolio management
- Daily and absolute loss limits
- Drawdown protection
- Kill switch mechanisms

Author: Bobby's Algo Trading Systems 🌙
Date: 2025-01-18
Version: 1.0.0
"""

import logging
from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# 🛡️ MODERN: Import @trading_functions for risk calculations
from trading_functions import (
    calculate_position_size,
    calculate_risk_reward_ratio,
    validate_trade_risk,
    check_drawdown_limits,
    generate_risk_report
)

logger = logging.getLogger(__name__)


# ============================================================
# 🎯 RISK CONFIGURATION
# ============================================================

@dataclass
class RiskParameters:
    """Risk management parameters"""
    # Account Risk Limits
    max_account_risk: float = 15.0  # 15% maximum account drawdown
    daily_loss_limit: float = 5.0   # 5% daily loss limit
    max_position_risk: float = 5.0  # 5% max per position
    min_position_risk: float = 1.0  # 1% min per position

    # Position Limits
    max_concurrent_positions: int = 3
    max_correlated_positions: int = 2  # Max positions in correlated assets
    correlation_threshold: float = 0.7  # Correlation threshold

    # Risk/Reward Requirements
    min_risk_reward: float = 2.0  # Minimum 2:1 R/R ratio
    max_risk_reward: float = 10.0  # Cap at 10:1 to be realistic

    # Volatility Adjustments
    high_volatility_threshold: float = 0.8  # 80% annual volatility
    low_volatility_threshold: float = 0.3   # 30% annual volatility
    volatility_scalar: float = 0.5         # Position size volatility adjustment

    # Time-based Risk
    max_daily_trades: int = 10  # Maximum trades per day
    min_trade_interval: int = 300  # Minimum seconds between trades
    position_timeout_hours: float = 24  # Maximum position hold time

    # Emergency Thresholds
    kill_switch_loss: float = 10.0  # Emergency stop at 10% daily loss
    pause_after_losses: int = 3     # Pause after consecutive losses


# ============================================================
# 🛡️ MOMENTUM RISK MANAGER
# ============================================================

class MomentumRiskManager:
    """
    🛡️ Comprehensive Risk Management for Momentum Trading 🛡️

    Manages all aspects of trading risk including:
    - Position sizing
    - Portfolio correlation
    - Loss limits
    - Volatility adjustments
    - Emergency controls
    """

    def __init__(self, risk_params: Optional[RiskParameters] = None):
        """Initialize risk manager with parameters"""
        self.params = risk_params or RiskParameters()

        # Risk tracking
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.consecutive_losses = 0
        self.last_trade_time = None
        self.peak_balance = 0.0
        self.current_drawdown = 0.0

        # Position tracking
        self.open_positions: Dict[str, Dict] = {}
        self.position_correlations: Dict[Tuple[str, str], float] = {}

        # Historical data for analysis
        self.pnl_history: List[float] = []
        self.trade_history: List[Dict] = []

        # Risk state
        self.is_paused = False
        self.pause_reason = ""

        logger.info("✅ Risk Manager initialized")

    # ============================================================
    # 📊 POSITION SIZING
    # ============================================================

    def calculate_position_size(
        self,
        account_balance: float,
        signal_strength: float,
        volatility: float,
        asset_config: Dict,
        current_positions: int = 0
    ) -> float:
        """
        Calculate optimal position size based on multiple factors

        Args:
            account_balance: Current account balance
            signal_strength: Signal strength (0-1)
            volatility: Asset volatility
            asset_config: Asset-specific configuration
            current_positions: Number of open positions

        Returns:
            Position size in base currency
        """
        # Check if trading is paused
        if self.is_paused:
            logger.warning(f"⚠️ Trading paused: {self.pause_reason}")
            return 0.0

        # Base position size from asset config
        base_risk_pct = asset_config.get('position_size', 0.03)

        # Adjust for signal strength
        strength_multiplier = min(signal_strength, 1.0)

        # Volatility adjustment
        vol_multiplier = self._calculate_volatility_multiplier(volatility)

        # Portfolio heat reduction (reduce size with more positions)
        portfolio_multiplier = 1.0 - (current_positions * 0.2)
        portfolio_multiplier = max(portfolio_multiplier, 0.4)

        # Drawdown adjustment
        drawdown_multiplier = self._calculate_drawdown_multiplier()

        # Calculate final position risk percentage
        position_risk = (
            base_risk_pct *
            strength_multiplier *
            vol_multiplier *
            portfolio_multiplier *
            drawdown_multiplier
        )

        # Apply min/max constraints
        position_risk = max(self.params.min_position_risk / 100, position_risk)
        position_risk = min(self.params.max_position_risk / 100, position_risk)

        # Calculate position size
        position_size = account_balance * position_risk

        logger.info(f"📊 Position size calculated: ${position_size:.2f}")
        logger.info(f"   Risk: {position_risk*100:.2f}%, Signal: {signal_strength:.2f}")
        logger.info(f"   Volatility multiplier: {vol_multiplier:.2f}")

        return position_size

    def _calculate_volatility_multiplier(self, volatility: float) -> float:
        """Calculate position size multiplier based on volatility"""
        if volatility > self.params.high_volatility_threshold:
            # High volatility - reduce position
            return 0.5
        elif volatility < self.params.low_volatility_threshold:
            # Low volatility - increase position
            return 1.2
        else:
            # Normal volatility - linear scaling
            vol_range = self.params.high_volatility_threshold - self.params.low_volatility_threshold
            vol_position = (volatility - self.params.low_volatility_threshold) / vol_range
            return 1.2 - (vol_position * 0.7)

    def _calculate_drawdown_multiplier(self) -> float:
        """Calculate position size multiplier based on current drawdown"""
        if self.current_drawdown < 5:
            return 1.0  # Normal sizing
        elif self.current_drawdown < 10:
            return 0.7  # Reduce by 30%
        elif self.current_drawdown < 15:
            return 0.5  # Reduce by 50%
        else:
            return 0.3  # Minimum sizing

    # ============================================================
    # 🚨 RISK VALIDATION
    # ============================================================

    def check_risk_limits(
        self,
        current_pnl: float,
        open_positions: int
    ) -> Tuple[bool, str]:
        """
        Check if current risk limits allow new trades

        Returns:
            Tuple of (can_trade, reason)
        """
        # Check if paused
        if self.is_paused:
            return False, f"Trading paused: {self.pause_reason}"

        # Daily loss limit
        if current_pnl <= -self.params.daily_loss_limit:
            self._pause_trading("Daily loss limit exceeded")
            return False, "Daily loss limit exceeded"

        # Kill switch check
        if current_pnl <= -self.params.kill_switch_loss:
            self._pause_trading("KILL SWITCH ACTIVATED")
            return False, "Kill switch activated - emergency stop"

        # Position limit
        if open_positions >= self.params.max_concurrent_positions:
            return False, "Maximum concurrent positions reached"

        # Daily trade limit
        if self.daily_trades >= self.params.max_daily_trades:
            return False, "Daily trade limit reached"

        # Consecutive losses check
        if self.consecutive_losses >= self.params.pause_after_losses:
            self._pause_trading(f"{self.consecutive_losses} consecutive losses")
            return False, "Too many consecutive losses"

        # Time between trades
        if self.last_trade_time:
            time_since_last = (datetime.now() - self.last_trade_time).seconds
            if time_since_last < self.params.min_trade_interval:
                return False, f"Wait {self.params.min_trade_interval - time_since_last}s before next trade"

        return True, "All risk checks passed"

    def validate_entry(
        self,
        symbol: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        position_size: float,
        account_balance: float
    ) -> Tuple[bool, str]:
        """
        Validate trade entry parameters

        Returns:
            Tuple of (is_valid, reason)
        """
        # Calculate risk/reward
        risk = abs(entry_price - stop_loss)
        reward = abs(take_profit - entry_price)

        if risk <= 0:
            return False, "Invalid stop loss"

        risk_reward = reward / risk

        # Check minimum R/R
        if risk_reward < self.params.min_risk_reward:
            return False, f"Risk/Reward too low: {risk_reward:.2f}"

        # Check position risk
        position_risk_amt = position_size * (risk / entry_price)
        position_risk_pct = (position_risk_amt / account_balance) * 100

        if position_risk_pct > self.params.max_position_risk:
            return False, f"Position risk too high: {position_risk_pct:.2f}%"

        # Check correlation with existing positions
        if not self._check_correlation_limits(symbol):
            return False, "Too many correlated positions"

        return True, "Entry validated"

    def _check_correlation_limits(self, symbol: str) -> bool:
        """Check if adding position violates correlation limits"""
        if len(self.open_positions) == 0:
            return True

        # Count correlated positions
        correlated_count = 0
        for existing_symbol in self.open_positions:
            correlation = self._get_correlation(symbol, existing_symbol)
            if abs(correlation) > self.params.correlation_threshold:
                correlated_count += 1

        return correlated_count < self.params.max_correlated_positions

    def _get_correlation(self, symbol1: str, symbol2: str) -> float:
        """Get correlation between two symbols"""
        pair = tuple(sorted([symbol1, symbol2]))

        # Return cached correlation if available
        if pair in self.position_correlations:
            return self.position_correlations[pair]

        # Default correlations for crypto assets
        default_correlations = {
            ('BTC', 'ETH'): 0.85,
            ('ETH', 'LINK'): 0.75,
            ('BTC', 'LINK'): 0.70,
            ('CRO', 'HBAR'): 0.60
        }

        correlation = default_correlations.get(pair, 0.5)
        self.position_correlations[pair] = correlation

        return correlation

    # ============================================================
    # 📈 RISK MONITORING
    # ============================================================

    def update_position(self, symbol: str, position_data: Dict):
        """Update position tracking"""
        self.open_positions[symbol] = position_data

        # Update P&L
        if 'pnl' in position_data:
            self._update_pnl(position_data['pnl'])

    def close_position(self, symbol: str, pnl: float):
        """Handle position closure"""
        if symbol in self.open_positions:
            del self.open_positions[symbol]

        # Update metrics
        self._update_pnl(pnl)
        self.daily_trades += 1
        self.last_trade_time = datetime.now()

        # Track consecutive losses
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

        # Record trade
        self.trade_history.append({
            'symbol': symbol,
            'pnl': pnl,
            'timestamp': datetime.now().isoformat()
        })

    def _update_pnl(self, pnl_change: float):
        """Update P&L tracking"""
        self.daily_pnl += pnl_change
        self.pnl_history.append(self.daily_pnl)

    def calculate_dynamic_stop(
        self,
        entry_price: float,
        volatility: float,
        base_stop: float = 0.02
    ) -> float:
        """
        Calculate dynamic stop loss based on volatility

        Args:
            entry_price: Entry price
            volatility: Current volatility
            base_stop: Base stop loss percentage

        Returns:
            Stop loss price
        """
        # ATR-based adjustment
        volatility_adjustment = min(volatility * 0.5, 0.03)

        # Drawdown-based adjustment (tighter stops in drawdown)
        if self.current_drawdown > 10:
            drawdown_adjustment = -0.005
        else:
            drawdown_adjustment = 0

        # Calculate final stop percentage
        stop_pct = base_stop + volatility_adjustment + drawdown_adjustment
        stop_pct = min(stop_pct, 0.05)  # Cap at 5%

        return entry_price * (1 - stop_pct)

    # ============================================================
    # 🚨 EMERGENCY CONTROLS
    # ============================================================

    def _pause_trading(self, reason: str):
        """Pause trading due to risk limits"""
        self.is_paused = True
        self.pause_reason = reason
        logger.warning(f"🚨 TRADING PAUSED: {reason}")

    def resume_trading(self):
        """Resume trading after pause"""
        self.is_paused = False
        self.pause_reason = ""
        self.consecutive_losses = 0
        logger.info("✅ Trading resumed")

    def reset_daily_limits(self):
        """Reset daily risk limits (call at start of trading day)"""
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.consecutive_losses = 0

        # Auto-resume if was paused for daily limits
        if "Daily" in self.pause_reason:
            self.resume_trading()

        logger.info("📅 Daily risk limits reset")

    def emergency_stop(self):
        """Emergency stop all trading"""
        self._pause_trading("EMERGENCY STOP ACTIVATED")
        logger.critical("🚨🚨🚨 EMERGENCY STOP - All trading halted")

        return {
            'action': 'EMERGENCY_STOP',
            'open_positions': len(self.open_positions),
            'daily_pnl': self.daily_pnl,
            'current_drawdown': self.current_drawdown,
            'reason': 'Manual emergency stop or critical risk breach'
        }

    # ============================================================
    # 📊 REPORTING
    # ============================================================

    def generate_risk_report(self) -> Dict:
        """Generate comprehensive risk report"""
        return {
            'timestamp': datetime.now().isoformat(),
            'risk_status': {
                'is_paused': self.is_paused,
                'pause_reason': self.pause_reason,
                'daily_pnl': self.daily_pnl,
                'daily_trades': self.daily_trades,
                'consecutive_losses': self.consecutive_losses,
                'current_drawdown': self.current_drawdown
            },
            'positions': {
                'open_count': len(self.open_positions),
                'positions': list(self.open_positions.keys()),
                'max_allowed': self.params.max_concurrent_positions
            },
            'limits': {
                'daily_loss_limit': self.params.daily_loss_limit,
                'max_drawdown': self.params.max_account_risk,
                'kill_switch': self.params.kill_switch_loss,
                'daily_trades_remaining': self.params.max_daily_trades - self.daily_trades
            },
            'recent_trades': self.trade_history[-10:] if self.trade_history else [],
            'recommendations': self._generate_recommendations()
        }

    def _generate_recommendations(self) -> List[str]:
        """Generate risk-based recommendations"""
        recommendations = []

        if self.current_drawdown > 10:
            recommendations.append("Consider reducing position sizes due to drawdown")

        if self.consecutive_losses >= 2:
            recommendations.append("Review strategy - consecutive losses detected")

        if self.daily_trades > self.params.max_daily_trades * 0.8:
            recommendations.append("Approaching daily trade limit - be selective")

        if self.daily_pnl < -self.params.daily_loss_limit * 0.7:
            recommendations.append("Approaching daily loss limit - consider stopping")

        return recommendations

# 🌙💫🚀 Risk Management Ready for Production! 🌙💫🚀