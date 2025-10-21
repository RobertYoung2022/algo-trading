"""
🛡️ Portfolio Risk Management System 🛡️
Cross-strategy risk management, correlation monitoring, and
portfolio-level protection mechanisms.

Created: 2025
Author: Bobby Younghoward
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


@dataclass
class Position:
    """Data class for tracking individual positions"""
    strategy: str
    asset: str
    size: float
    entry_price: float
    entry_time: datetime
    current_price: float
    stop_loss: float
    take_profit: float
    pnl: float
    pnl_percent: float


@dataclass
class RiskMetrics:
    """Data class for portfolio risk metrics"""
    total_exposure: float
    portfolio_var: float  # Value at Risk
    portfolio_cvar: float  # Conditional Value at Risk
    current_drawdown: float
    max_drawdown: float
    correlation_risk: float
    concentration_risk: float
    liquidity_risk: float


class PortfolioRiskManager:
    """
    🛡️ Comprehensive Risk Management Across All Strategies 🛡️

    Manages risk at the portfolio level, monitoring correlations,
    exposures, and implementing protective mechanisms.
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize portfolio risk manager with configuration

        Parameters:
        -----------
        config : dict, optional
            Risk management configuration parameters
        """

        # Default configuration
        default_config = {
            'max_portfolio_exposure': 0.08,      # 8% maximum exposure
            'max_portfolio_drawdown': 0.15,      # 15% maximum drawdown
            'max_correlation_limit': 0.70,       # 70% correlation limit
            'max_positions': 6,                  # Maximum concurrent positions
            'max_position_size': 0.03,           # 3% max per position
            'var_confidence': 0.95,              # 95% VaR confidence
            'lookback_period': 30,               # 30-day lookback for metrics
            'rebalance_threshold': 0.10,        # 10% drift triggers rebalance
            'emergency_stop_loss': 0.20,        # 20% portfolio stop loss
            'risk_free_rate': 0.02              # 2% annual risk-free rate
        }

        # Merge with provided config
        self.config = {**default_config, **(config or {})}

        # Initialize tracking
        self.positions: List[Position] = []
        self.historical_returns: List[float] = []
        self.correlation_matrix: Optional[pd.DataFrame] = None
        self.risk_metrics: Optional[RiskMetrics] = None

        # Risk states
        self.emergency_mode = False
        self.reduce_risk_mode = False
        self.last_rebalance = datetime.now()

        # Performance tracking
        self.peak_equity = 0
        self.current_equity = 0
        self.drawdown_start = None
        self.consecutive_losses = 0

    def add_position(self, position: Position) -> bool:
        """
        Add a new position with risk checks

        Parameters:
        -----------
        position : Position
            New position to add

        Returns:
        --------
        bool
            True if position added, False if rejected
        """

        # Check emergency mode
        if self.emergency_mode:
            print("❌ Emergency mode active - no new positions allowed")
            return False

        # Check position limits
        if len(self.positions) >= self.config['max_positions']:
            print(f"❌ Maximum positions ({self.config['max_positions']}) reached")
            return False

        # Check position size
        if position.size > self.config['max_position_size']:
            print(f"❌ Position size {position.size:.2%} exceeds maximum {self.config['max_position_size']:.2%}")
            return False

        # Check total exposure
        total_exposure = self.calculate_total_exposure() + position.size
        if total_exposure > self.config['max_portfolio_exposure']:
            print(f"❌ Total exposure {total_exposure:.2%} would exceed maximum {self.config['max_portfolio_exposure']:.2%}")
            return False

        # Check correlation with existing positions
        if not self.check_correlation_limits(position):
            print(f"❌ Position correlation exceeds limits")
            return False

        # Add position
        self.positions.append(position)
        print(f"✅ Position added: {position.strategy} - {position.asset} (size: {position.size:.2%})")

        # Update risk metrics
        self.update_risk_metrics()

        return True

    def update_position_prices(self, price_updates: Dict[str, float]):
        """
        Update current prices for all positions

        Parameters:
        -----------
        price_updates : dict
            Dictionary of asset: current_price
        """

        for position in self.positions:
            if position.asset in price_updates:
                position.current_price = price_updates[position.asset]
                position.pnl = (position.current_price - position.entry_price) * position.size
                position.pnl_percent = (position.current_price / position.entry_price - 1) * 100

        # Check for stop losses and risk limits
        self.check_stop_losses()
        self.check_portfolio_risk_limits()

    def check_stop_losses(self) -> List[Position]:
        """
        Check and execute stop losses for all positions

        Returns:
        --------
        stopped_positions : list
            List of positions that hit stop loss
        """

        stopped_positions = []

        for position in self.positions[:]:  # Copy list for safe iteration
            # Individual position stop loss
            if position.current_price <= position.stop_loss:
                print(f"⛔ Stop loss hit for {position.asset}: {position.pnl_percent:.2f}%")
                self.positions.remove(position)
                stopped_positions.append(position)

            # Individual position take profit
            elif position.current_price >= position.take_profit:
                print(f"🎯 Take profit hit for {position.asset}: {position.pnl_percent:.2f}%")
                self.positions.remove(position)
                stopped_positions.append(position)

        return stopped_positions

    def check_portfolio_risk_limits(self):
        """Check and enforce portfolio-level risk limits"""

        # Calculate current drawdown
        current_drawdown = self.calculate_portfolio_drawdown()

        # Emergency stop loss
        if current_drawdown > self.config['emergency_stop_loss']:
            self.trigger_emergency_stop()
            return

        # Maximum drawdown protection
        if current_drawdown > self.config['max_portfolio_drawdown']:
            self.reduce_portfolio_exposure(reduction_factor=0.5)
            self.reduce_risk_mode = True
            print(f"⚠️ Maximum drawdown exceeded ({current_drawdown:.2%}) - reducing exposure")

        # Correlation risk check
        if self.risk_metrics and self.risk_metrics.correlation_risk > 0.8:
            self.reduce_correlated_positions()
            print(f"⚠️ High correlation risk ({self.risk_metrics.correlation_risk:.2f}) - reducing correlated positions")

    def calculate_total_exposure(self) -> float:
        """
        Calculate total portfolio exposure

        Returns:
        --------
        float
            Total exposure as fraction of portfolio
        """
        return sum(position.size for position in self.positions)

    def calculate_portfolio_drawdown(self) -> float:
        """
        Calculate current portfolio drawdown

        Returns:
        --------
        float
            Current drawdown as percentage
        """

        if self.peak_equity == 0:
            return 0

        drawdown = (self.peak_equity - self.current_equity) / self.peak_equity
        return max(0, drawdown)

    def calculate_value_at_risk(self, confidence: Optional[float] = None) -> float:
        """
        Calculate portfolio Value at Risk (VaR)

        Parameters:
        -----------
        confidence : float, optional
            Confidence level (default from config)

        Returns:
        --------
        float
            VaR at specified confidence level
        """

        if len(self.historical_returns) < 20:
            return 0

        confidence = confidence or self.config['var_confidence']
        returns = np.array(self.historical_returns[-self.config['lookback_period']:])

        # Calculate VaR
        var = np.percentile(returns, (1 - confidence) * 100)
        return abs(var)

    def calculate_conditional_value_at_risk(self, confidence: Optional[float] = None) -> float:
        """
        Calculate portfolio Conditional Value at Risk (CVaR)

        Parameters:
        -----------
        confidence : float, optional
            Confidence level (default from config)

        Returns:
        --------
        float
            CVaR at specified confidence level
        """

        if len(self.historical_returns) < 20:
            return 0

        confidence = confidence or self.config['var_confidence']
        returns = np.array(self.historical_returns[-self.config['lookback_period']:])

        # Calculate CVaR
        var_threshold = np.percentile(returns, (1 - confidence) * 100)
        cvar = np.mean(returns[returns <= var_threshold])
        return abs(cvar)

    def check_correlation_limits(self, new_position: Position) -> bool:
        """
        Check if new position violates correlation limits

        Parameters:
        -----------
        new_position : Position
            Proposed new position

        Returns:
        --------
        bool
            True if within limits, False otherwise
        """

        if self.correlation_matrix is None or len(self.positions) == 0:
            return True

        # Check correlation with each existing position
        for position in self.positions:
            if position.asset in self.correlation_matrix and new_position.asset in self.correlation_matrix:
                correlation = self.correlation_matrix.loc[position.asset, new_position.asset]

                if abs(correlation) > self.config['max_correlation_limit']:
                    # Allow if opposite strategies (hedging)
                    if position.strategy != new_position.strategy:
                        return True
                    return False

        return True

    def update_correlation_matrix(self, price_data: pd.DataFrame):
        """
        Update correlation matrix from price data

        Parameters:
        -----------
        price_data : pd.DataFrame
            DataFrame with asset prices as columns
        """

        if len(price_data) < 20:
            return

        # Calculate returns
        returns = price_data.pct_change().dropna()

        # Calculate correlation matrix
        self.correlation_matrix = returns.corr()

    def calculate_position_sizing(self, signal_strength: float, asset_volatility: float) -> float:
        """
        Calculate optimal position size based on Kelly Criterion and risk limits

        Parameters:
        -----------
        signal_strength : float
            Signal strength (0 to 1)
        asset_volatility : float
            Asset volatility (annualized)

        Returns:
        --------
        float
            Optimal position size as fraction of portfolio
        """

        # Base position size
        base_size = self.config['max_position_size']

        # Adjust for signal strength
        signal_adjustment = min(1.5, max(0.5, signal_strength))

        # Adjust for volatility (inverse relationship)
        volatility_adjustment = min(1.5, max(0.5, 0.15 / asset_volatility))

        # Adjust for current risk mode
        risk_adjustment = 1.0
        if self.emergency_mode:
            risk_adjustment = 0
        elif self.reduce_risk_mode:
            risk_adjustment = 0.5

        # Adjust for current drawdown
        drawdown_adjustment = 1.0
        current_dd = self.calculate_portfolio_drawdown()
        if current_dd > 0.10:  # >10% drawdown
            drawdown_adjustment = 0.5
        elif current_dd > 0.05:  # >5% drawdown
            drawdown_adjustment = 0.75

        # Calculate final position size
        position_size = base_size * signal_adjustment * volatility_adjustment * risk_adjustment * drawdown_adjustment

        # Apply limits
        position_size = min(position_size, self.config['max_position_size'])
        position_size = max(position_size, 0)

        return position_size

    def reduce_portfolio_exposure(self, reduction_factor: float = 0.5):
        """
        Reduce portfolio exposure by closing or reducing positions

        Parameters:
        -----------
        reduction_factor : float
            Factor by which to reduce exposure (0.5 = 50% reduction)
        """

        print(f"📉 Reducing portfolio exposure by {reduction_factor:.0%}")

        # Sort positions by PnL (close losers first)
        self.positions.sort(key=lambda p: p.pnl)

        # Calculate target exposure
        current_exposure = self.calculate_total_exposure()
        target_exposure = current_exposure * (1 - reduction_factor)

        # Close positions until target reached
        while self.calculate_total_exposure() > target_exposure and len(self.positions) > 0:
            position = self.positions[0]
            print(f"  Closing {position.asset} (PnL: {position.pnl_percent:.2f}%)")
            self.positions.remove(position)

    def reduce_correlated_positions(self):
        """Reduce highly correlated positions to manage concentration risk"""

        if self.correlation_matrix is None or len(self.positions) < 2:
            return

        # Find highly correlated position pairs
        high_correlation_pairs = []

        for i, pos1 in enumerate(self.positions):
            for pos2 in self.positions[i+1:]:
                if pos1.asset in self.correlation_matrix and pos2.asset in self.correlation_matrix:
                    corr = self.correlation_matrix.loc[pos1.asset, pos2.asset]

                    if abs(corr) > self.config['max_correlation_limit']:
                        high_correlation_pairs.append((pos1, pos2, corr))

        # Reduce smaller position in each highly correlated pair
        for pos1, pos2, corr in high_correlation_pairs:
            if pos1.size < pos2.size:
                if pos1 in self.positions:
                    print(f"  Reducing correlated position: {pos1.asset} (corr: {corr:.2f})")
                    self.positions.remove(pos1)
            else:
                if pos2 in self.positions:
                    print(f"  Reducing correlated position: {pos2.asset} (corr: {corr:.2f})")
                    self.positions.remove(pos2)

    def trigger_emergency_stop(self):
        """Trigger emergency stop - close all positions immediately"""

        print("🚨 EMERGENCY STOP TRIGGERED - CLOSING ALL POSITIONS")
        self.emergency_mode = True

        # Close all positions
        for position in self.positions[:]:
            print(f"  Emergency close: {position.asset} (PnL: {position.pnl_percent:.2f}%)")

        self.positions.clear()

        # Set recovery period
        self.recovery_end_time = datetime.now() + timedelta(hours=24)
        print(f"  Trading suspended until {self.recovery_end_time}")

    def update_risk_metrics(self):
        """Update comprehensive risk metrics for the portfolio"""

        # Calculate metrics
        total_exposure = self.calculate_total_exposure()
        portfolio_var = self.calculate_value_at_risk()
        portfolio_cvar = self.calculate_conditional_value_at_risk()
        current_drawdown = self.calculate_portfolio_drawdown()

        # Max drawdown tracking
        if current_drawdown > getattr(self.risk_metrics, 'max_drawdown', 0):
            max_drawdown = current_drawdown
        else:
            max_drawdown = getattr(self.risk_metrics, 'max_drawdown', 0)

        # Correlation risk
        correlation_risk = self.calculate_correlation_risk()

        # Concentration risk
        concentration_risk = self.calculate_concentration_risk()

        # Liquidity risk (simplified - based on position count)
        liquidity_risk = len(self.positions) / self.config['max_positions']

        # Update metrics
        self.risk_metrics = RiskMetrics(
            total_exposure=total_exposure,
            portfolio_var=portfolio_var,
            portfolio_cvar=portfolio_cvar,
            current_drawdown=current_drawdown,
            max_drawdown=max_drawdown,
            correlation_risk=correlation_risk,
            concentration_risk=concentration_risk,
            liquidity_risk=liquidity_risk
        )

    def calculate_correlation_risk(self) -> float:
        """
        Calculate portfolio correlation risk

        Returns:
        --------
        float
            Correlation risk score (0 to 1)
        """

        if self.correlation_matrix is None or len(self.positions) < 2:
            return 0

        correlations = []
        for i, pos1 in enumerate(self.positions):
            for pos2 in self.positions[i+1:]:
                if pos1.asset in self.correlation_matrix and pos2.asset in self.correlation_matrix:
                    corr = abs(self.correlation_matrix.loc[pos1.asset, pos2.asset])
                    correlations.append(corr)

        if correlations:
            return np.mean(correlations)
        return 0

    def calculate_concentration_risk(self) -> float:
        """
        Calculate portfolio concentration risk using Herfindahl index

        Returns:
        --------
        float
            Concentration risk score (0 to 1)
        """

        if len(self.positions) == 0:
            return 0

        total_exposure = self.calculate_total_exposure()
        if total_exposure == 0:
            return 0

        # Calculate Herfindahl index
        herfindahl = sum((pos.size / total_exposure) ** 2 for pos in self.positions)

        return herfindahl

    def get_risk_report(self) -> Dict:
        """
        Generate comprehensive risk report

        Returns:
        --------
        dict
            Detailed risk report
        """

        if self.risk_metrics is None:
            self.update_risk_metrics()

        report = {
            'timestamp': datetime.now().isoformat(),
            'portfolio_metrics': {
                'total_exposure': f"{self.risk_metrics.total_exposure:.2%}",
                'position_count': len(self.positions),
                'current_drawdown': f"{self.risk_metrics.current_drawdown:.2%}",
                'max_drawdown': f"{self.risk_metrics.max_drawdown:.2%}",
                'var_95': f"{self.risk_metrics.portfolio_var:.2%}",
                'cvar_95': f"{self.risk_metrics.portfolio_cvar:.2%}",
            },
            'risk_scores': {
                'correlation_risk': f"{self.risk_metrics.correlation_risk:.2f}",
                'concentration_risk': f"{self.risk_metrics.concentration_risk:.2f}",
                'liquidity_risk': f"{self.risk_metrics.liquidity_risk:.2f}",
            },
            'risk_status': {
                'emergency_mode': self.emergency_mode,
                'reduce_risk_mode': self.reduce_risk_mode,
                'positions_at_limit': len(self.positions) >= self.config['max_positions'],
                'exposure_at_limit': self.risk_metrics.total_exposure >= self.config['max_portfolio_exposure'] * 0.9,
            },
            'positions': [
                {
                    'asset': pos.asset,
                    'strategy': pos.strategy,
                    'size': f"{pos.size:.2%}",
                    'pnl': f"{pos.pnl_percent:.2f}%",
                }
                for pos in self.positions
            ]
        }

        return report

    def reset_emergency_mode(self):
        """Reset emergency mode after recovery period"""

        if self.emergency_mode and hasattr(self, 'recovery_end_time'):
            if datetime.now() > self.recovery_end_time:
                self.emergency_mode = False
                self.reduce_risk_mode = False
                print("✅ Emergency mode lifted - normal trading resumed")
                return True

        return False


if __name__ == "__main__":
    print("🛡️ Portfolio Risk Management System Loaded 🛡️")
    print("=" * 80)
    print("Risk Controls:")
    print("  - Portfolio-level stop loss and drawdown protection")
    print("  - Correlation monitoring and limits")
    print("  - Position sizing optimization")
    print("  - Emergency stop mechanisms")
    print("  - VaR and CVaR calculations")
    print("=" * 80)
    print("\nUsage:")
    print("  from portfolio_risk_manager import PortfolioRiskManager, Position")
    print("  risk_manager = PortfolioRiskManager()")
    print("  position = Position(...)")
    print("  risk_manager.add_position(position)")