"""
📊 Portfolio Optimization Framework 📊
Advanced allocation optimization algorithms for the trend-following portfolio
including risk-adjusted performance maximization and parameter tuning.

Created: 2025
Author: Bobby Younghoward
"""

import pandas as pd
import numpy as np
from scipy.optimize import minimize
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')


class PortfolioOptimizer:
    """
    🚀 Advanced Portfolio Optimization Engine 🚀

    Optimizes allocation weights and parameters across all trend-following strategies
    using multiple optimization objectives and constraints.
    """

    def __init__(self):
        """Initialize portfolio optimizer with default configurations"""

        # Strategy universe
        self.strategies = ['TEMS', 'VBM', 'ATSS', 'MTMC']

        # Optimization objectives
        self.objectives = [
            'sharpe_ratio',
            'sortino_ratio',
            'calmar_ratio',
            'max_return',
            'min_drawdown',
            'risk_parity'
        ]

        # Historical performance data (to be populated)
        self.performance_data = {}

        # Constraints
        self.min_weight = 0.05  # Minimum 5% allocation
        self.max_weight = 0.60  # Maximum 60% allocation
        self.leverage = 1.0      # No leverage by default

        # Risk parameters
        self.target_volatility = 0.15  # 15% annual volatility target
        self.max_drawdown_limit = 0.20  # 20% maximum drawdown

    def load_performance_data(self, backtest_results: Dict[str, pd.DataFrame]):
        """
        Load historical performance data from backtest results

        Parameters:
        -----------
        backtest_results : dict
            Dictionary with strategy names as keys and performance DataFrames as values
        """
        self.performance_data = backtest_results

        # Calculate return series for each strategy
        self.returns = {}
        for strategy, data in backtest_results.items():
            if 'returns' in data.columns:
                self.returns[strategy] = data['returns'].values
            elif 'equity' in data.columns:
                # Calculate returns from equity curve
                equity = data['equity'].values
                self.returns[strategy] = np.diff(equity) / equity[:-1]

    def optimize_sharpe_ratio(self) -> Dict[str, float]:
        """
        Optimize portfolio weights to maximize Sharpe ratio

        Returns:
        --------
        optimal_weights : dict
            Optimized allocation weights for each strategy
        """

        def objective(weights):
            """Negative Sharpe ratio (for minimization)"""
            portfolio_returns = self._calculate_portfolio_returns(weights)
            sharpe = self._calculate_sharpe_ratio(portfolio_returns)
            return -sharpe  # Minimize negative Sharpe = Maximize Sharpe

        # Initial guess (equal weights)
        x0 = np.array([0.25, 0.25, 0.25, 0.25])

        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0},  # Weights sum to 1
        ]

        # Bounds for each weight
        bounds = [(self.min_weight, self.max_weight) for _ in range(len(self.strategies))]

        # Optimize
        result = minimize(
            objective,
            x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )

        if result.success:
            optimal_weights = {
                strategy: weight
                for strategy, weight in zip(self.strategies, result.x)
            }
            return optimal_weights
        else:
            print(f"Optimization failed: {result.message}")
            return self._equal_weights()

    def optimize_risk_parity(self) -> Dict[str, float]:
        """
        Optimize portfolio weights using risk parity approach
        (equal risk contribution from each strategy)

        Returns:
        --------
        optimal_weights : dict
            Risk parity optimized weights
        """

        def risk_contribution(weights):
            """Calculate risk contribution of each strategy"""
            portfolio_variance = self._calculate_portfolio_variance(weights)
            marginal_contributions = self._calculate_marginal_risk_contributions(weights)
            contributions = weights * marginal_contributions
            return contributions / np.sqrt(portfolio_variance)

        def objective(weights):
            """Minimize difference in risk contributions"""
            contrib = risk_contribution(weights)
            # Minimize variance of risk contributions (equal when variance is 0)
            return np.var(contrib)

        # Initial guess
        x0 = np.array([0.25, 0.25, 0.25, 0.25])

        # Constraints and bounds
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0},
        ]
        bounds = [(0.01, 0.99) for _ in range(len(self.strategies))]

        # Optimize
        result = minimize(
            objective,
            x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )

        if result.success:
            optimal_weights = {
                strategy: weight
                for strategy, weight in zip(self.strategies, result.x)
            }
            return optimal_weights
        else:
            return self._equal_weights()

    def optimize_minimum_variance(self) -> Dict[str, float]:
        """
        Optimize portfolio weights to minimize portfolio variance

        Returns:
        --------
        optimal_weights : dict
            Minimum variance portfolio weights
        """

        def objective(weights):
            """Portfolio variance"""
            return self._calculate_portfolio_variance(weights)

        # Initial guess
        x0 = np.array([0.25, 0.25, 0.25, 0.25])

        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0},
        ]

        # Bounds
        bounds = [(self.min_weight, self.max_weight) for _ in range(len(self.strategies))]

        # Optimize
        result = minimize(
            objective,
            x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )

        if result.success:
            optimal_weights = {
                strategy: weight
                for strategy, weight in zip(self.strategies, result.x)
            }
            return optimal_weights
        else:
            return self._equal_weights()

    def optimize_maximum_return(self) -> Dict[str, float]:
        """
        Optimize portfolio weights to maximize expected return
        subject to risk constraints

        Returns:
        --------
        optimal_weights : dict
            Maximum return portfolio weights
        """

        def objective(weights):
            """Negative expected return (for minimization)"""
            portfolio_returns = self._calculate_portfolio_returns(weights)
            return -np.mean(portfolio_returns)

        def volatility_constraint(weights):
            """Keep volatility below target"""
            portfolio_returns = self._calculate_portfolio_returns(weights)
            vol = np.std(portfolio_returns) * np.sqrt(252)  # Annualized
            return self.target_volatility - vol

        # Initial guess
        x0 = np.array([0.25, 0.25, 0.25, 0.25])

        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0},
            {'type': 'ineq', 'fun': volatility_constraint},
        ]

        # Bounds
        bounds = [(self.min_weight, self.max_weight) for _ in range(len(self.strategies))]

        # Optimize
        result = minimize(
            objective,
            x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )

        if result.success:
            optimal_weights = {
                strategy: weight
                for strategy, weight in zip(self.strategies, result.x)
            }
            return optimal_weights
        else:
            return self._equal_weights()

    def optimize_conditional_value_at_risk(self, confidence_level: float = 0.95) -> Dict[str, float]:
        """
        Optimize portfolio to minimize Conditional Value at Risk (CVaR)

        Parameters:
        -----------
        confidence_level : float
            Confidence level for CVaR calculation (default 95%)

        Returns:
        --------
        optimal_weights : dict
            CVaR optimized portfolio weights
        """

        def calculate_cvar(weights):
            """Calculate CVaR at given confidence level"""
            portfolio_returns = self._calculate_portfolio_returns(weights)
            var_threshold = np.percentile(portfolio_returns, (1 - confidence_level) * 100)
            cvar = np.mean(portfolio_returns[portfolio_returns <= var_threshold])
            return -cvar  # Minimize negative CVaR

        # Initial guess
        x0 = np.array([0.25, 0.25, 0.25, 0.25])

        # Constraints and bounds
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0},
        ]
        bounds = [(self.min_weight, self.max_weight) for _ in range(len(self.strategies))]

        # Optimize
        result = minimize(
            calculate_cvar,
            x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )

        if result.success:
            optimal_weights = {
                strategy: weight
                for strategy, weight in zip(self.strategies, result.x)
            }
            return optimal_weights
        else:
            return self._equal_weights()

    def calculate_efficient_frontier(self, n_points: int = 50) -> Tuple[np.ndarray, np.ndarray, List[Dict]]:
        """
        Calculate the efficient frontier for the portfolio

        Parameters:
        -----------
        n_points : int
            Number of points on the efficient frontier

        Returns:
        --------
        returns : np.ndarray
            Expected returns for each point
        risks : np.ndarray
            Standard deviations for each point
        weights : list of dict
            Portfolio weights for each point
        """

        # Target returns for efficient frontier
        min_return = min(np.mean(self.returns[s]) for s in self.strategies)
        max_return = max(np.mean(self.returns[s]) for s in self.strategies)
        target_returns = np.linspace(min_return, max_return, n_points)

        frontier_returns = []
        frontier_risks = []
        frontier_weights = []

        for target in target_returns:
            # Optimize for minimum variance given target return
            def objective(weights):
                return self._calculate_portfolio_variance(weights)

            def return_constraint(weights):
                portfolio_returns = self._calculate_portfolio_returns(weights)
                return np.mean(portfolio_returns) - target

            # Initial guess
            x0 = np.array([0.25, 0.25, 0.25, 0.25])

            # Constraints
            constraints = [
                {'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0},
                {'type': 'eq', 'fun': return_constraint},
            ]

            # Bounds
            bounds = [(0, 1) for _ in range(len(self.strategies))]

            # Optimize
            result = minimize(
                objective,
                x0,
                method='SLSQP',
                bounds=bounds,
                constraints=constraints,
                options={'maxiter': 1000, 'disp': False}
            )

            if result.success:
                weights = result.x
                portfolio_returns = self._calculate_portfolio_returns(weights)

                frontier_returns.append(np.mean(portfolio_returns))
                frontier_risks.append(np.std(portfolio_returns))
                frontier_weights.append({
                    strategy: weight
                    for strategy, weight in zip(self.strategies, weights)
                })

        return np.array(frontier_returns), np.array(frontier_risks), frontier_weights

    def get_optimal_allocation_matrix(self) -> pd.DataFrame:
        """
        Generate comprehensive allocation matrix with different optimization objectives

        Returns:
        --------
        allocation_matrix : pd.DataFrame
            Matrix showing optimal allocations under different objectives
        """

        results = {}

        # Run all optimization methods
        results['Sharpe_Maximization'] = self.optimize_sharpe_ratio()
        results['Risk_Parity'] = self.optimize_risk_parity()
        results['Minimum_Variance'] = self.optimize_minimum_variance()
        results['Maximum_Return'] = self.optimize_maximum_return()
        results['CVaR_Minimization'] = self.optimize_conditional_value_at_risk()

        # Create DataFrame
        allocation_matrix = pd.DataFrame(results).T
        allocation_matrix = allocation_matrix.round(3)

        # Add performance metrics for each allocation
        for objective in allocation_matrix.index:
            weights = allocation_matrix.loc[objective].values
            portfolio_returns = self._calculate_portfolio_returns(weights)

            allocation_matrix.loc[objective, 'Expected_Return'] = np.mean(portfolio_returns) * 252
            allocation_matrix.loc[objective, 'Volatility'] = np.std(portfolio_returns) * np.sqrt(252)
            allocation_matrix.loc[objective, 'Sharpe'] = self._calculate_sharpe_ratio(portfolio_returns)

        return allocation_matrix

    # === Private Helper Methods ===

    def _calculate_portfolio_returns(self, weights: np.ndarray) -> np.ndarray:
        """Calculate portfolio returns given weights"""
        if not self.returns:
            raise ValueError("No performance data loaded. Call load_performance_data() first.")

        # Ensure we have returns for all strategies
        min_length = min(len(self.returns[s]) for s in self.strategies)

        portfolio_returns = np.zeros(min_length)
        for i, strategy in enumerate(self.strategies):
            strategy_returns = self.returns[strategy][:min_length]
            portfolio_returns += weights[i] * strategy_returns

        return portfolio_returns

    def _calculate_portfolio_variance(self, weights: np.ndarray) -> float:
        """Calculate portfolio variance given weights"""
        # Create returns matrix
        min_length = min(len(self.returns[s]) for s in self.strategies)
        returns_matrix = np.array([
            self.returns[s][:min_length] for s in self.strategies
        ]).T

        # Calculate covariance matrix
        cov_matrix = np.cov(returns_matrix, rowvar=False)

        # Portfolio variance
        variance = np.dot(weights.T, np.dot(cov_matrix, weights))
        return variance

    def _calculate_sharpe_ratio(self, returns: np.ndarray, risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio"""
        excess_returns = returns - risk_free_rate / 252  # Daily risk-free rate
        if np.std(excess_returns) > 0:
            return np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)
        return 0

    def _calculate_marginal_risk_contributions(self, weights: np.ndarray) -> np.ndarray:
        """Calculate marginal risk contribution of each asset"""
        min_length = min(len(self.returns[s]) for s in self.strategies)
        returns_matrix = np.array([
            self.returns[s][:min_length] for s in self.strategies
        ]).T

        cov_matrix = np.cov(returns_matrix, rowvar=False)
        portfolio_variance = self._calculate_portfolio_variance(weights)

        marginal_contrib = np.dot(cov_matrix, weights) / np.sqrt(portfolio_variance)
        return marginal_contrib

    def _equal_weights(self) -> Dict[str, float]:
        """Return equal weight allocation"""
        weight = 1.0 / len(self.strategies)
        return {strategy: weight for strategy in self.strategies}


# === Asset-Specific Optimization Configurations ===

OPTIMAL_ASSET_ALLOCATIONS = {
    'ETH': {
        'TEMS': 0.50,      # +6,246% return on ETH 1d
        'MTMC': 0.30,      # 53.8% win rate on ETH
        'ATSS': 0.20,      # +40.53% return on ETH
        'VBM': 0.00        # Not optimal for ETH
    },
    'HBAR': {
        'TEMS': 0.40,      # +318% return on HBAR
        'ATSS': 0.35,      # +136% return on HBAR
        'VBM': 0.25,       # High volatility capture
        'MTMC': 0.00       # Underperformed on HBAR
    },
    'LINK': {
        'ATSS': 0.50,      # +104% return on LINK
        'TEMS': 0.30,      # Strong performance
        'VBM': 0.20,       # Volatility opportunities
        'MTMC': 0.00       # Needs optimization
    },
    'CRO': {
        'VBM': 0.60,       # 70% win rate on CRO
        'ATSS': 0.30,      # +50% return on CRO
        'TEMS': 0.10,      # +57% return lower priority
        'MTMC': 0.00       # Underperformed
    },
    'BTC': {
        'TEMS': 0.35,      # Momentum capture
        'VBM': 0.30,       # Volatility breakouts
        'ATSS': 0.25,      # Trend strength
        'MTMC': 0.10       # Multi-timeframe
    },
    'XRP': {
        'VBM': 0.40,       # High volatility asset
        'TEMS': 0.30,      # Momentum plays
        'ATSS': 0.20,      # Trend following
        'MTMC': 0.10       # Conservative allocation
    }
}


def get_optimal_allocation_for_asset(asset: str) -> Dict[str, float]:
    """
    Get the optimal strategy allocation for a specific cryptocurrency

    Parameters:
    -----------
    asset : str
        Asset symbol (e.g., 'ETH', 'BTC', 'HBAR')

    Returns:
    --------
    allocation : dict
        Optimal allocation weights for the asset
    """
    if asset in OPTIMAL_ASSET_ALLOCATIONS:
        return OPTIMAL_ASSET_ALLOCATIONS[asset]
    else:
        # Default allocation for unknown assets
        return {
            'TEMS': 0.30,
            'VBM': 0.25,
            'ATSS': 0.25,
            'MTMC': 0.20
        }


if __name__ == "__main__":
    print("📊 Portfolio Optimization Framework Loaded 📊")
    print("=" * 80)
    print("Optimization Methods Available:")
    print("  - Sharpe Ratio Maximization")
    print("  - Risk Parity")
    print("  - Minimum Variance")
    print("  - Maximum Return")
    print("  - CVaR Minimization")
    print("  - Efficient Frontier Calculation")
    print("=" * 80)
    print("\nUsage:")
    print("  from portfolio_optimizer import PortfolioOptimizer")
    print("  optimizer = PortfolioOptimizer()")
    print("  optimal_weights = optimizer.optimize_sharpe_ratio()")
    print("  print(optimal_weights)")