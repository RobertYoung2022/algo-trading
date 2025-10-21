"""
📊 Statistical Significance Validator - Phase 0 Diagnostic Tool
===============================================================
Validates if strategy results are statistically significant or luck.

🎯 Purpose:
    - Determine minimum trades needed for confidence
    - Calculate confidence intervals for returns
    - Run Monte Carlo simulations (bootstrapping)
    - Test win rate significance vs coin flip

💫 Statistical Tests:
    1. Sample Size Test - Minimum trades for 95% confidence
    2. Win Rate Z-Test - Is win rate significantly different from 50%?
    3. Bootstrap Confidence Intervals - 95% CI for expected returns
    4. Monte Carlo Simulation - Randomize trade order 1000× times
    5. Sharpe Ratio Stability - Is risk-adjusted return stable?

🌙💫🚀 Bobby's Algo-Fun Project
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
from scipy import stats
import warnings
warnings.filterwarnings('ignore')


@dataclass
class StatisticalValidationResult:
    """Results from statistical significance testing"""

    # Sample size analysis
    actual_trades: int
    required_trades_95pct: int
    required_trades_99pct: int
    sample_size_adequate: bool

    # Win rate significance
    win_rate: float
    win_rate_z_score: float
    win_rate_p_value: float
    win_rate_significant: bool

    # Bootstrap confidence intervals
    mean_return: float
    ci_95_lower: float
    ci_95_upper: float
    ci_99_lower: float
    ci_99_upper: float

    # Monte Carlo simulation
    monte_carlo_mean: float
    monte_carlo_std: float
    profitable_simulations_pct: float
    monte_carlo_passed: bool

    # Sharpe ratio analysis
    sharpe_ratio: float
    sharpe_stable: bool

    # Overall verdict
    is_statistically_significant: bool
    confidence_level: str  # 'High', 'Medium', 'Low', 'None'
    edge_likely_real: bool


class StatisticalValidator:
    """
    📊 Validates statistical significance of strategy results

    Uses multiple statistical tests to determine if edge is real or luck
    """

    def __init__(self, trades_data: pd.DataFrame):
        """
        Initialize validator with trade data

        Args:
            trades_data: Trades DataFrame from backtesting.py
        """
        self.trades_data = trades_data.copy()

        # Calculate returns if not present
        if 'ReturnPct' not in self.trades_data.columns and 'Return_%' not in self.trades_data.columns:
            self.trades_data['ReturnPct'] = (
                (self.trades_data['ExitPrice'] - self.trades_data['EntryPrice']) /
                self.trades_data['EntryPrice'] * 100
            )
        elif 'Return_%' in self.trades_data.columns:
            self.trades_data['ReturnPct'] = self.trades_data['Return_%']

    def validate(self,
                 confidence_level: float = 0.95,
                 monte_carlo_simulations: int = 1000) -> StatisticalValidationResult:
        """
        📊 Perform comprehensive statistical validation

        Args:
            confidence_level: Confidence level for tests (default 0.95)
            monte_carlo_simulations: Number of MC simulations (default 1000)

        Returns:
            StatisticalValidationResult with complete analysis
        """
        print("\n📊 Starting Statistical Validation...")
        print("=" * 60)

        # 1. Sample Size Analysis
        print("\n1️⃣ Sample Size Test...")
        sample_result = self._test_sample_size(confidence_level)

        # 2. Win Rate Significance Test
        print("2️⃣ Win Rate Significance Test...")
        win_rate_result = self._test_win_rate_significance()

        # 3. Bootstrap Confidence Intervals
        print("3️⃣ Bootstrap Confidence Intervals...")
        bootstrap_result = self._bootstrap_confidence_intervals()

        # 4. Monte Carlo Simulation
        print("4️⃣ Monte Carlo Simulation...")
        monte_carlo_result = self._monte_carlo_simulation(monte_carlo_simulations)

        # 5. Sharpe Ratio Stability
        print("5️⃣ Sharpe Ratio Analysis...")
        sharpe_result = self._analyze_sharpe_stability()

        # Determine overall significance
        significance_score = 0
        if sample_result['adequate']:
            significance_score += 1
        if win_rate_result['significant']:
            significance_score += 1
        if bootstrap_result['ci_95_lower'] > 0:
            significance_score += 1
        if monte_carlo_result['passed']:
            significance_score += 1
        if sharpe_result['stable']:
            significance_score += 1

        is_significant = significance_score >= 3
        edge_likely_real = significance_score >= 4

        if significance_score >= 4:
            confidence = 'High'
        elif significance_score == 3:
            confidence = 'Medium'
        elif significance_score == 2:
            confidence = 'Low'
        else:
            confidence = 'None'

        result = StatisticalValidationResult(
            actual_trades=sample_result['actual'],
            required_trades_95pct=sample_result['required_95'],
            required_trades_99pct=sample_result['required_99'],
            sample_size_adequate=sample_result['adequate'],

            win_rate=win_rate_result['win_rate'],
            win_rate_z_score=win_rate_result['z_score'],
            win_rate_p_value=win_rate_result['p_value'],
            win_rate_significant=win_rate_result['significant'],

            mean_return=bootstrap_result['mean_return'],
            ci_95_lower=bootstrap_result['ci_95_lower'],
            ci_95_upper=bootstrap_result['ci_95_upper'],
            ci_99_lower=bootstrap_result['ci_99_lower'],
            ci_99_upper=bootstrap_result['ci_99_upper'],

            monte_carlo_mean=monte_carlo_result['mean'],
            monte_carlo_std=monte_carlo_result['std'],
            profitable_simulations_pct=monte_carlo_result['profitable_pct'],
            monte_carlo_passed=monte_carlo_result['passed'],

            sharpe_ratio=sharpe_result['sharpe'],
            sharpe_stable=sharpe_result['stable'],

            is_statistically_significant=is_significant,
            confidence_level=confidence,
            edge_likely_real=edge_likely_real
        )

        self._print_results(result)

        return result

    def _test_sample_size(self, confidence_level: float) -> Dict:
        """📏 Test if sample size is adequate"""

        actual_trades = len(self.trades_data)

        # Calculate standard error
        returns = self.trades_data['ReturnPct'].values
        std_return = np.std(returns, ddof=1) if len(returns) > 1 else 0

        # Z-score for confidence levels
        z_95 = 1.96  # 95% confidence
        z_99 = 2.576  # 99% confidence

        # Margin of error = 10% of mean (or 1% if mean near zero)
        mean_return = np.mean(returns)
        margin_of_error = max(abs(mean_return) * 0.10, 1.0)

        # Required sample size formula: n = (z * σ / E)²
        if std_return > 0:
            required_95 = int((z_95 * std_return / margin_of_error) ** 2)
            required_99 = int((z_99 * std_return / margin_of_error) ** 2)
        else:
            required_95 = 30  # Minimum conventional sample size
            required_99 = 50

        adequate = actual_trades >= required_95

        print(f"   Actual trades: {actual_trades}")
        print(f"   Required (95% confidence): {required_95}")
        print(f"   Required (99% confidence): {required_99}")
        print(f"   Status: {'✅ ADEQUATE' if adequate else '❌ INSUFFICIENT'}")

        return {
            'actual': actual_trades,
            'required_95': required_95,
            'required_99': required_99,
            'adequate': adequate
        }

    def _test_win_rate_significance(self) -> Dict:
        """🎯 Test if win rate is significantly different from 50% (coin flip)"""

        wins = len(self.trades_data[self.trades_data['ReturnPct'] > 0])
        total = len(self.trades_data)

        win_rate = (wins / total * 100) if total > 0 else 0

        # Binomial test: H0: win_rate = 50%, H1: win_rate ≠ 50%
        # Use normal approximation for large samples
        if total >= 30:
            # Z-test for proportion
            p0 = 0.5  # Null hypothesis
            p_hat = wins / total

            # Standard error under null hypothesis
            se = np.sqrt(p0 * (1 - p0) / total)

            # Z-score
            z_score = (p_hat - p0) / se

            # Two-tailed p-value
            p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))

            significant = p_value < 0.05

        else:
            # Exact binomial test for small samples
            p_value = stats.binom_test(wins, total, 0.5, alternative='two-sided')
            z_score = (win_rate - 50) / (50 / np.sqrt(total))  # Approximate
            significant = p_value < 0.05

        print(f"   Win rate: {win_rate:.1f}%")
        print(f"   Z-score: {z_score:.2f}")
        print(f"   P-value: {p_value:.4f}")
        print(f"   Status: {'✅ SIGNIFICANT' if significant else '❌ NOT SIGNIFICANT (could be luck)'}")

        return {
            'win_rate': win_rate,
            'z_score': z_score,
            'p_value': p_value,
            'significant': significant
        }

    def _bootstrap_confidence_intervals(self, n_bootstrap: int = 10000) -> Dict:
        """🔄 Calculate confidence intervals using bootstrap resampling"""

        returns = self.trades_data['ReturnPct'].values
        n = len(returns)

        bootstrap_means = []

        # Bootstrap resampling
        for _ in range(n_bootstrap):
            sample = np.random.choice(returns, size=n, replace=True)
            bootstrap_means.append(np.mean(sample))

        bootstrap_means = np.array(bootstrap_means)

        mean_return = np.mean(returns)
        ci_95_lower = np.percentile(bootstrap_means, 2.5)
        ci_95_upper = np.percentile(bootstrap_means, 97.5)
        ci_99_lower = np.percentile(bootstrap_means, 0.5)
        ci_99_upper = np.percentile(bootstrap_means, 99.5)

        print(f"   Mean return: {mean_return:.2f}%")
        print(f"   95% CI: [{ci_95_lower:.2f}%, {ci_95_upper:.2f}%]")
        print(f"   99% CI: [{ci_99_lower:.2f}%, {ci_99_upper:.2f}%]")
        print(f"   Status: {'✅ POSITIVE EDGE' if ci_95_lower > 0 else '⚠️ UNCERTAIN EDGE'}")

        return {
            'mean_return': mean_return,
            'ci_95_lower': ci_95_lower,
            'ci_95_upper': ci_95_upper,
            'ci_99_lower': ci_99_lower,
            'ci_99_upper': ci_99_upper
        }

    def _monte_carlo_simulation(self, n_simulations: int) -> Dict:
        """🎲 Run Monte Carlo simulations by randomizing trade order"""

        returns = self.trades_data['ReturnPct'].values
        n_trades = len(returns)

        simulation_returns = []

        for _ in range(n_simulations):
            # Shuffle trade order
            shuffled = np.random.permutation(returns)

            # Calculate cumulative return
            cum_return = np.sum(shuffled)

            simulation_returns.append(cum_return)

        simulation_returns = np.array(simulation_returns)

        mean_mc = np.mean(simulation_returns)
        std_mc = np.std(simulation_returns)

        # How many simulations were profitable?
        profitable = np.sum(simulation_returns > 0)
        profitable_pct = (profitable / n_simulations * 100)

        # Pass if >70% of simulations profitable
        passed = profitable_pct > 70

        print(f"   Simulations: {n_simulations}")
        print(f"   Mean return: {mean_mc:.2f}%")
        print(f"   Std dev: {std_mc:.2f}%")
        print(f"   Profitable: {profitable_pct:.1f}%")
        print(f"   Status: {'✅ ROBUST' if passed else '⚠️ UNSTABLE (order-dependent)'}")

        return {
            'mean': mean_mc,
            'std': std_mc,
            'profitable_pct': profitable_pct,
            'passed': passed
        }

    def _analyze_sharpe_stability(self) -> Dict:
        """📈 Analyze Sharpe ratio stability"""

        returns = self.trades_data['ReturnPct'].values

        if len(returns) < 10:
            return {'sharpe': 0.0, 'stable': False}

        mean_return = np.mean(returns)
        std_return = np.std(returns, ddof=1) if len(returns) > 1 else 0

        sharpe = (mean_return / std_return) if std_return > 0 else 0

        # Test stability using rolling Sharpe
        window_size = min(20, len(returns) // 2)
        if window_size >= 5:
            rolling_sharpes = []
            for i in range(len(returns) - window_size + 1):
                window = returns[i:i+window_size]
                window_mean = np.mean(window)
                window_std = np.std(window, ddof=1)
                window_sharpe = window_mean / window_std if window_std > 0 else 0
                rolling_sharpes.append(window_sharpe)

            # Sharpe is stable if standard deviation of rolling Sharpes is low
            sharpe_std = np.std(rolling_sharpes) if rolling_sharpes else 999

            stable = sharpe_std < 0.5 and sharpe > 1.0

        else:
            stable = sharpe > 1.5  # For small samples, require high Sharpe

        print(f"   Sharpe Ratio: {sharpe:.2f}")
        print(f"   Status: {'✅ STABLE' if stable else '⚠️ UNSTABLE'}")

        return {
            'sharpe': sharpe,
            'stable': stable
        }

    def _print_results(self, result: StatisticalValidationResult):
        """📊 Print formatted validation results"""
        print("\n" + "=" * 60)
        print("📊 STATISTICAL VALIDATION RESULTS")
        print("=" * 60)

        print(f"\n🎯 OVERALL VERDICT:")
        print(f"   Confidence Level: {result.confidence_level}")
        print(f"   Statistically Significant: {'✅ YES' if result.is_statistically_significant else '❌ NO'}")
        print(f"   Edge Likely Real: {'✅ YES' if result.edge_likely_real else '❌ NO (could be luck)'}")

        print(f"\n📊 TEST RESULTS SUMMARY:")

        print(f"   1. Sample Size: {'✅ PASS' if result.sample_size_adequate else '❌ FAIL'} ({result.actual_trades}/{result.required_trades_95pct} trades)")
        print(f"   2. Win Rate Significance: {'✅ PASS' if result.win_rate_significant else '❌ FAIL'} ({result.win_rate:.1f}%, p={result.win_rate_p_value:.4f})")
        print(f"   3. Bootstrap CI Positive: {'✅ PASS' if result.ci_95_lower > 0 else '❌ FAIL'} (95% CI: [{result.ci_95_lower:.2f}%, {result.ci_95_upper:.2f}%])")
        print(f"   4. Monte Carlo Robust: {'✅ PASS' if result.monte_carlo_passed else '❌ FAIL'} ({result.profitable_simulations_pct:.1f}% profitable)")
        print(f"   5. Sharpe Stable: {'✅ PASS' if result.sharpe_stable else '❌ FAIL'} (Sharpe: {result.sharpe_ratio:.2f})")

        if not result.is_statistically_significant:
            print(f"\n⚠️ WARNING: Results are NOT statistically significant!")
            print(f"   Possible reasons:")
            if not result.sample_size_adequate:
                print(f"   • Too few trades ({result.actual_trades} < {result.required_trades_95pct})")
            if not result.win_rate_significant:
                print(f"   • Win rate not better than coin flip (p={result.win_rate_p_value:.4f})")
            if result.ci_95_lower <= 0:
                print(f"   • Confidence interval includes negative returns")
            if not result.monte_carlo_passed:
                print(f"   • Results unstable when trade order randomized")
            print(f"\n   Recommendation: DO NOT DEPLOY (likely luck, not edge)")

        elif result.confidence_level == 'Medium':
            print(f"\n⚠️ CAUTION: Marginal statistical significance")
            print(f"   Recommendation: Collect more data or optimize strategy")

        else:
            print(f"\n✅ CONFIDENCE: Results appear statistically sound")
            print(f"   Recommendation: Proceed to further validation (paper trading)")

        print("\n" + "=" * 60)


def run_statistical_validation(strategy_name: str,
                               trades_csv_path: str,
                               output_dir: str = 'strategies/diagnostics/results') -> StatisticalValidationResult:
    """
    📊 Run statistical validation on strategy backtest results

    Args:
        strategy_name: Name of strategy being analyzed
        trades_csv_path: Path to trades CSV from backtest
        output_dir: Directory to save results

    Returns:
        StatisticalValidationResult object
    """
    import os

    print(f"\n📊 Statistical Validation: {strategy_name}")
    print("=" * 70)

    # Load data
    print(f"📊 Loading trades data: {trades_csv_path}")
    trades_data = pd.read_csv(trades_csv_path)

    # Run validation
    validator = StatisticalValidator(trades_data)
    result = validator.validate()

    # Save results
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'{strategy_name}_statistical_validation.csv')

    summary_df = pd.DataFrame([{
        'Strategy': strategy_name,
        'Actual_Trades': result.actual_trades,
        'Required_Trades_95': result.required_trades_95pct,
        'Sample_Size_Adequate': result.sample_size_adequate,
        'Win_Rate': result.win_rate,
        'Win_Rate_Significant': result.win_rate_significant,
        'Mean_Return': result.mean_return,
        'CI_95_Lower': result.ci_95_lower,
        'CI_95_Upper': result.ci_95_upper,
        'Monte_Carlo_Profitable_Pct': result.profitable_simulations_pct,
        'Sharpe_Ratio': result.sharpe_ratio,
        'Sharpe_Stable': result.sharpe_stable,
        'Statistically_Significant': result.is_statistically_significant,
        'Confidence_Level': result.confidence_level,
        'Edge_Likely_Real': result.edge_likely_real,
        'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }])

    summary_df.to_csv(output_path, index=False)
    print(f"\n💾 Results saved to: {output_path}")

    return result


if __name__ == "__main__":
    """🧪 Example usage"""

    print("📊 Statistical Validator - Phase 0 Diagnostic Tool")
    print("=" * 70)
    print("\n📝 Usage:")
    print("   from strategies.diagnostics.statistical_validator import run_statistical_validation")
    print("   result = run_statistical_validation(")
    print("       strategy_name='Breakout_LINK_1d',")
    print("       trades_csv_path='results/breakout_link_trades.csv'")
    print("   )")
    print("\n🌙💫🚀 Ready to validate statistical significance!")
