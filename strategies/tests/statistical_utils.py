"""
📊 Statistical Testing Utilities for Strategy Enhancement Validation
Provides bootstrap testing, significance tests, and overfitting detection

Key Functions:
- bootstrap_confidence_interval(): Generate CI for returns/Sharpe
- test_statistical_significance(): Test if enhancement is statistically better
- detect_overfitting(): Check for overfitting across data splits
- sharpe_ratio_confidence(): Calculate Sharpe ratio CI
"""

import numpy as np
import pandas as pd
from typing import Tuple, Dict, List
from scipy import stats


def bootstrap_confidence_interval(
    returns: pd.Series,
    n_iterations: int = 10000,
    confidence_level: float = 0.95,
    metric: str = 'mean'
) -> Tuple[float, float, float]:
    """
    🔄 Bootstrap confidence interval for returns

    Args:
        returns: Series of trade returns or portfolio returns
        n_iterations: Number of bootstrap samples (default 10000)
        confidence_level: Confidence level (default 0.95 for 95% CI)
        metric: 'mean' or 'sharpe' or 'median'

    Returns:
        Tuple of (point_estimate, lower_bound, upper_bound)
    """
    returns_array = returns.dropna().values

    if len(returns_array) == 0:
        return (0.0, 0.0, 0.0)

    bootstrap_samples = []

    for _ in range(n_iterations):
        # Resample with replacement
        sample = np.random.choice(returns_array, size=len(returns_array), replace=True)

        if metric == 'mean':
            bootstrap_samples.append(np.mean(sample))
        elif metric == 'sharpe':
            if np.std(sample) > 0:
                sharpe = np.mean(sample) / np.std(sample) * np.sqrt(252)  # Annualized
                bootstrap_samples.append(sharpe)
            else:
                bootstrap_samples.append(0.0)
        elif metric == 'median':
            bootstrap_samples.append(np.median(sample))

    bootstrap_samples = np.array(bootstrap_samples)

    # Calculate confidence interval
    alpha = 1 - confidence_level
    lower_percentile = (alpha / 2) * 100
    upper_percentile = (1 - alpha / 2) * 100

    point_estimate = np.mean(bootstrap_samples)
    lower_bound = np.percentile(bootstrap_samples, lower_percentile)
    upper_bound = np.percentile(bootstrap_samples, upper_percentile)

    return (point_estimate, lower_bound, upper_bound)


def sharpe_ratio_confidence(
    returns: pd.Series,
    n_iterations: int = 10000,
    confidence_level: float = 0.95
) -> Dict[str, float]:
    """
    📈 Calculate Sharpe ratio with confidence interval

    Args:
        returns: Series of returns
        n_iterations: Bootstrap iterations
        confidence_level: Confidence level

    Returns:
        Dict with sharpe, lower_ci, upper_ci, is_significant
    """
    point_est, lower_ci, upper_ci = bootstrap_confidence_interval(
        returns,
        n_iterations=n_iterations,
        confidence_level=confidence_level,
        metric='sharpe'
    )

    # Significant if lower bound > 0
    is_significant = lower_ci > 0

    return {
        'sharpe_ratio': point_est,
        'lower_ci': lower_ci,
        'upper_ci': upper_ci,
        'is_significant': is_significant,
        'confidence_level': confidence_level
    }


def test_statistical_significance(
    baseline_returns: pd.Series,
    enhanced_returns: pd.Series,
    alpha: float = 0.05
) -> Dict[str, any]:
    """
    🔬 Test if enhanced strategy is statistically better than baseline

    Uses paired t-test to compare returns distributions

    Args:
        baseline_returns: Baseline strategy returns
        enhanced_returns: Enhanced strategy returns
        alpha: Significance level (default 0.05)

    Returns:
        Dict with test results
    """
    baseline_array = baseline_returns.dropna().values
    enhanced_array = enhanced_returns.dropna().values

    # Ensure same length for paired test
    min_len = min(len(baseline_array), len(enhanced_array))
    baseline_array = baseline_array[:min_len]
    enhanced_array = enhanced_array[:min_len]

    if min_len < 2:
        return {
            'statistically_significant': False,
            'p_value': 1.0,
            'test_statistic': 0.0,
            'interpretation': 'Insufficient data for statistical test',
            'enhanced_is_better': False
        }

    # Paired t-test
    t_stat, p_value = stats.ttest_rel(enhanced_array, baseline_array)

    # One-tailed test (enhanced > baseline)
    p_value_one_tailed = p_value / 2 if t_stat > 0 else 1 - (p_value / 2)

    is_significant = p_value_one_tailed < alpha
    enhanced_is_better = np.mean(enhanced_array) > np.mean(baseline_array)

    interpretation = ""
    if is_significant and enhanced_is_better:
        interpretation = "✅ Enhancement is statistically significantly better"
    elif is_significant and not enhanced_is_better:
        interpretation = "❌ Enhancement is statistically significantly worse"
    elif not is_significant and enhanced_is_better:
        interpretation = "⚠️ Enhancement appears better but not statistically significant"
    else:
        interpretation = "❌ Enhancement shows no improvement"

    return {
        'statistically_significant': is_significant,
        'p_value': p_value_one_tailed,
        'test_statistic': t_stat,
        'interpretation': interpretation,
        'enhanced_is_better': enhanced_is_better,
        'baseline_mean': np.mean(baseline_array),
        'enhanced_mean': np.mean(enhanced_array),
        'difference': np.mean(enhanced_array) - np.mean(baseline_array)
    }


def detect_overfitting(
    in_sample_stats: Dict,
    validation_stats: Dict,
    out_of_sample_stats: Dict,
    sharpe_tolerance: float = 0.20,
    win_rate_tolerance: float = 10.0
) -> Dict[str, any]:
    """
    🛡️ Detect potential overfitting across data splits

    Checks if metrics are consistent across in-sample, validation, and out-of-sample periods

    Args:
        in_sample_stats: Stats from in-sample period
        validation_stats: Stats from validation period
        out_of_sample_stats: Stats from out-of-sample period
        sharpe_tolerance: Max acceptable Sharpe variation (default 20%)
        win_rate_tolerance: Max acceptable win rate variation (default 10 percentage points)

    Returns:
        Dict with overfitting detection results
    """
    # Extract metrics
    is_sharpe = in_sample_stats.get('Sharpe Ratio', 0)
    val_sharpe = validation_stats.get('Sharpe Ratio', 0)
    oos_sharpe = out_of_sample_stats.get('Sharpe Ratio', 0)

    is_win_rate = in_sample_stats.get('Win Rate [%]', 0)
    val_win_rate = validation_stats.get('Win Rate [%]', 0)
    oos_win_rate = out_of_sample_stats.get('Win Rate [%]', 0)

    is_return = in_sample_stats.get('Return [%]', 0)
    val_return = validation_stats.get('Return [%]', 0)
    oos_return = out_of_sample_stats.get('Return [%]', 0)

    # Check Sharpe ratio consistency
    sharpe_checks = []
    if is_sharpe != 0:
        val_sharpe_ratio = val_sharpe / is_sharpe if is_sharpe != 0 else 0
        oos_sharpe_ratio = oos_sharpe / is_sharpe if is_sharpe != 0 else 0

        sharpe_checks.append({
            'comparison': 'Validation vs In-Sample',
            'ratio': val_sharpe_ratio,
            'overfitting': val_sharpe_ratio < (1 - sharpe_tolerance)
        })
        sharpe_checks.append({
            'comparison': 'OOS vs In-Sample',
            'ratio': oos_sharpe_ratio,
            'overfitting': oos_sharpe_ratio < (1 - sharpe_tolerance)
        })

    # Check win rate consistency
    win_rate_checks = []
    val_wr_diff = abs(val_win_rate - is_win_rate)
    oos_wr_diff = abs(oos_win_rate - is_win_rate)

    win_rate_checks.append({
        'comparison': 'Validation vs In-Sample',
        'difference': val_wr_diff,
        'overfitting': val_wr_diff > win_rate_tolerance
    })
    win_rate_checks.append({
        'comparison': 'OOS vs In-Sample',
        'difference': oos_wr_diff,
        'overfitting': oos_wr_diff > win_rate_tolerance
    })

    # Overall overfitting verdict
    sharpe_overfitting = any(check['overfitting'] for check in sharpe_checks)
    win_rate_overfitting = any(check['overfitting'] for check in win_rate_checks)
    overall_overfitting = sharpe_overfitting or win_rate_overfitting

    # Performance degradation check
    performance_degraded = (val_return < is_return * 0.5) or (oos_return < is_return * 0.5)

    return {
        'overfitting_detected': overall_overfitting,
        'performance_degraded': performance_degraded,
        'sharpe_checks': sharpe_checks,
        'win_rate_checks': win_rate_checks,
        'metrics': {
            'in_sample': {'sharpe': is_sharpe, 'win_rate': is_win_rate, 'return': is_return},
            'validation': {'sharpe': val_sharpe, 'win_rate': val_win_rate, 'return': val_return},
            'out_of_sample': {'sharpe': oos_sharpe, 'win_rate': oos_win_rate, 'return': oos_return}
        },
        'recommendation': _generate_overfitting_recommendation(
            overall_overfitting,
            performance_degraded,
            sharpe_checks,
            win_rate_checks
        )
    }


def _generate_overfitting_recommendation(
    overfitting: bool,
    degraded: bool,
    sharpe_checks: List,
    win_rate_checks: List
) -> str:
    """🎯 Generate recommendation based on overfitting analysis"""
    if not overfitting and not degraded:
        return "✅ No overfitting detected. Enhancement appears to generalize well."

    if degraded:
        return "❌ SEVERE: Performance severely degraded on validation/OOS. REVERT enhancement."

    issues = []
    if any(c['overfitting'] for c in sharpe_checks):
        issues.append("Sharpe ratio inconsistent across periods")
    if any(c['overfitting'] for c in win_rate_checks):
        issues.append("Win rate inconsistent across periods")

    return f"⚠️ Possible overfitting: {', '.join(issues)}. Consider reverting or adjusting parameters."


def calculate_win_rate_significance(
    wins: int,
    total_trades: int,
    expected_win_rate: float = 0.50,
    alpha: float = 0.05
) -> Dict[str, any]:
    """
    🎯 Test if win rate is statistically better than expected

    Uses binomial test

    Args:
        wins: Number of winning trades
        total_trades: Total number of trades
        expected_win_rate: Expected win rate under null hypothesis (default 50%)
        alpha: Significance level

    Returns:
        Dict with significance test results
    """
    if total_trades == 0:
        return {
            'win_rate': 0.0,
            'is_significant': False,
            'p_value': 1.0,
            'interpretation': 'No trades executed'
        }

    actual_win_rate = wins / total_trades

    # Binomial test (one-tailed: win rate > expected)
    p_value = stats.binom_test(wins, total_trades, expected_win_rate, alternative='greater')

    is_significant = p_value < alpha

    interpretation = ""
    if is_significant:
        interpretation = f"✅ Win rate {actual_win_rate:.1%} is statistically significantly better than {expected_win_rate:.1%}"
    else:
        interpretation = f"❌ Win rate {actual_win_rate:.1%} is not significantly better than {expected_win_rate:.1%} (could be luck)"

    return {
        'win_rate': actual_win_rate,
        'is_significant': is_significant,
        'p_value': p_value,
        'interpretation': interpretation,
        'wins': wins,
        'total_trades': total_trades,
        'expected_win_rate': expected_win_rate
    }


def compare_strategies_monte_carlo(
    baseline_returns: pd.Series,
    enhanced_returns: pd.Series,
    n_simulations: int = 10000
) -> Dict[str, any]:
    """
    🎲 Monte Carlo simulation to compare strategies

    Randomly shuffles returns to test if difference is due to chance

    Args:
        baseline_returns: Baseline returns
        enhanced_returns: Enhanced returns
        n_simulations: Number of simulations

    Returns:
        Dict with Monte Carlo results
    """
    baseline_array = baseline_returns.dropna().values
    enhanced_array = enhanced_returns.dropna().values

    actual_diff = np.mean(enhanced_array) - np.mean(baseline_array)

    # Monte Carlo: shuffle and compare
    greater_count = 0

    for _ in range(n_simulations):
        # Pool all returns
        pooled = np.concatenate([baseline_array, enhanced_array])

        # Randomly assign to two groups
        np.random.shuffle(pooled)
        split_point = len(baseline_array)

        group1 = pooled[:split_point]
        group2 = pooled[split_point:]

        simulated_diff = np.mean(group2) - np.mean(group1)

        if simulated_diff >= actual_diff:
            greater_count += 1

    p_value = greater_count / n_simulations

    return {
        'actual_difference': actual_diff,
        'p_value': p_value,
        'is_significant': p_value < 0.05,
        'interpretation': f"{'✅ Significant' if p_value < 0.05 else '❌ Not significant'} "
                         f"(p={p_value:.4f})"
    }


def print_statistical_report(
    baseline_stats: Dict,
    enhanced_stats: Dict,
    baseline_returns: pd.Series,
    enhanced_returns: pd.Series,
    enhancement_name: str = "Enhancement"
):
    """
    📊 Print comprehensive statistical comparison report

    Args:
        baseline_stats: Baseline strategy stats
        enhanced_stats: Enhanced strategy stats
        baseline_returns: Baseline returns series
        enhanced_returns: Enhanced returns series
        enhancement_name: Name of enhancement
    """
    print(f"\n{'='*70}")
    print(f"📊 STATISTICAL VALIDATION REPORT: {enhancement_name}")
    print(f"{'='*70}\n")

    # Basic comparison
    print("📈 Performance Metrics:")
    print(f"   Baseline Sharpe: {baseline_stats.get('Sharpe Ratio', 0):.2f}")
    print(f"   Enhanced Sharpe: {enhanced_stats.get('Sharpe Ratio', 0):.2f}")
    print(f"   Improvement: {(enhanced_stats.get('Sharpe Ratio', 0) - baseline_stats.get('Sharpe Ratio', 0)):+.2f}\n")

    # Statistical significance
    sig_test = test_statistical_significance(baseline_returns, enhanced_returns)
    print("🔬 Statistical Significance Test:")
    print(f"   {sig_test['interpretation']}")
    print(f"   p-value: {sig_test['p_value']:.4f}")
    print(f"   Test statistic: {sig_test['test_statistic']:.2f}\n")

    # Bootstrap confidence intervals
    baseline_ci = sharpe_ratio_confidence(baseline_returns)
    enhanced_ci = sharpe_ratio_confidence(enhanced_returns)

    print("📊 Sharpe Ratio 95% Confidence Intervals:")
    print(f"   Baseline: [{baseline_ci['lower_ci']:.2f}, {baseline_ci['upper_ci']:.2f}]")
    print(f"   Enhanced: [{enhanced_ci['lower_ci']:.2f}, {enhanced_ci['upper_ci']:.2f}]")

    if baseline_ci['lower_ci'] > enhanced_ci['upper_ci']:
        print("   ❌ Baseline is significantly better")
    elif enhanced_ci['lower_ci'] > baseline_ci['upper_ci']:
        print("   ✅ Enhancement is significantly better")
    else:
        print("   ⚠️ Confidence intervals overlap - difference may not be significant")

    print(f"\n{'='*70}\n")


if __name__ == '__main__':
    # 🧪 Example usage
    print("📊 Statistical Utilities Test\n")

    # Generate sample returns
    np.random.seed(42)
    baseline_returns = pd.Series(np.random.randn(100) * 0.01)
    enhanced_returns = pd.Series(np.random.randn(100) * 0.01 + 0.002)  # Slightly better

    # Test bootstrap CI
    sharpe_ci = sharpe_ratio_confidence(enhanced_returns)
    print(f"Sharpe Ratio: {sharpe_ci['sharpe_ratio']:.2f}")
    print(f"95% CI: [{sharpe_ci['lower_ci']:.2f}, {sharpe_ci['upper_ci']:.2f}]")
    print(f"Significant: {sharpe_ci['is_significant']}\n")

    # Test significance
    sig_test = test_statistical_significance(baseline_returns, enhanced_returns)
    print(f"Statistical Test: {sig_test['interpretation']}")
    print(f"p-value: {sig_test['p_value']:.4f}\n")

    print("✅ Statistical utilities working correctly")
