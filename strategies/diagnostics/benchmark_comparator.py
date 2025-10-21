"""
📈 Benchmark Comparator - Phase 0 Diagnostic Tool
================================================
Compares strategy performance to buy-and-hold benchmark.

🎯 Purpose:
    - Calculate buy-and-hold returns for same period
    - Compare risk-adjusted returns (Sharpe, Sortino)
    - Calculate alpha (excess returns over benchmark)
    - Determine if active trading adds value

💫 Comparison Metrics:
    1. Total Return - Strategy vs Buy-Hold
    2. Risk-Adjusted Returns - Sharpe/Sortino comparison
    3. Drawdown - Max DD comparison
    4. Alpha - Excess returns above benchmark
    5. Beta - Strategy sensitivity to market moves

🌙💫🚀 Bobby's Algo-Fun Project
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


@dataclass
class BenchmarkComparisonResult:
    """Results from benchmark comparison analysis"""

    # Strategy performance
    strategy_return: float
    strategy_sharpe: float
    strategy_sortino: float
    strategy_max_dd: float
    strategy_volatility: float

    # Benchmark (buy-hold) performance
    benchmark_return: float
    benchmark_sharpe: float
    benchmark_sortino: float
    benchmark_max_dd: float
    benchmark_volatility: float

    # Comparison metrics
    alpha: float  # Excess return over benchmark
    beta: float   # Sensitivity to market moves
    information_ratio: float  # Risk-adjusted alpha
    tracking_error: float  # Volatility of excess returns

    # Verdicts
    beats_benchmark: bool
    risk_adjusted_better: bool
    generates_alpha: bool
    worth_active_trading: bool


class BenchmarkComparator:
    """
    📈 Compares strategy performance to passive buy-hold benchmark

    Calculates alpha, beta, and risk-adjusted metrics
    """

    def __init__(self,
                 price_data: pd.DataFrame,
                 trades_data: pd.DataFrame,
                 initial_capital: float = 10000.0):
        """
        Initialize comparator

        Args:
            price_data: OHLCV DataFrame with datetime index
            trades_data: Trades DataFrame from backtesting.py
            initial_capital: Starting capital
        """
        self.price_data = price_data.copy()
        self.trades_data = trades_data.copy()
        self.initial_capital = initial_capital

        # Extract strategy equity curve
        self._build_strategy_equity_curve()

        # Calculate buy-hold equity curve
        self._build_buyhold_equity_curve()

    def _build_strategy_equity_curve(self):
        """📊 Build equity curve from strategy trades"""

        # Get trade times
        if 'EntryTime' not in self.trades_data.columns:
            print("⚠️ No trade times available - using trade sequence")
            self.strategy_equity = pd.Series([self.initial_capital], index=[self.price_data.index[0]])
            return

        # Sort trades by time
        trades_sorted = self.trades_data.sort_values('EntryTime')

        equity_curve = []
        timestamps = []
        current_equity = self.initial_capital

        for idx, trade in trades_sorted.iterrows():
            # Record equity at trade exit
            exit_time = trade['ExitTime'] if 'ExitTime' in trade else trade['EntryTime']

            if 'PnL' in trade:
                current_equity += trade['PnL']
            elif 'ReturnPct' in trade:
                current_equity *= (1 + trade['ReturnPct'] / 100)
            elif 'Return_%' in trade:
                current_equity *= (1 + trade['Return_%'] / 100)

            timestamps.append(exit_time)
            equity_curve.append(current_equity)

        self.strategy_equity = pd.Series(equity_curve, index=timestamps)

        # Fill forward to match price data timeline
        self.strategy_equity = self.strategy_equity.reindex(
            self.price_data.index,
            method='ffill'
        ).fillna(self.initial_capital)

    def _build_buyhold_equity_curve(self):
        """📈 Calculate buy-and-hold equity curve"""

        # Use close prices
        if 'Close' not in self.price_data.columns:
            print("❌ No close price data available")
            self.buyhold_equity = pd.Series([self.initial_capital] * len(self.price_data),
                                           index=self.price_data.index)
            return

        prices = self.price_data['Close']

        # Calculate returns
        start_price = prices.iloc[0]
        equity_curve = (prices / start_price) * self.initial_capital

        self.buyhold_equity = equity_curve

    def compare(self) -> BenchmarkComparisonResult:
        """
        📈 Perform comprehensive benchmark comparison

        Returns:
            BenchmarkComparisonResult with complete analysis
        """
        print("\n📈 Starting Benchmark Comparison...")
        print("=" * 60)

        # Strategy performance
        strategy_result = self._calculate_performance_metrics(self.strategy_equity, "Strategy")

        # Benchmark performance
        benchmark_result = self._calculate_performance_metrics(self.buyhold_equity, "Buy-Hold")

        # Calculate alpha and beta
        print("\n🎯 Calculating Alpha & Beta...")

        # Strategy daily returns
        strategy_returns = self.strategy_equity.pct_change().dropna()

        # Benchmark daily returns
        benchmark_returns = self.buyhold_equity.pct_change().dropna()

        # Align the two series
        aligned = pd.DataFrame({
            'strategy': strategy_returns,
            'benchmark': benchmark_returns
        }).dropna()

        if len(aligned) > 10:
            # Beta: Covariance(strategy, benchmark) / Variance(benchmark)
            cov = aligned['strategy'].cov(aligned['benchmark'])
            var = aligned['benchmark'].var()
            beta = cov / var if var > 0 else 1.0

            # Alpha: Strategy return - (risk_free_rate + beta × (benchmark_return - risk_free_rate))
            # Assuming risk_free_rate = 0 for simplicity
            alpha = strategy_result['total_return'] - (beta * benchmark_result['total_return'])

            # Tracking error: Std dev of excess returns
            excess_returns = aligned['strategy'] - aligned['benchmark']
            tracking_error = excess_returns.std() * np.sqrt(252)  # Annualized

            # Information ratio: Alpha / Tracking Error
            information_ratio = alpha / tracking_error if tracking_error > 0 else 0

        else:
            beta = 1.0
            alpha = strategy_result['total_return'] - benchmark_result['total_return']
            tracking_error = 0.0
            information_ratio = 0.0

        print(f"   Alpha: {alpha:.2f}%")
        print(f"   Beta: {beta:.2f}")
        print(f"   Information Ratio: {information_ratio:.2f}")
        print(f"   Tracking Error: {tracking_error:.2f}%")

        # Determine verdicts
        beats_benchmark = strategy_result['total_return'] > benchmark_result['total_return']
        risk_adjusted_better = strategy_result['sharpe'] > benchmark_result['sharpe']
        generates_alpha = alpha > 0
        worth_active_trading = beats_benchmark and risk_adjusted_better and alpha > 5.0  # Need >5% alpha to justify effort

        result = BenchmarkComparisonResult(
            strategy_return=strategy_result['total_return'],
            strategy_sharpe=strategy_result['sharpe'],
            strategy_sortino=strategy_result['sortino'],
            strategy_max_dd=strategy_result['max_dd'],
            strategy_volatility=strategy_result['volatility'],

            benchmark_return=benchmark_result['total_return'],
            benchmark_sharpe=benchmark_result['sharpe'],
            benchmark_sortino=benchmark_result['sortino'],
            benchmark_max_dd=benchmark_result['max_dd'],
            benchmark_volatility=benchmark_result['volatility'],

            alpha=alpha,
            beta=beta,
            information_ratio=information_ratio,
            tracking_error=tracking_error,

            beats_benchmark=beats_benchmark,
            risk_adjusted_better=risk_adjusted_better,
            generates_alpha=generates_alpha,
            worth_active_trading=worth_active_trading
        )

        self._print_results(result)

        return result

    def _calculate_performance_metrics(self, equity_curve: pd.Series, label: str) -> Dict:
        """📊 Calculate performance metrics for an equity curve"""

        print(f"\n📊 {label} Performance:")

        # Total return
        start_equity = equity_curve.iloc[0]
        end_equity = equity_curve.iloc[-1]
        total_return = ((end_equity - start_equity) / start_equity) * 100

        print(f"   Total Return: {total_return:.2f}%")

        # Calculate daily returns
        returns = equity_curve.pct_change().dropna()

        # Volatility (annualized)
        volatility = returns.std() * np.sqrt(252) * 100  # Annualized %

        # Sharpe Ratio (assuming 0% risk-free rate)
        mean_return = returns.mean() * 252  # Annualized
        sharpe = mean_return / (returns.std() * np.sqrt(252)) if returns.std() > 0 else 0

        # Sortino Ratio (downside deviation)
        downside_returns = returns[returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0.01
        sortino = mean_return / downside_std if downside_std > 0 else 0

        # Maximum Drawdown
        cummax = equity_curve.cummax()
        drawdown = (equity_curve - cummax) / cummax * 100
        max_dd = drawdown.min()

        print(f"   Sharpe Ratio: {sharpe:.2f}")
        print(f"   Sortino Ratio: {sortino:.2f}")
        print(f"   Max Drawdown: {max_dd:.2f}%")
        print(f"   Volatility: {volatility:.2f}%")

        return {
            'total_return': total_return,
            'sharpe': sharpe,
            'sortino': sortino,
            'max_dd': max_dd,
            'volatility': volatility
        }

    def _print_results(self, result: BenchmarkComparisonResult):
        """📊 Print formatted comparison results"""
        print("\n" + "=" * 60)
        print("📈 BENCHMARK COMPARISON RESULTS")
        print("=" * 60)

        print(f"\n📊 PERFORMANCE COMPARISON:")
        print(f"                    {'Strategy':>12}  {'Buy-Hold':>12}  {'Winner':>12}")
        print(f"   Total Return:    {result.strategy_return:>11.2f}% {result.benchmark_return:>11.2f}%  {'Strategy' if result.strategy_return > result.benchmark_return else 'Buy-Hold':>12}")
        print(f"   Sharpe Ratio:    {result.strategy_sharpe:>12.2f} {result.benchmark_sharpe:>12.2f}  {'Strategy' if result.strategy_sharpe > result.benchmark_sharpe else 'Buy-Hold':>12}")
        print(f"   Sortino Ratio:   {result.strategy_sortino:>12.2f} {result.benchmark_sortino:>12.2f}  {'Strategy' if result.strategy_sortino > result.benchmark_sortino else 'Buy-Hold':>12}")
        print(f"   Max Drawdown:    {result.strategy_max_dd:>11.2f}% {result.benchmark_max_dd:>11.2f}%  {'Strategy' if result.strategy_max_dd > result.benchmark_max_dd else 'Buy-Hold':>12}")

        print(f"\n🎯 ALPHA & BETA ANALYSIS:")
        print(f"   Alpha: {result.alpha:+.2f}%")
        if result.alpha > 5:
            print(f"   ✅ Strong positive alpha - strategy adds significant value")
        elif result.alpha > 0:
            print(f"   ⚠️ Small positive alpha - marginal improvement over buy-hold")
        else:
            print(f"   ❌ Negative alpha - strategy underperforms buy-hold")

        print(f"   Beta: {result.beta:.2f}")
        if abs(result.beta - 1.0) < 0.2:
            print(f"   📊 Beta near 1.0 - strategy tracks market closely")
        elif result.beta > 1.2:
            print(f"   ⚠️ High beta - strategy amplifies market moves (higher risk)")
        else:
            print(f"   🛡️ Low beta - strategy less sensitive to market (defensive)")

        print(f"   Information Ratio: {result.information_ratio:.2f}")
        if result.information_ratio > 0.5:
            print(f"   ✅ Good information ratio - efficient alpha generation")
        else:
            print(f"   ⚠️ Low information ratio - alpha not worth the tracking error")

        print(f"\n⚖️ FINAL VERDICT:")

        if result.worth_active_trading:
            print(f"   ✅ ACTIVE TRADING JUSTIFIED")
            print(f"      Strategy beats buy-hold with strong risk-adjusted returns")
            print(f"      Alpha: {result.alpha:+.2f}% justifies trading costs and effort")
        elif result.beats_benchmark and result.generates_alpha:
            print(f"   ⚠️ MARGINAL IMPROVEMENT")
            print(f"      Strategy beats buy-hold but alpha is small")
            print(f"      Consider if {result.alpha:.2f}% alpha worth trading effort")
        else:
            print(f"   ❌ BUY-HOLD SUPERIOR")
            print(f"      Strategy underperforms passive investing")
            print(f"      Recommendation: Just buy and hold the asset")

        print("=" * 60)


def run_benchmark_comparison(strategy_name: str,
                            price_data_path: str,
                            trades_csv_path: str,
                            initial_capital: float = 10000.0,
                            output_dir: str = 'strategies/diagnostics/results') -> BenchmarkComparisonResult:
    """
    📈 Run benchmark comparison on strategy backtest results

    Args:
        strategy_name: Name of strategy being analyzed
        price_data_path: Path to OHLCV price data CSV
        trades_csv_path: Path to trades CSV from backtest
        initial_capital: Starting capital
        output_dir: Directory to save results

    Returns:
        BenchmarkComparisonResult object
    """
    import os

    print(f"\n📈 Benchmark Comparison: {strategy_name}")
    print("=" * 70)

    # Load data
    print(f"📊 Loading price data: {price_data_path}")
    price_data = pd.read_csv(price_data_path)

    # Handle different date column names
    date_cols = [col for col in price_data.columns if 'date' in col.lower() or 'time' in col.lower()]
    if date_cols:
        price_data[date_cols[0]] = pd.to_datetime(price_data[date_cols[0]])
        price_data.set_index(date_cols[0], inplace=True)

    # 🔧 Standardize OHLCV column names to Titlecase (fixes lowercase data files)
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        if col not in price_data.columns:
            # Try case-insensitive match
            matching = [c for c in price_data.columns if c.lower() == col.lower()]
            if matching:
                price_data[col] = price_data[matching[0]]
            else:
                print(f"⚠️ Warning: Missing column {col} in price data")

    print(f"📊 Loading trades data: {trades_csv_path}")
    trades_data = pd.read_csv(trades_csv_path)

    # Convert times to datetime
    if 'EntryTime' in trades_data.columns:
        trades_data['EntryTime'] = pd.to_datetime(trades_data['EntryTime'])
    if 'ExitTime' in trades_data.columns:
        trades_data['ExitTime'] = pd.to_datetime(trades_data['ExitTime'])

    # Run comparison
    comparator = BenchmarkComparator(price_data, trades_data, initial_capital)
    result = comparator.compare()

    # Save results
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'{strategy_name}_benchmark_comparison.csv')

    summary_df = pd.DataFrame([{
        'Strategy': strategy_name,
        'Strategy_Return': result.strategy_return,
        'Benchmark_Return': result.benchmark_return,
        'Alpha': result.alpha,
        'Beta': result.beta,
        'Strategy_Sharpe': result.strategy_sharpe,
        'Benchmark_Sharpe': result.benchmark_sharpe,
        'Strategy_Max_DD': result.strategy_max_dd,
        'Benchmark_Max_DD': result.benchmark_max_dd,
        'Information_Ratio': result.information_ratio,
        'Beats_Benchmark': result.beats_benchmark,
        'Generates_Alpha': result.generates_alpha,
        'Worth_Active_Trading': result.worth_active_trading,
        'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }])

    summary_df.to_csv(output_path, index=False)
    print(f"\n💾 Results saved to: {output_path}")

    return result


if __name__ == "__main__":
    """🧪 Example usage"""

    print("📈 Benchmark Comparator - Phase 0 Diagnostic Tool")
    print("=" * 70)
    print("\n📝 Usage:")
    print("   from strategies.diagnostics.benchmark_comparator import run_benchmark_comparison")
    print("   result = run_benchmark_comparison(")
    print("       strategy_name='Breakout_LINK_1d',")
    print("       price_data_path='data/LINKUSD-1d.csv',")
    print("       trades_csv_path='results/breakout_link_trades.csv'")
    print("   )")
    print("\n🌙💫🚀 Ready to compare against buy-hold!")
