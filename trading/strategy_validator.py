"""
Strategy Validator - Performance Tracking & Validation
=======================================================
Analyzes trading performance and validates strategy effectiveness

Features:
- Performance metrics calculation (win rate, Sharpe ratio, etc.)
- Strategy comparison (RSI vs SMA vs Breakout)
- Weekly/monthly reports
- Validation against 1-2 month goals
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from termcolor import cprint

# Add parent directory to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import configuration
from trading.config_eth_aggressive import (
    TRADES_LOG_PATH,
    REPORTS_PATH,
    STARTING_CAPITAL,
    VALIDATION_GOALS,
)


class StrategyValidator:
    """
    Performance tracking and strategy validation
    """

    def __init__(self):
        """Initialize validator"""
        self.trades_df = self._load_trades()
        cprint("✅ Strategy Validator initialized", "green")

    def _load_trades(self) -> pd.DataFrame:
        """Load trades from CSV log"""
        if not TRADES_LOG_PATH.exists():
            cprint("⚠️  No trades log found", "yellow")
            return pd.DataFrame()

        try:
            df = pd.DataFrame(pd.read_csv(TRADES_LOG_PATH))
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Filter out incomplete trades (no exit price yet)
            df = df[df["exit_price"] > 0]

            return df

        except Exception as e:
            cprint(f"❌ Error loading trades: {e}", "red")
            return pd.DataFrame()

    def refresh_data(self):
        """Reload trades from log"""
        self.trades_df = self._load_trades()

    def calculate_metrics(self) -> Dict:
        """
        Calculate comprehensive performance metrics

        Returns:
            Dict with all performance metrics
        """
        if self.trades_df.empty:
            return {
                "total_trades": 0,
                "total_return_pct": 0,
                "total_pnl_usd": 0,
                "win_rate_pct": 0,
                "avg_win_usd": 0,
                "avg_loss_usd": 0,
                "profit_factor": 0,
                "sharpe_ratio": 0,
                "max_drawdown_pct": 0,
                "avg_return_pct": 0,
            }

        df = self.trades_df

        # Basic metrics
        total_trades = len(df)
        wins = df[df["win"] == True]
        losses = df[df["win"] == False]

        total_pnl = df["pnl_usd"].sum()
        final_balance = STARTING_CAPITAL + total_pnl
        total_return_pct = (total_pnl / STARTING_CAPITAL) * 100

        win_rate_pct = (len(wins) / total_trades * 100) if total_trades > 0 else 0
        avg_win = wins["pnl_usd"].mean() if len(wins) > 0 else 0
        avg_loss = abs(losses["pnl_usd"].mean()) if len(losses) > 0 else 0

        # Profit factor
        total_wins = wins["pnl_usd"].sum() if len(wins) > 0 else 0
        total_losses = abs(losses["pnl_usd"].sum()) if len(losses) > 0 else 0
        profit_factor = (total_wins / total_losses) if total_losses > 0 else 0

        # Sharpe ratio (annualized)
        if len(df) > 1:
            returns = df["pnl_pct"] / 100  # Convert to decimal
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() > 0 else 0
        else:
            sharpe_ratio = 0

        # Max drawdown
        df = df.sort_values("timestamp")
        df["cumulative_pnl"] = df["pnl_usd"].cumsum()
        df["cumulative_balance"] = STARTING_CAPITAL + df["cumulative_pnl"]
        df["peak_balance"] = df["cumulative_balance"].cummax()
        df["drawdown"] = (df["cumulative_balance"] - df["peak_balance"]) / df["peak_balance"] * 100
        max_drawdown_pct = abs(df["drawdown"].min()) if len(df) > 0 else 0

        # Average return per trade
        avg_return_pct = df["pnl_pct"].mean() if len(df) > 0 else 0

        return {
            "total_trades": total_trades,
            "total_return_pct": total_return_pct,
            "total_pnl_usd": total_pnl,
            "final_balance": final_balance,
            "win_rate_pct": win_rate_pct,
            "avg_win_usd": avg_win,
            "avg_loss_usd": avg_loss,
            "profit_factor": profit_factor,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown_pct": max_drawdown_pct,
            "avg_return_pct": avg_return_pct,
            "num_wins": len(wins),
            "num_losses": len(losses),
        }

    def compare_strategies(self) -> Dict:
        """
        Compare performance of different strategies

        Returns:
            Dict with per-strategy metrics
        """
        if self.trades_df.empty:
            return {}

        strategies = self.trades_df["strategy"].unique()
        comparison = {}

        for strategy in strategies:
            strategy_df = self.trades_df[self.trades_df["strategy"] == strategy]

            wins = strategy_df[strategy_df["win"] == True]
            total_trades = len(strategy_df)

            comparison[strategy] = {
                "trades": total_trades,
                "win_rate": (len(wins) / total_trades * 100) if total_trades > 0 else 0,
                "total_pnl": strategy_df["pnl_usd"].sum(),
                "avg_pnl": strategy_df["pnl_usd"].mean(),
                "best_trade": strategy_df["pnl_usd"].max(),
                "worst_trade": strategy_df["pnl_usd"].min(),
                "avg_confidence": strategy_df["confidence"].mean(),
            }

        return comparison

    def weekly_performance(self) -> pd.DataFrame:
        """
        Calculate performance by week

        Returns:
            DataFrame with weekly metrics
        """
        if self.trades_df.empty:
            return pd.DataFrame()

        df = self.trades_df.copy()
        df["week"] = df["timestamp"].dt.to_period("W")

        weekly = df.groupby("week").agg({
            "pnl_usd": ["sum", "count"],
            "win": "sum",
        }).reset_index()

        weekly.columns = ["week", "pnl_usd", "trades", "wins"]
        weekly["win_rate"] = (weekly["wins"] / weekly["trades"] * 100)
        weekly["profitable"] = weekly["pnl_usd"] > 0

        return weekly

    def validate_goals(self) -> Dict:
        """
        Validate performance against 1-2 month goals

        Returns:
            Dict with validation results
        """
        metrics = self.calculate_metrics()
        weekly = self.weekly_performance()

        validation = {
            "goals_met": [],
            "goals_missed": [],
            "overall_validated": False
        }

        # Goal 1: Minimum return
        if metrics["total_return_pct"] >= VALIDATION_GOALS["min_return_pct"]:
            validation["goals_met"].append(
                f"✅ Return: {metrics['total_return_pct']:.1f}% >= {VALIDATION_GOALS['min_return_pct']:.0f}%"
            )
        else:
            validation["goals_missed"].append(
                f"❌ Return: {metrics['total_return_pct']:.1f}% < {VALIDATION_GOALS['min_return_pct']:.0f}%"
            )

        # Goal 2: Minimum win rate
        if metrics["win_rate_pct"] >= VALIDATION_GOALS["min_win_rate_pct"]:
            validation["goals_met"].append(
                f"✅ Win Rate: {metrics['win_rate_pct']:.1f}% >= {VALIDATION_GOALS['min_win_rate_pct']:.0f}%"
            )
        else:
            validation["goals_missed"].append(
                f"❌ Win Rate: {metrics['win_rate_pct']:.1f}% < {VALIDATION_GOALS['min_win_rate_pct']:.0f}%"
            )

        # Goal 3: Minimum profitable weeks
        if not weekly.empty:
            profitable_weeks = weekly["profitable"].sum()
            total_weeks = len(weekly)
            target_weeks = VALIDATION_GOALS["min_profitable_weeks"]

            if profitable_weeks >= target_weeks:
                validation["goals_met"].append(
                    f"✅ Profitable Weeks: {profitable_weeks}/{total_weeks} >= {target_weeks}"
                )
            else:
                validation["goals_missed"].append(
                    f"❌ Profitable Weeks: {profitable_weeks}/{total_weeks} < {target_weeks}"
                )

        # Goal 4: Max single loss
        if not self.trades_df.empty:
            losses = self.trades_df[self.trades_df["win"] == False]
            if len(losses) > 0:
                worst_loss_pct = abs(losses["pnl_pct"].min())
                if worst_loss_pct <= VALIDATION_GOALS["max_single_loss_pct"]:
                    validation["goals_met"].append(
                        f"✅ Max Single Loss: {worst_loss_pct:.1f}% <= {VALIDATION_GOALS['max_single_loss_pct']:.0f}%"
                    )
                else:
                    validation["goals_missed"].append(
                        f"❌ Max Single Loss: {worst_loss_pct:.1f}% > {VALIDATION_GOALS['max_single_loss_pct']:.0f}%"
                    )

        # Overall validation
        validation["overall_validated"] = len(validation["goals_missed"]) == 0

        return validation

    def generate_report(self, report_type: str = "weekly") -> str:
        """
        Generate performance report

        Args:
            report_type: "daily", "weekly", or "monthly"

        Returns:
            Report as formatted string
        """
        metrics = self.calculate_metrics()
        comparison = self.compare_strategies()
        validation = self.validate_goals()

        report = f"""
{'='*70}
📊 ETH MULTI-STRATEGY ROTATION - PERFORMANCE REPORT
{'='*70}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Report Type: {report_type.upper()}

{'='*70}
💰 OVERALL PERFORMANCE
{'='*70}
Starting Capital:    ${STARTING_CAPITAL:,.2f}
Final Balance:       ${metrics.get('final_balance', STARTING_CAPITAL):,.2f}
Total Return:        {metrics['total_return_pct']:+.2f}%
Total P&L:           ${metrics['total_pnl_usd']:+,.2f}

Total Trades:        {metrics['total_trades']}
Winning Trades:      {metrics.get('num_wins', 0)} ({metrics['win_rate_pct']:.1f}%)
Losing Trades:       {metrics.get('num_losses', 0)}

Average Win:         ${metrics['avg_win_usd']:.2f}
Average Loss:        ${metrics['avg_loss_usd']:.2f}
Profit Factor:       {metrics['profit_factor']:.2f}

Risk Metrics:
  Sharpe Ratio:      {metrics['sharpe_ratio']:.2f}
  Max Drawdown:      {metrics['max_drawdown_pct']:.2f}%
  Avg Return/Trade:  {metrics['avg_return_pct']:+.2f}%

{'='*70}
🎯 STRATEGY COMPARISON
{'='*70}
"""

        for strategy, stats in comparison.items():
            report += f"""
{strategy}:
  Trades:            {stats['trades']}
  Win Rate:          {stats['win_rate']:.1f}%
  Total P&L:         ${stats['total_pnl']:+,.2f}
  Avg P&L:           ${stats['avg_pnl']:+,.2f}
  Best Trade:        ${stats['best_trade']:+,.2f}
  Worst Trade:       ${stats['worst_trade']:+,.2f}
  Avg Confidence:    {stats['avg_confidence']:.1f}%
"""

        report += f"""
{'='*70}
✅ VALIDATION STATUS (1-2 Month Goals)
{'='*70}
"""

        for goal in validation["goals_met"]:
            report += f"{goal}\n"

        for goal in validation["goals_missed"]:
            report += f"{goal}\n"

        report += f"""
{'='*70}
Overall Status: {"✅ VALIDATED - System is performing as expected!" if validation['overall_validated'] else "⚠️  NEEDS IMPROVEMENT - Some goals not met"}
{'='*70}
"""

        return report

    def save_report(self, report_type: str = "weekly"):
        """
        Generate and save report to file

        Args:
            report_type: "daily", "weekly", or "monthly"
        """
        report = self.generate_report(report_type)

        # Create reports directory if it doesn't exist
        REPORTS_PATH.mkdir(parents=True, exist_ok=True)

        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = REPORTS_PATH / f"{report_type}_report_{timestamp}.txt"

        with open(filename, "w") as f:
            f.write(report)

        cprint(f"✅ Report saved: {filename}", "green")

        return filename

    def print_summary(self):
        """Print quick performance summary"""
        metrics = self.calculate_metrics()

        print("\n" + "="*60)
        print("📊 QUICK PERFORMANCE SUMMARY")
        print("="*60)
        print(f"Total Trades:     {metrics['total_trades']}")
        print(f"Win Rate:         {metrics['win_rate_pct']:.1f}%")
        print(f"Total Return:     {metrics['total_return_pct']:+.2f}%")
        print(f"Total P&L:        ${metrics['total_pnl_usd']:+,.2f}")
        print(f"Sharpe Ratio:     {metrics['sharpe_ratio']:.2f}")
        print(f"Max Drawdown:     {metrics['max_drawdown_pct']:.2f}%")
        print("="*60 + "\n")


if __name__ == "__main__":
    """Test strategy validator"""
    print("🧪 Testing Strategy Validator...\n")

    validator = StrategyValidator()

    # Print summary
    validator.print_summary()

    # Generate and save weekly report
    if not validator.trades_df.empty:
        print("\n📊 Generating Weekly Report...\n")
        report = validator.generate_report("weekly")
        print(report)

        # Save report
        validator.save_report("weekly")
    else:
        print("⚠️  No trades to analyze yet")
