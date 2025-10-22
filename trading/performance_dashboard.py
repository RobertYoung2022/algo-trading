"""
Performance Dashboard - Real-time Trading Monitoring
=====================================================
Simple CLI dashboard to monitor trading system performance

Features:
- Real-time position monitoring
- Performance metrics
- Strategy comparison
- Recent trades
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
from termcolor import cprint, colored

# Add parent directory to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import our modules
from trading.strategy_validator import StrategyValidator
from trading.hyperliquid_executor import HyperLiquidExecutor
from trading.config_eth_aggressive import SYMBOLS, STARTING_CAPITAL


def clear_screen():
    """Clear terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')


def print_header():
    """Print dashboard header"""
    print(colored("="*70, "cyan"))
    print(colored("🚀 ETH MULTI-STRATEGY ROTATION - PERFORMANCE DASHBOARD", "cyan", attrs=["bold"]))
    print(colored("="*70, "cyan"))
    print(colored(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "cyan"))
    print(colored("="*70, "cyan"))


def print_current_position(executor: HyperLiquidExecutor):
    """Print current position info"""
    symbol = SYMBOLS[0]
    position = executor.get_position(symbol)
    balance = executor.get_account_balance()

    print(f"\n💰 ACCOUNT STATUS")
    print("-" * 70)
    print(f"Balance:          ${balance:,.2f}")
    print(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
    print(f"Total P&L:        ${balance - STARTING_CAPITAL:+,.2f} ({(balance/STARTING_CAPITAL-1)*100:+.2f}%)")

    print(f"\n📈 CURRENT POSITION - {symbol}")
    print("-" * 70)

    if position["in_position"]:
        pnl_color = "green" if position["pnl"] > 0 else "red"
        print(f"Status:           {colored('IN POSITION', 'green', attrs=['bold'])}")
        print(f"Side:             {'LONG' if position['is_long'] else 'SHORT'}")
        print(f"Size:             {position['size']:.4f} {symbol}")
        print(f"Entry Price:      ${position['entry_price']:.2f}")

        ask, bid = executor.get_current_price(symbol)
        current_price = bid  # Use bid for closing long

        print(f"Current Price:    ${current_price:.2f}")
        print(f"P&L:              {colored(f'${position["pnl"]:+,.2f}', pnl_color)} "
              f"({colored(f'{position["pnl_pct"]:+.2f}%', pnl_color)})")
    else:
        print(f"Status:           {colored('NO POSITION', 'yellow')}")
        print(f"Waiting for entry signal...")


def print_performance_metrics(validator: StrategyValidator):
    """Print performance metrics"""
    metrics = validator.calculate_metrics()

    print(f"\n📊 PERFORMANCE METRICS")
    print("-" * 70)
    print(f"Total Trades:     {metrics['total_trades']}")

    if metrics["total_trades"] > 0:
        win_rate_color = "green" if metrics["win_rate_pct"] >= 55 else "yellow"
        return_color = "green" if metrics["total_return_pct"] > 0 else "red"

        print(f"Win Rate:         {colored(f'{metrics["win_rate_pct"]:.1f}%', win_rate_color)} "
              f"({metrics.get('num_wins', 0)}W / {metrics.get('num_losses', 0)}L)")
        print(f"Total Return:     {colored(f'{metrics["total_return_pct"]:+.2f}%', return_color)}")
        print(f"Sharpe Ratio:     {metrics['sharpe_ratio']:.2f}")
        print(f"Max Drawdown:     {metrics['max_drawdown_pct']:.2f}%")
        print(f"Profit Factor:    {metrics['profit_factor']:.2f}")
        print(f"Avg Win:          ${metrics['avg_win_usd']:.2f}")
        print(f"Avg Loss:         ${metrics['avg_loss_usd']:.2f}")
    else:
        print(colored("No trades yet", "yellow"))


def print_strategy_comparison(validator: StrategyValidator):
    """Print strategy comparison"""
    comparison = validator.compare_strategies()

    if not comparison:
        return

    print(f"\n🎯 STRATEGY COMPARISON")
    print("-" * 70)

    for strategy, stats in comparison.items():
        pnl_color = "green" if stats["total_pnl"] > 0 else "red"
        win_rate_color = "green" if stats["win_rate"] >= 55 else "yellow"

        print(f"\n{strategy}:")
        print(f"  Trades:         {stats['trades']}")
        print(f"  Win Rate:       {colored(f'{stats["win_rate"]:.1f}%', win_rate_color)}")
        print(f"  Total P&L:      {colored(f'${stats["total_pnl"]:+,.2f}', pnl_color)}")
        print(f"  Avg P&L:        ${stats['avg_pnl']:+,.2f}")
        print(f"  Confidence:     {stats['avg_confidence']:.1f}%")


def print_recent_trades(validator: StrategyValidator, n: int = 5):
    """Print recent trades"""
    if validator.trades_df.empty:
        return

    print(f"\n📋 RECENT TRADES (Last {n})")
    print("-" * 70)

    recent = validator.trades_df.tail(n).iloc[::-1]  # Reverse to show newest first

    for idx, trade in recent.iterrows():
        timestamp = trade["timestamp"].strftime("%m/%d %H:%M")
        strategy = trade["strategy"].replace("_", " ")
        pnl_color = "green" if trade["win"] else "red"
        win_symbol = "✅" if trade["win"] else "❌"

        print(f"{win_symbol} {timestamp} | {strategy:20s} | "
              f"{colored(f'${trade["pnl_usd"]:+7.2f}', pnl_color)} "
              f"({colored(f'{trade["pnl_pct"]:+5.2f}%', pnl_color)}) | "
              f"Conf: {trade['confidence']:.0f}%")


def print_validation_status(validator: StrategyValidator):
    """Print validation status"""
    validation = validator.validate_goals()

    print(f"\n✅ VALIDATION STATUS (1-2 Month Goals)")
    print("-" * 70)

    for goal in validation["goals_met"]:
        print(colored(goal, "green"))

    for goal in validation["goals_missed"]:
        print(colored(goal, "yellow"))

    if validation["overall_validated"]:
        print(colored("\n🎉 SYSTEM VALIDATED - All goals met!", "green", attrs=["bold"]))
    else:
        print(colored("\n⚠️  Some goals not met yet", "yellow"))


def run_dashboard(refresh_interval: int = 30):
    """
    Run dashboard with auto-refresh

    Args:
        refresh_interval: Refresh interval in seconds
    """
    executor = HyperLiquidExecutor()
    validator = StrategyValidator()

    try:
        while True:
            # Clear screen and refresh data
            clear_screen()
            validator.refresh_data()

            # Print all sections
            print_header()
            print_current_position(executor)
            print_performance_metrics(validator)
            print_strategy_comparison(validator)
            print_recent_trades(validator, n=5)
            print_validation_status(validator)

            # Footer
            print(f"\n{'-'*70}")
            print(colored(f"Auto-refreshing every {refresh_interval} seconds... (Ctrl+C to exit)", "cyan"))

            # Wait for refresh
            time.sleep(refresh_interval)

    except KeyboardInterrupt:
        print(colored("\n\n👋 Dashboard closed.", "cyan"))


if __name__ == "__main__":
    """Run dashboard"""
    import argparse

    parser = argparse.ArgumentParser(description="ETH Trading Performance Dashboard")
    parser.add_argument(
        "--refresh",
        type=int,
        default=30,
        help="Refresh interval in seconds (default: 30)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Show dashboard once without auto-refresh"
    )

    args = parser.parse_args()

    if args.once:
        # Show once
        executor = HyperLiquidExecutor()
        validator = StrategyValidator()

        print_header()
        print_current_position(executor)
        print_performance_metrics(validator)
        print_strategy_comparison(validator)
        print_recent_trades(validator, n=10)
        print_validation_status(validator)
    else:
        # Auto-refresh
        run_dashboard(args.refresh)
