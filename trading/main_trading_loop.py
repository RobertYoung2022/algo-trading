"""
Main Trading Loop - ETH Multi-Strategy Rotation System
=======================================================
Orchestrates the complete trading pipeline:
1. Strategy selection (RSI / SMA / Breakout)
2. AI risk validation
3. HyperLiquid execution
4. Performance tracking

Runs every 15 minutes, 24/7
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import time
import schedule
import pandas as pd
from termcolor import cprint

# Add parent directory to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import our modules
from trading.config_eth_aggressive import (
    SYMBOLS,
    CHECK_INTERVAL_MINUTES,
    STARTING_CAPITAL,
    POSITION_SIZE_PCT,
    MAX_POSITION_SIZE_USD,
    LEVERAGE,
    STOP_LOSS_PCT,
    TAKE_PROFIT_PCT,
    MINIMUM_BALANCE_USD,
    MAX_CONSECUTIVE_LOSSES,
    MAX_DAILY_LOSSES,
    COOLDOWN_AFTER_LOSS_MINUTES,
    TRADES_LOG_PATH,
    PAPER_TRADING_MODE,
    ENABLE_CIRCUIT_BREAKER,
    EMERGENCY_STOP_FILE,
    CHECK_EMERGENCY_STOP,
    USE_RISK_AGENT,
)

from trading.strategy_selector import StrategySelector
from trading.hyperliquid_executor import HyperLiquidExecutor

# Import AI risk agent
from ai_agents.risk_management.risk_agent import RiskAgent


class TradingSystem:
    """
    Main Trading System - Orchestrates the complete pipeline
    """

    def __init__(self):
        """Initialize trading system"""
        cprint("\n" + "="*60, "cyan")
        cprint("🚀 ETH MULTI-STRATEGY ROTATION SYSTEM", "cyan")
        cprint("="*60 + "\n", "cyan")

        # Initialize components
        self.symbol = SYMBOLS[0]  # ETH
        self.strategy_selector = StrategySelector()
        self.executor = HyperLiquidExecutor()
        self.risk_agent = RiskAgent() if USE_RISK_AGENT else None

        # State tracking
        self.consecutive_losses = 0
        self.daily_losses = 0
        self.last_loss_time = None
        self.last_trade_date = None

        # Initialize trade log
        self._init_trade_log()

        cprint("✅ Trading System initialized", "green")
        self._print_system_status()

    def _init_trade_log(self):
        """Initialize CSV trade log"""
        if not TRADES_LOG_PATH.exists():
            # Create log file with headers
            TRADES_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

            df = pd.DataFrame(columns=[
                "timestamp", "symbol", "strategy", "signal",
                "entry_price", "exit_price", "size", "size_usd",
                "stop_loss", "take_profit", "leverage",
                "pnl_usd", "pnl_pct", "win",
                "confidence", "ai_tier", "reasoning",
                "balance_before", "balance_after"
            ])
            df.to_csv(TRADES_LOG_PATH, index=False)
            cprint(f"📝 Trade log created: {TRADES_LOG_PATH}", "green")

    def _print_system_status(self):
        """Print current system status"""
        balance = self.executor.get_account_balance()
        position = self.executor.get_position(self.symbol)

        print("\n" + "="*60)
        print("📊 SYSTEM STATUS")
        print("="*60)
        print(f"💰 Balance: ${balance:,.2f}")
        print(f"📈 Position: {'IN' if position['in_position'] else 'OUT'}")

        if position["in_position"]:
            print(f"   Size: {position['size']:.4f} {self.symbol}")
            print(f"   Entry: ${position['entry_price']:.2f}")
            print(f"   P&L: ${position['pnl']:.2f} ({position['pnl_pct']:+.2f}%)")

        print(f"🛡️  Circuit Breaker: {'✅ Active' if ENABLE_CIRCUIT_BREAKER else '❌ Off'}")
        print(f"   Consecutive Losses: {self.consecutive_losses}/{MAX_CONSECUTIVE_LOSSES}")
        print(f"   Daily Losses: {self.daily_losses}/{MAX_DAILY_LOSSES}")

        print(f"📝 Mode: {'PAPER TRADING' if PAPER_TRADING_MODE else '🚨 LIVE TRADING'}")
        print("="*60 + "\n")

    def _check_emergency_stop(self) -> bool:
        """Check if emergency stop file exists"""
        if CHECK_EMERGENCY_STOP and EMERGENCY_STOP_FILE.exists():
            cprint("\n🛑 EMERGENCY STOP DETECTED!", "red")
            cprint(f"   Stop file found: {EMERGENCY_STOP_FILE}", "red")
            cprint("   Delete this file to resume trading\n", "red")
            return True
        return False

    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker should halt trading"""
        if not ENABLE_CIRCUIT_BREAKER:
            return False

        # Check consecutive losses
        if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            cprint(f"\n🛑 CIRCUIT BREAKER: {self.consecutive_losses} consecutive losses!", "red")
            cprint("   System halted. Manual intervention required.\n", "red")
            return True

        # Check daily losses
        if self.daily_losses >= MAX_DAILY_LOSSES:
            cprint(f"\n🛑 CIRCUIT BREAKER: {self.daily_losses} losses today!", "red")
            cprint("   Trading halted for today.\n", "red")
            return True

        # Check cooldown after loss
        if self.last_loss_time:
            time_since_loss = (datetime.now() - self.last_loss_time).total_seconds() / 60
            if time_since_loss < COOLDOWN_AFTER_LOSS_MINUTES:
                remaining = COOLDOWN_AFTER_LOSS_MINUTES - time_since_loss
                cprint(f"⏸️  Cooldown active: {remaining:.0f} minutes remaining", "yellow")
                return True

        # Check minimum balance
        balance = self.executor.get_account_balance()
        if balance < MINIMUM_BALANCE_USD:
            cprint(f"\n🛑 CIRCUIT BREAKER: Balance ${balance:.2f} < ${MINIMUM_BALANCE_USD}!", "red")
            cprint("   System halted. Insufficient funds.\n", "red")
            return True

        return False

    def _reset_daily_counters(self):
        """Reset daily loss counter at start of new day"""
        current_date = datetime.now().date()
        if self.last_trade_date != current_date:
            self.daily_losses = 0
            self.last_trade_date = current_date
            cprint(f"🔄 Daily counters reset for {current_date}", "cyan")

    def _log_trade(self, strategy_signal, entry_price, exit_price=None,
                   pnl_usd=None, pnl_pct=None, win=None):
        """
        Log trade to CSV

        Args:
            strategy_signal: StrategySignal object
            entry_price: Entry price
            exit_price: Exit price (None if still open)
            pnl_usd: P&L in USD (None if still open)
            pnl_pct: P&L percentage (None if still open)
            win: Whether trade was winning (None if still open)
        """
        balance = self.executor.get_account_balance()
        position_size_usd = balance * POSITION_SIZE_PCT

        trade_data = {
            "timestamp": datetime.now().isoformat(),
            "symbol": self.symbol,
            "strategy": strategy_signal.strategy_name,
            "signal": strategy_signal.signal,
            "entry_price": entry_price,
            "exit_price": exit_price or 0,
            "size": position_size_usd / entry_price,
            "size_usd": position_size_usd,
            "stop_loss": entry_price * (1 - STOP_LOSS_PCT),
            "take_profit": entry_price * (1 + TAKE_PROFIT_PCT),
            "leverage": LEVERAGE,
            "pnl_usd": pnl_usd or 0,
            "pnl_pct": pnl_pct or 0,
            "win": win if win is not None else "",
            "confidence": strategy_signal.confidence,
            "ai_tier": strategy_signal.metadata.get("ai_tier", "N/A"),
            "reasoning": strategy_signal.reasoning[:200],  # Truncate
            "balance_before": balance,
            "balance_after": balance + (pnl_usd or 0)
        }

        df = pd.DataFrame([trade_data])
        df.to_csv(TRADES_LOG_PATH, mode="a", header=False, index=False)

    def _execute_trade(self, strategy_signal):
        """
        Execute trade based on strategy signal

        Args:
            strategy_signal: StrategySignal object

        Returns:
            Success status
        """
        cprint(f"\n{'='*60}", "cyan")
        cprint(f"🎯 EXECUTING TRADE", "cyan")
        cprint(f"{'='*60}", "cyan")

        balance = self.executor.get_account_balance()
        position_size_usd = balance * POSITION_SIZE_PCT

        # Risk validation (if enabled)
        if self.risk_agent:
            cprint("\n🛡️  Running AI risk validation...", "cyan")

            risk_result = self.risk_agent.execute(
                symbol=self.symbol,
                signal=strategy_signal.signal,
                entry_price=strategy_signal.entry_price,
                position_size_usd=position_size_usd,
                stop_loss=strategy_signal.entry_price * (1 - STOP_LOSS_PCT),
                take_profit=strategy_signal.entry_price * (1 + TAKE_PROFIT_PCT),
                account_balance=balance
            )

            if risk_result.decision != "APPROVED":
                cprint(f"\n❌ TRADE REJECTED by Risk Agent", "red")
                cprint(f"   Reason: {risk_result.reasoning}", "red")
                return False

            cprint(f"✅ Risk validation passed (confidence: {risk_result.confidence}%)", "green")

        # Execute on HyperLiquid
        success = self.executor.enter_long_position(
            symbol=self.symbol,
            size_usd=position_size_usd,
            entry_price=strategy_signal.entry_price
        )

        if success:
            # Log entry
            self._log_trade(strategy_signal, strategy_signal.entry_price)
            cprint(f"\n✅ TRADE EXECUTED SUCCESSFULLY", "green")
            return True
        else:
            cprint(f"\n❌ TRADE EXECUTION FAILED", "red")
            return False

    def _monitor_position(self):
        """Monitor open position and handle exit"""
        position = self.executor.get_position(self.symbol)

        if not position["in_position"]:
            return

        cprint(f"\n📊 Monitoring {self.symbol} position...", "cyan")
        cprint(f"   P&L: ${position['pnl']:.2f} ({position['pnl_pct']:+.2f}%)", "cyan")

        # Check if TP or SL hit (handled by HyperLiquid automatically)
        # But we can also check for manual exit conditions here

        # For now, rely on HyperLiquid's TP/SL orders
        # In production, you might want additional exit logic

    def trading_iteration(self):
        """
        Single trading loop iteration

        Called every CHECK_INTERVAL_MINUTES
        """
        try:
            cprint(f"\n{'='*60}", "blue")
            cprint(f"🔄 TRADING ITERATION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "blue")
            cprint(f"{'='*60}", "blue")

            # Check emergency stop
            if self._check_emergency_stop():
                return

            # Reset daily counters
            self._reset_daily_counters()

            # Check circuit breaker
            if self._check_circuit_breaker():
                return

            # Check if already in position
            position = self.executor.get_position(self.symbol)
            if position["in_position"]:
                self._monitor_position()
                cprint("⏳ Already in position, waiting for exit...", "yellow")
                return

            # Find best strategy opportunity
            cprint("\n🔍 Searching for trading opportunities...", "cyan")
            best_strategy = self.strategy_selector.select_best_strategy()

            if not best_strategy:
                cprint("⏸️  No good opportunities at this time", "yellow")
                return

            # Execute trade
            self._execute_trade(best_strategy)

        except Exception as e:
            cprint(f"\n❌ Error in trading iteration: {e}", "red")
            import traceback
            traceback.print_exc()

    def run(self):
        """Run trading system 24/7"""
        cprint("\n" + "="*60, "green")
        cprint("🚀 STARTING TRADING SYSTEM", "green")
        cprint("="*60, "green")

        cprint(f"\n⏰ Check Interval: Every {CHECK_INTERVAL_MINUTES} minutes", "cyan")
        cprint(f"💰 Position Size: {POSITION_SIZE_PCT*100:.0f}% ({MAX_POSITION_SIZE_USD:.0f} USD)", "cyan")
        cprint(f"📊 Leverage: {LEVERAGE}x", "cyan")
        cprint(f"🎯 Risk:Reward: {STOP_LOSS_PCT*100:.1f}% SL / {TAKE_PROFIT_PCT*100:.1f}% TP", "cyan")
        cprint(f"📝 Mode: {'PAPER TRADING' if PAPER_TRADING_MODE else '🚨 LIVE TRADING'}", "cyan")

        # Run first iteration immediately
        self.trading_iteration()

        # Schedule regular iterations
        schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(self.trading_iteration)

        cprint(f"\n✅ System running. Press Ctrl+C to stop.\n", "green")

        # Main loop
        while True:
            try:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds

            except KeyboardInterrupt:
                cprint("\n\n🛑 Shutting down trading system...", "yellow")
                break

            except Exception as e:
                cprint(f"\n❌ Unexpected error: {e}", "red")
                import traceback
                traceback.print_exc()
                time.sleep(60)  # Wait 1 minute before retrying

        cprint("\n👋 Trading system stopped.\n", "cyan")


if __name__ == "__main__":
    """Start trading system"""
    system = TradingSystem()
    system.run()
