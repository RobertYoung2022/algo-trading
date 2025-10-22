"""
Parallel Execution Framework
Run traditional algo-fun system alongside AI-enhanced system for comparison
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
import json
import pandas as pd
from termcolor import cprint

from ai_agents.market_intelligence import FundingAgent, LiquidationAgent, WhaleAgent
from ai_agents.risk_management import RiskAgent
from ai_config import AIConfig


class ParallelExecutor:
    """
    Execute traditional and AI-enhanced trading systems in parallel

    Compares:
    - Traditional rule-based signals vs AI-enhanced signals
    - Performance metrics
    - Risk management decisions
    - Market intelligence integration
    """

    def __init__(self,
                 config: Optional[AIConfig] = None,
                 results_dir: str = "results/parallel_testing"):
        """
        Initialize Parallel Executor

        Args:
            config: AI configuration
            results_dir: Directory to store results
        """
        self.config = config or AIConfig()
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)

        # Initialize AI agents
        self.funding_agent = FundingAgent() if self.config.is_agent_enabled("funding") else None
        self.liquidation_agent = LiquidationAgent() if self.config.is_agent_enabled("liquidation") else None
        self.whale_agent = WhaleAgent() if self.config.is_agent_enabled("whale") else None
        self.risk_agent = RiskAgent(
            max_position_pct=self.config.agents.max_position_percentage / 100,
            max_loss_usd=self.config.agents.max_loss_usd,
            min_balance=self.config.agents.minimum_balance_usd
        ) if self.config.is_agent_enabled("risk") else None

        # Results tracking
        self.results = {
            "traditional": [],
            "ai_enhanced": [],
            "comparisons": []
        }

        cprint("\n🔄 Parallel Executor initialized", "cyan")
        cprint(f"   Traditional System: ✅", "green")
        cprint(f"   AI-Enhanced System: ✅", "green")
        cprint(f"   Results Dir: {self.results_dir}", "cyan")

    def execute_traditional(self,
                           symbol: str,
                           strategy_signal: str,
                           strategy_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute traditional algo-fun logic

        Args:
            symbol: Trading symbol
            strategy_signal: Traditional strategy signal (BUY/SELL/HOLD)
            strategy_data: Strategy data and metrics

        Returns:
            Dict with traditional execution results
        """
        cprint(f"\n📊 Traditional System: {symbol}", "yellow")

        result = {
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "signal": strategy_signal,
            "confidence": strategy_data.get("confidence", 0.5),
            "system": "traditional",
            "market_intelligence": None,
            "ai_validation": None
        }

        cprint(f"   Signal: {strategy_signal}", "yellow")
        cprint(f"   Confidence: {result['confidence']:.1%}", "yellow")

        self.results["traditional"].append(result)
        return result

    def execute_ai_enhanced(self,
                           symbol: str,
                           strategy_signal: str,
                           strategy_data: Dict[str, Any],
                           position_data: Optional[Dict[str, Any]] = None,
                           account_balance: float = 10000.0) -> Dict[str, Any]:
        """
        Execute AI-enhanced logic with market intelligence and risk validation

        Args:
            symbol: Trading symbol
            strategy_signal: Base strategy signal
            strategy_data: Strategy data
            position_data: Position details for risk assessment
            account_balance: Current account balance

        Returns:
            Dict with AI-enhanced execution results
        """
        cprint(f"\n🤖 AI-Enhanced System: {symbol}", "cyan")

        result = {
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "base_signal": strategy_signal,
            "final_signal": strategy_signal,
            "confidence": strategy_data.get("confidence", 0.5),
            "system": "ai_enhanced",
            "market_intelligence": {},
            "ai_validation": None
        }

        # Phase 1: Gather market intelligence
        cprint("   Phase 1: Market Intelligence...", "cyan")

        if self.funding_agent:
            funding_result = self.funding_agent.execute(symbol=symbol)
            if funding_result.success:
                result["market_intelligence"]["funding"] = {
                    "signal": funding_result.data.get("signal"),
                    "current_rate": funding_result.data.get("current_rate"),
                    "reasoning": funding_result.data.get("reasoning")
                }
                cprint(f"      Funding: {funding_result.data.get('signal')}", "cyan")

        if self.liquidation_agent:
            liq_result = self.liquidation_agent.execute(symbol=symbol)
            if liq_result.success:
                result["market_intelligence"]["liquidation"] = {
                    "signal": liq_result.data.get("signal"),
                    "total_24h": liq_result.data.get("total_liquidations_24h"),
                    "reasoning": liq_result.data.get("reasoning")
                }
                cprint(f"      Liquidation: {liq_result.data.get('signal')}", "cyan")

        if self.whale_agent:
            whale_result = self.whale_agent.execute(symbol=symbol)
            if whale_result.success:
                result["market_intelligence"]["whale"] = {
                    "signal": whale_result.data.get("signal"),
                    "sentiment": whale_result.data.get("whale_sentiment"),
                    "reasoning": whale_result.data.get("reasoning")
                }
                cprint(f"      Whale: {whale_result.data.get('signal')}", "cyan")

        # Phase 2: Combine signals
        market_signals = [
            result["market_intelligence"].get("funding", {}).get("signal"),
            result["market_intelligence"].get("liquidation", {}).get("signal"),
            result["market_intelligence"].get("whale", {}).get("signal")
        ]
        market_signals = [s for s in market_signals if s]

        # Count signal types
        buy_signals = market_signals.count("BUY")
        sell_signals = market_signals.count("SELL")

        # Modify signal based on market intelligence
        if buy_signals >= 2 and strategy_signal != "SELL":
            result["final_signal"] = "BUY"
            result["signal_modified"] = True
            result["modification_reason"] = "Strong buy signals from market intelligence"
        elif sell_signals >= 2 and strategy_signal != "BUY":
            result["final_signal"] = "SELL"
            result["signal_modified"] = True
            result["modification_reason"] = "Strong sell signals from market intelligence"
        else:
            result["signal_modified"] = False

        cprint(f"   Phase 2: Final Signal: {result['final_signal']}", "cyan")

        # Phase 3: Risk validation (if position data provided)
        if position_data and self.risk_agent:
            cprint("   Phase 3: Risk Validation...", "cyan")

            risk_result = self.risk_agent.execute(
                position_data=position_data,
                account_balance=account_balance
            )

            if risk_result.success:
                result["ai_validation"] = {
                    "approved": risk_result.data.get("approved"),
                    "reasoning": risk_result.data.get("reasoning"),
                    "risk_score": risk_result.data.get("risk_score")
                }

                if not risk_result.data.get("approved"):
                    result["final_signal"] = "HOLD"
                    result["risk_override"] = True
                    cprint(f"      Risk Override: Trade rejected", "red")
                else:
                    cprint(f"      Risk Approved", "green")

        self.results["ai_enhanced"].append(result)
        return result

    def compare_results(self,
                       traditional_result: Dict[str, Any],
                       ai_enhanced_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare traditional vs AI-enhanced results

        Args:
            traditional_result: Traditional system result
            ai_enhanced_result: AI-enhanced system result

        Returns:
            Comparison analysis
        """
        comparison = {
            "timestamp": datetime.now().isoformat(),
            "symbol": traditional_result["symbol"],
            "traditional_signal": traditional_result["signal"],
            "ai_signal": ai_enhanced_result["final_signal"],
            "signals_match": traditional_result["signal"] == ai_enhanced_result["final_signal"],
            "ai_modified_signal": ai_enhanced_result.get("signal_modified", False),
            "ai_risk_override": ai_enhanced_result.get("risk_override", False),
            "market_intelligence_used": len(ai_enhanced_result.get("market_intelligence", {})) > 0
        }

        self.results["comparisons"].append(comparison)

        cprint("\n📈 Comparison:", "magenta")
        cprint(f"   Traditional: {comparison['traditional_signal']}", "yellow")
        cprint(f"   AI-Enhanced: {comparison['ai_signal']}", "cyan")
        cprint(f"   Match: {'✅' if comparison['signals_match'] else '❌'}", "magenta")

        return comparison

    def save_results(self):
        """Save all results to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = self.results_dir / f"parallel_results_{timestamp}.json"

        with open(filepath, 'w') as f:
            json.dump(self.results, f, indent=2)

        cprint(f"\n💾 Results saved to {filepath}", "green")

        # Generate summary
        self._print_summary()

    def _print_summary(self):
        """Print summary of parallel execution"""
        if not self.results["comparisons"]:
            cprint("\n⚠️  No comparisons to summarize", "yellow")
            return

        total = len(self.results["comparisons"])
        matches = sum(1 for c in self.results["comparisons"] if c["signals_match"])
        ai_modifications = sum(1 for c in self.results["comparisons"] if c["ai_modified_signal"])
        risk_overrides = sum(1 for c in self.results["comparisons"] if c["ai_risk_override"])

        print("\n" + "="*60)
        print("📊 PARALLEL EXECUTION SUMMARY")
        print("="*60)
        print(f"Total Comparisons: {total}")
        print(f"Signals Matched: {matches} ({matches/total*100:.1f}%)")
        print(f"AI Modified Signal: {ai_modifications} ({ai_modifications/total*100:.1f}%)")
        print(f"Risk Overrides: {risk_overrides} ({risk_overrides/total*100:.1f}%)")
        print("="*60)


# Standalone execution / demo
if __name__ == "__main__":
    print("Testing Parallel Executor...\n")

    executor = ParallelExecutor()

    # Simulate a test case
    symbol = "BTC"
    strategy_signal = "BUY"
    strategy_data = {"confidence": 0.65, "indicator": "RSI Oversold"}

    # Traditional execution
    trad_result = executor.execute_traditional(
        symbol=symbol,
        strategy_signal=strategy_signal,
        strategy_data=strategy_data
    )

    # AI-enhanced execution
    position_data = {
        "symbol": symbol,
        "position_size": 1000.0,
        "entry_price": 50000.0,
        "stop_loss": 49000.0,
        "take_profit": 52000.0
    }

    ai_result = executor.execute_ai_enhanced(
        symbol=symbol,
        strategy_signal=strategy_signal,
        strategy_data=strategy_data,
        position_data=position_data,
        account_balance=10000.0
    )

    # Compare
    comparison = executor.compare_results(trad_result, ai_result)

    # Save results
    executor.save_results()
