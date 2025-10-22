"""
Liquidation Agent
Monitors liquidation events to identify capitulation and market extremes
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from termcolor import cprint

from ai_agents.base_agent import BaseAIAgent, AgentResult


class LiquidationAgent(BaseAIAgent):
    """
    Monitors liquidation events for market extreme signals

    Large liquidation cascades often signal:
    - Market capitulation (good buy opportunity)
    - Overextension (potential reversal)
    - Volatility spikes (risk management)
    """

    def __init__(self):
        super().__init__(
            name="LiquidationAgent",
            description="Monitors liquidations for capitulation and reversal signals",
            complexity="complex"
        )

        # Thresholds (in millions USD)
        self.significant_liquidation_threshold = 10.0  # $10M+
        self.extreme_liquidation_threshold = 50.0      # $50M+

    def execute(self,
                symbol: str = "BTC",
                lookback_hours: int = 24) -> AgentResult:
        """
        Analyze liquidation events

        Args:
            symbol: Trading symbol
            lookback_hours: Hours of history to analyze

        Returns:
            AgentResult with liquidation analysis
        """
        self.log_info(f"Analyzing liquidations for {symbol}")

        try:
            # Get liquidation data
            liquidation_data = self._get_liquidation_data(symbol, lookback_hours)

            if not liquidation_data:
                return self.create_result(
                    success=False,
                    errors=["Failed to fetch liquidation data"]
                )

            # Analyze liquidations
            analysis = self._analyze_liquidations(liquidation_data)

            # Generate AI signal
            ai_signal = self._generate_ai_signal(symbol, liquidation_data, analysis)

            self.log_success(f"Liquidation analysis complete: {ai_signal['signal']}")

            return self.create_result(
                success=True,
                data={
                    "symbol": symbol,
                    "total_liquidations_24h": analysis["total_liquidations_24h"],
                    "long_liquidations": analysis["long_liquidations"],
                    "short_liquidations": analysis["short_liquidations"],
                    "dominant_side": analysis["dominant_side"],
                    "signal": ai_signal["signal"],
                    "reasoning": ai_signal["reasoning"],
                    "severity": analysis["severity"]
                },
                ai_response=ai_signal.get("ai_response"),
                confidence=ai_signal.get("confidence", 0.0)
            )

        except Exception as e:
            self.log_error(f"Error executing liquidation agent: {e}")
            return self.create_result(
                success=False,
                errors=[str(e)]
            )

    def _get_liquidation_data(self,
                             symbol: str,
                             lookback_hours: int) -> Optional[Dict[str, Any]]:
        """
        Get liquidation data

        For now, returns simulated data.
        In production, integrate with Coinglass, Hyblock, or exchange APIs.
        """
        # TODO: Integrate with real liquidation data providers
        # Coinglass API, Hyblock Capital, or exchange websockets

        current_time = datetime.now()
        liquidations = []

        # Simulate some liquidation events
        # In reality, you'd pull this from an API
        import random
        for i in range(lookback_hours):
            if random.random() > 0.7:  # 30% chance of liquidation event per hour
                liquidations.append({
                    "timestamp": current_time - timedelta(hours=i),
                    "side": random.choice(["long", "short"]),
                    "amount_usd": random.uniform(1, 20) * 1_000_000,  # $1M-$20M
                    "price": 50000 + random.uniform(-5000, 5000)
                })

        self.log_warning("Using simulated liquidation data. Integrate real API for production.")

        return {
            "liquidations": liquidations,
            "symbol": symbol
        }

    def _analyze_liquidations(self, liquidation_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze liquidation patterns

        Args:
            liquidation_data: Liquidation data

        Returns:
            Analysis results
        """
        liquidations = liquidation_data["liquidations"]

        if not liquidations:
            return {
                "total_liquidations_24h": 0,
                "long_liquidations": 0,
                "short_liquidations": 0,
                "dominant_side": "none",
                "severity": "low"
            }

        # Calculate totals
        long_liq = sum(l["amount_usd"] for l in liquidations if l["side"] == "long")
        short_liq = sum(l["amount_usd"] for l in liquidations if l["side"] == "short")
        total_liq = long_liq + short_liq

        # Determine dominant side
        if long_liq > short_liq * 1.5:
            dominant_side = "long"
        elif short_liq > long_liq * 1.5:
            dominant_side = "short"
        else:
            dominant_side = "balanced"

        # Determine severity
        if total_liq >= self.extreme_liquidation_threshold * 1_000_000:
            severity = "extreme"
        elif total_liq >= self.significant_liquidation_threshold * 1_000_000:
            severity = "high"
        else:
            severity = "moderate"

        return {
            "total_liquidations_24h": total_liq / 1_000_000,  # Convert to millions
            "long_liquidations": long_liq / 1_000_000,
            "short_liquidations": short_liq / 1_000_000,
            "dominant_side": dominant_side,
            "severity": severity,
            "num_events": len(liquidations)
        }

    def _generate_ai_signal(self,
                           symbol: str,
                           liquidation_data: Dict[str, Any],
                           analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate trading signal using AI

        Args:
            symbol: Trading symbol
            liquidation_data: Raw liquidation data
            analysis: Liquidation analysis

        Returns:
            Dict with signal, reasoning, and confidence
        """
        prompt = f"""Analyze liquidation events for {symbol}:

Total Liquidations (24h): ${analysis['total_liquidations_24h']:.1f}M
Long Liquidations: ${analysis['long_liquidations']:.1f}M
Short Liquidations: ${analysis['short_liquidations']:.1f}M
Dominant Side: {analysis['dominant_side']}
Severity: {analysis['severity']}

Liquidation Interpretation:
- Heavy long liquidations = price dropped, longs got liquidated (potential capitulation/buy signal)
- Heavy short liquidations = price pumped, shorts got squeezed (potential exhaustion/sell signal)
- Extreme liquidations often mark local bottoms/tops

Based on this liquidation data, provide:
1. Trading signal (BUY/SELL/HOLD)
2. Brief reasoning (1-2 sentences)
3. Confidence level (0-100)

Format your response as:
SIGNAL: [BUY/SELL/HOLD]
REASONING: [your reasoning]
CONFIDENCE: [0-100]"""

        # Generate AI response
        ai_response = self.generate_ai_response(
            prompt=prompt,
            temperature=0.3
        )

        if not ai_response:
            return self._rule_based_signal(analysis)

        # Parse AI response
        try:
            content = ai_response.content
            signal = "HOLD"
            reasoning = "Unable to parse AI response"
            confidence = 50.0

            if "SIGNAL:" in content:
                signal = content.split("SIGNAL:")[1].split("\n")[0].strip()
            if "REASONING:" in content:
                reasoning = content.split("REASONING:")[1].split("\n")[0].strip()
            if "CONFIDENCE:" in content:
                conf_str = content.split("CONFIDENCE:")[1].split("\n")[0].strip()
                confidence = float(conf_str)

            return {
                "signal": signal,
                "reasoning": reasoning,
                "confidence": confidence / 100.0,
                "ai_response": ai_response
            }

        except Exception as e:
            self.log_warning(f"Failed to parse AI response: {e}")
            return self._rule_based_signal(analysis)

    def _rule_based_signal(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fallback rule-based signal generation

        Args:
            analysis: Liquidation analysis

        Returns:
            Dict with signal, reasoning, and confidence
        """
        dominant_side = analysis["dominant_side"]
        severity = analysis["severity"]
        total_liq = analysis["total_liquidations_24h"]

        if dominant_side == "long" and severity in ["high", "extreme"]:
            return {
                "signal": "BUY",
                "reasoning": f"Heavy long liquidations (${total_liq:.1f}M) suggest capitulation - potential buy opportunity",
                "confidence": 0.75
            }
        elif dominant_side == "short" and severity in ["high", "extreme"]:
            return {
                "signal": "SELL",
                "reasoning": f"Heavy short liquidations (${total_liq:.1f}M) suggest overextension - potential reversal",
                "confidence": 0.75
            }
        else:
            return {
                "signal": "HOLD",
                "reasoning": "Liquidations not extreme enough for clear signal",
                "confidence": 0.5
            }


# Standalone execution
if __name__ == "__main__":
    print("Testing Liquidation Agent...\n")

    agent = LiquidationAgent()
    result = agent.execute(symbol="BTC")

    print("\n" + "="*60)
    print("LIQUIDATION AGENT RESULT")
    print("="*60)
    print(f"Success: {result.success}")
    print(f"Symbol: {result.data.get('symbol')}")
    print(f"Total Liquidations: ${result.data.get('total_liquidations_24h', 0):.1f}M")
    print(f"Dominant Side: {result.data.get('dominant_side')}")
    print(f"Signal: {result.data.get('signal')}")
    print(f"Reasoning: {result.data.get('reasoning')}")
    print(f"Confidence: {result.confidence:.1%}")
    print("="*60)
