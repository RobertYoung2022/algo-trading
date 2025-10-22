"""
Whale Agent
Monitors large transactions and whale wallet activity
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from termcolor import cprint

from ai_agents.base_agent import BaseAIAgent, AgentResult


class WhaleAgent(BaseAIAgent):
    """
    Monitors whale activity and large transactions

    Whale movements often precede:
    - Major price moves
    - Trend reversals
    - Accumulation/distribution phases
    """

    def __init__(self):
        super().__init__(
            name="WhaleAgent",
            description="Monitors whale transactions for market positioning signals",
            complexity="complex"
        )

        # Thresholds (in millions USD for crypto, or % of average volume)
        self.whale_transaction_threshold = 1.0  # $1M+ transaction
        self.mega_whale_threshold = 10.0         # $10M+ transaction

    def execute(self,
                symbol: str = "BTC",
                lookback_hours: int = 24) -> AgentResult:
        """
        Analyze whale activity

        Args:
            symbol: Trading symbol
            lookback_hours: Hours of history to analyze

        Returns:
            AgentResult with whale analysis
        """
        self.log_info(f"Analyzing whale activity for {symbol}")

        try:
            # Get whale transaction data
            whale_data = self._get_whale_data(symbol, lookback_hours)

            if not whale_data:
                return self.create_result(
                    success=False,
                    errors=["Failed to fetch whale data"]
                )

            # Analyze whale activity
            analysis = self._analyze_whale_activity(whale_data)

            # Generate AI signal
            ai_signal = self._generate_ai_signal(symbol, whale_data, analysis)

            self.log_success(f"Whale analysis complete: {ai_signal['signal']}")

            return self.create_result(
                success=True,
                data={
                    "symbol": symbol,
                    "whale_transactions_24h": analysis["num_whale_transactions"],
                    "total_whale_volume": analysis["total_whale_volume"],
                    "net_flow": analysis["net_flow"],
                    "whale_sentiment": analysis["whale_sentiment"],
                    "signal": ai_signal["signal"],
                    "reasoning": ai_signal["reasoning"]
                },
                ai_response=ai_signal.get("ai_response"),
                confidence=ai_signal.get("confidence", 0.0)
            )

        except Exception as e:
            self.log_error(f"Error executing whale agent: {e}")
            return self.create_result(
                success=False,
                errors=[str(e)]
            )

    def _get_whale_data(self,
                       symbol: str,
                       lookback_hours: int) -> Optional[Dict[str, Any]]:
        """
        Get whale transaction data

        For now, returns simulated data.
        In production, integrate with blockchain explorers or whale alert APIs.
        """
        # TODO: Integrate with real whale tracking services
        # Options: WhaleAlert, Glassnode, CryptoQuant APIs

        current_time = datetime.now()
        transactions = []

        # Simulate some whale transactions
        import random
        for i in range(lookback_hours):
            if random.random() > 0.85:  # 15% chance of whale tx per hour
                tx_type = random.choice(["buy", "sell", "transfer"])
                amount_usd = random.uniform(1, 15) * 1_000_000  # $1M-$15M

                transactions.append({
                    "timestamp": current_time - timedelta(hours=i),
                    "type": tx_type,
                    "amount_usd": amount_usd,
                    "from_exchange": random.choice([True, False]),
                    "to_exchange": random.choice([True, False])
                })

        self.log_warning("Using simulated whale data. Integrate real API for production.")

        return {
            "transactions": transactions,
            "symbol": symbol
        }

    def _analyze_whale_activity(self, whale_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze whale transaction patterns

        Args:
            whale_data: Whale transaction data

        Returns:
            Analysis results
        """
        transactions = whale_data["transactions"]

        if not transactions:
            return {
                "num_whale_transactions": 0,
                "total_whale_volume": 0,
                "net_flow": 0,
                "whale_sentiment": "neutral"
            }

        # Calculate metrics
        total_volume = sum(tx["amount_usd"] for tx in transactions)
        num_transactions = len(transactions)

        # Calculate net flow (to exchange - from exchange)
        # Positive = accumulation (bullish), Negative = distribution (bearish)
        inflow = sum(tx["amount_usd"] for tx in transactions if tx.get("to_exchange"))
        outflow = sum(tx["amount_usd"] for tx in transactions if tx.get("from_exchange"))
        net_flow = outflow - inflow  # Positive = accumulation, Negative = distribution

        # Determine whale sentiment
        if net_flow > total_volume * 0.3:
            whale_sentiment = "accumulating"  # Bullish
        elif net_flow < -total_volume * 0.3:
            whale_sentiment = "distributing"  # Bearish
        else:
            whale_sentiment = "neutral"

        return {
            "num_whale_transactions": num_transactions,
            "total_whale_volume": total_volume / 1_000_000,  # Convert to millions
            "net_flow": net_flow / 1_000_000,
            "whale_sentiment": whale_sentiment,
            "inflow": inflow / 1_000_000,
            "outflow": outflow / 1_000_000
        }

    def _generate_ai_signal(self,
                           symbol: str,
                           whale_data: Dict[str, Any],
                           analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate trading signal using AI

        Args:
            symbol: Trading symbol
            whale_data: Raw whale data
            analysis: Whale activity analysis

        Returns:
            Dict with signal, reasoning, and confidence
        """
        prompt = f"""Analyze whale activity for {symbol}:

Whale Transactions (24h): {analysis['num_whale_transactions']}
Total Whale Volume: ${analysis['total_whale_volume']:.1f}M
Exchange Inflow: ${analysis['inflow']:.1f}M
Exchange Outflow: ${analysis['outflow']:.1f}M
Net Flow: ${analysis['net_flow']:.1f}M
Whale Sentiment: {analysis['whale_sentiment']}

Whale Activity Interpretation:
- Accumulation (positive net flow) = whales buying/withdrawing from exchanges (bullish)
- Distribution (negative net flow) = whales selling/depositing to exchanges (bearish)
- Large transactions often precede major moves

Based on this whale activity, provide:
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
            analysis: Whale activity analysis

        Returns:
            Dict with signal, reasoning, and confidence
        """
        sentiment = analysis["whale_sentiment"]
        net_flow = analysis["net_flow"]
        volume = analysis["total_whale_volume"]

        if sentiment == "accumulating" and volume > 5.0:  # $5M+
            return {
                "signal": "BUY",
                "reasoning": f"Whales accumulating (${net_flow:.1f}M net outflow) - bullish positioning",
                "confidence": 0.7
            }
        elif sentiment == "distributing" and volume > 5.0:
            return {
                "signal": "SELL",
                "reasoning": f"Whales distributing (${abs(net_flow):.1f}M net inflow) - bearish positioning",
                "confidence": 0.7
            }
        else:
            return {
                "signal": "HOLD",
                "reasoning": "Whale activity not decisive enough for clear signal",
                "confidence": 0.5
            }


# Standalone execution
if __name__ == "__main__":
    print("Testing Whale Agent...\n")

    agent = WhaleAgent()
    result = agent.execute(symbol="BTC")

    print("\n" + "="*60)
    print("WHALE AGENT RESULT")
    print("="*60)
    print(f"Success: {result.success}")
    print(f"Symbol: {result.data.get('symbol')}")
    print(f"Whale Transactions: {result.data.get('whale_transactions_24h', 0)}")
    print(f"Total Volume: ${result.data.get('total_whale_volume', 0):.1f}M")
    print(f"Whale Sentiment: {result.data.get('whale_sentiment')}")
    print(f"Signal: {result.data.get('signal')}")
    print(f"Reasoning: {result.data.get('reasoning')}")
    print(f"Confidence: {result.confidence:.1%}")
    print("="*60)
