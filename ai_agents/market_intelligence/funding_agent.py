"""
Funding Rate Agent
Monitors funding rates across exchanges for market sentiment analysis
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from termcolor import cprint

from ai_agents.base_agent import BaseAIAgent, AgentResult


class FundingAgent(BaseAIAgent):
    """
    Monitors funding rates to gauge market sentiment

    Funding rates indicate long/short balance:
    - Positive funding = more longs (bullish sentiment)
    - Negative funding = more shorts (bearish sentiment)
    - Extreme rates = potential reversal signals
    """

    def __init__(self):
        super().__init__(
            name="FundingAgent",
            description="Monitors funding rates for market sentiment and reversal signals",
            complexity="complex"
        )

        # Thresholds for significant funding rates (annualized %)
        self.extreme_positive_threshold = 20.0  # Very bullish
        self.extreme_negative_threshold = -5.0  # Very bearish

    def execute(self,
                symbol: str = "BTC",
                exchange: str = "binance",
                lookback_hours: int = 24) -> AgentResult:
        """
        Analyze funding rates for a symbol

        Args:
            symbol: Trading symbol (e.g., "BTC", "ETH")
            exchange: Exchange name
            lookback_hours: Hours of history to analyze

        Returns:
            AgentResult with funding analysis
        """
        self.log_info(f"Analyzing funding rates for {symbol} on {exchange}")

        try:
            # Get funding rate data
            funding_data = self._get_funding_data(symbol, exchange, lookback_hours)

            if not funding_data:
                return self.create_result(
                    success=False,
                    errors=["Failed to fetch funding rate data"]
                )

            # Analyze funding rates
            analysis = self._analyze_funding_rates(funding_data)

            # Generate AI signal
            ai_signal = self._generate_ai_signal(symbol, funding_data, analysis)

            self.log_success(f"Funding analysis complete: {ai_signal['signal']}")

            return self.create_result(
                success=True,
                data={
                    "symbol": symbol,
                    "exchange": exchange,
                    "current_rate": analysis["current_rate"],
                    "avg_rate_24h": analysis["avg_rate_24h"],
                    "trend": analysis["trend"],
                    "signal": ai_signal["signal"],
                    "reasoning": ai_signal["reasoning"],
                    "extremity": analysis["extremity"]
                },
                ai_response=ai_signal.get("ai_response"),
                confidence=ai_signal.get("confidence", 0.0)
            )

        except Exception as e:
            self.log_error(f"Error executing funding agent: {e}")
            return self.create_result(
                success=False,
                errors=[str(e)]
            )

    def _get_funding_data(self,
                         symbol: str,
                         exchange: str,
                         lookback_hours: int) -> Optional[Dict[str, Any]]:
        """
        Get funding rate data from exchange

        For now, returns simulated data.
        In production, integrate with actual exchange APIs.
        """
        # TODO: Integrate with real exchange APIs
        # This is a placeholder that returns simulated funding data

        current_time = datetime.now()
        funding_history = []

        # Simulate 24 hours of 8-hourly funding rates (3 periods per day)
        for i in range(lookback_hours // 8):
            timestamp = current_time - timedelta(hours=i*8)
            # Simulate funding rate (typically -0.01% to +0.01% per 8h)
            rate = 0.0001 * (i % 3 - 1)  # Oscillates between -0.01%, 0%, +0.01%
            funding_history.append({
                "timestamp": timestamp,
                "rate": rate,
                "rate_annualized": rate * 365 * 3  # Convert 8h rate to annual %
            })

        current_rate = funding_history[0]["rate_annualized"]

        self.log_warning("Using simulated funding data. Integrate real exchange API for production.")

        return {
            "current_rate": current_rate,
            "history": funding_history
        }

    def _analyze_funding_rates(self, funding_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze funding rate patterns

        Args:
            funding_data: Funding rate data

        Returns:
            Analysis results
        """
        history = funding_data["history"]
        current_rate = funding_data["current_rate"]

        # Calculate average rate
        rates = [h["rate_annualized"] for h in history]
        avg_rate = sum(rates) / len(rates) if rates else 0

        # Determine trend
        if len(rates) >= 2:
            recent_avg = sum(rates[:3]) / 3 if len(rates) >= 3 else rates[0]
            older_avg = sum(rates[-3:]) / 3 if len(rates) >= 3 else rates[-1]
            trend = "increasing" if recent_avg > older_avg else "decreasing"
        else:
            trend = "neutral"

        # Check extremity
        extremity = "neutral"
        if current_rate >= self.extreme_positive_threshold:
            extremity = "extreme_positive"
        elif current_rate <= self.extreme_negative_threshold:
            extremity = "extreme_negative"
        elif abs(current_rate) < 5:
            extremity = "neutral"
        else:
            extremity = "moderate"

        return {
            "current_rate": current_rate,
            "avg_rate_24h": avg_rate,
            "trend": trend,
            "extremity": extremity,
            "rates_history": rates
        }

    def _generate_ai_signal(self,
                           symbol: str,
                           funding_data: Dict[str, Any],
                           analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate trading signal using AI

        Args:
            symbol: Trading symbol
            funding_data: Raw funding data
            analysis: Funding rate analysis

        Returns:
            Dict with signal, reasoning, and confidence
        """
        # Prepare prompt for AI
        prompt = f"""Analyze funding rates for {symbol}:

Current Rate: {analysis['current_rate']:.2f}% (annualized)
24h Average: {analysis['avg_rate_24h']:.2f}%
Trend: {analysis['trend']}
Extremity: {analysis['extremity']}

Funding Rate Interpretation:
- Extreme positive funding (>{self.extreme_positive_threshold}%): Overcrowded longs, possible reversal down
- Extreme negative funding (<{self.extreme_negative_threshold}%): Overcrowded shorts, possible squeeze up
- Moderate funding: Normal market conditions

Based on this funding data, provide:
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
            # Fallback to rule-based signal
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
            analysis: Funding rate analysis

        Returns:
            Dict with signal, reasoning, and confidence
        """
        current_rate = analysis["current_rate"]
        extremity = analysis["extremity"]

        if extremity == "extreme_negative":
            return {
                "signal": "BUY",
                "reasoning": f"Extreme negative funding ({current_rate:.2f}%) indicates overcrowded shorts - potential squeeze",
                "confidence": 0.7
            }
        elif extremity == "extreme_positive":
            return {
                "signal": "SELL",
                "reasoning": f"Extreme positive funding ({current_rate:.2f}%) indicates overcrowded longs - potential reversal",
                "confidence": 0.7
            }
        else:
            return {
                "signal": "HOLD",
                "reasoning": "Funding rates in normal range - no clear contrarian signal",
                "confidence": 0.5
            }


# Standalone execution
if __name__ == "__main__":
    print("Testing Funding Agent...\n")

    agent = FundingAgent()
    result = agent.execute(symbol="BTC", exchange="binance")

    print("\n" + "="*60)
    print("FUNDING AGENT RESULT")
    print("="*60)
    print(f"Success: {result.success}")
    print(f"Symbol: {result.data.get('symbol')}")
    print(f"Current Rate: {result.data.get('current_rate', 0):.2f}%")
    print(f"Signal: {result.data.get('signal')}")
    print(f"Reasoning: {result.data.get('reasoning')}")
    print(f"Confidence: {result.confidence:.1%}")
    print("="*60)
