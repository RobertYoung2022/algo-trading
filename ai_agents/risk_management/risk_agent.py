"""
Risk Management Agent
AI-powered risk assessment and position validation
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from termcolor import cprint

from ai_agents.base_agent import BaseAIAgent, AgentResult
from ai_agents.utils.validators import validate_position_size, validate_risk_reward_ratio


class RiskAgent(BaseAIAgent):
    """
    AI-powered risk management agent

    Validates:
    - Position sizing
    - Risk/reward ratios
    - Portfolio exposure
    - Market conditions
    """

    def __init__(self,
                 max_position_pct: float = 0.2,
                 max_loss_usd: float = 500.0,
                 min_balance: float = 100.0,
                 min_rr_ratio: float = 1.5):
        """
        Initialize Risk Agent

        Args:
            max_position_pct: Max position as % of balance
            max_loss_usd: Max loss allowed before circuit breaker
            min_balance: Minimum account balance
            min_rr_ratio: Minimum risk/reward ratio
        """
        super().__init__(
            name="RiskAgent",
            description="AI-powered risk assessment and position validation",
            complexity="complex"
        )

        self.max_position_pct = max_position_pct
        self.max_loss_usd = max_loss_usd
        self.min_balance = min_balance
        self.min_rr_ratio = min_rr_ratio

        # Track daily stats for circuit breakers
        self.daily_pnl = 0.0
        self.trades_today = 0
        self.last_reset = datetime.now().date()

    def execute(self,
                position_data: Dict[str, Any],
                account_balance: float,
                portfolio: Dict[str, Any] = None) -> AgentResult:
        """
        Assess risk for a proposed trade

        Args:
            position_data: Trade details (symbol, size, entry, stop, target)
            account_balance: Current account balance
            portfolio: Current portfolio positions

        Returns:
            AgentResult with risk assessment
        """
        self.log_info(f"Assessing risk for {position_data.get('symbol', 'UNKNOWN')}")

        try:
            # Reset daily stats if new day
            self._check_daily_reset()

            # Validate inputs
            validation_errors = self._validate_inputs(position_data)
            if validation_errors:
                return self.create_result(
                    success=False,
                    errors=validation_errors
                )

            # Run risk checks
            risk_checks = self._run_risk_checks(position_data, account_balance, portfolio)

            # Generate AI risk assessment
            ai_assessment = self._generate_ai_assessment(position_data, account_balance, risk_checks)

            # Determine final decision
            decision = self._make_decision(risk_checks, ai_assessment)

            if decision["approved"]:
                self.log_success(f"Trade approved: {decision['reasoning']}")
            else:
                self.log_warning(f"Trade rejected: {decision['reasoning']}")

            return self.create_result(
                success=True,
                data={
                    "approved": decision["approved"],
                    "reasoning": decision["reasoning"],
                    "risk_checks": risk_checks,
                    "suggested_position_size": risk_checks.get("suggested_position_size"),
                    "risk_score": decision["risk_score"]
                },
                ai_response=ai_assessment.get("ai_response"),
                confidence=ai_assessment.get("confidence", 0.0)
            )

        except Exception as e:
            self.log_error(f"Error assessing risk: {e}")
            return self.create_result(
                success=False,
                errors=[str(e)]
            )

    def _check_daily_reset(self):
        """Reset daily statistics if new day"""
        today = datetime.now().date()
        if today > self.last_reset:
            self.daily_pnl = 0.0
            self.trades_today = 0
            self.last_reset = today
            self.log_info("Daily risk stats reset")

    def _validate_inputs(self, position_data: Dict[str, Any]) -> List[str]:
        """Validate required inputs"""
        errors = []

        required_fields = ["symbol", "position_size", "entry_price"]
        for field in required_fields:
            if field not in position_data:
                errors.append(f"Missing required field: {field}")

        return errors

    def _run_risk_checks(self,
                        position_data: Dict[str, Any],
                        account_balance: float,
                        portfolio: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run comprehensive risk checks

        Args:
            position_data: Trade details
            account_balance: Current balance
            portfolio: Current positions

        Returns:
            Dict with risk check results
        """
        checks = {}

        # 1. Position size validation
        position_size = position_data["position_size"]
        size_valid, size_errors = validate_position_size(
            position_size,
            account_balance,
            self.max_position_pct,
            self.min_balance
        )
        checks["position_size_valid"] = size_valid
        checks["position_size_errors"] = size_errors

        # Calculate suggested position size
        max_position = account_balance * self.max_position_pct
        checks["suggested_position_size"] = min(position_size, max_position)

        # 2. Risk/reward ratio (if stop loss and take profit provided)
        if "stop_loss" in position_data and "take_profit" in position_data:
            rr_valid, rr_errors = validate_risk_reward_ratio(
                position_data["entry_price"],
                position_data["stop_loss"],
                position_data["take_profit"],
                self.min_rr_ratio
            )
            checks["rr_ratio_valid"] = rr_valid
            checks["rr_ratio_errors"] = rr_errors

            # Calculate actual R/R
            risk = abs(position_data["entry_price"] - position_data["stop_loss"])
            reward = abs(position_data["take_profit"] - position_data["entry_price"])
            checks["actual_rr_ratio"] = reward / risk if risk > 0 else 0
        else:
            checks["rr_ratio_valid"] = None
            checks["rr_ratio_errors"] = ["Stop loss or take profit not provided"]

        # 3. Circuit breaker check
        circuit_breaker_triggered = False
        if self.daily_pnl <= -self.max_loss_usd:
            circuit_breaker_triggered = True
            checks["circuit_breaker"] = f"Max daily loss reached: ${self.daily_pnl:.2f}"
        else:
            checks["circuit_breaker"] = None

        checks["circuit_breaker_triggered"] = circuit_breaker_triggered

        # 4. Portfolio exposure (if portfolio provided)
        if portfolio:
            total_exposure = sum(pos.get("value", 0) for pos in portfolio.values())
            checks["total_exposure"] = total_exposure
            checks["exposure_after_trade"] = total_exposure + position_size
            checks["exposure_pct"] = (total_exposure + position_size) / account_balance
        else:
            checks["total_exposure"] = 0
            checks["exposure_after_trade"] = position_size
            checks["exposure_pct"] = position_size / account_balance

        return checks

    def _generate_ai_assessment(self,
                                position_data: Dict[str, Any],
                                account_balance: float,
                                risk_checks: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate AI risk assessment

        Args:
            position_data: Trade details
            account_balance: Current balance
            risk_checks: Results from risk checks

        Returns:
            Dict with AI assessment
        """
        prompt = f"""Assess the risk for this trading position:

Symbol: {position_data['symbol']}
Position Size: ${position_data['position_size']:.2f}
Account Balance: ${account_balance:.2f}
Position as % of Balance: {(position_data['position_size']/account_balance)*100:.1f}%

Entry: ${position_data.get('entry_price', 0):.2f}
Stop Loss: ${position_data.get('stop_loss', 'Not set')}
Take Profit: ${position_data.get('take_profit', 'Not set')}

Risk Checks:
- Position size valid: {risk_checks['position_size_valid']}
- Suggested position size: ${risk_checks['suggested_position_size']:.2f}
- R/R ratio valid: {risk_checks.get('rr_ratio_valid', 'N/A')}
- Actual R/R: {risk_checks.get('actual_rr_ratio', 'N/A')}
- Circuit breaker: {risk_checks['circuit_breaker'] or 'OK'}
- Portfolio exposure: {risk_checks['exposure_pct']*100:.1f}%

Based on this risk analysis, provide:
1. Recommendation (APPROVE/REJECT/MODIFY)
2. Key risk factors (1-2 sentences)
3. Confidence in assessment (0-100)

Format your response as:
RECOMMENDATION: [APPROVE/REJECT/MODIFY]
REASONING: [your reasoning]
CONFIDENCE: [0-100]"""

        # Generate AI response
        ai_response = self.generate_ai_response(
            prompt=prompt,
            temperature=0.2  # Lower temperature for risk assessment
        )

        if not ai_response:
            return self._rule_based_assessment(risk_checks)

        # Parse AI response
        try:
            content = ai_response.content
            recommendation = "REJECT"
            reasoning = "Unable to parse AI response"
            confidence = 50.0

            if "RECOMMENDATION:" in content:
                recommendation = content.split("RECOMMENDATION:")[1].split("\n")[0].strip()
            if "REASONING:" in content:
                reasoning = content.split("REASONING:")[1].split("\n")[0].strip()
            if "CONFIDENCE:" in content:
                conf_str = content.split("CONFIDENCE:")[1].split("\n")[0].strip()
                confidence = float(conf_str)

            return {
                "recommendation": recommendation,
                "reasoning": reasoning,
                "confidence": confidence / 100.0,
                "ai_response": ai_response
            }

        except Exception as e:
            self.log_warning(f"Failed to parse AI response: {e}")
            return self._rule_based_assessment(risk_checks)

    def _rule_based_assessment(self, risk_checks: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fallback rule-based risk assessment

        Args:
            risk_checks: Risk check results

        Returns:
            Dict with assessment
        """
        # Circuit breaker takes priority
        if risk_checks["circuit_breaker_triggered"]:
            return {
                "recommendation": "REJECT",
                "reasoning": risk_checks["circuit_breaker"],
                "confidence": 1.0
            }

        # Position size check
        if not risk_checks["position_size_valid"]:
            return {
                "recommendation": "MODIFY",
                "reasoning": f"Position size too large. Reduce to ${risk_checks['suggested_position_size']:.2f}",
                "confidence": 0.9
            }

        # R/R ratio check
        if risk_checks.get("rr_ratio_valid") is False:
            return {
                "recommendation": "REJECT",
                "reasoning": "Risk/reward ratio too low",
                "confidence": 0.85
            }

        # Exposure check
        if risk_checks["exposure_pct"] > 0.8:  # 80% max exposure
            return {
                "recommendation": "REJECT",
                "reasoning": "Portfolio exposure too high after this trade",
                "confidence": 0.9
            }

        # All checks passed
        return {
            "recommendation": "APPROVE",
            "reasoning": "All risk checks passed",
            "confidence": 0.85
        }

    def _make_decision(self,
                      risk_checks: Dict[str, Any],
                      ai_assessment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make final risk decision

        Args:
            risk_checks: Risk check results
            ai_assessment: AI assessment

        Returns:
            Dict with decision
        """
        recommendation = ai_assessment["recommendation"]
        reasoning = ai_assessment["reasoning"]
        confidence = ai_assessment["confidence"]

        # Calculate overall risk score (0-100, lower is better)
        risk_score = 0

        if not risk_checks["position_size_valid"]:
            risk_score += 30
        if risk_checks.get("rr_ratio_valid") is False:
            risk_score += 25
        if risk_checks["circuit_breaker_triggered"]:
            risk_score += 100  # Auto-reject
        if risk_checks["exposure_pct"] > 0.5:
            risk_score += 20

        # Approved if recommendation is APPROVE and risk score < 50
        approved = recommendation == "APPROVE" and risk_score < 50 and not risk_checks["circuit_breaker_triggered"]

        return {
            "approved": approved,
            "reasoning": reasoning,
            "risk_score": min(risk_score, 100),
            "confidence": confidence
        }

    def update_daily_pnl(self, pnl: float):
        """Update daily PnL for circuit breaker tracking"""
        self.daily_pnl += pnl
        self.trades_today += 1
        self.log_info(f"Daily PnL updated: ${self.daily_pnl:.2f} ({self.trades_today} trades)")


# Standalone execution
if __name__ == "__main__":
    print("Testing Risk Agent...\n")

    agent = RiskAgent(
        max_position_pct=0.2,
        max_loss_usd=500.0,
        min_balance=100.0
    )

    # Test trade
    position = {
        "symbol": "BTC",
        "position_size": 500.0,
        "entry_price": 50000.0,
        "stop_loss": 49000.0,
        "take_profit": 52000.0
    }

    result = agent.execute(
        position_data=position,
        account_balance=5000.0
    )

    print("\n" + "="*60)
    print("RISK AGENT RESULT")
    print("="*60)
    print(f"Success: {result.success}")
    print(f"Approved: {result.data.get('approved')}")
    print(f"Risk Score: {result.data.get('risk_score')}/100")
    print(f"Reasoning: {result.data.get('reasoning')}")
    print(f"Confidence: {result.confidence:.1%}")
    if not result.data.get('approved'):
        print(f"Suggested Position: ${result.data.get('suggested_position_size', 0):.2f}")
    print("="*60)
