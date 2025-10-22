"""
RBI Agent (Research-Backtest-Implement)
AI-powered strategy generation from research, videos, and market analysis
"""

from typing import Dict, Any, Optional
from pathlib import Path
from termcolor import cprint

from ai_agents.base_agent import BaseAIAgent, AgentResult


class RBIAgent(BaseAIAgent):
    """
    Research-Backtest-Implement Agent

    Generates trading strategies from:
    - Market research
    - YouTube trading videos
    - PDF research papers
    - Trading ideas

    Workflow:
    1. Research: Extract strategy logic from source
    2. Backtest: Generate and test strategy code
    3. Implement: Deploy if performance meets criteria
    """

    def __init__(self, min_quality_score: float = 75.0):
        """
        Initialize RBI Agent

        Args:
            min_quality_score: Minimum quality score (0-100) to approve strategy
        """
        super().__init__(
            name="RBIAgent",
            description="AI-powered strategy generation from research sources",
            complexity="complex"
        )

        self.min_quality_score = min_quality_score

    def execute(self,
                source_type: str,
                source_content: str,
                symbol: str = "BTC") -> AgentResult:
        """
        Generate strategy from research source

        Args:
            source_type: Type of source (text, youtube, pdf, idea)
            source_content: Source content or URL
            symbol: Trading symbol for backtesting

        Returns:
            AgentResult with generated strategy
        """
        self.log_info(f"Generating strategy from {source_type} source")

        try:
            # Phase 1: Research - Extract strategy logic
            strategy_concept = self._research_phase(source_type, source_content)

            if not strategy_concept:
                return self.create_result(
                    success=False,
                    errors=["Failed to extract strategy concept from source"]
                )

            # Phase 2: Validate concept
            validation = self._validate_concept(strategy_concept)

            # Phase 3: Generate strategy code (simulated for now)
            strategy_code = self._generate_strategy_code(strategy_concept, symbol)

            # Phase 4: Quality assessment
            quality_score = self._assess_quality(strategy_concept, validation)

            approved = quality_score >= self.min_quality_score

            if approved:
                self.log_success(f"Strategy approved with score {quality_score:.1f}/100")
            else:
                self.log_warning(f"Strategy rejected with score {quality_score:.1f}/100")

            return self.create_result(
                success=True,
                data={
                    "strategy_name": strategy_concept.get("name"),
                    "strategy_description": strategy_concept.get("description"),
                    "quality_score": quality_score,
                    "approved": approved,
                    "validation_results": validation,
                    "strategy_code_preview": strategy_code[:500] if strategy_code else None
                },
                ai_response=strategy_concept.get("ai_response"),
                confidence=quality_score / 100.0
            )

        except Exception as e:
            self.log_error(f"Error generating strategy: {e}")
            return self.create_result(
                success=False,
                errors=[str(e)]
            )

    def _research_phase(self,
                       source_type: str,
                       source_content: str) -> Optional[Dict[str, Any]]:
        """
        Phase 1: Extract strategy logic from source

        Args:
            source_type: Source type
            source_content: Source content

        Returns:
            Dict with strategy concept
        """
        prompt = f"""Extract a trading strategy from the following {source_type}:

{source_content}

Analyze the content and extract:
1. Strategy Name
2. Strategy Description
3. Entry Conditions
4. Exit Conditions
5. Risk Management Rules
6. Timeframe
7. Indicators Used

Format your response as:
NAME: [strategy name]
DESCRIPTION: [brief description]
ENTRY: [entry conditions]
EXIT: [exit conditions]
RISK_MANAGEMENT: [risk rules]
TIMEFRAME: [e.g., 15m, 1h, 4h]
INDICATORS: [list of indicators]"""

        ai_response = self.generate_ai_response(
            prompt=prompt,
            temperature=0.4
        )

        if not ai_response:
            return None

        # Parse AI response
        try:
            content = ai_response.content
            concept = {"ai_response": ai_response}

            for field in ["NAME", "DESCRIPTION", "ENTRY", "EXIT", "RISK_MANAGEMENT", "TIMEFRAME", "INDICATORS"]:
                if f"{field}:" in content:
                    value = content.split(f"{field}:")[1].split("\n")[0].strip()
                    concept[field.lower()] = value

            return concept

        except Exception as e:
            self.log_error(f"Failed to parse strategy concept: {e}")
            return None

    def _validate_concept(self, strategy_concept: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate strategy concept

        Args:
            strategy_concept: Extracted strategy concept

        Returns:
            Validation results
        """
        validation = {
            "has_entry_rules": bool(strategy_concept.get("entry")),
            "has_exit_rules": bool(strategy_concept.get("exit")),
            "has_risk_management": bool(strategy_concept.get("risk_management")),
            "has_clear_timeframe": bool(strategy_concept.get("timeframe")),
            "completeness_score": 0
        }

        # Calculate completeness
        required_fields = ["name", "description", "entry", "exit", "risk_management", "timeframe"]
        present_fields = sum(1 for field in required_fields if strategy_concept.get(field))
        validation["completeness_score"] = (present_fields / len(required_fields)) * 100

        return validation

    def _generate_strategy_code(self,
                                strategy_concept: Dict[str, Any],
                                symbol: str) -> Optional[str]:
        """
        Generate Python strategy code

        Args:
            strategy_concept: Strategy concept
            symbol: Trading symbol

        Returns:
            Python code for strategy
        """
        # For now, return a template
        # In production, use moon-dev-agents RBI to generate actual backtest code

        code_template = f'''"""
{strategy_concept.get("name", "Generated Strategy")}
Auto-generated by RBI Agent

Description: {strategy_concept.get("description", "N/A")}
"""

from strategies.base_strategy import BaseStrategy
import pandas as pd
import pandas_ta as ta


class {strategy_concept.get("name", "GeneratedStrategy").replace(" ", "")}(BaseStrategy):
    """
    {strategy_concept.get("description", "")}

    Entry: {strategy_concept.get("entry", "N/A")}
    Exit: {strategy_concept.get("exit", "N/A")}
    Timeframe: {strategy_concept.get("timeframe", "N/A")}
    """

    name = "{strategy_concept.get("name", "Generated")}"
    description = "{strategy_concept.get("description", "")}"

    def generate_signals(self, data: pd.DataFrame):
        """Generate trading signals"""
        # TODO: Implement based on strategy concept
        # Entry: {strategy_concept.get("entry", "")}
        # Exit: {strategy_concept.get("exit", "")}

        return {{"action": "HOLD", "confidence": 0.0}}
'''

        return code_template

    def _assess_quality(self,
                       strategy_concept: Dict[str, Any],
                       validation: Dict[str, Any]) -> float:
        """
        Assess strategy quality

        Args:
            strategy_concept: Strategy concept
            validation: Validation results

        Returns:
            Quality score (0-100)
        """
        score = 0.0

        # Completeness (40 points)
        score += validation["completeness_score"] * 0.4

        # Entry/Exit clarity (30 points)
        if validation["has_entry_rules"] and validation["has_exit_rules"]:
            score += 30

        # Risk management (20 points)
        if validation["has_risk_management"]:
            score += 20

        # Timeframe specified (10 points)
        if validation["has_clear_timeframe"]:
            score += 10

        return min(score, 100.0)


# Standalone execution
if __name__ == "__main__":
    print("Testing RBI Agent...\n")

    agent = RBIAgent(min_quality_score=75.0)

    # Test with a trading idea
    trading_idea = """
    RSI Divergence Strategy

    This strategy looks for divergences between price and RSI indicator.

    Entry: When price makes a lower low but RSI makes a higher low (bullish divergence), enter long.
    Exit: Exit when RSI crosses above 70 (overbought) or price breaks below recent swing low.
    Risk Management: Use 2% stop loss below entry, target 1:2 risk/reward ratio.
    Timeframe: 1-hour charts
    Indicators: RSI(14), Price action swing points
    """

    result = agent.execute(
        source_type="idea",
        source_content=trading_idea,
        symbol="BTC"
    )

    print("\n" + "="*60)
    print("RBI AGENT RESULT")
    print("="*60)
    print(f"Success: {result.success}")
    print(f"Strategy Name: {result.data.get('strategy_name')}")
    print(f"Quality Score: {result.data.get('quality_score', 0):.1f}/100")
    print(f"Approved: {result.data.get('approved')}")
    print(f"Description: {result.data.get('strategy_description')}")
    print("="*60)
