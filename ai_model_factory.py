"""
AI Model Factory for Algo-Fun
Bridges algo-fun system with moon-dev-agents AI models

Uses DeepSeek for cost-effective operations, Claude for complex decisions
"""

import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any, Literal
from dataclasses import dataclass
from termcolor import cprint

# Add moon-dev-agents to Python path
MOON_DEV_PATH = Path(__file__).parent / "moon-dev-agents" / "src"
if str(MOON_DEV_PATH) not in sys.path:
    sys.path.insert(0, str(MOON_DEV_PATH))

# Import moon-dev-agents model factory
try:
    from models.model_factory import ModelFactory as MoonDevModelFactory
    from models.base_model import BaseModel as MoonDevBaseModel
    MOON_DEV_AVAILABLE = True
except ImportError as e:
    cprint(f"⚠️  Warning: Could not import moon-dev-agents models: {e}", "yellow")
    MOON_DEV_AVAILABLE = False
    # Define placeholder for type hints
    MoonDevBaseModel = Any

# Import our config
from ai_config import AIConfig


@dataclass
class AIResponse:
    """Standardized AI response for algo-fun system"""
    content: str
    model_used: str
    confidence: float = 0.0
    reasoning: str = ""
    raw_response: Any = None


class AlgoFunModelFactory:
    """
    Model Factory for Algo-Fun Trading System

    Intelligently routes requests to appropriate AI models:
    - DeepSeek for cost-effective operations (simple analysis, data parsing)
    - Claude for complex decisions (strategy analysis, risk assessment)
    """

    # Task complexity levels
    SIMPLE_TASKS = ["parse", "extract", "format", "validate"]
    COMPLEX_TASKS = ["analyze", "decide", "strategize", "risk", "predict"]

    def __init__(self, config: Optional[AIConfig] = None):
        """
        Initialize Model Factory

        Args:
            config: AI configuration (uses global config if not provided)
        """
        self.config = config or AIConfig()
        self.moon_dev_factory: Optional[MoonDevModelFactory] = None

        # Validate configuration
        validation = self.config.validate()
        if not validation['is_valid']:
            cprint("❌ Configuration validation failed:", "red")
            for error in validation['errors']:
                cprint(f"  - {error}", "red")
            raise ValueError("Invalid AI configuration")

        if validation['warnings']:
            cprint("⚠️  Configuration warnings:", "yellow")
            for warning in validation['warnings']:
                cprint(f"  - {warning}", "yellow")

        # Initialize moon-dev-agents if available
        if MOON_DEV_AVAILABLE:
            self._initialize_moon_dev()
        else:
            cprint("❌ Moon-dev-agents not available. AI features will be limited.", "red")

    def _initialize_moon_dev(self):
        """Initialize moon-dev-agents model factory"""
        try:
            # Set environment variables for moon-dev-agents
            if self.config.ai_models.anthropic_api_key:
                os.environ["ANTHROPIC_KEY"] = self.config.ai_models.anthropic_api_key

            if self.config.ai_models.deepseek_api_key:
                os.environ["DEEPSEEK_KEY"] = self.config.ai_models.deepseek_api_key

            # Initialize moon-dev factory
            cprint("\n🏭 Initializing Moon Dev Model Factory...", "cyan")
            self.moon_dev_factory = MoonDevModelFactory()
            cprint("✅ Moon Dev Model Factory initialized successfully", "green")

        except Exception as e:
            cprint(f"❌ Failed to initialize Moon Dev Model Factory: {e}", "red")
            self.moon_dev_factory = None

    def get_model(self,
                  complexity: Literal["simple", "medium", "complex", "critical"] = "complex",
                  model_type: Optional[str] = None,
                  position_size: float = 0) -> Optional[MoonDevBaseModel]:
        """
        Get appropriate AI model based on task complexity (3-tier routing)

        Args:
            complexity: Task complexity level
                - "simple": DeepSeek ($0.14/1M tokens)
                - "medium": Claude Haiku 4.5 ($0.80/1M tokens)
                - "complex" or "critical": Claude Sonnet 4.5 ($3/1M tokens)
            model_type: Specific model type to use (overrides complexity)
            position_size: USD size of position (auto-escalates to critical if >$5000)

        Returns:
            AI model instance or None if unavailable
        """
        if not self.moon_dev_factory:
            cprint("❌ Model factory not initialized", "red")
            return None

        # If specific model type requested, use that
        if model_type:
            return self.moon_dev_factory.get_model(model_type)

        # Auto-escalate large positions to critical
        if position_size > 5000:
            complexity = "critical"
            cprint(f"💰 Large position (${position_size:.0f}) - auto-escalating to critical tier", "yellow")

        # Map legacy "complex" to "critical" for backward compatibility
        if complexity == "complex":
            complexity = "critical"

        # 3-tier routing
        if complexity == "simple":
            # Tier 3: DeepSeek (cost-effective for simple tasks)
            model = self.moon_dev_factory.get_model("deepseek")
            if model:
                cprint("🚀 Tier 3: DeepSeek (simple task, cost-effective)", "cyan")
                return model
            else:
                cprint("⚠️  DeepSeek unavailable, upgrading to Haiku", "yellow")
                complexity = "medium"  # Fallback to medium

        if complexity == "medium":
            # Tier 2: Claude Haiku 4.5 (fast & capable for medium complexity)
            model = self.moon_dev_factory.get_model("claude-haiku")
            if model:
                cprint("⚡ Tier 2: Claude Haiku 4.5 (medium complexity, fast)", "cyan")
                return model
            else:
                cprint("⚠️  Haiku unavailable, upgrading to Sonnet", "yellow")
                complexity = "critical"  # Fallback to critical

        if complexity == "critical":
            # Tier 1: Claude Sonnet 4.5 (best reasoning for critical decisions)
            model = self.moon_dev_factory.get_model("claude")
            if model:
                cprint("🧠 Tier 1: Claude Sonnet 4.5 (critical decision, best reasoning)", "cyan")
                return model
            else:
                cprint("⚠️  Sonnet unavailable, falling back to DeepSeek", "yellow")
                return self.moon_dev_factory.get_model("deepseek")

    def generate_response(self,
                         prompt: str,
                         system_prompt: str = "You are a helpful trading assistant.",
                         complexity: Literal["simple", "medium", "complex", "critical"] = "complex",
                         temperature: float = None,
                         max_tokens: int = None,
                         position_size: float = 0) -> Optional[AIResponse]:
        """
        Generate AI response with automatic 3-tier model selection

        Args:
            prompt: User prompt/question
            system_prompt: System instructions
            complexity: Task complexity (simple/medium/complex/critical)
            temperature: Response randomness (0.0-1.0)
            max_tokens: Maximum response length
            position_size: USD position size (auto-escalates to critical if >$5000)

        Returns:
            AIResponse or None if generation failed
        """
        # Get appropriate model based on 3-tier routing
        model = self.get_model(complexity=complexity, position_size=position_size)
        if not model:
            cprint("❌ No AI models available", "red")
            return None

        # Use config defaults if not specified
        if temperature is None:
            temperature = self.config.ai_models.temperature
        if max_tokens is None:
            max_tokens = self.config.ai_models.max_tokens

        try:
            cprint(f"\n💭 Generating response with {model.model_name}...", "cyan")

            # Generate response using moon-dev model
            response = model.generate_response(
                system_prompt=system_prompt,
                user_content=prompt,
                temperature=temperature,
                max_tokens=max_tokens
            )

            if not response:
                cprint("❌ Model returned empty response", "red")
                return None

            # Extract content based on response type
            if hasattr(response, 'content'):
                # Anthropic/Claude response
                content = response.content[0].text if isinstance(response.content, list) else response.content
            elif hasattr(response, 'message'):
                # OpenAI/DeepSeek response
                content = response.message.content if hasattr(response.message, 'content') else str(response.message)
            else:
                # Fallback
                content = str(response)

            cprint("✅ Response generated successfully", "green")

            return AIResponse(
                content=content,
                model_used=model.model_name,
                raw_response=response
            )

        except Exception as e:
            cprint(f"❌ Error generating response: {e}", "red")
            import traceback
            if self.config.system.verbose_ai_output:
                cprint(traceback.format_exc(), "red")
            return None

    def analyze_market_data(self, data: Dict[str, Any]) -> Optional[AIResponse]:
        """
        Analyze market data using AI

        Args:
            data: Market data dictionary

        Returns:
            AIResponse with market analysis
        """
        prompt = f"""Analyze the following market data and provide insights:

{data}

Provide:
1. Key observations
2. Market sentiment (bullish/bearish/neutral)
3. Trading recommendations
4. Risk factors

Keep the response concise and actionable."""

        return self.generate_response(
            prompt=prompt,
            system_prompt="You are an expert quantitative trading analyst.",
            complexity="complex"  # Market analysis requires complex reasoning
        )

    def validate_strategy(self, strategy_description: str, backtest_results: Dict[str, Any]) -> Optional[AIResponse]:
        """
        Validate a trading strategy using AI

        Args:
            strategy_description: Description of the strategy
            backtest_results: Backtest performance metrics

        Returns:
            AIResponse with strategy validation
        """
        prompt = f"""Validate the following trading strategy:

Strategy: {strategy_description}

Backtest Results:
{backtest_results}

Evaluate:
1. Statistical significance of results
2. Robustness of the strategy
3. Overfitting risks
4. Recommended improvements
5. Overall quality score (0-100)

Provide a concise assessment."""

        return self.generate_response(
            prompt=prompt,
            system_prompt="You are an expert quantitative strategy validator with deep knowledge of statistical analysis and overfitting prevention.",
            complexity="complex"
        )

    def assess_risk(self, position_data: Dict[str, Any]) -> Optional[AIResponse]:
        """
        Assess risk for a trading position

        Args:
            position_data: Position details (size, entry, stop loss, etc.)

        Returns:
            AIResponse with risk assessment
        """
        prompt = f"""Assess the risk for this trading position:

{position_data}

Evaluate:
1. Position sizing appropriateness
2. Risk/reward ratio
3. Stop loss placement
4. Correlation risks
5. Recommendation (approve/modify/reject)

Provide brief, actionable risk assessment."""

        return self.generate_response(
            prompt=prompt,
            system_prompt="You are a risk management specialist focused on capital preservation.",
            complexity="complex"
        )

    def is_available(self) -> bool:
        """Check if AI models are available"""
        return self.moon_dev_factory is not None and len(self.moon_dev_factory._models) > 0

    def get_available_models(self) -> Dict[str, str]:
        """Get list of available models"""
        if not self.moon_dev_factory:
            return {}

        return {
            model_type: model.model_name
            for model_type, model in self.moon_dev_factory._models.items()
        }

    def print_status(self):
        """Print model factory status"""
        print("\n" + "="*60)
        print("🤖 AI MODEL FACTORY STATUS")
        print("="*60)

        if self.is_available():
            print("\n✅ Model Factory: OPERATIONAL")

            available = self.get_available_models()
            print(f"\n📊 Available Models ({len(available)}):")
            for model_type, model_name in available.items():
                print(f"  ├─ {model_type}: {model_name}")

            print("\n🎯 Routing Strategy:")
            print("  ├─ Simple tasks → DeepSeek (cost-effective)")
            print("  └─ Complex tasks → Claude (advanced reasoning)")
        else:
            print("\n❌ Model Factory: UNAVAILABLE")
            print("   Please check API keys in .env.ai")

        print("\n" + "="*60)


# Global factory instance (lazy initialization)
_factory_instance: Optional[AlgoFunModelFactory] = None


def get_factory() -> AlgoFunModelFactory:
    """
    Get or create global model factory instance

    Returns:
        Singleton AlgoFunModelFactory instance
    """
    global _factory_instance
    if _factory_instance is None:
        _factory_instance = AlgoFunModelFactory()
    return _factory_instance


if __name__ == "__main__":
    # Test model factory
    print("Testing AI Model Factory...\n")

    factory = AlgoFunModelFactory()
    factory.print_status()

    if factory.is_available():
        print("\n🧪 Testing AI Response Generation...")

        # Test simple response
        simple_response = factory.generate_response(
            prompt="What is 2+2?",
            complexity="simple"
        )

        if simple_response:
            print(f"\n✅ Simple Response:")
            print(f"   Model: {simple_response.model_used}")
            print(f"   Content: {simple_response.content[:100]}...")

        # Test complex response
        complex_response = factory.generate_response(
            prompt="Analyze the risk-reward profile of a mean reversion strategy in volatile markets.",
            complexity="complex"
        )

        if complex_response:
            print(f"\n✅ Complex Response:")
            print(f"   Model: {complex_response.model_used}")
            print(f"   Content: {complex_response.content[:200]}...")
    else:
        print("\n⚠️  Model factory not available. Check API keys.")
