"""
Base AI Agent for Algo-Fun System
Foundation class for all AI agents integrating with moon-dev-agents
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json
from termcolor import cprint

from ai_model_factory import get_factory, AIResponse


@dataclass
class AgentResult:
    """Standardized result format for all AI agents"""
    success: bool
    agent_name: str
    timestamp: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)
    ai_response: Optional[AIResponse] = None
    confidence: float = 0.0
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return {
            "success": self.success,
            "agent_name": self.agent_name,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "confidence": self.confidence,
            "warnings": self.warnings,
            "errors": self.errors,
            "ai_model_used": self.ai_response.model_used if self.ai_response else None
        }

    def save(self, output_dir: Path):
        """Save result to JSON file"""
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self.agent_name}_{self.timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = output_dir / filename

        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

        cprint(f"💾 Saved {self.agent_name} result to {filepath}", "green")


class BaseAIAgent(ABC):
    """
    Base class for all AI agents in algo-fun system

    Provides:
    - Standard interface for agent execution
    - AI model integration via model factory
    - Result standardization
    - Logging and error handling
    """

    def __init__(self,
                 name: str,
                 description: str,
                 complexity: str = "complex"):
        """
        Initialize AI agent

        Args:
            name: Agent name
            description: Agent description
            complexity: AI task complexity (simple/complex)
        """
        self.name = name
        self.description = description
        self.complexity = complexity
        self.model_factory = get_factory()

        cprint(f"\n🤖 Initializing {self.name}", "cyan")
        cprint(f"   {self.description}", "cyan")

    @abstractmethod
    def execute(self, **kwargs) -> AgentResult:
        """
        Execute agent logic

        Args:
            **kwargs: Agent-specific parameters

        Returns:
            AgentResult with execution results
        """
        pass

    def generate_ai_response(self,
                            prompt: str,
                            system_prompt: Optional[str] = None,
                            temperature: Optional[float] = None) -> Optional[AIResponse]:
        """
        Generate AI response using model factory

        Args:
            prompt: User prompt
            system_prompt: System instructions (uses default if not provided)
            temperature: Response randomness

        Returns:
            AIResponse or None if failed
        """
        if not system_prompt:
            system_prompt = f"You are {self.name}, {self.description}"

        try:
            response = self.model_factory.generate_response(
                prompt=prompt,
                system_prompt=system_prompt,
                complexity=self.complexity,
                temperature=temperature
            )

            if response:
                cprint(f"✅ AI response generated successfully", "green")
            else:
                cprint(f"❌ Failed to generate AI response", "red")

            return response

        except Exception as e:
            cprint(f"❌ Error generating AI response: {e}", "red")
            return None

    def create_result(self,
                     success: bool,
                     data: Dict[str, Any] = None,
                     ai_response: Optional[AIResponse] = None,
                     confidence: float = 0.0,
                     warnings: List[str] = None,
                     errors: List[str] = None) -> AgentResult:
        """
        Create standardized agent result

        Args:
            success: Whether execution succeeded
            data: Result data
            ai_response: AI response if any
            confidence: Confidence score (0-1)
            warnings: Warning messages
            errors: Error messages

        Returns:
            AgentResult
        """
        return AgentResult(
            success=success,
            agent_name=self.name,
            data=data or {},
            ai_response=ai_response,
            confidence=confidence,
            warnings=warnings or [],
            errors=errors or []
        )

    def log_info(self, message: str):
        """Log info message"""
        cprint(f"ℹ️  [{self.name}] {message}", "cyan")

    def log_success(self, message: str):
        """Log success message"""
        cprint(f"✅ [{self.name}] {message}", "green")

    def log_warning(self, message: str):
        """Log warning message"""
        cprint(f"⚠️  [{self.name}] {message}", "yellow")

    def log_error(self, message: str):
        """Log error message"""
        cprint(f"❌ [{self.name}] {message}", "red")

    def __repr__(self) -> str:
        return f"<{self.name}: {self.description}>"
