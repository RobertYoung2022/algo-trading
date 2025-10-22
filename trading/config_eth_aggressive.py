"""
ETH Multi-Strategy Rotation - Aggressive Configuration
========================================================
Optimized for $1,000 ETH capital, 1-2 month validation period

Risk Profile: AGGRESSIVE
- 90% position sizing
- 3x leverage on HyperLiquid
- 3% stop loss, 9% take profit (3:1 R:R)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env.ai"
if env_path.exists():
    load_dotenv(env_path)

# ============================================================================
# CAPITAL & RISK MANAGEMENT
# ============================================================================

# Starting capital (in USD equivalent)
STARTING_CAPITAL = 1000.0  # $1,000 in ETH

# Position sizing (aggressive)
POSITION_SIZE_PCT = 0.90  # Use 90% of available capital per trade
MAX_POSITION_SIZE_USD = STARTING_CAPITAL * POSITION_SIZE_PCT  # $900

# Leverage settings (HyperLiquid)
LEVERAGE = 3  # 3x leverage

# Risk per trade
STOP_LOSS_PCT = 0.03  # 3% stop loss
TAKE_PROFIT_PCT = 0.09  # 9% take profit (3:1 reward:risk)
MAX_LOSS_PER_TRADE_PCT = 0.05  # Maximum 5% account loss per trade

# Portfolio limits
MINIMUM_BALANCE_USD = 100.0  # Stop trading if balance drops below $100
MAX_DRAWDOWN_PCT = 0.25  # Stop trading if 25% drawdown from peak

# ============================================================================
# TRADING PARAMETERS
# ============================================================================

# Assets to trade
SYMBOLS = ["ETH"]  # ETH only for this validation period

# Trading frequency
CHECK_INTERVAL_MINUTES = 15  # Check for opportunities every 15 minutes

# Minimum confidence threshold
MIN_CONFIDENCE_THRESHOLD = 65  # Require 65% confidence to take trade

# Strategy selection
ENABLE_RSI_STRATEGY = True  # RSI Mean Reversion
ENABLE_SMA_STRATEGY = True  # SMA Crossover
ENABLE_BREAKOUT_STRATEGY = True  # Breakout Momentum

# ============================================================================
# STRATEGY-SPECIFIC SETTINGS
# ============================================================================

# RSI Mean Reversion (1h timeframe)
RSI_CONFIG = {
    "timeframe": "1h",
    "lookback_bars": 100,
    "rsi_period": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
    "ai_tier": "simple",  # Use Tier 3 (DeepSeek) for cost efficiency
}

# SMA Crossover (4h timeframe)
SMA_CONFIG = {
    "timeframe": "4h",
    "lookback_bars": 100,
    "fast_period": 10,
    "slow_period": 30,
    "ai_tier": "medium",  # Use Tier 2 (Haiku 4.5)
}

# Breakout Momentum (1d timeframe)
BREAKOUT_CONFIG = {
    "timeframe": "1d",
    "lookback_bars": 100,
    "resistance_lookback_days": 20,
    "volume_threshold": 1.5,  # Volume must be 1.5x average
    "ai_tier": "medium",  # Use Tier 2 (Haiku 4.5)
}

# ============================================================================
# AI ENHANCEMENT SETTINGS
# ============================================================================

# AI model tiers (from ai_config.py)
USE_AI_VALIDATION = True  # Enable AI-enhanced decision making

# Market intelligence agents
USE_FUNDING_AGENT = True  # Monitor funding rates
USE_LIQUIDATION_AGENT = True  # Track liquidation cascades
USE_WHALE_AGENT = True  # Watch whale movements

# Risk validation
USE_RISK_AGENT = True  # AI risk validation before execution
AUTO_ESCALATE_LARGE_POSITIONS = True  # Auto-escalate to Tier 1 if position >$5000

# ============================================================================
# HYPERLIQUID SETTINGS
# ============================================================================

# HyperLiquid authentication (uses ETH private key)
HYPERLIQUID_SECRET_KEY = os.getenv("PH_SECRET_KEY")

# HyperLiquid API endpoints
HYPERLIQUID_API_URL = "https://api.hyperliquid.xyz"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"

# Order settings
ORDER_TYPE = "limit"  # Use limit orders for better fill prices
SLIPPAGE_TOLERANCE_PCT = 0.001  # 0.1% slippage tolerance

# ============================================================================
# PERFORMANCE TRACKING
# ============================================================================

# Logging
TRADES_LOG_PATH = Path(__file__).parent / "logs" / "trades_log.csv"
REPORTS_PATH = Path(__file__).parent / "reports"

# Performance calculation
CALCULATE_SHARPE_RATIO = True
CALCULATE_MAX_DRAWDOWN = True
GENERATE_DAILY_REPORTS = True
GENERATE_WEEKLY_REPORTS = True

# Validation criteria (1-2 month goals)
VALIDATION_GOALS = {
    "min_return_pct": 10.0,  # Minimum 10% total return
    "min_win_rate_pct": 55.0,  # Minimum 55% win rate
    "min_profitable_weeks": 3,  # At least 3 out of 4 weeks profitable
    "max_single_loss_pct": 5.0,  # No single loss >5%
    "min_ai_advantage_pct": 5.0,  # AI should add >5% performance
}

# ============================================================================
# SAFETY FEATURES
# ============================================================================

# Circuit breakers
ENABLE_CIRCUIT_BREAKER = True
MAX_CONSECUTIVE_LOSSES = 3  # Stop after 3 consecutive losses
MAX_DAILY_LOSSES = 2  # Max 2 losing trades per day
COOLDOWN_AFTER_LOSS_MINUTES = 60  # Wait 1 hour after a loss

# Paper trading mode
PAPER_TRADING_MODE = os.getenv("PAPER_TRADING", "true").lower() == "true"

# Emergency stops
EMERGENCY_STOP_FILE = Path(__file__).parent / "EMERGENCY_STOP"
CHECK_EMERGENCY_STOP = True

# ============================================================================
# NOTIFICATIONS (Optional)
# ============================================================================

# Enable notifications for trade execution
ENABLE_NOTIFICATIONS = False
NOTIFICATION_METHOD = "console"  # console, email, telegram, discord

# ============================================================================
# VALIDATION HELPERS
# ============================================================================

def validate_config():
    """Validate configuration settings"""
    errors = []
    warnings = []

    # Check capital settings
    if STARTING_CAPITAL <= 0:
        errors.append("STARTING_CAPITAL must be positive")

    if POSITION_SIZE_PCT <= 0 or POSITION_SIZE_PCT > 1:
        errors.append("POSITION_SIZE_PCT must be between 0 and 1")

    # Check risk settings
    if STOP_LOSS_PCT >= TAKE_PROFIT_PCT:
        errors.append("TAKE_PROFIT_PCT must be greater than STOP_LOSS_PCT")

    if LEVERAGE < 1 or LEVERAGE > 10:
        warnings.append(f"Leverage {LEVERAGE}x is {'low' if LEVERAGE < 2 else 'high'}")

    # Check HyperLiquid credentials
    if not HYPERLIQUID_SECRET_KEY:
        errors.append("PH_SECRET_KEY not found in environment")
    elif HYPERLIQUID_SECRET_KEY.startswith("0xYOUR"):
        errors.append("PH_SECRET_KEY not configured in .env.ai")

    # Check strategy config
    if not any([ENABLE_RSI_STRATEGY, ENABLE_SMA_STRATEGY, ENABLE_BREAKOUT_STRATEGY]):
        errors.append("At least one strategy must be enabled")

    # Check paths
    if not TRADES_LOG_PATH.parent.exists():
        warnings.append(f"Log directory {TRADES_LOG_PATH.parent} does not exist")

    if not REPORTS_PATH.exists():
        warnings.append(f"Reports directory {REPORTS_PATH} does not exist")

    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }

def print_config_summary():
    """Print configuration summary"""
    print("\n" + "="*60)
    print("ETH MULTI-STRATEGY ROTATION - AGGRESSIVE CONFIG")
    print("="*60)

    print(f"\n💰 Capital & Risk:")
    print(f"  Starting Capital: ${STARTING_CAPITAL:,.0f}")
    print(f"  Position Size: {POSITION_SIZE_PCT*100:.0f}% (${MAX_POSITION_SIZE_USD:,.0f})")
    print(f"  Leverage: {LEVERAGE}x")
    print(f"  Stop Loss: {STOP_LOSS_PCT*100:.1f}%")
    print(f"  Take Profit: {TAKE_PROFIT_PCT*100:.1f}%")
    print(f"  Risk:Reward: 1:{TAKE_PROFIT_PCT/STOP_LOSS_PCT:.1f}")

    print(f"\n📊 Trading:")
    print(f"  Assets: {', '.join(SYMBOLS)}")
    print(f"  Check Interval: Every {CHECK_INTERVAL_MINUTES} minutes")
    print(f"  Min Confidence: {MIN_CONFIDENCE_THRESHOLD}%")

    print(f"\n🎯 Enabled Strategies:")
    if ENABLE_RSI_STRATEGY:
        print(f"  ✅ RSI Mean Reversion ({RSI_CONFIG['timeframe']})")
    if ENABLE_SMA_STRATEGY:
        print(f"  ✅ SMA Crossover ({SMA_CONFIG['timeframe']})")
    if ENABLE_BREAKOUT_STRATEGY:
        print(f"  ✅ Breakout Momentum ({BREAKOUT_CONFIG['timeframe']})")

    print(f"\n🤖 AI Enhancement:")
    print(f"  AI Validation: {'✅ Enabled' if USE_AI_VALIDATION else '❌ Disabled'}")
    print(f"  Funding Agent: {'✅' if USE_FUNDING_AGENT else '❌'}")
    print(f"  Liquidation Agent: {'✅' if USE_LIQUIDATION_AGENT else '❌'}")
    print(f"  Whale Agent: {'✅' if USE_WHALE_AGENT else '❌'}")
    print(f"  Risk Agent: {'✅' if USE_RISK_AGENT else '❌'}")

    print(f"\n🛡️ Safety:")
    print(f"  Paper Trading: {'✅ ON' if PAPER_TRADING_MODE else '❌ OFF (LIVE!)'}")
    print(f"  Circuit Breaker: {'✅ Enabled' if ENABLE_CIRCUIT_BREAKER else '❌ Disabled'}")
    print(f"  Max Consecutive Losses: {MAX_CONSECUTIVE_LOSSES}")
    print(f"  Emergency Stop Check: {'✅' if CHECK_EMERGENCY_STOP else '❌'}")

    print(f"\n🎯 Validation Goals (1-2 months):")
    print(f"  Min Return: +{VALIDATION_GOALS['min_return_pct']:.0f}%")
    print(f"  Min Win Rate: {VALIDATION_GOALS['min_win_rate_pct']:.0f}%")
    print(f"  Min Profitable Weeks: {VALIDATION_GOALS['min_profitable_weeks']}/4")

    print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    """Test configuration"""
    print("🧪 Testing Configuration...")

    # Validate config
    validation = validate_config()

    if validation["errors"]:
        print("\n❌ Configuration Errors:")
        for error in validation["errors"]:
            print(f"  - {error}")

    if validation["warnings"]:
        print("\n⚠️  Configuration Warnings:")
        for warning in validation["warnings"]:
            print(f"  - {warning}")

    if validation["is_valid"]:
        print("\n✅ Configuration valid!")
        print_config_summary()
    else:
        print("\n❌ Configuration invalid - please fix errors above")
