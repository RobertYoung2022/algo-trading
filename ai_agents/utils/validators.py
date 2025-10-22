"""
Validation Utilities
Functions for validating trading parameters and positions
"""

from typing import Dict, Any, Tuple, List


def validate_strategy_params(params: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate strategy parameters

    Args:
        params: Strategy parameters dictionary

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []

    # Check required parameters
    required = ["name", "description"]
    for param in required:
        if param not in params:
            errors.append(f"Missing required parameter: {param}")

    # Validate parameter types
    if "lookback_period" in params:
        if not isinstance(params["lookback_period"], int) or params["lookback_period"] <= 0:
            errors.append("lookback_period must be a positive integer")

    if "stop_loss" in params:
        if not isinstance(params["stop_loss"], (int, float)) or params["stop_loss"] <= 0:
            errors.append("stop_loss must be a positive number")

    if "take_profit" in params:
        if not isinstance(params["take_profit"], (int, float)) or params["take_profit"] <= 0:
            errors.append("take_profit must be a positive number")

    return len(errors) == 0, errors


def validate_position_size(position_size: float,
                          account_balance: float,
                          max_position_pct: float = 0.2,
                          min_balance: float = 100.0) -> Tuple[bool, List[str]]:
    """
    Validate position size

    Args:
        position_size: Requested position size in USD
        account_balance: Current account balance
        max_position_pct: Maximum position as % of balance
        min_balance: Minimum balance required

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []

    # Check minimum balance
    if account_balance < min_balance:
        errors.append(f"Account balance ${account_balance:.2f} below minimum ${min_balance:.2f}")

    # Check position size vs balance
    if position_size > account_balance:
        errors.append(f"Position size ${position_size:.2f} exceeds balance ${account_balance:.2f}")

    # Check position size vs max percentage
    max_position = account_balance * max_position_pct
    if position_size > max_position:
        errors.append(f"Position size ${position_size:.2f} exceeds max {max_position_pct*100}% of balance (${max_position:.2f})")

    # Check for zero or negative position
    if position_size <= 0:
        errors.append("Position size must be positive")

    return len(errors) == 0, errors


def validate_risk_reward_ratio(entry_price: float,
                               stop_loss: float,
                               take_profit: float,
                               min_rr_ratio: float = 1.5) -> Tuple[bool, List[str]]:
    """
    Validate risk/reward ratio

    Args:
        entry_price: Entry price
        stop_loss: Stop loss price
        take_profit: Take profit price
        min_rr_ratio: Minimum acceptable risk/reward ratio

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []

    # Calculate risk and reward
    risk = abs(entry_price - stop_loss)
    reward = abs(take_profit - entry_price)

    if risk <= 0:
        errors.append("Invalid stop loss - risk must be > 0")
        return False, errors

    # Calculate R/R ratio
    rr_ratio = reward / risk

    if rr_ratio < min_rr_ratio:
        errors.append(f"Risk/Reward ratio {rr_ratio:.2f} below minimum {min_rr_ratio:.2f}")

    return len(errors) == 0, errors
