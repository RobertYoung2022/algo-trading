#!/usr/bin/env python3
"""
🚀 MODERNIZED Hyperliquid Functions - Phase 3 Migration
======================================================

🌟 MIGRATION SHOWCASE: Legacy patterns → Modern @trading_functions
This module demonstrates the complete migration from legacy Hyperliquid
functions to modern universal wrappers following UPGRADE_OPPORTUNITIES_REPORT.md

🔄 FUNCTION MIGRATION MAPPING:
- ask_bid() → universal_get_ask_bid()
- get_position() → universal_get_positions()
- limit_order() → universal_limit_order() [to be implemented]
- cancel_all_orders() → universal_cancel_orders() [to be implemented]
- kill_switch() → universal_kill_switch()
- pnl_close() → universal_monitor_pnl()

💫 Bobby's Patterns Maintained:
- Same function signatures for compatibility
- Enhanced error handling and logging
- Production-ready safety features
"""

import os
import time
from typing import Dict, Any, Tuple, Optional
from dotenv import load_dotenv

# 🛡️ MODERN: Import @trading_functions instead of direct API calls
from trading_functions import (
    create_universal_client,
    universal_get_ask_bid,
    universal_get_positions,
    universal_monitor_pnl,
    universal_kill_switch,
    ExchangeType,
    UniversalTradingError
)

# 🛡️ MODERN: Secure credential management
load_dotenv()

print("🚀 Modernized Hyperliquid Functions Loading with @trading_functions... 💫")

# ============================================================
# 🌍 MODERN: Universal Client Management
# ============================================================

def get_modernized_client(private_key: str = None) -> Optional[Any]:
    """
    🔧 MODERN: Get universal Hyperliquid client
    Replaces direct eth_account.Account.from_key() usage
    """
    try:
        if not private_key:
            private_key = os.getenv("HYPERLIQUID_PRIVATE_KEY")

        if not private_key or private_key == "your_private_key_here":
            raise UniversalTradingError("HYPERLIQUID_PRIVATE_KEY not configured in .env")

        return create_universal_client(
            exchange='hyperliquid',
            private_key=private_key,
            testnet=False
        )
    except Exception as e:
        print(f"❌ Failed to create modernized client: {e}")
        return None

# ============================================================
# 🔄 MODERNIZED: Market Data Functions
# ============================================================

def ask_bid_modernized(symbol: str, private_key: str = None) -> Tuple[float, float, Optional[Dict]]:
    """
    📊 MODERNIZED: Get ask/bid using universal wrapper

    MIGRATION: ask_bid() → universal_get_ask_bid()

    Args:
        symbol: Trading symbol
        private_key: Optional private key (uses .env if not provided)

    Returns:
        Tuple[ask, bid, l2_data]
    """
    print(f"📊 MODERNIZED: Getting ask/bid for {symbol} via universal wrapper...")

    try:
        client = get_modernized_client(private_key)
        if not client:
            raise UniversalTradingError("Failed to initialize client")

        # 🛡️ MODERN: Universal ask/bid call
        ask, bid, l2_data = universal_get_ask_bid(client, symbol)

        print(f"   ✅ Ask: ${ask:.4f}, Bid: ${bid:.4f}")
        return ask, bid, l2_data

    except Exception as e:
        print(f"❌ MODERNIZED ask_bid error: {e}")
        # Legacy fallback pattern maintained
        return 0.0, 0.0, None

# ============================================================
# 🔄 MODERNIZED: Position Management Functions
# ============================================================

def get_position_modernized(symbol: str, private_key: str = None) -> Tuple[Any, bool, float, str, float, float, Optional[bool]]:
    """
    📊 MODERNIZED: Get position info using universal wrapper

    MIGRATION: get_position() → universal_get_positions()

    Args:
        symbol: Trading symbol
        private_key: Optional private key

    Returns:
        Tuple[positions, in_pos, size, pos_sym, entry_px, pnl_perc, long]
        (Maintains legacy signature for compatibility)
    """
    print(f"📊 MODERNIZED: Getting position for {symbol} via universal wrapper...")

    try:
        client = get_modernized_client(private_key)
        if not client:
            raise UniversalTradingError("Failed to initialize client")

        # 🛡️ MODERN: Universal position call
        position_data = universal_get_positions(client, symbol)

        # 🔄 Extract data in legacy format for compatibility
        positions = position_data.get('raw_positions', [])
        in_pos = position_data.get('has_position', False)
        size = position_data.get('size', 0.0)
        pos_sym = symbol if in_pos else None
        entry_px = position_data.get('entry_price', 0.0)
        pnl_perc = position_data.get('pnl_percentage', 0.0)

        # Determine long/short
        if size > 0:
            long = True
        elif size < 0:
            long = False
        else:
            long = None

        print(f"   ✅ Position: {size:.4f}, PnL: {pnl_perc:.2f}%, Long: {long}")
        return positions, in_pos, size, pos_sym, entry_px, pnl_perc, long

    except Exception as e:
        print(f"❌ MODERNIZED get_position error: {e}")
        # Legacy fallback pattern maintained
        return [], False, 0.0, None, 0.0, 0.0, None

# ============================================================
# 🔄 MODERNIZED: Risk Management Functions
# ============================================================

def pnl_close_modernized(symbol: str, target: float, max_loss: float, private_key: str = None) -> bool:
    """
    💰 MODERNIZED: PnL monitoring and closing using universal wrapper

    MIGRATION: pnl_close() → universal_monitor_pnl() + universal_kill_switch()

    Args:
        symbol: Trading symbol
        target: Profit target percentage
        max_loss: Maximum loss percentage (negative)
        private_key: Optional private key

    Returns:
        bool: True if position was closed
    """
    print(f"💰 MODERNIZED: PnL monitoring for {symbol} (Target: {target}%, Max Loss: {max_loss}%)")

    try:
        client = get_modernized_client(private_key)
        if not client:
            raise UniversalTradingError("Failed to initialize client")

        # 🛡️ MODERN: Universal PnL monitoring
        pnl_result = universal_monitor_pnl(
            client=client,
            symbol=symbol,
            target_pct=target,
            max_loss_pct=abs(max_loss)
        )

        if pnl_result.get('should_close', False):
            close_reason = pnl_result.get('close_reason', 'Unknown')
            current_pnl = pnl_result.get('current_pnl_pct', 0)

            print(f"🚨 MODERNIZED: PnL trigger - {close_reason} (Current: {current_pnl:.2f}%)")

            # 🛡️ MODERN: Universal kill switch
            success = universal_kill_switch(client, symbol)
            if success:
                print("✅ MODERNIZED: Position closed successfully")
                return True
            else:
                print("❌ MODERNIZED: Position closure failed")
                return False
        else:
            current_pnl = pnl_result.get('current_pnl_pct', 0)
            print(f"💰 MODERNIZED: PnL within targets ({current_pnl:.2f}%) - continuing")
            return False

    except Exception as e:
        print(f"❌ MODERNIZED pnl_close error: {e}")
        return False

def kill_switch_modernized(symbol: str, private_key: str = None) -> bool:
    """
    🚨 MODERNIZED: Emergency position closure using universal wrapper

    MIGRATION: kill_switch() → universal_kill_switch()

    Args:
        symbol: Trading symbol
        private_key: Optional private key

    Returns:
        bool: True if position successfully closed
    """
    print(f"🚨 MODERNIZED: Kill switch activated for {symbol}")

    try:
        client = get_modernized_client(private_key)
        if not client:
            raise UniversalTradingError("Failed to initialize client")

        # 🛡️ MODERN: Universal kill switch
        success = universal_kill_switch(client, symbol)

        if success:
            print("✅ MODERNIZED: Emergency closure successful")
        else:
            print("❌ MODERNIZED: Emergency closure failed")

        return success

    except Exception as e:
        print(f"❌ MODERNIZED kill_switch error: {e}")
        return False

# ============================================================
# 🔄 MODERNIZED: Trading Functions (Placeholder for Future Implementation)
# ============================================================

def limit_order_modernized(coin: str, is_buy: bool, sz: float, limit_px: float, reduce_only: bool, private_key: str = None) -> Dict[str, Any]:
    """
    📝 MODERNIZED: Limit order placement (To be implemented in universal wrapper)

    MIGRATION: limit_order() → universal_limit_order() [Future]

    Args:
        coin: Trading symbol
        is_buy: True for buy, False for sell
        sz: Order size
        limit_px: Limit price
        reduce_only: Reduce-only flag
        private_key: Optional private key

    Returns:
        Dict with order result
    """
    print(f"📝 MODERNIZED: Limit order {coin} {'BUY' if is_buy else 'SELL'} {sz} @ ${limit_px:.4f}")
    print("⚠️ MODERNIZED: universal_limit_order() implementation pending in @trading_functions")

    # TODO: Implement universal_limit_order() in @trading_functions
    # For now, return mock response for compatibility
    return {
        'success': False,
        'message': 'universal_limit_order() not yet implemented',
        'order_id': None
    }

def cancel_all_orders_modernized(private_key: str = None) -> bool:
    """
    ❌ MODERNIZED: Cancel all orders (To be implemented in universal wrapper)

    MIGRATION: cancel_all_orders() → universal_cancel_orders() [Future]

    Args:
        private_key: Optional private key

    Returns:
        bool: True if orders cancelled successfully
    """
    print("❌ MODERNIZED: Cancelling all orders")
    print("⚠️ MODERNIZED: universal_cancel_orders() implementation pending in @trading_functions")

    # TODO: Implement universal_cancel_orders() in @trading_functions
    return False

# ============================================================
# 🔄 MODERNIZED: Account Functions
# ============================================================

def acct_bal_modernized(private_key: str = None) -> float:
    """
    💰 MODERNIZED: Get account balance using universal wrapper

    MIGRATION: acct_bal() → universal_get_positions() (account data)

    Args:
        private_key: Optional private key

    Returns:
        float: Account value
    """
    print("💰 MODERNIZED: Getting account balance via universal wrapper...")

    try:
        client = get_modernized_client(private_key)
        if not client:
            raise UniversalTradingError("Failed to initialize client")

        # 🛡️ MODERN: Get all positions to extract account value
        position_data = universal_get_positions(client)

        account_value = 0.0
        if 'margin_summary' in position_data:
            account_value = float(position_data['margin_summary'].get('accountValue', 0))

        print(f"   ✅ Account Value: ${account_value:.2f}")
        return account_value

    except Exception as e:
        print(f"❌ MODERNIZED acct_bal error: {e}")
        return 0.0

# ============================================================
# 🎯 MIGRATION COMPATIBILITY LAYER
# ============================================================

# 🔄 Provide legacy function names for backward compatibility
ask_bid = ask_bid_modernized
get_position = get_position_modernized
pnl_close = pnl_close_modernized
kill_switch = kill_switch_modernized
limit_order = limit_order_modernized
cancel_all_orders = cancel_all_orders_modernized
acct_bal = acct_bal_modernized

# ============================================================
# 🎯 MODULE VALIDATION & TESTING
# ============================================================

def test_modernized_functions():
    """
    🧪 Test modernized functions for validation
    """
    print("\n" + "=" * 60)
    print("🧪 TESTING MODERNIZED HYPERLIQUID FUNCTIONS")
    print("=" * 60)

    test_symbol = 'ETH'

    try:
        # Test ask/bid
        print(f"\n📊 Testing ask_bid_modernized({test_symbol})...")
        ask, bid, l2 = ask_bid_modernized(test_symbol)
        print(f"   Result: Ask={ask}, Bid={bid}")

        # Test position
        print(f"\n📊 Testing get_position_modernized({test_symbol})...")
        pos_data = get_position_modernized(test_symbol)
        print(f"   Result: In position={pos_data[1]}, Size={pos_data[2]}")

        # Test account balance
        print(f"\n💰 Testing acct_bal_modernized()...")
        balance = acct_bal_modernized()
        print(f"   Result: Balance=${balance:.2f}")

        print("\n✅ Modernized functions test completed")

    except Exception as e:
        print(f"\n❌ Test error: {e}")

if __name__ == "__main__":
    print("=" * 80)
    print("🛡️ PHASE 3 MIGRATION: Hyperliquid Functions Modernization")
    print("=" * 80)
    print("🔄 MIGRATION PATTERNS IMPLEMENTED:")
    print("   ✅ ask_bid() → universal_get_ask_bid()")
    print("   ✅ get_position() → universal_get_positions()")
    print("   ✅ pnl_close() → universal_monitor_pnl() + universal_kill_switch()")
    print("   ✅ kill_switch() → universal_kill_switch()")
    print("   ✅ acct_bal() → universal_get_positions() (account data)")
    print("   ⚠️ limit_order() → universal_limit_order() [Pending implementation]")
    print("   ⚠️ cancel_all_orders() → universal_cancel_orders() [Pending implementation]")
    print("=" * 80)
    print("💫 Modernized Hyperliquid functions ready for Phase 3!")
    print("=" * 80)

    # Run validation test
    test_modernized_functions()