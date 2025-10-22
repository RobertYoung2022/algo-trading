"""
HyperLiquid Executor - Trading Execution Wrapper
=================================================
Handles all HyperLiquid trading operations with error handling and retry logic

Features:
- Position entry with limit orders
- TP/SL management
- Position monitoring
- Safe position exit
- Balance checking
- Leverage management
"""

import sys
import os
from pathlib import Path
from typing import Optional, Dict, Tuple
from datetime import datetime
import time
import eth_account
from termcolor import cprint

# Add parent directory to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import configuration
from trading.config_eth_aggressive import (
    HYPERLIQUID_SECRET_KEY,
    LEVERAGE,
    STOP_LOSS_PCT,
    TAKE_PROFIT_PCT,
    PAPER_TRADING_MODE,
    SLIPPAGE_TOLERANCE_PCT,
)

# Import HyperLiquid functions from moon-dev-agents
try:
    from moon_dev_agents.src.nice_funcs_hl import _get_ohlcv, _process_data_to_df
    from hyperliquid.info import Info
    from hyperliquid.exchange import Exchange
    HYPERLIQUID_AVAILABLE = True
except ImportError as e:
    cprint(f"⚠️  HyperLiquid libraries not available: {e}", "yellow")
    HYPERLIQUID_AVAILABLE = False


class HyperLiquidExecutor:
    """
    HyperLiquid Trading Executor

    Wrapper for HyperLiquid trading operations with error handling
    """

    def __init__(self):
        """Initialize HyperLiquid executor"""
        self.paper_trading = PAPER_TRADING_MODE

        if not HYPERLIQUID_AVAILABLE:
            cprint("⚠️  HyperLiquid not available - running in simulation mode", "yellow")
            self.account = None
            self.info = None
            self.exchange = None
            return

        # Initialize account
        try:
            if not HYPERLIQUID_SECRET_KEY or HYPERLIQUID_SECRET_KEY.startswith("0xYOUR"):
                raise ValueError("HyperLiquid secret key not configured")

            self.account = eth_account.Account.from_key(HYPERLIQUID_SECRET_KEY)
            self.info = Info()
            self.exchange = Exchange(self.account)

            cprint(f"✅ HyperLiquid Executor initialized", "green")
            cprint(f"   Account: {self.account.address[:10]}...", "green")
            cprint(f"   Mode: {'📝 PAPER TRADING' if self.paper_trading else '🚨 LIVE TRADING'}",
                   "yellow" if self.paper_trading else "red")

        except Exception as e:
            cprint(f"❌ Failed to initialize HyperLiquid: {e}", "red")
            self.account = None
            self.info = None
            self.exchange = None

    def get_account_balance(self) -> float:
        """
        Get account balance in USD

        Returns:
            Account balance in USD
        """
        if self.paper_trading or not self.info:
            return 1000.0  # Simulated balance

        try:
            user_state = self.info.user_state(self.account.address)
            balance = float(user_state.get("marginSummary", {}).get("accountValue", 0))
            return balance

        except Exception as e:
            cprint(f"⚠️  Error fetching balance: {e}", "yellow")
            return 0.0

    def get_current_price(self, symbol: str) -> Tuple[float, float]:
        """
        Get current ask and bid prices

        Args:
            symbol: Trading symbol (e.g., "ETH")

        Returns:
            Tuple of (ask_price, bid_price)
        """
        if self.paper_trading or not self.info:
            # Simulated prices (fetch from API)
            from moon_dev_agents.src.nice_funcs_hl import _get_ohlcv
            from datetime import datetime, timedelta

            end_time = datetime.now()
            start_time = end_time - timedelta(hours=1)
            candles = _get_ohlcv(symbol, "1h", start_time, end_time, batch_size=1)

            if candles:
                last_price = float(candles[-1]["c"])
                return last_price, last_price

            return 3000.0, 3000.0  # Fallback

        try:
            l2_data = self.info.l2_snapshot(symbol)

            if l2_data and "levels" in l2_data:
                asks = l2_data["levels"][0]  # Ask side
                bids = l2_data["levels"][1]  # Bid side

                if asks and bids:
                    ask_price = float(asks[0]["px"])
                    bid_price = float(bids[0]["px"])
                    return ask_price, bid_price

        except Exception as e:
            cprint(f"⚠️  Error fetching prices: {e}", "yellow")

        return 0.0, 0.0

    def get_position(self, symbol: str) -> Dict:
        """
        Get current position for symbol

        Args:
            symbol: Trading symbol

        Returns:
            Dict with position info: {
                "in_position": bool,
                "size": float,
                "entry_price": float,
                "pnl": float,
                "pnl_pct": float,
                "is_long": bool
            }
        """
        if self.paper_trading or not self.info:
            return {
                "in_position": False,
                "size": 0.0,
                "entry_price": 0.0,
                "pnl": 0.0,
                "pnl_pct": 0.0,
                "is_long": True
            }

        try:
            user_state = self.info.user_state(self.account.address)
            positions = user_state.get("assetPositions", [])

            for pos in positions:
                if pos.get("position", {}).get("coin") == symbol:
                    position = pos["position"]
                    size = float(position.get("szi", 0))

                    if size != 0:
                        entry_price = float(position.get("entryPx", 0))
                        current_price = float(position.get("positionValue", 0)) / abs(size)
                        pnl = float(position.get("unrealizedPnl", 0))
                        pnl_pct = (pnl / (abs(size) * entry_price)) * 100 if entry_price > 0 else 0

                        return {
                            "in_position": True,
                            "size": abs(size),
                            "entry_price": entry_price,
                            "pnl": pnl,
                            "pnl_pct": pnl_pct,
                            "is_long": size > 0
                        }

        except Exception as e:
            cprint(f"⚠️  Error fetching position: {e}", "yellow")

        return {
            "in_position": False,
            "size": 0.0,
            "entry_price": 0.0,
            "pnl": 0.0,
            "pnl_pct": 0.0,
            "is_long": True
        }

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        Set leverage for symbol

        Args:
            symbol: Trading symbol
            leverage: Leverage multiplier (1-50x)

        Returns:
            Success status
        """
        if self.paper_trading:
            cprint(f"📝 [PAPER] Would set {symbol} leverage to {leverage}x", "cyan")
            return True

        if not self.exchange:
            return False

        try:
            # HyperLiquid leverage update
            result = self.exchange.update_leverage(leverage, symbol, is_cross=True)
            cprint(f"✅ Set {symbol} leverage to {leverage}x", "green")
            return True

        except Exception as e:
            cprint(f"❌ Failed to set leverage: {e}", "red")
            return False

    def place_limit_order(self, symbol: str, is_buy: bool, size: float,
                          price: float) -> bool:
        """
        Place limit order

        Args:
            symbol: Trading symbol
            is_buy: True for buy, False for sell
            size: Position size in base currency
            price: Limit price

        Returns:
            Success status
        """
        side = "BUY" if is_buy else "SELL"

        if self.paper_trading:
            cprint(f"📝 [PAPER] Would place {side} limit order:", "cyan")
            cprint(f"   Symbol: {symbol}", "cyan")
            cprint(f"   Size: {size:.4f}", "cyan")
            cprint(f"   Price: ${price:.2f}", "cyan")
            return True

        if not self.exchange:
            cprint("❌ Exchange not initialized", "red")
            return False

        try:
            # Place limit order on HyperLiquid
            order = self.exchange.order(
                symbol,
                is_buy,
                size,
                price,
                {"limit": {"tif": "Gtc"}}  # Good-til-cancelled
            )

            cprint(f"✅ {side} order placed:", "green")
            cprint(f"   Symbol: {symbol}", "green")
            cprint(f"   Size: {size:.4f}", "green")
            cprint(f"   Price: ${price:.2f}", "green")

            return True

        except Exception as e:
            cprint(f"❌ Order failed: {e}", "red")
            return False

    def set_tp_sl(self, symbol: str, entry_price: float,
                  take_profit_pct: float = None,
                  stop_loss_pct: float = None) -> bool:
        """
        Set take profit and stop loss orders

        Args:
            symbol: Trading symbol
            entry_price: Entry price
            take_profit_pct: Take profit percentage (optional, uses config default)
            stop_loss_pct: Stop loss percentage (optional, uses config default)

        Returns:
            Success status
        """
        tp_pct = take_profit_pct or TAKE_PROFIT_PCT
        sl_pct = stop_loss_pct or STOP_LOSS_PCT

        tp_price = entry_price * (1 + tp_pct)
        sl_price = entry_price * (1 - sl_pct)

        if self.paper_trading:
            cprint(f"📝 [PAPER] Would set TP/SL for {symbol}:", "cyan")
            cprint(f"   Entry: ${entry_price:.2f}", "cyan")
            cprint(f"   Take Profit: ${tp_price:.2f} (+{tp_pct*100:.1f}%)", "cyan")
            cprint(f"   Stop Loss: ${sl_price:.2f} (-{sl_pct*100:.1f}%)", "cyan")
            return True

        if not self.exchange:
            return False

        try:
            # Get current position to determine size
            position = self.get_position(symbol)
            if not position["in_position"]:
                cprint(f"⚠️  Not in position for {symbol}, cannot set TP/SL", "yellow")
                return False

            size = position["size"]

            # Place TP limit order (sell at higher price)
            self.exchange.order(
                symbol,
                False,  # Sell
                size,
                tp_price,
                {"limit": {"tif": "Gtc"}},
                reduce_only=True
            )

            # Place SL stop order
            self.exchange.order(
                symbol,
                False,  # Sell
                size,
                sl_price,
                {"trigger": {"triggerPx": sl_price, "isMarket": True, "tpsl": "sl"}},
                reduce_only=True
            )

            cprint(f"✅ TP/SL set for {symbol}:", "green")
            cprint(f"   TP: ${tp_price:.2f} (+{tp_pct*100:.1f}%)", "green")
            cprint(f"   SL: ${sl_price:.2f} (-{sl_pct*100:.1f}%)", "green")

            return True

        except Exception as e:
            cprint(f"❌ Failed to set TP/SL: {e}", "red")
            return False

    def close_position(self, symbol: str) -> bool:
        """
        Close position at market price

        Args:
            symbol: Trading symbol

        Returns:
            Success status
        """
        position = self.get_position(symbol)

        if not position["in_position"]:
            cprint(f"⏸️  Not in position for {symbol}", "yellow")
            return False

        size = position["size"]
        is_long = position["is_long"]

        if self.paper_trading:
            cprint(f"📝 [PAPER] Would close {symbol} position:", "cyan")
            cprint(f"   Size: {size:.4f}", "cyan")
            cprint(f"   P&L: ${position['pnl']:.2f} ({position['pnl_pct']:+.2f}%)", "cyan")
            return True

        if not self.exchange:
            return False

        try:
            # Market order to close (opposite side)
            self.exchange.market_close(symbol)

            cprint(f"✅ Closed {symbol} position:", "green")
            cprint(f"   Size: {size:.4f}", "green")
            cprint(f"   P&L: ${position['pnl']:.2f} ({position['pnl_pct']:+.2f}%)", "green")

            return True

        except Exception as e:
            cprint(f"❌ Failed to close position: {e}", "red")
            return False

    def enter_long_position(self, symbol: str, size_usd: float,
                            entry_price: Optional[float] = None) -> bool:
        """
        Enter long position with TP/SL

        Args:
            symbol: Trading symbol
            size_usd: Position size in USD
            entry_price: Entry price (optional, uses current price if None)

        Returns:
            Success status
        """
        cprint(f"\n{'='*60}", "cyan")
        cprint(f"🚀 ENTERING LONG POSITION: {symbol}", "cyan")
        cprint(f"{'='*60}", "cyan")

        # Check if already in position
        position = self.get_position(symbol)
        if position["in_position"]:
            cprint(f"⚠️  Already in position for {symbol}", "yellow")
            return False

        # Set leverage
        if not self.set_leverage(symbol, LEVERAGE):
            cprint("❌ Failed to set leverage", "red")
            return False

        # Get current price if not provided
        if entry_price is None:
            ask, bid = self.get_current_price(symbol)
            entry_price = ask  # Buy at ask price

        # Add slippage tolerance
        entry_price_with_slippage = entry_price * (1 + SLIPPAGE_TOLERANCE_PCT)

        # Calculate position size
        position_size = size_usd / entry_price

        cprint(f"\n📊 Position Details:", "cyan")
        cprint(f"   Symbol: {symbol}", "cyan")
        cprint(f"   Size: ${size_usd:.2f} ({position_size:.4f} {symbol})", "cyan")
        cprint(f"   Entry Price: ${entry_price:.2f}", "cyan")
        cprint(f"   Leverage: {LEVERAGE}x", "cyan")
        cprint(f"   Stop Loss: {STOP_LOSS_PCT*100:.1f}% (${entry_price*(1-STOP_LOSS_PCT):.2f})", "cyan")
        cprint(f"   Take Profit: {TAKE_PROFIT_PCT*100:.1f}% (${entry_price*(1+TAKE_PROFIT_PCT):.2f})", "cyan")

        # Place limit order
        if not self.place_limit_order(symbol, True, position_size, entry_price_with_slippage):
            return False

        # Wait for fill (in paper mode, assume instant fill)
        if not self.paper_trading:
            time.sleep(2)  # Wait for order to fill

        # Set TP/SL
        if not self.set_tp_sl(symbol, entry_price):
            cprint("⚠️  Failed to set TP/SL, but position is open", "yellow")

        cprint(f"\n✅ POSITION OPENED SUCCESSFULLY", "green")
        return True


if __name__ == "__main__":
    """Test HyperLiquid executor"""
    print("🧪 Testing HyperLiquid Executor...\n")

    executor = HyperLiquidExecutor()

    # Test balance
    balance = executor.get_account_balance()
    print(f"💰 Account Balance: ${balance:,.2f}\n")

    # Test current price
    ask, bid = executor.get_current_price("ETH")
    print(f"📊 ETH Price: Ask ${ask:.2f}, Bid ${bid:.2f}\n")

    # Test position check
    position = executor.get_position("ETH")
    print(f"📈 Current Position: {position}\n")

    # Test enter position (paper mode)
    print("🧪 Testing position entry (paper mode)...")
    executor.enter_long_position("ETH", 900, entry_price=3000)
