#!/usr/bin/env python3
"""
🚀 PRODUCTION CRYPTO MOMENTUM TRADING BOT 🚀
=============================================
Production-ready implementation of the successful Crypto Momentum Surge Strategy
targeting deployment on Hyperliquid exchange with best-performing assets.

PERFORMANCE TARGETS:
- HBAR: +111.1% return, 1.73 Sharpe ratio (PRIMARY)
- CRO: +48.3% return, 0.91 Sharpe ratio (SECONDARY)
- LINK: +15.9% return, 0.71 Sharpe ratio (TERTIARY)

MODERN ARCHITECTURE:
- Uses @trading_functions universal wrappers
- Secure credential management via .env
- Comprehensive risk management
- Real-time signal detection
- Multi-asset portfolio management

Author: Bobby's Algo Trading Systems 🌙
Date: 2025-01-18
Version: 1.0.0
"""

import os
import sys
import time
import json
import logging
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv

# 🛡️ MODERN: Import @trading_functions instead of legacy nice_funcs
from trading_functions import (
    create_universal_client,
    universal_get_ask_bid,
    universal_get_positions,
    universal_monitor_pnl,
    universal_kill_switch,
    place_limit_order_hyperliquid,
    cancel_all_orders_hyperliquid,
    get_ohlcv_hyperliquid,
    get_account_balance_hyperliquid,
    calculate_macd,
    calculate_rsi,
    ExchangeType,
    production_readiness_check
)

# Import local modules
from momentum_risk_manager import MomentumRiskManager
from momentum_signals import MomentumSignalDetector
from momentum_config import (
    ASSET_CONFIGS,
    TRADING_MODE,
    MAX_CONCURRENT_POSITIONS,
    POSITION_TIMEOUT_BARS,
    SIGNAL_CHECK_INTERVAL
)

# 🛡️ PRODUCTION: Secure credential management
load_dotenv()

# ============================================================
# 📊 LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_momentum_bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# 🎯 TRADING STATE MANAGEMENT
# ============================================================

class TradingState(Enum):
    """Bot trading states"""
    INITIALIZING = "initializing"
    SCANNING = "scanning"
    ENTERING = "entering"
    MANAGING = "managing"
    EXITING = "exiting"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class Position:
    """Position tracking dataclass"""
    symbol: str
    side: str
    entry_price: float
    size: float
    entry_time: datetime
    stop_loss: float
    take_profit: float
    bars_held: int = 0
    unrealized_pnl: float = 0.0
    signal_strength: float = 0.0


@dataclass
class BotMetrics:
    """Real-time bot performance metrics"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    current_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    daily_pnl: float = 0.0
    peak_balance: float = 0.0


# ============================================================
# 🚀 MAIN CRYPTO MOMENTUM BOT
# ============================================================

class CryptoMomentumBot:
    """
    🔥 Production Crypto Momentum Trading Bot 🔥

    Implements the successful momentum surge strategy with:
    - Real-time signal detection
    - Multi-asset portfolio management
    - Dynamic risk management
    - Comprehensive monitoring
    """

    def __init__(self):
        """Initialize the momentum trading bot"""
        logger.info("🚀 Initializing Crypto Momentum Bot...")

        # Trading state
        self.state = TradingState.INITIALIZING
        self.is_running = False

        # Initialize components
        self.client = None
        self.risk_manager = MomentumRiskManager()
        self.signal_detector = MomentumSignalDetector()

        # Position tracking
        self.positions: Dict[str, Position] = {}
        self.pending_orders: Dict[str, Any] = {}

        # Performance tracking
        self.metrics = BotMetrics()
        self.trade_history: List[Dict] = []

        # Asset management
        self.tradeable_assets = list(ASSET_CONFIGS.keys())
        self.asset_priority = ['HBAR', 'CRO', 'LINK']  # Priority order

        # Load credentials
        self.private_key = os.getenv("HYPERLIQUID_PRIVATE_KEY")
        if not self.private_key:
            logger.error("❌ HYPERLIQUID_PRIVATE_KEY not found in .env")
            raise ValueError("Missing Hyperliquid credentials")

        # Initialize client
        self._initialize_client()

        # Load initial account state
        self._update_account_state()

        logger.info("✅ Crypto Momentum Bot initialized successfully")

    def _initialize_client(self):
        """Initialize Hyperliquid universal client"""
        try:
            self.client = create_universal_client(
                exchange='hyperliquid',
                private_key=self.private_key,
                testnet=(TRADING_MODE == 'paper')
            )
            logger.info("✅ Hyperliquid client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize client: {e}")
            self.state = TradingState.ERROR
            raise

    def _update_account_state(self):
        """Update account balance and positions"""
        try:
            # Get account balance
            self.account_balance = get_account_balance_hyperliquid(
                self.client,
                include_positions=True
            )

            # Get open positions
            positions_data = universal_get_positions(self.client)

            # Update position tracking
            self._sync_positions(positions_data)

            # Update metrics
            self._update_metrics()

        except Exception as e:
            logger.error(f"❌ Failed to update account state: {e}")

    def _sync_positions(self, positions_data: List[Dict]):
        """Synchronize local position tracking with exchange"""
        exchange_positions = {}

        for pos in positions_data:
            symbol = pos['symbol']
            if symbol in self.tradeable_assets:
                exchange_positions[symbol] = Position(
                    symbol=symbol,
                    side=pos['side'],
                    entry_price=pos['entry_price'],
                    size=pos['size'],
                    entry_time=datetime.fromisoformat(pos['timestamp']),
                    stop_loss=pos.get('stop_loss', 0),
                    take_profit=pos.get('take_profit', 0),
                    unrealized_pnl=pos.get('unrealized_pnl', 0)
                )

        self.positions = exchange_positions

    def _update_metrics(self):
        """Update bot performance metrics"""
        # Calculate current P&L
        current_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
        self.metrics.daily_pnl = current_pnl

        # Update drawdown
        if self.account_balance > self.metrics.peak_balance:
            self.metrics.peak_balance = self.account_balance

        drawdown = (self.metrics.peak_balance - self.account_balance) / self.metrics.peak_balance
        self.metrics.current_drawdown = drawdown * 100
        self.metrics.max_drawdown = max(self.metrics.max_drawdown, self.metrics.current_drawdown)

        # Calculate win rate
        if self.metrics.total_trades > 0:
            self.metrics.win_rate = (self.metrics.winning_trades / self.metrics.total_trades) * 100

        # Calculate profit factor
        if self.metrics.avg_loss != 0:
            self.metrics.profit_factor = abs(self.metrics.avg_win / self.metrics.avg_loss)

    # ============================================================
    # 📈 SIGNAL DETECTION & TRADING
    # ============================================================

    async def scan_for_signals(self):
        """Scan assets for momentum surge signals"""
        self.state = TradingState.SCANNING
        logger.info("🔍 Scanning for momentum signals...")

        signals = {}

        for symbol in self.asset_priority:
            if symbol in self.positions:
                continue  # Skip if already have position

            try:
                # Get market data
                ohlcv = get_ohlcv_hyperliquid(
                    self.client,
                    symbol,
                    timeframe='5m',
                    limit=100
                )

                if ohlcv is None or len(ohlcv) < 50:
                    continue

                # Detect signals
                signal = self.signal_detector.detect_momentum_surge(
                    ohlcv,
                    symbol,
                    ASSET_CONFIGS[symbol]
                )

                if signal['has_signal']:
                    signals[symbol] = signal
                    logger.info(f"📈 Signal detected for {symbol}: strength={signal['strength']:.2f}")

            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}")
                continue

        return signals

    async def execute_entry(self, symbol: str, signal: Dict):
        """Execute position entry based on signal"""
        self.state = TradingState.ENTERING

        try:
            # Get current market price
            ask, bid, mid = universal_get_ask_bid(self.client, symbol)

            # Calculate position size
            position_size = self.risk_manager.calculate_position_size(
                account_balance=self.account_balance,
                signal_strength=signal['strength'],
                volatility=signal.get('volatility', 0.02),
                asset_config=ASSET_CONFIGS[symbol]
            )

            # Check risk limits
            can_trade, reason = self.risk_manager.check_risk_limits(
                current_pnl=self.metrics.daily_pnl,
                open_positions=len(self.positions)
            )

            if not can_trade:
                logger.warning(f"⚠️ Risk limit hit: {reason}")
                return False

            # Calculate stop loss and take profit
            config = ASSET_CONFIGS[symbol]
            stop_loss = ask * (1 - config['stop_loss'])
            take_profit = ask * (1 + config['take_profit'])

            # Place limit order
            order = place_limit_order_hyperliquid(
                self.client,
                symbol=symbol,
                is_buy=True,
                size=position_size,
                limit_price=ask,
                reduce_only=False,
                post_only=False
            )

            if order and order.get('status') == 'success':
                # Create position record
                self.positions[symbol] = Position(
                    symbol=symbol,
                    side='long',
                    entry_price=ask,
                    size=position_size,
                    entry_time=datetime.now(),
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    signal_strength=signal['strength']
                )

                logger.info(f"✅ Entered {symbol} position: size={position_size}, entry={ask:.4f}")
                logger.info(f"   SL={stop_loss:.4f}, TP={take_profit:.4f}")

                # Record trade
                self._record_trade({
                    'symbol': symbol,
                    'side': 'buy',
                    'price': ask,
                    'size': position_size,
                    'timestamp': datetime.now().isoformat(),
                    'signal_strength': signal['strength']
                })

                return True
            else:
                logger.error(f"❌ Failed to enter {symbol} position")
                return False

        except Exception as e:
            logger.error(f"❌ Error executing entry for {symbol}: {e}")
            return False

    async def manage_positions(self):
        """Manage open positions"""
        self.state = TradingState.MANAGING

        for symbol, position in list(self.positions.items()):
            try:
                # Get current price
                ask, bid, mid = universal_get_ask_bid(self.client, symbol)

                # Update position metrics
                position.unrealized_pnl = (bid - position.entry_price) * position.size
                position.bars_held += 1

                # Check exit conditions
                should_exit = False
                exit_reason = ""

                # Stop loss check
                if bid <= position.stop_loss:
                    should_exit = True
                    exit_reason = "Stop loss hit"

                # Take profit check
                elif bid >= position.take_profit:
                    should_exit = True
                    exit_reason = "Take profit hit"

                # Timeout check
                elif position.bars_held > POSITION_TIMEOUT_BARS:
                    should_exit = True
                    exit_reason = "Position timeout"

                # Signal fade check
                else:
                    ohlcv = get_ohlcv_hyperliquid(
                        self.client,
                        symbol,
                        timeframe='5m',
                        limit=20
                    )

                    if self.signal_detector.detect_momentum_fade(ohlcv):
                        should_exit = True
                        exit_reason = "Momentum fading"

                # Execute exit if needed
                if should_exit:
                    await self.execute_exit(symbol, exit_reason)

            except Exception as e:
                logger.error(f"Error managing {symbol} position: {e}")

    async def execute_exit(self, symbol: str, reason: str):
        """Execute position exit"""
        self.state = TradingState.EXITING

        try:
            position = self.positions.get(symbol)
            if not position:
                return

            # Get current price
            ask, bid, mid = universal_get_ask_bid(self.client, symbol)

            # Place market sell order
            order = place_limit_order_hyperliquid(
                self.client,
                symbol=symbol,
                is_buy=False,
                size=position.size,
                limit_price=bid,
                reduce_only=True,
                post_only=False
            )

            if order and order.get('status') == 'success':
                # Calculate P&L
                pnl = (bid - position.entry_price) * position.size
                pnl_pct = ((bid - position.entry_price) / position.entry_price) * 100

                # Update metrics
                self.metrics.total_trades += 1
                self.metrics.total_pnl += pnl

                if pnl > 0:
                    self.metrics.winning_trades += 1
                    self.metrics.avg_win = (
                        (self.metrics.avg_win * (self.metrics.winning_trades - 1) + pnl) /
                        self.metrics.winning_trades
                    )
                else:
                    self.metrics.losing_trades += 1
                    self.metrics.avg_loss = (
                        (self.metrics.avg_loss * (self.metrics.losing_trades - 1) + pnl) /
                        self.metrics.losing_trades
                    )

                logger.info(f"✅ Exited {symbol}: {reason}")
                logger.info(f"   P&L: ${pnl:.2f} ({pnl_pct:.2f}%)")

                # Record trade
                self._record_trade({
                    'symbol': symbol,
                    'side': 'sell',
                    'price': bid,
                    'size': position.size,
                    'timestamp': datetime.now().isoformat(),
                    'pnl': pnl,
                    'pnl_pct': pnl_pct,
                    'exit_reason': reason
                })

                # Remove position
                del self.positions[symbol]

        except Exception as e:
            logger.error(f"❌ Error exiting {symbol}: {e}")

    # ============================================================
    # 🔄 MAIN TRADING LOOP
    # ============================================================

    async def trading_loop(self):
        """Main asynchronous trading loop"""
        logger.info("🚀 Starting trading loop...")
        self.is_running = True

        while self.is_running:
            try:
                # Update account state
                self._update_account_state()

                # Check kill switch
                if self.metrics.current_drawdown > self.risk_manager.max_account_risk:
                    logger.error(f"🚨 KILL SWITCH ACTIVATED - Drawdown: {self.metrics.current_drawdown:.2f}%")
                    await self.emergency_shutdown()
                    break

                # Scan for new signals if below position limit
                if len(self.positions) < MAX_CONCURRENT_POSITIONS:
                    signals = await self.scan_for_signals()

                    # Execute best signal
                    if signals:
                        best_symbol = max(signals.keys(),
                                        key=lambda x: signals[x]['strength'])
                        await self.execute_entry(best_symbol, signals[best_symbol])

                # Manage existing positions
                if self.positions:
                    await self.manage_positions()

                # Display status
                self._display_status()

                # Wait for next iteration
                await asyncio.sleep(SIGNAL_CHECK_INTERVAL)

            except Exception as e:
                logger.error(f"❌ Error in trading loop: {e}")
                await asyncio.sleep(60)  # Wait before retry

    async def emergency_shutdown(self):
        """Emergency shutdown procedure"""
        logger.warning("🚨 Executing emergency shutdown...")
        self.state = TradingState.STOPPED

        try:
            # Cancel all pending orders
            cancel_all_orders_hyperliquid(self.client)

            # Close all positions
            for symbol in list(self.positions.keys()):
                await self.execute_exit(symbol, "Emergency shutdown")

            # Kill switch
            universal_kill_switch(self.client)

            logger.info("✅ Emergency shutdown completed")

        except Exception as e:
            logger.error(f"❌ Error during emergency shutdown: {e}")

    # ============================================================
    # 📊 MONITORING & REPORTING
    # ============================================================

    def _display_status(self):
        """Display current bot status"""
        print("\n" + "="*60)
        print(f"🤖 CRYPTO MOMENTUM BOT STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

        print(f"State: {self.state.value}")
        print(f"Account Balance: ${self.account_balance:.2f}")
        print(f"Daily P&L: ${self.metrics.daily_pnl:.2f}")
        print(f"Current Drawdown: {self.metrics.current_drawdown:.2f}%")

        print("\n📊 Positions:")
        if self.positions:
            for symbol, pos in self.positions.items():
                print(f"  {symbol}: ${pos.unrealized_pnl:.2f} "
                      f"({pos.bars_held} bars)")
        else:
            print("  No open positions")

        print("\n📈 Performance:")
        print(f"  Total Trades: {self.metrics.total_trades}")
        print(f"  Win Rate: {self.metrics.win_rate:.1f}%")
        print(f"  Profit Factor: {self.metrics.profit_factor:.2f}")
        print(f"  Max Drawdown: {self.metrics.max_drawdown:.2f}%")
        print("="*60)

    def _record_trade(self, trade_data: Dict):
        """Record trade for analysis"""
        self.trade_history.append(trade_data)

        # Save to file
        if len(self.trade_history) % 10 == 0:
            self._save_trade_history()

    def _save_trade_history(self):
        """Save trade history to file"""
        try:
            filename = f"trades_{datetime.now().strftime('%Y%m%d')}.json"
            with open(filename, 'w') as f:
                json.dump(self.trade_history, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save trade history: {e}")

    def generate_performance_report(self) -> Dict:
        """Generate comprehensive performance report"""
        return {
            'timestamp': datetime.now().isoformat(),
            'account_balance': self.account_balance,
            'metrics': {
                'total_trades': self.metrics.total_trades,
                'win_rate': self.metrics.win_rate,
                'profit_factor': self.metrics.profit_factor,
                'sharpe_ratio': self.metrics.sharpe_ratio,
                'max_drawdown': self.metrics.max_drawdown,
                'total_pnl': self.metrics.total_pnl
            },
            'positions': {
                symbol: {
                    'unrealized_pnl': pos.unrealized_pnl,
                    'bars_held': pos.bars_held,
                    'entry_price': pos.entry_price
                }
                for symbol, pos in self.positions.items()
            },
            'recent_trades': self.trade_history[-10:] if self.trade_history else []
        }

    # ============================================================
    # 🚀 BOT EXECUTION
    # ============================================================

    async def start(self):
        """Start the trading bot"""
        logger.info("🚀 Starting Crypto Momentum Bot...")

        # Check production readiness
        readiness = production_readiness_check()
        if not readiness.get('ready'):
            logger.error("❌ System not production ready")
            return

        # Start trading loop
        await self.trading_loop()

    def stop(self):
        """Stop the trading bot"""
        logger.info("Stopping bot...")
        self.is_running = False
        self.state = TradingState.STOPPED


# ============================================================
# 🚀 MAIN EXECUTION
# ============================================================

async def main():
    """Main execution function"""
    print("🚀 CRYPTO MOMENTUM TRADING BOT 🚀")
    print("="*60)
    print(f"Trading Mode: {TRADING_MODE}")
    print(f"Assets: {', '.join(ASSET_CONFIGS.keys())}")
    print("="*60)

    # Create and start bot
    bot = CryptoMomentumBot()

    try:
        await bot.start()
    except KeyboardInterrupt:
        print("\n⚠️ Shutdown requested...")
        await bot.emergency_shutdown()
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        await bot.emergency_shutdown()
    finally:
        # Save final report
        report = bot.generate_performance_report()
        with open(f"final_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
            json.dump(report, f, indent=2)

        print("\n✅ Bot shutdown complete")
        print(f"Final P&L: ${bot.metrics.total_pnl:.2f}")


if __name__ == "__main__":
    # Run the bot
    asyncio.run(main())

# 🌙💫🚀 Bobby's Crypto Momentum Bot - Ready for Production! 🌙💫🚀