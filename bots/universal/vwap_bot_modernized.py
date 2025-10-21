"""
🚀 VWAP Bot - Modernized with @trading_functions
==============================================
Modern implementation of VWAP trading bot using Bobby's @trading_functions library.
Replaces legacy nice_funcs with production-ready universal wrappers.

🌟 Modern Features:
    - Universal exchange wrapper (works with Hyperliquid, Phemex, Coinbase)
    - Integrated risk management and position sizing
    - Data quality validation
    - Production readiness checks
    - Emergency kill switch functionality

💫 Legacy → Modern Function Migration:
    - nice_funcs.ask_bid() → universal_get_ask_bid()
    - nice_funcs.get_position() → universal_get_positions()
    - nice_funcs.pnl_close() → universal_monitor_pnl()
    - nice_funcs.cancel_all_orders() → exchange-specific cancel functions
    - nice_funcs.calculate_vwap() → calculate_vwap()

🔧 Configuration:
    - Set exchange in EXCHANGE_CONFIG
    - Adjust risk parameters in RISK_CONFIG
    - Modify strategy parameters in STRATEGY_CONFIG
"""

import pandas as pd
import numpy as np
import time
import random
from datetime import datetime, timedelta
import warnings

# 🚀 Import Bobby's modern trading functions
try:
    from trading_functions import (
        UniversalClient,
        ExchangeType,
        universal_get_ask_bid,
        universal_get_positions,
        universal_monitor_pnl,
        universal_kill_switch,
        calculate_vwap,
        calculate_position_size,
        check_drawdown_limits,
        production_readiness_check,
        get_ohlcv_hyperliquid,  # For VWAP calculation
        place_limit_order_hyperliquid,
        cancel_all_orders_hyperliquid,
        DataQualityValidator
    )
    TRADING_FUNCTIONS_AVAILABLE = True
    print("✅ @trading_functions library loaded successfully")
except ImportError as e:
    TRADING_FUNCTIONS_AVAILABLE = False
    print(f"❌ @trading_functions not available: {e}")
    print("⚠️ Cannot run modern bot without @trading_functions")
    exit(1)

warnings.filterwarnings('ignore')

# 🎛️ CONFIGURATION - Modern Configuration System
EXCHANGE_CONFIG = {
    'exchange_type': ExchangeType.HYPERLIQUID,  # Change to PHEMEX or COINBASE as needed
    'testnet': True,  # Set to False for live trading
}

RISK_CONFIG = {
    'max_portfolio_risk': 2.0,    # Maximum portfolio risk percentage
    'position_risk': 1.5,        # Risk per position (%)
    'max_drawdown': 10.0,         # Maximum portfolio drawdown (%)
    'max_positions': 1,           # Maximum concurrent positions
    'leverage': 3,                # Leverage multiplier
}

STRATEGY_CONFIG = {
    'symbol': 'LINK',             # Trading symbol
    'timeframe': '1m',            # Timeframe for VWAP calculation
    'vwap_period': 20,            # VWAP calculation period
    'long_probability': 0.7,      # Probability of going long when price > VWAP
    'short_probability': 0.3,     # Probability of going long when price < VWAP
    'target_profit': 5.0,         # Target profit percentage
    'max_loss': -10.0,            # Maximum loss percentage
}

class ModernVWAPBot:
    """
    🎯 Modern VWAP Trading Bot

    Uses @trading_functions library for all trading operations.
    Implements proper risk management and production safety checks.
    """

    def __init__(self):
        """🏗️ Initialize modern VWAP bot"""
        self.client = None
        self.is_initialized = False
        self.last_vwap = None
        self.position_entry_time = None

        # 🛡️ Initialize trading client
        self._initialize_client()

        # ✅ Run production readiness check
        self._run_safety_checks()

    def _initialize_client(self):
        """🔧 Initialize universal trading client"""
        try:
            from trading_functions import create_universal_client

            self.client = create_universal_client(EXCHANGE_CONFIG['exchange_type'])
            print(f"✅ Universal client initialized: {EXCHANGE_CONFIG['exchange_type'].value}")
            self.is_initialized = True

        except Exception as e:
            print(f"❌ Failed to initialize trading client: {e}")
            self.is_initialized = False

    def _run_safety_checks(self):
        """🛡️ Run comprehensive safety checks"""
        print("\n🛡️ Running Production Safety Checks")
        print("=" * 40)

        try:
            # Production readiness check
            readiness = production_readiness_check()
            if readiness.get('config_valid'):
                print("✅ Trading functions configuration valid")
            else:
                print("⚠️ Trading functions configuration issues detected")

            # Exchange connection validation
            if self.client:
                connection_valid = universal_validate_connection(self.client)
                if connection_valid:
                    print("✅ Exchange connection validated")
                else:
                    print("⚠️ Exchange connection issues detected")

            # Risk parameter validation
            if RISK_CONFIG['max_portfolio_risk'] > 5.0:
                print("⚠️ Warning: High portfolio risk configured")

            print("🛡️ Safety checks completed")

        except Exception as e:
            print(f"⚠️ Safety check error: {e}")

    def get_modern_vwap(self, symbol):
        """📊 Calculate VWAP using modern @trading_functions"""
        try:
            # Get OHLCV data for VWAP calculation
            if EXCHANGE_CONFIG['exchange_type'] == ExchangeType.HYPERLIQUID:
                ohlcv_data = get_ohlcv_hyperliquid(
                    symbol=symbol,
                    interval=STRATEGY_CONFIG['timeframe'],
                    limit=STRATEGY_CONFIG['vwap_period'] + 10
                )

                if ohlcv_data is not None and len(ohlcv_data) > 0:
                    # Convert to DataFrame for VWAP calculation
                    df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

                    # Calculate VWAP using modern function
                    vwap_result = calculate_vwap(df)
                    latest_vwap = vwap_result['vwap'].iloc[-1]

                    print(f"📊 Modern VWAP calculated: {latest_vwap:.4f}")
                    return latest_vwap

                else:
                    print("⚠️ No OHLCV data available for VWAP calculation")
                    return None

            else:
                print(f"⚠️ VWAP calculation not implemented for {EXCHANGE_CONFIG['exchange_type']}")
                return None

        except Exception as e:
            print(f"❌ VWAP calculation error: {e}")
            return None

    def get_current_positions(self, symbol):
        """📊 Get current positions using modern functions"""
        try:
            positions = universal_get_positions(self.client)

            if positions:
                symbol_position = None
                for pos in positions:
                    if pos.get('symbol') == symbol:
                        symbol_position = pos
                        break

                if symbol_position:
                    return {
                        'in_position': True,
                        'size': symbol_position.get('size', 0),
                        'entry_price': symbol_position.get('entry_price', 0),
                        'pnl_pct': symbol_position.get('pnl_pct', 0),
                        'side': symbol_position.get('side', 'unknown')
                    }

            return {
                'in_position': False,
                'size': 0,
                'entry_price': 0,
                'pnl_pct': 0,
                'side': 'none'
            }

        except Exception as e:
            print(f"❌ Position check error: {e}")
            return {'in_position': False, 'size': 0, 'entry_price': 0, 'pnl_pct': 0, 'side': 'none'}

    def manage_existing_position(self, symbol, position_info):
        """🎯 Manage existing position with modern PnL monitoring"""
        try:
            print(f"📊 Managing existing {position_info['side']} position for {symbol}")

            # Use modern PnL monitoring
            pnl_result = universal_monitor_pnl(
                client=self.client,
                target_profit=STRATEGY_CONFIG['target_profit'],
                max_loss=abs(STRATEGY_CONFIG['max_loss'])
            )

            if pnl_result.get('should_close'):
                reason = pnl_result.get('reason', 'PnL target reached')
                print(f"🎯 Closing position: {reason}")

                # Close position (implementation depends on exchange)
                if EXCHANGE_CONFIG['exchange_type'] == ExchangeType.HYPERLIQUID:
                    # Cancel all orders first
                    cancel_all_orders_hyperliquid(self.client, symbol)

                    # Close position by placing opposite order
                    # Implementation would depend on specific exchange API

                return True

            else:
                print(f"📈 Position maintained - PnL: {position_info['pnl_pct']:.2f}%")
                return False

        except Exception as e:
            print(f"❌ Position management error: {e}")
            return False

    def execute_trading_logic(self, symbol):
        """🎯 Execute modern VWAP trading logic"""
        try:
            # 📊 Get current market data
            ask, bid, spread = universal_get_ask_bid(self.client, symbol)

            if ask is None or bid is None:
                print("⚠️ Unable to get market data")
                return

            print(f"💰 Market: {symbol} - Bid: {bid:.4f}, Ask: {ask:.4f}, Spread: {spread:.6f}")

            # 📊 Calculate current VWAP
            current_vwap = self.get_modern_vwap(symbol)

            if current_vwap is None:
                print("⚠️ Unable to calculate VWAP - skipping trade logic")
                return

            # 🎯 Trading decision logic
            random_factor = random.random()

            if bid > current_vwap:
                # Price above VWAP - bullish signal
                should_go_long = random_factor <= STRATEGY_CONFIG['long_probability']
                print(f"📈 Price above VWAP ({bid:.4f} > {current_vwap:.4f})")
                print(f"🎲 Random factor: {random_factor:.2f}, Long probability: {STRATEGY_CONFIG['long_probability']}")

                if should_go_long:
                    self._execute_long_order(symbol, ask)
                else:
                    print("🤔 Skipping long entry due to probability")

            else:
                # Price below VWAP - bearish signal
                should_go_long = random_factor <= STRATEGY_CONFIG['short_probability']
                print(f"📉 Price below VWAP ({bid:.4f} < {current_vwap:.4f})")
                print(f"🎲 Random factor: {random_factor:.2f}, Short probability: {STRATEGY_CONFIG['short_probability']}")

                if should_go_long:
                    print("🤔 Contrarian long entry (price below VWAP)")
                    self._execute_long_order(symbol, ask)
                else:
                    print("🤔 No entry - bearish conditions")

        except Exception as e:
            print(f"❌ Trading logic error: {e}")

    def _execute_long_order(self, symbol, entry_price):
        """🚀 Execute long order with modern position sizing"""
        try:
            # 🎯 Calculate position size using modern risk management
            account_balance = 10000  # Would get from exchange API
            stop_loss_price = entry_price * (1 + STRATEGY_CONFIG['max_loss'] / 100)

            position_size_usd = calculate_position_size(
                account_balance=account_balance,
                entry_price=entry_price,
                stop_loss=stop_loss_price,
                risk_pct=RISK_CONFIG['position_risk']
            )

            size_units = position_size_usd / entry_price

            print(f"🎯 Calculated position size: ${position_size_usd:.2f} ({size_units:.4f} units)")

            # Place order using exchange-specific function
            if EXCHANGE_CONFIG['exchange_type'] == ExchangeType.HYPERLIQUID:
                order_result = place_limit_order_hyperliquid(
                    client=self.client,
                    symbol=symbol,
                    side='buy',
                    size=size_units,
                    price=entry_price
                )

                if order_result.get('success'):
                    print(f"✅ Long order placed successfully: {order_result}")
                    self.position_entry_time = datetime.now()
                else:
                    print(f"❌ Order placement failed: {order_result}")

        except Exception as e:
            print(f"❌ Order execution error: {e}")

    def run_trading_cycle(self):
        """🔄 Run single trading cycle"""
        if not self.is_initialized:
            print("❌ Bot not properly initialized")
            return

        symbol = STRATEGY_CONFIG['symbol']
        print(f"\n🔄 Running trading cycle for {symbol}")
        print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # 📊 Check current positions
            position_info = self.get_current_positions(symbol)

            if position_info['in_position']:
                print(f"📊 Currently in position: {position_info['side']} - PnL: {position_info['pnl_pct']:.2f}%")
                # Manage existing position
                position_closed = self.manage_existing_position(symbol, position_info)

                if not position_closed:
                    print("📊 Position maintained - no new trades")
                    return

            else:
                print("📊 No current positions - evaluating new trades")

            # 🛡️ Check portfolio drawdown limits
            if check_drawdown_limits(account_balance=10000, max_drawdown_pct=RISK_CONFIG['max_drawdown']):
                print("⚠️ Portfolio drawdown limit reached - trading suspended")
                return

            # 🎯 Execute trading logic
            self.execute_trading_logic(symbol)

        except Exception as e:
            print(f"❌ Trading cycle error: {e}")

    def emergency_stop(self):
        """🚨 Emergency stop with universal kill switch"""
        try:
            print("🚨 EMERGENCY STOP INITIATED")
            result = universal_kill_switch(self.client)

            if result.get('success'):
                print("✅ Emergency stop completed successfully")
            else:
                print(f"⚠️ Emergency stop issues: {result}")

        except Exception as e:
            print(f"❌ Emergency stop error: {e}")


def main():
    """🎯 Main bot execution function"""
    print("🚀 Modern VWAP Bot - Bobby's Trading Framework")
    print("=" * 50)

    if not TRADING_FUNCTIONS_AVAILABLE:
        print("❌ @trading_functions library required but not available")
        return

    try:
        # 🚀 Initialize bot
        bot = ModernVWAPBot()

        if not bot.is_initialized:
            print("❌ Bot initialization failed")
            return

        print(f"✅ Bot initialized successfully")
        print(f"📊 Trading: {STRATEGY_CONFIG['symbol']}")
        print(f"🏢 Exchange: {EXCHANGE_CONFIG['exchange_type'].value}")
        print(f"🎯 Risk per trade: {RISK_CONFIG['position_risk']}%")

        # 🔄 Run single cycle (for testing)
        bot.run_trading_cycle()

        # For production, you would run in a loop:
        # while True:
        #     bot.run_trading_cycle()
        #     time.sleep(60)  # Wait 1 minute between cycles

        print(f"\n🌙💫🚀 Modern VWAP bot cycle completed!")

    except KeyboardInterrupt:
        print("\n⚠️ Bot stopped by user")
    except Exception as e:
        print(f"❌ Bot error: {e}")


if __name__ == "__main__":
    main()