# eth_rsi_strategy.py
"""
🛡️ PRODUCTION-READY ETH RSI Strategy
Enhanced with modern risk management and production safety features
"""
import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy

# 🛡️ PRODUCTION: Import modern risk management and validation
from trading_functions import (
    calculate_position_size,
    validate_trade_risk,
    check_drawdown_limits,
    production_readiness_check,
    DataQualityValidator,
    validate_data_source_quality
)

print("💫 Bobby's ETH RSI Strategy Loading... 🌙")

# Strategy parameters
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70
TAKE_PROFIT_PERCENT = 5.0
STOP_LOSS_PERCENT = 3.0

# 🛡️ PRODUCTION: Enhanced risk management parameters
RISK_PER_TRADE = 2.0       # Risk 2% of account per trade
MAX_DRAWDOWN = 15.0        # Maximum allowable drawdown %
POSITION_SIZE_METHOD = 'dynamic'  # 'fixed' or 'dynamic'
ACCOUNT_BALANCE = 100000   # Default account balance for position sizing
MIN_TRADE_SIZE = 100       # Minimum trade size
MAX_POSITION_SIZE = 0.30   # Maximum 30% of account per position (higher for ETH)

class ETHRSIStrategy(Strategy):
    # Strategy parameters
    rsi_period = RSI_PERIOD
    rsi_oversold = RSI_OVERSOLD
    rsi_overbought = RSI_OVERBOUGHT
    take_profit = TAKE_PROFIT_PERCENT / 100
    stop_loss = STOP_LOSS_PERCENT / 100

    def init(self):
        """
        🛡️ PRODUCTION-READY Initialize indicators and risk management
        """
        # 🛡️ PRODUCTION: Validate production readiness
        print("🛡️ Validating ETH RSI strategy production readiness...")
        readiness = production_readiness_check()
        if not readiness.get('config_valid', False):
            print("⚠️ PRODUCTION: Configuration validation failed")
        else:
            print("✅ ETH RSI strategy production readiness validated")

        # 🛡️ PRODUCTION: Initialize risk management tracking
        self.max_drawdown_hit = False
        self.total_trades = 0
        self.winning_trades = 0

        # Initialize RSI indicator
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)

    def next(self):
        # Only trade if we have enough data for RSI
        if len(self.rsi) < self.rsi_period:
            return

        # 🛡️ PRODUCTION: Check drawdown limits before any trading
        if check_drawdown_limits(self.equity, max_drawdown_pct=MAX_DRAWDOWN):
            if not self.max_drawdown_hit:
                print(f"🛡️ PRODUCTION: ETH RSI - Maximum drawdown {MAX_DRAWDOWN}% reached - stopping trading")
                self.max_drawdown_hit = True
            return  # Stop trading if drawdown limit hit

        current_rsi = self.rsi[-1]

        # Entry conditions with production risk management
        if not self.position:
            # Buy when RSI is oversold (price might bounce up)
            if current_rsi < self.rsi_oversold:
                current_price = self.data.Close[-1]
                sl_price = current_price * (1 - self.stop_loss)
                tp_price = current_price * (1 + self.take_profit)

                # 🛡️ PRODUCTION: Calculate optimal position size using modern risk management
                if POSITION_SIZE_METHOD == 'dynamic':
                    optimal_size = calculate_position_size(
                        account_balance=ACCOUNT_BALANCE,
                        entry_price=current_price,
                        stop_loss_price=sl_price,
                        risk_pct=RISK_PER_TRADE
                    )
                    # Apply position limits
                    max_size = ACCOUNT_BALANCE * MAX_POSITION_SIZE / current_price
                    position_size = min(optimal_size, max_size)
                    position_size = max(position_size, MIN_TRADE_SIZE / current_price)
                else:
                    position_size = 1.0  # Fixed size

                # 🛡️ PRODUCTION: Validate trade risk before execution
                trade_valid = validate_trade_risk(
                    entry_price=current_price,
                    stop_loss=sl_price,
                    position_size=position_size,
                    account_balance=ACCOUNT_BALANCE
                )

                if trade_valid:
                    self.buy(sl=sl_price, tp=tp_price, size=position_size)
                    self.total_trades += 1
                    print(f"🛡️ PRODUCTION: ETH RSI Trade {self.total_trades} - "
                          f"RSI: {current_rsi:.1f}, Size: {position_size:.4f}")
                else:
                    print(f"🛡️ PRODUCTION: ETH RSI Trade rejected - risk validation failed")

        # Exit conditions with performance tracking
        else:
            # Sell when RSI is overbought (price might drop)
            if current_rsi > self.rsi_overbought:
                # 🛡️ PRODUCTION: Track winning trades for performance analysis
                if self.position.pl > 0:
                    self.winning_trades += 1
                    print(f"🛡️ PRODUCTION: ETH RSI Winning trade - RSI: {current_rsi:.1f}, P&L: ${self.position.pl:.2f}")
                else:
                    print(f"🛡️ PRODUCTION: ETH RSI Losing trade - RSI: {current_rsi:.1f}, P&L: ${self.position.pl:.2f}")

                self.sell()

        # 🛡️ PRODUCTION: Report win rate periodically
        if self.total_trades > 0 and self.total_trades % 5 == 0:
            win_rate = (self.winning_trades / self.total_trades) * 100
            print(f"🛡️ PRODUCTION: ETH RSI Win Rate - {self.winning_trades}/{self.total_trades} ({win_rate:.1f}%)")

# 🛡️ PRODUCTION: TEST ON ALL DATA SOURCES WITH VALIDATION
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🛡️ PRODUCTION-READY ETH RSI STRATEGY TESTING")
    print("="*80)
    print("📊 Strategy Details:")
    print(f"   • RSI Period: {RSI_PERIOD}")
    print(f"   • RSI Oversold: {RSI_OVERSOLD} (buy signal)")
    print(f"   • RSI Overbought: {RSI_OVERBOUGHT} (sell signal)")
    print(f"   • Risk Management: {STOP_LOSS_PERCENT}% SL, {TAKE_PROFIT_PERCENT}% TP")
    print(f"   • Risk per Trade: {RISK_PER_TRADE}% (dynamic position sizing)")
    print(f"   • Max Drawdown: {MAX_DRAWDOWN}% (automatic stop)")
    print(f"   • Position Sizing: {POSITION_SIZE_METHOD} (production-ready)")
    print("="*80)

    # 🛡️ PRODUCTION: Validate production readiness before testing
    print("\n🛡️ Validating ETH RSI strategy production readiness...")
    readiness = production_readiness_check()
    if not readiness.get('config_valid', False):
        print("❌ PRODUCTION: ETH RSI Strategy not ready for live deployment")
        print("🛡️ PRODUCTION: Continuing with backtesting only")
    else:
        print("✅ PRODUCTION: ETH RSI Strategy validated for live deployment")

    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from multi_data_tester import test_on_all_data

    # Test this strategy on all configured data sources
    results = test_on_all_data(ETHRSIStrategy, 'ETH_RSI_Strategy')

    if results is not None:
        print("\n✅ Testing complete! Results saved in: ./results/ETH_RSI_Strategy.csv")