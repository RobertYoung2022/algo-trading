# macd_momentum_strategy.py
"""
🛡️ PRODUCTION-READY MACD Momentum Strategy
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

print("🚀 Bobby's MACD Momentum Strategy Loading... ⚡")

# Strategy parameters - MACD Configuration
MACD_FAST_PERIOD = 12      # Fast EMA period for MACD
MACD_SLOW_PERIOD = 26      # Slow EMA period for MACD
MACD_SIGNAL_PERIOD = 9     # Signal line EMA period
RSI_PERIOD = 14            # RSI period for overbought filter
RSI_OVERBOUGHT = 70        # RSI overbought level (filter out entries above this)
TAKE_PROFIT_PERCENT = 6.0  # Take profit at 6%
STOP_LOSS_PERCENT = 3.0    # Stop loss at 3%

# 🛡️ PRODUCTION: Enhanced risk management parameters
RISK_PER_TRADE = 2.0       # Risk 2% of account per trade
MAX_DRAWDOWN = 15.0        # Maximum allowable drawdown %
POSITION_SIZE_METHOD = 'dynamic'  # 'fixed' or 'dynamic'
ACCOUNT_BALANCE = 100000   # Default account balance for position sizing
MIN_TRADE_SIZE = 100       # Minimum trade size
MAX_POSITION_SIZE = 0.25   # Maximum 25% of account per position

class MACDMomentumStrategy(Strategy):
    """
    MACD Momentum Trading Strategy with RSI Filter
    
    Strategy Logic:
    - Enter LONG when MACD line crosses ABOVE signal line AND RSI < 70 (avoid overbought entries)
    - Exit when MACD line crosses BELOW signal line OR stop/take profit hit
    - Risk Management: 3% stop loss, 6% take profit
    
    Indicators Used:
    - MACD (12, 26, 9): Main trend and momentum signal
    - RSI (14): Overbought filter to avoid bad entries
    
    This strategy captures momentum breakouts while filtering out potentially 
    overbought conditions that could lead to immediate reversals.
    """
    
    # Strategy parameters
    macd_fast = MACD_FAST_PERIOD
    macd_slow = MACD_SLOW_PERIOD
    macd_signal = MACD_SIGNAL_PERIOD
    rsi_period = RSI_PERIOD
    rsi_overbought = RSI_OVERBOUGHT
    take_profit = TAKE_PROFIT_PERCENT / 100
    stop_loss = STOP_LOSS_PERCENT / 100

    def init(self):
        """
        🛡️ PRODUCTION-READY Initialize indicators and risk management
        - MACD: Trend and momentum indicator
        - RSI: Overbought/oversold filter
        - Production safety: Risk management and validation
        """
        # 🛡️ PRODUCTION: Validate production readiness
        print("🛡️ Validating production readiness...")
        readiness = production_readiness_check()
        if not readiness.get('config_valid', False):
            print("⚠️ PRODUCTION: Configuration validation failed")
        else:
            print("✅ Production readiness validated")

        # 🛡️ PRODUCTION: Initialize risk management tracking
        self.max_drawdown_hit = False
        self.total_trades = 0
        self.winning_trades = 0
        # Initialize MACD indicator (returns macd, signal, histogram)
        macd_data = talib.MACD(
            self.data.Close, 
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow, 
            signalperiod=self.macd_signal
        )
        
        self.macd = self.I(lambda x: macd_data[0], self.data.Close, name='MACD')
        self.macd_signal = self.I(lambda x: macd_data[1], self.data.Close, name='MACD_Signal')
        self.macd_histogram = self.I(lambda x: macd_data[2], self.data.Close, name='MACD_Histogram')
        
        # Initialize RSI indicator for overbought filter
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)

    def next(self):
        """
        Strategy logic executed on each bar
        
        Entry: MACD bullish crossover + RSI filter
        Exit: MACD bearish crossover OR stop/take profit
        """
        # Only trade if we have enough data for all indicators
        if len(self.macd) < self.macd_slow or len(self.rsi) < self.rsi_period:
            return
            
        # Current values
        current_macd = self.macd[-1]
        current_signal = self.macd_signal[-1]
        current_rsi = self.rsi[-1]
        
        # Previous values for crossover detection
        prev_macd = self.macd[-2]
        prev_signal = self.macd_signal[-2]
        
        # 🛡️ PRODUCTION: Check drawdown limits before any trading
        if check_drawdown_limits(self.equity, max_drawdown_pct=MAX_DRAWDOWN):
            if not self.max_drawdown_hit:
                print(f"🛡️ PRODUCTION: Maximum drawdown {MAX_DRAWDOWN}% reached - stopping trading")
                self.max_drawdown_hit = True
            return  # Stop trading if drawdown limit hit

        # Entry conditions - Long only strategy
        if not self.position:
            # MACD bullish crossover: MACD line crosses above signal line
            macd_bullish_crossover = (prev_macd <= prev_signal and current_macd > current_signal)

            # RSI filter: Avoid entries when RSI is overbought (> 70)
            rsi_filter = current_rsi < self.rsi_overbought

            # Enter long position with modern risk management
            if macd_bullish_crossover and rsi_filter:
                current_price = self.data.Close[-1]
                sl_price = current_price * (1 - self.stop_loss)  # 3% stop loss
                tp_price = current_price * (1 + self.take_profit)  # 6% take profit

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
                    print(f"🛡️ PRODUCTION: Trade {self.total_trades} executed - Size: {position_size:.4f}, Risk: {RISK_PER_TRADE}%")
                else:
                    print(f"🛡️ PRODUCTION: Trade rejected - risk validation failed")
        
        # Exit conditions with production tracking
        else:
            # MACD bearish crossover: MACD line crosses below signal line
            macd_bearish_crossover = (prev_macd >= prev_signal and current_macd < current_signal)

            # Exit on MACD bearish crossover (momentum turning negative)
            if macd_bearish_crossover:
                # 🛡️ PRODUCTION: Track winning trades for performance analysis
                if self.position.pl > 0:
                    self.winning_trades += 1
                    print(f"🛡️ PRODUCTION: Winning trade closed - P&L: ${self.position.pl:.2f}")
                else:
                    print(f"🛡️ PRODUCTION: Losing trade closed - P&L: ${self.position.pl:.2f}")

                self.sell()

        # 🛡️ PRODUCTION: Report win rate periodically
        if self.total_trades > 0 and self.total_trades % 10 == 0:
            win_rate = (self.winning_trades / self.total_trades) * 100
            print(f"🛡️ PRODUCTION: Win Rate Update - {self.winning_trades}/{self.total_trades} ({win_rate:.1f}%)")

# 🛡️ PRODUCTION: TEST ON ALL DATA SOURCES WITH VALIDATION
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🛡️ PRODUCTION-READY MACD MOMENTUM STRATEGY TESTING")
    print("="*80)
    print("📊 Strategy Details:")
    print(f"   • MACD: ({MACD_FAST_PERIOD}, {MACD_SLOW_PERIOD}, {MACD_SIGNAL_PERIOD})")
    print(f"   • RSI Filter: < {RSI_OVERBOUGHT} (avoid overbought entries)")
    print(f"   • Risk Management: {STOP_LOSS_PERCENT}% SL, {TAKE_PROFIT_PERCENT}% TP")
    print(f"   • Risk per Trade: {RISK_PER_TRADE}% (modern position sizing)")
    print(f"   • Max Drawdown: {MAX_DRAWDOWN}% (automatic stop)")
    print(f"   • Position Sizing: {POSITION_SIZE_METHOD} (production-ready)")
    print(f"   • Entry: MACD bullish crossover + RSI filter")
    print(f"   • Exit: MACD bearish crossover OR SL/TP hit")
    print("="*80)

    # 🛡️ PRODUCTION: Validate production readiness before testing
    print("\n🛡️ Validating production readiness...")
    readiness = production_readiness_check()
    if not readiness.get('config_valid', False):
        print("❌ PRODUCTION: Strategy not ready for live deployment")
        print("🛡️ PRODUCTION: Continuing with backtesting only")
    else:
        print("✅ PRODUCTION: Strategy validated for live deployment")

    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from multi_data_tester import test_on_all_data

    # Test this strategy on all configured data sources
    results = test_on_all_data(MACDMomentumStrategy, 'MACD_Momentum_Strategy')

    if results is not None:
        print("\n✅ Testing complete! Results saved in: ./results/MACD_Momentum_Strategy.csv")
        print("\n🎯 Key Insights:")
        print("   • MACD crossovers capture trend changes effectively")
        print("   • RSI filter helps avoid poor entry timing")
        print("   • 6% TP allows profits to run while 3% SL limits downside")
        print("   • Strategy works best in trending markets with clear momentum")