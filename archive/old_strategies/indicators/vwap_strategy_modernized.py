#!/usr/bin/env python3
"""
🚀 MODERNIZED VWAP Strategy - Phase 3 Integration
================================================

🌟 MIGRATION SHOWCASE: Legacy patterns → Modern @trading_functions
This strategy demonstrates the complete modernization from legacy ccxt
patterns to production-ready universal wrappers with comprehensive risk management.

🔄 MIGRATION PATTERNS DEMONSTRATED:
- Legacy ccxt.phemex() → create_universal_client()
- Legacy hardcoded keys → secure .env management
- Legacy direct API calls → universal_* wrapper functions
- No risk management → comprehensive position sizing and drawdown protection
- Basic VWAP → enhanced VWAP with modern indicators from @trading_functions

💫 Bobby's Trading Vision: Modern, Safe, Production-Ready VWAP Strategy
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
import os
from dotenv import load_dotenv

# 🛡️ MODERN: Import @trading_functions instead of legacy patterns
from trading_functions import (
    calculate_position_size,
    validate_trade_risk,
    check_drawdown_limits,
    production_readiness_check,
    DataQualityValidator,
    validate_data_source_quality,
    calculate_vwap,  # Modern VWAP calculation
    create_universal_client,
    universal_get_ask_bid,
    universal_get_positions,
    universal_monitor_pnl,
    universal_kill_switch,
    ExchangeType
)

print("🚀 MODERNIZED VWAP Strategy Loading with @trading_functions... 💫")

# ============================================================
# 🛡️ PRODUCTION: Modern Configuration Management
# ============================================================

# Strategy parameters
VWAP_PERIOD = 20
VWAP_DEVIATION_THRESHOLD = 0.002  # 0.2% deviation from VWAP
TAKE_PROFIT_PERCENT = 3.0
STOP_LOSS_PERCENT = 1.5

# 🛡️ PRODUCTION: Enhanced risk management parameters
RISK_PER_TRADE = 1.5       # Risk 1.5% of account per trade (conservative for VWAP)
MAX_DRAWDOWN = 12.0        # Maximum allowable drawdown %
POSITION_SIZE_METHOD = 'dynamic'  # 'fixed' or 'dynamic'
ACCOUNT_BALANCE = 100000   # Default account balance for position sizing
MIN_TRADE_SIZE = 100       # Minimum trade size
MAX_POSITION_SIZE = 0.20   # Maximum 20% of account per position (conservative for VWAP)

# 🛡️ MODERN: Secure credential management
load_dotenv()

class ModernizedVWAPStrategy(Strategy):
    """
    🛡️ MODERNIZED VWAP Strategy with Production-Ready Features

    MIGRATION SHOWCASE:
    - Modern risk management with dynamic position sizing
    - Universal exchange integration capabilities
    - Production readiness validation
    - Enhanced error handling and monitoring
    - Data quality validation integration

    Strategy Logic:
    - Buy when price is below VWAP by threshold (value opportunity)
    - Sell when price is above VWAP by threshold (overvalued)
    - Modern risk management with dynamic stops and position sizing
    """

    # Strategy parameters
    vwap_period = VWAP_PERIOD
    deviation_threshold = VWAP_DEVIATION_THRESHOLD
    take_profit = TAKE_PROFIT_PERCENT / 100
    stop_loss = STOP_LOSS_PERCENT / 100

    def init(self):
        """
        🛡️ PRODUCTION-READY Initialize indicators and risk management
        """
        # 🛡️ PRODUCTION: Validate production readiness
        print("🛡️ Validating VWAP strategy production readiness...")
        readiness = production_readiness_check()
        if not readiness.get('config_valid', False):
            print("⚠️ PRODUCTION: Configuration validation failed")
        else:
            print("✅ VWAP strategy production readiness validated")

        # 🛡️ PRODUCTION: Initialize risk management tracking
        self.max_drawdown_hit = False
        self.total_trades = 0
        self.winning_trades = 0

        # 🔄 MODERN: Use enhanced VWAP calculation from @trading_functions
        try:
            # Convert data to DataFrame for modern indicator calculation
            price_df = pd.DataFrame({
                'close': self.data.Close,
                'open': self.data.Open,
                'high': self.data.High,
                'low': self.data.Low,
                'volume': self.data.Volume
            }, index=self.data.index)

            # 🛡️ MODERN: Calculate VWAP using @trading_functions
            vwap_df = calculate_vwap(price_df, period=self.vwap_period)
            self.vwap = self.I(lambda: vwap_df['vwap'] if not vwap_df.empty else pd.Series(self.data.Close, index=self.data.index))

        except Exception as e:
            print(f"⚠️ Modern VWAP calculation failed, using fallback: {e}")
            # Fallback to simple VWAP calculation
            def simple_vwap(high, low, close, volume, period):
                typical_price = (high + low + close) / 3
                vwap_values = []
                for i in range(len(typical_price)):
                    if i < period - 1:
                        vwap_values.append(close[i])  # Use close for early values
                    else:
                        start_idx = max(0, i - period + 1)
                        period_typical = typical_price[start_idx:i+1]
                        period_volume = volume[start_idx:i+1]

                        if sum(period_volume) > 0:
                            vwap_val = sum(period_typical * period_volume) / sum(period_volume)
                        else:
                            vwap_val = period_typical.mean()
                        vwap_values.append(vwap_val)

                return pd.Series(vwap_values, index=typical_price.index)

            self.vwap = self.I(simple_vwap, self.data.High, self.data.Low, self.data.Close, self.data.Volume, self.vwap_period)

        print(f"✅ VWAP indicator initialized (Period: {self.vwap_period})")

    def next(self):
        """
        🛡️ PRODUCTION-READY Strategy logic with comprehensive risk management
        """
        # Only trade if we have enough data for VWAP
        if len(self.vwap) < self.vwap_period:
            return

        # 🛡️ PRODUCTION: Check drawdown limits before any trading
        if check_drawdown_limits(self.equity, max_drawdown_pct=MAX_DRAWDOWN):
            if not self.max_drawdown_hit:
                print(f"🛡️ PRODUCTION: VWAP - Maximum drawdown {MAX_DRAWDOWN}% reached - stopping trading")
                self.max_drawdown_hit = True
            return  # Stop trading if drawdown limit hit

        current_price = self.data.Close[-1]
        current_vwap = self.vwap[-1]

        # Calculate deviation from VWAP
        price_deviation = (current_price - current_vwap) / current_vwap

        # Entry conditions with production risk management
        if not self.position:
            # 🎯 BUY Signal: Price below VWAP by threshold (value opportunity)
            if price_deviation < -self.deviation_threshold:
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
                    print(f"🛡️ PRODUCTION: VWAP BUY Trade {self.total_trades} - "
                          f"Price: ${current_price:.2f}, VWAP: ${current_vwap:.2f}, "
                          f"Deviation: {price_deviation:.3f}, Size: {position_size:.4f}")
                else:
                    print(f"🛡️ PRODUCTION: VWAP BUY Trade rejected - risk validation failed")

        # Exit conditions with performance tracking
        else:
            # 🎯 SELL Signal: Price above VWAP by threshold (overvalued)
            if price_deviation > self.deviation_threshold:
                # 🛡️ PRODUCTION: Track winning trades for performance analysis
                if self.position.pl > 0:
                    self.winning_trades += 1
                    print(f"🛡️ PRODUCTION: VWAP Winning trade - "
                          f"Price: ${current_price:.2f}, VWAP: ${current_vwap:.2f}, "
                          f"Deviation: {price_deviation:.3f}, P&L: ${self.position.pl:.2f}")
                else:
                    print(f"🛡️ PRODUCTION: VWAP Losing trade - "
                          f"Price: ${current_price:.2f}, VWAP: ${current_vwap:.2f}, "
                          f"Deviation: {price_deviation:.3f}, P&L: ${self.position.pl:.2f}")

                self.sell()

        # 🛡️ PRODUCTION: Report win rate periodically
        if self.total_trades > 0 and self.total_trades % 5 == 0:
            win_rate = (self.winning_trades / self.total_trades) * 100
            print(f"🛡️ PRODUCTION: VWAP Win Rate - {self.winning_trades}/{self.total_trades} ({win_rate:.1f}%)")

# ============================================================
# 🛡️ PRODUCTION: Enhanced Testing with Data Validation
# ============================================================

def test_modernized_vwap_strategy():
    """
    🧪 Test the modernized VWAP strategy with data validation
    """
    print("\\n" + "="*80)
    print("🛡️ MODERNIZED VWAP STRATEGY TESTING")
    print("="*80)
    print("📊 Strategy Details:")
    print(f"   • VWAP Period: {VWAP_PERIOD}")
    print(f"   • Deviation Threshold: {VWAP_DEVIATION_THRESHOLD:.3f} ({VWAP_DEVIATION_THRESHOLD*100:.1f}%)")
    print(f"   • Risk Management: {STOP_LOSS_PERCENT}% SL, {TAKE_PROFIT_PERCENT}% TP")
    print(f"   • Risk per Trade: {RISK_PER_TRADE}% (dynamic position sizing)")
    print(f"   • Max Drawdown: {MAX_DRAWDOWN}% (automatic stop)")
    print(f"   • Position Sizing: {POSITION_SIZE_METHOD} (production-ready)")
    print("="*80)

    # 🛡️ PRODUCTION: Validate production readiness before testing
    print("\\n🛡️ Validating production readiness...")
    readiness = production_readiness_check()
    if not readiness.get('config_valid', False):
        print("❌ PRODUCTION: VWAP Strategy not ready for live deployment")
        print("🛡️ PRODUCTION: Continuing with backtesting only")
    else:
        print("✅ PRODUCTION: VWAP Strategy validated for live deployment")

    # 🛡️ MODERN: Load and validate data
    data_path = '/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv'

    print(f"\\n🛡️ Validating data quality: {data_path}")
    validator = DataQualityValidator()
    validation_result = validate_data_source_quality(data_path, validator)

    if validation_result.overall_score < 75:
        print(f"❌ SECURITY BLOCK: Data quality too low: {validation_result.overall_score}")
        print("🛡️ SECURITY: Preventing processing of potentially corrupted data")
        return

    print(f"✅ Data security validated - Quality score: {validation_result.overall_score}")

    # Load the data
    try:
        data = pd.read_csv(data_path, parse_dates=['Datetime'], index_col='Datetime')
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
        data = data.sort_index().dropna()

        print(f"📊 Data loaded: {len(data)} rows from {data.index[0]} to {data.index[-1]}")

        # Run backtest
        bt = Backtest(data, ModernizedVWAPStrategy, cash=ACCOUNT_BALANCE, commission=0.001)
        stats = bt.run()

        print("\\n" + "="*60)
        print("📈 MODERNIZED VWAP STRATEGY RESULTS:")
        print("="*60)
        print(stats)
        print("="*60)

        # 🎯 Production readiness assessment
        return_pct = stats.get('Return [%]', 0)
        sharpe_ratio = stats.get('Sharpe Ratio', 0)
        max_drawdown = stats.get('Max. Drawdown [%]', 100)
        win_rate = stats.get('Win Rate [%]', 0)
        total_trades = stats.get('# Trades', 0)

        print("\\n🛡️ PRODUCTION READINESS ASSESSMENT:")
        print(f"   • Return: {return_pct:.2f}% {'✅' if return_pct > 0 else '❌'}")
        print(f"   • Sharpe Ratio: {sharpe_ratio:.3f} {'✅' if sharpe_ratio > 0.5 else '❌'}")
        print(f"   • Max Drawdown: {max_drawdown:.2f}% {'✅' if max_drawdown < 25 else '❌'}")
        print(f"   • Win Rate: {win_rate:.1f}% {'✅' if win_rate > 35 else '❌'}")
        print(f"   • Total Trades: {total_trades} {'✅' if total_trades >= 10 else '❌'}")

        criteria_met = sum([
            return_pct > 0,
            sharpe_ratio > 0.5,
            max_drawdown < 25,
            win_rate > 35,
            total_trades >= 10
        ])

        if criteria_met >= 4:
            print("\\n🚀 PRODUCTION STATUS: READY for live deployment")
        elif criteria_met >= 3:
            print("\\n⚠️ PRODUCTION STATUS: NEEDS WORK - optimize before deployment")
        else:
            print("\\n❌ PRODUCTION STATUS: NOT READY - significant improvements needed")

        print("\\n✅ Modernized VWAP Strategy testing completed!")

    except Exception as e:
        print(f"❌ Error testing strategy: {e}")

# ============================================================
# 🎯 MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    print("=" * 80)
    print("🛡️ PHASE 3 MIGRATION: VWAP Strategy Modernization")
    print("=" * 80)
    print("🔄 MODERNIZATION FEATURES:")
    print("   ✅ Legacy ccxt → @trading_functions universal wrappers")
    print("   ✅ Hardcoded keys → secure .env credential management")
    print("   ✅ Basic VWAP → enhanced VWAP with modern indicators")
    print("   ✅ No risk management → comprehensive position sizing and drawdown protection")
    print("   ✅ No validation → production readiness checks and data validation")
    print("   ✅ Basic strategy → production-ready monitoring and tracking")
    print("=" * 80)
    print("💫 Ready to demonstrate Phase 3 VWAP strategy modernization!")
    print("=" * 80)

    # Run the modernized strategy test
    test_modernized_vwap_strategy()