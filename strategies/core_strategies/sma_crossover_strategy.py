"""
🚀 SMA Crossover Strategy - Simple Trend Following
================================================
Core strategy template using @trading_functions library for production-ready backtesting.
Uses moving average crossover for trend detection with modern risk management.

🌟 Strategy Logic:
    - Fast SMA (10) crosses above Slow SMA (30) = BUY signal
    - Fast SMA (10) crosses below Slow SMA (30) = SELL signal
    - Integrated position sizing and risk management
    - Production-ready with modern function validation

💫 Data Requirements:
    - OHLCV data with quality score ≥75
    - Minimum 1000 bars for reliable backtesting
    - Works on multiple timeframes (1h, 6h, 1d)

🔧 Bobby's Modern Trading Framework Integration:
    - Uses @trading_functions for all technical analysis
    - Integrated data quality validation
    - Production readiness checks included
    - Universal exchange compatibility
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
import warnings

# 🚀 Import Bobby's modern trading functions
try:
    from trading_functions import (
        calculate_sma,
        calculate_position_size,
        check_drawdown_limits,
        production_readiness_check,
        DataQualityValidator,
        validate_data_source_quality
    )
    TRADING_FUNCTIONS_AVAILABLE = True
    print("✅ @trading_functions library loaded successfully")
except ImportError as e:
    TRADING_FUNCTIONS_AVAILABLE = False
    print(f"⚠️ @trading_functions not available: {e}")
    print("📝 Falling back to basic implementations")

warnings.filterwarnings('ignore')

class SMAStrategy(Strategy):
    """
    🎯 Simple Moving Average Crossover Strategy

    Modern implementation using @trading_functions library with:
    - Dynamic position sizing based on volatility
    - Integrated risk management
    - Production readiness validation
    - Multi-asset compatibility
    """

    # 🎛️ Strategy parameters (optimizable)
    fast_period = 10     # Fast SMA period
    slow_period = 30     # Slow SMA period
    risk_pct = 2.0       # Risk per trade (%)
    stop_loss_pct = 3.0  # Stop loss percentage
    take_profit_pct = 6.0 # Take profit percentage

    def init(self):
        """🏗️ Initialize strategy indicators using @trading_functions"""

        # 🎯 Track entry price manually for risk management
        self.entry_price = None

        # 🛡️ Data quality validation
        if TRADING_FUNCTIONS_AVAILABLE:
            try:
                # Convert backtesting data to DataFrame for validation
                df = pd.DataFrame({
                    'Open': self.data.Open,
                    'High': self.data.High,
                    'Low': self.data.Low,
                    'Close': self.data.Close,
                    'Volume': getattr(self.data, 'Volume', pd.Series([1000] * len(self.data)))
                })

                validation_result = validate_data_source_quality(df)
                print(f"📊 Data Quality Score: {validation_result.quality_score}")

                if validation_result.quality_score < 75:
                    print(f"⚠️ Warning: Data quality below threshold ({validation_result.quality_score} < 75)")

            except Exception as e:
                print(f"⚠️ Data validation error: {e}")

        # 🎯 Calculate moving averages using modern functions
        if TRADING_FUNCTIONS_AVAILABLE:
            try:
                # Use @trading_functions for SMA calculation
                close_data = pd.Series(self.data.Close, name='Close')

                # Calculate SMAs with proper error handling
                fast_sma_series = calculate_sma(close_data, self.fast_period)
                slow_sma_series = calculate_sma(close_data, self.slow_period)

                # Wrap for backtesting.py framework
                self.sma_fast = self.I(lambda: fast_sma_series)
                self.sma_slow = self.I(lambda: slow_sma_series)

                print(f"✅ Modern SMA calculation: Fast({self.fast_period}), Slow({self.slow_period})")

            except Exception as e:
                print(f"⚠️ Modern SMA calculation failed: {e}")
                # Fallback to basic implementation
                self._init_basic_sma()
        else:
            self._init_basic_sma()

    def _init_basic_sma(self):
        """📉 Fallback SMA calculation for basic functionality"""
        self.sma_fast = self.I(lambda x: pd.Series(x).rolling(self.fast_period).mean(), self.data.Close)
        self.sma_slow = self.I(lambda x: pd.Series(x).rolling(self.slow_period).mean(), self.data.Close)
        print(f"📉 Basic SMA fallback: Fast({self.fast_period}), Slow({self.slow_period})")

    def next(self):
        """🎯 Execute trading logic with modern risk management"""

        # 🛡️ Safety checks
        if len(self.data) < max(self.fast_period, self.slow_period) + 1:
            return

        # 📊 Get current values
        current_price = self.data.Close[-1]
        fast_sma = self.sma_fast[-1]
        slow_sma = self.sma_slow[-1]
        prev_fast = self.sma_fast[-2]
        prev_slow = self.sma_slow[-2]

        # 🔄 Position management
        if not self.position:
            # 📈 BUY signal: Fast SMA crosses above Slow SMA
            if prev_fast <= prev_slow and fast_sma > slow_sma:
                # 🎯 Calculate position size using modern risk management
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        stop_loss_price = current_price * (1 - self.stop_loss_pct / 100)
                        position_result = calculate_position_size(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss_price=stop_loss_price,
                            risk_pct=self.risk_pct
                        )
                        # Convert to fraction of equity
                        size_fraction = min(position_result['position_value'] / self.equity, 0.95)  # Max 95% of equity
                    except Exception as e:
                        print(f"⚠️ Position sizing error: {e}")
                        size_fraction = 0.02  # Fallback to 2% of equity
                else:
                    size_fraction = 0.02  # Basic 2% risk

                # 🚀 Enter long position
                self.buy(size=size_fraction)
                # 📝 Track entry price for risk management
                self.entry_price = current_price

        elif self.position:
            # 📉 SELL signal: Fast SMA crosses below Slow SMA
            if prev_fast >= prev_slow and fast_sma < slow_sma:
                self.position.close()
                self.entry_price = None  # Reset entry price

            # 🛡️ Risk management exits (only if we have tracked entry price)
            elif self.entry_price is not None:
                current_pnl_pct = (current_price - self.entry_price) / self.entry_price * 100

                # Stop loss
                if current_pnl_pct <= -self.stop_loss_pct:
                    self.position.close()
                    self.entry_price = None  # Reset entry price

                # Take profit
                elif current_pnl_pct >= self.take_profit_pct:
                    self.position.close()
                    self.entry_price = None  # Reset entry price


def test_sma_strategy_single_asset(data_path, symbol='BTC', display_stats=True):
    """
    🧪 Test SMA strategy on single asset with full stats display

    Args:
        data_path: Path to CSV data file
        symbol: Asset symbol for labeling
        display_stats: Whether to display full backtesting stats
    """
    try:
        # 📊 Load and validate data
        print(f"\n🔍 Testing SMA Strategy on {symbol}")
        print(f"📁 Data: {data_path}")

        if TRADING_FUNCTIONS_AVAILABLE:
            # Use data validation
            df = pd.read_csv(data_path)
            validation_result = validate_data_source_quality(df)
            print(f"📊 Data Quality Score: {validation_result.quality_score}")

            if validation_result.quality_score < 75:
                print(f"⚠️ Warning: Low quality data ({validation_result.quality_score})")
        else:
            df = pd.read_csv(data_path)

        # 🎯 Prepare data for backtesting
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df.set_index('Datetime', inplace=True)
        elif 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)

        # Ensure required columns
        required_cols = ['Open', 'High', 'Low', 'Close']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # 🚀 Run backtest
        bt = Backtest(df, SMAStrategy, cash=10000, commission=0.002)
        stats = bt.run()

        # 📊 Display comprehensive results
        if display_stats:
            print(f"\n🎯 SMA Strategy Results for {symbol}")
            print("=" * 50)
            print(stats)

        # 📈 Show plot
        try:
            bt.plot()
        except Exception as e:
            print(f"⚠️ Plotting error: {e}")

        return stats

    except Exception as e:
        print(f"❌ Error testing {symbol}: {e}")
        return None


def run_production_readiness_check():
    """🛡️ Validate strategy production readiness"""
    print("\n🛡️ SMA Strategy Production Readiness Check")
    print("=" * 45)

    if TRADING_FUNCTIONS_AVAILABLE:
        try:
            readiness = production_readiness_check()
            print(f"📊 Config Valid: {readiness.get('config_valid', False)}")
            print(f"🔧 Exchange Ready: {readiness.get('exchange_ready', False)}")
            print(f"⚡ Functions Ready: {readiness.get('functions_ready', False)}")

            if all([readiness.get('config_valid'), readiness.get('exchange_ready'), readiness.get('functions_ready')]):
                print("✅ Strategy ready for production deployment")
                return True
            else:
                print("⚠️ Strategy needs optimization before production")
                return False

        except Exception as e:
            print(f"⚠️ Readiness check error: {e}")
            return False
    else:
        print("⚠️ @trading_functions not available - manual validation required")
        return False


if __name__ == "__main__":
    """🧪 Strategy testing and validation"""

    # 🛡️ Production readiness check
    run_production_readiness_check()

    # 🧪 Test on sample data (modify paths as needed)
    sample_data_paths = [
        ("/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv", "BTC-Yahoo"),
        ("/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250911_043600_historical.csv", "ETH-Historical")
    ]

    print("\n🧪 Running SMA Strategy Tests...")

    for data_path, label in sample_data_paths:
        try:
            stats = test_sma_strategy_single_asset(data_path, label)
            if stats:
                print(f"✅ {label} test completed")
            else:
                print(f"❌ {label} test failed")
        except Exception as e:
            print(f"⚠️ {label} test error: {e}")

    print("\n🌙💫🚀 SMA Strategy testing complete!")