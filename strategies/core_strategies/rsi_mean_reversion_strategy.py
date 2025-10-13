"""
🚀 RSI Mean Reversion Strategy - Counter-Trend Trading
====================================================
Core strategy template using @trading_functions library for mean reversion trading.
Uses RSI oversold/overbought conditions with dynamic position sizing.

🌟 Strategy Logic:
    - RSI < 30 (oversold) = BUY signal
    - RSI > 70 (overbought) = SELL signal
    - Mean reversion exits when RSI returns to neutral zone (40-60)
    - Modern risk management and position sizing

💫 Data Requirements:
    - OHLCV data with quality score ≥75
    - Works best on shorter timeframes (1h, 4h)
    - Minimum 500 bars for RSI calculation

🔧 Bobby's Modern Trading Framework Integration:
    - Uses @trading_functions for RSI calculation
    - Integrated volatility-based position sizing
    - Production readiness validation
    - Multi-asset testing capability
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
import warnings

# 🚀 Import Bobby's modern trading functions
try:
    from trading_functions import (
        calculate_rsi,
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

class RSIMeanReversionStrategy(Strategy):
    """
    🎯 RSI Mean Reversion Strategy

    Modern implementation using @trading_functions library with:
    - Dynamic position sizing based on RSI divergence strength
    - Volatility-adjusted risk management
    - Production readiness validation
    - Multi-timeframe compatibility
    """

    # 🎛️ Strategy parameters (optimizable)
    rsi_period = 14        # RSI calculation period
    rsi_oversold = 30      # Oversold threshold (buy signal)
    rsi_overbought = 70    # Overbought threshold (sell signal)
    rsi_neutral_low = 40   # Exit threshold for long positions
    rsi_neutral_high = 60  # Exit threshold for short positions
    risk_pct = 1.5         # Risk per trade (%)
    max_hold_periods = 20  # Maximum holding periods for mean reversion

    def init(self):
        """🏗️ Initialize strategy indicators using @trading_functions"""

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

        # 🎯 Calculate RSI using modern functions
        if TRADING_FUNCTIONS_AVAILABLE:
            try:
                # Use @trading_functions for RSI calculation
                close_data = pd.Series(self.data.Close, name='Close')
                rsi_series = calculate_rsi(close_data, period=self.rsi_period)

                # Wrap for backtesting.py framework
                self.rsi = self.I(lambda: rsi_series)

                print(f"✅ Modern RSI calculation: Period({self.rsi_period})")

            except Exception as e:
                print(f"⚠️ Modern RSI calculation failed: {e}")
                # Fallback to basic implementation
                self._init_basic_rsi()
        else:
            self._init_basic_rsi()

        # 📊 Additional indicators for confluence
        self.entry_bar = None  # Track entry bar for max hold period

    def _init_basic_rsi(self):
        """📉 Fallback RSI calculation"""
        def rsi_calc(close, period=14):
            delta = pd.Series(close).diff()
            gain = delta.where(delta > 0, 0).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            return 100 - (100 / (1 + rs))

        self.rsi = self.I(rsi_calc, self.data.Close, self.rsi_period)
        print(f"📉 Basic RSI fallback: Period({self.rsi_period})")

    def next(self):
        """🎯 Execute mean reversion trading logic"""

        # 🛡️ Safety checks
        if len(self.data) < self.rsi_period + 1:
            return

        # 📊 Get current values
        current_price = self.data.Close[-1]
        current_rsi = self.rsi[-1]

        # Skip if RSI is NaN
        if pd.isna(current_rsi):
            return

        # 🔄 Position management
        if not self.position:
            # 📈 BUY signal: RSI oversold (mean reversion opportunity)
            if current_rsi < self.rsi_oversold:
                # 🎯 Calculate position size with mean reversion logic
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        # More aggressive sizing for stronger oversold conditions
                        oversold_strength = (self.rsi_oversold - current_rsi) / self.rsi_oversold
                        adjusted_risk = self.risk_pct * (1 + oversold_strength * 0.5)  # Up to 50% more risk

                        # Conservative stop loss for mean reversion (price typically bounces)
                        stop_loss_price = current_price * 0.95  # 5% stop loss

                        position_result = calculate_position_size(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss_price=stop_loss_price,
                            risk_pct=min(adjusted_risk, 3.0)  # Cap at 3%
                        )

                        size_fraction = min(position_result['position_value'] / self.equity, 0.9)  # Max 90% equity

                    except Exception as e:
                        print(f"⚠️ Position sizing error: {e}")
                        size_fraction = 0.015  # Fallback to 1.5% of equity
                else:
                    size_fraction = 0.015  # Basic 1.5% risk for mean reversion

                # 🚀 Enter long position (mean reversion buy)
                self.buy(size=size_fraction)
                self.entry_bar = len(self.data)  # Track entry time

        elif self.position and self.position.is_long:
            # 📊 Long position management
            bars_held = len(self.data) - self.entry_bar if self.entry_bar else 0

            # 📈 Profit taking: RSI returns to neutral zone (mean reversion complete)
            if current_rsi >= self.rsi_neutral_high:
                self.position.close()

            # ⏰ Max hold period exit (mean reversion didn't occur)
            elif bars_held >= self.max_hold_periods:
                self.position.close()

            # 🛡️ Emergency exit: RSI becomes extremely overbought
            elif current_rsi >= 85:
                self.position.close()

        # 📉 Short selling logic (optional - can be disabled for long-only strategy)
        if not self.position:
            # 📉 SELL signal: RSI overbought (mean reversion opportunity)
            if current_rsi > self.rsi_overbought:
                # Similar logic but for shorting
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        overbought_strength = (current_rsi - self.rsi_overbought) / (100 - self.rsi_overbought)
                        adjusted_risk = self.risk_pct * (1 + overbought_strength * 0.5)

                        stop_loss_price = current_price * 1.05  # 5% stop for short

                        position_result = calculate_position_size(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss_price=stop_loss_price,
                            risk_pct=min(adjusted_risk, 3.0)
                        )

                        size_fraction = min(position_result['position_value'] / self.equity, 0.9)

                    except Exception as e:
                        size_fraction = 0.015
                else:
                    size_fraction = 0.015

                # 🔻 Enter short position (mean reversion sell) - Uncomment to enable
                # self.sell(size=size_fraction)
                # self.entry_bar = len(self.data)

        elif self.position and self.position.is_short:
            # 📊 Short position management
            bars_held = len(self.data) - self.entry_bar if self.entry_bar else 0

            # 📉 Cover short: RSI returns to neutral zone
            if current_rsi <= self.rsi_neutral_low:
                self.position.close()

            # ⏰ Max hold period exit
            elif bars_held >= self.max_hold_periods:
                self.position.close()

            # 🛡️ Emergency exit: RSI becomes extremely oversold
            elif current_rsi <= 15:
                self.position.close()


def test_rsi_strategy_single_asset(data_path, symbol='BTC', display_stats=True):
    """
    🧪 Test RSI Mean Reversion strategy on single asset

    Args:
        data_path: Path to CSV data file
        symbol: Asset symbol for labeling
        display_stats: Whether to display full backtesting stats
    """
    try:
        # 📊 Load and validate data
        print(f"\n🔍 Testing RSI Mean Reversion Strategy on {symbol}")
        print(f"📁 Data: {data_path}")

        if TRADING_FUNCTIONS_AVAILABLE:
            df = pd.read_csv(data_path)
            validation_result = validate_data_source_quality(df)
            print(f"📊 Data Quality Score: {validation_result.quality_score}")
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
        bt = Backtest(df, RSIMeanReversionStrategy, cash=10000, commission=0.002)
        stats = bt.run()

        # 📊 Display comprehensive results
        if display_stats:
            print(f"\n🎯 RSI Mean Reversion Results for {symbol}")
            print("=" * 55)
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


def run_optimization_test(data_path, symbol='BTC'):
    """🔧 Run parameter optimization for RSI strategy"""
    try:
        print(f"\n🔧 Running RSI Strategy Optimization for {symbol}")

        df = pd.read_csv(data_path)
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df.set_index('Datetime', inplace=True)
        elif 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)

        bt = Backtest(df, RSIMeanReversionStrategy, cash=10000, commission=0.002)

        # 🎯 Optimize key parameters
        optimization_results = bt.optimize(
            rsi_period=range(10, 21, 2),        # RSI period: 10, 12, 14, 16, 18, 20
            rsi_oversold=range(25, 36, 5),      # Oversold: 25, 30, 35
            rsi_overbought=range(65, 76, 5),    # Overbought: 65, 70, 75
            maximize='Sharpe Ratio',
            constraint=lambda p: p.rsi_oversold < p.rsi_overbought - 20  # Ensure sufficient gap
        )

        print(f"\n🏆 Optimization Results for {symbol}")
        print("=" * 40)
        print(optimization_results)

        return optimization_results

    except Exception as e:
        print(f"❌ Optimization error for {symbol}: {e}")
        return None


if __name__ == "__main__":
    """🧪 Strategy testing and validation"""

    # 🛡️ Production readiness check
    print("\n🛡️ RSI Strategy Production Readiness Check")
    print("=" * 50)

    if TRADING_FUNCTIONS_AVAILABLE:
        try:
            readiness = production_readiness_check()
            print(f"📊 Config Valid: {readiness.get('config_valid', False)}")
            if readiness.get('config_valid'):
                print("✅ Strategy ready for testing")
            else:
                print("⚠️ Configuration needs validation")
        except Exception as e:
            print(f"⚠️ Readiness check error: {e}")

    # 🧪 Test on sample data
    sample_data_paths = [
        ("/Users/bobbyyo/Projects/algo-fun/data/yahoo/BTCUSD-20yr-yahoo-data.csv", "BTC-Yahoo"),
        ("/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250911_043600_historical.csv", "ETH-Historical")
    ]

    print("\n🧪 Running RSI Mean Reversion Strategy Tests...")

    for data_path, label in sample_data_paths:
        try:
            # Regular backtest
            stats = test_rsi_strategy_single_asset(data_path, label)
            if stats:
                print(f"✅ {label} test completed - Sharpe: {stats['Sharpe Ratio']:.2f}")

                # Run optimization on first dataset
                if label == "BTC-Yahoo":
                    opt_results = run_optimization_test(data_path, label)

            else:
                print(f"❌ {label} test failed")
        except Exception as e:
            print(f"⚠️ {label} test error: {e}")

    print("\n🌙💫🚀 RSI Mean Reversion Strategy testing complete!")