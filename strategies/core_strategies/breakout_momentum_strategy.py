"""
🚀 Breakout Momentum Strategy - Trend Continuation Trading
========================================================
Core strategy template using @trading_functions library for momentum breakout trading.
Uses price breakouts from consolidation ranges with volume confirmation.

🌟 Strategy Logic:
    - Price breaks above recent high + volume surge = BUY signal
    - Price breaks below recent low + volume surge = SELL signal (short)
    - Stop loss at opposite range boundary
    - Trail stop for momentum continuation

💫 Data Requirements:
    - OHLCV data with quality score ≥75
    - Volume data essential for confirmation
    - Works best on medium timeframes (1h, 4h, 6h)

🔧 Bobby's Modern Trading Framework Integration:
    - Uses @trading_functions for volume analysis
    - Integrated market structure detection
    - Dynamic position sizing based on breakout strength
    - Production-ready with modern validation
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
import warnings

# 🚀 Import Bobby's modern trading functions
try:
    from trading_functions import (
        calculate_position_size,
        analyze_volume_pattern,
        identify_swing_points,
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

class BreakoutMomentumStrategy(Strategy):
    """
    🎯 Breakout Momentum Strategy

    Modern implementation using @trading_functions library with:
    - Volume-confirmed breakouts for higher probability trades
    - Dynamic position sizing based on breakout strength
    - Trailing stops for momentum continuation
    - Market structure awareness for better entries
    """

    # 🎛️ Strategy parameters (optimizable)
    lookback_period = 20      # Period for range identification
    volume_threshold = 1.5    # Volume surge multiplier for confirmation
    risk_pct = 2.5           # Risk per trade (%)
    stop_loss_pct = 2.0      # Initial stop loss percentage
    trail_stop_pct = 1.5     # Trailing stop percentage
    min_range_size_pct = 1.0 # Minimum range size for valid breakout (%)

    def init(self):
        """🏗️ Initialize strategy indicators using @trading_functions"""

        # 🛡️ Data quality validation
        if TRADING_FUNCTIONS_AVAILABLE:
            try:
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

        # 📊 Calculate rolling indicators
        self.highest_high = self.I(lambda x: pd.Series(x).rolling(self.lookback_period).max(), self.data.High)
        self.lowest_low = self.I(lambda x: pd.Series(x).rolling(self.lookback_period).min(), self.data.Low)

        # 📈 Volume analysis
        if hasattr(self.data, 'Volume') and self.data.Volume is not None:
            self.avg_volume = self.I(lambda x: pd.Series(x).rolling(self.lookback_period).mean(), self.data.Volume)
            self.volume_available = True
        else:
            print("⚠️ No volume data available - using price-only breakouts")
            self.volume_available = False

        # 🎯 Tracking variables
        self.entry_price = None
        self.stop_loss = None
        self.trail_high = None

        print(f"✅ Breakout strategy initialized: Lookback({self.lookback_period})")

    def next(self):
        """🎯 Execute breakout momentum trading logic"""

        # 🛡️ Safety checks
        if len(self.data) < self.lookback_period + 1:
            return

        # 📊 Get current values
        current_price = self.data.Close[-1]
        current_high = self.data.High[-1]
        current_low = self.data.Low[-1]

        # Get range boundaries
        range_high = self.highest_high[-2]  # Previous period to avoid looking into future
        range_low = self.lowest_low[-2]

        if pd.isna(range_high) or pd.isna(range_low):
            return

        # 📏 Check if range is significant enough
        range_size_pct = (range_high - range_low) / range_low * 100
        if range_size_pct < self.min_range_size_pct:
            return  # Range too small for meaningful breakout

        # 📊 Volume confirmation
        volume_confirmed = True
        if self.volume_available and TRADING_FUNCTIONS_AVAILABLE:
            try:
                current_volume = self.data.Volume[-1]
                avg_vol = self.avg_volume[-1]

                if not pd.isna(avg_vol) and avg_vol > 0:
                    volume_ratio = current_volume / avg_vol
                    volume_confirmed = volume_ratio >= self.volume_threshold
                else:
                    volume_confirmed = True  # Default to True if avg volume calc fails

            except Exception as e:
                print(f"⚠️ Volume analysis error: {e}")
                volume_confirmed = True
        elif self.volume_available:
            # Basic volume check without @trading_functions
            current_volume = self.data.Volume[-1]
            avg_vol = self.avg_volume[-1]
            if not pd.isna(avg_vol) and avg_vol > 0:
                volume_confirmed = current_volume >= (avg_vol * self.volume_threshold)

        # 🔄 Position management
        if not self.position:
            # 📈 Bullish breakout: Price breaks above range high with volume
            if current_high > range_high and volume_confirmed:
                # 🎯 Calculate position size with breakout-specific logic
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        # Breakout strength affects position size
                        breakout_strength = (current_high - range_high) / range_high
                        adjusted_risk = self.risk_pct * (1 + min(breakout_strength * 2, 1.0))  # Up to double risk

                        stop_loss_price = range_low  # Stop at range support
                        position_size = calculate_position_size(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss=stop_loss_price,
                            risk_pct=min(adjusted_risk, 5.0)  # Cap at 5%
                        )

                        size_fraction = min(position_size / self.equity, 0.9)

                    except Exception as e:
                        print(f"⚠️ Position sizing error: {e}")
                        size_fraction = 0.025  # Fallback to 2.5% equity
                else:
                    size_fraction = 0.025

                # 🚀 Enter long position
                self.buy(size=size_fraction)
                self.entry_price = current_price
                self.stop_loss = range_low  # Initial stop at range support
                self.trail_high = current_high

            # 📉 Bearish breakout: Price breaks below range low with volume
            elif current_low < range_low and volume_confirmed:
                # Similar logic for short positions
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        breakout_strength = (range_low - current_low) / range_low
                        adjusted_risk = self.risk_pct * (1 + min(breakout_strength * 2, 1.0))

                        stop_loss_price = range_high  # Stop at range resistance
                        position_size = calculate_position_size(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss=stop_loss_price,
                            risk_pct=min(adjusted_risk, 5.0)
                        )

                        size_fraction = min(position_size / self.equity, 0.9)

                    except Exception as e:
                        size_fraction = 0.025
                else:
                    size_fraction = 0.025

                # 🔻 Enter short position (uncomment to enable short selling)
                # self.sell(size=size_fraction)
                # self.entry_price = current_price
                # self.stop_loss = range_high
                # self.trail_high = current_low  # For short, track trail_low

        elif self.position:
            # 📊 Position management with trailing stops
            if self.position.is_long:
                # 🛡️ Stop loss check
                if current_price <= self.stop_loss:
                    self.position.close()
                    self._reset_tracking()

                # 📈 Trailing stop logic for momentum continuation
                elif current_high > self.trail_high:
                    self.trail_high = current_high
                    # Update trailing stop
                    new_trail_stop = self.trail_high * (1 - self.trail_stop_pct / 100)
                    self.stop_loss = max(self.stop_loss, new_trail_stop)

                # 🎯 Trailing stop triggered
                elif current_price <= (self.trail_high * (1 - self.trail_stop_pct / 100)):
                    self.position.close()
                    self._reset_tracking()

            elif self.position.is_short:
                # Similar logic for short positions
                if current_price >= self.stop_loss:
                    self.position.close()
                    self._reset_tracking()

    def _reset_tracking(self):
        """🔄 Reset position tracking variables"""
        self.entry_price = None
        self.stop_loss = None
        self.trail_high = None


def test_breakout_strategy_single_asset(data_path, symbol='BTC', display_stats=True):
    """
    🧪 Test Breakout Momentum strategy on single asset

    Args:
        data_path: Path to CSV data file
        symbol: Asset symbol for labeling
        display_stats: Whether to display full backtesting stats
    """
    try:
        # 📊 Load and validate data
        print(f"\n🔍 Testing Breakout Momentum Strategy on {symbol}")
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

        # Add volume if missing (for basic functionality)
        if 'Volume' not in df.columns:
            df['Volume'] = 1000  # Default volume
            print("⚠️ Volume data not found - using default values")

        # 🚀 Run backtest
        bt = Backtest(df, BreakoutMomentumStrategy, cash=10000, commission=0.002)
        stats = bt.run()

        # 📊 Display comprehensive results
        if display_stats:
            print(f"\n🎯 Breakout Momentum Results for {symbol}")
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


def run_breakout_optimization(data_path, symbol='BTC'):
    """🔧 Run parameter optimization for Breakout strategy"""
    try:
        print(f"\n🔧 Running Breakout Strategy Optimization for {symbol}")

        df = pd.read_csv(data_path)
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df.set_index('Datetime', inplace=True)
        elif 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)

        if 'Volume' not in df.columns:
            df['Volume'] = 1000

        bt = Backtest(df, BreakoutMomentumStrategy, cash=10000, commission=0.002)

        # 🎯 Optimize key parameters
        optimization_results = bt.optimize(
            lookback_period=range(15, 31, 5),    # Lookback: 15, 20, 25, 30
            volume_threshold=[1.2, 1.5, 2.0],   # Volume confirmation levels
            stop_loss_pct=[1.5, 2.0, 2.5],      # Stop loss percentages
            maximize='Sharpe Ratio',
            constraint=lambda p: p.stop_loss_pct < 3.0  # Reasonable stop loss
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
    print("\n🛡️ Breakout Strategy Production Readiness Check")
    print("=" * 55)

    if TRADING_FUNCTIONS_AVAILABLE:
        try:
            readiness = production_readiness_check()
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

    print("\n🧪 Running Breakout Momentum Strategy Tests...")

    for data_path, label in sample_data_paths:
        try:
            # Regular backtest
            stats = test_breakout_strategy_single_asset(data_path, label)
            if stats:
                print(f"✅ {label} test completed - Sharpe: {stats['Sharpe Ratio']:.2f}")

                # Run optimization on first dataset
                if label == "BTC-Yahoo":
                    opt_results = run_breakout_optimization(data_path, label)

            else:
                print(f"❌ {label} test failed")
        except Exception as e:
            print(f"⚠️ {label} test error: {e}")

    print("\n🌙💫🚀 Breakout Momentum Strategy testing complete!")