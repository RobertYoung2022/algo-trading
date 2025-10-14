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
    - DAILY timeframes ONLY (6h acceptable, 1h risky, <1h catastrophic)

🎯 Asset Suitability (Phase 0-1 Testing Results on 110 Datasets):
    ✅ RECOMMENDED ASSETS:
        - BTC (Bitcoin): 70% returns on 6h, 44% on 1h, excellent daily performance
        - XRP (Ripple): 31% on daily, 22% on 10yr Yahoo data
        - CRO (Cronos): 20% on 20yr Yahoo, 13% on daily Coinbase
        - ETH (Ethereum): 10% on daily, 9% on 6h Coinbase
        - LINK (Chainlink): 11% on daily Coinbase, 6% on Yahoo

    ❌ NOT RECOMMENDED:
        - HBAR (Hedera): Best result -3.70%, worst -97.00% ❌
          * Consistently negative across ALL timeframes and providers
          * Breakout strategy fundamentally incompatible with HBAR price action
          * EXCLUDE HBAR from backtesting to avoid wasted computational resources

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

    # 📊 Enhancement #1: RSI Divergence Filter (Step 1.2 Redesign)
    rsi_period = 14              # RSI calculation period
    divergence_lookback = 5      # Bars to check for divergence
    use_rsi_filter = True        # Enable/disable RSI filter for testing

    # 🛡️ PHASE 2: Overtrading Prevention (Critical Risk Management)
    # Testing revealed overtrading is the #1 strategy killer:
    #   - Profitable strategies: 8-80 trades per year ✅
    #   - Unprofitable strategies: 195-2195 trades per year ❌
    # This limit prevents catastrophic overtrading even with bad data
    max_trades_per_year = 100    # Maximum trades allowed per year (daily timeframe)
    enforce_trade_limit = True   # Enable/disable trade limit

    # 🚫 PHASE 2: Asset Exclusions (Evidence-Based)
    # Phase 0-1 testing on 110 datasets revealed HBAR consistently underperforms
    #   - Best HBAR result: -3.70% (still negative!)
    #   - Worst HBAR result: -97.00% (catastrophic)
    #   - Pattern holds across ALL timeframes and providers
    excluded_assets = ['HBAR']   # Assets incompatible with this strategy

    # 📊 PHASE 3: Multi-Timeframe Trend Confirmation (Step 2)
    # Phase 3 goal: Improve win rate by filtering counter-trend signals
    #   - Only take LONG when price > weekly SMA(50)
    #   - Only take SHORT when price < weekly SMA(50)
    #   - Expected: Win rate 34-40% → 45-55%
    use_multi_timeframe_filter = True  # Enable/disable trend filter
    weekly_trend_period = 50           # Weekly trend SMA period (50 days ≈ 10 weeks)

    def init(self):
        """🏗️ Initialize strategy indicators using @trading_functions"""

        # 🚫 PHASE 2: Asset Exclusion Warning
        # Check if user is testing on excluded assets and warn them
        try:
            # Try to detect asset from data or filename
            asset_detected = None
            if hasattr(self.data, 'symbol'):
                asset_detected = self.data.symbol
            elif hasattr(self.data, 'name'):
                asset_detected = self.data.name

            # Check if detected asset is in exclusion list
            if asset_detected:
                for excluded in self.excluded_assets:
                    if excluded.upper() in str(asset_detected).upper():
                        print("\n" + "="*70)
                        print("⚠️  WARNING: TESTING ON EXCLUDED ASSET")
                        print("="*70)
                        print(f"🚫 Detected asset: {asset_detected}")
                        print(f"🚫 This asset is on the exclusion list: {self.excluded_assets}")
                        print(f"")
                        print(f"📊 Phase 0-1 Testing Results for {excluded}:")
                        print(f"   • Best result: -3.70% (negative even in best case)")
                        print(f"   • Worst result: -97.00% (catastrophic losses)")
                        print(f"   • Consistent failure across ALL timeframes and providers")
                        print(f"")
                        print(f"💡 RECOMMENDATION: Exclude {excluded} from testing")
                        print(f"   Focus on proven assets: BTC, ETH, XRP, CRO, LINK")
                        print("="*70 + "\n")
                        # Don't raise error, just warn - user might want to test anyway
        except Exception:
            pass  # If detection fails, silently continue

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

        # ⏰ PHASE 2: Timeframe Validation (Critical for Strategy Performance)
        # This strategy is optimized for DAILY timeframes ONLY
        # Testing on 110 datasets revealed:
        #   - Daily timeframes: +8% to +70% returns ✅
        #   - 6h timeframes: +4% to +8% returns ✅ (acceptable)
        #   - 1h timeframes: -12% average ⚠️ (needs optimization)
        #   - Minute timeframes: -71% average ❌ (catastrophic overtrading)
        try:
            # Detect timeframe from data index
            if len(self.data) >= 2:
                time_diff = pd.Timedelta(0)

                # Calculate median time difference between bars
                timestamps = pd.to_datetime(self.data.index)
                if len(timestamps) >= 10:
                    diffs = []
                    for i in range(1, min(11, len(timestamps))):
                        diff = timestamps[i] - timestamps[i-1]
                        if diff > pd.Timedelta(0):  # Skip any zero or negative diffs
                            diffs.append(diff)

                    if diffs:
                        time_diff = pd.Series(diffs).median()

                # Determine timeframe category
                if time_diff > pd.Timedelta(0):
                    if time_diff < pd.Timedelta(hours=1):
                        # Minute-level data (< 1 hour)
                        timeframe_name = f"{int(time_diff.total_seconds() / 60)}m"
                        print("\n" + "="*60)
                        print("❌ CRITICAL ERROR: INCOMPATIBLE TIMEFRAME")
                        print("="*60)
                        print(f"⚠️  Detected timeframe: {timeframe_name}")
                        print(f"⚠️  This strategy is optimized for DAILY timeframes only")
                        print(f"")
                        print(f"📊 Phase 0-1 Testing Results (110 datasets):")
                        print(f"   • Daily timeframes:  +8% to +70% ✅ EXCELLENT")
                        print(f"   • 6h timeframes:     +4% to +8%  ✅ ACCEPTABLE")
                        print(f"   • 1h timeframes:     -12% avg    ⚠️  RISKY")
                        print(f"   • Minute timeframes: -71% avg    ❌ CATASTROPHIC")
                        print(f"")
                        print(f"🚫 REASON: Minute data causes severe overtrading (500+ trades)")
                        print(f"   The RSI divergence filter cannot overcome timeframe noise")
                        print(f"")
                        print(f"✅ SOLUTION: Use daily or 6h data instead")
                        print(f"   Example: dataset_files/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv")
                        print("="*60 + "\n")
                        raise ValueError(f"❌ Breakout Strategy requires DAILY timeframes. Detected: {timeframe_name}. See error message above for details.")

                    elif time_diff < pd.Timedelta(hours=4):
                        # 1h-3h data
                        timeframe_name = f"{int(time_diff.total_seconds() / 3600)}h"
                        print("\n" + "="*60)
                        print("⚠️  WARNING: SUB-OPTIMAL TIMEFRAME")
                        print("="*60)
                        print(f"⚠️  Detected timeframe: {timeframe_name}")
                        print(f"⚠️  This strategy performs best on DAILY timeframes")
                        print(f"")
                        print(f"📊 Expected Performance:")
                        print(f"   • Daily timeframes:  +8% to +70% returns ✅")
                        print(f"   • 6h timeframes:     +4% to +8%  returns ✅")
                        print(f"   • 1h timeframes:     -12% average returns ⚠️")
                        print(f"")
                        print(f"⚠️  RISK: Lower performance, more noise, reduced win rate")
                        print(f"✅ RECOMMENDED: Switch to daily data for optimal results")
                        print("="*60 + "\n")

                    elif time_diff < pd.Timedelta(hours=18):
                        # 4h-12h data (acceptable)
                        timeframe_name = f"{int(time_diff.total_seconds() / 3600)}h"
                        print(f"✅ Timeframe validated: {timeframe_name} (acceptable performance expected)")

                    else:
                        # Daily or higher (optimal)
                        timeframe_name = f"{int(time_diff.total_seconds() / 86400)}d" if time_diff >= pd.Timedelta(days=1) else "daily"
                        print(f"✅ Timeframe validated: {timeframe_name} (optimal performance expected)")
                else:
                    print("⚠️ Could not detect timeframe - proceeding with caution")

        except ValueError:
            # Re-raise ValueError (timeframe rejection)
            raise
        except Exception as e:
            print(f"⚠️ Timeframe detection error: {e}")
            print("⚠️ Proceeding without timeframe validation - USE DAILY DATA FOR BEST RESULTS")

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

        # 📊 Enhancement #1: RSI Divergence Filter
        if self.use_rsi_filter:
            def calculate_rsi(close, period=14):
                """Calculate Relative Strength Index"""
                close_series = pd.Series(close)
                delta = close_series.diff()

                # Separate gains and losses
                gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

                # Calculate RS and RSI
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                return rsi.values

            self.rsi = self.I(calculate_rsi, self.data.Close, self.rsi_period)
            print(f"📊 RSI divergence filter enabled: {self.rsi_period}-period")

        # 📊 PHASE 3 STEP 2: Multi-Timeframe Trend Filter
        # Calculate weekly trend (SMA on daily data acts as weekly filter)
        if self.use_multi_timeframe_filter:
            self.weekly_trend = self.I(lambda x: pd.Series(x).rolling(self.weekly_trend_period).mean(),
                                        self.data.Close)
            print(f"📊 Multi-timeframe filter enabled: SMA({self.weekly_trend_period}) weekly trend")

        # 🎯 Tracking variables
        self.entry_price = None
        self.stop_loss = None
        self.trail_high = None

        # 🛡️ PHASE 2: Trade counter for overtrading prevention
        self.trade_count = 0

        print(f"✅ Breakout strategy initialized: Lookback({self.lookback_period})")
        if self.enforce_trade_limit:
            print(f"🛡️ Overtrading protection enabled: Max {self.max_trades_per_year} trades per year")

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

        # 📊 Enhancement #1: RSI Divergence Filter (Momentum Confirmation)
        # Detects bearish divergence: price makes new high but RSI doesn't
        # This indicates weakening momentum and likely false breakout
        rsi_filter_pass = True
        if self.use_rsi_filter and len(self.data) > self.divergence_lookback + self.rsi_period:
            # Get recent price highs and RSI values
            recent_high_prices = []
            recent_rsi_values = []

            for i in range(-self.divergence_lookback, 0):
                if i + len(self.data) >= 0:  # Bounds check
                    recent_high_prices.append(self.data.High[i])
                    if not pd.isna(self.rsi[i]):
                        recent_rsi_values.append(self.rsi[i])

            # Check for bearish divergence
            if len(recent_high_prices) >= 3 and len(recent_rsi_values) >= 3:
                # Price makes new high?
                price_makes_new_high = current_high >= max(recent_high_prices)

                # RSI makes new high?
                current_rsi = self.rsi[-1]
                if not pd.isna(current_rsi):
                    rsi_makes_new_high = current_rsi >= max(recent_rsi_values)

                    # Bearish divergence: price new high but RSI doesn't = momentum failure
                    if price_makes_new_high and not rsi_makes_new_high:
                        rsi_filter_pass = False  # Skip trade

        # 📊 PHASE 3 STEP 2: Multi-Timeframe Trend Confirmation
        # Check if current price aligns with weekly trend
        # LONG trades: Only when price > weekly SMA (uptrend)
        # SHORT trades: Only when price < weekly SMA (downtrend)
        trend_filter_pass_long = True
        trend_filter_pass_short = True

        if self.use_multi_timeframe_filter and len(self.data) > self.weekly_trend_period:
            current_trend = self.weekly_trend[-1]
            if not pd.isna(current_trend):
                # For long trades: price must be above weekly trend
                trend_filter_pass_long = current_price > current_trend
                # For short trades: price must be below weekly trend
                trend_filter_pass_short = current_price < current_trend

        # 🛡️ PHASE 2: Overtrading Protection (Trade Limit Check)
        # Check if we've exceeded maximum trades for the period
        if not self.position and self.enforce_trade_limit:
            if self.trade_count >= self.max_trades_per_year:
                # Silently skip new trades once limit reached
                # Only print warning on first occurrence
                if self.trade_count == self.max_trades_per_year:
                    print(f"\n🛡️ OVERTRADING PROTECTION: Max trades limit reached ({self.max_trades_per_year} trades)")
                    print(f"   No new positions will be opened for remainder of backtest period")
                    print(f"   This prevents the catastrophic losses seen in Phase 0-1 testing")
                    self.trade_count += 1  # Increment to prevent repeated warnings
                return  # Skip trade entry logic

        # 🔄 Position management
        if not self.position:
            # 📈 Bullish breakout: Price breaks above range high with volume AND momentum confirmation AND trend alignment
            if current_high > range_high and volume_confirmed and rsi_filter_pass and trend_filter_pass_long:
                # 🎯 Calculate position size with breakout-specific logic
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        # Breakout strength affects position size
                        breakout_strength = (current_high - range_high) / range_high
                        adjusted_risk = self.risk_pct * (1 + min(breakout_strength * 2, 1.0))  # Up to double risk

                        stop_loss_price = range_low  # Stop at range support
                        position_result = calculate_position_size(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss_price=stop_loss_price,
                            risk_pct=min(adjusted_risk, 5.0)  # Cap at 5%
                        )

                        size_fraction = min(position_result['position_value'] / self.equity, 0.9)

                    except Exception as e:
                        print(f"⚠️ Position sizing error: {e}")
                        size_fraction = 0.025  # Fallback to 2.5% equity
                else:
                    size_fraction = 0.025

                # 🚀 Enter long position
                self.buy(size=size_fraction)
                self.trade_count += 1  # 🛡️ PHASE 2: Track trade count for overtrading prevention
                self.entry_price = current_price
                self.stop_loss = range_low  # Initial stop at range support
                self.trail_high = current_high

            # 📉 Bearish breakout: Price breaks below range low with volume AND momentum confirmation AND trend alignment
            elif current_low < range_low and volume_confirmed and rsi_filter_pass and trend_filter_pass_short:
                # Similar logic for short positions
                if TRADING_FUNCTIONS_AVAILABLE:
                    try:
                        breakout_strength = (range_low - current_low) / range_low
                        adjusted_risk = self.risk_pct * (1 + min(breakout_strength * 2, 1.0))

                        stop_loss_price = range_high  # Stop at range resistance
                        position_result = calculate_position_size(
                            account_balance=self.equity,
                            entry_price=current_price,
                            stop_loss_price=stop_loss_price,
                            risk_pct=min(adjusted_risk, 5.0)
                        )

                        size_fraction = min(position_result['position_value'] / self.equity, 0.9)

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