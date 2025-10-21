"""
🚀 ClucMay72018 Flexible Parameters Strategy
============================================
Flexible version of the ultra-selective momentum reversal strategy
Adjustable entry requirements to generate tradeable signals while maintaining core philosophy

Key Flexibility Features:
- Adjustable BB entry threshold (98.5% → 102% → 105%)
- Flexible volume anomaly detection (5% → 50% → 75%)
- Optional RSI oversold condition
- Configurable entry logic (all conditions vs 2-out-of-3)

Strategy Phases:
Phase 1: Moderate Flexibility - BB 102%, Volume 50%, All conditions
Phase 2: High Flexibility - BB 105%, Volume 75%, RSI < 35
Phase 3: Alternative - RSI or BB, Any below-avg volume, MACD confluence

Created: September 2025
Author: Bobby 🌙💫🚀
"""

import numpy as np
import pandas as pd
import talib
from backtesting import Strategy
from backtesting.lib import crossover
from typing import Optional


class ClucMay72018FlexibleStrategy(Strategy):
    """
    Flexible momentum-reversal strategy with adjustable entry parameters
    Maintains core philosophy while enabling actual trade generation
    """

    # Strategy parameters - NOW FLEXIBLE
    rsi_period = 5           # Short-term RSI for momentum
    rsi_ema_period = 5       # EMA smoothing for RSI
    ema_period = 100         # Long-term trend filter
    bb_period = 20           # Bollinger Bands period
    bb_std = 2               # Bollinger Bands standard deviation
    adx_period = 14          # ADX period for trend strength

    # MACD parameters
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9

    # 🔧 FLEXIBLE PARAMETERS (can be adjusted)
    # Phase 1: Moderate (default)
    volume_lookback = 30         # Periods for volume comparison
    volume_threshold = 0.50      # Volume must be < 50% of average (was 5%)
    bb_entry_threshold = 1.02    # Enter when price < 102% of lower BB (was 98.5%)

    # Optional parameters
    rsi_oversold_level = 35      # RSI oversold threshold (was strict 30)
    use_rsi_alternative = True   # Use RSI < level as alternative entry
    require_all_conditions = False  # If False, allow 2-out-of-3 main conditions
    min_conditions_required = 2     # Minimum conditions needed when not requiring all

    # Additional flexibility
    use_macd_confluence = True   # Use MACD as additional confirmation
    adx_max_threshold = 40       # Skip if ADX too high (strong trend)

    # Risk management (keep conservative)
    stop_loss_pct = 0.05     # 5% stop loss
    take_profit_pct = 0.01   # 1% take profit

    def init(self):
        """Initialize all technical indicators"""

        # 🌟 Price and volume data
        close = self.data.Close
        high = self.data.High
        low = self.data.Low
        volume = self.data.Volume

        # 📊 RSI and its EMA
        self.rsi = self.I(talib.RSI, close, self.rsi_period)
        self.rsi_ema = self.I(talib.EMA, self.rsi, self.rsi_ema_period)

        # 📈 MACD
        macd_result = talib.MACD(close,
                                  fastperiod=self.macd_fast,
                                  slowperiod=self.macd_slow,
                                  signalperiod=self.macd_signal)
        self.macd = self.I(lambda: macd_result[0])  # MACD line
        self.macd_signal_line = self.I(lambda: macd_result[1])  # Signal line
        self.macd_histogram = self.I(lambda: macd_result[2])  # Histogram

        # 💪 ADX for trend strength
        self.adx = self.I(talib.ADX, high, low, close, self.adx_period)

        # 📉 EMA for trend filter
        self.ema = self.I(talib.EMA, close, self.ema_period)

        # 🎯 Bollinger Bands
        bb_result = talib.BBANDS(close,
                                  timeperiod=self.bb_period,
                                  nbdevup=self.bb_std,
                                  nbdevdn=self.bb_std)
        self.bb_upper = self.I(lambda: bb_result[0])  # Upper band
        self.bb_middle = self.I(lambda: bb_result[1])  # Middle band (SMA)
        self.bb_lower = self.I(lambda: bb_result[2])  # Lower band

        # 📊 Volume analysis
        # Calculate rolling mean volume for anomaly detection
        self.volume_sma = self.I(talib.SMA, volume, self.volume_lookback)

        # Track entry price and trade stats
        self.entry_price = None
        self.trade_count = 0
        self.conditions_met_history = []

    def next(self):
        """Execute trading logic with flexible parameters"""

        # Skip if we don't have enough data
        if len(self.data) < max(self.ema_period, self.volume_lookback):
            return

        # Get current values
        current_price = self.data.Close[-1]
        current_volume = self.data.Volume[-1]
        current_rsi = self.rsi[-1]
        current_rsi_ema = self.rsi_ema[-1]
        current_macd = self.macd[-1]
        current_macd_signal = self.macd_signal_line[-1]
        current_macd_hist = self.macd_histogram[-1]
        current_adx = self.adx[-1]
        current_ema = self.ema[-1]
        current_bb_lower = self.bb_lower[-1]
        current_bb_middle = self.bb_middle[-1]
        current_bb_upper = self.bb_upper[-1]
        current_volume_avg = self.volume_sma[-1]

        # Skip if any critical indicators are NaN
        if any(pd.isna(x) for x in [current_rsi, current_adx, current_ema,
                                      current_bb_lower, current_volume_avg]):
            return

        # 🎯 FLEXIBLE Entry Logic
        if not self.position:

            # Track conditions for analysis
            conditions_met = {}

            # 1. Trend filter: Price below EMA(100)
            below_ema = current_price < current_ema
            conditions_met['below_ema'] = below_ema

            # 2. FLEXIBLE BB condition: Price < threshold% of lower BB
            bb_condition = current_price < (current_bb_lower * self.bb_entry_threshold)
            conditions_met['bb_oversold'] = bb_condition

            # 3. FLEXIBLE Volume anomaly: Current volume below threshold
            volume_condition = False
            volume_ratio = 0
            if current_volume_avg > 0:
                volume_ratio = current_volume / current_volume_avg
                volume_condition = volume_ratio < self.volume_threshold
            conditions_met['volume_anomaly'] = volume_condition

            # 4. OPTIONAL RSI condition (can be alternative or additional)
            rsi_condition = current_rsi < self.rsi_oversold_level
            conditions_met['rsi_oversold'] = rsi_condition

            # 5. OPTIONAL MACD confluence
            macd_bearish = current_macd < current_macd_signal
            conditions_met['macd_bearish'] = macd_bearish

            # 6. ADX filter (avoid strong trends if configured)
            adx_acceptable = current_adx < self.adx_max_threshold
            conditions_met['adx_ok'] = adx_acceptable

            # Calculate how many primary conditions are met
            primary_conditions = [below_ema, bb_condition, volume_condition]
            primary_met_count = sum(primary_conditions)

            # Alternative entry with RSI
            if self.use_rsi_alternative:
                # Allow RSI as alternative to BB condition
                alternative_bb = bb_condition or rsi_condition
                alternative_conditions = [below_ema, alternative_bb, volume_condition]
                alternative_met_count = sum(alternative_conditions)
            else:
                alternative_met_count = primary_met_count

            # Determine if we should enter
            should_enter = False

            if self.require_all_conditions:
                # Strict mode: need all primary conditions
                should_enter = all(primary_conditions) and adx_acceptable
            else:
                # Flexible mode: need minimum conditions
                if self.use_rsi_alternative:
                    should_enter = (alternative_met_count >= self.min_conditions_required) and adx_acceptable
                else:
                    should_enter = (primary_met_count >= self.min_conditions_required) and adx_acceptable

            # Add MACD confluence if configured
            if self.use_macd_confluence and should_enter:
                # Only enter if MACD also bearish (additional filter)
                should_enter = should_enter and macd_bearish

            # Enter position if conditions met
            if should_enter:
                # Calculate position size (use most of available cash)
                size = 0.95

                # Enter long position
                self.buy(size=size)
                self.entry_price = current_price
                self.trade_count += 1

                # Log entry conditions for analysis
                print(f"\n🚀 ENTRY #{self.trade_count} at {current_price:.4f}")
                print(f"  Conditions Met: {primary_met_count}/3 primary, {sum(conditions_met.values())}/6 total")
                print(f"  EMA Filter: {below_ema} | BB Oversold: {bb_condition} | Volume Low: {volume_condition}")
                print(f"  RSI: {current_rsi:.2f} (OS: {rsi_condition}) | ADX: {current_adx:.2f}")
                print(f"  Volume Ratio: {volume_ratio:.2%} of avg | MACD: {'Bearish' if macd_bearish else 'Bullish'}")
                print(f"  BB Position: Price={current_price:.4f}, Lower={current_bb_lower:.4f}, Threshold={self.bb_entry_threshold:.1%}")

                # Store conditions for later analysis
                self.conditions_met_history.append({
                    'trade_num': self.trade_count,
                    'conditions': conditions_met,
                    'primary_met': primary_met_count,
                    'entry_price': current_price
                })

        # 🚪 Exit Logic (keep original conservative exits)
        elif self.position:

            if self.entry_price is None:
                self.entry_price = self.position.open_price

            # Calculate current P&L
            pnl_pct = (current_price - self.entry_price) / self.entry_price

            # Exit conditions
            # 1. Stop Loss: 5% loss
            stop_loss_hit = pnl_pct <= -self.stop_loss_pct

            # 2. Take Profit: 1% gain
            take_profit_hit = pnl_pct >= self.take_profit_pct

            # 3. BB Midline Cross: Mean reversion complete
            bb_midline_cross = current_price >= current_bb_middle

            # Exit if any condition met
            if stop_loss_hit or take_profit_hit or bb_midline_cross:
                exit_reason = "Stop Loss" if stop_loss_hit else \
                             "Take Profit" if take_profit_hit else \
                             "BB Midline Cross"

                print(f"💫 EXIT #{self.trade_count} at {current_price:.4f} - {exit_reason}")
                print(f"  P&L: {pnl_pct:.2%} | Entry: {self.entry_price:.4f} → Exit: {current_price:.4f}")

                self.position.close()
                self.entry_price = None


def create_phase1_strategy():
    """Create Phase 1: Moderate Flexibility strategy"""
    class Phase1Strategy(ClucMay72018FlexibleStrategy):
        # Moderate flexibility
        volume_threshold = 0.50      # 50% of average
        bb_entry_threshold = 1.02    # 102% of lower BB
        rsi_oversold_level = 35
        use_rsi_alternative = False
        require_all_conditions = True  # Still need all 3
        use_macd_confluence = False

    return Phase1Strategy


def create_phase2_strategy():
    """Create Phase 2: High Flexibility strategy"""
    class Phase2Strategy(ClucMay72018FlexibleStrategy):
        # High flexibility
        volume_threshold = 0.75      # 75% of average
        bb_entry_threshold = 1.05    # 105% of lower BB
        rsi_oversold_level = 35
        use_rsi_alternative = True   # Allow RSI as alternative
        require_all_conditions = False
        min_conditions_required = 2  # Only need 2 out of 3
        use_macd_confluence = False

    return Phase2Strategy


def create_phase3_strategy():
    """Create Phase 3: Alternative approach"""
    class Phase3Strategy(ClucMay72018FlexibleStrategy):
        # Alternative approach
        volume_threshold = 1.0       # Any below average
        bb_entry_threshold = 1.05    # 105% of lower BB
        rsi_oversold_level = 30      # Stricter RSI
        use_rsi_alternative = True   # RSI or BB
        require_all_conditions = False
        min_conditions_required = 2
        use_macd_confluence = True   # Add MACD for confluence
        adx_max_threshold = 50       # More lenient ADX

    return Phase3Strategy


def test_strategy(data, strategy_class=ClucMay72018FlexibleStrategy, cash=10000, commission=0.002):
    """
    Test the flexible ClucMay72018 strategy on provided data

    Args:
        data: DataFrame with OHLCV data
        strategy_class: Strategy class to test
        cash: Starting capital
        commission: Trading commission rate

    Returns:
        Backtest results
    """
    from backtesting import Backtest

    bt = Backtest(data, strategy_class,
                  cash=cash,
                  commission=commission,
                  exclusive_orders=True)

    stats = bt.run()
    return stats, bt


if __name__ == "__main__":
    print("🌙 ClucMay72018 Flexible Parameters Strategy")
    print("=" * 60)
    print("Adjustable entry requirements for generating tradeable signals")
    print("\nPhase 1: BB 102%, Volume 50%, All conditions required")
    print("Phase 2: BB 105%, Volume 75%, 2-out-of-3 conditions")
    print("Phase 3: Alternative with RSI, any below-avg volume, MACD")
    print("=" * 60)