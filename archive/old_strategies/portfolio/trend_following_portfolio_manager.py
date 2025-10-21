"""
🌙 Unified Trend-Following Portfolio Manager 🌙
Integrates TEMS, VBM, ATSS, MTMC strategies with dynamic allocation
and risk management for optimal crypto momentum capture.

Created: 2025
Author: Bobby Younghoward
"""

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')


class TrendFollowingPortfolioStrategy(Strategy):
    """
    🚀 Unified Portfolio Manager - Combining All 4 Trend-Following Strategies 🚀

    Integrates:
    - TEMS: Triple EMA Momentum System (+312% avg return)
    - VBM: Volatility Breakout Method (70% win rate)
    - ATSS: ADX Trend Strength System (+136% HBAR)
    - MTMC: Multi-Timeframe Momentum Cascade (53.8% win rate ETH)

    With dynamic allocation, risk management, and performance optimization.
    """

    # === Portfolio Configuration Parameters ===
    # Strategy allocation weights (will be dynamic in production)
    tems_weight = 0.40    # Highest weight - proven +312% performance
    vbm_weight = 0.25     # Strong performer - 70% win rate
    atss_weight = 0.25    # Consistent performer - +136% HBAR
    mtmc_weight = 0.10    # Conservative allocation - needs optimization

    # === Risk Management Parameters ===
    max_portfolio_risk = 0.08      # 8% maximum portfolio exposure
    max_correlation_limit = 0.7    # Limit correlated positions
    max_positions = 4              # Maximum concurrent positions
    position_size_pct = 0.95       # Base position size as % of equity

    # Portfolio-level stop loss and take profit
    portfolio_stop_loss = 0.15     # 15% portfolio drawdown limit
    portfolio_take_profit = 0.50   # 50% portfolio profit target

    # === TEMS Parameters (Triple EMA Momentum) ===
    tems_ema_fast = 8
    tems_ema_medium = 21
    tems_ema_slow = 55
    tems_momentum_period = 14
    tems_volume_ma = 20

    # === VBM Parameters (Volatility Breakout) ===
    vbm_atr_period = 14
    vbm_bb_period = 20
    vbm_bb_std = 2.0
    vbm_keltner_multiplier = 2.0
    vbm_volume_threshold = 1.5

    # === ATSS Parameters (ADX Trend Strength) ===
    atss_adx_period = 14
    atss_adx_threshold = 25
    atss_rsi_period = 14
    atss_rsi_oversold = 30
    atss_rsi_overbought = 70

    # === MTMC Parameters (Multi-Timeframe Cascade) ===
    mtmc_short_ma = 10
    mtmc_long_ma = 30
    mtmc_trend_filter = 50
    mtmc_macd_fast = 12
    mtmc_macd_slow = 26
    mtmc_macd_signal = 9

    # === Dynamic Allocation Parameters ===
    rebalance_frequency = 20    # Rebalance every 20 bars
    performance_window = 30      # Look back 30 bars for performance
    min_strategy_weight = 0.05   # Minimum 5% allocation per strategy
    max_strategy_weight = 0.60   # Maximum 60% allocation per strategy

    def init(self):
        """Initialize all strategy indicators and portfolio management systems"""

        # === TEMS Indicators ===
        self.tems_ema_fast_line = self.I(talib.EMA, self.data.Close, self.tems_ema_fast)
        self.tems_ema_medium_line = self.I(talib.EMA, self.data.Close, self.tems_ema_medium)
        self.tems_ema_slow_line = self.I(talib.EMA, self.data.Close, self.tems_ema_slow)
        self.tems_momentum = self.I(talib.MOM, self.data.Close, self.tems_momentum_period)
        self.tems_volume_ma = self.I(talib.SMA, self.data.Volume, self.tems_volume_ma)

        # === VBM Indicators ===
        self.vbm_atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, self.vbm_atr_period)

        # Handle backtesting.py's data format
        close_values = self.data.Close.s if hasattr(self.data.Close, 's') else self.data.Close
        bb_upper, bb_middle, bb_lower = talib.BBANDS(
            close_values,
            timeperiod=self.vbm_bb_period,
            nbdevup=self.vbm_bb_std,
            nbdevdn=self.vbm_bb_std
        )
        self.vbm_bb_upper = self.I(lambda: bb_upper)
        self.vbm_bb_lower = self.I(lambda: bb_lower)
        self.vbm_bb_middle = self.I(lambda: bb_middle)

        # Keltner Channels for squeeze detection
        keltner_middle = self.I(talib.EMA, self.data.Close, self.vbm_bb_period)
        self.vbm_keltner_upper = self.I(lambda: keltner_middle + self.vbm_keltner_multiplier * self.vbm_atr)
        self.vbm_keltner_lower = self.I(lambda: keltner_middle - self.vbm_keltner_multiplier * self.vbm_atr)

        # === ATSS Indicators ===
        self.atss_adx = self.I(talib.ADX, self.data.High, self.data.Low, self.data.Close, self.atss_adx_period)
        self.atss_plus_di = self.I(talib.PLUS_DI, self.data.High, self.data.Low, self.data.Close, self.atss_adx_period)
        self.atss_minus_di = self.I(talib.MINUS_DI, self.data.High, self.data.Low, self.data.Close, self.atss_adx_period)
        self.atss_rsi = self.I(talib.RSI, self.data.Close, self.atss_rsi_period)

        # === MTMC Indicators ===
        self.mtmc_ma_short = self.I(talib.SMA, self.data.Close, self.mtmc_short_ma)
        self.mtmc_ma_long = self.I(talib.SMA, self.data.Close, self.mtmc_long_ma)
        self.mtmc_ma_trend = self.I(talib.SMA, self.data.Close, self.mtmc_trend_filter)

        # MACD for momentum confirmation
        close_values_macd = self.data.Close.s if hasattr(self.data.Close, 's') else self.data.Close
        macd, macd_signal, macd_hist = talib.MACD(
            close_values_macd,
            fastperiod=self.mtmc_macd_fast,
            slowperiod=self.mtmc_macd_slow,
            signalperiod=self.mtmc_macd_signal
        )
        self.mtmc_macd = self.I(lambda: macd)
        self.mtmc_macd_signal = self.I(lambda: macd_signal)
        self.mtmc_macd_hist = self.I(lambda: macd_hist)

        # === Portfolio Management Indicators ===
        # Track strategy performance
        self.strategy_returns = {
            'TEMS': [],
            'VBM': [],
            'ATSS': [],
            'MTMC': []
        }

        # Dynamic allocation weights
        self.current_weights = {
            'TEMS': self.tems_weight,
            'VBM': self.vbm_weight,
            'ATSS': self.atss_weight,
            'MTMC': self.mtmc_weight
        }

        # Position tracking
        self.active_strategy = None
        self.entry_bar = 0
        self.rebalance_counter = 0

        # Performance tracking
        self.portfolio_equity_curve = []
        self.strategy_signals = []

    def next(self):
        """Execute portfolio strategy logic with dynamic allocation"""

        # Skip if not enough data
        if len(self.data) < 100:
            return

        # === Update Portfolio Tracking ===
        self.rebalance_counter += 1

        # === Dynamic Rebalancing ===
        if self.rebalance_counter >= self.rebalance_frequency:
            self.rebalance_portfolio()
            self.rebalance_counter = 0

        # === Generate Individual Strategy Signals ===
        tems_signal = self.get_tems_signal()
        vbm_signal = self.get_vbm_signal()
        atss_signal = self.get_atss_signal()
        mtmc_signal = self.get_mtmc_signal()

        # === Combine Signals with Weighted Voting ===
        combined_signal = self.combine_signals({
            'TEMS': (tems_signal, self.current_weights['TEMS']),
            'VBM': (vbm_signal, self.current_weights['VBM']),
            'ATSS': (atss_signal, self.current_weights['ATSS']),
            'MTMC': (mtmc_signal, self.current_weights['MTMC'])
        })

        # === Portfolio Risk Check ===
        portfolio_risk_ok = self.check_portfolio_risk()

        # === Execute Trading Logic ===
        if not self.position:
            # Entry logic with portfolio risk management
            if combined_signal > 0.5 and portfolio_risk_ok:
                # Determine position size based on signal strength
                position_size = self.calculate_position_size(combined_signal)

                # Enter long position
                self.buy(size=position_size)
                self.active_strategy = self.get_dominant_strategy({
                    'TEMS': tems_signal,
                    'VBM': vbm_signal,
                    'ATSS': atss_signal,
                    'MTMC': mtmc_signal
                })
                self.entry_bar = len(self.data)

        else:
            # Exit logic with portfolio-level risk management
            bars_since_entry = len(self.data) - self.entry_bar

            # Portfolio-level stop loss
            portfolio_pnl = (self.data.Close[-1] - self.position.pl) / self.position.pl
            if portfolio_pnl < -self.portfolio_stop_loss:
                self.position.close()
                self.record_strategy_performance(self.active_strategy, portfolio_pnl)
                self.active_strategy = None
                return

            # Portfolio-level take profit
            if portfolio_pnl > self.portfolio_take_profit:
                self.position.close()
                self.record_strategy_performance(self.active_strategy, portfolio_pnl)
                self.active_strategy = None
                return

            # Check for exit signals from strategies
            exit_signal = self.get_exit_signal()
            if exit_signal < -0.5:
                self.position.close()
                self.record_strategy_performance(self.active_strategy, portfolio_pnl)
                self.active_strategy = None

    def get_tems_signal(self) -> float:
        """Generate signal from Triple EMA Momentum System"""
        signal_strength = 0.0

        # Triple EMA alignment
        if (self.tems_ema_fast_line[-1] > self.tems_ema_medium_line[-1] > self.tems_ema_slow_line[-1]):
            signal_strength += 0.4

        # Momentum confirmation
        if self.tems_momentum[-1] > 0 and self.tems_momentum[-1] > self.tems_momentum[-2]:
            signal_strength += 0.3

        # Volume confirmation
        if self.data.Volume[-1] > self.tems_volume_ma[-1] * 1.2:
            signal_strength += 0.3

        return signal_strength

    def get_vbm_signal(self) -> float:
        """Generate signal from Volatility Breakout Method"""
        signal_strength = 0.0

        # Bollinger Band squeeze detection
        bb_squeeze = (self.vbm_bb_upper[-1] < self.vbm_keltner_upper[-1] and
                     self.vbm_bb_lower[-1] > self.vbm_keltner_lower[-1])

        # Breakout detection
        if self.data.Close[-1] > self.vbm_bb_upper[-1]:
            signal_strength += 0.5

            # Volume confirmation
            if self.data.Volume[-1] > self.data.Volume[-2] * self.vbm_volume_threshold:
                signal_strength += 0.3

            # Squeeze bonus
            if bb_squeeze:
                signal_strength += 0.2

        return signal_strength

    def get_atss_signal(self) -> float:
        """Generate signal from ADX Trend Strength System"""
        signal_strength = 0.0

        # Strong trend confirmation
        if self.atss_adx[-1] > self.atss_adx_threshold:
            # Bullish trend
            if self.atss_plus_di[-1] > self.atss_minus_di[-1]:
                signal_strength += 0.5

                # RSI not overbought
                if self.atss_rsi[-1] < self.atss_rsi_overbought:
                    signal_strength += 0.3

                # Trend strengthening
                if self.atss_adx[-1] > self.atss_adx[-2]:
                    signal_strength += 0.2

        return signal_strength

    def get_mtmc_signal(self) -> float:
        """Generate signal from Multi-Timeframe Momentum Cascade"""
        signal_strength = 0.0

        # Multi-timeframe alignment
        if (self.mtmc_ma_short[-1] > self.mtmc_ma_long[-1] and
            self.mtmc_ma_long[-1] > self.mtmc_ma_trend[-1]):
            signal_strength += 0.4

        # MACD momentum confirmation
        if self.mtmc_macd[-1] > self.mtmc_macd_signal[-1] and self.mtmc_macd_hist[-1] > 0:
            signal_strength += 0.3

        # Trend continuation pattern
        if self.data.Close[-1] > self.mtmc_ma_short[-1]:
            signal_strength += 0.3

        return signal_strength

    def combine_signals(self, signals: Dict[str, Tuple[float, float]]) -> float:
        """
        Combine signals from all strategies using weighted voting
        Returns a value between -1 (strong sell) and 1 (strong buy)
        """
        weighted_sum = 0.0
        total_weight = 0.0

        for strategy_name, (signal, weight) in signals.items():
            weighted_sum += signal * weight
            total_weight += weight

        if total_weight > 0:
            return weighted_sum / total_weight
        return 0.0

    def get_exit_signal(self) -> float:
        """Generate exit signals from all strategies"""
        exit_strength = 0.0

        # TEMS exit: EMA crossover
        if self.tems_ema_fast_line[-1] < self.tems_ema_medium_line[-1]:
            exit_strength -= 0.25 * self.current_weights['TEMS']

        # VBM exit: Price below middle band
        if self.data.Close[-1] < self.vbm_bb_middle[-1]:
            exit_strength -= 0.25 * self.current_weights['VBM']

        # ATSS exit: Trend weakening
        if self.atss_adx[-1] < self.atss_adx[-2] and self.atss_adx[-1] < 20:
            exit_strength -= 0.25 * self.current_weights['ATSS']

        # MTMC exit: MA crossover
        if self.mtmc_ma_short[-1] < self.mtmc_ma_long[-1]:
            exit_strength -= 0.25 * self.current_weights['MTMC']

        return exit_strength

    def check_portfolio_risk(self) -> bool:
        """Check portfolio-level risk constraints"""

        # Check maximum drawdown
        if len(self.portfolio_equity_curve) > 0:
            peak_equity = max(self.portfolio_equity_curve[-50:]) if len(self.portfolio_equity_curve) > 50 else max(self.portfolio_equity_curve)
            current_equity = self.equity
            drawdown = (peak_equity - current_equity) / peak_equity if peak_equity > 0 else 0

            if drawdown > self.portfolio_stop_loss:
                return False

        # Check position limits
        if self.position and self.position.size >= self.max_positions:
            return False

        return True

    def calculate_position_size(self, signal_strength: float) -> float:
        """Calculate position size based on signal strength and risk management"""

        # Base position size
        base_size = self.position_size_pct

        # Adjust for signal strength (stronger signals get larger positions)
        signal_multiplier = min(1.5, max(0.5, signal_strength))

        # Adjust for portfolio risk
        risk_multiplier = 1.0
        if len(self.portfolio_equity_curve) > 0:
            recent_performance = np.mean(self.portfolio_equity_curve[-10:]) if len(self.portfolio_equity_curve) > 10 else 0
            if recent_performance < 0:
                risk_multiplier = 0.5  # Reduce size after losses

        # Calculate final position size
        position_size = base_size * signal_multiplier * risk_multiplier

        # Ensure we don't exceed maximum risk
        position_size = min(position_size, self.max_portfolio_risk)

        return position_size

    def rebalance_portfolio(self):
        """Dynamically rebalance strategy allocation weights based on recent performance"""

        if len(self.strategy_returns['TEMS']) < 5:
            return  # Not enough data to rebalance

        # Calculate recent performance for each strategy
        performance_scores = {}
        for strategy in self.strategy_returns:
            if len(self.strategy_returns[strategy]) > 0:
                recent_returns = self.strategy_returns[strategy][-self.performance_window:]
                if len(recent_returns) > 0:
                    # Calculate Sharpe-like score
                    avg_return = np.mean(recent_returns)
                    std_return = np.std(recent_returns) if np.std(recent_returns) > 0 else 1
                    performance_scores[strategy] = avg_return / std_return
                else:
                    performance_scores[strategy] = 0
            else:
                performance_scores[strategy] = 0

        # Calculate new weights based on performance
        total_score = sum(max(0, score) for score in performance_scores.values())

        if total_score > 0:
            for strategy in self.current_weights:
                # Performance-based weight
                performance_weight = max(0, performance_scores[strategy]) / total_score

                # Blend with original weight (momentum factor)
                momentum_factor = 0.7  # 70% performance, 30% original
                new_weight = momentum_factor * performance_weight + (1 - momentum_factor) * self.current_weights[strategy]

                # Apply min/max constraints
                new_weight = max(self.min_strategy_weight, min(self.max_strategy_weight, new_weight))

                self.current_weights[strategy] = new_weight

        # Normalize weights to sum to 1
        total_weight = sum(self.current_weights.values())
        if total_weight > 0:
            for strategy in self.current_weights:
                self.current_weights[strategy] /= total_weight

    def get_dominant_strategy(self, signals: Dict[str, float]) -> str:
        """Identify which strategy has the strongest signal"""
        return max(signals, key=signals.get)

    def record_strategy_performance(self, strategy: str, pnl: float):
        """Record performance for strategy attribution"""
        if strategy and strategy in self.strategy_returns:
            self.strategy_returns[strategy].append(pnl)

        # Update portfolio equity curve
        self.portfolio_equity_curve.append(self.equity)


def run_portfolio_backtest(data, cash=10000, commission=0.002):
    """
    Run the unified portfolio backtest

    Parameters:
    -----------
    data : pd.DataFrame
        OHLCV data for backtesting
    cash : float
        Starting capital
    commission : float
        Trading commission rate

    Returns:
    --------
    stats : dict
        Backtest statistics
    """

    bt = Backtest(
        data,
        TrendFollowingPortfolioStrategy,
        cash=cash,
        commission=commission,
        exclusive_orders=True
    )

    stats = bt.run()

    # Add portfolio-specific metrics
    stats['Portfolio_Sharpe'] = stats['Sharpe Ratio'] if 'Sharpe Ratio' in stats else 0
    stats['Portfolio_Sortino'] = stats['Sortino Ratio'] if 'Sortino Ratio' in stats else 0
    stats['Portfolio_Calmar'] = stats['Calmar Ratio'] if 'Calmar Ratio' in stats else 0

    return stats, bt


def optimize_portfolio_allocation(data, optimization_params=None):
    """
    Optimize portfolio allocation weights

    Parameters:
    -----------
    data : pd.DataFrame
        OHLCV data for optimization
    optimization_params : dict
        Parameters to optimize

    Returns:
    --------
    optimal_params : dict
        Optimized parameter values
    """

    if optimization_params is None:
        optimization_params = {
            'tems_weight': [0.1, 0.2, 0.3, 0.4, 0.5],
            'vbm_weight': [0.1, 0.2, 0.3, 0.4],
            'atss_weight': [0.1, 0.2, 0.3, 0.4],
            'mtmc_weight': [0.05, 0.1, 0.15, 0.2],
        }

    bt = Backtest(
        data,
        TrendFollowingPortfolioStrategy,
        cash=10000,
        commission=0.002,
        exclusive_orders=True
    )

    # Run optimization
    stats = bt.optimize(
        **optimization_params,
        constraint=lambda p: p.tems_weight + p.vbm_weight + p.atss_weight + p.mtmc_weight == 1.0,
        maximize='Sharpe Ratio',
        max_tries=500
    )

    return stats


if __name__ == "__main__":
    print("🌙 Trend-Following Portfolio Manager Module Loaded 🌙")
    print("=" * 80)
    print("Integrated Strategies:")
    print("  - TEMS: Triple EMA Momentum System")
    print("  - VBM: Volatility Breakout Method")
    print("  - ATSS: ADX Trend Strength System")
    print("  - MTMC: Multi-Timeframe Momentum Cascade")
    print("=" * 80)
    print("\nUsage:")
    print("  from trend_following_portfolio_manager import run_portfolio_backtest")
    print("  stats, bt = run_portfolio_backtest(data)")
    print("  print(stats)")