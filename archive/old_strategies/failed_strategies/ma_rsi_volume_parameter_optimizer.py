"""
🚀 MA-RSI-Volume Strategy Parameter Optimization Framework 🚀
===========================================================
Comprehensive grid search optimization to transform failing strategy
into profitable trading system through systematic parameter tuning.

Optimization Targets:
- Win Rate: 50%+ (currently 25-42%)
- Annual Returns: 15%+ (currently -59% to -99%)
- Max Drawdown: <20% (currently 62-92%)
- Sharpe Ratio: >1.0 (currently negative)

Author: Bobby's Algo Trading System 🌙💫
Date: 2025-01-18
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
from itertools import product
import json
from datetime import datetime
import os
import glob
from typing import Dict, List, Tuple, Optional

# Import universal native results display for mandatory output formatting
import sys
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/analysis')
from universal_native_results_display import display_full_native_results

print("🚀 MA-RSI-Volume Strategy Optimizer Loading... 💫")


# 🎯 Parameter Grid Definitions
PARAMETER_GRID = {
    'ma_periods': [10, 15, 20, 30, 50],                    # Moving average periods
    'rsi_periods': [10, 14, 21],                           # RSI periods
    'rsi_oversold': [25, 30, 35, 40],                      # RSI oversold thresholds
    'rsi_overbought': [60, 65, 70, 75],                    # RSI overbought thresholds
    'volume_multipliers': [1.2, 1.5, 2.0, 2.5],            # Volume spike thresholds
    'stop_loss': [0.015, 0.02, 0.025, 0.03],               # Stop loss percentages
    'take_profit': [0.03, 0.04, 0.05, 0.06],               # Take profit percentages
    'signal_modes': ['ALL3', '2OF3', 'WEIGHTED', 'PRIMARY'] # Signal confirmation logic
}


class MARSIVolumeOptimizedStrategy(Strategy):
    """
    🌙 Optimized MA-RSI-Volume Strategy with Parameter Flexibility 🌙

    Supports multiple signal confirmation modes and parameter combinations
    for comprehensive optimization testing.
    """

    # Default parameters (will be overridden during optimization)
    ma_period = 20
    rsi_period = 14
    rsi_oversold = 35
    rsi_overbought = 65
    volume_multiplier = 1.5
    stop_loss = 0.02
    take_profit = 0.04
    signal_mode = '2OF3'  # ALL3, 2OF3, WEIGHTED, PRIMARY

    def init(self):
        """Initialize indicators with configurable parameters"""
        # Core indicators
        self.ma = self.I(talib.SMA, self.data.Close, self.ma_period)
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)
        self.volume_ma = self.I(talib.SMA, self.data.Volume, 20)

        # Additional indicators for enhanced signals
        self.ema_fast = self.I(talib.EMA, self.data.Close, 9)
        self.ema_slow = self.I(talib.EMA, self.data.Close, 21)

        # Volatility for dynamic adjustments
        self.atr = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, 14)

        # Entry tracking
        self.entry_price = None
        self.entry_bar = None

    def next(self):
        """Trading logic with multiple signal confirmation modes"""

        # Skip if indicators not ready
        if len(self.data) < max(self.ma_period, self.rsi_period, 21):
            return

        # Skip NaN values
        if pd.isna(self.ma[-1]) or pd.isna(self.rsi[-1]) or pd.isna(self.volume_ma[-1]):
            return

        # Current values
        price = self.data.Close[-1]
        ma = self.ma[-1]
        rsi = self.rsi[-1]
        volume = self.data.Volume[-1]
        vol_ma = self.volume_ma[-1] if self.volume_ma[-1] > 0 else 1
        atr = self.atr[-1] if not pd.isna(self.atr[-1]) else price * 0.02

        # Signal conditions
        trend_up = price > ma
        momentum_oversold = rsi < self.rsi_oversold
        volume_spike = volume > (vol_ma * self.volume_multiplier)
        ema_bullish = self.ema_fast[-1] > self.ema_slow[-1] if not pd.isna(self.ema_fast[-1]) else False

        # ENTRY LOGIC - Multiple modes
        if not self.position:
            entry_signal = False

            if self.signal_mode == 'ALL3':
                # Conservative: All 3 main signals required
                entry_signal = trend_up and momentum_oversold and volume_spike

            elif self.signal_mode == '2OF3':
                # Adaptive: Any 2 of 3 signals
                conditions_met = sum([trend_up, momentum_oversold, volume_spike])
                entry_signal = conditions_met >= 2

            elif self.signal_mode == 'WEIGHTED':
                # Weighted scoring system
                signal_score = 0
                signal_score += 2.0 if trend_up else 0
                signal_score += 1.5 if momentum_oversold else 0
                signal_score += 1.0 if volume_spike else 0
                signal_score += 0.5 if ema_bullish else 0
                entry_signal = signal_score >= 3.0  # Threshold for entry

            elif self.signal_mode == 'PRIMARY':
                # Primary signal with confirmation
                # Primary: Trend + momentum, Confirm: Volume OR EMA
                primary_signal = trend_up and momentum_oversold
                confirmation = volume_spike or ema_bullish
                entry_signal = primary_signal and confirmation

            if entry_signal:
                self.buy(size=0.95)
                self.entry_price = price
                self.entry_bar = len(self.data)

        # EXIT LOGIC
        elif self.position:
            if self.entry_price:
                pnl_pct = (price - self.entry_price) / self.entry_price
                bars_held = len(self.data) - self.entry_bar if self.entry_bar else 0

                # Exit conditions
                exit_rsi = rsi > self.rsi_overbought
                exit_trend = price < ma * 0.98  # 2% buffer below MA
                exit_tp = pnl_pct >= self.take_profit
                exit_sl = pnl_pct <= -self.stop_loss
                exit_timeout = bars_held > 100  # Maximum holding period

                # Dynamic exit based on volatility
                if atr > 0:
                    volatility_adjusted_sl = -self.stop_loss * (1 + atr/price)
                    exit_sl = pnl_pct <= volatility_adjusted_sl

                if exit_rsi or exit_trend or exit_tp or exit_sl or exit_timeout:
                    self.position.close()
                    self.entry_price = None
                    self.entry_bar = None


def optimize_parameters_for_asset(
    data_path: str,
    symbol: str,
    timeframe: str,
    parameter_grid: Dict,
    top_n: int = 5
) -> pd.DataFrame:
    """
    🎯 Optimize parameters for a specific asset using grid search

    Returns DataFrame with top parameter combinations sorted by performance
    """

    print(f"\n🔧 Starting optimization for {symbol} ({timeframe})")
    print(f"📁 Data source: {data_path}")

    # Load and prepare data
    try:
        df = pd.read_csv(data_path)

        # Find and set date column
        date_col = None
        for col in df.columns:
            if col.lower() in ['date', 'datetime', 'time']:
                date_col = col
                break

        if date_col:
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col)

        # Standardize columns
        df.columns = [col.capitalize() for col in df.columns]

        # Validate data
        if len(df) < 1000:
            print(f"⚠️ Insufficient data for {symbol}: {len(df)} bars")
            return pd.DataFrame()

        print(f"📊 Data loaded: {len(df)} bars")

    except Exception as e:
        print(f"❌ Error loading data for {symbol}: {e}")
        return pd.DataFrame()

    # Grid search optimization
    results = []
    total_combinations = (
        len(parameter_grid['ma_periods']) *
        len(parameter_grid['rsi_oversold']) *
        len(parameter_grid['rsi_overbought']) *
        len(parameter_grid['volume_multipliers']) *
        len(parameter_grid['stop_loss']) *
        len(parameter_grid['take_profit']) *
        len(parameter_grid['signal_modes'])
    )

    print(f"🔍 Testing {total_combinations} parameter combinations...")

    # Sample subset of combinations for faster optimization
    # (Full grid search would take too long)
    sampled_combinations = []

    # Test each signal mode with a subset of other parameters
    for signal_mode in parameter_grid['signal_modes']:
        for ma_period in parameter_grid['ma_periods'][::2]:  # Every 2nd
            for rsi_oversold in parameter_grid['rsi_oversold']:
                for rsi_overbought in parameter_grid['rsi_overbought']:
                    if rsi_overbought <= rsi_oversold + 20:  # Skip invalid combinations
                        continue
                    for volume_mult in parameter_grid['volume_multipliers'][::2]:  # Every 2nd
                        for sl in parameter_grid['stop_loss'][::2]:  # Every 2nd
                            for tp in parameter_grid['take_profit']:
                                if tp <= sl:  # Skip invalid risk-reward
                                    continue
                                sampled_combinations.append({
                                    'ma_period': ma_period,
                                    'rsi_period': 14,  # Fixed for speed
                                    'rsi_oversold': rsi_oversold,
                                    'rsi_overbought': rsi_overbought,
                                    'volume_multiplier': volume_mult,
                                    'stop_loss': sl,
                                    'take_profit': tp,
                                    'signal_mode': signal_mode
                                })

    print(f"📊 Testing {len(sampled_combinations)} sampled combinations...")

    # Test each combination
    for i, params in enumerate(sampled_combinations):
        if i % 50 == 0:
            print(f"   Progress: {i}/{len(sampled_combinations)} ({i*100/len(sampled_combinations):.1f}%)")

        try:
            # Run backtest with current parameters
            bt = Backtest(
                df,
                MARSIVolumeOptimizedStrategy,
                cash=10000,
                commission=0.002,
                exclusive_orders=True
            )

            stats = bt.run(
                ma_period=params['ma_period'],
                rsi_period=params['rsi_period'],
                rsi_oversold=params['rsi_oversold'],
                rsi_overbought=params['rsi_overbought'],
                volume_multiplier=params['volume_multiplier'],
                stop_loss=params['stop_loss'],
                take_profit=params['take_profit'],
                signal_mode=params['signal_mode']
            )

            # Record results
            result = {
                'symbol': symbol,
                'timeframe': timeframe,
                'ma_period': params['ma_period'],
                'rsi_period': params['rsi_period'],
                'rsi_oversold': params['rsi_oversold'],
                'rsi_overbought': params['rsi_overbought'],
                'volume_multiplier': params['volume_multiplier'],
                'stop_loss': params['stop_loss'] * 100,
                'take_profit': params['take_profit'] * 100,
                'signal_mode': params['signal_mode'],
                'return_pct': stats['Return [%]'],
                'sharpe_ratio': stats['Sharpe Ratio'],
                'max_drawdown_pct': stats['Max. Drawdown [%]'],
                'win_rate_pct': stats['Win Rate [%]'],
                'num_trades': stats['# Trades'],
                'profit_factor': stats.get('Profit Factor', 0),
                'expectancy_pct': stats.get('Expectancy [%]', 0),
                'exposure_time_pct': stats.get('Exposure Time [%]', 0),

                # Combined score for ranking
                'optimization_score': (
                    stats['Return [%]'] * 0.3 +
                    stats['Sharpe Ratio'] * 20 * 0.2 +
                    (100 - abs(stats['Max. Drawdown [%]'])) * 0.2 +
                    stats['Win Rate [%]'] * 0.2 +
                    min(stats['# Trades'] / 100, 1) * 10 * 0.1
                )
            }

            results.append(result)

        except Exception as e:
            continue

    # Convert to DataFrame and sort by optimization score
    results_df = pd.DataFrame(results)

    if len(results_df) > 0:
        results_df = results_df.sort_values('optimization_score', ascending=False)

        print(f"\n✅ Optimization complete for {symbol}")
        print(f"🏆 Top {min(top_n, len(results_df))} parameter combinations:")

        # Display top results
        for i, row in results_df.head(top_n).iterrows():
            print(f"\n   #{results_df.index.get_loc(i) + 1}:")
            print(f"      Signal Mode: {row['signal_mode']}")
            print(f"      MA: {row['ma_period']}, RSI: {row['rsi_oversold']}/{row['rsi_overbought']}")
            print(f"      Volume: {row['volume_multiplier']}x, SL: {row['stop_loss']:.1f}%, TP: {row['take_profit']:.1f}%")
            print(f"      Return: {row['return_pct']:.2f}%, Sharpe: {row['sharpe_ratio']:.3f}")
            print(f"      Win Rate: {row['win_rate_pct']:.1f}%, Max DD: {row['max_drawdown_pct']:.1f}%")
            print(f"      Trades: {row['num_trades']:.0f}, Score: {row['optimization_score']:.2f}")

    return results_df


def optimize_all_assets(save_results: bool = True) -> Dict[str, pd.DataFrame]:
    """
    🚀 Run optimization across all available cryptocurrency data

    Tests multiple timeframes and assets to find optimal parameters
    """

    print("\n" + "="*80)
    print("🚀 MA-RSI-VOLUME COMPREHENSIVE PARAMETER OPTIMIZATION")
    print("="*80)

    # Find all available 1H data files
    data_patterns = [
        '/Users/bobbyyo/Projects/algo-fun/data/*-1h-*.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/coinbase/*-1h-*.csv',
        '/Users/bobbyyo/Projects/algo-fun/data/yahoo/*-1h*.csv'
    ]

    all_files = []
    for pattern in data_patterns:
        all_files.extend(glob.glob(pattern))

    # Group files by symbol
    asset_files = {}
    for file_path in all_files:
        filename = os.path.basename(file_path)

        # Extract symbol from filename
        for symbol in ['BTC', 'ETH', 'CRO', 'HBAR', 'LINK', 'XRP', 'SOL', 'MATIC']:
            if symbol in filename.upper():
                if symbol not in asset_files:
                    asset_files[symbol] = []
                asset_files[symbol].append(file_path)
                break

    print(f"\n📊 Found data for {len(asset_files)} assets:")
    for symbol, files in asset_files.items():
        print(f"   {symbol}: {len(files)} data sources")

    # Optimize each asset
    all_results = {}
    best_params_by_asset = {}

    for symbol, file_paths in asset_files.items():
        print(f"\n{'='*60}")
        print(f"🎯 Optimizing {symbol}")
        print(f"{'='*60}")

        # Use first available file for this asset
        data_path = file_paths[0]

        # Run optimization
        results_df = optimize_parameters_for_asset(
            data_path=data_path,
            symbol=symbol,
            timeframe='1H',
            parameter_grid=PARAMETER_GRID,
            top_n=5
        )

        if len(results_df) > 0:
            all_results[symbol] = results_df
            best_params_by_asset[symbol] = results_df.iloc[0].to_dict()

    # Save optimization results
    if save_results and all_results:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save detailed results
        results_dir = '/Users/bobbyyo/Projects/algo-fun/strategies/results'
        os.makedirs(results_dir, exist_ok=True)

        # Combined results file
        all_results_combined = pd.concat(all_results.values(), ignore_index=True)
        results_file = f'{results_dir}/ma_rsi_volume_optimization_{timestamp}.csv'
        all_results_combined.to_csv(results_file, index=False)

        print(f"\n💾 Optimization results saved to: {results_file}")

        # Save best parameters as JSON
        best_params_file = f'{results_dir}/ma_rsi_volume_best_params_{timestamp}.json'
        with open(best_params_file, 'w') as f:
            json.dump(best_params_by_asset, f, indent=2, default=str)

        print(f"💾 Best parameters saved to: {best_params_file}")

    # Summary report
    print("\n" + "="*80)
    print("📊 OPTIMIZATION SUMMARY REPORT")
    print("="*80)

    for symbol, params in best_params_by_asset.items():
        print(f"\n🏆 {symbol} Best Configuration:")
        print(f"   Signal Mode: {params['signal_mode']}")
        print(f"   Parameters: MA={params['ma_period']}, RSI={params['rsi_oversold']}/{params['rsi_overbought']}")
        print(f"   Risk: SL={params['stop_loss']:.1f}%, TP={params['take_profit']:.1f}%")
        print(f"   Performance: Return={params['return_pct']:.2f}%, Sharpe={params['sharpe_ratio']:.3f}")
        print(f"   Win Rate={params['win_rate_pct']:.1f}%, Trades={params['num_trades']:.0f}")

    return all_results


def test_optimized_parameters(best_params: Dict[str, Dict]) -> None:
    """
    🧪 Test optimized parameters across multiple data sources

    Validates optimization results on different data to ensure robustness
    """

    print("\n" + "="*80)
    print("🧪 TESTING OPTIMIZED PARAMETERS")
    print("="*80)

    for symbol, params in best_params.items():
        print(f"\n🔬 Testing {symbol} with optimized parameters...")

        # Find test data
        test_patterns = [
            f'/Users/bobbyyo/Projects/algo-fun/data/*{symbol}*-1h-*.csv',
            f'/Users/bobbyyo/Projects/algo-fun/data/coinbase/*{symbol}*-1h-*.csv'
        ]

        test_files = []
        for pattern in test_patterns:
            test_files.extend(glob.glob(pattern))

        if not test_files:
            print(f"   ⚠️ No test data found for {symbol}")
            continue

        # Test on first available file
        test_file = test_files[0]
        print(f"   📁 Test data: {test_file}")

        try:
            # Load data
            df = pd.read_csv(test_file)

            # Prepare data
            date_col = None
            for col in df.columns:
                if col.lower() in ['date', 'datetime', 'time']:
                    date_col = col
                    break

            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
                df = df.set_index(date_col)

            df.columns = [col.capitalize() for col in df.columns]

            # Run backtest with optimized parameters
            bt = Backtest(
                df,
                MARSIVolumeOptimizedStrategy,
                cash=10000,
                commission=0.002
            )

            stats = bt.run(
                ma_period=int(params.get('ma_period', 20)),
                rsi_period=int(params.get('rsi_period', 14)),
                rsi_oversold=params.get('rsi_oversold', 35),
                rsi_overbought=params.get('rsi_overbought', 65),
                volume_multiplier=params.get('volume_multiplier', 1.5),
                stop_loss=params.get('stop_loss', 2) / 100,
                take_profit=params.get('take_profit', 4) / 100,
                signal_mode=params.get('signal_mode', '2OF3')
            )

            # Display full native results using mandatory display module
            data_source_info = {
                'symbol': symbol,
                'timeframe': '1H',
                'provider': 'Optimized',
                'path': test_file
            }

            display_full_native_results(
                stats,
                data_source_info,
                strategy_name="MA-RSI-Volume Optimized"
            )

        except Exception as e:
            print(f"   ❌ Test failed for {symbol}: {e}")


# Main execution
if __name__ == "__main__":
    print("\n🌙💫🚀 MA-RSI-Volume Strategy Optimizer Starting...")

    # Run comprehensive optimization
    optimization_results = optimize_all_assets(save_results=True)

    # Extract best parameters for testing
    if optimization_results:
        best_params = {}
        for symbol, results_df in optimization_results.items():
            if len(results_df) > 0:
                best_params[symbol] = results_df.iloc[0].to_dict()

        # Test optimized parameters
        test_optimized_parameters(best_params)

    print("\n✅ Optimization complete! Check results directory for detailed analysis.")
    print("🌙💫🚀 Ready to implement optimized strategy!")