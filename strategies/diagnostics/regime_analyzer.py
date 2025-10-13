"""
🌦️ Market Regime Classifier - Phase 0 Diagnostic Tool
=====================================================
Classifies market conditions for each trade to identify when strategies work best.

🎯 Purpose:
    - Classify market regimes for each trade
    - Identify which conditions strategy thrives/fails in
    - Generate regime-specific performance metrics
    - Provide strategy ON/OFF recommendations

💫 Regime Classifications:
    1. Trend: Trending vs Ranging (ADX-based)
    2. Volatility: High vs Low (ATR percentile)
    3. Direction: Bull vs Bear (price vs SMA)
    4. Momentum: Strong vs Weak (ROC-based)

🌙💫🚀 Bobby's Algo-Fun Project
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


@dataclass
class RegimeAnalysisResult:
    """Results from market regime analysis"""
    total_trades: int

    # Regime distribution
    regime_counts: Dict[str, int]
    regime_returns: Dict[str, float]
    regime_win_rates: Dict[str, float]
    regime_sharpe: Dict[str, float]

    # Best/worst regimes
    best_regime: str
    worst_regime: str

    # Regime-specific trade lists
    trades_by_regime: Dict[str, List[Dict]]

    # Recommendations
    avoid_regimes: List[str]
    prefer_regimes: List[str]
    regime_definitions: Dict


class MarketRegimeAnalyzer:
    """
    🌦️ Analyzes market regimes and strategy performance across conditions

    Uses multiple technical indicators to classify market states
    """

    def __init__(self, price_data: pd.DataFrame, trades_data: pd.DataFrame):
        """
        Initialize analyzer with price and trade data

        Args:
            price_data: OHLCV DataFrame with datetime index
            trades_data: Trades DataFrame from backtesting.py
        """
        self.price_data = price_data.copy()
        self.trades_data = trades_data.copy()

        # Calculate regime indicators
        self._calculate_regime_indicators()

    def _calculate_regime_indicators(self):
        """📊 Calculate technical indicators for regime classification"""

        print("📊 Calculating regime indicators...")

        df = self.price_data

        # 1. ADX for trend strength (14-period)
        high = df['High']
        low = df['Low']
        close = df['Close']

        # True Range
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # Directional Movement
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low

        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

        # Smoothed TR and DM
        atr14 = tr.rolling(14).mean()
        plus_di = 100 * pd.Series(plus_dm).rolling(14).mean() / atr14
        minus_di = 100 * pd.Series(minus_dm).rolling(14).mean() / atr14

        # ADX calculation
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(14).mean()

        self.price_data['ADX'] = adx

        # 2. ATR for volatility (14-period)
        self.price_data['ATR'] = atr14
        self.price_data['ATR_Pct'] = (atr14 / close) * 100

        # ATR percentile
        self.price_data['ATR_Percentile'] = self.price_data['ATR_Pct'].rolling(100).apply(
            lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100 if len(x) > 0 else 50
        )

        # 3. SMA for trend direction (50-period)
        sma50 = close.rolling(50).mean()
        self.price_data['SMA50'] = sma50
        self.price_data['Price_vs_SMA'] = ((close - sma50) / sma50) * 100

        # 4. Rate of Change for momentum (10-period)
        roc10 = ((close - close.shift(10)) / close.shift(10)) * 100
        self.price_data['ROC10'] = roc10

        print("✅ Regime indicators calculated")

    def classify_regime(self, timestamp: pd.Timestamp) -> Dict[str, str]:
        """
        🏷️ Classify market regime at specific timestamp

        Returns:
            Dict with regime classifications
        """
        try:
            row = self.price_data.loc[timestamp]
        except KeyError:
            # Find nearest timestamp
            idx = self.price_data.index.get_indexer([timestamp], method='nearest')[0]
            row = self.price_data.iloc[idx]

        regime = {}

        # 1. Trend Classification (ADX)
        adx = row['ADX']
        if pd.isna(adx):
            regime['trend'] = 'unknown'
        elif adx > 25:
            regime['trend'] = 'trending'
        else:
            regime['trend'] = 'ranging'

        # 2. Volatility Classification (ATR Percentile)
        atr_pct = row['ATR_Percentile']
        if pd.isna(atr_pct):
            regime['volatility'] = 'unknown'
        elif atr_pct > 70:
            regime['volatility'] = 'high'
        elif atr_pct < 30:
            regime['volatility'] = 'low'
        else:
            regime['volatility'] = 'medium'

        # 3. Direction Classification (Price vs SMA50)
        price_vs_sma = row['Price_vs_SMA']
        if pd.isna(price_vs_sma):
            regime['direction'] = 'unknown'
        elif price_vs_sma > 2:
            regime['direction'] = 'bull'
        elif price_vs_sma < -2:
            regime['direction'] = 'bear'
        else:
            regime['direction'] = 'neutral'

        # 4. Momentum Classification (ROC)
        roc = row['ROC10']
        if pd.isna(roc):
            regime['momentum'] = 'unknown'
        elif roc > 5:
            regime['momentum'] = 'strong_up'
        elif roc < -5:
            regime['momentum'] = 'strong_down'
        else:
            regime['momentum'] = 'weak'

        # Composite regime label
        regime['composite'] = f"{regime['trend']}_{regime['volatility']}_{regime['direction']}"

        return regime

    def analyze(self) -> RegimeAnalysisResult:
        """
        🌦️ Perform comprehensive regime analysis

        Returns:
            RegimeAnalysisResult with complete analysis
        """
        print("\n🌦️ Starting Market Regime Analysis...")
        print("=" * 60)

        # Classify regime for each trade
        trades_by_regime = {}
        regime_returns = {}
        regime_counts = {}
        regime_trades = {}

        for idx, trade in self.trades_data.iterrows():
            entry_time = trade['EntryTime']
            regime = self.classify_regime(entry_time)

            # Store regime classification with trade
            trade_dict = trade.to_dict()
            trade_dict['regime'] = regime

            composite = regime['composite']

            # Initialize if first trade in this regime
            if composite not in trades_by_regime:
                trades_by_regime[composite] = []
                regime_returns[composite] = []
                regime_counts[composite] = 0
                regime_trades[composite] = []

            trades_by_regime[composite].append(trade_dict)
            regime_trades[composite].append(trade_dict)

            # Calculate return
            if 'ReturnPct' in trade:
                ret = trade['ReturnPct']
            elif 'Return_%' in trade:
                ret = trade['Return_%']
            else:
                ret = (trade['ExitPrice'] - trade['EntryPrice']) / trade['EntryPrice'] * 100

            regime_returns[composite].append(ret)
            regime_counts[composite] += 1

        # Calculate regime statistics
        regime_stats = {}
        for regime in regime_returns:
            returns = regime_returns[regime]
            trades = trades_by_regime[regime]

            winners = [r for r in returns if r > 0]
            win_rate = (len(winners) / len(returns) * 100) if len(returns) > 0 else 0

            avg_return = np.mean(returns) if returns else 0
            std_return = np.std(returns) if returns else 0
            sharpe = (avg_return / std_return) if std_return > 0 else 0

            regime_stats[regime] = {
                'count': len(returns),
                'avg_return': avg_return,
                'win_rate': win_rate,
                'sharpe': sharpe,
                'total_return': sum(returns)
            }

        # Identify best/worst regimes
        sorted_by_return = sorted(regime_stats.items(), key=lambda x: x[1]['avg_return'], reverse=True)
        best_regime = sorted_by_return[0][0] if sorted_by_return else 'unknown'
        worst_regime = sorted_by_return[-1][0] if sorted_by_return else 'unknown'

        # Generate recommendations
        avoid_regimes = [r for r, s in regime_stats.items() if s['avg_return'] < -1.0 or s['win_rate'] < 40]
        prefer_regimes = [r for r, s in regime_stats.items() if s['avg_return'] > 1.0 and s['win_rate'] > 55]

        # Extract regime definitions
        regime_definitions = {
            'trending': 'ADX > 25 (strong directional movement)',
            'ranging': 'ADX < 25 (choppy, sideways movement)',
            'high_vol': 'ATR Percentile > 70 (high volatility)',
            'medium_vol': 'ATR Percentile 30-70 (normal volatility)',
            'low_vol': 'ATR Percentile < 30 (low volatility)',
            'bull': 'Price > 2% above 50 SMA',
            'bear': 'Price > 2% below 50 SMA',
            'neutral': 'Price within ±2% of 50 SMA'
        }

        result = RegimeAnalysisResult(
            total_trades=len(self.trades_data),
            regime_counts={r: s['count'] for r, s in regime_stats.items()},
            regime_returns={r: s['avg_return'] for r, s in regime_stats.items()},
            regime_win_rates={r: s['win_rate'] for r, s in regime_stats.items()},
            regime_sharpe={r: s['sharpe'] for r, s in regime_stats.items()},
            best_regime=best_regime,
            worst_regime=worst_regime,
            trades_by_regime=regime_trades,
            avoid_regimes=avoid_regimes,
            prefer_regimes=prefer_regimes,
            regime_definitions=regime_definitions
        )

        self._print_results(result)

        return result

    def _print_results(self, result: RegimeAnalysisResult):
        """📊 Print formatted regime analysis results"""
        print("=" * 60)
        print("🌦️ MARKET REGIME ANALYSIS RESULTS")
        print("=" * 60)

        print(f"\n📊 REGIME PERFORMANCE BREAKDOWN:")
        print(f"{'Regime':<35} {'Trades':>7} {'Avg Return':>12} {'Win Rate':>10} {'Sharpe':>8}")
        print("-" * 80)

        # Sort by average return
        sorted_regimes = sorted(result.regime_returns.items(), key=lambda x: x[1], reverse=True)

        for regime, avg_return in sorted_regimes:
            count = result.regime_counts[regime]
            win_rate = result.regime_win_rates[regime]
            sharpe = result.regime_sharpe[regime]

            status = "✅" if avg_return > 1.0 and win_rate > 55 else ("⚠️" if avg_return < 0 else "  ")

            print(f"{status} {regime:<32} {count:>7} {avg_return:>11.2f}% {win_rate:>9.1f}% {sharpe:>8.2f}")

        print(f"\n🏆 BEST REGIME: {result.best_regime}")
        best_return = result.regime_returns[result.best_regime]
        best_wr = result.regime_win_rates[result.best_regime]
        print(f"   Avg Return: {best_return:.2f}% | Win Rate: {best_wr:.1f}%")

        print(f"\n❌ WORST REGIME: {result.worst_regime}")
        worst_return = result.regime_returns[result.worst_regime]
        worst_wr = result.regime_win_rates[result.worst_regime]
        print(f"   Avg Return: {worst_return:.2f}% | Win Rate: {worst_wr:.1f}%")

        if result.prefer_regimes:
            print(f"\n✅ PREFERRED REGIMES (Trade Only These):")
            for regime in result.prefer_regimes:
                print(f"   • {regime}")

        if result.avoid_regimes:
            print(f"\n⚠️ AVOID REGIMES (Turn Strategy OFF):")
            for regime in result.avoid_regimes:
                print(f"   • {regime}")

        print("\n" + "=" * 60)


def run_regime_analysis(strategy_name: str,
                       price_data_path: str,
                       trades_csv_path: str,
                       output_dir: str = 'strategies/diagnostics/results') -> RegimeAnalysisResult:
    """
    🌦️ Run market regime analysis on strategy backtest results

    Args:
        strategy_name: Name of strategy being analyzed
        price_data_path: Path to OHLCV price data CSV
        trades_csv_path: Path to trades CSV from backtest
        output_dir: Directory to save results

    Returns:
        RegimeAnalysisResult object
    """
    import os

    print(f"\n🌦️ Regime Analysis: {strategy_name}")
    print("=" * 70)

    # Load data
    print(f"📊 Loading price data: {price_data_path}")
    price_data = pd.read_csv(price_data_path)

    # Handle different date column names
    date_cols = [col for col in price_data.columns if 'date' in col.lower() or 'time' in col.lower()]
    if date_cols:
        price_data[date_cols[0]] = pd.to_datetime(price_data[date_cols[0]])
        price_data.set_index(date_cols[0], inplace=True)

    # 🔧 Standardize OHLCV column names to Titlecase (fixes lowercase data files)
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        if col not in price_data.columns:
            # Try case-insensitive match
            matching = [c for c in price_data.columns if c.lower() == col.lower()]
            if matching:
                price_data[col] = price_data[matching[0]]
            else:
                print(f"⚠️ Warning: Missing column {col} in price data")

    print(f"📊 Loading trades data: {trades_csv_path}")
    trades_data = pd.read_csv(trades_csv_path)

    # Convert times to datetime
    if 'EntryTime' in trades_data.columns:
        trades_data['EntryTime'] = pd.to_datetime(trades_data['EntryTime'])
    if 'ExitTime' in trades_data.columns:
        trades_data['ExitTime'] = pd.to_datetime(trades_data['ExitTime'])

    # Run analysis
    analyzer = MarketRegimeAnalyzer(price_data, trades_data)
    result = analyzer.analyze()

    # Save results
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'{strategy_name}_regime_analysis.csv')

    # Create summary DataFrame
    summary_rows = []
    for regime in result.regime_counts:
        summary_rows.append({
            'Strategy': strategy_name,
            'Regime': regime,
            'Trade_Count': result.regime_counts[regime],
            'Avg_Return': result.regime_returns[regime],
            'Win_Rate': result.regime_win_rates[regime],
            'Sharpe': result.regime_sharpe[regime],
            'Status': 'PREFER' if regime in result.prefer_regimes else ('AVOID' if regime in result.avoid_regimes else 'NEUTRAL')
        })

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_path, index=False)
    print(f"\n💾 Results saved to: {output_path}")

    return result


if __name__ == "__main__":
    """🧪 Example usage"""

    print("🌦️ Market Regime Analyzer - Phase 0 Diagnostic Tool")
    print("=" * 70)
    print("\n📝 Usage:")
    print("   from strategies.diagnostics.regime_analyzer import run_regime_analysis")
    print("   result = run_regime_analysis(")
    print("       strategy_name='Breakout_LINK_1d',")
    print("       price_data_path='data/LINKUSD-1d.csv',")
    print("       trades_csv_path='results/breakout_link_trades.csv'")
    print("   )")
    print("\n🌙💫🚀 Ready to identify best market conditions!")
