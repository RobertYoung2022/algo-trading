"""
🔬 Trade Autopsy Analyzer - Phase 0 Diagnostic Tool
=====================================================
Performs post-mortem analysis on every losing trade to identify failure patterns.

🎯 Purpose:
    - Categorize losing trades into specific failure modes
    - Quantify damage from each failure type
    - Identify fixable vs fundamental issues
    - Generate actionable recommendations

💫 Failure Categories:
    1. False Breakout - Price reverses immediately after entry
    2. Late Exit - Gave back profits before exit
    3. Stopped at Bottom - Stop hit right before reversal
    4. Premature Entry - Entered too early in move
    5. Legitimate Loss - Fundamentally correct exit

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
class TradeAutopsyResult:
    """Results from trade autopsy analysis"""
    total_trades: int
    losing_trades: int
    winning_trades: int

    # Failure category counts
    false_breakouts: int
    late_exits: int
    stopped_at_bottom: int
    premature_entries: int
    legitimate_losses: int

    # Financial impact
    false_breakout_damage: float
    late_exit_damage: float
    stopped_bottom_damage: float
    premature_entry_damage: float
    legitimate_loss_damage: float

    # Detailed trade lists
    categorized_trades: Dict[str, List[Dict]]

    # Summary statistics
    fixable_loss_pct: float
    fundamental_loss_pct: float
    total_preventable_damage: float


class TradeAutopsyAnalyzer:
    """
    🔬 Analyzes trade-by-trade data to identify failure patterns

    Uses price action analysis to categorize why trades failed
    """

    def __init__(self, price_data: pd.DataFrame, trades_data: pd.DataFrame):
        """
        Initialize analyzer with price and trade data

        Args:
            price_data: OHLCV DataFrame with datetime index
            trades_data: Trades DataFrame from backtesting.py
        """
        self.price_data = price_data
        self.trades_data = trades_data

        # Ensure price data has datetime index
        if not isinstance(self.price_data.index, pd.DatetimeIndex):
            raise ValueError("price_data must have DatetimeIndex")

    def analyze(self,
                false_breakout_bars: int = 5,
                late_exit_threshold_pct: float = 30.0,
                stop_reversal_bars: int = 10) -> TradeAutopsyResult:
        """
        🔬 Perform comprehensive trade autopsy

        Args:
            false_breakout_bars: Bars to check for reversal (default 5)
            late_exit_threshold_pct: % profit given back for late exit (default 30%)
            stop_reversal_bars: Bars to check for reversal after stop (default 10)

        Returns:
            TradeAutopsyResult with complete analysis
        """
        print("\n🔬 Starting Trade Autopsy Analysis...")
        print("=" * 60)

        categorized = {
            'false_breakout': [],
            'late_exit': [],
            'stopped_at_bottom': [],
            'premature_entry': [],
            'legitimate_loss': []
        }

        damage = {
            'false_breakout': 0.0,
            'late_exit': 0.0,
            'stopped_at_bottom': 0.0,
            'premature_entry': 0.0,
            'legitimate_loss': 0.0
        }

        total_trades = len(self.trades_data)
        losing_trades = len(self.trades_data[self.trades_data['PnL'] < 0])
        winning_trades = len(self.trades_data[self.trades_data['PnL'] > 0])

        print(f"📊 Total Trades: {total_trades}")
        print(f"   Winners: {winning_trades} ({winning_trades/total_trades*100:.1f}%)")
        print(f"   Losers: {losing_trades} ({losing_trades/total_trades*100:.1f}%)")
        print(f"\n🔍 Analyzing losing trades...\n")

        # Analyze each losing trade
        for idx, trade in self.trades_data[self.trades_data['PnL'] < 0].iterrows():
            category, analysis = self._categorize_loss(
                trade,
                false_breakout_bars,
                late_exit_threshold_pct,
                stop_reversal_bars
            )

            categorized[category].append(analysis)
            damage[category] += abs(trade['PnL'])

        # Calculate fixable vs fundamental
        fixable_categories = ['false_breakout', 'late_exit', 'stopped_at_bottom', 'premature_entry']
        fixable_damage = sum(damage[cat] for cat in fixable_categories)
        total_damage = sum(damage.values())

        fixable_pct = (fixable_damage / total_damage * 100) if total_damage > 0 else 0
        fundamental_pct = 100 - fixable_pct

        # Create result
        result = TradeAutopsyResult(
            total_trades=total_trades,
            losing_trades=losing_trades,
            winning_trades=winning_trades,

            false_breakouts=len(categorized['false_breakout']),
            late_exits=len(categorized['late_exit']),
            stopped_at_bottom=len(categorized['stopped_at_bottom']),
            premature_entries=len(categorized['premature_entry']),
            legitimate_losses=len(categorized['legitimate_loss']),

            false_breakout_damage=damage['false_breakout'],
            late_exit_damage=damage['late_exit'],
            stopped_bottom_damage=damage['stopped_at_bottom'],
            premature_entry_damage=damage['premature_entry'],
            legitimate_loss_damage=damage['legitimate_loss'],

            categorized_trades=categorized,

            fixable_loss_pct=fixable_pct,
            fundamental_loss_pct=fundamental_pct,
            total_preventable_damage=fixable_damage
        )

        self._print_results(result)

        return result

    def _categorize_loss(self,
                         trade: pd.Series,
                         false_breakout_bars: int,
                         late_exit_threshold_pct: float,
                         stop_reversal_bars: int) -> Tuple[str, Dict]:
        """
        🔍 Categorize a single losing trade

        Returns:
            Tuple of (category_name, analysis_dict)
        """
        entry_time = trade['EntryTime']
        exit_time = trade['ExitTime']
        entry_price = trade['EntryPrice']
        exit_price = trade['ExitPrice']
        pnl = trade['PnL']

        # Get price action during trade
        mask = (self.price_data.index >= entry_time) & (self.price_data.index <= exit_time)
        trade_prices = self.price_data[mask]

        if len(trade_prices) == 0:
            return 'legitimate_loss', {
                'trade': trade.to_dict(),
                'reason': 'Insufficient price data'
            }

        # Calculate max favorable excursion (MFE) and max adverse excursion (MAE)
        if trade['Size'] > 0:  # Long trade
            mfe = (trade_prices['High'].max() - entry_price) / entry_price * 100
            mae = (trade_prices['Low'].min() - entry_price) / entry_price * 100
        else:  # Short trade
            mfe = (entry_price - trade_prices['Low'].min()) / entry_price * 100
            mae = (entry_price - trade_prices['High'].max()) / entry_price * 100

        actual_pnl_pct = (exit_price - entry_price) / entry_price * 100

        # 1. Check for FALSE BREAKOUT
        # Entry looks good, but price reverses within a few bars
        if len(trade_prices) <= false_breakout_bars:
            if abs(mae) > abs(mfe) * 2:  # Adverse move 2× larger than favorable
                return 'false_breakout', {
                    'trade': trade.to_dict(),
                    'mfe': mfe,
                    'mae': mae,
                    'bars_held': len(trade_prices),
                    'reason': f'Price reversed within {len(trade_prices)} bars, MAE {mae:.2f}% vs MFE {mfe:.2f}%'
                }

        # 2. Check for LATE EXIT
        # Had significant profit but gave it back
        if mfe > 2.0:  # Had at least 2% profit at some point
            profit_given_back_pct = (mfe - actual_pnl_pct) / mfe * 100 if mfe != 0 else 0

            if profit_given_back_pct > late_exit_threshold_pct:
                return 'late_exit', {
                    'trade': trade.to_dict(),
                    'mfe': mfe,
                    'mae': mae,
                    'peak_profit_pct': mfe,
                    'profit_given_back_pct': profit_given_back_pct,
                    'reason': f'Had {mfe:.2f}% profit, gave back {profit_given_back_pct:.1f}%'
                }

        # 3. Check for STOPPED AT BOTTOM
        # Stop hit, then price immediately reversed
        # Get prices AFTER exit
        after_exit_mask = self.price_data.index > exit_time
        after_exit_prices = self.price_data[after_exit_mask].head(stop_reversal_bars)

        if len(after_exit_prices) > 0:
            if trade['Size'] > 0:  # Long trade
                # Check if price rallied significantly after stop
                post_exit_high = after_exit_prices['High'].max()
                reversal_pct = (post_exit_high - exit_price) / exit_price * 100

                if reversal_pct > 3.0:  # Price rallied 3%+ after stop
                    return 'stopped_at_bottom', {
                        'trade': trade.to_dict(),
                        'mfe': mfe,
                        'mae': mae,
                        'post_exit_reversal_pct': reversal_pct,
                        'reason': f'Stopped out, then price rallied {reversal_pct:.2f}% within {len(after_exit_prices)} bars'
                    }

        # 4. Check for PREMATURE ENTRY
        # Entered, but price continued in unfavorable direction before eventual target
        if abs(mae) > 5.0 and mfe > 0:  # Large adverse move but eventually went favorable
            return 'premature_entry', {
                'trade': trade.to_dict(),
                'mfe': mfe,
                'mae': mae,
                'reason': f'Entered too early, MAE {mae:.2f}% before reaching MFE {mfe:.2f}%'
            }

        # 5. LEGITIMATE LOSS
        # Trade went against us and exited correctly
        return 'legitimate_loss', {
            'trade': trade.to_dict(),
            'mfe': mfe,
            'mae': mae,
            'reason': 'Market moved against position, legitimate directional loss'
        }

    def _print_results(self, result: TradeAutopsyResult):
        """📊 Print formatted autopsy results"""
        print("=" * 60)
        print("📊 TRADE AUTOPSY RESULTS")
        print("=" * 60)

        print(f"\n🎯 LOSS BREAKDOWN:")
        print(f"   1. False Breakouts:    {result.false_breakouts:3d} trades (${result.false_breakout_damage:,.2f} damage)")
        print(f"   2. Late Exits:         {result.late_exits:3d} trades (${result.late_exit_damage:,.2f} damage)")
        print(f"   3. Stopped at Bottom:  {result.stopped_at_bottom:3d} trades (${result.stopped_bottom_damage:,.2f} damage)")
        print(f"   4. Premature Entries:  {result.premature_entries:3d} trades (${result.premature_entry_damage:,.2f} damage)")
        print(f"   5. Legitimate Losses:  {result.legitimate_losses:3d} trades (${result.legitimate_loss_damage:,.2f} damage)")

        total_damage = (result.false_breakout_damage + result.late_exit_damage +
                       result.stopped_bottom_damage + result.premature_entry_damage +
                       result.legitimate_loss_damage)

        print(f"\n💉 DAMAGE ANALYSIS:")
        print(f"   Total Loss Damage:     ${total_damage:,.2f}")
        print(f"   Fixable Losses:        {result.fixable_loss_pct:.1f}%")
        print(f"   Fundamental Losses:    {result.fundamental_loss_pct:.1f}%")
        print(f"   Preventable Damage:    ${result.total_preventable_damage:,.2f}")

        print(f"\n🎯 PRIORITY FIXES:")

        # Rank fixes by damage
        fixes = [
            ('False Breakout Filter', result.false_breakout_damage, result.false_breakouts),
            ('Better Exit Rules', result.late_exit_damage, result.late_exits),
            ('Smarter Stop Placement', result.stopped_bottom_damage, result.stopped_at_bottom),
            ('Entry Timing Filter', result.premature_entry_damage, result.premature_entries)
        ]

        fixes.sort(key=lambda x: x[1], reverse=True)

        for i, (fix, damage, count) in enumerate(fixes, 1):
            if damage > 0:
                damage_pct = (damage / total_damage * 100) if total_damage > 0 else 0
                print(f"   {i}. {fix:25s} - ${damage:,.2f} ({damage_pct:.1f}% of losses, {count} trades)")

        print("\n" + "=" * 60)


def run_trade_autopsy(strategy_name: str,
                      price_data_path: str,
                      trades_csv_path: str,
                      output_dir: str = 'strategies/diagnostics/results') -> TradeAutopsyResult:
    """
    🔬 Run trade autopsy analysis on strategy backtest results

    Args:
        strategy_name: Name of strategy being analyzed
        price_data_path: Path to OHLCV price data CSV
        trades_csv_path: Path to trades CSV from backtest
        output_dir: Directory to save results

    Returns:
        TradeAutopsyResult object
    """
    import os

    print(f"\n🔬 Trade Autopsy: {strategy_name}")
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
    analyzer = TradeAutopsyAnalyzer(price_data, trades_data)
    result = analyzer.analyze()

    # Save results
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'{strategy_name}_trade_autopsy.csv')

    # Create summary DataFrame
    summary_df = pd.DataFrame([{
        'Strategy': strategy_name,
        'Total_Trades': result.total_trades,
        'Winners': result.winning_trades,
        'Losers': result.losing_trades,
        'False_Breakouts': result.false_breakouts,
        'Late_Exits': result.late_exits,
        'Stopped_At_Bottom': result.stopped_at_bottom,
        'Premature_Entries': result.premature_entries,
        'Legitimate_Losses': result.legitimate_losses,
        'False_Breakout_Damage': result.false_breakout_damage,
        'Late_Exit_Damage': result.late_exit_damage,
        'Stopped_Bottom_Damage': result.stopped_bottom_damage,
        'Premature_Entry_Damage': result.premature_entry_damage,
        'Legitimate_Loss_Damage': result.legitimate_loss_damage,
        'Fixable_Loss_Pct': result.fixable_loss_pct,
        'Preventable_Damage': result.total_preventable_damage,
        'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }])

    summary_df.to_csv(output_path, index=False)
    print(f"\n💾 Results saved to: {output_path}")

    return result


if __name__ == "__main__":
    """🧪 Example usage"""

    # Example: Analyze Breakout Momentum LINK-1d strategy
    # (Requires trade export from backtesting.py)

    print("🔬 Trade Autopsy Analyzer - Phase 0 Diagnostic Tool")
    print("=" * 70)
    print("\n📝 Usage:")
    print("   from strategies.diagnostics.trade_autopsy import run_trade_autopsy")
    print("   result = run_trade_autopsy(")
    print("       strategy_name='Breakout_LINK_1d',")
    print("       price_data_path='data/LINKUSD-1d.csv',")
    print("       trades_csv_path='results/breakout_link_trades.csv'")
    print("   )")
    print("\n🌙💫🚀 Ready to diagnose trading failures!")
