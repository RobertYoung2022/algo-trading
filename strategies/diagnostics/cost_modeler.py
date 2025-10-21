"""
💸 Transaction Cost Modeler - Phase 0 Diagnostic Tool
====================================================
Models real-world trading costs impact on strategy profitability.

🎯 Purpose:
    - Calculate commission drag on returns
    - Estimate slippage based on volatility
    - Model bid-ask spread costs
    - Determine if strategy profitable after REAL costs

💫 Cost Components:
    1. Commission - Fixed % per trade (both sides)
    2. Slippage - Variable % based on market volatility
    3. Spread - Bid-ask difference (market orders)
    4. Total Cost Impact - Combined effect on returns

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
class CostModelingResult:
    """Results from transaction cost modeling"""

    # Backtest performance
    backtest_return_pct: float
    backtest_total_return_dollars: float
    total_trades: int

    # Cost breakdown
    total_commission_dollars: float
    total_slippage_dollars: float
    total_spread_dollars: float
    total_costs_dollars: float

    # Cost as percentage
    commission_pct: float
    slippage_pct: float
    spread_pct: float
    total_cost_pct: float

    # Real-world adjusted performance
    real_return_pct: float
    real_total_return_dollars: float

    # Cost efficiency metrics
    cost_to_return_ratio: float
    net_profit_after_costs: float
    still_profitable: bool

    # Recommendations
    max_acceptable_trades: int
    cost_breakdown_per_trade: Dict[str, float]


class TransactionCostModeler:
    """
    💸 Models real-world transaction costs for strategy evaluation

    Estimates commission, slippage, and spread costs
    """

    def __init__(self,
                 trades_data: pd.DataFrame,
                 price_data: Optional[pd.DataFrame] = None,
                 initial_capital: float = 10000.0):
        """
        Initialize cost modeler

        Args:
            trades_data: Trades DataFrame from backtesting.py
            price_data: OHLCV price data (for volatility-based slippage)
            initial_capital: Starting capital for returns calculation
        """
        self.trades_data = trades_data.copy()
        self.price_data = price_data
        self.initial_capital = initial_capital

        # Calculate trade returns if not present
        if 'ReturnPct' not in self.trades_data.columns and 'Return_%' not in self.trades_data.columns:
            if 'PnL' in self.trades_data.columns:
                # Estimate entry capital (assuming proportional to trade size)
                self.trades_data['ReturnPct'] = (self.trades_data['PnL'] / self.initial_capital) * 100
            else:
                self.trades_data['ReturnPct'] = (
                    (self.trades_data['ExitPrice'] - self.trades_data['EntryPrice']) /
                    self.trades_data['EntryPrice'] * 100
                )
        elif 'Return_%' in self.trades_data.columns:
            self.trades_data['ReturnPct'] = self.trades_data['Return_%']

    def model_costs(self,
                   commission_pct: float = 0.10,
                   base_slippage_pct: float = 0.10,
                   spread_pct: float = 0.05) -> CostModelingResult:
        """
        💸 Model transaction costs and calculate real returns

        Args:
            commission_pct: Commission per trade side (default 0.10%)
            base_slippage_pct: Base slippage estimate (default 0.10%)
            spread_pct: Bid-ask spread estimate (default 0.05%)

        Returns:
            CostModelingResult with complete cost analysis
        """
        print("\n💸 Starting Transaction Cost Modeling...")
        print("=" * 60)

        print(f"\n📊 Cost Assumptions:")
        print(f"   Commission: {commission_pct:.2f}% per side ({commission_pct*2:.2f}% round-trip)")
        print(f"   Base Slippage: {base_slippage_pct:.2f}%")
        print(f"   Spread: {spread_pct:.2f}%")

        # Calculate backtest performance (before costs)
        total_trades = len(self.trades_data)
        backtest_return_pct = self.trades_data['ReturnPct'].sum()

        if 'PnL' in self.trades_data.columns:
            backtest_total_return_dollars = self.trades_data['PnL'].sum()
        else:
            backtest_total_return_dollars = (backtest_return_pct / 100) * self.initial_capital

        print(f"\n📈 Backtest Performance (Before Costs):")
        print(f"   Total Trades: {total_trades}")
        print(f"   Total Return: {backtest_return_pct:.2f}%")
        print(f"   Total P&L: ${backtest_total_return_dollars:,.2f}")

        # Calculate costs for each trade
        commission_costs = []
        slippage_costs = []
        spread_costs = []

        for idx, trade in self.trades_data.iterrows():
            # Estimate trade value
            if 'Size' in trade and 'EntryPrice' in trade:
                trade_value = abs(trade['Size'] * trade['EntryPrice'])
            else:
                # Estimate as fraction of capital
                trade_value = self.initial_capital * 0.10  # Assume 10% position

            # Commission (both entry and exit)
            commission = trade_value * (commission_pct / 100) * 2  # Both sides

            # Slippage (volatility-adjusted if price data available)
            if self.price_data is not None and 'EntryTime' in trade:
                try:
                    entry_time = trade['EntryTime']
                    # Get price volatility around entry
                    mask = (self.price_data.index >= entry_time - pd.Timedelta(days=5)) & \
                           (self.price_data.index <= entry_time)
                    recent_prices = self.price_data[mask]

                    if len(recent_prices) > 0:
                        # Calculate ATR-based volatility
                        high_low = recent_prices['High'] - recent_prices['Low']
                        volatility = high_low.mean() / recent_prices['Close'].mean()

                        # Adjust slippage based on volatility
                        # High vol = more slippage, low vol = less slippage
                        vol_multiplier = 1 + (volatility * 5)  # Scale factor
                        adjusted_slippage_pct = base_slippage_pct * vol_multiplier
                    else:
                        adjusted_slippage_pct = base_slippage_pct
                except Exception:
                    adjusted_slippage_pct = base_slippage_pct
            else:
                adjusted_slippage_pct = base_slippage_pct

            slippage = trade_value * (adjusted_slippage_pct / 100) * 2  # Both entry and exit

            # Spread (for market orders)
            spread = trade_value * (spread_pct / 100)

            commission_costs.append(commission)
            slippage_costs.append(slippage)
            spread_costs.append(spread)

        # Total costs
        total_commission = sum(commission_costs)
        total_slippage = sum(slippage_costs)
        total_spread = sum(spread_costs)
        total_costs = total_commission + total_slippage + total_spread

        # Costs as percentage of initial capital
        commission_pct_total = (total_commission / self.initial_capital) * 100
        slippage_pct_total = (total_slippage / self.initial_capital) * 100
        spread_pct_total = (total_spread / self.initial_capital) * 100
        total_cost_pct_total = (total_costs / self.initial_capital) * 100

        print(f"\n💸 Cost Breakdown:")
        print(f"   Commission: ${total_commission:,.2f} ({commission_pct_total:.2f}%)")
        print(f"   Slippage: ${total_slippage:,.2f} ({slippage_pct_total:.2f}%)")
        print(f"   Spread: ${total_spread:,.2f} ({spread_pct_total:.2f}%)")
        print(f"   TOTAL COSTS: ${total_costs:,.2f} ({total_cost_pct_total:.2f}%)")

        # Real-world adjusted returns
        real_return_dollars = backtest_total_return_dollars - total_costs
        real_return_pct = (real_return_dollars / self.initial_capital) * 100

        print(f"\n📊 Real-World Performance (After Costs):")
        print(f"   Adjusted Return: {real_return_pct:.2f}%")
        print(f"   Adjusted P&L: ${real_return_dollars:,.2f}")

        still_profitable = real_return_dollars > 0

        if still_profitable:
            print(f"   Status: ✅ STILL PROFITABLE")
        else:
            print(f"   Status: ❌ UNPROFITABLE (costs ate all profits)")

        # Cost efficiency metrics
        if backtest_total_return_dollars != 0:
            cost_to_return_ratio = total_costs / abs(backtest_total_return_dollars)
        else:
            cost_to_return_ratio = 999.0

        print(f"\n📉 Cost Efficiency:")
        print(f"   Cost/Return Ratio: {cost_to_return_ratio:.2f}")
        if cost_to_return_ratio > 1.0:
            print(f"   ⚠️ Costs EXCEED profits ({cost_to_return_ratio:.1f}x)")
        elif cost_to_return_ratio > 0.5:
            print(f"   ⚠️ Costs eat {cost_to_return_ratio*100:.0f}% of profits")
        else:
            print(f"   ✅ Costs manageable ({cost_to_return_ratio*100:.0f}% of profits)")

        # Calculate maximum acceptable trades
        # How many trades before costs equal profits?
        if backtest_return_pct > 0:
            avg_cost_per_trade_pct = total_cost_pct_total / total_trades
            max_trades = int(backtest_return_pct / avg_cost_per_trade_pct) if avg_cost_per_trade_pct > 0 else 999
        else:
            max_trades = 0

        print(f"\n🎯 Trade Frequency Recommendation:")
        print(f"   Current trades: {total_trades}")
        print(f"   Max profitable trades: {max_trades}")
        if total_trades > max_trades:
            print(f"   ⚠️ Overtrading! Reduce frequency by {((total_trades - max_trades) / total_trades * 100):.0f}%")

        # Per-trade cost breakdown
        avg_commission = total_commission / total_trades if total_trades > 0 else 0
        avg_slippage = total_slippage / total_trades if total_trades > 0 else 0
        avg_spread = total_spread / total_trades if total_trades > 0 else 0

        cost_breakdown_per_trade = {
            'avg_commission': avg_commission,
            'avg_slippage': avg_slippage,
            'avg_spread': avg_spread,
            'avg_total': (avg_commission + avg_slippage + avg_spread)
        }

        result = CostModelingResult(
            backtest_return_pct=backtest_return_pct,
            backtest_total_return_dollars=backtest_total_return_dollars,
            total_trades=total_trades,

            total_commission_dollars=total_commission,
            total_slippage_dollars=total_slippage,
            total_spread_dollars=total_spread,
            total_costs_dollars=total_costs,

            commission_pct=commission_pct_total,
            slippage_pct=slippage_pct_total,
            spread_pct=spread_pct_total,
            total_cost_pct=total_cost_pct_total,

            real_return_pct=real_return_pct,
            real_total_return_dollars=real_return_dollars,

            cost_to_return_ratio=cost_to_return_ratio,
            net_profit_after_costs=real_return_dollars,
            still_profitable=still_profitable,

            max_acceptable_trades=max_trades,
            cost_breakdown_per_trade=cost_breakdown_per_trade
        )

        self._print_verdict(result)

        return result

    def _print_verdict(self, result: CostModelingResult):
        """📊 Print final verdict"""
        print("\n" + "=" * 60)
        print("💸 TRANSACTION COST VERDICT")
        print("=" * 60)

        if result.still_profitable:
            print("✅ Strategy REMAINS PROFITABLE after real-world costs")
            profit_margin = (result.net_profit_after_costs / abs(result.backtest_total_return_dollars) * 100) if result.backtest_total_return_dollars != 0 else 0
            print(f"   Profit margin: {profit_margin:.1f}% of backtest profits remain")

            if profit_margin < 25:
                print(f"   ⚠️ WARNING: Low margin - strategy fragile to cost increases")
            elif profit_margin < 50:
                print(f"   ⚠️ CAUTION: Medium margin - optimize to improve cushion")
            else:
                print(f"   ✅ HEALTHY: Good profit margin after costs")

        else:
            print("❌ Strategy UNPROFITABLE after real-world costs")
            loss_pct = abs(result.net_profit_after_costs / result.backtest_total_return_dollars * 100) if result.backtest_total_return_dollars != 0 else 0
            print(f"   Costs exceed profits by {loss_pct:.1f}%")
            print(f"\n   💡 Recommendations:")
            print(f"      1. Reduce trade frequency (currently {result.total_trades} trades)")
            print(f"      2. Increase profit per trade (better entries/exits)")
            print(f"      3. Use limit orders to reduce slippage")
            print(f"      4. Consider higher timeframes (fewer trades)")

        print("=" * 60)


def run_cost_modeling(strategy_name: str,
                     trades_csv_path: str,
                     price_data_path: Optional[str] = None,
                     initial_capital: float = 10000.0,
                     commission_pct: float = 0.10,
                     slippage_pct: float = 0.10,
                     spread_pct: float = 0.05,
                     output_dir: str = 'strategies/diagnostics/results') -> CostModelingResult:
    """
    💸 Run transaction cost modeling on strategy backtest results

    Args:
        strategy_name: Name of strategy being analyzed
        trades_csv_path: Path to trades CSV from backtest
        price_data_path: Optional path to OHLCV price data
        initial_capital: Starting capital
        commission_pct: Commission % per side
        slippage_pct: Slippage % estimate
        spread_pct: Spread % estimate
        output_dir: Directory to save results

    Returns:
        CostModelingResult object
    """
    import os

    print(f"\n💸 Transaction Cost Modeling: {strategy_name}")
    print("=" * 70)

    # Load data
    print(f"📊 Loading trades data: {trades_csv_path}")
    trades_data = pd.read_csv(trades_csv_path)

    # Convert times to datetime if present
    if 'EntryTime' in trades_data.columns:
        trades_data['EntryTime'] = pd.to_datetime(trades_data['EntryTime'])
    if 'ExitTime' in trades_data.columns:
        trades_data['ExitTime'] = pd.to_datetime(trades_data['ExitTime'])

    # Load price data if provided
    price_data = None
    if price_data_path:
        print(f"📊 Loading price data: {price_data_path}")
        price_data = pd.read_csv(price_data_path)

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

    # Run cost modeling
    modeler = TransactionCostModeler(trades_data, price_data, initial_capital)
    result = modeler.model_costs(commission_pct, slippage_pct, spread_pct)

    # Save results
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'{strategy_name}_cost_modeling.csv')

    summary_df = pd.DataFrame([{
        'Strategy': strategy_name,
        'Backtest_Return_Pct': result.backtest_return_pct,
        'Total_Costs_Pct': result.total_cost_pct,
        'Real_Return_Pct': result.real_return_pct,
        'Total_Trades': result.total_trades,
        'Commission_Cost': result.total_commission_dollars,
        'Slippage_Cost': result.total_slippage_dollars,
        'Spread_Cost': result.total_spread_dollars,
        'Total_Cost': result.total_costs_dollars,
        'Still_Profitable': result.still_profitable,
        'Cost_to_Return_Ratio': result.cost_to_return_ratio,
        'Net_Profit_After_Costs': result.net_profit_after_costs,
        'Max_Acceptable_Trades': result.max_acceptable_trades,
        'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }])

    summary_df.to_csv(output_path, index=False)
    print(f"\n💾 Results saved to: {output_path}")

    return result


if __name__ == "__main__":
    """🧪 Example usage"""

    print("💸 Transaction Cost Modeler - Phase 0 Diagnostic Tool")
    print("=" * 70)
    print("\n📝 Usage:")
    print("   from strategies.diagnostics.cost_modeler import run_cost_modeling")
    print("   result = run_cost_modeling(")
    print("       strategy_name='Breakout_LINK_1d',")
    print("       trades_csv_path='results/breakout_link_trades.csv',")
    print("       price_data_path='data/LINKUSD-1d.csv',")
    print("       commission_pct=0.10,")
    print("       slippage_pct=0.15,")
    print("       spread_pct=0.05")
    print("   )")
    print("\n🌙💫🚀 Ready to model real-world costs!")
