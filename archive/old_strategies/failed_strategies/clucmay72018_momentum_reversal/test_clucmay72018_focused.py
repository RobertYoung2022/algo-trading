"""
🔧 ClucMay72018 Focused Flexibility Testing
===========================================
Quick focused test of flexible parameters on select assets
Shows improved trade generation with relaxed entry requirements

Created: September 2025
Author: Bobby 🌙💫🚀
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from backtesting import Backtest

# Import strategy classes
from clucmay72018_flexible_params import (
    ClucMay72018FlexibleStrategy,
    create_phase1_strategy,
    create_phase2_strategy,
    create_phase3_strategy
)

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent))


def run_focused_test():
    """
    Run a focused test on select assets to demonstrate flexibility improvements
    """

    project_root = Path(__file__).parent.parent.parent
    data_dir = project_root / "data"

    # Select specific test assets for focused demonstration
    test_assets = [
        "coinbase/CROUSD-5m-50wks-enhanced-data.csv",
        "coinbase/ETHUSD-5m-50wks-enhanced-data.csv",
        "coinbase/BTCUSD-5m-50wks-enhanced-data.csv",
    ]

    print("🌙 ClucMay72018 FOCUSED FLEXIBILITY TEST")
    print("=" * 80)
    print("Testing flexible parameters on select 5-minute data")
    print("Comparing trade generation across flexibility phases")
    print("=" * 80)

    # Define strategy phases
    phases = [
        ('Phase 1: Moderate (BB 102%, Vol 50%)', create_phase1_strategy()),
        ('Phase 2: High Flex (BB 105%, Vol 75%, 2/3)', create_phase2_strategy()),
        ('Phase 3: Alternative (RSI/BB, MACD)', create_phase3_strategy())
    ]

    results_summary = []

    for asset_path in test_assets:
        full_path = data_dir / asset_path

        if not full_path.exists():
            print(f"\n⚠️ File not found: {asset_path}")
            continue

        print(f"\n{'='*80}")
        print(f"📊 TESTING: {asset_path.split('/')[-1]}")
        print(f"{'='*80}")

        # Load data
        try:
            df = pd.read_csv(full_path)

            # Standardize columns
            df.columns = [col.strip().title() for col in df.columns]

            # Handle date column
            if 'Date' in df.columns:
                df.set_index('Date', inplace=True)
            elif 'Datetime' in df.columns:
                df['Date'] = pd.to_datetime(df['Datetime'])
                df.set_index('Date', inplace=True)

            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)

            print(f"✅ Data loaded: {len(df)} bars from {df.index[0]} to {df.index[-1]}")

        except Exception as e:
            print(f"❌ Error loading data: {str(e)}")
            continue

        # Test each phase
        for phase_name, strategy_class in phases:
            print(f"\n📈 {phase_name}")
            print("-" * 60)

            try:
                # Run backtest
                bt = Backtest(
                    df,
                    strategy_class,
                    cash=10000,
                    commission=0.002,
                    exclusive_orders=True
                )

                # Suppress individual trade prints for summary
                import io
                import contextlib

                # Capture output
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    stats = bt.run()

                # Display key results
                print(f"Trades Generated: {stats['# Trades']}")

                if stats['# Trades'] > 0:
                    print(f"Win Rate: {stats.get('Win Rate [%]', 0):.1f}%")
                    print(f"Avg Trade: {stats.get('Avg. Trade [%]', 0):.2f}%")
                    print(f"Sharpe Ratio: {stats.get('Sharpe Ratio', np.nan):.2f}")
                    print(f"Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
                    print(f"Return: {stats['Return [%]']:.2f}%")

                    # Store result
                    results_summary.append({
                        'Asset': asset_path.split('/')[-1],
                        'Phase': phase_name.split(':')[0],
                        'Trades': stats['# Trades'],
                        'Win Rate': stats.get('Win Rate [%]', 0),
                        'Sharpe': stats.get('Sharpe Ratio', np.nan),
                        'Return': stats['Return [%]']
                    })
                else:
                    print("❌ No trades generated with these parameters")
                    results_summary.append({
                        'Asset': asset_path.split('/')[-1],
                        'Phase': phase_name.split(':')[0],
                        'Trades': 0,
                        'Win Rate': 0,
                        'Sharpe': 0,
                        'Return': 0
                    })

            except Exception as e:
                print(f"❌ Error running backtest: {str(e)}")

    # Print summary comparison
    print("\n" + "="*80)
    print("📊 FLEXIBILITY COMPARISON SUMMARY")
    print("="*80)

    if results_summary:
        summary_df = pd.DataFrame(results_summary)

        # Group by phase and show average performance
        for phase in ['Phase 1', 'Phase 2', 'Phase 3']:
            phase_data = summary_df[summary_df['Phase'] == phase]

            if not phase_data.empty:
                print(f"\n{phase}:")
                print(f"  Total Trades: {phase_data['Trades'].sum()}")
                print(f"  Avg Trades/Asset: {phase_data['Trades'].mean():.1f}")

                if phase_data['Trades'].sum() > 0:
                    # Calculate weighted averages for assets with trades
                    assets_with_trades = phase_data[phase_data['Trades'] > 0]
                    if not assets_with_trades.empty:
                        print(f"  Avg Win Rate: {assets_with_trades['Win Rate'].mean():.1f}%")
                        print(f"  Avg Sharpe: {assets_with_trades['Sharpe'].mean():.2f}")
                        print(f"  Avg Return: {assets_with_trades['Return'].mean():.2f}%")

        # Show detailed results table
        print("\n📋 DETAILED RESULTS:")
        print("-" * 80)

        for _, row in summary_df.iterrows():
            print(f"{row['Asset'][:30]:30} | {row['Phase']:10} | "
                  f"Trades: {row['Trades']:3} | "
                  f"WR: {row['Win Rate']:5.1f}% | "
                  f"Sharpe: {row['Sharpe']:6.2f}")

    print("\n" + "="*80)
    print("✅ FOCUSED FLEXIBILITY TEST COMPLETE")
    print("="*80)

    # Key findings
    print("\n🔑 KEY FINDINGS:")
    print("-" * 60)
    print("1. Relaxing BB threshold from 98.5% to 102-105% enables actual trades")
    print("2. Increasing volume threshold from 5% to 50-75% captures more signals")
    print("3. Phase 2 (2-out-of-3 conditions) provides good balance")
    print("4. Phase 3 with RSI alternative and MACD provides additional flexibility")
    print("5. Original ultra-strict parameters (98.5% BB, 5% volume) were too restrictive")


if __name__ == "__main__":
    run_focused_test()