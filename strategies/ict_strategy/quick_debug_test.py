#!/usr/bin/env python3
"""
Quick Debug Test - Minimal test to see debug counters
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from strategies.ict_strategy.ict_backtest_strategy import run_ict_backtest

if __name__ == "__main__":
    print("\n" + "="*60)
    print(" QUICK DEBUG TEST")
    print("="*60)

    # Run on 1-year data
    stats = run_ict_backtest(
        data_path="data/storage/dataset_files/BTCUSD-1h-1year-test.csv",
        symbol="BTC-1Y-DEBUG",
        cash=1000000,  # Increased from $10k to $1M for realistic BTC position sizing
        commission=0.002
    )

    print("\n" + "="*60)
    print(" TEST COMPLETE")
    print("="*60)
