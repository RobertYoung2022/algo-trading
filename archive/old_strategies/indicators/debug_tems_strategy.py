"""
Debug Version of TEMS Strategy
===============================
Simplified version with debug output to understand why no trades are occurring
"""

from backtesting import Strategy
import talib as ta
import pandas as pd
import numpy as np


class DebugTEMSStrategy(Strategy):
    """Debug version of TEMS with relaxed conditions and debug output"""

    # Strategy Parameters
    ema_fast = 8
    ema_medium = 21
    ema_slow = 55
    rsi_period = 14
    atr_period = 14
    volume_sma = 20

    # Risk Management
    stop_loss_atr_mult = 2.5
    take_profit_atr_mult = 3.0

    def init(self):
        """Initialize indicators"""
        # Triple EMA System
        self.ema_fast = self.I(ta.EMA, self.data.Close, self.ema_fast)
        self.ema_medium = self.I(ta.EMA, self.data.Close, self.ema_medium)
        self.ema_slow = self.I(ta.EMA, self.data.Close, self.ema_slow)

        # RSI
        self.rsi = self.I(ta.RSI, self.data.Close, self.rsi_period)

        # ATR
        self.atr = self.I(ta.ATR, self.data.High, self.data.Low,
                          self.data.Close, self.atr_period)

        # Volume SMA
        self.volume_sma = self.I(ta.SMA, self.data.Volume, self.volume_sma)

        # Track signals
        self.signal_count = 0
        self.debug_printed = False

    def next(self):
        """Trading logic with debug output"""

        # Skip if indicators not ready
        if len(self.data) < self.ema_slow:
            return

        price = self.data.Close[-1]
        volume = self.data.Volume[-1]

        # Print debug info once
        if not self.debug_printed and len(self.data) > 100:
            print(f"\n=== DEBUG INFO at bar {len(self.data)} ===")
            print(f"Price: {price:.2f}")
            print(f"EMA Fast: {self.ema_fast[-1]:.2f}")
            print(f"EMA Medium: {self.ema_medium[-1]:.2f}")
            print(f"EMA Slow: {self.ema_slow[-1]:.2f}")
            print(f"RSI: {self.rsi[-1]:.2f}")
            print(f"Volume: {volume:.2f}")
            print(f"Volume SMA: {self.volume_sma[-1]:.2f}")
            print(f"ATR: {self.atr[-1]:.2f}")

            # Check individual conditions
            print("\n=== CONDITION CHECKS ===")
            print(f"EMA Alignment Bullish (F>M>S): {self.ema_fast[-1] > self.ema_medium[-1]} and {self.ema_medium[-1] > self.ema_slow[-1]}")
            print(f"RSI > 50: {self.rsi[-1] > 50}")
            print(f"Volume > SMA: {volume > self.volume_sma[-1]}")
            print(f"Price > Fast EMA: {price > self.ema_fast[-1]}")
            self.debug_printed = True

        # Only trade if not in position
        if self.position.size == 0:

            # Relaxed Long Entry (just EMA alignment)
            simple_long = (
                self.ema_fast[-1] > self.ema_medium[-1] and
                self.ema_medium[-1] > self.ema_slow[-1]
            )

            # Original Long Entry
            full_long = (
                self.ema_fast[-1] > self.ema_medium[-1] and
                self.ema_medium[-1] > self.ema_slow[-1] and
                self.rsi[-1] > 50 and
                volume > self.volume_sma[-1] and
                price > self.ema_fast[-1]
            )

            # Count signals
            if simple_long:
                self.signal_count += 1
                if self.signal_count <= 5:  # Print first 5 signals
                    print(f"\nSignal #{self.signal_count} at bar {len(self.data)}")
                    print(f"Simple Long: True, Full Long: {full_long}")

            # Take position on simple signal for testing
            if simple_long:
                self.buy(size=0.95)
                print(f"\n=== LONG ENTRY at bar {len(self.data)} ===")
                print(f"Entry Price: {price:.2f}")

        # Simple exit - just use stop loss
        elif self.position.size > 0:
            # Exit on EMA cross
            if self.ema_fast[-1] < self.ema_medium[-1]:
                self.position.close()
                print(f"\n=== EXIT at bar {len(self.data)} ===")
                print(f"Exit Price: {price:.2f}")