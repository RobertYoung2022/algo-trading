#!/usr/bin/env python3
"""
Debug Backtest - Add extensive logging to see why trades aren't executing
"""

import pandas as pd
import sys
from pathlib import Path
from backtesting import Backtest, Strategy

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from strategies.ict_strategy.signal_generator import ICTSignalGenerator
from strategies.ict_strategy.risk_manager import ICTRiskManager
from strategies.ict_strategy.session_manager import ICTSessionManager


class ICTStrategyDebug(Strategy):
    """ICT Strategy with debug logging"""

    higher_tf_period = 240
    lower_tf_period = 60
    min_confirmations = 1
    max_daily_losses = 2
    base_risk_pct = 0.5
    require_session_filter = False
    use_trailing_stops = True

    def init(self):
        self.signal_generator = ICTSignalGenerator()
        self.risk_manager = ICTRiskManager(
            account_balance=self._broker._cash,
            max_daily_losses=self.max_daily_losses
        )
        self.session_manager = ICTSessionManager()

        self.entry_price = None
        self.current_signal = None
        self.position_id = None
        self.trade_log = []

        # Debug counters
        self.bars_processed = 0
        self.signals_generated = 0
        self.signals_valid = 0
        self.signals_rejected = 0
        self.trades_attempted = 0

        print(f"✅ ICT Strategy DEBUG initialized")

    def next(self):
        self.bars_processed += 1

        if len(self.data) < 100:
            return

        self.risk_manager.account_balance = self.equity

        if not self.position:
            self._check_entry_signal()

    def _check_entry_signal(self):
        """Check for entry with debug logging"""

        # Only check every 10 bars to reduce spam
        if self.bars_processed % 100 != 0:
            return

        df_higher = pd.DataFrame({
            'open': self.data.Open[-200:],
            'high': self.data.High[-200:],
            'low': self.data.Low[-200:],
            'close': self.data.Close[-200:],
        })

        df_lower = pd.DataFrame({
            'open': self.data.Open[-100:],
            'high': self.data.High[-100:],
            'low': self.data.Low[-100:],
            'close': self.data.Close[-100:],
        })

        current_price = self.data.Close[-1]

        for direction in ['long', 'short']:
            signal = self.signal_generator.generate_signal(
                df_higher=df_higher,
                df_lower=df_lower,
                symbol='BACKTEST',
                direction=direction
            )

            self.signals_generated += 1

            if signal['valid']:
                self.signals_valid += 1
                print(f"\n✅ Bar {self.bars_processed}: Valid {direction.upper()} signal!")
                print(f"   Confirmations: {signal['confirmations']}")
                print(f"   Entry: ${signal['entry']:.2f}, Stop: ${signal['stop']:.2f}, Target: ${signal['target']:.2f}")
                print(f"   R:R: {signal['rr_ratio']:.2f}")

                # Check confirmations
                if len(signal['confirmations']) < self.min_confirmations:
                    self.signals_rejected += 1
                    print(f"   ❌ REJECTED: Not enough confirmations ({len(signal['confirmations'])} < {self.min_confirmations})")
                    continue

                # Check risk manager
                position_info = self.risk_manager.calculate_position_size(signal)
                print(f"   Risk Manager: can_trade={position_info['can_trade']}")

                if not position_info['can_trade']:
                    self.signals_rejected += 1
                    print(f"   ❌ REJECTED: Risk manager blocked trade")
                    print(f"      Reason: {position_info.get('reason', 'Unknown')}")
                    continue

                # Calculate size
                size_fraction = min(position_info['risk_amount'] / self.equity, 0.02)
                print(f"   Position size: {size_fraction:.4f} ({size_fraction*100:.2f}%)")

                # Try to execute
                self.trades_attempted += 1

                if direction == 'long' and not self.position:
                    print(f"   🚀 EXECUTING LONG TRADE...")
                    self.buy(size=size_fraction)
                    self.entry_price = current_price
                    self.current_signal = signal

                    self.position_id = self.risk_manager.add_position({
                        'symbol': 'BACKTEST',
                        'direction': 'long',
                        'entry': signal['entry'],
                        'stop': signal['stop'],
                        'target': signal['target'],
                        'size': size_fraction,
                        'risk_amount': position_info['risk_amount']
                    })['id']

                    print(f"   ✅ LONG TRADE EXECUTED")
                    break

                elif direction == 'short' and not self.position:
                    print(f"   🚀 EXECUTING SHORT TRADE...")
                    self.sell(size=size_fraction)
                    self.entry_price = current_price
                    self.current_signal = signal

                    self.position_id = self.risk_manager.add_position({
                        'symbol': 'BACKTEST',
                        'direction': 'short',
                        'entry': signal['entry'],
                        'stop': signal['stop'],
                        'target': signal['target'],
                        'size': size_fraction,
                        'risk_amount': position_info['risk_amount']
                    })['id']

                    print(f"   ✅ SHORT TRADE EXECUTED")
                    break


def test_debug_backtest():
    """Run debug backtest"""

    print("\n" + "="*80)
    print(" DEBUG BACKTEST")
    print("="*80)

    data_path = "data/storage/dataset_files/BTCUSD-1h-1year-test.csv"
    df = pd.read_csv(data_path)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    df.columns = [c.capitalize() for c in df.columns]

    print(f"\n📊 Data loaded: {len(df)} bars")
    print(f"📅 Range: {df.index[0]} to {df.index[-1]}")

    # Run backtest
    print("\n🚀 Running debug backtest...")
    bt = Backtest(df, ICTStrategyDebug, cash=1000000, commission=0.002)
    stats = bt.run()

    print("\n" + "="*80)
    print(" DEBUG SUMMARY")
    print("="*80)
    print(f"Bars processed: {stats._strategy.bars_processed}")
    print(f"Signals generated: {stats._strategy.signals_generated}")
    print(f"Signals valid: {stats._strategy.signals_valid}")
    print(f"Signals rejected: {stats._strategy.signals_rejected}")
    print(f"Trades attempted: {stats._strategy.trades_attempted}")
    print(f"Trades executed: {stats['# Trades']}")
    print("="*80)

    return stats


if __name__ == "__main__":
    test_debug_backtest()
