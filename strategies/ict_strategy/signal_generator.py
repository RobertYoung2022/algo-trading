#!/usr/bin/env python3
"""
ICT Signal Generator - Generate trading signals based on ICT methodology
Adapted for algo-fun project structure
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional

class ICTSignalGenerator:
    """Generate ICT trading signals with confirmations"""

    def __init__(self):
        self.confirmations = []
        self.signal_history = []

    def check_inverted_fvg(self, df: pd.DataFrame, direction: str) -> bool:
        """Check for Inverted Fair Value Gap confirmation"""
        if len(df) < 5:
            return False

        recent = df.tail(5)

        for i in range(2, len(recent)):
            current = recent.iloc[i]
            prev_prev = recent.iloc[i-2]

            if direction == 'bullish':
                # Bullish IFVG: gap fills and holds
                if prev_prev['high'] < current['low']:
                    # Check if gap was filled and held
                    if recent.iloc[-1]['low'] > prev_prev['high']:
                        return True
            else:
                # Bearish IFVG: gap fills and holds
                if prev_prev['low'] > current['high']:
                    # Check if gap was filled and held
                    if recent.iloc[-1]['high'] < prev_prev['low']:
                        return True

        return False

    def check_cisd(self, df: pd.DataFrame, direction: str) -> bool:
        """Check for Change in State of Delivery (engulfing pattern)"""
        if len(df) < 2:
            return False

        current = df.iloc[-1]
        previous = df.iloc[-2]

        # Calculate body sizes
        current_body = abs(current['close'] - current['open'])
        prev_body = abs(previous['close'] - previous['open'])

        if direction == 'bullish':
            # Bullish engulfing
            return (current['close'] > current['open'] and  # Current is green
                   previous['close'] < previous['open'] and  # Previous is red
                   current['open'] <= previous['close'] and  # Engulfs low
                   current['close'] >= previous['open'] and  # Engulfs high
                   current_body > prev_body * 1.2)  # Significant engulfing
        else:
            # Bearish engulfing
            return (current['close'] < current['open'] and  # Current is red
                   previous['close'] > previous['open'] and  # Previous is green
                   current['open'] >= previous['close'] and  # Engulfs high
                   current['close'] <= previous['open'] and  # Engulfs low
                   current_body > prev_body * 1.2)  # Significant engulfing

    def check_sfp(self, df: pd.DataFrame, direction: str, key_level: float) -> bool:
        """Check for Swing Failure Pattern (false breakout)"""
        if len(df) < 3:
            return False

        recent = df.tail(3)

        for candle_idx, candle in recent.iterrows():
            if direction == 'bullish':
                # Check for wick below level without close below
                if (candle['low'] < key_level * 0.998 and  # Wick below
                    candle['close'] > key_level and  # Close above
                    (candle['high'] - candle['low']) > (candle['close'] - candle['open']) * 2):  # Long wick
                    return True
            else:
                # Check for wick above level without close above
                if (candle['high'] > key_level * 1.002 and  # Wick above
                    candle['close'] < key_level and  # Close below
                    (candle['high'] - candle['low']) > (candle['open'] - candle['close']) * 2):  # Long wick
                    return True

        return False

    def check_market_structure_shift(self, df: pd.DataFrame, direction: str) -> bool:
        """Check for Market Structure Shift (MSS)"""
        if len(df) < 20:
            return False

        # Find recent swing points
        highs = df['high'].rolling(5).max()
        lows = df['low'].rolling(5).min()

        if direction == 'bullish':
            # Look for break of previous swing high
            prev_high = highs.iloc[-10:-5].max()
            current = df.iloc[-1]['close']
            return current > prev_high
        else:
            # Look for break of previous swing low
            prev_low = lows.iloc[-10:-5].min()
            current = df.iloc[-1]['close']
            return current < prev_low

    def get_confirmations(self, df: pd.DataFrame, direction: str,
                         key_level: float) -> List[str]:
        """Check all confirmation patterns"""
        confirmations = []

        # Direction for confirmations
        conf_direction = 'bullish' if direction == 'long' else 'bearish'

        if self.check_inverted_fvg(df, conf_direction):
            confirmations.append('IFVG')

        if self.check_cisd(df, conf_direction):
            confirmations.append('CISD')

        if self.check_sfp(df, conf_direction, key_level):
            confirmations.append('SFP')

        if self.check_market_structure_shift(df, conf_direction):
            confirmations.append('MSS')

        return confirmations

    def calculate_entry_levels(self, df: pd.DataFrame, direction: str,
                             key_level: float) -> Dict:
        """Calculate precise entry, stop, and target levels"""
        current_price = df.iloc[-1]['close']

        # Calculate ATR for dynamic stop placement
        atr = self.calculate_atr(df, period=14)

        if direction == 'long':
            # Entry at current price or retest of level
            entry = min(current_price, key_level * 1.001)

            # Stop below recent swing low with ATR buffer
            recent_low = df.tail(10)['low'].min()
            stop = recent_low - (atr * 2)  # 2x ATR for crypto volatility

            # Target at 1.5R minimum
            risk = entry - stop
            target = entry + (risk * 1.5)

        else:  # short
            # Entry at current price or retest of level
            entry = max(current_price, key_level * 0.999)

            # Stop above recent swing high with ATR buffer
            recent_high = df.tail(10)['high'].max()
            stop = recent_high + (atr * 2)  # 2x ATR for crypto volatility

            # Target at 1.5R minimum
            risk = stop - entry
            target = entry - (risk * 1.5)

        return {
            'entry': entry,
            'stop': stop,
            'target': target,
            'risk': abs(entry - stop),
            'reward': abs(target - entry),
            'rr_ratio': abs(target - entry) / abs(entry - stop)
        }

    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        high = df['high']
        low = df['low']
        close = df['close']

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]

        return atr

    def generate_signal(self, df_higher: pd.DataFrame, df_lower: pd.DataFrame,
                       symbol: str, key_level: float = None,
                       direction: str = None) -> Dict:
        """
        Generate complete trading signal

        Args:
            df_higher: Higher timeframe dataframe (e.g., 1H, 4H)
            df_lower: Lower timeframe dataframe (e.g., 15m, 1m)
            symbol: Trading pair symbol
            key_level: Key price level for entry (optional)
            direction: 'long' or 'short' (optional, will be determined if None)

        Returns:
            Dict containing signal information
        """

        signal = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'symbol': symbol,
            'valid': False,
            'direction': direction,
            'confirmations': [],
            'entry': None,
            'stop': None,
            'target': None,
            'rr_ratio': None,
            'confidence': None,
            'notes': []
        }

        # If no key level provided, use recent high/low
        if key_level is None:
            key_level = df_higher.iloc[-1]['close']
            signal['notes'].append('Using current price as key level')

        # If no direction provided, determine from market structure
        if direction is None:
            # Simple trend detection
            sma20 = df_higher['close'].rolling(20).mean().iloc[-1]
            sma50 = df_higher['close'].rolling(50).mean().iloc[-1]
            current = df_higher.iloc[-1]['close']

            if current > sma20 > sma50:
                direction = 'long'
                signal['notes'].append('Bullish trend structure')
            elif current < sma20 < sma50:
                direction = 'short'
                signal['notes'].append('Bearish trend structure')
            else:
                signal['notes'].append('Unclear trend - no signal')
                return signal

        signal['direction'] = direction

        # Get confirmations
        confirmations = self.get_confirmations(df_lower, direction, key_level)
        signal['confirmations'] = confirmations

        # Require at least one confirmation
        if not confirmations:
            signal['notes'].append('No confirmations present')
            return signal

        # Calculate entry levels
        levels = self.calculate_entry_levels(df_lower, direction, key_level)
        signal.update(levels)

        # Determine confidence based on confirmations
        if len(confirmations) >= 3:
            signal['confidence'] = 'high'
        elif len(confirmations) == 2:
            signal['confidence'] = 'medium'
        else:
            signal['confidence'] = 'low'

        # Check R:R ratio
        if signal['rr_ratio'] >= 1.5:
            signal['valid'] = True
            signal['notes'].append(f'Valid setup with {len(confirmations)} confirmations')
        else:
            signal['notes'].append(f'Poor R:R ratio: {signal["rr_ratio"]:.2f}')

        # Store in history
        self.signal_history.append(signal)

        return signal

    def filter_high_probability(self, signals: List[Dict]) -> List[Dict]:
        """Filter for high probability signals only"""
        return [s for s in signals
                if s['valid'] and
                s['confidence'] in ['high', 'medium'] and
                len(s['confirmations']) >= 2]

    def get_signal_statistics(self) -> Dict:
        """Get statistics on generated signals"""
        if not self.signal_history:
            return {'total': 0}

        valid_signals = [s for s in self.signal_history if s['valid']]

        return {
            'total': len(self.signal_history),
            'valid': len(valid_signals),
            'high_confidence': len([s for s in valid_signals if s['confidence'] == 'high']),
            'medium_confidence': len([s for s in valid_signals if s['confidence'] == 'medium']),
            'low_confidence': len([s for s in valid_signals if s['confidence'] == 'low']),
            'avg_rr_ratio': np.mean([s['rr_ratio'] for s in valid_signals]) if valid_signals else 0,
            'confirmations_used': list(set([c for s in valid_signals for c in s['confirmations']]))
        }


if __name__ == "__main__":
    # Example usage
    generator = ICTSignalGenerator()
    print("ICT Signal Generator initialized")
    print("Available methods:")
    print("- generate_signal(df_higher, df_lower, symbol, key_level, direction)")
    print("- get_confirmations(df, direction, key_level)")
    print("- filter_high_probability(signals)")
    print("- get_signal_statistics()")
