#!/usr/bin/env python3
"""
ICT Session Manager - Track trading sessions and session-based liquidity levels
Adapted for algo-fun project structure
"""

import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple


class ICTSessionManager:
    """
    Manage trading sessions (Asia, London, NY) and session-based liquidity levels

    ICT methodology emphasizes:
    - London/NY overlap (1pm-4pm UTC) as highest probability window
    - Session highs/lows as key liquidity levels
    - Avoiding low-liquidity Asian session for major trades
    """

    def __init__(self):
        # Session times in UTC
        self.session_times = {
            'asia': (20, 4),      # 8pm-4am UTC (Tokyo open to close)
            'london': (7, 16),    # 7am-4pm UTC (London open to NY close)
            'ny': (13, 21)        # 1pm-9pm UTC (NY open to close)
        }

        # Overlap periods (highest probability)
        self.overlap_times = {
            'london_ny': (13, 16)  # 1pm-4pm UTC (3-hour overlap)
        }

        self.key_levels = {}

    def get_current_session(self, timestamp: Optional[datetime] = None) -> str:
        """
        Get the active trading session for given time

        Args:
            timestamp: Optional datetime to check. If None, uses current time.

        Returns:
            Session name (london_ny_overlap, london, ny, asia, off_hours)
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        current_hour = timestamp.hour

        # Check overlaps first (highest priority)
        london_ny_start, london_ny_end = self.overlap_times['london_ny']
        if london_ny_start <= current_hour < london_ny_end:
            return 'london_ny_overlap'

        # Check individual sessions
        for session, (start, end) in self.session_times.items():
            if start > end:  # Session crosses midnight (Asia)
                if current_hour >= start or current_hour < end:
                    return session
            else:
                if start <= current_hour < end:
                    return session

        return 'off_hours'

    def is_high_probability_session(self, timestamp: Optional[datetime] = None) -> bool:
        """
        Check if given time is high probability for trading

        Args:
            timestamp: Optional datetime to check. If None, uses current time.

        Returns:
            True if high probability session, False otherwise
        """
        session = self.get_current_session(timestamp)
        return session in ['london_ny_overlap', 'london', 'ny']

    def mark_session_liquidity(self, df: pd.DataFrame) -> Dict:
        """
        Mark session highs and lows as key liquidity levels

        Args:
            df: DataFrame with OHLC data and datetime index

        Returns:
            Dict of key levels with prices and metadata
        """
        levels = {}

        for session, (start, end) in self.session_times.items():
            # Handle sessions that cross midnight
            if start > end:
                mask = (df.index.hour >= start) | (df.index.hour < end)
            else:
                mask = (df.index.hour >= start) & (df.index.hour < end)

            session_data = df[mask]
            if not session_data.empty:
                levels[f'{session}_high'] = {
                    'price': session_data['high'].max(),
                    'tapped': False,
                    'swept': False,
                    'strength': 'strong' if session in ['london', 'ny'] else 'medium',
                    'session': session
                }
                levels[f'{session}_low'] = {
                    'price': session_data['low'].min(),
                    'tapped': False,
                    'swept': False,
                    'strength': 'strong' if session in ['london', 'ny'] else 'medium',
                    'session': session
                }

        # Add previous day high/low (very important levels)
        yesterday = df[df.index.date == (datetime.now(timezone.utc).date() - timedelta(days=1))]
        if not yesterday.empty:
            levels['prev_day_high'] = {
                'price': yesterday['high'].max(),
                'tapped': False,
                'swept': False,
                'strength': 'strong',
                'session': 'daily'
            }
            levels['prev_day_low'] = {
                'price': yesterday['low'].min(),
                'tapped': False,
                'swept': False,
                'strength': 'strong',
                'session': 'daily'
            }

        # Add current week high/low
        current_week = df[df.index.isocalendar().week == datetime.now(timezone.utc).isocalendar().week]
        if not current_week.empty:
            levels['week_high'] = {
                'price': current_week['high'].max(),
                'tapped': False,
                'swept': False,
                'strength': 'strong',
                'session': 'weekly'
            }
            levels['week_low'] = {
                'price': current_week['low'].min(),
                'tapped': False,
                'swept': False,
                'strength': 'strong',
                'session': 'weekly'
            }

        self.key_levels = levels
        return levels

    def find_nearest_level(self, current_price: float,
                          max_distance_pct: float = 0.005) -> Tuple[Optional[float], Optional[str]]:
        """
        Find the nearest untapped key level to current price

        Args:
            current_price: Current market price
            max_distance_pct: Maximum distance as percentage (default 0.5%)

        Returns:
            Tuple of (level_price, level_name) or (None, None)
        """
        if not self.key_levels:
            return None, None

        min_distance = float('inf')
        nearest_level = None
        level_name = None

        for name, level_data in self.key_levels.items():
            if level_data['tapped']:
                continue

            distance = abs(current_price - level_data['price'])
            distance_pct = distance / current_price

            # Only consider levels within max_distance_pct
            if distance_pct <= max_distance_pct and distance < min_distance:
                min_distance = distance
                nearest_level = level_data['price']
                level_name = name

        return nearest_level, level_name

    def update_level_status(self, df: pd.DataFrame, lookback: int = 10):
        """
        Update tapped/swept status of key levels based on recent price action

        Args:
            df: Recent OHLC dataframe
            lookback: Number of candles to check
        """
        recent = df.tail(lookback)

        for level_name, level_data in self.key_levels.items():
            level_price = level_data['price']

            # Check if level was tapped (price touched it)
            if not level_data['tapped']:
                if 'high' in level_name:
                    if recent['high'].max() >= level_price * 0.999:
                        level_data['tapped'] = True
                else:  # low level
                    if recent['low'].min() <= level_price * 1.001:
                        level_data['tapped'] = True

            # Check if level was swept (wicked through without close beyond)
            if not level_data['swept']:
                for idx, candle in recent.iterrows():
                    if 'high' in level_name:
                        # Swept above
                        if (candle['high'] > level_price * 1.001 and
                            candle['close'] < level_price):
                            level_data['swept'] = True
                            break
                    else:
                        # Swept below
                        if (candle['low'] < level_price * 0.999 and
                            candle['close'] > level_price):
                            level_data['swept'] = True
                            break

    def get_session_bias(self, df: pd.DataFrame) -> Dict:
        """
        Get directional bias based on session structure

        Args:
            df: Recent OHLC dataframe

        Returns:
            Dict with bias information
        """
        if not self.key_levels:
            return {'bias': 'neutral', 'confidence': 'low', 'reason': 'No levels marked'}

        current_price = df.iloc[-1]['close']

        # Count swept levels above and below
        swept_highs = [l for n, l in self.key_levels.items()
                      if 'high' in n and l['swept'] and l['price'] > current_price]
        swept_lows = [l for n, l in self.key_levels.items()
                     if 'low' in n and l['swept'] and l['price'] < current_price]

        # ICT logic: Sweeps often lead to reversals
        if len(swept_highs) > len(swept_lows):
            bias = 'bearish'
            reason = f'{len(swept_highs)} highs swept - expect reversal down'
        elif len(swept_lows) > len(swept_highs):
            bias = 'bullish'
            reason = f'{len(swept_lows)} lows swept - expect reversal up'
        else:
            bias = 'neutral'
            reason = 'Equal sweeps both directions'

        # Confidence based on session
        session = self.get_current_session()
        if session == 'london_ny_overlap':
            confidence = 'high'
        elif session in ['london', 'ny']:
            confidence = 'medium'
        else:
            confidence = 'low'

        return {
            'bias': bias,
            'confidence': confidence,
            'reason': reason,
            'session': session,
            'swept_highs': len(swept_highs),
            'swept_lows': len(swept_lows)
        }

    def get_session_info(self, timestamp: Optional[datetime] = None) -> Dict:
        """
        Get comprehensive session information

        Args:
            timestamp: Optional datetime to check. If None, uses current time.

        Returns:
            Dict with session details
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        current_session = self.get_current_session(timestamp)
        is_high_prob = self.is_high_probability_session(timestamp)

        return {
            'current_session': current_session,
            'is_high_probability': is_high_prob,
            'current_time_utc': timestamp.isoformat(),
            'recommendation': self._get_session_recommendation(current_session),
            'key_levels_count': len(self.key_levels),
            'untapped_levels': len([l for l in self.key_levels.values() if not l['tapped']])
        }

    def _get_session_recommendation(self, session: str) -> str:
        """Get trading recommendation based on session"""
        recommendations = {
            'london_ny_overlap': 'OPTIMAL - Highest liquidity and probability',
            'london': 'GOOD - High liquidity, quality setups',
            'ny': 'GOOD - High liquidity, quality setups',
            'asia': 'CAUTION - Lower liquidity, wider stops recommended',
            'off_hours': 'AVOID - Very low liquidity, high risk'
        }
        return recommendations.get(session, 'NEUTRAL')

    def should_trade_now(self, timestamp: Optional[datetime] = None) -> Tuple[bool, str]:
        """
        Determine if given time is suitable for trading

        Args:
            timestamp: Optional datetime to check. If None, uses current time.

        Returns:
            Tuple of (should_trade: bool, reason: str)
        """
        session = self.get_current_session(timestamp)

        if session == 'london_ny_overlap':
            return True, 'Optimal trading window - London/NY overlap'
        elif session in ['london', 'ny']:
            return True, f'{session.capitalize()} session - good liquidity'
        elif session == 'asia':
            return False, 'Asian session - lower liquidity, avoid major trades'
        else:
            return False, 'Off hours - insufficient liquidity'


if __name__ == "__main__":
    # Example usage
    session_mgr = ICTSessionManager()
    print("ICT Session Manager initialized")
    print(f"\nCurrent session: {session_mgr.get_current_session()}")
    print(f"High probability? {session_mgr.is_high_probability_session()}")
    print(f"\nSession info: {session_mgr.get_session_info()}")

    should_trade, reason = session_mgr.should_trade_now()
    print(f"\nShould trade now? {should_trade}")
    print(f"Reason: {reason}")
