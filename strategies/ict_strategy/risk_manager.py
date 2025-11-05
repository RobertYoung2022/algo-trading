#!/usr/bin/env python3
"""
ICT Risk Manager - Position sizing and risk management
Adapted for algo-fun project structure
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional


class ICTRiskManager:
    """Manage risk according to ICT principles"""

    def __init__(self, account_balance: float, max_daily_losses: int = 2):
        self.account_balance = account_balance
        self.max_daily_losses = max_daily_losses
        self.daily_losses = 0
        self.daily_wins = 0
        self.open_positions = []
        self.closed_positions = []
        self.current_date = datetime.now(timezone.utc).date()

    def reset_daily_counters(self):
        """Reset daily loss/win counters"""
        current = datetime.now(timezone.utc).date()
        if current != self.current_date:
            self.daily_losses = 0
            self.daily_wins = 0
            self.current_date = current

    def can_trade(self) -> bool:
        """Check if allowed to take new trades"""
        self.reset_daily_counters()
        return self.daily_losses < self.max_daily_losses

    def assess_setup_quality(self, signal: Dict) -> str:
        """Assess the quality of a trading setup"""
        quality_score = 0

        # Confirmations scoring
        confirmations = signal.get('confirmations', [])
        if 'IFVG' in confirmations:
            quality_score += 2
        if 'CISD' in confirmations:
            quality_score += 2
        if 'SFP' in confirmations:
            quality_score += 1
        if 'MSS' in confirmations:
            quality_score += 1

        # R:R ratio scoring
        rr_ratio = signal.get('rr_ratio', 0)
        if rr_ratio >= 3:
            quality_score += 2
        elif rr_ratio >= 2:
            quality_score += 1

        # Confidence scoring
        confidence = signal.get('confidence', 'low')
        if confidence == 'high':
            quality_score += 2
        elif confidence == 'medium':
            quality_score += 1

        # Determine quality level
        if quality_score >= 8:
            return 'high_prob'
        elif quality_score >= 5:
            return 'medium_prob'
        else:
            return 'low_prob'

    def calculate_position_size(self, signal: Dict,
                               leverage: float = 1.0,
                               use_kelly: bool = False) -> Dict:
        """
        Calculate optimal position size

        Args:
            signal: Trading signal dict from signal_generator
            leverage: Leverage to apply (default 1.0 for spot)
            use_kelly: Whether to use Kelly Criterion adjustment

        Returns:
            Dict with position sizing information
        """

        if not self.can_trade():
            return {
                'can_trade': False,
                'reason': 'Maximum daily losses reached',
                'position_size': 0
            }

        # Base risk percentages
        risk_percentages = {
            'high_prob': 0.015,    # 1.5% risk
            'medium_prob': 0.01,   # 1% risk
            'low_prob': 0.005      # 0.5% risk
        }

        # Assess setup quality
        quality = self.assess_setup_quality(signal)
        base_risk = risk_percentages[quality]

        # Kelly Criterion adjustment if enabled
        if use_kelly:
            kelly_fraction = self.calculate_kelly_criterion(signal)
            base_risk = min(base_risk * kelly_fraction, base_risk * 2)  # Cap at 2x

        # Calculate position size
        risk_amount = self.account_balance * base_risk
        stop_distance = abs(signal['entry'] - signal['stop'])
        stop_percentage = stop_distance / signal['entry']

        # Position size in base currency
        position_size = risk_amount / stop_distance

        # Apply leverage
        effective_position_size = position_size * leverage

        # Ensure we don't exceed account balance with leverage
        max_position = self.account_balance * leverage * 0.95  # 95% max utilization
        effective_position_size = min(effective_position_size, max_position)

        return {
            'can_trade': True,
            'position_size': round(effective_position_size, 2),
            'risk_amount': round(risk_amount, 2),
            'risk_percentage': round(base_risk * 100, 2),
            'quality': quality,
            'leverage': leverage,
            'stop_percentage': round(stop_percentage * 100, 2),
            'position_value': round(position_size * signal['entry'], 2)
        }

    def calculate_kelly_criterion(self, signal: Dict) -> float:
        """Calculate Kelly Criterion for position sizing"""
        # Estimate win probability based on confidence
        win_prob = {
            'high': 0.7,
            'medium': 0.6,
            'low': 0.5
        }.get(signal.get('confidence', 'low'), 0.5)

        # Get R:R ratio
        rr_ratio = signal.get('rr_ratio', 1.5)

        # Kelly formula: f = (p * b - q) / b
        # where p = win probability, q = loss probability, b = odds
        q = 1 - win_prob
        kelly = (win_prob * rr_ratio - q) / rr_ratio

        # Cap Kelly fraction at 25% (conservative)
        return min(max(kelly, 0), 0.25)

    def add_position(self, position: Dict) -> Dict:
        """Add a new position to track"""
        position_entry = {
            'id': len(self.open_positions) + len(self.closed_positions) + 1,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'symbol': position['symbol'],
            'direction': position['direction'],
            'entry': position['entry'],
            'stop': position['stop'],
            'target': position['target'],
            'size': position['size'],
            'risk_amount': position.get('risk_amount', 0),
            'status': 'open',
            'trailing_stop': position['stop'],
            'highest_price': position['entry'] if position['direction'] == 'long' else None,
            'lowest_price': position['entry'] if position['direction'] == 'short' else None
        }

        self.open_positions.append(position_entry)
        return position_entry

    def trail_stop(self, position_id: int, current_price: float,
                  trail_factor: float = 0.5) -> Dict:
        """Trail stop loss for winning positions"""

        position = None
        for pos in self.open_positions:
            if pos['id'] == position_id:
                position = pos
                break

        if not position:
            return {'error': 'Position not found'}

        if position['direction'] == 'long':
            # Update highest price
            if current_price > position['highest_price']:
                position['highest_price'] = current_price

                # Calculate new trailing stop
                profit = current_price - position['entry']
                new_stop = position['entry'] + (profit * trail_factor)

                # Only move stop up, never down
                if new_stop > position['trailing_stop']:
                    position['trailing_stop'] = new_stop
                    return {
                        'updated': True,
                        'new_stop': new_stop,
                        'locked_profit': new_stop - position['entry']
                    }

        else:  # short
            # Update lowest price
            if current_price < position['lowest_price']:
                position['lowest_price'] = current_price

                # Calculate new trailing stop
                profit = position['entry'] - current_price
                new_stop = position['entry'] - (profit * trail_factor)

                # Only move stop down, never up
                if new_stop < position['trailing_stop']:
                    position['trailing_stop'] = new_stop
                    return {
                        'updated': True,
                        'new_stop': new_stop,
                        'locked_profit': position['entry'] - new_stop
                    }

        return {'updated': False}

    def close_position(self, position_id: int, exit_price: float,
                      reason: str = 'manual') -> Dict:
        """Close a position and update statistics"""

        position = None
        for i, pos in enumerate(self.open_positions):
            if pos['id'] == position_id:
                position = pos
                self.open_positions.pop(i)
                break

        if not position:
            return {'error': 'Position not found'}

        # Calculate P&L
        if position['direction'] == 'long':
            pnl = (exit_price - position['entry']) * position['size']
            pnl_percentage = ((exit_price - position['entry']) / position['entry']) * 100
        else:
            pnl = (position['entry'] - exit_price) * position['size']
            pnl_percentage = ((position['entry'] - exit_price) / position['entry']) * 100

        # Update position record
        position['exit_price'] = exit_price
        position['exit_time'] = datetime.now(timezone.utc).isoformat()
        position['pnl'] = pnl
        position['pnl_percentage'] = pnl_percentage
        position['exit_reason'] = reason
        position['status'] = 'closed'

        # Update daily counters
        if pnl > 0:
            self.daily_wins += 1
        else:
            self.daily_losses += 1

        # Add to closed positions
        self.closed_positions.append(position)

        # Update account balance
        self.account_balance += pnl

        return {
            'position_id': position_id,
            'pnl': round(pnl, 2),
            'pnl_percentage': round(pnl_percentage, 2),
            'new_balance': round(self.account_balance, 2),
            'exit_reason': reason,
            'daily_stats': {
                'wins': self.daily_wins,
                'losses': self.daily_losses
            }
        }

    def get_portfolio_risk(self) -> Dict:
        """Calculate current portfolio risk exposure"""

        total_risk = 0
        position_risks = []

        for position in self.open_positions:
            # Calculate risk for each position
            if position['direction'] == 'long':
                risk = (position['entry'] - position['trailing_stop']) * position['size']
            else:
                risk = (position['trailing_stop'] - position['entry']) * position['size']

            position_risks.append({
                'symbol': position['symbol'],
                'risk': risk,
                'risk_percentage': (risk / self.account_balance) * 100
            })

            total_risk += risk

        return {
            'total_risk': round(total_risk, 2),
            'total_risk_percentage': round((total_risk / self.account_balance) * 100, 2),
            'positions_at_risk': len(self.open_positions),
            'position_risks': position_risks,
            'can_add_position': total_risk < (self.account_balance * 0.05)  # Max 5% total risk
        }

    def get_performance_stats(self) -> Dict:
        """Get trading performance statistics"""

        if not self.closed_positions:
            return {'total_trades': 0}

        wins = [p for p in self.closed_positions if p['pnl'] > 0]
        losses = [p for p in self.closed_positions if p['pnl'] <= 0]

        total_profit = sum([p['pnl'] for p in wins])
        total_loss = sum([p['pnl'] for p in losses])

        win_rate = len(wins) / len(self.closed_positions) * 100 if self.closed_positions else 0

        avg_win = total_profit / len(wins) if wins else 0
        avg_loss = total_loss / len(losses) if losses else 0

        # Profit factor
        profit_factor = abs(total_profit / total_loss) if total_loss != 0 else float('inf')

        # Max drawdown calculation
        balance_history = [self.account_balance]
        running_balance = self.account_balance

        for position in sorted(self.closed_positions, key=lambda x: x['exit_time']):
            running_balance -= position['pnl']
            balance_history.append(running_balance)

        balance_history.reverse()
        peak = balance_history[0]
        max_drawdown = 0

        for balance in balance_history:
            if balance > peak:
                peak = balance
            drawdown = (peak - balance) / peak * 100
            max_drawdown = max(max_drawdown, drawdown)

        return {
            'total_trades': len(self.closed_positions),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': round(win_rate, 2),
            'total_pnl': round(total_profit + total_loss, 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'profit_factor': round(profit_factor, 2),
            'max_drawdown': round(max_drawdown, 2),
            'current_balance': round(self.account_balance, 2),
            'roi': round(((self.account_balance - 10000) / 10000) * 100, 2)  # Assuming 10k start
        }

    def should_scale_in(self, position_id: int, current_price: float) -> bool:
        """Determine if should scale into winning position"""

        position = None
        for pos in self.open_positions:
            if pos['id'] == position_id:
                position = pos
                break

        if not position:
            return False

        # Only scale into winners
        if position['direction'] == 'long':
            profit_percentage = ((current_price - position['entry']) / position['entry']) * 100
        else:
            profit_percentage = ((position['entry'] - current_price) / position['entry']) * 100

        # Scale in if:
        # 1. Position is profitable by at least 1%
        # 2. Not exceeding max portfolio risk
        # 3. Strong momentum confirmed

        portfolio_risk = self.get_portfolio_risk()

        return (profit_percentage > 1 and
                portfolio_risk['can_add_position'] and
                self.daily_losses == 0)  # Only scale on winning days


if __name__ == "__main__":
    # Example usage
    risk_mgr = ICTRiskManager(account_balance=10000, max_daily_losses=2)
    print("ICT Risk Manager initialized")
    print(f"Account Balance: ${risk_mgr.account_balance}")
    print(f"Max Daily Losses: {risk_mgr.max_daily_losses}")
    print("\nAvailable methods:")
    print("- calculate_position_size(signal, leverage)")
    print("- add_position(position)")
    print("- trail_stop(position_id, current_price)")
    print("- close_position(position_id, exit_price)")
    print("- get_portfolio_risk()")
    print("- get_performance_stats()")
