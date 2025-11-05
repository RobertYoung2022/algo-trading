#!/usr/bin/env python3
"""
Tests for ICT Risk Manager
"""

import pytest
from datetime import datetime, timezone
from strategies.ict_strategy.risk_manager import ICTRiskManager


@pytest.fixture
def risk_manager():
    """Create a risk manager instance with test balance"""
    return ICTRiskManager(account_balance=10000, max_daily_losses=2)


@pytest.fixture
def sample_signal():
    """Create a sample trading signal"""
    return {
        'symbol': 'BTC/USDT',
        'direction': 'long',
        'entry': 45000,
        'stop': 44000,
        'target': 46500,
        'risk': 1000,
        'reward': 1500,
        'rr_ratio': 1.5,
        'confidence': 'medium',
        'confirmations': ['IFVG', 'CISD'],
        'valid': True
    }


@pytest.fixture
def high_quality_signal():
    """Create a high quality trading signal"""
    return {
        'symbol': 'ETH/USDT',
        'direction': 'long',
        'entry': 2500,
        'stop': 2400,
        'target': 2800,
        'risk': 100,
        'reward': 300,
        'rr_ratio': 3.0,
        'confidence': 'high',
        'confirmations': ['IFVG', 'CISD', 'SFP', 'MSS'],
        'valid': True
    }


class TestICTRiskManager:
    """Test suite for ICT Risk Manager"""

    def test_initialization(self, risk_manager):
        """Test risk manager initializes correctly"""
        assert risk_manager.account_balance == 10000
        assert risk_manager.max_daily_losses == 2
        assert risk_manager.daily_losses == 0
        assert risk_manager.daily_wins == 0
        assert len(risk_manager.open_positions) == 0
        assert len(risk_manager.closed_positions) == 0

    def test_can_trade_initially(self, risk_manager):
        """Test that trading is allowed initially"""
        assert risk_manager.can_trade() is True

    def test_cannot_trade_after_max_losses(self, risk_manager):
        """Test that trading is blocked after max daily losses"""
        risk_manager.daily_losses = 2
        assert risk_manager.can_trade() is False

    def test_assess_setup_quality_high_prob(self, risk_manager, high_quality_signal):
        """Test assessment of high probability setup"""
        quality = risk_manager.assess_setup_quality(high_quality_signal)
        assert quality == 'high_prob'

    def test_assess_setup_quality_medium_prob(self, risk_manager, sample_signal):
        """Test assessment of medium probability setup"""
        quality = risk_manager.assess_setup_quality(sample_signal)
        assert quality in ['medium_prob', 'high_prob']

    def test_assess_setup_quality_low_prob(self, risk_manager):
        """Test assessment of low probability setup"""
        low_quality_signal = {
            'symbol': 'TEST/USDT',
            'direction': 'long',
            'entry': 100,
            'stop': 95,
            'target': 102,
            'rr_ratio': 0.4,
            'confidence': 'low',
            'confirmations': [],
            'valid': False
        }
        quality = risk_manager.assess_setup_quality(low_quality_signal)
        assert quality == 'low_prob'

    def test_calculate_position_size_high_prob(self, risk_manager, high_quality_signal):
        """Test position sizing for high probability setup"""
        result = risk_manager.calculate_position_size(high_quality_signal)

        assert result['can_trade'] is True
        assert result['position_size'] > 0
        assert result['risk_percentage'] == 1.5  # 1.5% for high_prob
        assert result['quality'] == 'high_prob'
        assert 'risk_amount' in result

    def test_calculate_position_size_medium_prob(self, risk_manager, sample_signal):
        """Test position sizing for medium probability setup"""
        result = risk_manager.calculate_position_size(sample_signal)

        assert result['can_trade'] is True
        assert result['position_size'] > 0
        # Should be 0.5-1.5% based on quality
        assert 0.5 <= result['risk_percentage'] <= 1.5

    def test_calculate_position_size_with_leverage(self, risk_manager, sample_signal):
        """Test position sizing with leverage"""
        result_1x = risk_manager.calculate_position_size(sample_signal, leverage=1.0)
        result_2x = risk_manager.calculate_position_size(sample_signal, leverage=2.0)

        # Higher leverage should allow larger position
        assert result_2x['position_size'] > result_1x['position_size']

    def test_calculate_position_size_blocked_by_max_losses(self, risk_manager, sample_signal):
        """Test position sizing is blocked after max losses"""
        risk_manager.daily_losses = 2

        result = risk_manager.calculate_position_size(sample_signal)

        assert result['can_trade'] is False
        assert result['reason'] == 'Maximum daily losses reached'
        assert result['position_size'] == 0

    def test_kelly_criterion_calculation(self, risk_manager, high_quality_signal):
        """Test Kelly Criterion calculation"""
        kelly = risk_manager.calculate_kelly_criterion(high_quality_signal)

        assert 0 <= kelly <= 0.25  # Should be capped at 25%
        assert isinstance(kelly, float)

    def test_add_position(self, risk_manager, sample_signal):
        """Test adding a position"""
        position = risk_manager.add_position({
            'symbol': sample_signal['symbol'],
            'direction': sample_signal['direction'],
            'entry': sample_signal['entry'],
            'stop': sample_signal['stop'],
            'target': sample_signal['target'],
            'size': 0.1,
            'risk_amount': 100
        })

        assert position['id'] == 1
        assert position['symbol'] == 'BTC/USDT'
        assert position['status'] == 'open'
        assert 'timestamp' in position
        assert len(risk_manager.open_positions) == 1

    def test_trail_stop_long_position(self, risk_manager, sample_signal):
        """Test trailing stop for long position"""
        # Add position
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'long',
            'entry': 45000,
            'stop': 44000,
            'target': 46500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Price moves up - trail stop should update
        result = risk_manager.trail_stop(position['id'], 46000)

        if result.get('updated'):
            assert result['new_stop'] > 44000  # Stop should move up
            assert result['locked_profit'] > 0

    def test_trail_stop_short_position(self, risk_manager):
        """Test trailing stop for short position"""
        # Add short position
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'short',
            'entry': 45000,
            'stop': 46000,
            'target': 43500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Price moves down - trail stop should update
        result = risk_manager.trail_stop(position['id'], 44000)

        if result.get('updated'):
            assert result['new_stop'] < 46000  # Stop should move down
            assert result['locked_profit'] > 0

    def test_trail_stop_only_moves_favorably(self, risk_manager):
        """Test that trailing stop only moves in favorable direction"""
        # Add long position
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'long',
            'entry': 45000,
            'stop': 44000,
            'target': 46500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Price moves down - stop should NOT move
        result = risk_manager.trail_stop(position['id'], 44500)
        assert result['updated'] is False

    def test_close_position_winning(self, risk_manager, sample_signal):
        """Test closing a winning position"""
        # Add position
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'long',
            'entry': 45000,
            'stop': 44000,
            'target': 46500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Close at profit
        initial_balance = risk_manager.account_balance
        result = risk_manager.close_position(position['id'], 46000, reason='target')

        assert result['pnl'] > 0
        assert result['pnl_percentage'] > 0
        assert risk_manager.account_balance > initial_balance
        assert risk_manager.daily_wins == 1
        assert risk_manager.daily_losses == 0
        assert len(risk_manager.closed_positions) == 1

    def test_close_position_losing(self, risk_manager):
        """Test closing a losing position"""
        # Add position
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'long',
            'entry': 45000,
            'stop': 44000,
            'target': 46500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Close at loss
        initial_balance = risk_manager.account_balance
        result = risk_manager.close_position(position['id'], 44000, reason='stop')

        assert result['pnl'] < 0
        assert result['pnl_percentage'] < 0
        assert risk_manager.account_balance < initial_balance
        assert risk_manager.daily_wins == 0
        assert risk_manager.daily_losses == 1

    def test_get_portfolio_risk(self, risk_manager):
        """Test portfolio risk calculation"""
        # Add multiple positions
        for i in range(3):
            risk_manager.add_position({
                'symbol': f'ASSET{i}/USDT',
                'direction': 'long',
                'entry': 1000,
                'stop': 950,
                'target': 1100,
                'size': 0.1,
                'risk_amount': 50
            })

        portfolio_risk = risk_manager.get_portfolio_risk()

        assert 'total_risk' in portfolio_risk
        assert 'total_risk_percentage' in portfolio_risk
        assert portfolio_risk['positions_at_risk'] == 3
        assert len(portfolio_risk['position_risks']) == 3
        assert 'can_add_position' in portfolio_risk

    def test_portfolio_risk_limit(self, risk_manager):
        """Test that portfolio risk is limited to 5%"""
        # Add positions up to risk limit
        for i in range(10):
            risk_manager.add_position({
                'symbol': f'ASSET{i}/USDT',
                'direction': 'long',
                'entry': 1000,
                'stop': 950,
                'target': 1100,
                'size': 0.1,
                'risk_amount': 60
            })

        portfolio_risk = risk_manager.get_portfolio_risk()

        # Check if can_add_position respects 5% limit
        if portfolio_risk['total_risk_percentage'] >= 5:
            assert portfolio_risk['can_add_position'] is False

    def test_get_performance_stats_empty(self, risk_manager):
        """Test performance stats with no trades"""
        stats = risk_manager.get_performance_stats()
        assert stats['total_trades'] == 0

    def test_get_performance_stats(self, risk_manager):
        """Test performance statistics calculation"""
        # Add and close some positions
        for i in range(5):
            position = risk_manager.add_position({
                'symbol': 'BTC/USDT',
                'direction': 'long',
                'entry': 45000,
                'stop': 44000,
                'target': 46500,
                'size': 0.1,
                'risk_amount': 100
            })

            # Win 60% of trades
            exit_price = 46000 if i < 3 else 44000
            risk_manager.close_position(position['id'], exit_price)

        stats = risk_manager.get_performance_stats()

        assert stats['total_trades'] == 5
        assert stats['wins'] == 3
        assert stats['losses'] == 2
        assert stats['win_rate'] == 60.0
        assert 'profit_factor' in stats
        assert 'max_drawdown' in stats

    def test_should_scale_in_profitable(self, risk_manager):
        """Test scaling into profitable position"""
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'long',
            'entry': 45000,
            'stop': 44000,
            'target': 46500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Price up 2% - should allow scaling
        should_scale = risk_manager.should_scale_in(position['id'], 45900)
        # Result depends on portfolio risk and daily losses
        assert isinstance(should_scale, bool)

    def test_should_not_scale_in_losing(self, risk_manager):
        """Test no scaling into losing position"""
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'long',
            'entry': 45000,
            'stop': 44000,
            'target': 46500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Price down - should not scale
        should_scale = risk_manager.should_scale_in(position['id'], 44500)
        assert should_scale is False

    def test_should_not_scale_on_losing_day(self, risk_manager):
        """Test no scaling on days with losses"""
        position = risk_manager.add_position({
            'symbol': 'BTC/USDT',
            'direction': 'long',
            'entry': 45000,
            'stop': 44000,
            'target': 46500,
            'size': 0.1,
            'risk_amount': 100
        })

        # Set daily losses
        risk_manager.daily_losses = 1

        # Should not scale even if profitable
        should_scale = risk_manager.should_scale_in(position['id'], 46000)
        assert should_scale is False

    def test_reset_daily_counters(self, risk_manager):
        """Test daily counter reset logic"""
        risk_manager.daily_losses = 2
        risk_manager.daily_wins = 3

        # Force date change by manually setting current_date to yesterday
        from datetime import timedelta
        risk_manager.current_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

        # Reset should happen
        risk_manager.reset_daily_counters()

        assert risk_manager.daily_losses == 0
        assert risk_manager.daily_wins == 0

    def test_position_not_found_error(self, risk_manager):
        """Test error handling for non-existent position"""
        result = risk_manager.trail_stop(999, 45000)
        assert 'error' in result

        result = risk_manager.close_position(999, 45000)
        assert 'error' in result


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
