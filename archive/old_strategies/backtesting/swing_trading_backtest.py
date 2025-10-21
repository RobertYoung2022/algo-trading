"""
ETH Swing Trading Strategy Backtesting System
============================================

Based on expert traders' recommendations for ETH swing trading:
- Fibonacci retracements for pullback entries
- Bollinger Bands for volatility and breakouts
- Volume confirmation for accumulation zones
- RSI for oversold signals
- Support/resistance levels for entries and exits

Strategy focuses on:
- Buying dips to support levels (pullbacks)
- Entering breakouts on high volume
- 3-10 day holds for quick profits
- Risk management with trailing stops

Author: AI Assistant
Date: 2025
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Try to import technical analysis libraries
try:
    from ta.momentum import RSIIndicator
    from ta.volatility import BollingerBands
    from ta.trend import MACD
    TA_AVAILABLE = True
except ImportError:
    print("⚠️ ta library not available. Installing...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'ta'])
    from ta.momentum import RSIIndicator
    from ta.volatility import BollingerBands
    from ta.trend import MACD
    TA_AVAILABLE = True

class ETHSwingTradingBacktester:
    """
    ETH Swing Trading Strategy Backtester based on expert traders' recommendations
    """
    
    def __init__(self, initial_capital=10000, risk_per_trade=0.015):
        """
        Initialize the swing trading backtester
        
        Args:
            initial_capital (float): Starting capital
            risk_per_trade (float): Risk per trade as percentage (0.015 = 1.5%)
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_per_trade = risk_per_trade
        self.positions = []
        self.trades = []
        self.equity_curve = []
        
        # Strategy parameters
        self.rsi_period = 14
        self.rsi_oversold = 40  # Buy on oversold
        self.rsi_overbought = 70  # Sell on overbought
        
        # Bollinger Bands
        self.bb_period = 20
        self.bb_std = 2
        
        # Fibonacci levels
        self.fib_levels = [0.236, 0.382, 0.5, 0.618, 0.786]
        
        # Risk management
        self.stop_loss_pct = 0.03  # 3% stop loss (tighter for swing trading)
        self.take_profit_ratio = 3.0  # 3:1 risk/reward (expert recommendation)
        self.trailing_stop_pct = 0.02  # 2% trailing stop
        
        # Volume analysis
        self.volume_period = 20  # For volume average calculation
        
    def load_data(self, file_path):
        """
        Load historical ETH data from CSV file
        """
        try:
            self.data = pd.read_csv(file_path)
            self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
            self.data = self.data.sort_values('timestamp').reset_index(drop=True)
            
            print(f"✅ Loaded {len(self.data)} candles")
            print(f"📅 Date range: {self.data['timestamp'].min()} to {self.data['timestamp'].max()}")
            print(f"💰 Price range: ${self.data['close'].min():.2f} - ${self.data['close'].max():.2f}")
            
            return True
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def calculate_indicators(self):
        """
        Calculate all technical indicators needed for swing trading
        """
        print("📊 Calculating swing trading indicators...")
        
        # RSI
        rsi = RSIIndicator(self.data['close'], window=self.rsi_period)
        self.data['rsi'] = rsi.rsi()
        
        # Bollinger Bands
        bb = BollingerBands(self.data['close'], window=self.bb_period, window_dev=self.bb_std)
        self.data['bb_upper'] = bb.bollinger_hband()
        self.data['bb_middle'] = bb.bollinger_mavg()
        self.data['bb_lower'] = bb.bollinger_lband()
        self.data['bb_width'] = (self.data['bb_upper'] - self.data['bb_lower']) / self.data['bb_middle']
        
        # Volume analysis
        self.data['volume_avg'] = self.data['volume'].rolling(window=self.volume_period).mean()
        self.data['volume_ratio'] = self.data['volume'] / self.data['volume_avg']
        
        # Price momentum
        self.data['price_change'] = self.data['close'].pct_change()
        self.data['price_change_5d'] = self.data['close'].pct_change(periods=5)
        
        # Support and Resistance levels (simplified)
        self.data['support'] = self.data['low'].rolling(window=20).min()
        self.data['resistance'] = self.data['high'].rolling(window=20).max()
        
        # Fibonacci levels (simplified - using recent swing high/low)
        self.data['swing_high'] = self.data['high'].rolling(window=50).max()
        self.data['swing_low'] = self.data['low'].rolling(window=50).min()
        self.data['fib_range'] = self.data['swing_high'] - self.data['swing_low']
        
        # Calculate Fibonacci retracement levels
        for level in self.fib_levels:
            self.data[f'fib_{level}'] = self.data['swing_high'] - (self.data['fib_range'] * level)
        
        print("✅ Swing trading indicators calculated successfully")
    
    def identify_swing_setups(self, i):
        """
        Identify swing trading setups based on expert strategies
        """
        current_price = self.data['close'].iloc[i]
        rsi = self.data['rsi'].iloc[i]
        bb_upper = self.data['bb_upper'].iloc[i]
        bb_lower = self.data['bb_lower'].iloc[i]
        bb_width = self.data['bb_width'].iloc[i]
        volume_ratio = self.data['volume_ratio'].iloc[i]
        support = self.data['support'].iloc[i]
        resistance = self.data['resistance'].iloc[i]
        
        setups = []
        
        # Setup 1: Pullback to Support (Fibonacci retracement)
        for level in self.fib_levels:
            fib_level = self.data[f'fib_{level}'].iloc[i]
            if abs(current_price - fib_level) / current_price < 0.02:  # Within 2% of fib level
                if rsi < self.rsi_oversold and volume_ratio > 1.2:  # Oversold + high volume
                    setups.append({
                        'type': 'PULLBACK_FIB',
                        'level': level,
                        'price': fib_level,
                        'strength': 'HIGH' if volume_ratio > 1.5 else 'MEDIUM'
                    })
        
        # Setup 2: Pullback to Horizontal Support
        if abs(current_price - support) / current_price < 0.02:  # Within 2% of support
            if rsi < self.rsi_oversold and volume_ratio > 1.1:
                setups.append({
                    'type': 'PULLBACK_SUPPORT',
                    'level': 'support',
                    'price': support,
                    'strength': 'HIGH' if volume_ratio > 1.3 else 'MEDIUM'
                })
        
        # Setup 3: Bollinger Band Squeeze Breakout
        if bb_width < 0.1:  # Tight bands (squeeze)
            if current_price > bb_upper and volume_ratio > 1.5:  # Breakout with volume
                setups.append({
                    'type': 'BB_BREAKOUT',
                    'level': 'upper',
                    'price': bb_upper,
                    'strength': 'HIGH'
                })
        
        # Setup 4: Volume Accumulation (price near support, high volume)
        if current_price <= bb_lower * 1.01 and volume_ratio > 1.8:  # Near lower band + high volume
            setups.append({
                'type': 'VOLUME_ACCUMULATION',
                'level': 'lower_band',
                'price': bb_lower,
                'strength': 'HIGH'
            })
        
        return setups
    
    def generate_signals(self):
        """
        Generate buy/sell signals for swing trading
        """
        print("🎯 Generating swing trading signals...")
        
        # Initialize signal columns
        self.data['signal'] = 0
        self.data['position'] = 0
        self.data['entry_price'] = np.nan
        self.data['stop_loss'] = np.nan
        self.data['take_profit'] = np.nan
        self.data['signal_type'] = ''
        self.data['setup_strength'] = ''
        
        position = 0
        entry_price = 0
        stop_loss = 0
        take_profit = 0
        entry_type = ''
        
        for i in range(50, len(self.data)):  # Start after enough data for indicators
            
            current_price = self.data['close'].iloc[i]
            rsi = self.data['rsi'].iloc[i]
            bb_upper = self.data['bb_upper'].iloc[i]
            bb_lower = self.data['bb_lower'].iloc[i]
            resistance = self.data['resistance'].iloc[i]
            support = self.data['support'].iloc[i]
            
            # Entry Signal: Look for swing setups
            if position == 0:
                setups = self.identify_swing_setups(i)
                
                if setups:
                    # Take the strongest setup
                    best_setup = max(setups, key=lambda x: x['strength'])
                    
                    if best_setup['strength'] in ['HIGH', 'MEDIUM']:
                        position = 1
                        entry_price = current_price
                        
                        # Calculate stop loss based on setup type
                        if best_setup['type'] in ['PULLBACK_FIB', 'PULLBACK_SUPPORT']:
                            stop_loss = support * 0.97  # 3% below support
                        elif best_setup['type'] == 'BB_BREAKOUT':
                            stop_loss = bb_lower * 0.98  # 2% below lower band
                        else:
                            stop_loss = entry_price * (1 - self.stop_loss_pct)
                        
                        # Calculate take profit (3:1 risk/reward)
                        risk = entry_price - stop_loss
                        take_profit = entry_price + (risk * self.take_profit_ratio)
                        
                        # Cap take profit at resistance
                        if take_profit > resistance:
                            take_profit = resistance * 0.99
                        
                        self.data.loc[i, 'signal'] = 1
                        self.data.loc[i, 'position'] = position
                        self.data.loc[i, 'entry_price'] = entry_price
                        self.data.loc[i, 'stop_loss'] = stop_loss
                        self.data.loc[i, 'take_profit'] = take_profit
                        self.data.loc[i, 'signal_type'] = best_setup['type']
                        self.data.loc[i, 'setup_strength'] = best_setup['strength']
                        
                        entry_type = best_setup['type']
            
            # Exit Signals: Multiple exit conditions
            elif position == 1:
                # Update trailing stop loss
                new_stop_loss = current_price * (1 - self.trailing_stop_pct)
                if new_stop_loss > stop_loss:
                    stop_loss = new_stop_loss
                    self.data.loc[i, 'stop_loss'] = stop_loss
                
                # Exit conditions
                stop_hit = current_price <= stop_loss
                take_profit_hit = current_price >= take_profit
                rsi_exit = rsi > self.rsi_overbought
                
                # Additional exits based on setup type
                bb_exit = False
                if entry_type == 'BB_BREAKOUT':
                    bb_exit = current_price < bb_lower  # Price falls back below lower band
                
                # Time-based exit (max 10 days for swing trade)
                days_in_trade = (self.data['timestamp'].iloc[i] - 
                                self.data.loc[self.data['entry_price'] == entry_price, 'timestamp'].iloc[0]).days
                time_exit = days_in_trade >= 10
                
                if stop_hit or take_profit_hit or rsi_exit or bb_exit or time_exit:
                    position = 0
                    self.data.loc[i, 'signal'] = -1
                    self.data.loc[i, 'position'] = position
                    
                    # Record the trade
                    self.record_trade(entry_price, current_price, i, entry_type)
                    
                    entry_price = 0
                    stop_loss = 0
                    take_profit = 0
                    entry_type = ''
            
            # Update position tracking
            self.data.loc[i, 'position'] = position
        
        print("✅ Swing trading signals generated successfully")
    
    def record_trade(self, entry_price, exit_price, exit_index, entry_type):
        """
        Record a completed swing trade
        """
        entry_date = self.data.loc[self.data['entry_price'] == entry_price, 'timestamp'].iloc[0]
        exit_date = self.data['timestamp'].iloc[exit_index]
        
        # Calculate trade metrics
        pnl = exit_price - entry_price
        pnl_pct = (pnl / entry_price) * 100
        duration = (exit_date - entry_date).days
        
        trade = {
            'entry_date': entry_date,
            'exit_date': exit_date,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'duration_days': duration,
            'entry_type': entry_type
        }
        
        self.trades.append(trade)
        
        # Update capital
        trade_value = self.current_capital * self.risk_per_trade
        capital_change = trade_value * (pnl_pct / 100)
        self.current_capital += capital_change
        
        print(f"📈 Swing Trade ({entry_type}): {entry_date.strftime('%Y-%m-%d')} → {exit_date.strftime('%Y-%m-%d')} | "
              f"${entry_price:.2f} → ${exit_price:.2f} | PnL: {pnl_pct:.2f}% | Duration: {duration} days")
    
    def calculate_performance_metrics(self):
        """
        Calculate comprehensive performance metrics for swing trading
        """
        if not self.trades:
            print("❌ No trades to analyze")
            return
        
        trades_df = pd.DataFrame(self.trades)
        
        # Basic metrics
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['pnl'] > 0])
        losing_trades = len(trades_df[trades_df['pnl'] < 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        # PnL metrics
        total_pnl = trades_df['pnl'].sum()
        total_pnl_pct = trades_df['pnl_pct'].sum()
        avg_pnl = trades_df['pnl'].mean()
        avg_pnl_pct = trades_df['pnl_pct'].mean()
        
        # Risk metrics
        max_win = trades_df['pnl_pct'].max()
        max_loss = trades_df['pnl_pct'].min()
        
        # Duration metrics
        avg_duration = trades_df['duration_days'].mean()
        
        # Capital metrics
        final_capital = self.current_capital
        total_return = ((final_capital - self.initial_capital) / self.initial_capital) * 100
        
        # Strategy-specific metrics
        pullback_trades = len(trades_df[trades_df['entry_type'].str.contains('PULLBACK')])
        breakout_trades = len(trades_df[trades_df['entry_type'].str.contains('BREAKOUT')])
        volume_trades = len(trades_df[trades_df['entry_type'].str.contains('VOLUME')])
        
        # Create performance summary
        self.performance = {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'total_pnl_pct': total_pnl_pct,
            'avg_pnl': avg_pnl,
            'avg_pnl_pct': avg_pnl_pct,
            'max_win': max_win,
            'max_loss': max_loss,
            'avg_duration': avg_duration,
            'initial_capital': self.initial_capital,
            'final_capital': final_capital,
            'total_return': total_return,
            'pullback_trades': pullback_trades,
            'breakout_trades': breakout_trades,
            'volume_trades': volume_trades
        }
        
        return self.performance
    
    def print_performance_summary(self):
        """
        Print comprehensive performance summary for swing trading
        """
        if not hasattr(self, 'performance'):
            self.calculate_performance_metrics()
        
        print("\n" + "="*70)
        print("📊 ETH SWING TRADING STRATEGY PERFORMANCE SUMMARY")
        print("="*70)
        
        print(f"💰 Capital Performance:")
        print(f"   Initial Capital: ${self.performance['initial_capital']:,.2f}")
        print(f"   Final Capital: ${self.performance['final_capital']:,.2f}")
        print(f"   Total Return: {self.performance['total_return']:.2f}%")
        
        print(f"\n📈 Trading Statistics:")
        print(f"   Total Trades: {self.performance['total_trades']}")
        print(f"   Winning Trades: {self.performance['winning_trades']}")
        print(f"   Losing Trades: {self.performance['losing_trades']}")
        print(f"   Win Rate: {self.performance['win_rate']:.2f}%")
        
        print(f"\n💵 PnL Analysis:")
        print(f"   Total PnL: ${self.performance['total_pnl']:,.2f}")
        print(f"   Total PnL %: {self.performance['total_pnl_pct']:.2f}%")
        print(f"   Average PnL: ${self.performance['avg_pnl']:.2f}")
        print(f"   Average PnL %: {self.performance['avg_pnl_pct']:.2f}%")
        print(f"   Best Trade: {self.performance['max_win']:.2f}%")
        print(f"   Worst Trade: {self.performance['max_loss']:.2f}%")
        
        print(f"\n⏱️ Trade Duration:")
        print(f"   Average Duration: {self.performance['avg_duration']:.1f} days")
        
        print(f"\n🎯 Strategy Breakdown:")
        print(f"   Pullback Trades: {self.performance['pullback_trades']}")
        print(f"   Breakout Trades: {self.performance['breakout_trades']}")
        print(f"   Volume Trades: {self.performance['volume_trades']}")
        
        print("="*70)
    
    def plot_results(self):
        """
        Create comprehensive visualization of swing trading results
        """
        fig, axes = plt.subplots(3, 2, figsize=(15, 12))
        fig.suptitle('ETH Swing Trading Strategy Backtest Results', fontsize=16, fontweight='bold')
        
        # Plot 1: Price with Bollinger Bands and signals
        ax1 = axes[0, 0]
        ax1.plot(self.data['timestamp'], self.data['close'], label='ETH Price', alpha=0.7, linewidth=1)
        ax1.plot(self.data['timestamp'], self.data['bb_upper'], label='BB Upper', alpha=0.6, color='red')
        ax1.plot(self.data['timestamp'], self.data['bb_middle'], label='BB Middle', alpha=0.6, color='blue')
        ax1.plot(self.data['timestamp'], self.data['bb_lower'], label='BB Lower', alpha=0.6, color='red')
        
        # Mark buy/sell signals
        buy_signals = self.data[self.data['signal'] == 1]
        sell_signals = self.data[self.data['signal'] == -1]
        
        ax1.scatter(buy_signals['timestamp'], buy_signals['close'], 
                   color='green', marker='^', s=100, label='Buy Signal', zorder=5)
        ax1.scatter(sell_signals['timestamp'], sell_signals['close'], 
                   color='red', marker='v', s=100, label='Sell Signal', zorder=5)
        
        ax1.set_title('ETH Price with Bollinger Bands and Swing Signals')
        ax1.set_ylabel('Price ($)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: RSI
        ax2 = axes[0, 1]
        ax2.plot(self.data['timestamp'], self.data['rsi'], label='RSI', color='purple')
        ax2.axhline(y=self.rsi_oversold, color='green', linestyle='--', alpha=0.7, label=f'Oversold ({self.rsi_oversold})')
        ax2.axhline(y=self.rsi_overbought, color='red', linestyle='--', alpha=0.7, label=f'Overbought ({self.rsi_overbought})')
        ax2.axhline(y=50, color='black', linestyle='-', alpha=0.3)
        ax2.set_title('RSI Indicator')
        ax2.set_ylabel('RSI')
        ax2.set_ylim(0, 100)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Volume Analysis
        ax3 = axes[1, 0]
        ax3.bar(self.data['timestamp'], self.data['volume'], alpha=0.6, color='orange', label='Volume')
        ax3.plot(self.data['timestamp'], self.data['volume_avg'], color='red', label='Volume Avg')
        ax3.set_title('Volume Analysis')
        ax3.set_ylabel('Volume')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Plot 4: Trade PnL Distribution
        ax4 = axes[1, 1]
        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            ax4.hist(trades_df['pnl_pct'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
            ax4.axvline(x=0, color='red', linestyle='--', alpha=0.7)
            ax4.set_title('Swing Trade PnL Distribution')
            ax4.set_xlabel('PnL (%)')
            ax4.set_ylabel('Frequency')
            ax4.grid(True, alpha=0.3)
        
        # Plot 5: Cumulative Returns
        ax5 = axes[2, 0]
        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            cumulative_pnl = trades_df['pnl_pct'].cumsum()
            ax5.plot(range(len(cumulative_pnl)), cumulative_pnl, color='green', linewidth=2)
            ax5.axhline(y=0, color='red', linestyle='--', alpha=0.7)
            ax5.set_title('Cumulative Swing Trading PnL')
            ax5.set_xlabel('Trade Number')
            ax5.set_ylabel('Cumulative PnL (%)')
            ax5.grid(True, alpha=0.3)
        
        # Plot 6: Trade Duration Distribution
        ax6 = axes[2, 1]
        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            ax6.hist(trades_df['duration_days'], bins=15, alpha=0.7, color='orange', edgecolor='black')
            ax6.set_title('Swing Trade Duration Distribution')
            ax6.set_xlabel('Duration (Days)')
            ax6.set_ylabel('Frequency')
            ax6.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def run_backtest(self, data_file):
        """
        Run the complete swing trading backtest
        """
        print("🚀 Starting ETH Swing Trading Strategy Backtest")
        print("="*70)
        
        # Load data
        if not self.load_data(data_file):
            return False
        
        # Calculate indicators
        self.calculate_indicators()
        
        # Generate signals
        self.generate_signals()
        
        # Calculate performance
        self.calculate_performance_metrics()
        
        # Print results
        self.print_performance_summary()
        
        # Create visualizations
        self.plot_results()
        
        print("\n✅ Swing trading backtest completed successfully!")
        return True

def main():
    """
    Main function to run the swing trading backtest
    """
    # Initialize swing trading backtester
    backtester = ETHSwingTradingBacktester(
        initial_capital=10000,  # $10,000 starting capital
        risk_per_trade=0.015   # 1.5% risk per trade (lower for swing trading)
    )
    
    # Run backtest with ETH data
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    success = backtester.run_backtest(data_file)
    
    if success:
        print("\n🎯 Swing Trading Strategy Analysis Complete!")
        print("📊 Check the charts above for detailed analysis")
        print("💡 This strategy focuses on pullbacks, breakouts, and volume accumulation")
    else:
        print("❌ Swing trading backtest failed. Please check your data file.")

if __name__ == "__main__":
    main()
