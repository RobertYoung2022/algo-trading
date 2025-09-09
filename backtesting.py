"""
ETH Trend-Following Strategy Backtesting System
===============================================

This backtesting system implements the ETH trend-following strategy outlined:
- Uses 10-day and 50-day SMA crossovers for trend confirmation
- MACD momentum confirmation
- RSI filtering for overbought conditions
- Position management with stop-loss and trailing stops
- Risk management (1-2% per trade)

Author: AI Assistant
Date: 2025
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Try to import technical analysis libraries
try:
    from ta.momentum import RSIIndicator
    from ta.trend import MACD
    TA_AVAILABLE = True
except ImportError:
    print("⚠️ ta library not available. Installing...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'ta'])
    from ta.momentum import RSIIndicator
    from ta.trend import MACD
    TA_AVAILABLE = True

class ETHTrendBacktester:
    """
    Comprehensive backtesting system for ETH trend-following strategy
    """
    
    def __init__(self, initial_capital=10000, risk_per_trade=0.02):
        """
        Initialize the backtester
        
        Args:
            initial_capital (float): Starting capital
            risk_per_trade (float): Risk per trade as percentage (0.02 = 2%)
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_per_trade = risk_per_trade
        self.positions = []
        self.trades = []
        self.equity_curve = []
        
        # Strategy parameters
        self.sma_short = 10
        self.sma_long = 50
        self.rsi_period = 14
        self.rsi_overbought = 70
        self.rsi_exit_overbought = 80
        
        # Risk management
        self.stop_loss_pct = 0.05  # 5% stop loss
        self.take_profit_ratio = 2.0  # 2:1 risk/reward
        self.trailing_stop_pct = 0.03  # 3% trailing stop
        
    def load_data(self, file_path):
        """
        Load historical ETH data from CSV file
        
        Args:
            file_path (str): Path to CSV file with OHLCV data
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
        Calculate all technical indicators needed for the strategy
        """
        print("📊 Calculating technical indicators...")
        
        # Simple Moving Averages
        self.data[f'sma_{self.sma_short}'] = self.data['close'].rolling(window=self.sma_short).mean()
        self.data[f'sma_{self.sma_long}'] = self.data['close'].rolling(window=self.sma_long).mean()
        
        # MACD
        macd = MACD(self.data['close'])
        self.data['macd'] = macd.macd()
        self.data['macd_signal'] = macd.macd_signal()
        self.data['macd_histogram'] = macd.macd_diff()
        
        # RSI
        rsi = RSIIndicator(self.data['close'], window=self.rsi_period)
        self.data['rsi'] = rsi.rsi()
        
        # Price change for returns calculation
        self.data['price_change'] = self.data['close'].pct_change()
        
        print("✅ Indicators calculated successfully")
    
    def generate_signals(self):
        """
        Generate buy/sell signals based on the strategy rules
        """
        print("🎯 Generating trading signals...")
        
        # Initialize signal columns
        self.data['signal'] = 0
        self.data['position'] = 0
        self.data['entry_price'] = np.nan
        self.data['stop_loss'] = np.nan
        self.data['take_profit'] = np.nan
        
        position = 0  # 0 = no position, 1 = long position
        entry_price = 0
        stop_loss = 0
        take_profit = 0
        
        for i in range(self.sma_long, len(self.data)):
            current_price = self.data['close'].iloc[i]
            sma_short = self.data[f'sma_{self.sma_short}'].iloc[i]
            sma_long = self.data[f'sma_{self.sma_long}'].iloc[i]
            macd = self.data['macd'].iloc[i]
            macd_signal = self.data['macd_signal'].iloc[i]
            rsi = self.data['rsi'].iloc[i]
            
            # Previous values for crossover detection
            prev_sma_short = self.data[f'sma_{self.sma_short}'].iloc[i-1]
            prev_sma_long = self.data[f'sma_{self.sma_long}'].iloc[i-1]
            prev_macd = self.data['macd'].iloc[i-1]
            prev_macd_signal = self.data['macd_signal'].iloc[i-1]
            
            # Entry Signal: SMA crossover + MACD confirmation + RSI not overbought
            if position == 0:
                # SMA crossover: short SMA crosses above long SMA
                sma_crossover = (prev_sma_short <= prev_sma_long) and (sma_short > sma_long)
                
                # MACD confirmation: MACD crosses above signal line
                macd_crossover = (prev_macd <= prev_macd_signal) and (macd > macd_signal)
                
                # RSI filter: not overbought
                rsi_filter = rsi < self.rsi_overbought
                
                if sma_crossover and macd_crossover and rsi_filter:
                    position = 1
                    entry_price = current_price
                    stop_loss = entry_price * (1 - self.stop_loss_pct)
                    take_profit = entry_price * (1 + self.stop_loss_pct * self.take_profit_ratio)
                    
                    self.data.loc[i, 'signal'] = 1  # Buy signal
                    self.data.loc[i, 'position'] = position
                    self.data.loc[i, 'entry_price'] = entry_price
                    self.data.loc[i, 'stop_loss'] = stop_loss
                    self.data.loc[i, 'take_profit'] = take_profit
                    
            # Exit Signals: SMA crossover down OR RSI overbought OR stop/take profit hit
            elif position == 1:
                # Update trailing stop loss
                new_stop_loss = current_price * (1 - self.trailing_stop_pct)
                if new_stop_loss > stop_loss:
                    stop_loss = new_stop_loss
                    self.data.loc[i, 'stop_loss'] = stop_loss
                
                # Exit conditions
                sma_crossunder = (prev_sma_short >= prev_sma_long) and (sma_short < sma_long)
                rsi_exit = rsi > self.rsi_exit_overbought
                stop_hit = current_price <= stop_loss
                take_profit_hit = current_price >= take_profit
                
                if sma_crossunder or rsi_exit or stop_hit or take_profit_hit:
                    position = 0
                    self.data.loc[i, 'signal'] = -1  # Sell signal
                    self.data.loc[i, 'position'] = position
                    
                    # Record the trade
                    self.record_trade(entry_price, current_price, i)
                    
                    entry_price = 0
                    stop_loss = 0
                    take_profit = 0
            
            # Update position tracking
            self.data.loc[i, 'position'] = position
        
        print("✅ Signals generated successfully")
    
    def record_trade(self, entry_price, exit_price, exit_index):
        """
        Record a completed trade
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
            'duration_days': duration
        }
        
        self.trades.append(trade)
        
        # Update capital
        trade_value = self.current_capital * self.risk_per_trade
        capital_change = trade_value * (pnl_pct / 100)
        self.current_capital += capital_change
        
        print(f"📈 Trade: {entry_date.strftime('%Y-%m-%d')} → {exit_date.strftime('%Y-%m-%d')} | "
              f"${entry_price:.2f} → ${exit_price:.2f} | PnL: {pnl_pct:.2f}% | Duration: {duration} days")
    
    def calculate_performance_metrics(self):
        """
        Calculate comprehensive performance metrics
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
            'total_return': total_return
        }
        
        return self.performance
    
    def print_performance_summary(self):
        """
        Print a comprehensive performance summary
        """
        if not hasattr(self, 'performance'):
            self.calculate_performance_metrics()
        
        print("\n" + "="*60)
        print("📊 ETH TREND-FOLLOWING STRATEGY PERFORMANCE SUMMARY")
        print("="*60)
        
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
        
        print("="*60)
    
    def plot_results(self):
        """
        Create comprehensive visualization of backtest results
        """
        fig, axes = plt.subplots(3, 2, figsize=(15, 12))
        fig.suptitle('ETH Trend-Following Strategy Backtest Results', fontsize=16, fontweight='bold')
        
        # Plot 1: Price with SMA and signals
        ax1 = axes[0, 0]
        ax1.plot(self.data['timestamp'], self.data['close'], label='ETH Price', alpha=0.7, linewidth=1)
        ax1.plot(self.data['timestamp'], self.data[f'sma_{self.sma_short}'], label=f'SMA {self.sma_short}', alpha=0.8)
        ax1.plot(self.data['timestamp'], self.data[f'sma_{self.sma_long}'], label=f'SMA {self.sma_long}', alpha=0.8)
        
        # Mark buy/sell signals
        buy_signals = self.data[self.data['signal'] == 1]
        sell_signals = self.data[self.data['signal'] == -1]
        
        ax1.scatter(buy_signals['timestamp'], buy_signals['close'], 
                   color='green', marker='^', s=100, label='Buy Signal', zorder=5)
        ax1.scatter(sell_signals['timestamp'], sell_signals['close'], 
                   color='red', marker='v', s=100, label='Sell Signal', zorder=5)
        
        ax1.set_title('ETH Price with SMA Crossovers and Signals')
        ax1.set_ylabel('Price ($)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: MACD
        ax2 = axes[0, 1]
        ax2.plot(self.data['timestamp'], self.data['macd'], label='MACD', color='blue')
        ax2.plot(self.data['timestamp'], self.data['macd_signal'], label='Signal', color='red')
        ax2.bar(self.data['timestamp'], self.data['macd_histogram'], label='Histogram', alpha=0.3)
        ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        ax2.set_title('MACD Indicator')
        ax2.set_ylabel('MACD')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: RSI
        ax3 = axes[1, 0]
        ax3.plot(self.data['timestamp'], self.data['rsi'], label='RSI', color='purple')
        ax3.axhline(y=self.rsi_overbought, color='red', linestyle='--', alpha=0.7, label=f'Overbought ({self.rsi_overbought})')
        ax3.axhline(y=self.rsi_exit_overbought, color='darkred', linestyle='--', alpha=0.7, label=f'Exit Overbought ({self.rsi_exit_overbought})')
        ax3.axhline(y=50, color='black', linestyle='-', alpha=0.3)
        ax3.set_title('RSI Indicator')
        ax3.set_ylabel('RSI')
        ax3.set_ylim(0, 100)
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Plot 4: Trade PnL Distribution
        ax4 = axes[1, 1]
        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            ax4.hist(trades_df['pnl_pct'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
            ax4.axvline(x=0, color='red', linestyle='--', alpha=0.7)
            ax4.set_title('Trade PnL Distribution')
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
            ax5.set_title('Cumulative PnL')
            ax5.set_xlabel('Trade Number')
            ax5.set_ylabel('Cumulative PnL (%)')
            ax5.grid(True, alpha=0.3)
        
        # Plot 6: Trade Duration Distribution
        ax6 = axes[2, 1]
        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            ax6.hist(trades_df['duration_days'], bins=15, alpha=0.7, color='orange', edgecolor='black')
            ax6.set_title('Trade Duration Distribution')
            ax6.set_xlabel('Duration (Days)')
            ax6.set_ylabel('Frequency')
            ax6.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def run_backtest(self, data_file):
        """
        Run the complete backtest
        
        Args:
            data_file (str): Path to historical data CSV file
        """
        print("🚀 Starting ETH Trend-Following Strategy Backtest")
        print("="*60)
        
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
        
        print("\n✅ Backtest completed successfully!")
        return True

def main():
    """
    Main function to run the backtest
    """
    # Initialize backtester
    backtester = ETHTrendBacktester(
        initial_capital=10000,  # $10,000 starting capital
        risk_per_trade=0.02     # 2% risk per trade
    )
    
    # Run backtest with ETH data
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    success = backtester.run_backtest(data_file)
    
    if success:
        print("\n🎯 Strategy Analysis Complete!")
        print("📊 Check the charts above for detailed analysis")
        print("💡 Consider adjusting parameters based on results")
    else:
        print("❌ Backtest failed. Please check your data file.")

if __name__ == "__main__":
    main()
