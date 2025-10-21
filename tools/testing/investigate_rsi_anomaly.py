"""
🔍 RSI Mean Reversion BTC-1d Anomaly Investigation
=================================================
Investigating the suspicious 30,571% return to determine if it's:
- A legitimate edge discovery
- A compounding calculation error
- A data quality issue
- A position sizing bug
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '/Users/bobbyyo/Projects/algo-fun')

from strategies.core_strategies.rsi_mean_reversion_strategy import RSIMeanReversionStrategy
from backtesting import Backtest

def load_btc_data():
    """Load BTC 1d data for investigation"""
    df = pd.read_csv('/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv')
    
    # Standardize columns
    if 'Datetime' in df.columns:
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        df.set_index('Datetime', inplace=True)
    elif 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
    
    df.columns = [col.capitalize() for col in df.columns]
    
    if 'Volume' not in df.columns:
        df['Volume'] = 1000
    
    return df


def run_detailed_backtest():
    """Run backtest with detailed trade logging"""
    print("\n" + "="*80)
    print("🔍 RSI MEAN REVERSION BTC-1D ANOMALY INVESTIGATION")
    print("="*80)
    
    df = load_btc_data()
    
    print(f"\n📊 Data Overview:")
    print(f"   Rows: {len(df)}")
    print(f"   Date Range: {df.index[0]} to {df.index[-1]}")
    print(f"   Price Range: ${df['Close'].min():.2f} to ${df['Close'].max():.2f}")
    print(f"   Columns: {list(df.columns)}")
    
    # Run backtest with trade recording
    print("\n🚀 Running Backtest...")
    bt = Backtest(df, RSIMeanReversionStrategy, cash=10000, commission=0.002)
    stats = bt.run()
    
    # Display full stats
    print("\n" + "="*80)
    print("📊 FULL BACKTEST STATISTICS")
    print("="*80)
    print(stats)
    print("\n")
    
    # Get trades details
    trades = stats._trades
    if trades is not None and len(trades) > 0:
        print("="*80)
        print(f"📋 TRADE DETAILS ({len(trades)} total trades)")
        print("="*80)
        
        trades_df = trades.copy()
        trades_df['Return_%'] = (trades_df['ReturnPct'] * 100).round(2)
        trades_df['PnL'] = trades_df['PnL'].round(2)
        
        print("\n🎯 First 10 Trades:")
        print(trades_df.head(10)[['EntryTime', 'ExitTime', 'Size', 'EntryPrice', 'ExitPrice', 'PnL', 'Return_%']].to_string())
        
        print("\n🎯 Last 10 Trades:")
        print(trades_df.tail(10)[['EntryTime', 'ExitTime', 'Size', 'EntryPrice', 'ExitPrice', 'PnL', 'Return_%']].to_string())
        
        print("\n🎯 Top 5 Winners:")
        top_winners = trades_df.nlargest(5, 'PnL')[['EntryTime', 'ExitTime', 'Size', 'EntryPrice', 'ExitPrice', 'PnL', 'Return_%']]
        print(top_winners.to_string())
        
        print("\n🎯 Top 5 Losers:")
        top_losers = trades_df.nsmallest(5, 'PnL')[['EntryTime', 'ExitTime', 'Size', 'EntryPrice', 'ExitPrice', 'PnL', 'Return_%']]
        print(top_losers.to_string())
        
        # Analyze position sizing
        print("\n" + "="*80)
        print("💰 POSITION SIZING ANALYSIS")
        print("="*80)
        print(f"   Min Position Size: {trades_df['Size'].min():.2f}")
        print(f"   Max Position Size: {trades_df['Size'].max():.2f}")
        print(f"   Avg Position Size: {trades_df['Size'].mean():.2f}")
        print(f"   Std Position Size: {trades_df['Size'].std():.2f}")
        
        # Check for compounding
        print("\n" + "="*80)
        print("🔬 COMPOUNDING ANALYSIS")
        print("="*80)
        
        # Calculate equity progression
        trades_df = trades_df.sort_values('EntryTime')
        trades_df['CumPnL'] = trades_df['PnL'].cumsum()
        trades_df['EquityAfterTrade'] = 10000 + trades_df['CumPnL']
        
        print("\n📈 Equity Progression (every 10th trade):")
        equity_sample = trades_df.iloc[::10][['EntryTime', 'PnL', 'CumPnL', 'EquityAfterTrade']]
        print(equity_sample.to_string())
        
        # Check if position sizes are scaling with equity
        correlation = trades_df['Size'].corr(trades_df['EquityAfterTrade'])
        print(f"\n🔗 Correlation between Position Size and Equity: {correlation:.4f}")
        if correlation > 0.7:
            print("   ⚠️ HIGH CORRELATION - Position sizing appears to be compounding")
        elif correlation > 0.3:
            print("   ⚠️ MODERATE CORRELATION - Some compounding effect present")
        else:
            print("   ✅ LOW CORRELATION - Fixed position sizing")
        
        # Final equity check
        final_equity = stats['Equity Final [$]']
        expected_simple_return = (1 + trades_df['ReturnPct'].mean()) ** len(trades_df) * 10000
        
        print("\n" + "="*80)
        print("🧮 RETURN CALCULATION VERIFICATION")
        print("="*80)
        print(f"   Starting Capital: $10,000")
        print(f"   Final Equity: ${final_equity:,.2f}")
        print(f"   Total Return: {stats['Return [%]']:.2f}%")
        print(f"   Number of Trades: {len(trades_df)}")
        print(f"   Avg Trade Return: {trades_df['ReturnPct'].mean()*100:.2f}%")
        print(f"   Expected (Simple Compounding): ${expected_simple_return:,.2f}")
        print(f"   Difference: ${abs(final_equity - expected_simple_return):,.2f}")
        
        if abs(final_equity - expected_simple_return) / final_equity > 0.1:
            print("\n   ⚠️ WARNING: Large discrepancy detected!")
            print("   Possible causes:")
            print("   - Position sizing compounding effect")
            print("   - Leverage being applied")
            print("   - Calculation error in strategy")
        
        # Save detailed trades for manual inspection
        output_file = '/Users/bobbyyo/Projects/algo-fun/rsi_btc_trades_detailed.csv'
        trades_df.to_csv(output_file, index=False)
        print(f"\n✅ Full trade details saved to: {output_file}")
        
    else:
        print("❌ No trades found!")
    
    # Plot the results
    try:
        print("\n📊 Generating interactive chart...")
        bt.plot()
    except Exception as e:
        print(f"⚠️ Could not generate plot: {e}")
    
    return stats, trades_df if trades is not None and len(trades) > 0 else None


def analyze_rsi_parameters():
    """Analyze if RSI parameters are causing the anomaly"""
    print("\n" + "="*80)
    print("🎛️ RSI PARAMETER ANALYSIS")
    print("="*80)
    
    df = load_btc_data()
    
    # Test with different RSI parameters
    test_configs = [
        {'rsi_period': 14, 'rsi_oversold': 30, 'risk_pct': 1.5, 'label': 'Default'},
        {'rsi_period': 14, 'rsi_oversold': 25, 'risk_pct': 1.5, 'label': 'Aggressive Oversold'},
        {'rsi_period': 7, 'rsi_oversold': 30, 'risk_pct': 1.0, 'label': 'Shorter Period'},
        {'rsi_period': 21, 'rsi_oversold': 35, 'risk_pct': 1.0, 'label': 'Conservative'},
    ]
    
    results = []
    for config in test_configs:
        print(f"\nTesting {config['label']}...")
        
        # Create custom strategy class with these parameters
        class CustomRSI(RSIMeanReversionStrategy):
            rsi_period = config['rsi_period']
            rsi_oversold = config['rsi_oversold']
            risk_pct = config['risk_pct']
        
        bt = Backtest(df, CustomRSI, cash=10000, commission=0.002)
        stats = bt.run()
        
        results.append({
            'Config': config['label'],
            'RSI_Period': config['rsi_period'],
            'RSI_Oversold': config['rsi_oversold'],
            'Risk_%': config['risk_pct'],
            'Return_%': stats['Return [%]'],
            'Trades': stats['# Trades'],
            'Win_Rate_%': stats['Win Rate [%]'],
            'Max_DD_%': stats['Max. Drawdown [%]']
        })
    
    results_df = pd.DataFrame(results)
    print("\n📊 Parameter Comparison:")
    print(results_df.to_string(index=False))
    
    return results_df


if __name__ == "__main__":
    # Run detailed investigation
    stats, trades = run_detailed_backtest()
    
    # Analyze parameters
    param_results = analyze_rsi_parameters()
    
    print("\n" + "="*80)
    print("🎯 INVESTIGATION SUMMARY")
    print("="*80)
    
    if stats['Return [%]'] > 1000:
        print("⚠️ ANOMALY CONFIRMED: Return exceeds 1000%")
        print("\nLikely causes (in order of probability):")
        print("1. 🔴 Position sizing compounding with equity growth")
        print("2. 🟡 Strategy is holding very large positions")
        print("3. 🟢 Legitimate edge in specific market conditions")
        print("\nRecommendation: Review position sizing logic and trade details above")
    else:
        print("✅ Returns appear reasonable for BTC over 10-year period")
    
    print("\n🌙💫🚀 Investigation complete!")

