"""
🧪 Core Strategies Comprehensive Backtest Runner
===============================================
Tests all 3 core strategies on multiple assets and timeframes

Strategies:
1. SMA Crossover Strategy
2. RSI Mean Reversion Strategy
3. Breakout Momentum Strategy

Assets: BTC, ETH, XRP, LINK
Timeframes: 1d, 6h, 1h
"""

import sys
import os
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.insert(0, '/Users/bobbyyo/Projects/algo-fun')

# Import the strategies
from strategies.core_strategies.sma_crossover_strategy import SMAStrategy, test_sma_strategy_single_asset
from strategies.core_strategies.rsi_mean_reversion_strategy import RSIMeanReversionStrategy, test_rsi_strategy_single_asset
from strategies.core_strategies.breakout_momentum_strategy import BreakoutMomentumStrategy, test_breakout_strategy_single_asset

from backtesting import Backtest

# Define test datasets with correct paths
TEST_DATASETS = {
    'BTC': [
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv', 'BTC-1d'),
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-6h-500wks-data.csv', 'BTC-6h'),
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1h-500wks-data.csv', 'BTC-1h'),
    ],
    'ETH': [
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv', 'ETH-1d'),
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-6h-200wks-enhanced-data.csv', 'ETH-6h'),
    ],
    'XRP': [
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/XRPUSD-1d-500wks-enhanced-data.csv', 'XRP-1d'),
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/XRPUSD-1h-100wks-enhanced-data.csv', 'XRP-1h'),
    ],
    'LINK': [
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv', 'LINK-1d'),
        ('/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/LINKUSD-6h-200wks-enhanced-data.csv', 'LINK-6h'),
    ]
}


def load_and_prepare_data(file_path):
    """Load and prepare data for backtesting"""
    try:
        df = pd.read_csv(file_path)
        
        # Handle different date column names
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df.set_index('Datetime', inplace=True)
        elif 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        
        # Standardize column names
        df.columns = [col.capitalize() for col in df.columns]
        
        # Ensure required columns exist
        required = ['Open', 'High', 'Low', 'Close']
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Add Volume if missing
        if 'Volume' not in df.columns:
            df['Volume'] = 1000
        
        return df
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None


def run_single_strategy_test(strategy_class, strategy_name, df, label):
    """Run a single strategy backtest"""
    try:
        bt = Backtest(df, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()
        
        return {
            'Strategy': strategy_name,
            'Asset': label,
            'Return_%': round(stats['Return [%]'], 2),
            'Sharpe': round(stats['Sharpe Ratio'], 2) if pd.notna(stats['Sharpe Ratio']) else 0.0,
            'Max_DD_%': round(stats['Max. Drawdown [%]'], 2),
            'Win_Rate_%': round(stats['Win Rate [%]'], 2) if pd.notna(stats['Win Rate [%]']) else 0.0,
            'Trades': int(stats['# Trades']),
            'Profit_Factor': round(stats['Profit Factor'], 2) if pd.notna(stats['Profit Factor']) else 0.0,
            'Bars': len(df)
        }
    except Exception as e:
        print(f"❌ Error testing {strategy_name} on {label}: {e}")
        return None


def run_comprehensive_backtest():
    """Run comprehensive backtests on all strategies"""
    
    print("\n" + "="*80)
    print("🚀 COMPREHENSIVE CORE STRATEGIES BACKTEST")
    print("="*80)
    print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💰 Initial Capital: $10,000")
    print(f"📊 Commission: 0.2%")
    print("\n")
    
    all_results = []
    
    strategies = [
        (SMAStrategy, 'SMA Crossover'),
        (RSIMeanReversionStrategy, 'RSI Mean Reversion'),
        (BreakoutMomentumStrategy, 'Breakout Momentum')
    ]
    
    # Test each strategy on each dataset
    for strategy_class, strategy_name in strategies:
        print(f"\n{'='*80}")
        print(f"🎯 Testing Strategy: {strategy_name}")
        print(f"{'='*80}\n")
        
        for asset_name, datasets in TEST_DATASETS.items():
            for file_path, label in datasets:
                print(f"📊 Loading {label}...", end=' ')
                
                # Check if file exists
                if not os.path.exists(file_path):
                    print(f"❌ File not found")
                    continue
                
                df = load_and_prepare_data(file_path)
                if df is None or len(df) < 100:
                    print(f"❌ Invalid data")
                    continue
                
                print(f"✅ {len(df)} bars")
                print(f"   Running {strategy_name}...", end=' ')
                
                result = run_single_strategy_test(strategy_class, strategy_name, df, label)
                if result:
                    all_results.append(result)
                    print(f"✅ Return: {result['Return_%']}% | Sharpe: {result['Sharpe']} | Win Rate: {result['Win_Rate_%']}%")
                else:
                    print("❌ Failed")
    
    # Create results DataFrame
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE RESULTS SUMMARY")
        print("="*80)
        print(results_df.to_string(index=False))
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'/Users/bobbyyo/Projects/algo-fun/strategies/results/core_strategies_backtest_{timestamp}.csv'
        results_df.to_csv(output_file, index=False)
        print(f"\n✅ Results saved to: {output_file}")
        
        # Generate summary by strategy
        print("\n" + "="*80)
        print("📈 STRATEGY PERFORMANCE SUMMARY")
        print("="*80)
        
        for strategy_name in results_df['Strategy'].unique():
            strategy_results = results_df[results_df['Strategy'] == strategy_name]
            
            print(f"\n🎯 {strategy_name}:")
            print(f"   Tests Run: {len(strategy_results)}")
            print(f"   Avg Return: {strategy_results['Return_%'].mean():.2f}%")
            print(f"   Avg Sharpe: {strategy_results['Sharpe'].mean():.2f}")
            print(f"   Avg Max DD: {strategy_results['Max_DD_%'].mean():.2f}%")
            print(f"   Avg Win Rate: {strategy_results['Win_Rate_%'].mean():.2f}%")
            print(f"   Profitable Tests: {len(strategy_results[strategy_results['Return_%'] > 0])}/{len(strategy_results)}")
            
            # Best performing asset
            best_test = strategy_results.loc[strategy_results['Return_%'].idxmax()]
            print(f"   🏆 Best: {best_test['Asset']} ({best_test['Return_%']}% return)")
        
        # Production readiness assessment
        print("\n" + "="*80)
        print("🛡️ PRODUCTION READINESS ASSESSMENT")
        print("="*80)
        print("\nCriteria: Return ≥ 20%, Sharpe ≥ 1.5, Max DD ≥ -15%\n")
        
        for strategy_name in results_df['Strategy'].unique():
            strategy_results = results_df[results_df['Strategy'] == strategy_name]
            
            # Check production criteria
            production_ready = strategy_results[
                (strategy_results['Return_%'] >= 20) &
                (strategy_results['Sharpe'] >= 1.5) &
                (strategy_results['Max_DD_%'] >= -15)
            ]
            
            print(f"{strategy_name}:")
            if len(production_ready) > 0:
                print(f"   ✅ PRODUCTION READY: {len(production_ready)} configuration(s)")
                for _, row in production_ready.iterrows():
                    print(f"      - {row['Asset']}: {row['Return_%']}% return, {row['Sharpe']} Sharpe")
            else:
                print(f"   ⚠️ NOT PRODUCTION READY - Needs optimization")
                
                # Show what needs improvement
                avg_return = strategy_results['Return_%'].mean()
                avg_sharpe = strategy_results['Sharpe'].mean()
                avg_dd = strategy_results['Max_DD_%'].mean()
                
                improvements = []
                if avg_return < 20:
                    improvements.append(f"Return ({avg_return:.1f}% < 20%)")
                if avg_sharpe < 1.5:
                    improvements.append(f"Sharpe ({avg_sharpe:.2f} < 1.5)")
                if avg_dd < -15:
                    improvements.append(f"Drawdown ({avg_dd:.1f}% < -15%)")
                
                print(f"      Needs: {', '.join(improvements)}")
        
        print("\n" + "="*80)
        print("✅ BACKTEST COMPLETE")
        print("="*80)
        
        return results_df
    else:
        print("\n❌ No results generated - all tests failed")
        return None


if __name__ == "__main__":
    results = run_comprehensive_backtest()
    
    if results is not None:
        print("\n🌙💫🚀 Core Strategies Backtest Complete!")
    else:
        print("\n❌ Backtest failed - check errors above")

