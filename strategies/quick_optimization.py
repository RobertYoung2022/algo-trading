"""
Quick Strategy Optimization Example
===================================

Demonstrates the optimization framework with a smaller parameter set
for faster execution and proof of concept.
"""

import sys
import os
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies')

import pandas as pd
import numpy as np
from backtesting import Backtest
from enhanced_eth_momentum_final import EnhancedETHMomentumFinal
from itertools import product

def quick_optimize():
    """Run a quick optimization example"""
    
    print("🚀 QUICK STRATEGY OPTIMIZATION EXAMPLE")
    print("=" * 60)
    
    # Load ETH data
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    # Load data
    data = pd.read_csv(data_file)
    date_cols = [col for col in data.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
    if date_cols:
        date_col = date_cols[0]
        data[date_col] = pd.to_datetime(data[date_col])
        data = data.set_index(date_col)
    
    data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    data = data.sort_index().dropna()
    
    print(f"📊 Data: {len(data)} bars from {data.index[0].date()} to {data.index[-1].date()}")
    
    # Define small parameter ranges for quick testing
    param_ranges = {
        'macd_fast': [5, 8, 12],
        'rsi_threshold': [65, 70, 75],
        'atr_multiplier': [1.5, 2.0, 2.5]
    }
    
    print(f"🔧 Testing parameters:")
    for param, values in param_ranges.items():
        print(f"   {param}: {values}")
    
    # Generate combinations
    param_names = list(param_ranges.keys())
    param_values = list(param_ranges.values())
    combinations = list(product(*param_values))
    
    print(f"🔄 Testing {len(combinations)} combinations...")
    
    results = []
    best_sharpe = float('-inf')
    best_params = None
    
    for i, combo in enumerate(combinations):
        try:
            # Create strategy with current parameters
            class OptimizedStrategy(EnhancedETHMomentumFinal):
                pass
            
            # Set parameters
            for param_name, param_value in zip(param_names, combo):
                setattr(OptimizedStrategy, param_name, param_value)
            
            # Run backtest
            bt = Backtest(data, OptimizedStrategy, cash=100000, commission=0.001)
            stats = bt.run()
            
            # Extract results
            result = {
                'macd_fast': combo[0],
                'rsi_threshold': combo[1],
                'atr_multiplier': combo[2],
                'Return_%': stats.get('Return [%]', 0),
                'Sharpe': stats.get('Sharpe Ratio', 0),
                'Max_DD_%': stats.get('Max. Drawdown [%]', 0),
                'Trades': stats.get('# Trades', 0),
                'Win_Rate_%': stats.get('Win Rate [%]', 0),
                'Profit_Factor': stats.get('Profit Factor', 0)
            }
            
            results.append(result)
            
            # Track best
            sharpe = stats.get('Sharpe Ratio', float('-inf'))
            if not pd.isna(sharpe) and sharpe > best_sharpe:
                best_sharpe = sharpe
                best_params = dict(zip(param_names, combo))
            
            print(f"   {i+1:2}/{len(combinations)}: MACD={combo[0]:2}, RSI={combo[1]:2}, ATR={combo[2]:.1f} → "
                  f"Sharpe={sharpe:6.3f}, Return={result['Return_%']:6.2f}%, Trades={result['Trades']:2.0f}")
            
        except Exception as e:
            print(f"   {i+1:2}/{len(combinations)}: Error - {str(e)[:50]}")
            continue
    
    # Analyze results
    if results:
        df = pd.DataFrame(results)
        
        print(f"\n📊 OPTIMIZATION RESULTS")
        print("=" * 50)
        print(f"Valid results: {len(df)}/{len(combinations)}")
        
        # Sort by Sharpe ratio
        df_sorted = df.sort_values('Sharpe', ascending=False)
        
        print(f"\n🏆 TOP 5 COMBINATIONS:")
        print("Rank | MACD | RSI | ATR | Sharpe | Return% | Trades | Win%")
        print("-" * 60)
        
        for i, (_, row) in enumerate(df_sorted.head(5).iterrows(), 1):
            print(f"{i:4} | {row['macd_fast']:4.0f} | {row['rsi_threshold']:3.0f} | "
                  f"{row['atr_multiplier']:3.1f} | {row['Sharpe']:6.3f} | "
                  f"{row['Return_%']:7.2f} | {row['Trades']:6.0f} | {row['Win_Rate_%']:4.1f}")
        
        # Best parameters
        if best_params:
            best_result = df_sorted.iloc[0]
            print(f"\n🎯 RECOMMENDED PARAMETERS:")
            print(f"   MACD Fast: {best_params['macd_fast']}")
            print(f"   RSI Threshold: {best_params['rsi_threshold']}")
            print(f"   ATR Multiplier: {best_params['atr_multiplier']}")
            print(f"\n📈 Expected Performance:")
            print(f"   Sharpe Ratio: {best_result['Sharpe']:.3f}")
            print(f"   Return: {best_result['Return_%']:.2f}%")
            print(f"   Max Drawdown: {best_result['Max_DD_%']:.2f}%")
            print(f"   Trades: {best_result['Trades']:.0f}")
            print(f"   Win Rate: {best_result['Win_Rate_%']:.1f}%")
        
        # Parameter analysis
        print(f"\n🔬 PARAMETER ANALYSIS:")
        
        # MACD Fast analysis
        macd_analysis = df.groupby('macd_fast')['Sharpe'].agg(['mean', 'std', 'count'])
        print(f"   MACD Fast Period:")
        for macd_val, stats in macd_analysis.iterrows():
            print(f"      {macd_val}: Avg Sharpe = {stats['mean']:.3f} ± {stats['std']:.3f} ({stats['count']} tests)")
        
        # RSI analysis
        rsi_analysis = df.groupby('rsi_threshold')['Sharpe'].agg(['mean', 'std', 'count'])
        print(f"   RSI Threshold:")
        for rsi_val, stats in rsi_analysis.iterrows():
            print(f"      {rsi_val}: Avg Sharpe = {stats['mean']:.3f} ± {stats['std']:.3f} ({stats['count']} tests)")
        
        # ATR analysis
        atr_analysis = df.groupby('atr_multiplier')['Sharpe'].agg(['mean', 'std', 'count'])
        print(f"   ATR Multiplier:")
        for atr_val, stats in atr_analysis.iterrows():
            print(f"      {atr_val}: Avg Sharpe = {stats['mean']:.3f} ± {stats['std']:.3f} ({stats['count']} tests)")
        
        # Save results
        output_file = '/Users/bobbyyo/Projects/algo-fun/strategies/optimization_results.csv'
        df.to_csv(output_file, index=False)
        print(f"\n📁 Results saved to: optimization_results.csv")
        
        return df, best_params
    
    else:
        print("❌ No valid results generated")
        return None, None

if __name__ == "__main__":
    results, best_params = quick_optimize()
    print("\n✅ Quick optimization complete!")