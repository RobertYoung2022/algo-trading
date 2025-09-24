"""
Enhanced ETH Momentum Strategy - Multi-Data Testing
==================================================

This script tests the Enhanced ETH Momentum Strategy on all available datasets
and compares performance against the original MACD strategy.

Features:
- Tests on all ETH datasets (Coinbase, CoinGecko, CoinMarketCap, CryptoCompare)
- Generates comprehensive performance comparison
- Validates optimization improvements
- Creates detailed analysis reports
"""

import sys
import os
sys.path.append('/Users/bobbyyo/Projects/algo-fun')

from multi_data_tester import test_on_all_data
from enhanced_eth_momentum_strategy import EnhancedETHMomentumStrategy
import pandas as pd
from datetime import datetime

def main():
    """Run comprehensive testing of the Enhanced ETH Momentum Strategy"""
    
    print("🚀 ENHANCED ETH MOMENTUM STRATEGY - COMPREHENSIVE TESTING")
    print("=" * 80)
    print(f"🕐 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📊 Testing Optimizations:")
    print("   • MACD (8,21,5) vs original (12,26,9)")
    print("   • RSI threshold 65 vs original 70")
    print("   • ATR-based dynamic stops vs fixed 3%")
    print("   • 200-day MA trend filter")
    print("   • Volume confirmation")
    print("   • 2% risk-based position sizing")
    print("=" * 80)
    
    # Test the enhanced strategy on all data sources
    print("\n🔍 Running Enhanced Strategy on All Data Sources...")
    results = test_on_all_data(
        strategy_class=EnhancedETHMomentumStrategy,
        strategy_name="Enhanced_ETH_Momentum_Strategy",
        optimize=False,
        cash=100000,
        commission=0.001,
        verbose=True
    )
    
    if results is not None:
        print("\n✅ Enhanced Strategy Testing Complete!")
        
        # Focus on ETH datasets for analysis
        eth_results = results[results['Data_Source'].str.contains('ETH', case=False, na=False)]
        
        if len(eth_results) > 0:
            print("\n📊 ETH PERFORMANCE SUMMARY - ENHANCED STRATEGY:")
            print("=" * 60)
            
            # Filter valid trading results
            valid_eth = eth_results[eth_results['Return_%'].notna() & (eth_results['Trades'] > 0)]
            
            if len(valid_eth) > 0:
                print(f"📈 Active ETH datasets: {len(valid_eth)}/{len(eth_results)}")
                print(f"💰 Average Return: {valid_eth['Return_%'].mean():.2f}%")
                print(f"🎯 Average Win Rate: {valid_eth['Win_Rate_%'].mean():.2f}%")
                print(f"📉 Average Max Drawdown: {valid_eth['Max_DD_%'].mean():.2f}%")
                print(f"🔄 Total Trades: {valid_eth['Trades'].sum()}")
                print(f"⚡ Average Sharpe Ratio: {valid_eth['Sharpe'].mean():.3f}")
                
                # Best performing dataset
                if len(valid_eth) > 0:
                    best_idx = valid_eth['Return_%'].idxmax()
                    best_dataset = valid_eth.loc[best_idx]
                    print(f"\n🏆 Best Performing Dataset: {best_dataset['Data_Source']}")
                    print(f"   Return: {best_dataset['Return_%']:.2f}%")
                    print(f"   Win Rate: {best_dataset['Win_Rate_%']:.2f}%")
                    print(f"   Max Drawdown: {best_dataset['Max_DD_%']:.2f}%")
                    print(f"   Trades: {best_dataset['Trades']}")
                
                # Show detailed results for each ETH dataset
                print("\n📋 DETAILED ETH RESULTS:")
                print("-" * 80)
                for _, row in valid_eth.iterrows():
                    print(f"{row['Data_Source']:<25} | "
                          f"Return: {row['Return_%']:>7.2f}% | "
                          f"Trades: {row['Trades']:>3.0f} | "
                          f"Win Rate: {row['Win_Rate_%']:>6.2f}% | "
                          f"Max DD: {row['Max_DD_%']:>7.2f}%")
                
            else:
                print("❌ No valid ETH trading results (strategies may need further optimization)")
        
        # Compare with original MACD results if available
        try:
            original_results = pd.read_csv('/Users/bobbyyo/Projects/algo-fun/strategies/results/MACD_Momentum_Strategy.csv')
            
            print("\n🔄 COMPARISON WITH ORIGINAL MACD STRATEGY:")
            print("=" * 60)
            
            # ETH comparison
            original_eth = original_results[original_results['Data_Source'].str.contains('ETH', case=False, na=False)]
            original_eth_valid = original_eth[original_eth['Return_%'].notna() & (original_eth['Trades'] > 0)]
            
            if len(original_eth_valid) > 0 and len(valid_eth) > 0:
                # Find common datasets
                common_datasets = set(valid_eth['Data_Source']).intersection(set(original_eth_valid['Data_Source']))
                
                if common_datasets:
                    print(f"📊 Comparing {len(common_datasets)} common ETH datasets:")
                    print("-" * 80)
                    
                    improvements = 0
                    total_compared = 0
                    
                    for dataset in common_datasets:
                        original_row = original_eth_valid[original_eth_valid['Data_Source'] == dataset].iloc[0]
                        enhanced_row = valid_eth[valid_eth['Data_Source'] == dataset].iloc[0]
                        
                        return_improvement = enhanced_row['Return_%'] - original_row['Return_%']
                        trades_change = enhanced_row['Trades'] - original_row['Trades']
                        
                        if return_improvement > 0:
                            improvements += 1
                        total_compared += 1
                        
                        print(f"{dataset:<25} | "
                              f"Original: {original_row['Return_%']:>7.2f}% | "
                              f"Enhanced: {enhanced_row['Return_%']:>7.2f}% | "
                              f"Δ: {return_improvement:>+7.2f}% | "
                              f"Trades: {original_row['Trades']:>3.0f}→{enhanced_row['Trades']:>3.0f}")
                    
                    improvement_rate = (improvements / total_compared) * 100 if total_compared > 0 else 0
                    print(f"\n📈 Performance Improvements: {improvements}/{total_compared} datasets ({improvement_rate:.1f}%)")
                    
                    # Overall comparison
                    orig_avg_return = original_eth_valid['Return_%'].mean()
                    enh_avg_return = valid_eth['Return_%'].mean()
                    overall_improvement = enh_avg_return - orig_avg_return
                    
                    print(f"💰 Overall Return Improvement: {overall_improvement:+.2f}% "
                          f"({orig_avg_return:.2f}% → {enh_avg_return:.2f}%)")
                
        except FileNotFoundError:
            print("\n⚠️  Original MACD results not found for comparison")
        
        print("\n📄 Detailed results saved to: ./results/Enhanced_ETH_Momentum_Strategy.csv")
        
    else:
        print("❌ No results generated. Check data availability and strategy implementation.")

if __name__ == "__main__":
    main()