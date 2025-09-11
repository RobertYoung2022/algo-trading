"""
Test Enhanced ETH Momentum Strategy - Final Version on All Datasets
==================================================================

This script tests the final working Enhanced ETH Momentum Strategy on all 
available datasets and provides comprehensive performance analysis.
"""

import sys
import os
sys.path.append('/Users/bobbyyo/Projects/algo-fun')

from multi_data_tester import test_on_all_data
from enhanced_eth_momentum_final import EnhancedETHMomentumFinal
import pandas as pd
from datetime import datetime

def compare_with_original(enhanced_results):
    """Compare with original MACD results"""
    try:
        original_file = '/Users/bobbyyo/Projects/algo-fun/strategies/results/MACD_Momentum_Strategy.csv'
        original_results = pd.read_csv(original_file)
        
        print("\n🔄 PERFORMANCE COMPARISON: ENHANCED vs ORIGINAL MACD")
        print("=" * 80)
        
        # Focus on ETH datasets
        enhanced_eth = enhanced_results[enhanced_results['Data_Source'].str.contains('ETH', case=False, na=False)]
        original_eth = original_results[original_results['Data_Source'].str.contains('ETH', case=False, na=False)]
        
        # Filter valid results (with trades)
        enhanced_valid = enhanced_eth[enhanced_eth['Trades'] > 0]
        original_valid = original_eth[original_eth['Trades'] > 0]
        
        print(f"📊 Enhanced Strategy - ETH Datasets:")
        print(f"   Active datasets: {len(enhanced_valid)}/{len(enhanced_eth)}")
        if len(enhanced_valid) > 0:
            print(f"   Average Return: {enhanced_valid['Return_%'].mean():.2f}%")
            print(f"   Average Win Rate: {enhanced_valid['Win_Rate_%'].mean():.2f}%")
            print(f"   Average Max DD: {enhanced_valid['Max_DD_%'].mean():.2f}%")
            print(f"   Total Trades: {enhanced_valid['Trades'].sum()}")
        
        print(f"\n📊 Original MACD - ETH Datasets:")
        print(f"   Active datasets: {len(original_valid)}/{len(original_eth)}")
        if len(original_valid) > 0:
            print(f"   Average Return: {original_valid['Return_%'].mean():.2f}%")
            print(f"   Average Win Rate: {original_valid['Win_Rate_%'].mean():.2f}%")
            print(f"   Average Max DD: {original_valid['Max_DD_%'].mean():.2f}%")
            print(f"   Total Trades: {original_valid['Trades'].sum()}")
        
        # Compare common datasets
        if len(enhanced_valid) > 0 and len(original_valid) > 0:
            common_datasets = set(enhanced_valid['Data_Source']).intersection(set(original_valid['Data_Source']))
            
            if common_datasets:
                print(f"\n📋 DATASET-BY-DATASET COMPARISON ({len(common_datasets)} common datasets):")
                print("-" * 80)
                
                improvements = 0
                total_comparisons = 0
                
                for dataset in common_datasets:
                    enh = enhanced_valid[enhanced_valid['Data_Source'] == dataset].iloc[0]
                    orig = original_valid[original_valid['Data_Source'] == dataset].iloc[0]
                    
                    return_improvement = enh['Return_%'] - orig['Return_%']
                    trades_change = enh['Trades'] - orig['Trades']
                    
                    if return_improvement > 0:
                        improvements += 1
                    total_comparisons += 1
                    
                    status = "🟢" if return_improvement > 0 else "🔴" if return_improvement < -1 else "🟡"
                    
                    print(f"{status} {dataset:<25} | "
                          f"Enhanced: {enh['Return_%']:>7.2f}% ({enh['Trades']:>2.0f} trades) | "
                          f"Original: {orig['Return_%']:>7.2f}% ({orig['Trades']:>2.0f} trades) | "
                          f"Δ: {return_improvement:>+7.2f}%")
                
                improvement_rate = (improvements / total_comparisons) * 100 if total_comparisons > 0 else 0
                print(f"\n📈 Improvement Rate: {improvements}/{total_comparisons} datasets ({improvement_rate:.1f}%)")
                
                # Overall metrics
                enh_avg = enhanced_valid['Return_%'].mean()
                orig_avg = original_valid['Return_%'].mean()
                overall_improvement = enh_avg - orig_avg
                
                print(f"💰 Overall Return Change: {overall_improvement:+.2f}% ({orig_avg:.2f}% → {enh_avg:.2f}%)")
                
                # Trade frequency
                enh_trades = enhanced_valid['Trades'].mean()
                orig_trades = original_valid['Trades'].mean()
                trade_improvement = enh_trades - orig_trades
                
                print(f"🔄 Trade Frequency Change: {trade_improvement:+.1f} trades ({orig_trades:.1f} → {enh_trades:.1f})")
        
    except FileNotFoundError:
        print("\n⚠️  Original MACD results not found for comparison")
    except Exception as e:
        print(f"\n❌ Error during comparison: {e}")

def analyze_performance_by_market_conditions(results):
    """Analyze performance across different market conditions"""
    print("\n📊 PERFORMANCE BY MARKET CONDITIONS")
    print("=" * 60)
    
    # Filter ETH results with trades
    eth_results = results[results['Data_Source'].str.contains('ETH', case=False, na=False)]
    valid_results = eth_results[eth_results['Trades'] > 0]
    
    if len(valid_results) > 0:
        # Categorize by performance
        profitable = valid_results[valid_results['Return_%'] > 0]
        break_even = valid_results[(valid_results['Return_%'] >= -2) & (valid_results['Return_%'] <= 2)]
        losses = valid_results[valid_results['Return_%'] < -2]
        
        print(f"🟢 Profitable periods: {len(profitable)}/{len(valid_results)} datasets")
        print(f"🟡 Break-even periods: {len(break_even)}/{len(valid_results)} datasets") 
        print(f"🔴 Loss periods: {len(losses)}/{len(valid_results)} datasets")
        
        # Performance distribution
        returns = valid_results['Return_%']
        print(f"\n📈 Return Distribution:")
        print(f"   Best: {returns.max():.2f}%")
        print(f"   Worst: {returns.min():.2f}%")
        print(f"   Median: {returns.median():.2f}%")
        print(f"   Average: {returns.mean():.2f}%")
        print(f"   Std Dev: {returns.std():.2f}%")
        
        # Risk metrics
        drawdowns = valid_results['Max_DD_%']
        print(f"\n⚠️  Risk Metrics:")
        print(f"   Average Max DD: {drawdowns.mean():.2f}%")
        print(f"   Worst Max DD: {drawdowns.min():.2f}%")
        print(f"   Best Max DD: {drawdowns.max():.2f}%")
        
        # Sharpe analysis
        sharpe_values = valid_results['Sharpe'][valid_results['Sharpe'].notna()]
        if len(sharpe_values) > 0:
            print(f"   Average Sharpe: {sharpe_values.mean():.3f}")
            print(f"   Datasets with Sharpe > 0.5: {len(sharpe_values[sharpe_values > 0.5])}/{len(sharpe_values)}")

def generate_optimization_recommendations(results):
    """Generate specific optimization recommendations"""
    print("\n💡 OPTIMIZATION RECOMMENDATIONS")
    print("=" * 60)
    
    eth_results = results[results['Data_Source'].str.contains('ETH', case=False, na=False)]
    valid_results = eth_results[eth_results['Trades'] > 0]
    
    if len(valid_results) > 0:
        avg_return = valid_results['Return_%'].mean()
        avg_sharpe = valid_results['Sharpe'][valid_results['Sharpe'].notna()].mean()
        avg_trades = valid_results['Trades'].mean()
        avg_win_rate = valid_results['Win_Rate_%'].mean()
        avg_drawdown = valid_results['Max_DD_%'].mean()
        
        print(f"📊 Current Performance Summary:")
        print(f"   Average Return: {avg_return:.2f}%")
        print(f"   Average Sharpe: {avg_sharpe:.3f}")
        print(f"   Average Trades: {avg_trades:.1f}")
        print(f"   Average Win Rate: {avg_win_rate:.2f}%")
        print(f"   Average Max DD: {avg_drawdown:.2f}%")
        
        print(f"\n🔧 Specific Recommendations:")
        
        # Return optimization
        if avg_return < 10:
            print("   📈 RETURN OPTIMIZATION:")
            print("      • Test faster MACD parameters (5,13,5) for more signals")
            print("      • Consider adding momentum filter (Price > 20-day MA)")
            print("      • Test RSI threshold 65-75 range for optimal entry timing")
        
        # Risk management
        if avg_drawdown < -40:
            print("   ⚠️  RISK MANAGEMENT:")
            print("      • Reduce position size to 80% or implement volatility-based sizing")
            print("      • Add maximum drawdown stop (exit all positions if portfolio DD > 30%)")
            print("      • Consider shorter ATR multiplier (1.5x instead of 2.0x)")
        
        # Win rate improvement
        if avg_win_rate < 40:
            print("   🎯 WIN RATE IMPROVEMENT:")
            print("      • Add volume confirmation (Volume > 1.2x average)")
            print("      • Test trend filter with 100-day MA instead of 50-day")
            print("      • Consider multiple timeframe confirmation")
        
        # Trade frequency
        if avg_trades < 20:
            print("   🔄 TRADE FREQUENCY:")
            print("      • Relax entry conditions slightly")
            print("      • Add alternative entry signals (MACD histogram, RSI divergence)")
            print("      • Test on shorter timeframes (4h, 1h)")
        
        # Sharpe improvement
        if avg_sharpe < 0.5:
            print("   ⚡ RISK-ADJUSTED RETURNS:")
            print("      • Implement trailing stops more aggressively")
            print("      • Add profit-taking rules (take 50% at 10% profit)")
            print("      • Consider market regime filters (bull/bear detection)")
        
        # Next steps
        print(f"\n🚀 NEXT STEPS:")
        print("   1. Implement parameter optimization framework")
        print("   2. Test top 3 recommendations on validation set")
        print("   3. Create ensemble of best-performing variations")
        print("   4. Add transaction cost analysis")
        print("   5. Develop live trading implementation")

def main():
    """Run comprehensive testing and analysis"""
    
    print("🚀 ENHANCED ETH MOMENTUM STRATEGY - FINAL COMPREHENSIVE TESTING")
    print("=" * 90)
    print(f"🕐 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Strategy: Enhanced ETH Momentum (MACD 8,21,5 + RSI + ATR stops)")
    print("=" * 90)
    
    # Run comprehensive testing
    print("\n🔍 Testing Enhanced Strategy on All Data Sources...")
    results = test_on_all_data(
        strategy_class=EnhancedETHMomentumFinal,
        strategy_name="Enhanced_ETH_Momentum_Final",
        optimize=False,
        cash=100000,
        commission=0.001,
        verbose=True
    )
    
    if results is not None:
        print("\n✅ Testing Complete! Generating Analysis...")
        
        # Compare with original
        compare_with_original(results)
        
        # Analyze by market conditions
        analyze_performance_by_market_conditions(results)
        
        # Generate recommendations
        generate_optimization_recommendations(results)
        
        print(f"\n📄 Full results saved to: ./results/Enhanced_ETH_Momentum_Final.csv")
        print("🎯 Ready for Phase 4: Optimization Framework Development")
        
    else:
        print("❌ Testing failed. Check strategy implementation.")

if __name__ == "__main__":
    main()