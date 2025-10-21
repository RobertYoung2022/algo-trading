"""
🔍 Phase 2 "Production Ready" Strategies Testing
==============================================
Testing the strategies claimed to be production-ready in Phase 2 report:
1. MACD Momentum Strategy 
2. ETH RSI Strategy

These were reported as:
- MACD: 1,051% return, 0.927 Sharpe, -29.38% DD, 78.18% win rate
- ETH RSI: Production ready for ETH trading

Let's verify these claims with fresh backtests.
"""

import sys
import os
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '/Users/bobbyyo/Projects/algo-fun')
sys.path.insert(0, '/Users/bobbyyo/Projects/algo-fun/archive/strategies_backup')

try:
    from archive.strategies_backup.analysis.macd_momentum_strategy import MACDMomentumStrategy
    MACD_AVAILABLE = True
    print("✅ MACD Momentum Strategy loaded")
except Exception as e:
    MACD_AVAILABLE = False
    print(f"❌ MACD Momentum Strategy not available: {e}")

try:
    from archive.strategies_backup.eth_strategies.eth_rsi_strategy import ETHRSIStrategy
    ETH_RSI_AVAILABLE = True
    print("✅ ETH RSI Strategy loaded")
except Exception as e:
    ETH_RSI_AVAILABLE = False
    print(f"❌ ETH RSI Strategy not available: {e}")

from backtesting import Backtest

# Test datasets
TEST_DATASETS = {
    'BTC-1d': '/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv',
    'BTC-6h': '/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-6h-500wks-data.csv',
    'ETH-1d': '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv',
    'ETH-6h': '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-6h-200wks-enhanced-data.csv',
}


def load_data(file_path):
    """Load and prepare data"""
    df = pd.read_csv(file_path)
    
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


def test_strategy(strategy_class, strategy_name, dataset_name, file_path):
    """Test a single strategy on a dataset"""
    try:
        print(f"\n{'='*60}")
        print(f"🧪 Testing: {strategy_name} on {dataset_name}")
        print(f"{'='*60}")
        
        df = load_data(file_path)
        print(f"📊 Data: {len(df)} bars from {df.index[0]} to {df.index[-1]}")
        
        bt = Backtest(df, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()
        
        # Display key metrics
        print(f"\n🎯 Results:")
        print(f"   Return: {stats['Return [%]']:.2f}%")
        print(f"   Sharpe Ratio: {stats['Sharpe Ratio']:.2f}")
        print(f"   Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"   Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"   Trades: {stats['# Trades']}")
        print(f"   Profit Factor: {stats['Profit Factor']:.2f}")
        
        # Production criteria check
        return_ok = stats['Return [%]'] >= 20
        sharpe_ok = stats['Sharpe Ratio'] >= 1.5
        dd_ok = stats['Max. Drawdown [%]'] >= -15
        
        print(f"\n🛡️ Production Readiness:")
        print(f"   Return ≥ 20%: {'✅' if return_ok else '❌'} ({stats['Return [%]']:.2f}%)")
        print(f"   Sharpe ≥ 1.5: {'✅' if sharpe_ok else '❌'} ({stats['Sharpe Ratio']:.2f})")
        print(f"   Max DD ≥ -15%: {'✅' if dd_ok else '❌'} ({stats['Max. Drawdown [%]']:.2f}%)")
        
        if return_ok and sharpe_ok and dd_ok:
            print(f"   ✅ PRODUCTION READY")
        else:
            print(f"   ⚠️ NOT PRODUCTION READY")
        
        return {
            'Strategy': strategy_name,
            'Dataset': dataset_name,
            'Return_%': round(stats['Return [%]'], 2),
            'Sharpe': round(stats['Sharpe Ratio'], 2) if pd.notna(stats['Sharpe Ratio']) else 0.0,
            'Max_DD_%': round(stats['Max. Drawdown [%]'], 2),
            'Win_Rate_%': round(stats['Win Rate [%]'], 2) if pd.notna(stats['Win Rate [%]']) else 0.0,
            'Trades': int(stats['# Trades']),
            'Profit_Factor': round(stats['Profit Factor'], 2) if pd.notna(stats['Profit Factor']) else 0.0,
            'Production_Ready': return_ok and sharpe_ok and dd_ok
        }
        
    except Exception as e:
        print(f"❌ Error testing {strategy_name} on {dataset_name}: {e}")
        import traceback
        traceback.print_exc()
        return None


def run_phase2_tests():
    """Run all Phase 2 strategy tests"""
    print("\n" + "="*80)
    print("🚀 PHASE 2 PRODUCTION-READY STRATEGIES VERIFICATION")
    print("="*80)
    print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💰 Initial Capital: $10,000")
    print(f"📊 Commission: 0.2%")
    
    all_results = []
    
    # Test MACD Momentum Strategy
    if MACD_AVAILABLE:
        print("\n" + "="*80)
        print("🎯 TESTING: MACD Momentum Strategy")
        print("="*80)
        print("Phase 2 Claimed Performance:")
        print("   - Return: 1,051%")
        print("   - Sharpe: 0.927")
        print("   - Max DD: -29.38%")
        print("   - Win Rate: 78.18%")
        print("   - Status: PRODUCTION READY ✅")
        
        for dataset_name, file_path in TEST_DATASETS.items():
            if not os.path.exists(file_path):
                print(f"⚠️ Skipping {dataset_name} - file not found")
                continue
            
            result = test_strategy(MACDMomentumStrategy, 'MACD Momentum', dataset_name, file_path)
            if result:
                all_results.append(result)
    
    # Test ETH RSI Strategy
    if ETH_RSI_AVAILABLE:
        print("\n" + "="*80)
        print("🎯 TESTING: ETH RSI Strategy")
        print("="*80)
        print("Phase 2 Claimed Performance:")
        print("   - Risk Grade: B+")
        print("   - Production Status: READY (5/6 criteria passed)")
        print("   - Status: PRODUCTION READY ✅")
        print("   - Best Timeframes: 1d, 6h")
        
        # Test only on ETH data
        for dataset_name, file_path in TEST_DATASETS.items():
            if 'ETH' in dataset_name:
                if not os.path.exists(file_path):
                    print(f"⚠️ Skipping {dataset_name} - file not found")
                    continue
                
                result = test_strategy(ETHRSIStrategy, 'ETH RSI', dataset_name, file_path)
                if result:
                    all_results.append(result)
    
    # Summary
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE RESULTS")
        print("="*80)
        print(results_df.to_string(index=False))
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'/Users/bobbyyo/Projects/algo-fun/strategies/results/phase2_strategies_verification_{timestamp}.csv'
        results_df.to_csv(output_file, index=False)
        print(f"\n✅ Results saved to: {output_file}")
        
        # Comparison with Phase 2 claims
        print("\n" + "="*80)
        print("🔍 PHASE 2 CLAIMS VERIFICATION")
        print("="*80)
        
        # Check MACD
        macd_results = results_df[results_df['Strategy'] == 'MACD Momentum']
        if len(macd_results) > 0:
            print("\n📊 MACD Momentum Strategy:")
            print(f"   Phase 2 Claimed: 1,051% return, 0.927 Sharpe, -29% DD, 78% win rate")
            print(f"   Actual Results:")
            for _, row in macd_results.iterrows():
                print(f"   - {row['Dataset']}: {row['Return_%']}% return, {row['Sharpe']} Sharpe, {row['Win_Rate_%']}% win rate")
            
            # Best result
            best_macd = macd_results.loc[macd_results['Return_%'].idxmax()]
            match = abs(best_macd['Return_%'] - 1051) < 100  # Within 100% of claimed
            print(f"\n   Verification: {'✅ CONFIRMED' if match else '❌ DOES NOT MATCH'}")
            if not match:
                print(f"   ⚠️ Discrepancy: Claimed 1,051% vs Actual {best_macd['Return_%']}%")
        
        # Check ETH RSI
        eth_rsi_results = results_df[results_df['Strategy'] == 'ETH RSI']
        if len(eth_rsi_results) > 0:
            print("\n📊 ETH RSI Strategy:")
            print(f"   Phase 2 Claimed: Production Ready (B+ grade)")
            print(f"   Actual Results:")
            for _, row in eth_rsi_results.iterrows():
                print(f"   - {row['Dataset']}: {row['Return_%']}% return, {row['Sharpe']} Sharpe")
            
            production_ready_count = len(eth_rsi_results[eth_rsi_results['Production_Ready'] == True])
            print(f"\n   Verification: {production_ready_count}/{len(eth_rsi_results)} tests meet production criteria")
            if production_ready_count > 0:
                print(f"   ✅ PRODUCTION READY CONFIRMED")
            else:
                print(f"   ❌ DOES NOT MEET PRODUCTION CRITERIA")
        
        # Overall verdict
        print("\n" + "="*80)
        print("⚖️ FINAL VERDICT")
        print("="*80)
        
        production_ready = results_df[results_df['Production_Ready'] == True]
        print(f"\n📊 Production Ready Count: {len(production_ready)}/{len(results_df)}")
        
        if len(production_ready) > 0:
            print(f"\n✅ VERIFIED PRODUCTION-READY STRATEGIES:")
            for _, row in production_ready.iterrows():
                print(f"   - {row['Strategy']} on {row['Dataset']}")
                print(f"     Return: {row['Return_%']}% | Sharpe: {row['Sharpe']} | DD: {row['Max_DD_%']}%")
        else:
            print(f"\n❌ NO STRATEGIES MEET PRODUCTION CRITERIA")
            print(f"   Phase 2 report may be outdated or using different data/parameters")
        
        return results_df
    else:
        print("\n❌ No test results generated")
        return None


if __name__ == "__main__":
    results = run_phase2_tests()
    print("\n🌙💫🚀 Phase 2 verification complete!")

