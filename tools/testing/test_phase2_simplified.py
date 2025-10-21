"""
🔍 Simplified Phase 2 Strategy Testing
====================================
Testing Phase 2 strategies with simplified versions that bypass trading_functions API issues
"""

import sys
import os
import pandas as pd
import numpy as np
import talib
from datetime import datetime
from backtesting import Backtest, Strategy
import warnings
warnings.filterwarnings('ignore')

# Test datasets
TEST_DATASETS = {
    'BTC-1d': '/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-1d-1000wks-data.csv',
    'BTC-6h': '/Users/bobbyyo/Projects/algo-fun/dataset_files/BTCUSD-6h-500wks-data.csv',
    'ETH-1d': '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv',
    'ETH-6h': '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-6h-200wks-enhanced-data.csv',
}


class SimplifiedMACDMomentum(Strategy):
    """Simplified MACD Momentum Strategy (core logic only)"""
    
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9
    rsi_period = 14
    rsi_overbought = 70
    take_profit = 0.06
    stop_loss = 0.03
    
    def init(self):
        macd_data = talib.MACD(
            self.data.Close,
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow,
            signalperiod=self.macd_signal
        )
        
        self.macd = self.I(lambda x: macd_data[0], self.data.Close)
        self.macd_signal_line = self.I(lambda x: macd_data[1], self.data.Close)
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)
    
    def next(self):
        if len(self.data) < self.macd_slow + 1:
            return
        
        # Current values
        macd_val = self.macd[-1]
        signal_val = self.macd_signal_line[-1]
        prev_macd = self.macd[-2]
        prev_signal = self.macd_signal_line[-2]
        rsi_val = self.rsi[-1]
        
        if pd.isna(macd_val) or pd.isna(signal_val) or pd.isna(rsi_val):
            return
        
        # Position management
        if not self.position:
            # BUY: MACD crosses above signal AND RSI < 70
            if prev_macd <= prev_signal and macd_val > signal_val and rsi_val < self.rsi_overbought:
                self.buy(size=0.95)  # Use 95% of capital
        
        elif self.position:
            # SELL: MACD crosses below signal
            if prev_macd >= prev_signal and macd_val < signal_val:
                self.position.close()
            
            # Stop loss / Take profit
            if len(self.trades) > 0:
                entry_price = self.trades[-1].entry_price
                current_price = self.data.Close[-1]
                pnl_pct = (current_price - entry_price) / entry_price
                
                if pnl_pct <= -self.stop_loss or pnl_pct >= self.take_profit:
                    self.position.close()


class SimplifiedETHRSI(Strategy):
    """Simplified ETH RSI Strategy (core logic only)"""
    
    rsi_period = 14
    rsi_oversold = 30
    rsi_overbought = 70
    take_profit = 0.05
    stop_loss = 0.02
    
    def init(self):
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_period)
    
    def next(self):
        if len(self.data) < self.rsi_period + 1:
            return
        
        rsi_val = self.rsi[-1]
        
        if pd.isna(rsi_val):
            return
        
        # Position management
        if not self.position:
            # BUY: RSI < 30 (oversold)
            if rsi_val < self.rsi_oversold:
                self.buy(size=0.95)
        
        elif self.position:
            # SELL: RSI > 70 (overbought)
            if rsi_val > self.rsi_overbought:
                self.position.close()
            
            # Stop loss / Take profit
            if len(self.trades) > 0:
                entry_price = self.trades[-1].entry_price
                current_price = self.data.Close[-1]
                pnl_pct = (current_price - entry_price) / entry_price
                
                if pnl_pct <= -self.stop_loss or pnl_pct >= self.take_profit:
                    self.position.close()


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
    """Test a single strategy"""
    try:
        print(f"\n{'='*70}")
        print(f"🧪 Testing: {strategy_name} on {dataset_name}")
        print(f"{'='*70}")
        
        df = load_data(file_path)
        print(f"📊 Data: {len(df)} bars")
        
        bt = Backtest(df, strategy_class, cash=10000, commission=0.002)
        stats = bt.run()
        
        # Display results
        print(f"\n🎯 Results:")
        print(f"   Return: {stats['Return [%]']:.2f}%")
        print(f"   Sharpe Ratio: {stats['Sharpe Ratio']:.2f}")
        print(f"   Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"   Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"   Trades: {stats['# Trades']}")
        print(f"   Profit Factor: {stats['Profit Factor']:.2f}")
        
        # Production criteria
        return_ok = stats['Return [%]'] >= 20
        sharpe_ok = stats['Sharpe Ratio'] >= 1.5
        dd_ok = stats['Max. Drawdown [%]'] >= -15
        
        print(f"\n🛡️ Production Criteria:")
        print(f"   Return ≥ 20%: {'✅' if return_ok else '❌'}")
        print(f"   Sharpe ≥ 1.5: {'✅' if sharpe_ok else '❌'}")
        print(f"   Max DD ≥ -15%: {'✅' if dd_ok else '❌'}")
        print(f"   Status: {'✅ PRODUCTION READY' if (return_ok and sharpe_ok and dd_ok) else '⚠️ NOT READY'}")
        
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
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def run_tests():
    """Run all tests"""
    print("\n" + "="*80)
    print("🚀 PHASE 2 STRATEGIES VERIFICATION (Simplified)")
    print("="*80)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💰 Initial Capital: $10,000")
    print(f"📊 Commission: 0.2%")
    
    all_results = []
    
    # Test MACD Momentum
    print("\n" + "="*80)
    print("🎯 MACD Momentum Strategy")
    print("="*80)
    print("Phase 2 Claimed: 1,051% return, 0.927 Sharpe, 78.18% win rate")
    
    for dataset_name, file_path in TEST_DATASETS.items():
        if os.path.exists(file_path):
            result = test_strategy(SimplifiedMACDMomentum, 'MACD Momentum', dataset_name, file_path)
            if result:
                all_results.append(result)
    
    # Test ETH RSI (only on ETH data)
    print("\n" + "="*80)
    print("🎯 ETH RSI Strategy")
    print("="*80)
    print("Phase 2 Claimed: Production Ready (B+ grade)")
    
    for dataset_name, file_path in TEST_DATASETS.items():
        if 'ETH' in dataset_name and os.path.exists(file_path):
            result = test_strategy(SimplifiedETHRSI, 'ETH RSI', dataset_name, file_path)
            if result:
                all_results.append(result)
    
    # Summary
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        print("\n" + "="*80)
        print("📊 ALL RESULTS")
        print("="*80)
        print(results_df.to_string(index=False))
        
        # Save
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'/Users/bobbyyo/Projects/algo-fun/strategies/results/phase2_simplified_verification_{timestamp}.csv'
        results_df.to_csv(output_file, index=False)
        print(f"\n✅ Saved to: {output_file}")
        
        # Verdict
        print("\n" + "="*80)
        print("⚖️ VERIFICATION SUMMARY")
        print("="*80)
        
        # MACD comparison
        macd_results = results_df[results_df['Strategy'] == 'MACD Momentum']
        if len(macd_results) > 0:
            best_macd = macd_results.loc[macd_results['Return_%'].idxmax()]
            print(f"\n📊 MACD Momentum:")
            print(f"   Phase 2 Claimed: 1,051% return")
            print(f"   Best Actual: {best_macd['Return_%']}% on {best_macd['Dataset']}")
            
            if abs(best_macd['Return_%'] - 1051) < 100:
                print(f"   ✅ VERIFIED - Results match within margin")
            else:
                print(f"   ❌ MISMATCH - {abs(best_macd['Return_%'] - 1051):.0f}% difference")
        
        # ETH RSI
        eth_results = results_df[results_df['Strategy'] == 'ETH RSI']
        if len(eth_results) > 0:
            print(f"\n📊 ETH RSI:")
            print(f"   Phase 2 Claimed: Production Ready")
            prod_ready = len(eth_results[eth_results['Production_Ready'] == True])
            print(f"   Actual: {prod_ready}/{len(eth_results)} tests meet criteria")
            
            if prod_ready > 0:
                print(f"   ✅ PRODUCTION READY CONFIRMED")
            else:
                print(f"   ❌ DOES NOT MEET CRITERIA")
        
        # Overall
        prod_ready_total = len(results_df[results_df['Production_Ready'] == True])
        print(f"\n📊 Overall: {prod_ready_total}/{len(results_df)} strategies production-ready")
        
        if prod_ready_total > 0:
            print(f"\n✅ DEPLOYABLE STRATEGIES FOUND:")
            for _, row in results_df[results_df['Production_Ready'] == True].iterrows():
                print(f"   - {row['Strategy']} on {row['Dataset']}")
                print(f"     {row['Return_%']}% return | {row['Sharpe']} Sharpe | {row['Win_Rate_%']}% win rate")
        else:
            print(f"\n❌ NO STRATEGIES MEET PRODUCTION CRITERIA")
        
        return results_df
    else:
        print("\n❌ No results generated")
        return None


if __name__ == "__main__":
    results = run_tests()
    print("\n🌙💫🚀 Testing complete!")

