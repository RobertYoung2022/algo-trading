#!/usr/bin/env python3
"""
MACD Momentum Strategy - Comprehensive Multi-Data Analysis
==========================================================
Analyzes the MACD Momentum Strategy performance across all data sources
and provides detailed insights for strategy optimization.

Analysis includes:
- Cross-asset performance comparison (BTC vs ETH)
- Timeframe analysis (Daily vs Hourly vs 6-hour)
- Data provider reliability comparison
- Strategy optimization recommendations
- Risk assessment across datasets
- Market condition analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_results():
    """Load the MACD strategy results CSV"""
    return pd.read_csv('/Users/bobbyyo/Projects/algo-fun/strategies/results/MACD_Momentum_Strategy.csv')

def analyze_cross_asset_performance(df):
    """Compare BTC vs ETH performance across all available datasets"""
    print("=" * 80)
    print("🪙 CROSS-ASSET PERFORMANCE ANALYSIS (BTC vs ETH)")
    print("=" * 80)
    
    # Separate BTC and ETH datasets
    btc_data = df[df['Data_Source'].str.contains('BTC', case=False, na=False)]
    eth_data = df[df['Data_Source'].str.contains('ETH', case=False, na=False)]
    
    print(f"\n📊 BTC PERFORMANCE SUMMARY:")
    print(f"   • Number of datasets: {len(btc_data)}")
    if len(btc_data) > 0:
        print(f"   • Average Return: {btc_data['Return_%'].mean():.2f}%")
        print(f"   • Average Win Rate: {btc_data['Win_Rate_%'].mean():.2f}%") 
        print(f"   • Average Max Drawdown: {btc_data['Max_DD_%'].mean():.2f}%")
        print(f"   • Average Trades: {btc_data['Trades'].mean():.0f}")
        print(f"   • Best performing dataset: {btc_data.loc[btc_data['Return_%'].idxmax(), 'Data_Source']}")
    
    print(f"\n📊 ETH PERFORMANCE SUMMARY:")
    print(f"   • Number of datasets: {len(eth_data)}")
    if len(eth_data) > 0:
        eth_valid = eth_data[eth_data['Return_%'].notna() & (eth_data['Trades'] > 0)]
        if len(eth_valid) > 0:
            print(f"   • Average Return (valid datasets): {eth_valid['Return_%'].mean():.2f}%")
            print(f"   • Average Win Rate (valid datasets): {eth_valid['Win_Rate_%'].mean():.2f}%")
            print(f"   • Average Max Drawdown (valid datasets): {eth_valid['Max_DD_%'].mean():.2f}%")
            print(f"   • Average Trades (valid datasets): {eth_valid['Trades'].mean():.0f}")
            print(f"   • Best performing dataset: {eth_valid.loc[eth_valid['Return_%'].idxmax(), 'Data_Source']}")
        else:
            print("   • No valid trading datasets (all had 0 trades or missing data)")
            
    return btc_data, eth_data

def analyze_timeframe_performance(df):
    """Analyze performance across different timeframes"""
    print("\n" + "=" * 80)
    print("⏰ TIMEFRAME ANALYSIS")
    print("=" * 80)
    
    # Categorize by timeframe
    daily_data = df[df['Data_Source'].str.contains('1d|daily', case=False, na=False)]
    hourly_data = df[df['Data_Source'].str.contains('1h|hour', case=False, na=False)]
    six_hour_data = df[df['Data_Source'].str.contains('6h', case=False, na=False)]
    
    timeframes = [
        ("📅 DAILY", daily_data),
        ("🕐 HOURLY", hourly_data), 
        ("🕕 6-HOUR", six_hour_data)
    ]
    
    for name, data in timeframes:
        print(f"\n{name} TIMEFRAME:")
        if len(data) > 0:
            valid_data = data[data['Return_%'].notna() & (data['Trades'] > 0)]
            if len(valid_data) > 0:
                print(f"   • Datasets: {len(valid_data)}")
                print(f"   • Average Return: {valid_data['Return_%'].mean():.2f}%")
                print(f"   • Average Win Rate: {valid_data['Win_Rate_%'].mean():.2f}%")
                print(f"   • Average Max Drawdown: {valid_data['Max_DD_%'].mean():.2f}%")
                print(f"   • Average Exposure Time: {valid_data['Exposure_Time_%'].mean():.2f}%")
                print(f"   • Total Trades: {valid_data['Trades'].sum()}")
                print(f"   • Best dataset: {valid_data.loc[valid_data['Return_%'].idxmax(), 'Data_Source']}")
            else:
                print(f"   • Datasets: {len(data)} (but no valid trading data)")
        else:
            print("   • No datasets available")

def analyze_data_provider_reliability(df):
    """Compare performance across different data providers"""
    print("\n" + "=" * 80)
    print("🏢 DATA PROVIDER RELIABILITY ANALYSIS")
    print("=" * 80)
    
    # Categorize by data provider
    providers = {
        "Coinbase": df[df['Data_Source'].str.contains('BTC-1d|BTC-1h|BTC-6h|ETH-1d-5yrs', case=False, na=False)],
        "CoinGecko": df[df['Data_Source'].str.contains('CoinGecko', case=False, na=False)],
        "CoinMarketCap": df[df['Data_Source'].str.contains('CMC', case=False, na=False)],
        "CryptoCompare": df[df['Data_Source'].str.contains('CC-', case=False, na=False)]
    }
    
    for provider, data in providers.items():
        print(f"\n🔍 {provider.upper()}:")
        if len(data) > 0:
            valid_data = data[data['Return_%'].notna() & (data['Trades'] > 0)]
            if len(valid_data) > 0:
                print(f"   • Active datasets: {len(valid_data)}/{len(data)}")
                print(f"   • Average Return: {valid_data['Return_%'].mean():.2f}%")
                print(f"   • Data reliability: {'High' if len(valid_data) == len(data) else 'Moderate'}")
                print(f"   • Total data points: {valid_data['Rows'].sum():,}")
                print(f"   • Time coverage: {valid_data['Duration'].iloc[0] if len(valid_data) > 0 else 'N/A'}")
            else:
                print(f"   • Datasets: {len(data)} (no trading data - insufficient history)")
        else:
            print("   • No datasets available")

def analyze_risk_metrics(df):
    """Detailed risk analysis across all datasets"""
    print("\n" + "=" * 80)
    print("⚠️  COMPREHENSIVE RISK ANALYSIS")
    print("=" * 80)
    
    # Filter valid trading data
    valid_data = df[df['Return_%'].notna() & (df['Trades'] > 0)]
    
    if len(valid_data) > 0:
        print(f"\n📊 RISK METRICS SUMMARY (Valid datasets: {len(valid_data)}):")
        print(f"   • Average Max Drawdown: {valid_data['Max_DD_%'].mean():.2f}%")
        print(f"   • Worst Max Drawdown: {valid_data['Max_DD_%'].min():.2f}%")
        print(f"   • Best Max Drawdown: {valid_data['Max_DD_%'].max():.2f}%")
        print(f"   • Average Sharpe Ratio: {valid_data['Sharpe'].mean():.3f}")
        print(f"   • Average Sortino Ratio: {valid_data['Sortino'].mean():.3f}")
        
        print(f"\n🎯 RISK-ADJUSTED PERFORMANCE:")
        # Risk-adjusted return (return/max_drawdown)
        risk_adj = valid_data['Return_%'] / abs(valid_data['Max_DD_%'])
        print(f"   • Average Risk-Adjusted Return: {risk_adj.mean():.3f}")
        print(f"   • Best Risk-Adjusted Performance: {valid_data.loc[risk_adj.idxmax(), 'Data_Source']}")
        
        # High-risk datasets (>50% drawdown)
        high_risk = valid_data[valid_data['Max_DD_%'] < -50]
        if len(high_risk) > 0:
            print(f"\n⚠️  HIGH RISK DATASETS (>50% drawdown): {len(high_risk)}")
            for _, row in high_risk.iterrows():
                print(f"      • {row['Data_Source']}: {row['Max_DD_%']:.1f}% max drawdown")
    else:
        print("\n❌ No valid trading data for risk analysis")

def analyze_trade_patterns(df):
    """Analyze trading patterns and frequency"""
    print("\n" + "=" * 80) 
    print("📈 TRADING PATTERN ANALYSIS")
    print("=" * 80)
    
    valid_data = df[df['Return_%'].notna() & (df['Trades'] > 0)]
    
    if len(valid_data) > 0:
        print(f"\n🎯 TRADE FREQUENCY & PATTERNS:")
        print(f"   • Total Trades (all datasets): {valid_data['Trades'].sum()}")
        print(f"   • Average Trades per dataset: {valid_data['Trades'].mean():.1f}")
        print(f"   • Most active dataset: {valid_data.loc[valid_data['Trades'].idxmax(), 'Data_Source']} ({valid_data['Trades'].max()} trades)")
        print(f"   • Average Win Rate: {valid_data['Win_Rate_%'].mean():.2f}%")
        print(f"   • Best Win Rate: {valid_data['Win_Rate_%'].max():.2f}% ({valid_data.loc[valid_data['Win_Rate_%'].idxmax(), 'Data_Source']})")
        print(f"   • Average Exposure Time: {valid_data['Exposure_Time_%'].mean():.2f}%")
        
        # Profit Factor Analysis
        print(f"\n💰 PROFITABILITY METRICS:")
        pf_data = valid_data[valid_data['Profit_Factor'] > 0]
        if len(pf_data) > 0:
            print(f"   • Average Profit Factor: {pf_data['Profit_Factor'].mean():.3f}")
            print(f"   • Best Profit Factor: {pf_data['Profit_Factor'].max():.3f} ({pf_data.loc[pf_data['Profit_Factor'].idxmax(), 'Data_Source']})")
            profitable = pf_data[pf_data['Profit_Factor'] > 1.0]
            print(f"   • Profitable datasets (PF > 1.0): {len(profitable)}/{len(valid_data)}")
        
    else:
        print("\n❌ No valid trading data for pattern analysis")

def generate_optimization_recommendations(df):
    """Generate actionable strategy optimization recommendations"""
    print("\n" + "=" * 80)
    print("🚀 STRATEGY OPTIMIZATION RECOMMENDATIONS")
    print("=" * 80)
    
    valid_data = df[df['Return_%'].notna() & (df['Trades'] > 0)]
    
    # Identify best performing dataset
    if len(valid_data) > 0:
        best_dataset = valid_data.loc[valid_data['Return_%'].idxmax()]
        worst_dataset = valid_data.loc[valid_data['Return_%'].idxmin()]
        
        print(f"\n🏆 BEST PERFORMING SETUP:")
        print(f"   • Dataset: {best_dataset['Data_Source']}")
        print(f"   • Return: {best_dataset['Return_%']:.2f}%")
        print(f"   • Win Rate: {best_dataset['Win_Rate_%']:.2f}%")
        print(f"   • Max Drawdown: {best_dataset['Max_DD_%']:.2f}%")
        print(f"   • Trades: {best_dataset['Trades']}")
        
        print(f"\n📉 WORST PERFORMING SETUP:")
        print(f"   • Dataset: {worst_dataset['Data_Source']}")
        print(f"   • Return: {worst_dataset['Return_%']:.2f}%")
        print(f"   • Max Drawdown: {worst_dataset['Max_DD_%']:.2f}%")
    
    print(f"\n💡 KEY RECOMMENDATIONS:")
    
    # Analysis based on results
    all_negative = valid_data['Return_%'].max() <= 0 if len(valid_data) > 0 else True
    
    if all_negative:
        print("   1. 🔴 CRITICAL: Strategy shows negative returns across all datasets")
        print("      • Consider revising entry conditions - current RSI filter may be too restrictive")
        print("      • Test alternative MACD parameters (e.g., 8,21,5 for faster signals)")
        print("      • Consider adding trend filter (e.g., 200-day MA) to avoid choppy markets")
        print("      • Reduce stop-loss from 3% to 1-2% to limit individual trade losses")
        print("      • Consider implementing trailing stops instead of fixed take-profit")
    
    # High drawdown analysis
    high_dd_count = len(valid_data[valid_data['Max_DD_%'] < -50]) if len(valid_data) > 0 else 0
    if high_dd_count > 0:
        print("   2. ⚠️  RISK MANAGEMENT:")
        print("      • Multiple datasets show >50% drawdown - strategy is too risky")
        print("      • Implement position sizing based on volatility (e.g., ATR-based)")
        print("      • Add maximum concurrent positions limit")
        print("      • Consider portfolio heat limits (max 6-8% total portfolio risk)")
    
    # Low trade frequency
    avg_trades = valid_data['Trades'].mean() if len(valid_data) > 0 else 0
    if avg_trades < 50:  # Assuming datasets are long-term
        print("   3. 📊 SIGNAL FREQUENCY:")
        print("      • Low trade frequency suggests over-filtering")
        print("      • Test removing RSI filter in trending markets")
        print("      • Consider shorter timeframes for more opportunities")
        print("      • Test MACD histogram for earlier entry signals")
    
    # Timeframe recommendations
    daily_performance = df[df['Data_Source'].str.contains('1d|daily', case=False, na=False)]
    hourly_performance = df[df['Data_Source'].str.contains('1h', case=False, na=False)]
    
    print("   4. ⏰ TIMEFRAME OPTIMIZATION:")
    if len(daily_performance) > len(hourly_performance):
        print("      • Daily timeframes show more consistent data coverage")
        print("      • Focus optimization efforts on daily charts first")
    print("      • Avoid intraday timeframes during low-volatility periods")
    print("      • Consider session-based filters (avoid low-volume hours)")
    
    print("   5. 🔧 PARAMETER OPTIMIZATION:")
    print("      • Test MACD parameters: (8,21,5), (5,13,8) for faster signals")
    print("      • Vary RSI threshold: test 60, 65, 75 levels")
    print("      • Dynamic stop-loss based on ATR (e.g., 2*ATR)")
    print("      • Test different take-profit ratios (3%, 4%, 8%)")

def main():
    """Main analysis function"""
    print("🔍 MACD MOMENTUM STRATEGY - COMPREHENSIVE ANALYSIS")
    print("=" * 80)
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Data Sources: Multi-provider (Coinbase, CoinGecko, CoinMarketCap, CryptoCompare)")
    print("Strategy: MACD (12,26,9) + RSI Filter + 3% SL / 6% TP")
    
    # Load results
    try:
        df = load_results()
        print(f"✅ Loaded results: {len(df)} datasets analyzed")
    except Exception as e:
        print(f"❌ Error loading results: {e}")
        return
    
    # Perform all analyses
    btc_data, eth_data = analyze_cross_asset_performance(df)
    analyze_timeframe_performance(df)
    analyze_data_provider_reliability(df)
    analyze_risk_metrics(df)
    analyze_trade_patterns(df)
    generate_optimization_recommendations(df)
    
    # Summary
    print("\n" + "=" * 80)
    print("📋 EXECUTIVE SUMMARY")
    print("=" * 80)
    
    valid_data = df[df['Return_%'].notna() & (df['Trades'] > 0)]
    if len(valid_data) > 0:
        total_return = valid_data['Return_%'].mean()
        total_trades = valid_data['Trades'].sum()
        avg_win_rate = valid_data['Win_Rate_%'].mean()
        
        print(f"📊 Overall Performance:")
        print(f"   • Active datasets: {len(valid_data)}/{len(df)}")
        print(f"   • Average Return: {total_return:.2f}%")
        print(f"   • Total Trades: {total_trades}")
        print(f"   • Average Win Rate: {avg_win_rate:.2f}%")
        print(f"   • Strategy Status: {'🔴 NEEDS OPTIMIZATION' if total_return < 0 else '🟡 MODERATE' if total_return < 10 else '🟢 PERFORMING'}")
        
        print(f"\n🎯 Next Steps:")
        print("   1. Implement recommended parameter changes")
        print("   2. Test on additional market conditions") 
        print("   3. Add dynamic position sizing")
        print("   4. Backtest with transaction costs included")
        print("   5. Consider ensemble approach with multiple strategies")
    else:
        print("❌ No valid trading data - strategy needs fundamental revision")
    
    print(f"\n📄 Detailed results saved: ./results/MACD_Momentum_Strategy.csv")

if __name__ == "__main__":
    main()