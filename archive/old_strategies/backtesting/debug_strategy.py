"""
Strategy Debug Tool - Analyze why the Enhanced ETH Strategy isn't generating trades
=================================================================================

This debug script helps identify issues with the Enhanced ETH Strategy by:
- Loading ETH data and checking indicator values
- Analyzing entry condition frequency
- Identifying which filters are preventing trades
- Suggesting parameter adjustments
"""

import pandas as pd
import numpy as np
import talib
import matplotlib.pyplot as plt

def load_eth_data():
    """Load and prepare ETH data for analysis"""
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    # Load data
    data = pd.read_csv(data_file)
    
    # Find date column
    date_cols = [col for col in data.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
    if date_cols:
        date_col = date_cols[0]
        data[date_col] = pd.to_datetime(data[date_col])
        data = data.set_index(date_col)
    
    # Standardize columns
    data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    data = data.sort_index().dropna()
    
    print(f"📊 Loaded {len(data)} bars from {data.index[0]} to {data.index[-1]}")
    print(f"💰 Price range: ${data['Close'].min():.2f} - ${data['Close'].max():.2f}")
    
    return data

def calculate_indicators(data):
    """Calculate all indicators and add debug info"""
    close = data['Close'].values
    high = data['High'].values
    low = data['Low'].values
    volume = data['Volume'].values
    
    # Enhanced strategy parameters
    macd_fast = 8
    macd_slow = 21
    macd_signal = 5
    rsi_period = 14
    rsi_threshold = 65
    ma_trend_period = 200
    atr_period = 14
    volume_sma_period = 20
    
    # Calculate indicators
    print("📈 Calculating indicators...")
    
    # MACD
    macd_line, macd_signal_line, macd_histogram = talib.MACD(
        close, fastperiod=macd_fast, slowperiod=macd_slow, signalperiod=macd_signal
    )
    
    # RSI
    rsi = talib.RSI(close, timeperiod=rsi_period)
    
    # Moving averages
    ma_trend = talib.SMA(close, timeperiod=ma_trend_period)
    ma_50 = talib.SMA(close, timeperiod=50)
    
    # ATR
    atr = talib.ATR(high, low, close, timeperiod=atr_period)
    
    # Volume SMA
    volume_sma = talib.SMA(volume, timeperiod=volume_sma_period)
    
    # Add to dataframe
    data['macd'] = macd_line
    data['macd_signal'] = macd_signal_line
    data['macd_histogram'] = macd_histogram
    data['rsi'] = rsi
    data['ma_trend'] = ma_trend
    data['ma_50'] = ma_50
    data['atr'] = atr
    data['volume_sma'] = volume_sma
    
    # Calculate entry conditions
    data['macd_cross'] = ((data['macd'].shift(1) <= data['macd_signal'].shift(1)) & 
                         (data['macd'] > data['macd_signal']))
    
    data['rsi_filter'] = data['rsi'] < rsi_threshold
    data['trend_filter'] = data['Close'] > data['ma_trend']
    data['momentum_filter'] = data['Close'] > data['ma_50']
    data['volume_filter'] = data['Volume'] > data['volume_sma']
    data['macd_positive'] = data['macd'] > 0
    
    # Combined entry signal
    data['entry_signal'] = (data['macd_cross'] & 
                           data['rsi_filter'] & 
                           data['trend_filter'] & 
                           data['momentum_filter'] & 
                           data['volume_filter'] & 
                           data['macd_positive'])
    
    return data

def analyze_conditions(data):
    """Analyze each entry condition to see what's blocking trades"""
    print("\n🔍 ENTRY CONDITION ANALYSIS")
    print("=" * 60)
    
    # Remove NaN rows for analysis
    analysis_data = data.dropna()
    total_bars = len(analysis_data)
    
    print(f"📊 Total valid bars for analysis: {total_bars}")
    
    # Individual condition analysis
    conditions = {
        'MACD Bullish Cross': analysis_data['macd_cross'].sum(),
        'RSI < 65': analysis_data['rsi_filter'].sum(),
        'Price > 200-day MA': analysis_data['trend_filter'].sum(),
        'Price > 50-day MA': analysis_data['momentum_filter'].sum(),
        'Volume > Average': analysis_data['volume_filter'].sum(),
        'MACD > 0': analysis_data['macd_positive'].sum(),
        'ALL CONDITIONS': analysis_data['entry_signal'].sum()
    }
    
    print("\n📈 Individual Condition Frequency:")
    for condition, count in conditions.items():
        percentage = (count / total_bars) * 100
        print(f"   {condition:<20}: {count:>4} bars ({percentage:>5.1f}%)")
    
    print(f"\n🎯 Final Entry Signals: {conditions['ALL CONDITIONS']} ({(conditions['ALL CONDITIONS']/total_bars)*100:.1f}%)")
    
    # Show when each condition fails
    print("\n❌ What's blocking trades (when MACD crosses but other conditions fail):")
    macd_cross_bars = analysis_data[analysis_data['macd_cross']]
    if len(macd_cross_bars) > 0:
        cross_analysis = {
            'RSI too high (≥65)': (~macd_cross_bars['rsi_filter']).sum(),
            'Price below 200-day MA': (~macd_cross_bars['trend_filter']).sum(),
            'Price below 50-day MA': (~macd_cross_bars['momentum_filter']).sum(),
            'Volume below average': (~macd_cross_bars['volume_filter']).sum(),
            'MACD below zero': (~macd_cross_bars['macd_positive']).sum()
        }
        
        total_crosses = len(macd_cross_bars)
        for reason, count in cross_analysis.items():
            percentage = (count / total_crosses) * 100
            print(f"   {reason:<25}: {count:>3}/{total_crosses} crosses ({percentage:>5.1f}%)")
    
    return analysis_data

def suggest_optimizations(data):
    """Suggest parameter optimizations based on analysis"""
    print("\n💡 OPTIMIZATION SUGGESTIONS")
    print("=" * 60)
    
    analysis_data = data.dropna()
    
    # RSI analysis
    rsi_values = analysis_data['rsi']
    print(f"📊 RSI Statistics:")
    print(f"   Average RSI: {rsi_values.mean():.1f}")
    print(f"   RSI > 65: {(rsi_values > 65).sum()} bars ({(rsi_values > 65).mean()*100:.1f}%)")
    print(f"   RSI > 70: {(rsi_values > 70).sum()} bars ({(rsi_values > 70).mean()*100:.1f}%)")
    print(f"   RSI > 75: {(rsi_values > 75).sum()} bars ({(rsi_values > 75).mean()*100:.1f}%)")
    
    # Trend analysis
    trend_alignment = analysis_data['trend_filter'].mean() * 100
    momentum_alignment = analysis_data['momentum_filter'].mean() * 100
    
    print(f"\n📈 Trend Analysis:")
    print(f"   Price above 200-day MA: {trend_alignment:.1f}% of time")
    print(f"   Price above 50-day MA: {momentum_alignment:.1f}% of time")
    
    # Volume analysis
    volume_above_avg = analysis_data['volume_filter'].mean() * 100
    print(f"   Volume above average: {volume_above_avg:.1f}% of time")
    
    # MACD analysis
    macd_positive_pct = analysis_data['macd_positive'].mean() * 100
    macd_crosses = analysis_data['macd_cross'].sum()
    
    print(f"\n🔄 MACD Analysis:")
    print(f"   MACD above zero: {macd_positive_pct:.1f}% of time")
    print(f"   MACD crosses: {macd_crosses} total")
    
    print(f"\n💡 Recommendations:")
    
    if conditions_too_strict(analysis_data):
        print("   🔴 STRATEGY TOO RESTRICTIVE:")
        print("      • Consider relaxing RSI threshold to 70-75")
        print("      • Remove MACD positive requirement in bear markets")
        print("      • Use 100-day MA instead of 200-day for trend filter")
        print("      • Make volume filter optional (use as confirmation only)")
        
    if trend_alignment < 50:
        print("   📉 BEARISH MARKET DETECTED:")
        print("      • Consider shorter-term trend filter (50-day instead of 200-day)")
        print("      • Add short-selling capability")
        print("      • Focus on relative strength rather than absolute trends")
    
    if macd_crosses < 10:
        print("   ⚡ LOW SIGNAL FREQUENCY:")
        print("      • Try even faster MACD parameters (5,13,5)")
        print("      • Consider MACD histogram crossover signals")
        print("      • Add alternative entry signals (RSI divergence, etc.)")

def conditions_too_strict(data):
    """Check if conditions are too restrictive"""
    entry_signals = data['entry_signal'].sum()
    macd_crosses = data['macd_cross'].sum()
    
    # If less than 10% of MACD crosses result in entry signals, conditions are too strict
    if macd_crosses > 0:
        signal_ratio = entry_signals / macd_crosses
        return signal_ratio < 0.1
    return True

def plot_analysis(data):
    """Create diagnostic plots"""
    print("\n📊 Creating diagnostic plots...")
    
    # Use last 500 bars for clearer visualization
    plot_data = data.tail(500).copy()
    
    fig, axes = plt.subplots(4, 1, figsize=(15, 16))
    fig.suptitle('Enhanced ETH Strategy - Diagnostic Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Price with moving averages
    ax1 = axes[0]
    ax1.plot(plot_data.index, plot_data['Close'], label='ETH Price', alpha=0.8)
    ax1.plot(plot_data.index, plot_data['ma_50'], label='50-day MA', alpha=0.7)
    ax1.plot(plot_data.index, plot_data['ma_trend'], label='200-day MA', alpha=0.7)
    
    # Mark entry signals
    entry_points = plot_data[plot_data['entry_signal']]
    if len(entry_points) > 0:
        ax1.scatter(entry_points.index, entry_points['Close'], 
                   color='green', marker='^', s=100, label='Entry Signals', zorder=5)
    
    ax1.set_title('ETH Price with Moving Averages and Entry Signals')
    ax1.set_ylabel('Price ($)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: MACD
    ax2 = axes[1]
    ax2.plot(plot_data.index, plot_data['macd'], label='MACD', color='blue')
    ax2.plot(plot_data.index, plot_data['macd_signal'], label='Signal', color='red')
    ax2.bar(plot_data.index, plot_data['macd_histogram'], label='Histogram', alpha=0.3)
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # Mark MACD crosses
    macd_crosses = plot_data[plot_data['macd_cross']]
    if len(macd_crosses) > 0:
        ax2.scatter(macd_crosses.index, macd_crosses['macd'], 
                   color='orange', marker='o', s=50, label='MACD Crosses', zorder=5)
    
    ax2.set_title('MACD Indicator with Crossover Signals')
    ax2.set_ylabel('MACD')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: RSI
    ax3 = axes[2]
    ax3.plot(plot_data.index, plot_data['rsi'], label='RSI', color='purple')
    ax3.axhline(y=65, color='red', linestyle='--', alpha=0.7, label='RSI Threshold (65)')
    ax3.axhline(y=70, color='orange', linestyle='--', alpha=0.7, label='Original Threshold (70)')
    ax3.axhline(y=50, color='black', linestyle='-', alpha=0.3)
    ax3.set_title('RSI with Thresholds')
    ax3.set_ylabel('RSI')
    ax3.set_ylim(0, 100)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Volume analysis
    ax4 = axes[3]
    ax4.bar(plot_data.index, plot_data['Volume'], alpha=0.5, label='Volume')
    ax4.plot(plot_data.index, plot_data['volume_sma'], color='red', label='Volume SMA')
    ax4.set_title('Volume Analysis')
    ax4.set_ylabel('Volume')
    ax4.set_xlabel('Date')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/Users/bobbyyo/Projects/algo-fun/strategies/strategy_diagnosis.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    """Main debug analysis"""
    print("🔍 ENHANCED ETH STRATEGY DEBUG ANALYSIS")
    print("=" * 80)
    
    # Load data
    data = load_eth_data()
    
    # Calculate indicators
    data = calculate_indicators(data)
    
    # Analyze conditions
    analysis_data = analyze_conditions(data)
    
    # Suggest optimizations
    suggest_optimizations(analysis_data)
    
    # Create plots
    plot_analysis(analysis_data)
    
    print("\n✅ Debug analysis complete!")
    print("📊 Check strategy_diagnosis.png for visual analysis")
    print("💡 Use the recommendations above to optimize the strategy")

if __name__ == "__main__":
    main()