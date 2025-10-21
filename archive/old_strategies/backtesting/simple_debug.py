"""
Simple Strategy Debug - Identify trade generation issues
========================================================
"""

import pandas as pd
import numpy as np
import talib

def debug_strategy():
    """Debug the Enhanced ETH Strategy"""
    
    # Load ETH data
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
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
    
    print(f"📊 Loaded {len(data)} bars")
    print(f"💰 Price range: ${data['Close'].min():.2f} - ${data['Close'].max():.2f}")
    print(f"📅 Date range: {data.index[0]} to {data.index[-1]}")
    
    # Calculate indicators
    close = data['Close'].values
    high = data['High'].values
    low = data['Low'].values
    volume = data['Volume'].values
    
    # Strategy parameters
    print("\n📈 Calculating indicators...")
    
    # MACD (8,21,5)
    macd_line, macd_signal_line, macd_histogram = talib.MACD(close, fastperiod=8, slowperiod=21, signalperiod=5)
    data['macd'] = macd_line
    data['macd_signal'] = macd_signal_line
    
    # RSI
    data['rsi'] = talib.RSI(close, timeperiod=14)
    
    # Moving averages
    data['ma_200'] = talib.SMA(close, timeperiod=200)
    data['ma_50'] = talib.SMA(close, timeperiod=50)
    
    # ATR
    data['atr'] = talib.ATR(high, low, close, timeperiod=14)
    
    # Volume SMA
    data['volume_sma'] = talib.SMA(volume, timeperiod=20)
    
    # Remove NaN rows
    data = data.dropna()
    total_bars = len(data)
    
    print(f"✅ Valid bars after indicators: {total_bars}")
    
    # Analyze entry conditions
    print("\n🔍 ANALYZING ENTRY CONDITIONS")
    print("=" * 50)
    
    # Individual conditions
    macd_cross = ((data['macd'].shift(1) <= data['macd_signal'].shift(1)) & 
                  (data['macd'] > data['macd_signal']))
    rsi_filter = data['rsi'] < 65
    trend_filter = data['Close'] > data['ma_200']
    momentum_filter = data['Close'] > data['ma_50']
    volume_filter = data['Volume'] > data['volume_sma']
    macd_positive = data['macd'] > 0
    
    # Count conditions
    conditions = {
        'MACD Crosses': macd_cross.sum(),
        'RSI < 65': rsi_filter.sum(),
        'Price > 200MA': trend_filter.sum(),
        'Price > 50MA': momentum_filter.sum(),
        'Volume > Avg': volume_filter.sum(),
        'MACD > 0': macd_positive.sum()
    }
    
    print("Individual condition frequency:")
    for name, count in conditions.items():
        pct = (count / total_bars) * 100
        print(f"  {name:<15}: {count:>4} / {total_bars} ({pct:>5.1f}%)")
    
    # Combined conditions
    all_conditions = (macd_cross & rsi_filter & trend_filter & 
                     momentum_filter & volume_filter & macd_positive)
    entry_signals = all_conditions.sum()
    
    print(f"\n🎯 FINAL ENTRY SIGNALS: {entry_signals} / {total_bars} ({(entry_signals/total_bars)*100:.1f}%)")
    
    # Analyze what's blocking trades
    print(f"\n❌ BLOCKING ANALYSIS (when MACD crosses):")
    macd_cross_data = data[macd_cross]
    if len(macd_cross_data) > 0:
        total_crosses = len(macd_cross_data)
        blocks = {
            'RSI ≥ 65': (~macd_cross_data['rsi'] < 65).sum(),
            'Price ≤ 200MA': (~(macd_cross_data['Close'] > macd_cross_data['ma_200'])).sum(),
            'Price ≤ 50MA': (~(macd_cross_data['Close'] > macd_cross_data['ma_50'])).sum(),
            'Volume ≤ Avg': (~(macd_cross_data['Volume'] > macd_cross_data['volume_sma'])).sum(),
            'MACD ≤ 0': (~(macd_cross_data['macd'] > 0)).sum()
        }
        
        for reason, count in blocks.items():
            pct = (count / total_crosses) * 100
            print(f"  {reason:<15}: {count:>3} / {total_crosses} crosses blocked ({pct:>5.1f}%)")
    
    # Show recent data
    print(f"\n📊 RECENT DATA (last 10 bars):")
    recent = data.tail(10)
    for i, (date, row) in enumerate(recent.iterrows()):
        macd_cross_today = ((recent['macd'].shift(1).iloc[i] <= recent['macd_signal'].shift(1).iloc[i]) & 
                           (row['macd'] > row['macd_signal'])) if i > 0 else False
        
        print(f"{date.strftime('%Y-%m-%d')}: Price=${row['Close']:>7.2f} | "
              f"MACD={row['macd']:>6.3f} | RSI={row['rsi']:>5.1f} | "
              f"Cross={'✓' if macd_cross_today else '✗'} | "
              f"RSI_OK={'✓' if row['rsi'] < 65 else '✗'} | "
              f"Trend={'✓' if row['Close'] > row['ma_200'] else '✗'}")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    
    trend_pct = (trend_filter.sum() / total_bars) * 100
    if trend_pct < 30:
        print("  🔴 Price below 200MA most of time - try shorter MA (100d) or remove trend filter")
    
    rsi_blocks = (~rsi_filter).sum()
    if rsi_blocks > total_bars * 0.7:
        print("  🟡 RSI filter blocking many signals - try threshold 70-75")
    
    if conditions['MACD Crosses'] < 20:
        print("  🟡 Few MACD crosses - try faster parameters (5,13,5)")
    
    if entry_signals == 0:
        print("  🔴 CRITICAL: No entry signals generated - strategy too restrictive")
        print("      Suggested relaxed version:")
        print("      • Remove MACD > 0 requirement")
        print("      • Use 100-day MA instead of 200-day")
        print("      • Increase RSI threshold to 70")
        print("      • Make volume filter optional")

if __name__ == "__main__":
    debug_strategy()