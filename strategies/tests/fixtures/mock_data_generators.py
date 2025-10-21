"""
🎭 Mock Data Generators for Strategy Testing
Generates 6 different market scenarios with realistic OHLCV data

Scenarios:
1. False Breakout - High ATR, quick reversal (tests false breakout filters)
2. Valid Breakout - Consolidation + volume surge (tests true breakout detection)
3. Range-Bound - Sideways choppy market (tests overtrading prevention)
4. Trending - Strong directional move (tests trend-following ability)
5. High Volatility Whipsaw - Extreme volatility (tests regime filters)
6. Low Volatility Grind - Slow steady move (tests consolidation detection)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_base_ohlcv(n_bars=100, start_price=100, base_volatility=0.02):
    """
    🔧 Generate base OHLCV structure with realistic price action

    Args:
        n_bars: Number of bars to generate
        start_price: Starting price
        base_volatility: Base daily volatility (default 2%)

    Returns:
        pd.DataFrame with OHLCV columns
    """
    np.random.seed(42)  # Reproducible results

    dates = [datetime.now() - timedelta(days=n_bars-i) for i in range(n_bars)]

    # Generate realistic OHLCV
    closes = [start_price]
    for i in range(1, n_bars):
        change = np.random.randn() * base_volatility * start_price
        closes.append(closes[-1] + change)

    data = []
    for i in range(n_bars):
        close = closes[i]
        high_range = abs(np.random.randn() * base_volatility * close * 0.5)
        low_range = abs(np.random.randn() * base_volatility * close * 0.5)

        high = close + high_range
        low = close - low_range
        open_price = np.random.uniform(low, high)

        # Ensure OHLC logic is valid
        high = max(open_price, close, high)
        low = min(open_price, close, low)

        volume = np.random.uniform(1000000, 5000000)

        data.append({
            'Date': dates[i],
            'Open': open_price,
            'High': high,
            'Low': low,
            'Close': close,
            'Volume': volume
        })

    df = pd.DataFrame(data)
    df.set_index('Date', inplace=True)
    return df


def generate_false_breakout_scenario(n_bars=150):
    """
    🚫 Scenario 1: False Breakout
    - Extended consolidation phase (80 bars)
    - High ATR spike on breakout
    - Quick reversal back to range

    Tests: ATR volatility filter should REJECT this trade
    """
    df = generate_base_ohlcv(n_bars, start_price=100, base_volatility=0.01)

    # Phase 1: Tight consolidation (bars 0-80) - More bars for stable range
    for i in range(80):
        df.loc[df.index[i], 'Close'] = 100 + np.random.uniform(-0.5, 0.5)  # Tighter range
        df.loc[df.index[i], 'High'] = df.loc[df.index[i], 'Close'] + np.random.uniform(0, 0.3)
        df.loc[df.index[i], 'Low'] = df.loc[df.index[i], 'Close'] - np.random.uniform(0, 0.3)
        df.loc[df.index[i], 'Volume'] = 2000000  # Low volume consolidation

    # Phase 2: False breakout with high ATR (bars 80-85)
    for i in range(80, 85):
        # Sharp move up with high volatility
        df.loc[df.index[i], 'Close'] = 100 + (i - 80) * 3  # Sharp move up
        df.loc[df.index[i], 'High'] = df.loc[df.index[i], 'Close'] + 2  # Wide range
        df.loc[df.index[i], 'Low'] = df.loc[df.index[i], 'Close'] - 1
        df.loc[df.index[i], 'Volume'] = 8000000  # High volume (4x average)

    # Phase 3: Reversal back to range (bars 85-150)
    for i in range(85, n_bars):
        df.loc[df.index[i], 'Close'] = 100 + np.random.uniform(-0.5, 0.5)
        df.loc[df.index[i], 'High'] = df.loc[df.index[i], 'Close'] + 0.3
        df.loc[df.index[i], 'Low'] = df.loc[df.index[i], 'Close'] - 0.3
        df.loc[df.index[i], 'Volume'] = 2000000

    return df


def generate_valid_breakout_scenario(n_bars=150):
    """
    ✅ Scenario 2: Valid Breakout
    - Extended tight consolidation (80 bars for stable range)
    - Clear breakout above range high
    - Volume surge on breakout (2x+ average)
    - Sustained follow-through

    Tests: Should ACCEPT this trade (proper consolidation + volume)

    Strategy needs:
    - current_high > highest_high(20 bars)
    - volume >= 1.5x average volume
    """
    df = generate_base_ohlcv(n_bars, start_price=100, base_volatility=0.01)

    # Phase 1: Extended tight consolidation (bars 0-80)
    # Creates stable range for strategy to recognize
    consolidation_price = 100
    for i in range(80):
        # Very tight range: 99.5 - 100.5
        df.loc[df.index[i], 'Close'] = consolidation_price + np.random.uniform(-0.4, 0.4)
        df.loc[df.index[i], 'High'] = df.loc[df.index[i], 'Close'] + np.random.uniform(0.1, 0.2)
        df.loc[df.index[i], 'Low'] = df.loc[df.index[i], 'Close'] - np.random.uniform(0.1, 0.2)
        df.loc[df.index[i], 'Volume'] = 2000000  # Consistent low volume
        df.loc[df.index[i], 'Open'] = df.loc[df.index[i], 'Close'] + np.random.uniform(-0.2, 0.2)

    # Phase 2: Setup bars (bars 80-82) - Compression before breakout
    for i in range(80, 83):
        df.loc[df.index[i], 'Close'] = 100 + np.random.uniform(-0.3, 0.3)
        df.loc[df.index[i], 'High'] = df.loc[df.index[i], 'Close'] + 0.15
        df.loc[df.index[i], 'Low'] = df.loc[df.index[i], 'Close'] - 0.15
        df.loc[df.index[i], 'Volume'] = 2200000  # Slightly increasing
        df.loc[df.index[i], 'Open'] = df.loc[df.index[i], 'Low'] + 0.05

    # Phase 3: BREAKOUT (bars 83-87) - Clear break above consolidation range
    breakout_start_price = 100.5
    for i in range(83, 88):
        # Strong steady climb - breaks clearly above 100.5 range high
        breakout_price = breakout_start_price + (i - 83) * 1.5  # Gains 1.5 per bar
        df.loc[df.index[i], 'Open'] = breakout_price - 0.3
        df.loc[df.index[i], 'Close'] = breakout_price
        df.loc[df.index[i], 'High'] = breakout_price + 0.8  # Clear new highs
        df.loc[df.index[i], 'Low'] = breakout_price - 0.4
        df.loc[df.index[i], 'Volume'] = 5000000  # 2.5x average volume (well above 1.5x threshold)

    # Phase 4: Sustained follow-through with realistic pullbacks (bars 88-150)
    current_price = 107  # Continue from breakout
    for i in range(88, n_bars):
        # Uptrend with occasional pullbacks (80% up, 20% small pullback)
        if np.random.random() < 0.8:
            current_price += np.random.uniform(0.3, 1.0)  # Continued uptrend
        else:
            current_price -= np.random.uniform(0.1, 0.5)  # Small pullback

        df.loc[df.index[i], 'Open'] = current_price - 0.3
        df.loc[df.index[i], 'Close'] = current_price
        df.loc[df.index[i], 'High'] = current_price + np.random.uniform(0.5, 1.0)
        df.loc[df.index[i], 'Low'] = current_price - np.random.uniform(0.2, 0.5)
        df.loc[df.index[i], 'Volume'] = np.random.uniform(2800000, 3500000)  # Elevated but normalizing

        # Add a deliberate 2% pullback at bar 110 to trigger trailing stop
        if i == 110:
            pullback_price = current_price * 0.98  # 2% pullback
            df.loc[df.index[i], 'Close'] = pullback_price
            df.loc[df.index[i], 'Low'] = pullback_price - 0.5
            current_price = pullback_price

    return df


def generate_range_bound_scenario(n_bars=100):
    """
    📊 Scenario 3: Range-Bound Market
    - Choppy sideways action
    - Multiple false signals
    - No clear trend

    Tests: Should minimize trades (overtrading prevention)
    """
    df = generate_base_ohlcv(n_bars, start_price=100, base_volatility=0.02)

    # Create choppy range-bound action
    for i in range(n_bars):
        # Oscillate between 95-105
        phase = np.sin(i / 10.0)  # Sine wave creates oscillation
        df.loc[df.index[i], 'Close'] = 100 + phase * 5 + np.random.uniform(-1, 1)
        df.loc[df.index[i], 'High'] = df.loc[df.index[i], 'Close'] + np.random.uniform(1, 2)
        df.loc[df.index[i], 'Low'] = df.loc[df.index[i], 'Close'] - np.random.uniform(1, 2)
        df.loc[df.index[i], 'Volume'] = np.random.uniform(2000000, 4000000)

    return df


def generate_trending_scenario(n_bars=100):
    """
    📈 Scenario 4: Strong Trending Market
    - Clear uptrend
    - Higher highs, higher lows
    - Good volume

    Tests: Should capture trend efficiently
    """
    df = generate_base_ohlcv(n_bars, start_price=100, base_volatility=0.015)

    # Create strong uptrend
    current_price = 100
    for i in range(n_bars):
        # Add upward drift + some noise
        current_price += np.random.uniform(0.5, 1.5) + np.random.randn() * 0.3
        df.loc[df.index[i], 'Close'] = current_price
        df.loc[df.index[i], 'High'] = current_price + np.random.uniform(0.5, 1.5)
        df.loc[df.index[i], 'Low'] = current_price - np.random.uniform(0.2, 0.8)
        df.loc[df.index[i], 'Volume'] = np.random.uniform(3000000, 6000000)

    return df


def generate_high_volatility_whipsaw_scenario(n_bars=100):
    """
    ⚡ Scenario 5: High Volatility Whipsaw
    - Extreme price swings
    - Rapid reversals
    - Very high ATR

    Tests: ATR regime filter should AVOID trading this
    """
    df = generate_base_ohlcv(n_bars, start_price=100, base_volatility=0.05)

    # Create high volatility whipsaw action
    current_price = 100
    direction = 1
    for i in range(n_bars):
        # Create big swings with frequent reversals
        if i % 5 == 0:
            direction *= -1  # Reverse direction every 5 bars

        current_price += direction * np.random.uniform(3, 8)
        df.loc[df.index[i], 'Close'] = current_price
        df.loc[df.index[i], 'High'] = current_price + np.random.uniform(2, 5)
        df.loc[df.index[i], 'Low'] = current_price - np.random.uniform(2, 5)
        df.loc[df.index[i], 'Volume'] = np.random.uniform(5000000, 10000000)

    return df


def generate_low_volatility_grind_scenario(n_bars=100):
    """
    🐌 Scenario 6: Low Volatility Grind
    - Very small daily moves
    - Steady slow uptrend
    - Low ATR

    Tests: Consolidation detection should identify this as valid setup
    """
    df = generate_base_ohlcv(n_bars, start_price=100, base_volatility=0.005)

    # Create slow steady grind higher
    current_price = 100
    for i in range(n_bars):
        # Small steady gains
        current_price += np.random.uniform(0.1, 0.3) + np.random.randn() * 0.1
        df.loc[df.index[i], 'Close'] = current_price
        df.loc[df.index[i], 'High'] = current_price + np.random.uniform(0.1, 0.3)
        df.loc[df.index[i], 'Low'] = current_price - np.random.uniform(0.1, 0.2)
        df.loc[df.index[i], 'Volume'] = np.random.uniform(1500000, 2500000)

    return df


# 🎭 Scenario Registry
SCENARIOS = {
    'false_breakout': generate_false_breakout_scenario,
    'valid_breakout': generate_valid_breakout_scenario,
    'range_bound': generate_range_bound_scenario,
    'trending': generate_trending_scenario,
    'high_volatility': generate_high_volatility_whipsaw_scenario,
    'low_volatility': generate_low_volatility_grind_scenario
}


def get_scenario(scenario_name, n_bars=None):
    """
    🎯 Get a specific market scenario

    Args:
        scenario_name: One of: 'false_breakout', 'valid_breakout', 'range_bound',
                       'trending', 'high_volatility', 'low_volatility'
        n_bars: Number of bars to generate (default varies by scenario)
                - false_breakout, valid_breakout: 150 bars (need extended consolidation)
                - others: 100 bars

    Returns:
        pd.DataFrame with OHLCV data
    """
    if scenario_name not in SCENARIOS:
        raise ValueError(f"Unknown scenario: {scenario_name}. Available: {list(SCENARIOS.keys())}")

    # Use default n_bars if not specified
    if n_bars is None:
        return SCENARIOS[scenario_name]()
    else:
        return SCENARIOS[scenario_name](n_bars)


def get_all_scenarios(n_bars=100):
    """
    🎨 Get all 6 scenarios as a dictionary

    Returns:
        dict of {scenario_name: DataFrame}
    """
    return {name: func(n_bars) for name, func in SCENARIOS.items()}


if __name__ == '__main__':
    # 🧪 Test scenario generation
    print("🎭 Generating 6 market scenarios...\n")

    scenarios = get_all_scenarios(n_bars=100)

    for name, df in scenarios.items():
        price_change = ((df['Close'].iloc[-1] / df['Close'].iloc[0]) - 1) * 100
        volatility = df['Close'].pct_change().std() * 100
        volume_avg = df['Volume'].mean() / 1_000_000

        print(f"✅ {name.upper()}")
        print(f"   Bars: {len(df)}")
        print(f"   Price Change: {price_change:+.2f}%")
        print(f"   Volatility: {volatility:.2f}%")
        print(f"   Avg Volume: {volume_avg:.2f}M")
        print(f"   Date Range: {df.index[0]} to {df.index[-1]}")
        print()
