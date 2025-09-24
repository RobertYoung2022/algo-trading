#!/usr/bin/env python3
"""
CMC Data Analyzer - Example analysis script for daily CMC data
==============================================================
This script demonstrates how to analyze the daily CMC data files created by
the cmc_real_time_monitor.py script.

Usage:
    python analyze_cmc_data.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from cmc_data_utils import load_cmc_data, get_data_summary, analyze_watchlist_performance
import datetime
import os

def main():
    print("🔍 CMC Data Analysis Tool")
    print("=" * 50)

    # Check what data is available
    print("\n📊 Available Data Summary:")
    summary = get_data_summary()

    if not summary:
        print("❌ No CMC data files found.")
        print("💡 Make sure the cmc_real_time_monitor.py script has been running to collect data.")
        return

    for data_type, info in summary.items():
        print(f"✅ {data_type.title()}: {info['files_count']} files, "
              f"{info['total_records']} records ({info['total_size_mb']:.2f} MB)")
        print(f"   📅 Date range: {info['date_range']['start']} to {info['date_range']['end']}")

    # Analyze watchlist performance if available
    if 'watchlist' in summary:
        print("\n📈 Watchlist Performance Analysis (Last 7 days):")
        performance = analyze_watchlist_performance(days_back=7)

        if performance:
            # Sort by performance
            sorted_performance = sorted(performance.items(),
                                      key=lambda x: x[1]['price_change_percent'],
                                      reverse=True)

            print(f"{'Symbol':<8} {'Change %':<10} {'Volatility %':<12} {'Price Range':<20}")
            print("-" * 60)

            for symbol, metrics in sorted_performance:
                price_range = f"${metrics['min_price']:.4f} - ${metrics['max_price']:.4f}"
                print(f"{symbol:<8} {metrics['price_change_percent']:>+7.2f}%   "
                      f"{metrics['volatility_percent']:>8.2f}%     {price_range:<20}")
        else:
            print("   No performance data available (need multiple days of data)")

    # Show recent global metrics if available
    if 'global' in summary:
        print("\n🌍 Recent Global Market Metrics:")
        global_df = load_cmc_data('global', days_back=3)

        if not global_df.empty:
            # Show latest metrics
            latest = global_df.iloc[-1]
            market_cap_t = latest['total_market_cap'] / 1e12
            volume_b = latest['total_volume_24h'] / 1e9
            btc_dom = latest['bitcoin_dominance']
            eth_dom = latest['ethereum_dominance']

            print(f"   💰 Total Market Cap: ${market_cap_t:.2f}T")
            print(f"   📊 24h Volume: ${volume_b:.1f}B")
            print(f"   ₿ Bitcoin Dominance: {btc_dom:.1f}%")
            print(f"   ⟠ Ethereum Dominance: {eth_dom:.1f}%")
            print(f"   🪙 Active Cryptocurrencies: {latest['active_cryptocurrencies']:,}")
            print(f"   🏪 Active Exchanges: {latest['active_exchanges']:,}")

    # Show Fear & Greed Index if available
    if 'fear_greed' in summary:
        print("\n😨 Recent Fear & Greed Index:")
        fng_df = load_cmc_data('fear_greed', days_back=3)

        if not fng_df.empty:
            latest_fng = fng_df.iloc[-1]
            value = latest_fng['value']
            classification = latest_fng['value_classification']

            # Color coding
            if value >= 75:
                emoji = "😎"
                advice = "⚠️  Extreme Greed - Consider taking profits"
            elif value >= 55:
                emoji = "😊"
                advice = "📈 Greed - Market is optimistic"
            elif value >= 45:
                emoji = "😐"
                advice = "⚖️  Neutral - Balanced market sentiment"
            elif value >= 25:
                emoji = "😰"
                advice = "📉 Fear - Market is pessimistic"
            else:
                emoji = "😱"
                advice = "💡 Extreme Fear - Potential buying opportunity"

            print(f"   {emoji} Current Index: {value}/100 ({classification})")
            print(f"   {advice}")

    # Show market sentiment if available
    if 'market_sentiment' in summary:
        print("\n📊 Recent Market Sentiment Analysis:")
        sentiment_df = load_cmc_data('market_sentiment', days_back=3)

        if not sentiment_df.empty:
            latest_sentiment = sentiment_df.iloc[-1]
            score = latest_sentiment['score']
            classification = latest_sentiment['classification']
            market_breadth = latest_sentiment['market_breadth']
            positive_coins = latest_sentiment['positive_coins']
            negative_coins = latest_sentiment['negative_coins']

            print(f"   🎯 Sentiment Score: {score}/100 ({classification})")
            print(f"   📊 Market Breadth: {market_breadth:.1f}% positive")
            print(f"   ✅ Positive Coins: {positive_coins}/10")
            print(f"   ❌ Negative Coins: {negative_coins}/10")

    # Export options
    print("\n📁 Export Options:")
    print("   To export today's data to Excel:")
    today = datetime.date.today().strftime('%Y-%m-%d')
    print(f"   python -c \"from cmc_data_utils import export_daily_summary; export_daily_summary('{today}')\"")

    print("\n📚 Usage Examples:")
    print("   # Load today's watchlist data")
    print("   from cmc_data_utils import load_cmc_data")
    print("   df = load_cmc_data('watchlist')")
    print("")
    print("   # Load last 7 days of global metrics")
    print("   df = load_cmc_data('global', days_back=7)")
    print("")
    print("   # Load specific date range")
    print("   df = load_cmc_data('top_coins', start_date='2024-01-15', end_date='2024-01-20')")

def create_simple_chart():
    """Create a simple chart if matplotlib is available"""
    try:
        # Try to load recent watchlist data for charting
        df = load_cmc_data('watchlist', days_back=7)

        if df.empty or 'symbol' not in df.columns:
            return

        # Create a simple price change chart
        if 'change_24h' in df.columns:
            plt.figure(figsize=(12, 6))

            # Group by symbol and get latest change for each
            latest_changes = df.groupby('symbol')['change_24h'].last().sort_values()

            # Create bar chart
            colors = ['red' if x < 0 else 'green' for x in latest_changes.values]
            plt.bar(latest_changes.index, latest_changes.values, color=colors, alpha=0.7)
            plt.title('Latest 24h Price Changes - Watchlist Coins')
            plt.xlabel('Cryptocurrency Symbol')
            plt.ylabel('24h Change (%)')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()

            # Save chart
            chart_file = f"watchlist_changes_{datetime.date.today().strftime('%Y%m%d')}.png"
            plt.savefig(chart_file, dpi=300, bbox_inches='tight')
            print(f"\n📊 Chart saved: {chart_file}")

    except ImportError:
        print("\n📊 To create charts, install: pip install matplotlib seaborn")
    except Exception as e:
        print(f"\n❌ Chart creation failed: {e}")

if __name__ == "__main__":
    main()

    # Uncomment to create charts
    # create_simple_chart()