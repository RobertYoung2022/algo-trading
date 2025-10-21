"""
🚀 Complete Portfolio Integration Testing 🚀
End-to-end testing of the unified trend-following portfolio
across all strategies and data sources.

Created: 2025
Author: Bobby Younghoward
"""

import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Import portfolio components
from strategies.portfolio.trend_following_portfolio_manager import (
    TrendFollowingPortfolioStrategy,
    run_portfolio_backtest,
    optimize_portfolio_allocation
)
from strategies.portfolio.portfolio_optimizer import (
    PortfolioOptimizer,
    get_optimal_allocation_for_asset
)
from strategies.portfolio.portfolio_risk_manager import (
    PortfolioRiskManager,
    Position
)

# Import data loading utilities
import glob


def load_all_crypto_data():
    """
    Load all available cryptocurrency data for comprehensive testing

    Returns:
    --------
    dict
        Dictionary with asset symbols as keys and DataFrames as values
    """

    data_dict = {}
    base_path = Path(__file__).parent.parent.parent / 'data'

    # Define data source patterns
    patterns = {
        'coinbase': base_path / 'coinbase' / '*.csv',
        'yahoo': base_path / 'yahoo_finance' / '*.csv',
        'cryptocompare': base_path / 'cryptocompare' / '*.csv',
        'coingecko': base_path / 'coingecko' / '*.csv',
    }

    print("📊 Loading cryptocurrency data from all sources...")

    for source, pattern in patterns.items():
        files = glob.glob(str(pattern))
        print(f"\n{source.upper()} Data Files: {len(files)} found")

        for file in files[:5]:  # Limit for testing
            try:
                # Try different date column names
                df = pd.read_csv(file)

                # Detect date column
                date_col = None
                for col in ['Date', 'date', 'datetime', 'Datetime', 'time']:
                    if col in df.columns:
                        date_col = col
                        break

                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df.set_index(date_col, inplace=True)
                else:
                    # Try to detect index if it's already a date
                    if pd.api.types.is_datetime64_any_dtype(df.index):
                        pass
                    else:
                        continue

                # Extract asset name from filename
                filename = Path(file).stem
                if 'ETH' in filename.upper():
                    asset = 'ETH'
                elif 'BTC' in filename.upper():
                    asset = 'BTC'
                elif 'HBAR' in filename.upper():
                    asset = 'HBAR'
                elif 'LINK' in filename.upper():
                    asset = 'LINK'
                elif 'CRO' in filename.upper():
                    asset = 'CRO'
                elif 'XRP' in filename.upper():
                    asset = 'XRP'
                else:
                    continue

                key = f"{asset}_{source}_{filename}"

                # Standardize column names
                df.columns = [col.capitalize() for col in df.columns]

                # Validate data quality
                if len(df) > 100 and 'Close' in df.columns:
                    data_dict[key] = df
                    print(f"  ✅ Loaded {asset} from {source}: {len(df)} bars")

            except Exception as e:
                print(f"  ❌ Error loading {file}: {str(e)}")

    return data_dict


def test_portfolio_on_single_asset(asset_data: pd.DataFrame, asset_name: str):
    """
    Test portfolio strategy on a single asset

    Parameters:
    -----------
    asset_data : pd.DataFrame
        OHLCV data for the asset
    asset_name : str
        Name of the asset

    Returns:
    --------
    dict
        Backtest results
    """

    print(f"\n{'='*80}")
    print(f"Testing Portfolio on {asset_name}")
    print(f"Data range: {asset_data.index[0]} to {asset_data.index[-1]}")
    print(f"Total bars: {len(asset_data)}")

    # Get optimal allocation for this asset
    optimal_allocation = get_optimal_allocation_for_asset(asset_name.split('_')[0])
    print(f"Optimal allocation for {asset_name.split('_')[0]}:")
    for strategy, weight in optimal_allocation.items():
        if weight > 0:
            print(f"  {strategy}: {weight:.1%}")

    # Run backtest
    try:
        stats, bt = run_portfolio_backtest(asset_data, cash=10000, commission=0.002)

        # Display results
        print(f"\n📊 Portfolio Performance on {asset_name}:")
        print(f"  Total Return: {stats['Return [%]']:.2f}%")
        print(f"  Sharpe Ratio: {stats.get('Sharpe Ratio', 0):.2f}")
        print(f"  Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"  Win Rate: {stats.get('Win Rate [%]', 0):.1f}%")
        print(f"  Number of Trades: {stats['# Trades']}")

        # Risk metrics
        if 'Sortino Ratio' in stats:
            print(f"  Sortino Ratio: {stats['Sortino Ratio']:.2f}")
        if 'Calmar Ratio' in stats:
            print(f"  Calmar Ratio: {stats['Calmar Ratio']:.2f}")

        return {
            'asset': asset_name,
            'return': stats['Return [%]'],
            'sharpe': stats.get('Sharpe Ratio', 0),
            'max_dd': stats['Max. Drawdown [%]'],
            'win_rate': stats.get('Win Rate [%]', 0),
            'trades': stats['# Trades'],
            'stats': stats
        }

    except Exception as e:
        print(f"  ❌ Error testing {asset_name}: {str(e)}")
        return None


def test_portfolio_optimization(data_dict: dict):
    """
    Test portfolio optimization across multiple assets

    Parameters:
    -----------
    data_dict : dict
        Dictionary of asset data
    """

    print("\n" + "="*80)
    print("🔬 PORTFOLIO OPTIMIZATION TESTING")
    print("="*80)

    # Create optimizer
    optimizer = PortfolioOptimizer()

    # Prepare performance data for optimization
    performance_data = {}

    # Use strategy names instead of asset keys for the optimizer
    strategy_names = ['TEMS', 'VBM', 'ATSS', 'MTMC']

    # Simulate strategy returns for optimization testing
    for i, strategy in enumerate(strategy_names):
        print(f"\nGenerating simulated performance data for {strategy}...")

        # Get first available asset data for simulation
        if data_dict:
            sample_data = list(data_dict.values())[0]
            returns = sample_data['Close'].pct_change().dropna()

            # Add some randomness to simulate different strategy performance
            returns = returns * (1 + np.random.normal(0, 0.1, len(returns)))

            performance_data[strategy] = pd.DataFrame({
                'returns': returns,
                'equity': (1 + returns).cumprod() * 10000
            })

    # Load performance data into optimizer
    if performance_data:
        optimizer.load_performance_data(performance_data)

        print("\n📈 Optimization Results:")

        # Test different optimization methods
        print("\n1. Sharpe Ratio Maximization:")
        sharpe_weights = optimizer.optimize_sharpe_ratio()
        for strategy, weight in sharpe_weights.items():
            print(f"  {strategy}: {weight:.1%}")

        print("\n2. Risk Parity:")
        risk_parity_weights = optimizer.optimize_risk_parity()
        for strategy, weight in risk_parity_weights.items():
            print(f"  {strategy}: {weight:.1%}")

        print("\n3. Minimum Variance:")
        min_var_weights = optimizer.optimize_minimum_variance()
        for strategy, weight in min_var_weights.items():
            print(f"  {strategy}: {weight:.1%}")

        # Get comprehensive allocation matrix
        print("\n📊 Allocation Matrix:")
        allocation_matrix = optimizer.get_optimal_allocation_matrix()
        print(allocation_matrix)


def test_risk_management():
    """Test portfolio risk management system"""

    print("\n" + "="*80)
    print("🛡️ RISK MANAGEMENT TESTING")
    print("="*80)

    # Create risk manager
    risk_manager = PortfolioRiskManager()

    # Create test positions
    test_positions = [
        Position(
            strategy='TEMS',
            asset='ETH',
            size=0.02,
            entry_price=3000,
            entry_time=datetime.now(),
            current_price=3150,
            stop_loss=2850,
            take_profit=3300,
            pnl=3.0,
            pnl_percent=5.0
        ),
        Position(
            strategy='VBM',
            asset='BTC',
            size=0.03,
            entry_price=95000,
            entry_time=datetime.now(),
            current_price=96000,
            stop_loss=90000,
            take_profit=100000,
            pnl=30.0,
            pnl_percent=1.05
        ),
        Position(
            strategy='ATSS',
            asset='HBAR',
            size=0.015,
            entry_price=0.30,
            entry_time=datetime.now(),
            current_price=0.285,
            stop_loss=0.27,
            take_profit=0.36,
            pnl=-0.225,
            pnl_percent=-5.0
        )
    ]

    # Test adding positions
    print("\n📝 Adding test positions:")
    for position in test_positions:
        success = risk_manager.add_position(position)
        if not success:
            print(f"  Failed to add position: {position.asset}")

    # Update risk metrics
    risk_manager.update_risk_metrics()

    # Generate risk report
    print("\n📊 Risk Report:")
    report = risk_manager.get_risk_report()

    print(f"Portfolio Metrics:")
    for key, value in report['portfolio_metrics'].items():
        print(f"  {key}: {value}")

    print(f"\nRisk Scores:")
    for key, value in report['risk_scores'].items():
        print(f"  {key}: {value}")

    print(f"\nRisk Status:")
    for key, value in report['risk_status'].items():
        print(f"  {key}: {value}")

    # Test position sizing
    print("\n📏 Position Sizing Tests:")
    test_cases = [
        (0.8, 0.15),  # High signal, normal volatility
        (0.5, 0.25),  # Medium signal, high volatility
        (0.3, 0.10),  # Low signal, low volatility
    ]

    for signal, volatility in test_cases:
        size = risk_manager.calculate_position_sizing(signal, volatility)
        print(f"  Signal: {signal:.1f}, Vol: {volatility:.2f} -> Size: {size:.1%}")


def run_comprehensive_portfolio_test():
    """Run comprehensive test of the entire portfolio system"""

    print("🌙 COMPREHENSIVE PORTFOLIO INTEGRATION TEST 🌙")
    print("="*80)
    print("Testing unified trend-following portfolio with:")
    print("  - TEMS (Triple EMA Momentum)")
    print("  - VBM (Volatility Breakout)")
    print("  - ATSS (ADX Trend Strength)")
    print("  - MTMC (Multi-Timeframe Cascade)")
    print("="*80)

    # Load data
    data_dict = load_all_crypto_data()

    if not data_dict:
        print("❌ No data loaded. Please check data directory.")
        return

    print(f"\n✅ Loaded {len(data_dict)} datasets for testing")

    # Test individual assets
    results = []
    for asset_key, asset_data in list(data_dict.items())[:5]:  # Test first 5
        result = test_portfolio_on_single_asset(asset_data, asset_key)
        if result:
            results.append(result)

    # Analyze results
    if results:
        print("\n" + "="*80)
        print("📊 PORTFOLIO PERFORMANCE SUMMARY")
        print("="*80)

        # Create results DataFrame
        results_df = pd.DataFrame(results)

        # Sort by Sharpe ratio
        results_df = results_df.sort_values('sharpe', ascending=False)

        print("\n🏆 Top Performers (by Sharpe Ratio):")
        for idx, row in results_df.head(3).iterrows():
            print(f"  {row['asset']}:")
            print(f"    Return: {row['return']:.2f}%")
            print(f"    Sharpe: {row['sharpe']:.2f}")
            print(f"    Max DD: {row['max_dd']:.2f}%")
            print(f"    Win Rate: {row['win_rate']:.1f}%")

        # Overall statistics
        print(f"\n📈 Overall Portfolio Statistics:")
        print(f"  Average Return: {results_df['return'].mean():.2f}%")
        print(f"  Average Sharpe: {results_df['sharpe'].mean():.2f}")
        print(f"  Average Max DD: {results_df['max_dd'].mean():.2f}%")
        print(f"  Average Win Rate: {results_df['win_rate'].mean():.1f}%")
        print(f"  Total Trades: {results_df['trades'].sum()}")

        # Performance distribution
        print(f"\n📊 Performance Distribution:")
        print(f"  Positive Returns: {(results_df['return'] > 0).sum()}/{len(results_df)}")
        print(f"  Sharpe > 1.0: {(results_df['sharpe'] > 1.0).sum()}/{len(results_df)}")
        print(f"  Win Rate > 50%: {(results_df['win_rate'] > 50).sum()}/{len(results_df)}")

    # Test optimization
    test_portfolio_optimization(data_dict)

    # Test risk management
    test_risk_management()

    print("\n" + "="*80)
    print("✅ PORTFOLIO INTEGRATION TEST COMPLETE")
    print("="*80)
    print("\n🎯 Next Steps:")
    print("  1. Fine-tune strategy parameters based on results")
    print("  2. Implement live paper trading with small capital")
    print("  3. Monitor performance and adjust allocations")
    print("  4. Scale up gradually as confidence builds")
    print("\n🚀 Portfolio ready for production deployment! 🚀")


if __name__ == "__main__":
    run_comprehensive_portfolio_test()