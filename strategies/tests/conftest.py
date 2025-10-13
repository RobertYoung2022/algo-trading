"""
🔧 Pytest Configuration for Strategy Testing
Provides fixtures and configuration for TDD-based strategy enhancements
"""

import pytest
import pandas as pd
import json
import sys
from pathlib import Path

# 📁 Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from strategies.tests.fixtures.mock_data_generators import get_all_scenarios


@pytest.fixture(scope='session')
def project_root_path():
    """🏠 Get project root directory"""
    return project_root


@pytest.fixture(scope='session')
def baseline_metrics():
    """📊 Load baseline metrics from Phase 0 results"""
    metrics_path = project_root / 'strategies/results/phase0_improvements/baseline_metrics.json'
    with open(metrics_path, 'r') as f:
        return json.load(f)


@pytest.fixture(scope='session')
def data_splits():
    """📅 Load data split configuration (in-sample, validation, out-of-sample)"""
    splits_path = project_root / 'strategies/results/phase0_improvements/data_splits.json'
    with open(splits_path, 'r') as f:
        return json.load(f)


@pytest.fixture(scope='session')
def real_data_path():
    """💾 Get path to real LINK data file"""
    return project_root / 'dataset_files/coinbase/LINKUSD-1d-1000wks-enhanced-data.csv'


@pytest.fixture
def load_real_data(real_data_path):
    """📈 Load real LINK OHLCV data"""
    df = pd.read_csv(real_data_path)

    # 🔧 Normalize column names (handle both lowercase and Titlecase)
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        if col not in df.columns:
            matching = [c for c in df.columns if c.lower() == col.lower()]
            if matching:
                df[col] = df[matching[0]]

    # Parse date column
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

    return df


@pytest.fixture
def split_data(load_real_data, data_splits):
    """
    📊 Split real data into in-sample, validation, and out-of-sample periods

    Returns:
        dict with keys: 'in_sample', 'validation', 'out_of_sample'
    """
    df = load_real_data.copy()
    splits = data_splits['splits']

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        else:
            # Try to convert index directly
            df.index = pd.to_datetime(df.index)

    # Extract dates
    val_start = pd.to_datetime(splits['validation']['start_date'])
    val_end = pd.to_datetime(splits['validation']['end_date'])
    oos_start = pd.to_datetime(splits['out_of_sample']['start_date'])

    # Split data by date
    in_sample = df.loc[df.index < val_start].copy()
    validation = df.loc[(df.index >= val_start) & (df.index <= val_end)].copy()
    out_of_sample = df.loc[df.index >= oos_start].copy()

    return {
        'in_sample': in_sample,
        'validation': validation,
        'out_of_sample': out_of_sample,
        'full': df
    }


@pytest.fixture
def mock_scenarios():
    """🎭 Get all 6 mock market scenarios"""
    return get_all_scenarios(n_bars=100)


@pytest.fixture
def false_breakout_data():
    """🚫 Get false breakout scenario"""
    from strategies.tests.fixtures.mock_data_generators import generate_false_breakout_scenario
    return generate_false_breakout_scenario()  # Uses default 150 bars


@pytest.fixture
def valid_breakout_data():
    """✅ Get valid breakout scenario"""
    from strategies.tests.fixtures.mock_data_generators import generate_valid_breakout_scenario
    return generate_valid_breakout_scenario()  # Uses default 150 bars


@pytest.fixture
def range_bound_data():
    """📊 Get range-bound scenario"""
    from strategies.tests.fixtures.mock_data_generators import generate_range_bound_scenario
    return generate_range_bound_scenario(n_bars=100)


@pytest.fixture
def trending_data():
    """📈 Get trending scenario"""
    from strategies.tests.fixtures.mock_data_generators import generate_trending_scenario
    return generate_trending_scenario(n_bars=100)


@pytest.fixture
def high_volatility_data():
    """⚡ Get high volatility whipsaw scenario"""
    from strategies.tests.fixtures.mock_data_generators import generate_high_volatility_whipsaw_scenario
    return generate_high_volatility_whipsaw_scenario(n_bars=100)


@pytest.fixture
def low_volatility_data():
    """🐌 Get low volatility grind scenario"""
    from strategies.tests.fixtures.mock_data_generators import generate_low_volatility_grind_scenario
    return generate_low_volatility_grind_scenario(n_bars=100)


# 🎯 Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: Unit tests for individual functions"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests with full strategy"
    )
    config.addinivalue_line(
        "markers", "slow: Slow tests that run full backtests"
    )
    config.addinivalue_line(
        "markers", "mock: Tests using mock data scenarios"
    )
    config.addinivalue_line(
        "markers", "real_data: Tests using real market data"
    )


# 🎨 Pretty test output
def pytest_report_header(config):
    """Custom header for test reports"""
    return [
        "🧪 Phase 0 Enhancement Testing Framework",
        "📋 TDD Methodology: Test → Code → Validate → Decide",
        "🎯 Strategy: Breakout Momentum LINK-1d"
    ]
