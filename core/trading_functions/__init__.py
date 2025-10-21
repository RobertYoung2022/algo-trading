"""
🚀 Trading Functions Library - Production-Ready Trading Tools
========================================================

Modular trading functions library for algorithmic trading systems.
Supports multiple exchanges, technical analysis, and risk management.

🌟 Quick Start:
    from trading_functions import UniversalClient, ExchangeType
    from trading_functions import calculate_bollinger_bands, calculate_position_size
    from trading_functions import TRADING_CONFIG, production_readiness_check

💫 Usage Examples:
    # Universal trading client
    client = UniversalClient(ExchangeType.PHEMEX)
    ask, bid, _ = universal_get_ask_bid(client, 'BTCUSD')

    # Technical analysis
    df, is_tight, is_wide = calculate_bollinger_bands(price_df)

    # Risk management
    position = calculate_position_size(10000, 50000, 45000, risk_pct=2.0)

    # Production readiness check
    status = production_readiness_check()
"""

# Configuration imports - Core system configuration
from .config.trading_config import (
    TRADING_CONFIG,
    EXCHANGE_CREDS,
    DATA_SOURCES,
    production_readiness_check,
    validate_config
)

from .config.exchange_config import (
    PHEMEX_CONFIG,
    HYPERLIQUID_CONFIG,
    COINBASE_CONFIG
)

# Exchange client imports - Trading platform interfaces
from .exchanges.phemex_exchange import (
    PhemexClient,
    PhemexError,
    create_phemex_client,
    get_ask_bid_phemex,
    get_ohlcv_data_phemex,
    get_open_positions_phemex,
    kill_switch_phemex,
    monitor_pnl_phemex,
    validate_phemex_connection,
    phemex_production_readiness
)

from .exchanges.hyperliquid_exchange import (
    HyperliquidClient,
    HyperliquidError,
    create_hyperliquid_client,
    get_ask_bid_hyperliquid,
    get_decimals_hyperliquid,
    place_limit_order_hyperliquid,
    get_account_balance_hyperliquid,
    get_position_hyperliquid,
    cancel_all_orders_hyperliquid,
    kill_switch_hyperliquid,
    monitor_pnl_hyperliquid,
    close_all_positions_hyperliquid,
    get_ohlcv_hyperliquid,
    validate_hyperliquid_connection,
    hyperliquid_production_readiness
)

from .exchanges.coinbase_exchange import (
    CoinbaseClient,
    CoinbaseError,
    get_ask_bid_coinbase,
    place_order_coinbase,
    get_balance_coinbase,
    get_positions_coinbase,
    cancel_order_coinbase,
    get_order_history_coinbase
)

# Technical analysis imports - Market indicators and analysis
from .technical.indicators import (
    IndicatorConfig,
    TechnicalAnalysisError,
    validate_dataframe,
    calculate_sma,
    calculate_bollinger_bands,
    calculate_vwap,
    analyze_volume_pattern,
    calculate_rsi,
    calculate_macd,
    create_comprehensive_analysis,
    technical_analysis_production_readiness
)

# Advanced pattern recognition imports - FVG and institutional patterns
from .technical.patterns import (
    PatternAnalysisError,
    PatternConfig,
    identify_fair_value_gaps,
    is_price_in_fvg,
    detect_enhanced_engulfing_pattern,
    detect_pin_bar,
    identify_session_ranges,
    detect_range_break,
    calculate_pattern_strength,
    validate_pattern_data,
    patterns_production_readiness
)

# Market structure analysis imports - Swing points and trend analysis
from .technical.market_structure import (
    MarketStructureError,
    MarketStructureConfig,
    identify_swing_points,
    analyze_swing_structure,
    calculate_market_structure,
    analyze_volume_profile,
    identify_key_levels,
    validate_market_structure_data,
    market_structure_production_readiness
)

# Universal wrapper imports - Multi-exchange compatibility
from .utils.universal_wrappers import (
    ExchangeType,
    UniversalClient,
    UniversalTradingError,
    create_universal_client,
    universal_get_ask_bid,
    universal_monitor_pnl,
    universal_kill_switch,
    universal_get_positions,
    universal_validate_connection,
    universal_get_supported_symbols,
    get_exchange_capabilities,
    universal_wrappers_production_readiness
)

# Risk management imports - Trading safety and position sizing
from .utils.risk_management import (
    RiskManagementError,
    calculate_position_size,
    calculate_risk_reward_ratio,
    validate_trade_risk,
    process_ohlcv_data,
    calculate_portfolio_metrics,
    check_drawdown_limits,
    generate_risk_report,
    risk_management_production_readiness
)

# Strategy performance metrics imports - Comprehensive backtesting analysis
from .utils.strategy_metrics import (
    StrategyMetricsError,
    MetricsConfig,
    calculate_comprehensive_strategy_metrics,
    calculate_enhanced_reward_to_risk,
    validate_trades_data,
    strategy_metrics_production_readiness
)

# 🛡️ Data quality validation imports - Bulletproof data validation system
from .utils.data_quality_validator import (
    DataQualityValidator,
    ValidationResult,
    DataQualityError,
    validate_data_source_quality,
    data_quality_production_readiness
)

# 📊 Data health dashboard imports - Comprehensive quality assessment
from .utils.data_health_dashboard import (
    DataHealthDashboard,
    data_health_dashboard_production_readiness
)

# 🎯 Data validation configuration imports - Centralized validation rules
from .config.data_validation_config import (
    get_validation_config,
    get_backtesting_requirements,
    get_symbol_config,
    get_quality_threshold,
    validate_source_compatibility,
    get_comprehensive_config,
    SOURCE_RELIABILITY,
    QUALITY_THRESHOLDS,
    BACKTESTING_REQUIREMENTS,
    data_validation_config_production_readiness
)

# Version and metadata
__version__ = "1.0.0"
__author__ = "Bobby's Algo Trading Systems 🌙"
__description__ = "Production-ready modular trading functions library"

# Public API - Main exports for easy importing
__all__ = [
    # Configuration
    'TRADING_CONFIG',
    'EXCHANGE_CREDS',
    'DATA_SOURCES',
    'PHEMEX_CONFIG',
    'HYPERLIQUID_CONFIG',
    'COINBASE_CONFIG',
    'production_readiness_check',
    'validate_config',

    # Exchange Types and Errors
    'ExchangeType',
    'PhemexError',
    'HyperliquidError',
    'CoinbaseError',
    'UniversalTradingError',
    'TechnicalAnalysisError',
    'RiskManagementError',

    # Exchange Clients
    'PhemexClient',
    'HyperliquidClient',
    'CoinbaseClient',
    'UniversalClient',

    # Universal Trading Functions
    'create_universal_client',
    'universal_get_ask_bid',
    'universal_monitor_pnl',
    'universal_kill_switch',
    'universal_get_positions',
    'universal_validate_connection',
    'universal_get_supported_symbols',
    'get_exchange_capabilities',
    'universal_wrappers_production_readiness',

    # Exchange-Specific Functions (Phemex)
    'create_phemex_client',
    'get_ask_bid_phemex',
    'get_ohlcv_data_phemex',
    'get_open_positions_phemex',
    'kill_switch_phemex',
    'monitor_pnl_phemex',
    'validate_phemex_connection',
    'phemex_production_readiness',

    # Exchange-Specific Functions (Hyperliquid)
    'create_hyperliquid_client',
    'get_ask_bid_hyperliquid',
    'get_decimals_hyperliquid',
    'place_limit_order_hyperliquid',
    'get_account_balance_hyperliquid',
    'get_position_hyperliquid',
    'cancel_all_orders_hyperliquid',
    'kill_switch_hyperliquid',
    'monitor_pnl_hyperliquid',
    'close_all_positions_hyperliquid',
    'get_ohlcv_hyperliquid',
    'validate_hyperliquid_connection',
    'hyperliquid_production_readiness',

    # Exchange-Specific Functions (Coinbase)
    'get_ask_bid_coinbase',
    'place_order_coinbase',
    'get_balance_coinbase',
    'get_positions_coinbase',
    'cancel_order_coinbase',
    'get_order_history_coinbase',
    'get_market_data_coinbase',

    # Technical Analysis
    'IndicatorConfig',
    'validate_dataframe',
    'calculate_sma',
    'calculate_bollinger_bands',
    'calculate_vwap',
    'analyze_volume_pattern',
    'calculate_rsi',
    'calculate_macd',
    'create_comprehensive_analysis',
    'technical_analysis_production_readiness',

    # Advanced Pattern Recognition
    'PatternAnalysisError',
    'PatternConfig',
    'identify_fair_value_gaps',
    'is_price_in_fvg',
    'detect_enhanced_engulfing_pattern',
    'detect_pin_bar',
    'identify_session_ranges',
    'detect_range_break',
    'calculate_pattern_strength',
    'validate_pattern_data',
    'patterns_production_readiness',

    # Market Structure Analysis
    'MarketStructureError',
    'MarketStructureConfig',
    'identify_swing_points',
    'analyze_swing_structure',
    'calculate_market_structure',
    'analyze_volume_profile',
    'identify_key_levels',
    'validate_market_structure_data',
    'market_structure_production_readiness',

    # Risk Management
    'calculate_position_size',
    'calculate_risk_reward_ratio',
    'validate_trade_risk',
    'process_ohlcv_data',
    'calculate_portfolio_metrics',
    'check_drawdown_limits',
    'generate_risk_report',
    'risk_management_production_readiness',

    # Strategy Performance Metrics
    'StrategyMetricsError',
    'MetricsConfig',
    'calculate_comprehensive_strategy_metrics',
    'calculate_enhanced_reward_to_risk',
    'validate_trades_data',
    'strategy_metrics_production_readiness',

    # 🛡️ Data Quality Validation System
    'DataQualityValidator',
    'ValidationResult',
    'DataQualityError',
    'validate_data_source_quality',
    'data_quality_production_readiness',

    # 📊 Data Health Dashboard
    'DataHealthDashboard',
    'data_health_dashboard_production_readiness',

    # 🎯 Data Validation Configuration
    'get_validation_config',
    'get_backtesting_requirements',
    'get_symbol_config',
    'get_quality_threshold',
    'validate_source_compatibility',
    'get_comprehensive_config',
    'SOURCE_RELIABILITY',
    'QUALITY_THRESHOLDS',
    'BACKTESTING_REQUIREMENTS',
    'data_validation_config_production_readiness'
]

# Production readiness validation on import
def _validate_library_setup():
    """🛡️ Validate library setup on import - Enhanced with Data Validation System"""
    try:
        readiness = production_readiness_check()
        config_valid = readiness.get('config_valid', False)

        # 🛡️ Validate data quality system
        data_validation_ready = False
        try:
            data_val_readiness = data_quality_production_readiness()
            data_validation_ready = data_val_readiness.get('status') == 'ready'
        except:
            pass

        # 📊 Validate data health dashboard
        dashboard_ready = False
        try:
            dashboard_readiness = data_health_dashboard_production_readiness()
            dashboard_ready = dashboard_readiness.get('status') == 'ready'
        except:
            pass

        # 🎯 Validate data validation config
        config_system_ready = False
        try:
            config_readiness = data_validation_config_production_readiness()
            config_system_ready = config_readiness.get('status') == 'ready'
        except:
            pass

        if not config_valid:
            print("⚠️  Trading Functions Library: Configuration validation failed")
            print("📝 Run production_readiness_check() for detailed diagnostics")
        else:
            print("✅ Trading Functions Library loaded successfully 🚀")

            # 🛡️ Report data validation system status
            if data_validation_ready and dashboard_ready and config_system_ready:
                print("🛡️ Data Quality Validation System: READY")
            elif data_validation_ready:
                print("🛡️ Data Quality Validation System: PARTIAL (core ready)")
            else:
                print("⚠️ Data Quality Validation System: NOT AVAILABLE")

    except Exception as e:
        print(f"⚠️  Trading Functions Library: Setup validation error: {e}")

# Run validation on import
_validate_library_setup()