# 📊 Strategies Directory

This directory contains all trading strategies organized by type and functionality.

## 📁 Directory Structure

### `/indicators/`
Basic technical indicator strategies:
- `sma_strategy.py` - Simple Moving Average strategy
- `rsi_strategy.py` - Relative Strength Index strategy
- `vwap_strategy.py` - Volume Weighted Average Price strategy
- `vwma_strategy.py` - Volume Weighted Moving Average strategy

### `/eth_strategies/`
Ethereum-specific trading strategies:
- `eth_rsi_strategy.py` - ETH-specific RSI strategy
- `eth_backtesting.py` - ETH backtesting framework
- `eth_trend_converted_strategy.py` - ETH trend conversion strategy
- `enhanced_eth_momentum_*.py` - Enhanced ETH momentum strategies

### `/backtesting/`
Backtesting frameworks and test strategies:
- `backtesting_v2.py` - Main backtesting framework
- `swing_trading_backtest.py` - Swing trading backtest
- `adaptive_volatility_strategy.py` - Adaptive volatility strategy
- `debug_strategy.py` - Strategy debugging utilities
- `simple_debug.py` - Simple debugging tools
- `test_*.py` - Strategy testing files

### `/optimization/`
Strategy optimization and parameter tuning:
- `strategy_optimizer.py` - Main strategy optimization tool
- `quick_optimization.py` - Quick optimization utilities
- `production_readiness_assessment.py` - Production readiness assessment

### `/analysis/`
Strategy analysis and reporting:
- `macd_comprehensive_analysis.py` - MACD analysis tools
- `macd_momentum_strategy.py` - MACD momentum strategy
- `macd_summary_report.md` - MACD analysis summary
- `optimization_results.csv` - Optimization results data
- `heatmap_macd_fast_rsi_threshold_Sharpe.png` - Analysis heatmap
- `strategy_diagnosis.png` - Strategy diagnosis chart

### `/bonus_algorithms/`
Advanced trading algorithms:
- `1_turtle_trending_algo/` - Turtle trading system
- `2_correlation_algo/` - Correlation-based algorithms
- `3_consolidation_pop_algo/` - Consolidation breakout algorithms
- `4_nadarya_watson_algo/` - Nadarya-Watson algorithms
- `5_market_maker/` - Market making strategies
- `6_mean_reversion/` - Mean reversion strategies

### `/results/`
Strategy backtest results and performance data:
- Contains CSV files with strategy performance metrics
- Organized by strategy type and date

## 🚀 Usage

Each strategy directory contains self-contained trading strategies. Check individual strategy files for:
- Configuration requirements
- Data requirements
- Performance metrics
- Optimization parameters

## 📝 Notes

- All strategies have been reorganized from the original scattered structure
- File names have been standardized for better clarity
- Related strategies are grouped together for easier maintenance
- Documentation and analysis files are preserved with their respective strategies
