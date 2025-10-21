# 🤖 Algorithmic Trading Project

A comprehensive algorithmic trading system with data collection, strategy development, backtesting, and live trading capabilities.

## 📁 Project Structure

### 🏗️ **Core Directories**

#### `/bots/` - Trading Bots
- **`/hyperliquid/`** - Hyperliquid exchange trading bots
- **`/day_based/`** - Day-based development bots  
- **`/strategies/`** - Strategy-specific trading bots
- **`/utils/`** - Bot utility functions and helpers

#### `/strategies/` - Trading Strategies
- **`/indicators/`** - Basic technical indicator strategies
- **`/eth_strategies/`** - Ethereum-specific strategies
- **`/backtesting/`** - Backtesting frameworks and test strategies
- **`/optimization/`** - Strategy optimization and parameter tuning
- **`/analysis/`** - Strategy analysis and reporting
- **`/bonus_algorithms/`** - Advanced trading algorithms

#### `/data/` - Historical Data
- **`/coinbase/`** - Coinbase Exchange historical data
- **`/coingecko/`** - CoinGecko historical data
- **`/cryptocompare/`** - CryptoCompare historical data
- **`/hyperliquid/`** - Hyperliquid historical data
- **`/coinmarketcap/`** - CoinMarketCap historical data

#### `/data-streams/` - Real-time Data Streams
- **Liquidation monitors** - Binance liquidation data streams
- **Trade monitors** - Large trade detection streams
- **CMC real-time monitor** - CoinMarketCap real-time data

### 📊 **Data Collection Scripts**

#### Historical Data Collection (`/data-scripts/`)
- `coinbase_historical_data.py` - Coinbase Exchange historical data
- `coingecko_historical_data.py` - CoinGecko historical data  
- `cryptocompare_historical_data.py` - CryptoCompare historical data
- `hyperliquid_historical_data.py` - Hyperliquid historical data (5000 bar limit)
- `coinmarketcap_historical_data.py` - CoinMarketCap historical data

#### Real-time Data Streams
- `data-streams/liqs.py` - Binance liquidation monitor
- `data-streams/big_liqs.py` - Binance big liquidation monitor
- `data-streams/huge_trades.py` - Binance huge trades monitor
- `data-streams/cmc_real_time_monitor.py` - CoinMarketCap real-time monitor
- `data-streams/analyze_cmc_data.py` - CMC data analysis tool
- `data-streams/cmc_data_utils.py` - CMC data utilities

### 🔧 **Utility Files**
- `my_nice_function.py` - Comprehensive utility functions
- `multi_data_tester.py` - Multi-source data testing
- `sma.py` - Simple Moving Average calculations

### 📚 **Documentation**
- `DATA_COLLECTION_REFERENCE_GUIDE.md` - Complete data collection guide
- `DATA_SCRIPTS_QUICK_REFERENCE.md` - Quick reference for data scripts
- `COINBASE_SETUP.md` - Coinbase setup instructions
- `MULTI_DATA_TESTING.md` - Multi-data testing framework guide
- `backtest-architect.md` - Backtesting architecture documentation
- `data-streams/CMC_DAILY_FILES_GUIDE.md` - CMC daily data files guide

## 🚀 **Getting Started**

### 1. Environment Setup
```bash
# Activate conda environment
conda activate algo

# Install dependencies
pip install -r requirements.txt
```

### 2. API Configuration
- Set up API keys in `.env` file
- Configure exchange credentials
- Test connections with utility scripts

### 3. Data Collection
```bash
# Collect historical data
python data-scripts/coinbase_historical_data.py
python data-scripts/coingecko_historical_data.py
python data-scripts/cryptocompare_historical_data.py

# Start real-time monitoring
python data-streams/liqs.py
python data-streams/cmc_real_time_monitor.py

# Analyze CMC data
python data-streams/analyze_cmc_data.py
```

### 4. Strategy Development
```bash
# Run indicator strategies
python strategies/indicators/sma_strategy.py
python strategies/indicators/rsi_strategy.py

# Backtest strategies
python strategies/backtesting/backtesting_v2.py
```

## 📈 **Features**

### Data Collection
- ✅ Multiple exchange APIs (Coinbase, CoinGecko, CryptoCompare, Hyperliquid)
- ✅ Real-time data streams with monitoring
- ✅ Historical data collection with rate limiting
- ✅ Comprehensive error handling and logging

### Trading Strategies
- ✅ Technical indicator strategies (SMA, RSI, VWAP, VWMA)
- ✅ Ethereum-specific strategies
- ✅ Advanced algorithms (Turtle, Correlation, Mean Reversion)
- ✅ Adaptive and volatility-based strategies

### Backtesting & Analysis
- ✅ Comprehensive backtesting framework
- ✅ Strategy optimization tools
- ✅ Performance analysis and reporting
- ✅ Production readiness assessment

### Live Trading
- ✅ Hyperliquid exchange integration
- ✅ Risk management utilities
- ✅ Bot monitoring and control
- ✅ Credential testing and validation

## 🔧 **Configuration**

### Rate Limits & Usage
- **Coinbase**: 10 requests/second, extensive historical data
- **CoinGecko**: 5-15 calls/minute, 90-day limit
- **CryptoCompare**: 100,000 calls/month, 2000 data points
- **Hyperliquid**: No official limits, 5000-bar limit
- **CoinMarketCap**: 10 calls/minute, limited historical data

### File Naming Conventions
- Bot files: `{strategy}_bot.py`
- Strategy files: `{indicator}_strategy.py`
- Utility files: `{function}_utils.py`
- Data files: `{exchange}_{symbol}_{timeframe}.csv`

## 📝 **Notes**

- All scripts include inline comments for easy modification
- Rate limiting is implemented to comply with API terms
- Comprehensive error handling and logging throughout
- Organized structure for scalability and maintenance
- Production-ready monitoring and alerting systems

## 🤝 **Contributing**

1. Follow the established directory structure
2. Use consistent naming conventions
3. Include comprehensive documentation
4. Test all changes thoroughly
5. Update relevant README files

## 📄 **License**

This project is for educational and research purposes. Please ensure compliance with exchange terms of service and applicable regulations.
