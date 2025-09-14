# Algo Trading Data Streams

A comprehensive algorithmic trading data collection and monitoring system for cryptocurrency markets using multiple data sources including Binance WebSocket streams, CoinMarketCap API, and Phemex exchange.

## 🚀 System Overview

This repository contains **8 specialized monitoring scripts** that collect real-time cryptocurrency market data from different sources:

### **Binance WebSocket Monitors (6 scripts)**
- Real-time trade monitoring with multiple size thresholds
- Liquidation tracking for both regular and large liquidations
- Funding rate monitoring for perpetual futures
- Multi-symbol support for major cryptocurrencies

### **API-Based Monitors (2 scripts)**
- CoinMarketCap market data and alerts
- Phemex exchange automated trading bot

### **Key Features**
- **Production-ready error handling** and automatic reconnection
- **Color-coded terminal displays** for easy monitoring
- **CSV data logging** for analysis and backtesting
- **Configurable thresholds** and alerting systems
- **Memory-efficient processing** with batch operations

## 📊 Script Documentation

### **Trade Monitoring Scripts**

#### 1. Recent Trades Monitor (`recent_trades.py`)
**Purpose:** Basic trade monitoring for significant market movements
- **Threshold:** Monitors trades above $15,000 USD
- **Features:** Real-time display, color-coded buy/sell indicators
- **Output:** `recent_trades.csv`
- **Run:** `python recent_trades.py`

#### 2. Huge Trades Aggregator (`huge_trades.py`)
**Purpose:** Advanced monitoring for institutional-size trades
- **Threshold:** Tracks trades above $500,000 USD
- **Features:** Trade aggregation by second, automatic cleanup, blinking alerts for $10M+ trades
- **Output:** `huge_trades.csv`
- **Run:** `python huge_trades.py`

### **Liquidation Monitoring Scripts**

#### 3. Standard Liquidations (`liqs.py`)
**Purpose:** Monitor forced liquidations across all sizes
- **Threshold:** Displays liquidations above $3,000 USD
- **Features:** Enhanced formatting for large liquidations, batch processing
- **Output:** `liqs.csv`, `liqs.log`
- **Run:** `python liqs.py`

#### 4. Big Liquidations (`big_liqs.py`)
**Purpose:** Focus on major liquidation events
- **Threshold:** Displays liquidations above $100,000 USD
- **Features:** Special alerts for $1M+ and $5M+ liquidations
- **Output:** `big_liqs.csv`, `big_liqs.log`
- **Run:** `python big_liqs.py`

### **Market Data Scripts**

#### 5. Funding Rate Monitor (`funding.py`)
**Purpose:** Track perpetual futures funding rates
- **Features:** Real-time funding rates, yearly calculations, color-coded display
- **Metrics:** Annual funding rate projections
- **Run:** `python funding.py`

#### 6. CoinMarketCap Monitor (`cmc_real_time_monitor.py`)
**Purpose:** Comprehensive market monitoring via CMC API
- **Features:** Global market metrics, top coins tracking, watchlist monitoring, price/volume alerts
- **Output:** CSV files in `data/cmc_monitor/` directory
- **Setup Required:** CMC API key in `.env` file
- **Run:** `python cmc_real_time_monitor.py`

#### 7. CMC Data Analyzer (`analyze_cmc_data.py`)
**Purpose:** Analyze collected CMC data for performance insights
- **Features:** Watchlist performance analysis, market overview, Fear & Greed Index tracking
- **Output:** Terminal analysis and optional charts
- **Run:** `python analyze_cmc_data.py`

#### 8. CMC Data Utils (`cmc_data_utils.py`)
**Purpose:** Utility functions for loading and processing CMC data
- **Features:** Data loading, performance analysis, Excel export capabilities
- **Usage:** Import functions for custom analysis scripts

### **Trading Bot Scripts**

#### 9. Algo Orders Bot (`algo_orders.py`)
**Purpose:** Automated trading bot for Phemex exchange
- **Features:** Scheduled limit orders, automatic cancellation, position management
- **Setup Required:** Phemex API credentials in `.env` file
- **Run:** `python algo_orders.py`

## 📋 Script Comparison Table

| Script | Data Source | Threshold | Purpose | Output Files | API Required |
|--------|-------------|-----------|---------|-------------|--------------|
| `recent_trades.py` | Binance WebSocket | $15K+ | Basic trade monitoring | `recent_trades.csv` | ❌ |
| `huge_trades.py` | Binance WebSocket | $500K+ | Large trade aggregation | `huge_trades.csv` | ❌ |
| `liqs.py` | Binance WebSocket | $3K+ | All liquidations | `liqs.csv`, `liqs.log` | ❌ |
| `big_liqs.py` | Binance WebSocket | $100K+ | Major liquidations | `big_liqs.csv`, `big_liqs.log` | ❌ |
| `funding.py` | Binance WebSocket | N/A | Funding rates | Terminal only | ❌ |
| `cmc_real_time_monitor.py` | CoinMarketCap API | Configurable | Market overview | Multiple CSV files | ✅ CMC API |
| `analyze_cmc_data.py` | CMC CSV files | N/A | Data analysis | Terminal output | ❌ |
| `cmc_data_utils.py` | CMC CSV files | N/A | Data utilities | Various outputs | ❌ |
| `algo_orders.py` | Phemex API | N/A | Automated trading | None | ✅ Phemex API |

## 🛠️ Installation & Setup

### **Basic Setup (Binance WebSocket Scripts)**
1. **Install Python dependencies:**
   ```bash
   pip install websockets termcolor pytz aiohttp asyncio
   ```

2. **Run any Binance-based script:**
   ```bash
   python recent_trades.py    # Start with basic trade monitoring
   python huge_trades.py      # For large trades only
   python liqs.py            # Monitor liquidations
   python funding.py         # Check funding rates
   ```

### **CoinMarketCap API Setup**
1. **Get API Key:**
   - Visit [CoinMarketCap API](https://coinmarketcap.com/api/)
   - Sign up for free API key

2. **Create `.env` file:**
   ```bash
   CMC_API_KEY=your_coinmarketcap_api_key_here
   ```

3. **Install additional dependencies:**
   ```bash
   pip install pandas requests python-dotenv
   ```

4. **Run CMC monitor:**
   ```bash
   python cmc_real_time_monitor.py
   ```

5. **Analyze CMC data:**
   ```bash
   python analyze_cmc_data.py
   ```

### **Phemex Trading Bot Setup**
1. **Get Phemex API credentials:**
   - Create Phemex account and generate API keys

2. **Add to `.env` file:**
   ```bash
   PH_API_KEY=your_phemex_api_key
   PH_SECRET_KEY=your_phemex_secret_key
   ```

3. **Install trading library:**
   ```bash
   pip install ccxt schedule python-dotenv
   ```

4. **Run trading bot:**
   ```bash
   python algo_orders.py
   ```

## 🚀 Usage Examples

### **Quick Start Guide**

#### **Monitor Regular Trading Activity**
```bash
python recent_trades.py        # See trades $15K+
```

#### **Focus on Whale Activity**
```bash
python huge_trades.py          # See trades $500K+
```

#### **Watch Liquidations**
```bash
python liqs.py                 # All liquidations $3K+
python big_liqs.py            # Major liquidations $100K+
```

#### **Check Market Conditions**
```bash
python funding.py              # Funding rates
python cmc_real_time_monitor.py  # Market overview (requires API key)
python analyze_cmc_data.py     # Analyze collected CMC data
```

#### **Run Trading Bot**
```bash
python algo_orders.py          # Phemex bot (requires API keys)
```

### **Advanced Usage**

#### **Run Multiple Monitors Simultaneously**
```bash
# Terminal 1: Monitor large trades
python huge_trades.py

# Terminal 2: Monitor liquidations
python big_liqs.py

# Terminal 3: Check funding rates
python funding.py
```

#### **Background Monitoring with Screen/Tmux**
```bash
# Start screen session
screen -S crypto_monitor

# Run your preferred monitor
python huge_trades.py

# Detach: Ctrl+A, then D
# Reattach: screen -r crypto_monitor
```

## 📈 Supported Symbols & Markets

### **Binance WebSocket Scripts**
All Binance-based scripts monitor these 9 major cryptocurrency pairs:
- **BTCUSDT** (Bitcoin)
- **ETHUSDT** (Ethereum)
- **SOLUSDT** (Solana)
- **XRPUSDT** (Ripple)
- **LINKUSDT** (Chainlink)
- **SUIUSDT** (Sui)
- **HBARUSDT** (Hedera)
- **AAVEUSDT** (Aave)
- **OPUSDT** (Optimism)

### **CoinMarketCap Monitor**
- Top 20 cryptocurrencies by market cap
- Custom watchlist: BTC, ETH, SOL, XRP, ADA, DOT, MATIC, AVAX, LINK, UNI
- Global market metrics and dominance data

### **Phemex Trading Bot**
- Currently configured for **uBTCUSDT** (Bitcoin perpetual futures)
- Customizable for other Phemex trading pairs

## 📁 Data Output & File Structure

### **CSV Output Files**
| File | Source Script | Content | Size Estimates |
|------|---------------|---------|----------------|
| `recent_trades.csv` | `recent_trades.py` | Trades $15K+ | ~50MB/day |
| `huge_trades.csv` | `huge_trades.py` | Trades $500K+ | ~5MB/day |
| `liqs.csv` | `liqs.py` | All liquidations $3K+ | ~100MB/day |
| `big_liqs.csv` | `big_liqs.py` | Liquidations $100K+ | ~10MB/day |
| `data/cmc_monitor/*.csv` | `cmc_real_time_monitor.py` | Market data snapshots | ~1MB/day |

### **Log Files**
- `liqs.log` - Liquidation monitor logs
- `big_liqs.log` - Big liquidation monitor logs
- `data/cmc_monitor/cmc_monitor.log` - CMC monitor logs

### **Data Fields**
**Trade Data:**
- Event Time, Symbol, Aggregate Trade ID, Price, Quantity
- First Trade ID, Trade Time, Is Buyer Maker, USD Size

**Liquidation Data:**
- Symbol, Side, Order Type, Time in Force, Original Quantity
- Price, Average Price, Order Status, USD Size

## ⚙️ Configuration & Thresholds

### **Display Thresholds**
| Script | Minimum Display | Bold | Blink | Special Alert |
|--------|----------------|------|-------|---------------|
| `recent_trades.py` | $15K | $50K | - | - |
| `huge_trades.py` | $500K | $1M | $10M | - |
| `liqs.py` | $3K | $10K | $100K/$250K | $1M |
| `big_liqs.py` | $100K | $1M | $1M | $5M |

### **Customization Options**
- **Symbols:** Edit the `symbols` list in any script
- **Thresholds:** Modify size constants at the top of each script
- **Update Intervals:** Adjust timing constants
- **Colors:** Modify `termcolor` configurations

### **Connection Settings**
- **Ping Interval:** 20 seconds (prevents timeouts)
- **Reconnect Attempts:** 10 maximum with exponential backoff
- **Timezone:** US/Central for all timestamps

## 🔧 Troubleshooting

### **Common Issues**

#### **Connection Problems**
```bash
# Issue: WebSocket connection fails
# Solution: Check internet connection and try again
python script_name.py
```

#### **Permission Errors**
```bash
# Issue: Cannot write CSV files
# Solution: Check directory permissions
chmod 755 ./
```

#### **Missing Dependencies**
```bash
# Issue: ModuleNotFoundError
# Solution: Install required packages
pip install websockets termcolor pytz pandas requests ccxt python-dotenv
```

#### **API Key Issues**
```bash
# Issue: CMC API key invalid
# Solution: Verify .env file format
cat .env
# Should show: CMC_API_KEY=your_actual_key_here
```

### **Performance Tips**
- **Memory Usage:** Scripts use ~10-50MB RAM each
- **CPU Usage:** Low impact, mostly I/O operations
- **Disk Usage:** Monitor CSV file growth (see size estimates above)
- **Network:** Requires stable internet for WebSocket connections

### **Monitoring Health**
- All scripts include built-in error handling and reconnection
- Check log files for connection issues
- Use Ctrl+C for graceful shutdown
- Scripts automatically resume after temporary network issues

## 🚨 Important Notes

- **Rate Limits:** CoinMarketCap has API rate limits (free tier: 333 calls/day)
- **WebSocket Limits:** Binance allows multiple concurrent connections
- **Data Growth:** CSV files grow continuously - implement rotation if needed
- **Time Zones:** All timestamps use US/Central timezone
- **Market Hours:** Crypto markets operate 24/7
- **Risk Warning:** Trading bot (`algo_orders.py`) can place real orders - use with caution

## 🎯 Use Cases & Strategies

### **Trading Analysis**
- **Whale Tracking:** Use `huge_trades.py` to spot large institutional moves
- **Liquidation Cascades:** Monitor `big_liqs.py` for forced selling events
- **Funding Arbitrage:** Watch `funding.py` for high funding rate opportunities
- **Market Sentiment:** Use `cmc_real_time_monitor.py` for broader market trends

### **Research Applications**
- **Market Microstructure:** Analyze trade timing and sizes
- **Price Impact Studies:** Correlate large trades with price movements
- **Volatility Research:** Study liquidation events and market stress
- **Cross-Exchange Analysis:** Compare data across multiple platforms

### **Risk Management**
- **Position Sizing:** Use trade size distributions for risk models
- **Stop Loss Optimization:** Analyze liquidation patterns
- **Market Timing:** Identify high-volatility periods
- **Correlation Studies:** Track multiple assets simultaneously

## 🛡️ Security & Best Practices

### **API Key Security**
- Never commit `.env` files to version control
- Use separate API keys for different environments
- Regularly rotate API keys
- Monitor API usage and limits

### **Data Handling**
- Implement log rotation for large files
- Regular backup of important trading data
- Monitor disk space usage
- Consider data privacy implications

### **Trading Bot Safety**
- Start with small position sizes
- Implement position limits
- Use testnet/paper trading first
- Monitor bot performance continuously
- Have emergency stop procedures

## 🤝 Contributing

Contributions are welcome! Areas for enhancement:
- Additional exchange integrations
- New monitoring scripts
- Performance optimizations
- Documentation improvements
- Test coverage expansion

## 📚 Additional Documentation

- **CMC Daily Files Guide**: See `CMC_DAILY_FILES_GUIDE.md` for detailed information about the daily file organization system used by the CMC monitor.

## 📄 License

This project is for educational and research purposes.

**⚠️ Disclaimer:** Trading cryptocurrencies involves substantial risk. This software is provided as-is for educational purposes. Users are responsible for their own trading decisions and any financial losses.
