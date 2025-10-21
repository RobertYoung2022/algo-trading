# 📊 Data Collection Reference Guide
## Complete API Limits, Rate Limits & Usage Guidelines

---

## 🚀 **QUICK START - Which Script to Use When**

| **Need** | **Best Choice** | **Script** | **Max Data** |
|----------|----------------|------------|--------------|
| **Maximum Historical Data** | Coinbase | `coinbase_data_2025.py` | 500+ weeks (9+ years) |
| **Quick Free Data** | CoinGecko | `coingecko_data.py` | 365 days (1 year) |
| **Regular Updates** | CryptoCompare | `crypto_compare_data.py` | 2000 points (5.5 years daily) |
| **Hyperliquid Exchange Data** | Hyperliquid | `data_from_hl_2025.py` | 5000 bars (13+ years daily) |
| **Real-time Monitoring** | CMC Monitor | `data-streams/cmc_real_time_monitor.py` | Continuous |
| **Free but Limited** | CoinMarketCap | `coin_market_cap_data.py` | 1 point (latest only) |

---

## 📋 **DETAILED API SPECIFICATIONS**

### **1. Coinbase Exchange API** (`coinbase_data_2025.py`)

**🔧 Configuration:**
```python
SYMBOL = 'BTC-USD'        # Trading pair
TIMEFRAME = '5m'          # 1m, 5m, 15m, 1h, 6h, 1d
WEEKS = 70                # How many weeks of data
SAVE_DIR = 'data/coinbase'
```

**📊 Rate Limits:**
- **Requests**: ~3 per second (0.5s delay between requests)
- **Monthly**: No limits
- **Chunking**: Automatic (300 candles per request)

**⏰ Maximum Timeframes:**
- **1m timeframe**: 100-200 weeks (1.9-3.8 years)
- **5m timeframe**: 300-500 weeks (5.8-9.6 years)
- **1h timeframe**: 500-1000+ weeks (9.6+ years)
- **1d timeframe**: 1000+ weeks (19+ years)

**✅ Best For:**
- Maximum historical data collection
- High-frequency data updates
- Professional trading analysis
- Extensive backtesting

**⚠️ Requirements:**
- API key required
- Coinbase Exchange account

**🔄 Safe Usage:**
- **Frequency**: Multiple times per hour
- **Recommended**: Every 5-30 minutes
- **Rate Limit**: Built-in 0.5s delays

---

### **2. CoinGecko API** (`coingecko_data.py`)

**🔧 Configuration:**
```python
COIN_ID = 'ethereum'      # Coin ID (bitcoin, ethereum, solana, etc.)
VS_CURRENCY = 'usd'       # usd, eur, btc, eth, etc.
DAYS = 90                 # 1-365 days
SAVE_DIR = 'data/coingecko'
```

**📊 Rate Limits:**
- **Requests**: 5-15 per minute (5s delay between requests)
- **Monthly**: No documented limits
- **Free Tier**: Rate varies with global usage

**⏰ Maximum Timeframes:**
- **Hard Limit**: 365 days (1 year)
- **All timeframes**: Same 365-day limit
- **Single request**: Gets all data at once

**✅ Best For:**
- Free historical data
- Occasional updates
- Multiple cryptocurrency coverage
- No API key required

**⚠️ Limitations:**
- Maximum 365 days only
- Rate limits vary with usage
- No volume data in OHLC endpoint

**🔄 Safe Usage:**
- **Frequency**: Every 5-10 minutes
- **Recommended**: Every 10-30 minutes
- **Rate Limit**: Built-in 5s delays

---

### **3. CryptoCompare API** (`crypto_compare_data.py`)

**🔧 Configuration:**
```python
SYMBOL = 'ETH'            # BTC, ETH, SOL, ADA, etc.
VS_CURRENCY = 'USDT'      # USD, USDT, BTC, EUR, etc.
TIMEFRAME = 'day'         # minute, hour, day
LIMIT = 100               # 1-2000 points (max per request)
SAVE_DIR = 'data/cryptocompare'
```

**📊 Rate Limits:**
- **Requests**: 50 per second (1s delay between requests)
- **Monthly**: 100,000 calls
- **Daily**: ~3,333 calls

**⏰ Maximum Timeframes by Limit:**
- **1m timeframe**: 2000 minutes = 33.3 hours (1.4 days)
- **1h timeframe**: 2000 hours = 83.3 days (2.8 months)
- **1d timeframe**: 2000 days = 5.5 years

**✅ Best For:**
- Regular data updates
- Good free tier limits
- Reliable data quality
- Multiple timeframes

**⚠️ Limitations:**
- 2000 points maximum per request
- Monthly call limits
- No API key required

**🔄 Safe Usage:**
- **Frequency**: Every 2-5 minutes
- **Recommended**: Every 5-15 minutes
- **Rate Limit**: Built-in 1s delays

---

### **4. Hyperliquid API** (`data_from_hl_2025.py`)

**🔧 Configuration:**
```python
SYMBOL = 'ETH'            # BTC, ETH, SOL, ARB, AVAX, etc.
TIMEFRAME = '1h'          # 1m, 1h, 1d, 1w
SAVE_DIR = 'data/hyperliquid'
```

**📊 Rate Limits:**
- **Requests**: No documented limits (1s delay between requests)
- **Monthly**: No limits
- **Public API**: No authentication required

**⏰ Maximum Timeframes (5000 bar limit):**
- **1m timeframe**: 5000 minutes = 83.3 hours (3.5 days)
- **1h timeframe**: 5000 hours = 208.3 days (6.9 months)
- **1d timeframe**: 5000 days = 13.7 years
- **1w timeframe**: 5000 weeks = 96.2 years

**✅ Best For:**
- Hyperliquid exchange-specific data
- Long-term historical analysis
- No API key required
- Generous rate limits

**⚠️ Limitations:**
- **Hard limit**: 5000 bars maximum
- Limited to Hyperliquid trading pairs
- No API key required

**🔄 Safe Usage:**
- **Frequency**: Every 2-5 minutes
- **Recommended**: Every 5-15 minutes
- **Rate Limit**: Built-in 1s delays

---

### **5. CoinMarketCap API** (`coin_market_cap_data.py`)

**🔧 Configuration:**
```python
SYMBOL = 'ETH'            # BTC, ETH, SOL, ADA, etc.
VS_CURRENCY = 'USD'       # USD, EUR, BTC, ETH, etc.
COUNT = 30                # Data points (limited by plan)
INTERVAL = 'daily'        # 1h, 2h, 3h, daily, weekly, monthly
SAVE_DIR = 'data/coinmarketcap'
```

**📊 Rate Limits:**
- **Free Tier**: 10 calls per minute (7s delay between requests)
- **Monthly**: 10,000 calls
- **Daily**: ~333 calls

**⏰ Maximum Timeframes by Plan:**
- **Free Tier**: 1 point (latest quote only)
- **Hobbyist ($29/month)**: 30 days
- **Startup ($99/month)**: 90 days
- **Standard ($499/month)**: 2 years
- **Professional ($999/month)**: 5+ years

**✅ Best For:**
- Real-time monitoring (paid plans)
- Market cap and volume data
- Professional analysis (paid plans)

**⚠️ Limitations:**
- **Free tier**: Very limited (latest quotes only)
- **Paid plans**: Expensive for historical data
- **Rate limits**: Very restrictive

**🔄 Safe Usage:**
- **Frequency**: Every 10+ minutes
- **Recommended**: Every 15-30 minutes
- **Rate Limit**: Built-in 7s delays

---

### **6. CMC Real-Time Monitor** (`data-streams/cmc_real_time_monitor.py`)

**🔧 Configuration:**
```python
REFRESH_INTERVAL = 30     # Seconds between updates
TOP_COINS_LIMIT = 20      # Number of top coins to display
WATCHLIST = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOT', 'MATIC', 'AVAX', 'LINK', 'UNI']
MIN_PRICE_CHANGE = 5      # Alert threshold
MIN_VOLUME_CHANGE = 50    # Alert threshold
```

**📊 Rate Limits:**
- **Requests**: 10 calls per minute
- **Monthly**: 10,000 calls
- **Monitoring**: ~2 calls per minute

**⏰ Maximum Timeframes:**
- **Continuous monitoring**: Indefinite
- **Data logging**: Timestamped CSV files
- **Real-time updates**: 30-second intervals

**✅ Best For:**
- Real-time market monitoring
- Price and volume alerts
- Market sentiment tracking
- Live trading decisions

**⚠️ Requirements:**
- CoinMarketCap API key required
- Free tier has limited monitoring

**🔄 Safe Usage:**
- **Frequency**: Continuous monitoring
- **Recommended**: 30-second intervals
- **Rate Limit**: Built-in rate limiting

---

## 🎯 **USAGE RECOMMENDATIONS BY SCENARIO**

### **📈 For Maximum Historical Data (Years of Data):**
```python
# Best Choice: Coinbase
WEEKS = 500
TIMEFRAME = '1d'  # Daily candles for maximum history
# Result: ~9.6 years of daily data
```

### **⚡ For High-Frequency Trading (Minutes/Hours):**
```python
# Best Choice: Coinbase + CryptoCompare
# Coinbase
WEEKS = 50
TIMEFRAME = '1m'  # 1-minute candles
# CryptoCompare
TIMEFRAME = 'minute'
LIMIT = 2000  # 33 hours of 1-minute data
```

### **🔄 For Regular Updates (Every Few Hours):**
```python
# Best Choice: CryptoCompare
TIMEFRAME = 'hour'
LIMIT = 2000  # 83 days of hourly data
# Rate: Every 2-5 minutes
```

### **📊 For Long-Term Analysis (Years):**
```python
# Best Choice: Hyperliquid
TIMEFRAME = '1d'  # Daily candles
# Result: 13.7 years of daily data (5000 days)
```

### **💰 For Free Historical Data:**
```python
# Best Choice: CoinGecko
DAYS = 365  # Maximum 1 year
# Rate: Every 5-10 minutes
```

### **📱 For Real-Time Monitoring:**
```python
# Best Choice: CMC Real-Time Monitor
REFRESH_INTERVAL = 30  # 30-second updates
# Continuous monitoring with alerts
```

---

## ⚠️ **IMPORTANT LIMITATIONS & WARNINGS**

### **🚫 Hard Limits (Cannot Be Exceeded):**
- **CoinGecko**: 365 days maximum
- **CryptoCompare**: 2000 points per request
- **Hyperliquid**: 5000 bars maximum
- **CoinMarketCap Free**: 1 point (latest quote only)

### **⏰ Rate Limit Warnings:**
- **CoinGecko**: 5-15 calls/minute (varies with usage)
- **CoinMarketCap**: 10 calls/minute (very restrictive)
- **CryptoCompare**: 100,000 calls/month (monitor usage)

### **🔑 API Key Requirements:**
- **Required**: Coinbase, CoinMarketCap
- **Not Required**: CoinGecko, CryptoCompare, Hyperliquid

### **💰 Cost Considerations:**
- **Free**: CoinGecko, CryptoCompare, Hyperliquid
- **Free with Limits**: Coinbase (requires account)
- **Paid Plans**: CoinMarketCap ($29-$999/month)

---

## 🔧 **QUICK MODIFICATION GUIDE**

### **To Change Coin/Symbol:**
1. **Coinbase**: Change `SYMBOL = 'BTC-USD'`
2. **CoinGecko**: Change `COIN_ID = 'bitcoin'`
3. **CryptoCompare**: Change `SYMBOL = 'BTC'`
4. **Hyperliquid**: Change `SYMBOL = 'BTC'`
5. **CoinMarketCap**: Change `SYMBOL = 'BTC'`

### **To Change Timeframe:**
1. **Coinbase**: Change `TIMEFRAME = '1h'`
2. **CoinGecko**: Change `DAYS = 180`
3. **CryptoCompare**: Change `TIMEFRAME = 'hour'`
4. **Hyperliquid**: Change `TIMEFRAME = '1h'`
5. **CoinMarketCap**: Change `INTERVAL = 'hourly'`

### **To Change Data Amount:**
1. **Coinbase**: Change `WEEKS = 100`
2. **CoinGecko**: Change `DAYS = 365`
3. **CryptoCompare**: Change `LIMIT = 2000`
4. **Hyperliquid**: Fixed at 5000 bars
5. **CoinMarketCap**: Change `COUNT = 90`

---

## 📁 **FILE LOCATIONS & NAMING**

### **Data Storage:**
- **Coinbase**: `data/coinbase/BTCUSD-1d-500wks-data.csv`
- **CoinGecko**: `data/coingecko/BITCOINUSD-365d-coingecko-data.csv`
- **CryptoCompare**: `data/cryptocompare/ETHUSDT-day-2000pts-cc-data.csv`
- **Hyperliquid**: `data/hyperliquid/ETH-USD-1d-hyperliquid-data.csv`
- **CoinMarketCap**: `data/coinmarketcap/ETHUSD-daily-30pts-cmc-data.csv`

### **Log Files:**
- **CMC Monitor**: `data/cmc_monitor/cmc_monitor.log`
- **Real-time Data**: `data/cmc_monitor/global_metrics_*.csv`

---

## 🚀 **PRO TIPS**

### **💡 For Maximum Efficiency:**
1. **Use Coinbase** for extensive historical data
2. **Use CryptoCompare** for regular updates
3. **Use Hyperliquid** for exchange-specific data
4. **Use CoinGecko** for free historical data
5. **Use CMC Monitor** for real-time alerts

### **⚡ For Speed:**
1. **Coinbase**: Fastest with chunking
2. **CryptoCompare**: Good balance
3. **Hyperliquid**: Single request
4. **CoinGecko**: Single request but slow
5. **CoinMarketCap**: Slowest (free tier)

### **💰 For Cost-Effectiveness:**
1. **Free**: CoinGecko, CryptoCompare, Hyperliquid
2. **Free with Account**: Coinbase
3. **Paid**: CoinMarketCap (expensive)

### **📊 For Data Quality:**
1. **Most Comprehensive**: Coinbase
2. **Most Reliable**: CryptoCompare
3. **Exchange-Specific**: Hyperliquid
4. **Market Data**: CoinMarketCap
5. **Free Alternative**: CoinGecko

---

## 🔄 **MAINTENANCE & MONITORING**

### **Regular Checks:**
- Monitor API usage monthly
- Check rate limit compliance
- Verify data file sizes
- Update API keys if needed

### **Error Handling:**
- All scripts include retry logic
- Rate limit compliance built-in
- Graceful error handling
- Automatic reconnection

### **Data Validation:**
- CSV files include timestamps
- Data integrity checks
- Duplicate prevention
- Format consistency

---

*Last Updated: September 2024*
*For questions or updates, refer to this guide or check individual script comments.*
