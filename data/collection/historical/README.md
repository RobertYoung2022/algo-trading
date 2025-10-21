# 📊 Data Collection Scripts

This directory contains all historical data collection scripts organized by exchange/platform.

## 📁 Available Scripts

### 🏦 **Exchange Data Collection**

#### `coinbase_historical_data.py`
- **Exchange**: Coinbase Exchange
- **Data**: OHLCV historical data
- **Limits**: 10 requests/second, extensive historical data available
- **Best for**: Comprehensive historical analysis, large datasets
- **Rate limiting**: 0.5 seconds between requests

#### `hyperliquid_historical_data.py`
- **Exchange**: Hyperliquid
- **Data**: OHLCV historical data
- **Limits**: 5000 bars maximum per request
- **Best for**: Hyperliquid-specific trading pairs
- **Rate limiting**: 1 second between requests
- **⚠️ Note**: MAX 5000 bars only - use Coinbase for larger datasets

#### `coingecko_historical_data.py`
- **Exchange**: CoinGecko
- **Data**: OHLC historical data (no volume)
- **Limits**: 5-15 calls/minute, 90-day maximum
- **Best for**: Free tier historical data
- **Rate limiting**: 5 seconds between requests

#### `cryptocompare_historical_data.py`
- **Exchange**: CryptoCompare
- **Data**: OHLCV historical data
- **Limits**: 100,000 calls/month, 2000 data points
- **Best for**: Free tier with higher limits than CoinGecko
- **Rate limiting**: 1 second between requests

#### `coinmarketcap_historical_data.py`
- **Exchange**: CoinMarketCap
- **Data**: Latest quotes (limited historical data on free tier)
- **Limits**: 10 calls/minute
- **Best for**: Real-time quotes and market metrics
- **Rate limiting**: 7 seconds between requests

## 🚀 **Usage Examples**

### Quick Data Collection
```bash
# Fetch ETH 1-hour data from Hyperliquid (5000 bars max)
python hyperliquid_historical_data.py

# Fetch BTC daily data from Coinbase (extensive history)
python coinbase_historical_data.py

# Fetch recent ETH data from CoinGecko (90 days max)
python coingecko_historical_data.py

# Fetch ETH data from CryptoCompare (2000 points max)
python cryptocompare_historical_data.py

# Fetch latest ETH data from CoinMarketCap (limited on free tier)
python coinmarketcap_historical_data.py
```

### Configuration
Each script includes inline comments showing exactly what to modify:
- **SYMBOL**: Trading pair to fetch (BTC, ETH, SOL, etc.)
- **TIMEFRAME**: Data interval (1m, 1h, 1d, 1w)
- **SAVE_DIR**: Where to save the CSV files
- **Additional parameters**: Specific to each exchange

## 📋 **File Naming Convention**

All scripts save data with consistent naming:
- `{SYMBOL}-USD-{TIMEFRAME}-{EXCHANGE}-data.csv`
- Example: `ETH-USD-1h-hyperliquid-data.csv`

## ⚠️ **Important Notes**

### Rate Limiting
- All scripts include proper rate limiting
- Respects each exchange's API terms of service
- Includes retry logic and error handling

### Data Limits
- **Hyperliquid**: 5000 bars maximum
- **CoinGecko**: 90 days maximum
- **Coinbase**: Extensive historical data
- **CryptoCompare**: 2000 data points per request
- **CoinMarketCap**: Limited historical data on free tier

### VPN Requirements
- **Hyperliquid**: May require VPN depending on location
- **Others**: Generally no VPN required

## 🔧 **Modification Guide**

To change what data you collect, modify these variables in each script:

```python
# Example from hyperliquid_historical_data.py
SYMBOL = 'ETH'               # Change to: BTC, SOL, ARB, AVAX, etc.
TIMEFRAME = '1h'             # Change to: 1m, 1h, 1d, 1w
SAVE_DIR = 'data/hyperliquid' # Change to your preferred directory
```

## 📚 **Related Documentation**

- `../DATA_COLLECTION_REFERENCE_GUIDE.md` - Complete reference guide
- `../DATA_SCRIPTS_QUICK_REFERENCE.md` - Quick modification guide
- `../data-streams/` - Real-time data monitoring scripts

## 🎯 **Best Practices**

1. **Start with Coinbase** for comprehensive historical data
2. **Use Hyperliquid** for specific trading pairs (5000 bar limit)
3. **Use CoinGecko** for free tier data (90-day limit)
4. **Use CryptoCompare** for higher free tier limits
5. **Always check rate limits** before running multiple scripts
6. **Save data in organized directories** by exchange and timeframe
