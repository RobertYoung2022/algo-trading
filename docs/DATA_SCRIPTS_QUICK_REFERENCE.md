# 📊 Data Scripts Quick Reference Guide

## 🔧 How to Modify Each Script

### 1. **Coinbase Data** (`coinbase_data_2025.py`)
```python
SYMBOL = 'BTC-USD'        # Change trading pair
TIMEFRAME = '5m'          # Change timeframe  
WEEKS = 70                # Change data range
```

**Popular Trading Pairs:**
- `'BTC-USD'`, `'ETH-USD'`, `'SOL-USD'`, `'ADA-USD'`, `'DOT-USD'`
- `'MATIC-USD'`, `'AVAX-USD'`, `'LINK-USD'`, `'UNI-USD'`, `'XRP-USD'`

**Timeframes:**
- `'1m'`, `'5m'`, `'15m'`, `'1h'`, `'6h'`, `'1d'`

**Data Range Examples:**
- `10` = 2.5 months, `26` = 6 months, `52` = 1 year, `104` = 2 years

---

### 2. **CoinGecko Data** (`coingecko_data.py`)
```python
COIN_ID = 'ethereum'      # Change coin ID
VS_CURRENCY = 'usd'       # Change currency
DAYS = 90                 # Change days
```

**Popular Coin IDs:**
- `'bitcoin'`, `'ethereum'`, `'solana'`, `'cardano'`, `'polkadot'`
- `'polygon'`, `'avalanche-2'`, `'chainlink'`, `'uniswap'`, `'ripple'`

**Currencies:**
- Fiat: `'usd'`, `'eur'`, `'gbp'`, `'jpy'`, `'cad'`, `'aud'`
- Crypto: `'btc'`, `'eth'`, `'bnb'`, `'ada'`, `'sol'`

**Days Examples:**
- `7` = 1 week, `30` = 1 month, `90` = 3 months, `365` = 1 year

---

### 3. **CryptoCompare Data** (`crypto_compare_data.py`)
```python
SYMBOL = 'ETH'            # Change symbol
VS_CURRENCY = 'USDT'      # Change currency
TIMEFRAME = 'day'         # Change timeframe
LIMIT = 100               # Change data points
```

**Popular Symbols:**
- `'BTC'`, `'ETH'`, `'SOL'`, `'ADA'`, `'DOT'`, `'MATIC'`
- `'AVAX'`, `'LINK'`, `'UNI'`, `'XRP'`, `'LTC'`, `'BCH'`

**Currencies:**
- Fiat: `'USD'`, `'EUR'`, `'GBP'`, `'JPY'`, `'CAD'`, `'AUD'`
- Crypto: `'USDT'`, `'USDC'`, `'BTC'`, `'ETH'`, `'BNB'`

**Timeframes:**
- `'minute'`, `'hour'`, `'day'`

**Limit Examples:**
- `50`, `100`, `500`, `1000`, `2000` (max)

---

### 4. **Hyperliquid Data** (`data_from_hl_2025.py`)
```python
SYMBOL = 'ETH'            # Change symbol
TIMEFRAME = '1h'          # Change timeframe
```

**Available Symbols:**
- `'BTC'`, `'ETH'`, `'SOL'`, `'ARB'`, `'AVAX'`, `'ATOM'`
- `'DOT'`, `'MATIC'`, `'LINK'`, `'UNI'`, `'XRP'`, `'ADA'`

**Timeframes:**
- `'1m'`, `'1h'`, `'1d'`, `'1w'`

**⚠️ LIMITATION: Max 5000 bars only!**

---

### 5. **CoinMarketCap Data** (`coin_market_cap_data.py`)
```python
SYMBOL = 'ETH'            # Change symbol
VS_CURRENCY = 'USD'       # Change currency
COUNT = 30                # Change data points
INTERVAL = 'daily'        # Change interval
```

**Popular Symbols:**
- `'BTC'`, `'ETH'`, `'SOL'`, `'ADA'`, `'DOT'`, `'MATIC'`
- `'AVAX'`, `'LINK'`, `'UNI'`, `'XRP'`, `'LTC'`, `'BCH'`

**Currencies:**
- Fiat: `'USD'`, `'EUR'`, `'GBP'`, `'JPY'`, `'CAD'`, `'AUD'`
- Crypto: `'BTC'`, `'ETH'`, `'BNB'`, `'ADA'`, `'SOL'`

**Intervals:**
- `'1h'`, `'2h'`, `'3h'`, `'4h'`, `'6h'`, `'8h'`, `'12h'`, `'daily'`, `'weekly'`, `'monthly'`

**⚠️ FREE TIER: Only latest quotes, no historical data!**

---

## 🚀 Quick Examples

### Get 1 year of daily Bitcoin data from Coinbase:
```python
# In coinbase_data_2025.py
SYMBOL = 'BTC-USD'
TIMEFRAME = '1d'
WEEKS = 52
```

### Get 6 months of hourly Ethereum data from CryptoCompare:
```python
# In crypto_compare_data.py
SYMBOL = 'ETH'
TIMEFRAME = 'hour'
LIMIT = 4320  # 6 months * 30 days * 24 hours
```

### Get 3 months of Solana data from CoinGecko:
```python
# In coingecko_data.py
COIN_ID = 'solana'
DAYS = 90
```

### Get recent Hyperliquid data (max 5000 bars):
```python
# In data_from_hl_2025.py
SYMBOL = 'SOL'
TIMEFRAME = '1h'  # This will give you ~208 days of hourly data
```

---

## 💡 Pro Tips

1. **For extensive historical data**: Use **Coinbase** (requires API key)
2. **For quick free data**: Use **CoinGecko** or **CryptoCompare**
3. **For Hyperliquid-specific data**: Use **Hyperliquid** (max 5000 bars)
4. **For real-time monitoring**: Use the **CMC Real-Time Monitor**

5. **File locations**: All data saves to `data/[provider]/` directories
6. **File naming**: Automatically includes symbol, timeframe, and date
7. **CSV format**: All files use standard OHLCV format with datetime index
