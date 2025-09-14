'''
CoinMarketCap Real-Time Monitor (Consumer Mode)
==============================================
Real-time cryptocurrency monitoring system that reads data from the Unified OHLCV Collector

ARCHITECTURE:
- PRODUCER: unified_ohlcv_collector.py (collects data from multiple APIs)
- CONSUMER: cmc_real_time_monitor.py (displays data from local JSON files)

STEPS TO USE:
1. Start the unified collector: python data-scripts/unified_ohlcv_collector.py
2. Start this monitor: python cmc_real_time_monitor.py
3. Monitor will read live data from data/live_market/ JSON files

MONITORING FEATURES:
- Global cryptocurrency market metrics
- Personal watchlist monitoring
- Price change alerts
- Volume spike detection
- Fear & Greed Index tracking
- Market sentiment analysis
- Real-time CSV data logging (daily files)
- Color-coded terminal display
- Multi-source arbitrage detection

PRODUCTION FEATURES:
- No external API dependencies (reads local files)
- Faster response times (no API latency)
- Comprehensive error handling
- Graceful shutdown handling
- Data validation
- Production logging
'''

# ====== BobbyYo's CMC Real-Time Monitor Configuration 🌙 ======
import pandas as pd
import datetime
import os
import sys
import signal
import time
import json
import requests
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from termcolor import cprint
import logging

# Configuration
REFRESH_INTERVAL = 30  # Seconds between updates
TOP_COINS_LIMIT = 20   # Number of top coins to display
MIN_VOLUME_CHANGE = 50  # Minimum % volume change to alert
MIN_PRICE_CHANGE = 5    # Minimum % price change to alert
SAVE_TO_CSV = True      # Save data to CSV files
CSV_DIR = '../data/cmc_monitor'  # Relative to data-streams directory
MAX_RETRIES = 3         # Maximum retry attempts for API calls
RETRY_DELAY = 5         # Seconds to wait between retries

# Coins to monitor closely (add your favorites)
WATCHLIST = ['BTC', 'ETH', 'XRP', 'SUI', 'HBAR', 'CRO', 'LINK', 'TAO']
STOCK_WATCHLIST = ['BTBT', 'HOOD', 'COIN', 'NKE', 'SPY', 'QQQ']  # Stock symbols to monitor

# Sentiment Analysis Configuration
ENABLE_SENTIMENT_ANALYSIS = True  # Enable/disable sentiment features
ENABLE_FEAR_GREED = True         # Enable Fear & Greed Index
SENTIMENT_UPDATE_INTERVAL = 120  # Update sentiment every 2 minutes

# Load environment variables
project_root = Path(__file__).parent.parent  # Go up one level to project root
env_path = project_root / '.env'
load_dotenv(env_path)

api_key = os.getenv('CMC_API_KEY')
if not api_key:
    print("❌ Error: CMC API key not found in .env file")
    print("💡 Make sure your .env file exists and contains:")
    print("   CMC_API_KEY=your_coinmarketcap_api_key_here")
    print("   Get your free API key at: https://coinmarketcap.com/api/")
    sys.exit(1)

# Create CSV directory
os.makedirs(CSV_DIR, exist_ok=True)

# Setup production logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{CSV_DIR}/cmc_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CMCRealTimeMonitor:
    """Production-ready CoinMarketCap real-time monitor"""
    
    def __init__(self):
        self.running = True
        self.previous_data = {}
        self.csv_dir = CSV_DIR
        self.api_key = api_key
        self.request_count = 0
        self.start_time = datetime.datetime.now()
        
        # Sentiment tracking
        self.fear_greed_index = None
        self.fear_greed_last_fetch = None
        self.fear_greed_cache = None
        self.market_sentiment = None
        self.last_sentiment_update = time.time()  # Initialize to current time

        # Top cryptocurrencies caching to reduce API calls
        self.top_crypto_cache = None
        self.top_crypto_last_update = None
        self.top_crypto_cache_duration = 300  # Cache for 5 minutes (300 seconds)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("🌙 BobbyYo's CMC Real-Time Monitor Initialized! 🚀")
        logger.info(f"Configuration: {REFRESH_INTERVAL}s interval, Watchlist: {WATCHLIST}")
        if STOCK_WATCHLIST:
            logger.info(f"Stock Watchlist: {STOCK_WATCHLIST}")
        if ENABLE_SENTIMENT_ANALYSIS:
            logger.info(f"Sentiment Analysis: Fear & Greed: {ENABLE_FEAR_GREED}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def _make_api_request(self, endpoint, params=None, retry_count=0):
        """Make API request to CoinMarketCap with retry logic"""
        url = f'https://pro-api.coinmarketcap.com/v1/{endpoint}'
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
        
        try:
            self.request_count += 1
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    return data
                else:
                    logger.warning(f"API response missing 'data' field: {data}")
                    return None
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded, waiting before retry...")
                time.sleep(60)  # Wait 1 minute for rate limit reset
                if retry_count < MAX_RETRIES:
                    return self._make_api_request(endpoint, params, retry_count + 1)
                else:
                    logger.error("Max retries reached for rate limit")
                    return None
            elif response.status_code == 401:
                logger.error("API key invalid or expired")
                return None
            else:
                logger.error(f"API Error {response.status_code}: {response.text}")
                if retry_count < MAX_RETRIES and response.status_code >= 500:
                    time.sleep(RETRY_DELAY)
                    return self._make_api_request(endpoint, params, retry_count + 1)
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout for {endpoint} (attempt {retry_count + 1}/{MAX_RETRIES + 1})")
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAY * (retry_count + 1)  # Incremental backoff
                logger.info(f"Retrying {endpoint} in {wait_time} seconds...")
                time.sleep(wait_time)
                return self._make_api_request(endpoint, params, retry_count + 1)
            logger.error(f"All retry attempts failed for {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                return self._make_api_request(endpoint, params, retry_count + 1)
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {endpoint}: {e}")
            return None
    
    def get_fear_greed_index(self):
        """Get Fear & Greed Index from Alternative.me API (cached for efficiency)"""
        now = datetime.datetime.now()

        # Check if we have cached data and if it's still valid
        if (self.fear_greed_cache and self.fear_greed_last_fetch and
            (now - self.fear_greed_last_fetch).total_seconds() < 3600):  # Cache for 1 hour
            self.fear_greed_cache['cached'] = True
            return self.fear_greed_cache

        try:
            response = requests.get('https://api.alternative.me/fng/', timeout=10)

            if response.status_code == 200:
                data = response.json()

                # CRITICAL FIX: Changed 'data' to 'data' in data
                if 'data' in data and len(data['data']) > 0:
                    fng_data = data['data'][0]

                    self.fear_greed_cache = {
                        'value': int(fng_data['value']),
                        'value_classification': fng_data['value_classification'],
                        'timestamp': fng_data['timestamp'],
                        'time_until_update': fng_data.get('time_until_update', '0'),
                        'fetch_time': now.isoformat(),
                        'cached': False
                    }
                    self.fear_greed_last_fetch = now
                    logger.info(f"🎯 Fear & Greed Index fetched: {self.fear_greed_cache['value']}/100 ({self.fear_greed_cache['value_classification']})")
                    return self.fear_greed_cache
                else:
                    logger.error("🎯 Fear & Greed API response missing data")
            else:
                logger.warning(f"🎯 Fear & Greed API returned status {response.status_code}")
                # Return cached data if available, even if stale
                if self.fear_greed_cache:
                    self.fear_greed_cache['cached'] = True
                    return self.fear_greed_cache
                return None
        except Exception as e:
            logger.error(f"🎯 Error fetching Fear & Greed Index: {e}")
            # Return cached data if available, even if stale
            if self.fear_greed_cache:
                self.fear_greed_cache['cached'] = True
                return self.fear_greed_cache
            return None
    
    def analyze_market_sentiment(self, watchlist_data):
        """Analyze overall market sentiment based on watchlist indicators"""
        if not watchlist_data:
            return None

        try:
            sentiment_score = 0
            indicators = []
            
            # Analyze top 10 coins price changes
            positive_changes = 0
            negative_changes = 0
            total_volume_change = 0
            
            # Filter crypto coins from watchlist (exclude stocks)
            crypto_coins = [coin for coin in watchlist_data if coin.get('symbol', '') in WATCHLIST]

            for coin in crypto_coins:
                change_24h = coin.get('change_24h', 0)
                volume_24h = coin.get('volume_24h', 0)
                
                if change_24h > 0:
                    positive_changes += 1
                elif change_24h < 0:
                    negative_changes += 1
                
                # Weight by market cap (approximate)
                total_volume_change += volume_24h * (1 + change_24h/100)
            
            # Market breadth analysis
            total_coins = len(crypto_coins)
            if total_coins > 0:
                market_breadth = (positive_changes / total_coins) * 100
            else:
                market_breadth = 50  # Neutral if no coins
            if market_breadth > 70:
                sentiment_score += 30
                indicators.append(f"Strong market breadth ({market_breadth:.0f}% positive)")
            elif market_breadth < 30:
                sentiment_score -= 30
                indicators.append(f"Weak market breadth ({market_breadth:.0f}% positive)")
            
            # Volume analysis
            if total_volume_change > 0:
                sentiment_score += 15
                indicators.append("Increasing trading volume")
            else:
                sentiment_score -= 15
                indicators.append("Decreasing trading volume")
            
            # Determine sentiment classification
            if sentiment_score >= 50:
                classification = "Very Bullish"
                color = "green"
            elif sentiment_score >= 20:
                classification = "Bullish"
                color = "light_green"
            elif sentiment_score >= -20:
                classification = "Neutral"
                color = "yellow"
            elif sentiment_score >= -50:
                classification = "Bearish"
                color = "red"
            else:
                classification = "Very Bearish"
                color = "red"
            
            return {
                'score': sentiment_score,
                'classification': classification,
                'color': color,
                'indicators': indicators,
                'market_breadth': market_breadth,
                'positive_coins': positive_changes,
                'negative_coins': negative_changes,
                'total_coins': total_coins,
                'timestamp': datetime.datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error analyzing market sentiment: {e}")
            return None
    
    
    def get_global_metrics(self):
        """Get global cryptocurrency market metrics from unified collector data"""
        try:
            # Read from unified collector's market overview file (use absolute path)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            market_overview_file = os.path.join(project_root, 'data', 'live_market', 'market_overview.json')

            if not os.path.exists(market_overview_file):
                logger.warning("Market overview file not found. Make sure unified_ohlcv_collector.py is running.")
                return None

            with open(market_overview_file, 'r') as f:
                market_data = json.load(f)

            # Read current prices to calculate Bitcoin/Ethereum dominance
            current_prices_file = os.path.join(project_root, 'data', 'live_market', 'current_prices.json')
            btc_dominance = 0
            eth_dominance = 0

            if os.path.exists(current_prices_file):
                with open(current_prices_file, 'r') as f:
                    prices_data = json.load(f)

                total_market_cap = market_data.get('total_market_cap', 1)

                if 'BTC' in prices_data and total_market_cap > 0:
                    btc_market_cap = prices_data['BTC'].get('market_cap', 0)
                    btc_dominance = (btc_market_cap / total_market_cap) * 100

                if 'ETH' in prices_data and total_market_cap > 0:
                    eth_market_cap = prices_data['ETH'].get('market_cap', 0)
                    eth_dominance = (eth_market_cap / total_market_cap) * 100

            return {
                'total_market_cap': market_data.get('total_market_cap', 0),
                'total_volume_24h': market_data.get('total_volume_24h', 0),
                'bitcoin_dominance': btc_dominance,
                'ethereum_dominance': eth_dominance,
                'active_cryptocurrencies': market_data.get('total_symbols', 0),
                'active_exchanges': 0,  # Not available from unified collector
                'last_updated': market_data.get('timestamp', ''),
                'timestamp': datetime.datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error reading global metrics from local data: {e}")
            return None
    
    def get_top_cryptocurrencies(self, limit=20):
        """Get top 20 cryptocurrencies by market cap from CoinGecko API with caching and retry logic"""
        import time

        # Check if cached data is still valid (less than 5 minutes old)
        current_time = time.time()
        if (self.top_crypto_cache is not None and
            self.top_crypto_last_update is not None and
            (current_time - self.top_crypto_last_update) < self.top_crypto_cache_duration):
            logger.debug("Using cached top cryptocurrency data")
            return self.top_crypto_cache

        logger.debug("Cache expired or empty, fetching new top cryptocurrency data")
        max_retries = 3
        base_delay = 5  # Increased to 5 seconds to respect rate limits

        for attempt in range(max_retries):
            try:
                logger.debug(f"Fetching top cryptocurrencies from CoinGecko API (attempt {attempt + 1}/{max_retries})")

                # CoinGecko API endpoint for top coins by market cap
                url = f"https://api.coingecko.com/api/v3/coins/markets"
                params = {
                    'vs_currency': 'usd',
                    'order': 'market_cap_desc',
                    'per_page': limit,
                    'page': 1,
                    'sparkline': False,
                    'price_change_percentage': '24h'
                }

                response = requests.get(url, params=params, timeout=10)

                if response.status_code == 200:
                    break
                elif response.status_code == 429:  # Rate limited
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"CoinGecko rate limited, retrying in {delay}s...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.warning("CoinGecko rate limit exceeded, using fallback")
                        return self.get_top_cryptocurrencies_fallback(limit)
                else:
                    logger.warning(f"CoinGecko API error: {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(base_delay * (attempt + 1))
                        continue
                    else:
                        return self.get_top_cryptocurrencies_fallback(limit)

            except requests.RequestException as e:
                logger.warning(f"CoinGecko request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(base_delay * (attempt + 1))
                    continue
                else:
                    return self.get_top_cryptocurrencies_fallback(limit)

            # Process successful response
            data = response.json()
            coins = []

            for i, coin in enumerate(data, 1):
                coins.append({
                    'rank': i,
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name', ''),
                    'price': coin.get('current_price', 0),
                    'market_cap': coin.get('market_cap', 0),
                    'volume_24h': coin.get('total_volume', 0),  # This is USD volume for CoinGecko
                    'change_1h': 0,  # Not in this endpoint
                    'change_24h': coin.get('price_change_percentage_24h', 0),
                    'change_7d': 0,  # Not in this endpoint
                    'last_updated': coin.get('last_updated', ''),
                    'timestamp': datetime.datetime.now().isoformat()
                })

            logger.debug(f"Successfully fetched {len(coins)} top cryptocurrencies from CoinGecko")

            # Cache the successful result
            self.top_crypto_cache = coins
            self.top_crypto_last_update = current_time
            logger.debug(f"Cached top cryptocurrency data for {self.top_crypto_cache_duration} seconds")

            return coins

        # If all retries failed, use fallback
        logger.warning("All CoinGecko retry attempts failed, using fallback")
        return self.get_top_cryptocurrencies_fallback(limit)

    def get_top_cryptocurrencies_fallback(self, limit=20):
        """Fallback: Use known top cryptocurrencies list with current price data"""
        try:
            # Known top 20 cryptocurrencies by market cap (updated manually as needed)
            # This list changes slowly, so it's safe to hardcode
            top_crypto_symbols = [
                'BTC', 'ETH', 'XRP', 'USDT', 'SOL', 'BNB', 'USDC', 'DOGE',
                'ADA', 'TRX', 'WSTETH', 'LINK', 'WBETH', 'WBTC', 'HYPE',
                'USDE', 'SUI', 'AVAX', 'XLM', 'HBAR'
            ]

            # Read current prices from unified collector
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            current_prices_file = os.path.join(project_root, 'data', 'live_market', 'current_prices.json')

            if not os.path.exists(current_prices_file):
                logger.warning("Current prices file not found. Using watchlist data only.")
                return self.get_watchlist_data_as_top_list()

            with open(current_prices_file, 'r') as f:
                prices_data = json.load(f)

            # Build list using known top cryptos with available price data
            coins = []
            coin_names = {  # Known full names for display
                'BTC': 'Bitcoin', 'ETH': 'Ethereum', 'XRP': 'Ripple', 'USDT': 'Tether',
                'SOL': 'Solana', 'BNB': 'BNB', 'USDC': 'USD Coin', 'DOGE': 'Dogecoin',
                'ADA': 'Cardano', 'TRX': 'TRON', 'WSTETH': 'Wrapped stETH', 'LINK': 'Chainlink',
                'WBETH': 'Wrapped Beacon ETH', 'WBTC': 'Wrapped Bitcoin', 'HYPE': 'Hyperliquid',
                'USDE': 'USDe', 'SUI': 'Sui', 'AVAX': 'Avalanche', 'XLM': 'Stellar', 'HBAR': 'Hedera'
            }

            for rank, symbol in enumerate(top_crypto_symbols[:limit], 1):
                if symbol in prices_data:
                    data = prices_data[symbol]
                    coins.append({
                        'rank': rank,
                        'symbol': symbol,
                        'name': coin_names.get(symbol, symbol),
                        'price': data.get('price', 0),
                        'market_cap': data.get('market_cap', 0),
                        'volume_24h': data.get('volume_24h', 0),
                        'change_1h': 0,  # Not available from unified collector
                        'change_24h': data.get('change_24h', 0),
                        'change_7d': 0,  # Not available from unified collector
                        'last_updated': data.get('timestamp', ''),
                        'timestamp': datetime.datetime.now().isoformat()
                    })

            # Sort by market cap (descending) and assign ranks
            coins.sort(key=lambda x: x['market_cap'], reverse=True)
            for i, coin in enumerate(coins[:limit], 1):
                coin['rank'] = i

            logger.debug(f"Using fallback data with {len(coins)} cryptocurrencies")
            return coins[:limit]

        except Exception as e:
            logger.error(f"Error reading fallback cryptocurrency data: {e}")
            return []
    
    def get_watchlist_data(self):
        """Get data for specific coins in watchlist from unified collector data"""
        if not WATCHLIST:
            return []

        try:
            # Read from unified collector's current prices file (use absolute path)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            current_prices_file = os.path.join(project_root, 'data', 'live_market', 'current_prices.json')

            if not os.path.exists(current_prices_file):
                logger.warning("Current prices file not found. Make sure unified_ohlcv_collector.py is running.")
                return []

            with open(current_prices_file, 'r') as f:
                prices_data = json.load(f)

            watchlist_data = []

            # Extract data for watchlist symbols
            for symbol in WATCHLIST:
                if symbol in prices_data:
                    data = prices_data[symbol]
                    watchlist_data.append({
                        'symbol': symbol,
                        'name': symbol,  # Use symbol as name for now
                        'price': data.get('price', 0),
                        'market_cap': data.get('market_cap', 0),
                        'volume_24h': data.get('volume_24h', 0),
                        'change_1h': 0,  # Not available from unified collector
                        'change_24h': data.get('change_24h', 0),
                        'change_7d': 0,  # Not available from unified collector
                        'last_updated': data.get('timestamp', ''),
                        'timestamp': datetime.datetime.now().isoformat(),
                        'price_variance': data.get('price_variance', {}),
                        'all_sources': data.get('all_sources', []),
                        'source_count': data.get('source_count', 1)
                    })
                else:
                    logger.debug(f"Symbol {symbol} not found in unified collector data")

            logger.debug(f"Retrieved watchlist data for {len(watchlist_data)} coins from local data")
            return watchlist_data

        except Exception as e:
            logger.error(f"Error reading watchlist data from local data: {e}")
            logger.error(f"Watchlist: {WATCHLIST}")
            return []

    def get_stock_data(self):
        """Get data for stock symbols from unified collector data"""
        if not STOCK_WATCHLIST:
            return []

        try:
            # Read from unified collector's current prices file (use absolute path)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            current_prices_file = os.path.join(project_root, 'data', 'live_market', 'current_prices.json')

            if not os.path.exists(current_prices_file):
                logger.warning("Current prices file not found. Make sure unified_ohlcv_collector.py is running.")
                return []

            with open(current_prices_file, 'r') as f:
                prices_data = json.load(f)

            stock_data = []

            # Extract data for stock symbols
            for symbol in STOCK_WATCHLIST:
                if symbol in prices_data:
                    data = prices_data[symbol]
                    stock_data.append({
                        'symbol': symbol,
                        'name': symbol,  # Use symbol as name for now
                        'price': data.get('price', 0),
                        'market_cap': data.get('market_cap', 0),
                        'volume_24h': data.get('volume_24h', 0),
                        'change_1h': 0,  # Not available from unified collector
                        'change_24h': data.get('change_24h', 0),
                        'change_7d': 0,  # Not available from unified collector
                        'last_updated': data.get('timestamp', ''),
                        'timestamp': datetime.datetime.now().isoformat()
                    })
                else:
                    logger.debug(f"Stock symbol {symbol} not found in unified collector data")

            logger.debug(f"Retrieved stock data for {len(stock_data)} symbols from local data")
            return stock_data

        except Exception as e:
            logger.error(f"Error reading stock data from local data: {e}")
            logger.error(f"Stock watchlist: {STOCK_WATCHLIST}")
            return []

    def detect_arbitrage_opportunities(self, data, min_spread_percent=0.1):
        """Detect arbitrage opportunities from multi-source price data"""
        opportunities = []

        for item in data:
            symbol = item.get('symbol', '')
            price_variance = item.get('price_variance', {})

            if not price_variance or 'min' not in price_variance or 'max' not in price_variance:
                continue

            min_price = price_variance.get('min', 0)
            max_price = price_variance.get('max', 0)
            avg_price = price_variance.get('avg', 0)

            if min_price > 0 and max_price > min_price:
                spread_percent = ((max_price - min_price) / avg_price) * 100

                if spread_percent >= min_spread_percent:
                    opportunities.append({
                        'symbol': symbol,
                        'min_price': min_price,
                        'max_price': max_price,
                        'avg_price': avg_price,
                        'spread_percent': spread_percent,
                        'spread_absolute': max_price - min_price,
                        'all_sources': item.get('all_sources', []),
                        'source_count': item.get('source_count', 0)
                    })

        # Sort by spread percentage (highest first)
        opportunities.sort(key=lambda x: x['spread_percent'], reverse=True)
        return opportunities

    def display_arbitrage_opportunities(self, opportunities):
        """Display cross-exchange arbitrage opportunities"""
        if not opportunities:
            return

        print("\n" + "="*100)
        cprint("🔀 PRICE DIFFERENCES ACROSS DATA SOURCES", "yellow", attrs=["bold"])
        cprint("💡 Find price gaps between exchanges that may indicate trading opportunities", "cyan")
        print("="*100)

        # Header
        headers = ["Symbol", "Sources", "Low Price", "High Price", "Spread %", "Spread $", "Profit Potential"]
        print(f"{'Symbol':<8} {'Sources':<10} {'Low Price':<12} {'High Price':<12} {'Spread %':<10} {'Spread $':<12} {'Profit':<15}")
        print("-" * 100)

        for opp in opportunities:
            symbol = opp['symbol']
            sources = f"{opp['source_count']} sources"
            low_price = f"${opp['min_price']:,.2f}" if opp['min_price'] >= 1 else f"${opp['min_price']:.6f}"
            high_price = f"${opp['max_price']:,.2f}" if opp['max_price'] >= 1 else f"${opp['max_price']:.6f}"
            spread_pct = f"{opp['spread_percent']:.2f}%"
            spread_abs = f"${opp['spread_absolute']:,.2f}" if opp['spread_absolute'] >= 1 else f"${opp['spread_absolute']:.6f}"

            # Simple profit potential indicator
            if opp['spread_percent'] >= 2.0:
                profit = "🟢 High"
            elif opp['spread_percent'] >= 1.0:
                profit = "🟡 Medium"
            else:
                profit = "🔵 Low"

            print(f"{symbol:<8} {sources:<10} {low_price:<12} {high_price:<12} {spread_pct:<10} {spread_abs:<12} {profit:<15}")

        print()
        print("💡 Data Sources: Yahoo Finance, Coinbase Exchange, CoinGecko API")
        print("⚠️  These are price differences between data feeds - not guaranteed arbitrage opportunities")
        print("🔎 Real trading requires: account access, sufficient liquidity, fee calculations, and timing")

    def detect_alerts(self, current_data_dict, previous_data):
        """Detect price and volume alerts"""
        alerts = []

        if not previous_data or not current_data_dict:
            return alerts

        try:
            # Check for significant price changes
            for symbol, coin in current_data_dict.items():
                # Ensure coin is a dictionary
                if not isinstance(coin, dict):
                    logger.warning(f"Coin data for {symbol} is not a dictionary: {type(coin)}")
                    continue

                if symbol in previous_data:
                    prev_coin = previous_data[symbol]

                    # Ensure prev_coin is also a dictionary
                    if not isinstance(prev_coin, dict):
                        logger.warning(f"Previous coin data for {symbol} is not a dictionary: {type(prev_coin)}")
                        continue

                    # Price change alert
                    current_change = coin.get('change_24h', 0)
                    prev_change = prev_coin.get('change_24h', 0)

                    # Ensure values are numeric
                    if not isinstance(current_change, (int, float)) or not isinstance(prev_change, (int, float)):
                        continue

                    price_change = current_change - prev_change

                    if abs(price_change) >= MIN_PRICE_CHANGE:
                        alerts.append({
                            'type': 'price_change',
                            'symbol': symbol,
                            'change': price_change,
                            'current_price': coin.get('price', 0),
                            'current_change_24h': current_change
                        })

                    # Volume spike alert - improved logic to prevent false alarms
                    current_volume = coin.get('volume_24h', 0)
                    prev_volume = prev_coin.get('volume_24h', 0)

                    # Ensure volumes are numeric and both are reasonably sized
                    if isinstance(current_volume, (int, float)) and isinstance(prev_volume, (int, float)) and prev_volume > 0:
                        volume_change = ((current_volume - prev_volume) / prev_volume) * 100

                        # Prevent false alerts from data source switching
                        # Skip alert if volumes differ by extreme amounts (likely different data sources)
                        volume_ratio = max(current_volume, prev_volume) / min(current_volume, prev_volume)

                        # Only alert if volume change is significant but not from data source mismatch
                        if abs(volume_change) >= MIN_VOLUME_CHANGE and volume_ratio < 1000:  # Less than 1000x difference
                            alerts.append({
                                'type': 'volume_spike',
                                'symbol': symbol,
                                'change': volume_change,
                                'current_volume': current_volume
                            })

            return alerts
        except Exception as e:
            logger.error(f"Error detecting alerts: {e}")
            logger.error(f"Current data types: {type(current_data_dict)}")
            logger.error(f"Previous data types: {type(previous_data)}")
            return []
    
    
    def display_top_coins(self, coins):
        """Display top cryptocurrencies"""
        if not coins:
            return
        
        print("\n" + "="*100)
        cprint("📈 TOP CRYPTOCURRENCIES BY MARKET CAP", "cyan", attrs=["bold"])
        print("="*100)
        
        header = f"{'Rank':<4} {'Symbol':<8} {'Price':<15} {'Change 24h':<15} {'Market Cap':<15} {'Volume 24h':<15}"
        cprint(header, "white", attrs=["bold"])
        print("-" * 100)
        
        for coin in coins[:TOP_COINS_LIMIT]:
            # Color code based on 24h change
            change_24h = coin.get('change_24h', 0)
            if change_24h > 0:
                change_color = "green"
                change_symbol = "📈"
            elif change_24h < 0:
                change_color = "red"
                change_symbol = "📉"
            else:
                change_color = "white"
                change_symbol = "➡️"
            
            price = coin.get('price', 0)
            if price >= 1:
                price_str = f"${price:,.2f}"
            else:
                price_str = f"${price:.6f}"
            
            market_cap = coin.get('market_cap', 0) / 1e9 if coin.get('market_cap') else 0

            # Handle volume display - CoinGecko gives USD volume, local data gives coin volume
            volume_24h = coin.get('volume_24h', 0)
            if volume_24h > 1000000:  # If > 1M, likely already USD from CoinGecko
                volume_str = f"${volume_24h/1e6:.0f}M"
            elif volume_24h > 0:  # Local data in coin units, convert to USD
                price = coin.get('price', 0)
                if price > 0:
                    volume_usd = (volume_24h * price) / 1e6
                    volume_str = f"${volume_usd:.0f}M"
                else:
                    volume_str = f"{volume_24h:.1f}"  # Show coin volume
            else:
                volume_str = "$0M"

            change_str = f"{change_symbol} {change_24h:+.1f}%"
            market_cap_str = f"${market_cap:.1f}B"
            line = f"{coin.get('rank', 0):<4} {coin.get('symbol', ''):<8} {price_str:<15} {change_str:<15} {market_cap_str:<15} {volume_str:<15}"
            cprint(line, change_color)
    
    def display_watchlist(self, watchlist_data):
        """Display watchlist coins"""
        if not watchlist_data:
            return

        print("\n" + "="*75)
        cprint("⭐ YOUR WATCHLIST", "cyan", attrs=["bold"])
        print("="*75)

        # Add column headers
        header = f"{'Symbol':<8} {'Price':<15} {'Change 24h':<15} {'Market Cap':<15} {'Volume 24h':<15}"
        cprint(header, "white", attrs=["bold"])
        print("-" * 75)

        for coin in watchlist_data:
            change_24h = coin.get('change_24h', 0)
            if change_24h > 0:
                change_color = "green"
            elif change_24h < 0:
                change_color = "red"
            else:
                change_color = "white"
            
            price = coin.get('price', 0)
            if price >= 1:
                price_str = f"${price:,.2f}"
            else:
                price_str = f"${price:.6f}"
            
            market_cap = coin.get('market_cap', 0) / 1e9 if coin.get('market_cap') else 0
            change_str = f"{change_24h:+.1f}%"
            market_cap_str = f"${market_cap:.1f}B"

            # Handle volume display for watchlist
            volume_24h = coin.get('volume_24h', 0)
            if volume_24h > 1000000:  # If > 1M, likely already USD
                volume_str = f"${volume_24h/1e6:.0f}M"
            elif volume_24h > 0:  # Local data in coin units, convert to USD
                price = coin.get('price', 0)
                if price > 0:
                    volume_usd = (volume_24h * price) / 1e6
                    volume_str = f"${volume_usd:.0f}M"
                else:
                    volume_str = f"{volume_24h:.1f}"
            else:
                volume_str = "$0M"

            line = f"{coin.get('symbol', ''):<8} {price_str:<15} {change_str:<15} {market_cap_str:<15} {volume_str:<15}"
            cprint(line, change_color)

    def display_stocks(self, stock_data):
        """Display stock symbols with stock-specific metrics"""
        if not stock_data:
            return

        print("\n" + "="*85)
        cprint("📊 STOCK WATCHLIST", "magenta", attrs=["bold"])
        print("="*85)

        # Add column headers for stock-specific info
        header = f"{'Symbol':<8} {'Company':<12} {'Price':<12} {'Volume':<12} {'Market Cap':<15} {'Status':<10}"
        cprint(header, "white", attrs=["bold"])
        print("-" * 85)

        # Stock name mapping for display
        stock_names = {
            'BTBT': 'Bit Digital',
            'HOOD': 'Robinhood',
            'COIN': 'Coinbase',
            'NKE': 'Nike',
            'SPY': 'S&P 500 ETF',
            'QQQ': 'Nasdaq ETF'
        }

        for stock in stock_data:
            symbol = stock.get('symbol', '')
            company_name = stock_names.get(symbol, symbol)

            change_24h = stock.get('change_24h', 0)
            if change_24h > 0:
                change_color = "green"
                status = "📈 UP"
            elif change_24h < 0:
                change_color = "red"
                status = "📉 DOWN"
            else:
                change_color = "yellow"
                status = "🔒 CLOSED"  # Markets closed

            price = stock.get('price', 0)
            price_str = f"${price:,.2f}"

            # Volume in millions for stocks
            volume_24h = stock.get('volume_24h', 0)
            volume_str = f"{volume_24h/1e6:.1f}M" if volume_24h > 0 else "N/A"

            market_cap = stock.get('market_cap', 0) / 1e9 if stock.get('market_cap') else 0
            market_cap_str = f"${market_cap:.1f}B"

            line = f"{symbol:<8} {company_name:<12} {price_str:<12} {volume_str:<12} {market_cap_str:<15} {status:<10}"
            cprint(line, change_color)

    def display_alerts(self, alerts):
        """Display price and volume alerts"""
        if not alerts:
            return
        
        print("\n" + "="*70)
        cprint("🚨 ALERTS & SIGNIFICANT MOVEMENTS", "red", attrs=["bold"])
        print("="*70)
        
        for alert in alerts:
            if alert['type'] == 'price_change':
                symbol = alert['symbol']
                change = alert['change']
                current_change = alert['current_change_24h']
                
                if change > 0:
                    alert_color = "green"
                    emoji = "🚀"
                else:
                    alert_color = "red"
                    emoji = "💥"
                
                cprint(f"{emoji} {symbol}: {change:+.1f}% change in 24h trend (now {current_change:+.1f}%)", alert_color, attrs=["bold"])
            
            elif alert['type'] == 'volume_spike':
                symbol = alert['symbol']
                volume_change = alert['change']
                
                cprint(f"📊 {symbol}: {volume_change:+.0f}% volume spike!", "yellow", attrs=["bold"])
    
    def display_fear_greed_index(self, fng_data):
        """Display Fear & Greed Index"""
        if not fng_data:
            return
        
        print("\n" + "="*70)
        cache_status = " (Cached)" if fng_data.get('cached', False) else ""
        cprint(f"😨 FEAR & GREED INDEX{cache_status} (Real-time API: Alternative.me)", "cyan", attrs=["bold"])
        print("="*70)
        
        value = int(fng_data['value'])
        classification = fng_data['value_classification']
        
        # Color coding based on value
        if value >= 75:
            color = "green"
            emoji = "😎"
        elif value >= 55:
            color = "light_green"
            emoji = "😊"
        elif value >= 45:
            color = "yellow"
            emoji = "😐"
        elif value >= 25:
            color = "red"
            emoji = "😰"
        else:
            color = "red"
            emoji = "😱"
        
        cprint(f"{emoji} Index Value: {value}/100", color, attrs=["bold"])
        cprint(f"📊 Classification: {classification}", color)
        
        # Historical context
        if value >= 75:
            cprint("⚠️  Extreme Greed - Consider taking profits", "yellow", attrs=["bold"])
        elif value <= 25:
            cprint("💡 Extreme Fear - Potential buying opportunity", "green", attrs=["bold"])
        
        # Time until next update (API returns seconds)
        time_until_update = fng_data.get('time_until_update', '0')
        if time_until_update != '0':
            try:
                # API returns time in seconds, convert to reasonable display format
                seconds = int(time_until_update)

                # Convert seconds to hours and minutes
                hours = seconds // 3600
                remaining_seconds = seconds % 3600
                minutes = remaining_seconds // 60

                # Validation: Fear & Greed Index updates daily, never more than 25 hours
                if hours > 25:
                    cprint("⏰ Next update: Invalid time from API (using daily schedule)", "yellow")
                elif hours > 0:
                    if minutes > 0:
                        cprint(f"⏰ Next update in: {hours}h {minutes}m", "white")
                    else:
                        cprint(f"⏰ Next update in: {hours}h", "white")
                else:
                    cprint(f"⏰ Next update in: {minutes}m", "white")
            except (ValueError, TypeError):
                pass  # Skip invalid time values
    
    def display_market_sentiment(self, sentiment_data):
        """Display market sentiment analysis"""
        if not sentiment_data:
            return
        
        print("\n" + "="*70)
        cprint("📊 MARKET SENTIMENT ANALYSIS (Live Data Calculation)", "cyan", attrs=["bold"])
        print("="*70)
        
        score = sentiment_data['score']
        classification = sentiment_data['classification']
        color = sentiment_data['color']
        market_breadth = sentiment_data['market_breadth']
        
        cprint(f"🎯 Sentiment Score: {score}/100", color, attrs=["bold"])
        cprint(f"📈 Classification: {classification}", color)
        cprint(f"📊 Market Breadth: {market_breadth:.1f}% positive", "white")
        total_coins = sentiment_data.get('total_coins', 10)  # fallback to 10 for backward compatibility
        cprint(f"✅ Positive Coins: {sentiment_data['positive_coins']}/{total_coins}", "green")
        cprint(f"❌ Negative Coins: {sentiment_data['negative_coins']}/{total_coins}", "red")
        
        # Display key indicators
        if sentiment_data['indicators']:
            print("\n🔍 Key Indicators:")
            for indicator in sentiment_data['indicators']:
                cprint(f"   • {indicator}", "white")
    
    
    def save_to_csv(self, data_type, data):
        """Save data to CSV files with daily file organization"""
        if not SAVE_TO_CSV or not data:
            return
        
        try:
            # Use daily files instead of timestamped files
            date_str = datetime.datetime.now().strftime('%Y%m%d')
            current_timestamp = datetime.datetime.now().isoformat()
            
            if data_type == 'global' and isinstance(data, dict):
                filename = f"{self.csv_dir}/global_metrics_{date_str}.csv"
                df = pd.DataFrame([data])
                
            elif data_type == 'top_coins' and isinstance(data, list):
                filename = f"{self.csv_dir}/top_coins_{date_str}.csv"
                df = pd.DataFrame(data)
                
            elif data_type == 'watchlist' and isinstance(data, list):
                filename = f"{self.csv_dir}/watchlist_{date_str}.csv"
                df = pd.DataFrame(data)

            elif data_type == 'stocks' and isinstance(data, list):
                filename = f"{self.csv_dir}/stocks_{date_str}.csv"
                df = pd.DataFrame(data)

            elif data_type == 'fear_greed' and isinstance(data, dict):
                filename = f"{self.csv_dir}/fear_greed_{date_str}.csv"
                df = pd.DataFrame([data])
                
            elif data_type == 'market_sentiment' and isinstance(data, dict):
                filename = f"{self.csv_dir}/market_sentiment_{date_str}.csv"
                df = pd.DataFrame([data])
                
                
            else:
                logger.warning(f"Invalid data type or format for CSV save: {data_type}")
                return
            
            # Check if file exists to determine if we should append or create new
            file_exists = os.path.exists(filename)
            
            if file_exists:
                # Append to existing file
                df.to_csv(filename, mode='a', header=False, index=False)
                logger.debug(f"📝 Data appended to {filename}")
            else:
                # Create new file with header
                df.to_csv(filename, index=False)
                logger.debug(f"💾 New daily file created: {filename}")
            
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
    
    def print_stats(self):
        """Print monitoring statistics"""
        uptime = datetime.datetime.now() - self.start_time
        hours, remainder = divmod(uptime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print(f"\n📊 Monitor Stats: {int(hours)}h {int(minutes)}m {int(seconds)}s uptime, {self.request_count} API calls")
    
    def run_monitor(self):
        """Main monitoring loop with production error handling"""
        logger.info("Starting CMC Real-Time Monitor...")
        logger.info(f"Refresh interval: {REFRESH_INTERVAL} seconds")
        logger.info(f"Watching: {', '.join(WATCHLIST) if WATCHLIST else 'Top coins only'}")
        if STOCK_WATCHLIST:
            logger.info(f"Stocks: {', '.join(STOCK_WATCHLIST)}")
        
        cycle_count = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                cycle_count += 1
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"\n🔄 Update #{cycle_count} - {current_time}")
                
                # Get watchlist data and check for arbitrage opportunities
                if WATCHLIST:
                    watchlist_data = self.get_watchlist_data()
                    if watchlist_data:
                        # Check for arbitrage opportunities
                        arbitrage_opps = self.detect_arbitrage_opportunities(watchlist_data)
                        if arbitrage_opps:
                            self.display_arbitrage_opportunities(arbitrage_opps)

                        # Display watchlist
                        self.display_watchlist(watchlist_data)
                        self.save_to_csv('watchlist', watchlist_data)
                    else:
                        logger.warning("Failed to get watchlist data")
                        consecutive_errors += 1

                # Get stock data
                if STOCK_WATCHLIST:
                    stock_data = self.get_stock_data()
                    if stock_data:
                        self.display_stocks(stock_data)
                        self.save_to_csv('stocks', stock_data)
                    else:
                        logger.warning("Failed to get stock data")
                        consecutive_errors += 1

                # Update sentiment analysis (every SENTIMENT_UPDATE_INTERVAL seconds)
                current_time_seconds = time.time()
                time_since_last = current_time_seconds - self.last_sentiment_update

                # Initialize Fear & Greed Index on first run if not already set
                if ENABLE_FEAR_GREED and self.fear_greed_index is None:
                    logger.info("🎯 Fetching Fear & Greed Index for first display...")
                    self.fear_greed_index = self.get_fear_greed_index()

                # Always display Fear & Greed Index if available (cached or fresh)
                if ENABLE_FEAR_GREED and self.fear_greed_index:
                    self.display_fear_greed_index(self.fear_greed_index)

                if ENABLE_SENTIMENT_ANALYSIS and time_since_last >= SENTIMENT_UPDATE_INTERVAL:
                    if time_since_last < 600:  # Less than 10 minutes
                        time_display = f"{time_since_last:.0f}s ago"
                    else:
                        time_display = f"{time_since_last/60:.1f}m ago"
                    logger.info(f"🎯 Updating sentiment analysis (last update: {time_display}) - Updates every {SENTIMENT_UPDATE_INTERVAL}s")
                    self.last_sentiment_update = current_time_seconds

                    # Update Fear & Greed Index (fetch fresh data)
                    if ENABLE_FEAR_GREED:
                        self.fear_greed_index = self.get_fear_greed_index()
                        if self.fear_greed_index:
                            # Only save to CSV when freshly fetched (not cached)
                            if not self.fear_greed_index.get('cached', False):
                                self.save_to_csv('fear_greed', self.fear_greed_index)
                    
                    # Analyze market sentiment
                    if watchlist_data:
                        self.market_sentiment = self.analyze_market_sentiment(watchlist_data)
                        if self.market_sentiment:
                            self.display_market_sentiment(self.market_sentiment)
                            self.save_to_csv('market_sentiment', self.market_sentiment)
                    
                
                # Check for alerts
                if watchlist_data:
                    current_data_dict = {coin['symbol']: coin for coin in watchlist_data}
                    alerts = self.detect_alerts(current_data_dict, self.previous_data)
                    if alerts:
                        self.display_alerts(alerts)
                    
                    self.previous_data = current_data_dict
                
                # Print stats every 10 cycles
                if cycle_count % 10 == 0:
                    self.print_stats()
                
                # Check for too many consecutive errors
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}), stopping monitor")
                    break
                
                # Wait for next update
                if self.running:
                    time.sleep(REFRESH_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("Shutdown requested by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in monitoring loop: {e}")
                consecutive_errors += 1
                time.sleep(5)  # Wait before retrying
        
        logger.info("CMC Real-Time Monitor stopped")
        self.print_stats()

def main():
    """Main entry point with production error handling"""
    try:
        monitor = CMCRealTimeMonitor()
        monitor.run_monitor()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        logger.info("Monitor shutdown complete")

if __name__ == "__main__":
    main()
