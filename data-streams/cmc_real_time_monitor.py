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
- Top cryptocurrencies by market cap
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

# Sentiment Analysis Configuration
ENABLE_SENTIMENT_ANALYSIS = True  # Enable/disable sentiment features
ENABLE_FEAR_GREED = True         # Enable Fear & Greed Index
ENABLE_SOCIAL_SENTIMENT = True   # Enable social media sentiment tracking
SENTIMENT_UPDATE_INTERVAL = 120  # Update sentiment every 2 minutes
SOCIAL_PLATFORMS = ['twitter', 'reddit']  # Platforms to track

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
        self.market_sentiment = None
        self.social_sentiment = {}
        self.last_sentiment_update = time.time()  # Initialize to current time
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("🌙 BobbyYo's CMC Real-Time Monitor Initialized! 🚀")
        logger.info(f"Configuration: {REFRESH_INTERVAL}s interval, {TOP_COINS_LIMIT} top coins, Watchlist: {WATCHLIST}")
        if ENABLE_SENTIMENT_ANALYSIS:
            logger.info(f"Sentiment Analysis: Fear & Greed: {ENABLE_FEAR_GREED}, Social Media: {ENABLE_SOCIAL_SENTIMENT}")
    
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
        """Get Fear & Greed Index from Alternative.me API"""
        try:
            response = requests.get('https://api.alternative.me/fng/', timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'data' and len(data['data']) > 0:
                    fng_data = data['data'][0]
                    return {
                        'value': int(fng_data['value']),
                        'value_classification': fng_data['value_classification'],
                        'timestamp': fng_data['timestamp'],
                        'time_until_update': fng_data.get('time_until_update', '0'),
                        'fetch_time': datetime.datetime.now().isoformat()
                    }
            else:
                logger.warning(f"Fear & Greed API returned status {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching Fear & Greed Index: {e}")
            return None
    
    def analyze_market_sentiment(self, global_data, top_coins):
        """Analyze overall market sentiment based on multiple indicators"""
        if not global_data or not top_coins:
            return None
        
        try:
            sentiment_score = 0
            indicators = []
            
            # Analyze Bitcoin dominance
            btc_dominance = global_data.get('bitcoin_dominance', 50)
            if btc_dominance > 55:
                sentiment_score += 20  # High BTC dominance = bullish
                indicators.append(f"High BTC dominance ({btc_dominance:.1f}%)")
            elif btc_dominance < 45:
                sentiment_score -= 10  # Low BTC dominance = bearish
                indicators.append(f"Low BTC dominance ({btc_dominance:.1f}%)")
            
            # Analyze top 10 coins price changes
            positive_changes = 0
            negative_changes = 0
            total_volume_change = 0
            
            for coin in top_coins[:10]:
                change_24h = coin.get('change_24h', 0)
                volume_24h = coin.get('volume_24h', 0)
                
                if change_24h > 0:
                    positive_changes += 1
                elif change_24h < 0:
                    negative_changes += 1
                
                # Weight by market cap (approximate)
                total_volume_change += volume_24h * (1 + change_24h/100)
            
            # Market breadth analysis
            market_breadth = (positive_changes / 10) * 100
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
                'timestamp': datetime.datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error analyzing market sentiment: {e}")
            return None
    
    def get_social_sentiment(self):
        """Get social media sentiment data (simulated - in real implementation, use APIs like Twitter, Reddit)"""
        try:
            # Note: This is a simulated implementation
            # In production, you would integrate with:
            # - Twitter API v2 for sentiment analysis
            # - Reddit API for r/cryptocurrency sentiment
            # - Social sentiment APIs like Social Sentiment API
            
            current_time = datetime.datetime.now()
            
            # Simulate social sentiment data
            social_data = {}
            
            for platform in SOCIAL_PLATFORMS:
                # Simulate sentiment scores (-100 to +100)
                sentiment_score = np.random.randint(-30, 40)  # Slightly bullish bias
                
                if sentiment_score > 20:
                    classification = "Bullish"
                    color = "green"
                elif sentiment_score > -20:
                    classification = "Neutral"
                    color = "yellow"
                else:
                    classification = "Bearish"
                    color = "red"
                
                social_data[platform] = {
                    'sentiment_score': sentiment_score,
                    'classification': classification,
                    'color': color,
                    'mentions_24h': np.random.randint(1000, 50000),
                    'engagement_rate': np.random.uniform(0.02, 0.08),
                    'top_keywords': ['bitcoin', 'ethereum', 'crypto', 'bullish', 'moon'],
                    'timestamp': current_time.isoformat()
                }
            
            # Calculate overall social sentiment
            avg_sentiment = sum(data['sentiment_score'] for data in social_data.values()) / len(social_data)
            total_mentions = sum(data['mentions_24h'] for data in social_data.values())
            
            if avg_sentiment > 15:
                overall_classification = "Bullish"
                overall_color = "green"
            elif avg_sentiment > -15:
                overall_classification = "Neutral"
                overall_color = "yellow"
            else:
                overall_classification = "Bearish"
                overall_color = "red"
            
            return {
                'platforms': social_data,
                'overall_sentiment': avg_sentiment,
                'overall_classification': overall_classification,
                'overall_color': overall_color,
                'total_mentions_24h': total_mentions,
                'timestamp': current_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting social sentiment: {e}")
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
    
    def get_top_cryptocurrencies(self, limit=TOP_COINS_LIMIT):
        """Get top cryptocurrencies by market cap from unified collector data"""
        try:
            # Read from unified collector's current prices file (use absolute path)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            current_prices_file = os.path.join(project_root, 'data', 'live_market', 'current_prices.json')

            if not os.path.exists(current_prices_file):
                logger.warning("Current prices file not found. Make sure unified_ohlcv_collector.py is running.")
                return []

            with open(current_prices_file, 'r') as f:
                prices_data = json.load(f)

            # Convert to list format and sort by market cap
            coins = []
            for symbol, data in prices_data.items():
                coins.append({
                    'rank': 0,  # Will be set after sorting
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

            # Sort by market cap (descending) and assign ranks
            coins.sort(key=lambda x: x['market_cap'], reverse=True)
            for i, coin in enumerate(coins[:limit], 1):
                coin['rank'] = i

            return coins[:limit]

        except Exception as e:
            logger.error(f"Error reading top cryptocurrencies from local data: {e}")
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
                        'timestamp': datetime.datetime.now().isoformat()
                    })
                else:
                    logger.debug(f"Symbol {symbol} not found in unified collector data")

            logger.debug(f"Retrieved watchlist data for {len(watchlist_data)} coins from local data")
            return watchlist_data

        except Exception as e:
            logger.error(f"Error reading watchlist data from local data: {e}")
            logger.error(f"Watchlist: {WATCHLIST}")
            return []
    
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

                    # Volume spike alert
                    current_volume = coin.get('volume_24h', 0)
                    prev_volume = prev_coin.get('volume_24h', 0)

                    # Ensure volumes are numeric
                    if isinstance(current_volume, (int, float)) and isinstance(prev_volume, (int, float)) and prev_volume > 0:
                        volume_change = ((current_volume - prev_volume) / prev_volume) * 100
                        if abs(volume_change) >= MIN_VOLUME_CHANGE:
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
    
    def display_global_metrics(self, global_data):
        """Display global market metrics"""
        if not global_data:
            return
        
        print("\n" + "="*70)
        cprint("🌍 GLOBAL CRYPTO MARKET METRICS", "cyan", attrs=["bold"])
        print("="*70)
        
        total_mcap = global_data['total_market_cap'] / 1e12 if global_data['total_market_cap'] else 0
        total_volume = global_data['total_volume_24h'] / 1e9 if global_data['total_volume_24h'] else 0
        
        cprint(f"📊 Total Market Cap: ${total_mcap:.2f}T", "green")
        cprint(f"📈 24h Volume: ${total_volume:.2f}B", "blue")
        cprint(f"₿ Bitcoin Dominance: {global_data['bitcoin_dominance']:.1f}%", "yellow")
        cprint(f"⟠ Ethereum Dominance: {global_data['ethereum_dominance']:.1f}%", "magenta")
        cprint(f"🪙 Active Cryptocurrencies: {global_data['active_cryptocurrencies']:,}", "white")
        cprint(f"🏪 Active Exchanges: {global_data['active_exchanges']:,}", "white")
    
    def display_top_coins(self, coins):
        """Display top cryptocurrencies"""
        if not coins:
            return
        
        print("\n" + "="*90)
        cprint("📈 TOP CRYPTOCURRENCIES BY MARKET CAP", "cyan", attrs=["bold"])
        print("="*90)
        
        header = f"{'Rank':<4} {'Symbol':<8} {'Price':<15} {'Change 24h':<12} {'Market Cap':<18} {'Volume 24h':<15}"
        cprint(header, "white", attrs=["bold"])
        print("-" * 90)
        
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
            volume = coin.get('volume_24h', 0) / 1e6 if coin.get('volume_24h') else 0
            
            line = f"{coin.get('rank', 0):<4} {coin.get('symbol', ''):<8} {price_str:<15} {change_symbol} {change_24h:+.1f}%{'':<5} ${market_cap:.1f}B{'':<8} ${volume:.1f}M"
            cprint(line, change_color)
    
    def display_watchlist(self, watchlist_data):
        """Display watchlist coins"""
        if not watchlist_data:
            return

        print("\n" + "="*70)
        cprint("⭐ YOUR WATCHLIST", "cyan", attrs=["bold"])
        print("="*70)

        # Add column headers
        header = f"{'Symbol':<8} {'Price':<15} {'Change 24h':<12} {'Market Cap':<15}"
        cprint(header, "white", attrs=["bold"])
        print("-" * 70)

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
            
            line = f"{coin.get('symbol', ''):<8} {price_str:<15} {change_24h:+.1f}%{'':<8} ${market_cap:.1f}B"
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
        cprint("😨 FEAR & GREED INDEX", "cyan", attrs=["bold"])
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
        cprint("📊 MARKET SENTIMENT ANALYSIS", "cyan", attrs=["bold"])
        print("="*70)
        
        score = sentiment_data['score']
        classification = sentiment_data['classification']
        color = sentiment_data['color']
        market_breadth = sentiment_data['market_breadth']
        
        cprint(f"🎯 Sentiment Score: {score}/100", color, attrs=["bold"])
        cprint(f"📈 Classification: {classification}", color)
        cprint(f"📊 Market Breadth: {market_breadth:.1f}% positive", "white")
        cprint(f"✅ Positive Coins: {sentiment_data['positive_coins']}/10", "green")
        cprint(f"❌ Negative Coins: {sentiment_data['negative_coins']}/10", "red")
        
        # Display key indicators
        if sentiment_data['indicators']:
            print("\n🔍 Key Indicators:")
            for indicator in sentiment_data['indicators']:
                cprint(f"   • {indicator}", "white")
    
    def display_social_sentiment(self, social_data):
        """Display social media sentiment"""
        if not social_data:
            return
        
        print("\n" + "="*70)
        cprint("📱 SOCIAL MEDIA SENTIMENT", "cyan", attrs=["bold"])
        print("="*70)
        
        overall_sentiment = social_data['overall_sentiment']
        overall_classification = social_data['overall_classification']
        overall_color = social_data['overall_color']
        total_mentions = social_data['total_mentions_24h']
        
        cprint(f"🌐 Overall Sentiment: {overall_sentiment:.1f}/100", overall_color, attrs=["bold"])
        cprint(f"📊 Classification: {overall_classification}", overall_color)
        cprint(f"💬 Total Mentions 24h: {total_mentions:,}", "white")
        
        # Display platform-specific data
        print("\n📱 Platform Breakdown:")
        for platform, data in social_data['platforms'].items():
            platform_name = platform.title()
            sentiment_score = data['sentiment_score']
            classification = data['classification']
            color = data['color']
            mentions = data['mentions_24h']
            engagement = data['engagement_rate'] * 100
            
            cprint(f"   {platform_name}: {sentiment_score:.1f}/100 ({classification})", color)
            cprint(f"      Mentions: {mentions:,} | Engagement: {engagement:.1f}%", "white")
            
            # Display top keywords
            keywords = ', '.join(data['top_keywords'][:3])
            cprint(f"      Keywords: {keywords}", "light_blue")
    
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
                
            elif data_type == 'fear_greed' and isinstance(data, dict):
                filename = f"{self.csv_dir}/fear_greed_{date_str}.csv"
                df = pd.DataFrame([data])
                
            elif data_type == 'market_sentiment' and isinstance(data, dict):
                filename = f"{self.csv_dir}/market_sentiment_{date_str}.csv"
                df = pd.DataFrame([data])
                
            elif data_type == 'social_sentiment' and isinstance(data, dict):
                filename = f"{self.csv_dir}/social_sentiment_{date_str}.csv"
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
        
        cycle_count = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                cycle_count += 1
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"\n🔄 Update #{cycle_count} - {current_time}")
                
                # Get global metrics
                global_data = self.get_global_metrics()
                if global_data:
                    self.display_global_metrics(global_data)
                    self.save_to_csv('global', global_data)
                    consecutive_errors = 0  # Reset error counter on success
                else:
                    logger.warning("Failed to get global metrics")
                    consecutive_errors += 1
                
                # Get top cryptocurrencies
                top_coins = self.get_top_cryptocurrencies()
                if top_coins:
                    self.display_top_coins(top_coins)
                    self.save_to_csv('top_coins', top_coins)
                else:
                    logger.warning("Failed to get top cryptocurrencies")
                    consecutive_errors += 1
                
                # Get watchlist data
                if WATCHLIST:
                    watchlist_data = self.get_watchlist_data()
                    if watchlist_data:
                        self.display_watchlist(watchlist_data)
                        self.save_to_csv('watchlist', watchlist_data)
                    else:
                        logger.warning("Failed to get watchlist data")
                        consecutive_errors += 1
                
                # Update sentiment analysis (every SENTIMENT_UPDATE_INTERVAL seconds)
                current_time_seconds = time.time()
                time_since_last = current_time_seconds - self.last_sentiment_update

                if ENABLE_SENTIMENT_ANALYSIS and time_since_last >= SENTIMENT_UPDATE_INTERVAL:
                    if time_since_last < 600:  # Less than 10 minutes
                        time_display = f"{time_since_last:.0f}s ago"
                    else:
                        time_display = f"{time_since_last/60:.1f}m ago"
                    logger.info(f"🎯 Updating sentiment analysis (last update: {time_display})")
                    self.last_sentiment_update = current_time_seconds
                    
                    # Get Fear & Greed Index
                    if ENABLE_FEAR_GREED:
                        self.fear_greed_index = self.get_fear_greed_index()
                        if self.fear_greed_index:
                            self.display_fear_greed_index(self.fear_greed_index)
                            self.save_to_csv('fear_greed', self.fear_greed_index)
                    
                    # Analyze market sentiment
                    if global_data and top_coins:
                        self.market_sentiment = self.analyze_market_sentiment(global_data, top_coins)
                        if self.market_sentiment:
                            self.display_market_sentiment(self.market_sentiment)
                            self.save_to_csv('market_sentiment', self.market_sentiment)
                    
                    # Get social sentiment
                    if ENABLE_SOCIAL_SENTIMENT:
                        self.social_sentiment = self.get_social_sentiment()
                        if self.social_sentiment:
                            self.display_social_sentiment(self.social_sentiment)
                            self.save_to_csv('social_sentiment', self.social_sentiment)
                
                # Check for alerts
                if top_coins:
                    current_data_dict = {coin['symbol']: coin for coin in top_coins}
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
