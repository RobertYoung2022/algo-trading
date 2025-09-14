#!/usr/bin/env python3
"""
Unified OHLCV Data Collector - Multi-Source Producer
===================================================
This script collects OHLCV data from multiple free APIs and provides a unified
data feed for other scripts (like the CMC monitor) to consume.

SOURCES:
- Yahoo Finance (yfinance) - No API key, stocks + crypto
- Binance Public API - High rate limits, crypto only
- Alpha Vantage - 25 calls/day, needs API key
- CoinGecko - Existing integration, crypto only

FEATURES:
- Real-time data collection every 30-60 seconds
- Historical data backfill on startup
- Data validation & arbitrage detection
- Failover between API sources
- Standardized JSON output for consumers

STEPS TO USE:
1. Create .env file with API keys (Alpha Vantage optional)
2. Configure WATCHLIST and intervals below
3. Run: python unified_ohlcv_collector.py
4. Data will be written to data/live_market/ for other scripts to consume

Data Consumer Scripts:
- cmc_real_time_monitor.py (reads current_prices.json)
- Your trading bots (read ohlcv candle files)
"""

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
import yfinance as yf
from typing import Dict, List, Optional, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import hmac
import hashlib
import base64

# ====== BobbyYo's Configuration 🌙 ======
WATCHLIST = ['BTC', 'ETH', 'XRP', 'SUI', 'HBAR', 'CRO', 'LINK', 'TAO']  # Crypto symbols
STOCK_WATCHLIST = ['BTBT', 'HOOD', 'COIN', 'NKE', 'SPY', 'QQQ']  # Optional stock symbols

# Collection intervals (in seconds)
COLLECTION_INTERVALS = {
    '1m': 60,
    '1h': 3600,
    '1d': 86400
}

REFRESH_INTERVAL = 30  # Main loop interval in seconds
ENABLE_STOCKS = True   # Set to False to collect crypto only
ENABLE_ARBITRAGE_DETECTION = True
ENABLE_BINANCE = False  # Set to True to enable Binance API (may be geo-blocked)
ENABLE_COINBASE = True  # Set to False to disable Coinbase API (requires API key)
ARBITRAGE_THRESHOLD = 2.0  # Percentage difference to trigger arbitrage alert

# Data directories (use absolute paths relative to project root)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Go up from data-scripts to project root
BASE_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'live_market')
OHLCV_DIR = {
    '1m': os.path.join(BASE_DATA_DIR, 'ohlcv_1m'),
    '1h': os.path.join(BASE_DATA_DIR, 'ohlcv_1h'),
    '1d': os.path.join(BASE_DATA_DIR, 'ohlcv_1d')
}

# Create directories
for dir_path in [BASE_DATA_DIR] + list(OHLCV_DIR.values()):
    os.makedirs(dir_path, exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('unified_ohlcv_collector.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('unified_ohlcv_collector')

# Load environment variables
load_dotenv()

class UnifiedOHLCVCollector:
    """Multi-source OHLCV data collector with arbitrage detection"""

    def __init__(self):
        self.running = False
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.coinbase_api_key = os.getenv('COINBASE_API_KEY')
        self.coinbase_api_secret = os.getenv('COINBASE_API_SECRET')
        self.data_sources = ['yahoo', 'coingecko']

        # Add Coinbase if enabled and API key is available
        if ENABLE_COINBASE and self.coinbase_api_key and self.coinbase_api_secret:
            self.data_sources.append('coinbase')
            logger.info(f"✅ Coinbase API key found, including in data sources")
        elif ENABLE_COINBASE:
            logger.info("⚠️  Coinbase enabled but API credentials missing")
        else:
            logger.info("ℹ️  Coinbase API disabled (can be enabled in config)")

        # Add Binance if enabled (may be geo-blocked)
        if ENABLE_BINANCE:
            self.data_sources.append('binance')
            logger.info(f"✅ Binance API enabled, including in data sources")
        else:
            logger.info("ℹ️  Binance API disabled (can be enabled in config)")

        # Add Alpha Vantage if API key is available
        if self.alpha_vantage_key:
            self.data_sources.append('alphavantage')
            logger.info(f"✅ Alpha Vantage API key found, including in data sources")
        else:
            logger.info("⚠️  No Alpha Vantage API key found, using free sources only")

        # Data storage
        self.current_prices = {}
        self.market_overview = {}
        self.ohlcv_candles = {interval: {} for interval in COLLECTION_INTERVALS.keys()}
        self.data_quality = {source: {'success': 0, 'failures': 0} for source in self.data_sources}
        self.arbitrage_alerts = []

        # Thread management
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.last_collection_time = {interval: {} for interval in COLLECTION_INTERVALS.keys()}

        logger.info("🌙 BobbyYo's Unified OHLCV Collector Initialized! 🚀")
        logger.info(f"📊 Watchlist: {WATCHLIST}")
        logger.info(f"📈 Stock watchlist: {STOCK_WATCHLIST if ENABLE_STOCKS else 'Disabled'}")
        logger.info(f"🔗 Data sources: {', '.join(self.data_sources)}")

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("🛑 Shutdown signal received...")
        self.running = False
        self.executor.shutdown(wait=True)

    # ====== Data Collection Methods ======

    def sign_coinbase_request(self, method: str, path: str, body: str = '') -> Dict[str, str]:
        """Sign a Coinbase API request"""
        timestamp = str(int(time.time()))
        message = f"{timestamp}{method}{path}{body}"

        signature = hmac.new(
            self.coinbase_api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return {
            'CB-ACCESS-KEY': self.coinbase_api_key,
            'CB-ACCESS-SIGN': signature,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'Content-Type': 'application/json'
        }

    def collect_coinbase_data(self, symbols: List[str]) -> Dict:
        """Collect data from Coinbase Exchange API"""
        try:
            results = {}

            # Convert symbols to Coinbase format
            coinbase_symbols = [f"{symbol}-USD" for symbol in symbols if symbol in WATCHLIST]

            if not coinbase_symbols:
                return results

            # Get 24hr stats for all symbols
            for symbol in WATCHLIST:
                try:
                    coinbase_symbol = f"{symbol}-USD"

                    # Get 24hr stats
                    stats_path = f"/products/{coinbase_symbol}/stats"
                    stats_headers = self.sign_coinbase_request('GET', stats_path)
                    stats_url = f"https://api.exchange.coinbase.com{stats_path}"

                    stats_response = requests.get(stats_url, headers=stats_headers, timeout=10)

                    if stats_response.status_code == 200:
                        stats_data = stats_response.json()

                        # Get current ticker
                        ticker_path = f"/products/{coinbase_symbol}/ticker"
                        ticker_headers = self.sign_coinbase_request('GET', ticker_path)
                        ticker_url = f"https://api.exchange.coinbase.com{ticker_path}"

                        ticker_response = requests.get(ticker_url, headers=ticker_headers, timeout=10)

                        if ticker_response.status_code == 200:
                            ticker_data = ticker_response.json()

                            current_price = float(ticker_data.get('price', 0))
                            open_24h = float(stats_data.get('open', current_price))
                            volume_24h = float(stats_data.get('volume', 0))

                            # Calculate 24h change
                            change_24h = 0
                            if open_24h > 0:
                                change_24h = ((current_price - open_24h) / open_24h) * 100

                            results[symbol] = {
                                'price': current_price,
                                'volume_24h': volume_24h,
                                'change_24h': change_24h,
                                'source': 'coinbase',
                                'timestamp': datetime.datetime.now().isoformat(),
                                'high_24h': float(stats_data.get('high', current_price)),
                                'low_24h': float(stats_data.get('low', current_price)),
                                'symbol': symbol
                            }

                    # Rate limiting - Coinbase allows 10 req/sec
                    time.sleep(0.1)  # 100ms delay between requests

                except Exception as e:
                    logger.warning(f"Coinbase error for {symbol}: {e}")
                    continue

            if results:
                self.data_quality['coinbase']['success'] += 1
            else:
                self.data_quality['coinbase']['failures'] += 1

            return results

        except Exception as e:
            self.data_quality['coinbase']['failures'] += 1
            logger.error(f"Coinbase API error: {e}")
            return {}

    def collect_yahoo_data(self, symbols: List[str]) -> Dict:
        """Collect data from Yahoo Finance API"""
        try:
            results = {}

            # Convert crypto symbols to Yahoo format
            yahoo_symbols = []
            for symbol in symbols:
                if symbol in WATCHLIST:
                    yahoo_symbols.append(f"{symbol}-USD")
                else:
                    yahoo_symbols.append(symbol)

            # Use yfinance to get current data
            tickers = yf.Tickers(' '.join(yahoo_symbols))

            for i, symbol in enumerate(symbols):
                try:
                    yahoo_symbol = yahoo_symbols[i]
                    ticker = tickers.tickers[yahoo_symbol]

                    # Get current price info
                    info = ticker.info
                    hist = ticker.history(period="2d", interval="1h")

                    if not hist.empty:
                        current_price = hist['Close'].iloc[-1]
                        volume_24h = hist['Volume'].sum()  # Rough 24h volume

                        # Calculate 24h change
                        if len(hist) >= 24:
                            price_24h_ago = hist['Close'].iloc[-24]
                            change_24h = ((current_price - price_24h_ago) / price_24h_ago) * 100
                        else:
                            change_24h = 0

                        results[symbol] = {
                            'price': float(current_price),
                            'volume_24h': float(volume_24h),
                            'change_24h': float(change_24h),
                            'source': 'yahoo',
                            'timestamp': datetime.datetime.now().isoformat(),
                            'market_cap': info.get('marketCap', 0),
                            'symbol': symbol
                        }

                except Exception as e:
                    logger.warning(f"Yahoo Finance error for {symbol}: {e}")
                    continue

            self.data_quality['yahoo']['success'] += 1
            return results

        except Exception as e:
            self.data_quality['yahoo']['failures'] += 1
            logger.error(f"Yahoo Finance API error: {e}")
            return {}

    def collect_binance_data(self, symbols: List[str]) -> Dict:
        """Collect data from Binance Public API"""
        try:
            results = {}

            # Get 24hr ticker statistics for all symbols
            binance_symbols = [f"{symbol}USDT" for symbol in symbols if symbol in WATCHLIST]

            if not binance_symbols:
                return results

            # Get 24hr ticker stats
            url = "https://api.binance.com/api/v3/ticker/24hr"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                ticker_data = response.json()

                # Create lookup dict
                binance_lookup = {}
                for ticker in ticker_data:
                    symbol = ticker['symbol']
                    if symbol.endswith('USDT'):
                        base_symbol = symbol[:-4]  # Remove 'USDT'
                        binance_lookup[base_symbol] = ticker

                # Extract data for our watchlist
                for symbol in symbols:
                    if symbol in binance_lookup:
                        ticker = binance_lookup[symbol]

                        results[symbol] = {
                            'price': float(ticker['lastPrice']),
                            'volume_24h': float(ticker['volume']),
                            'change_24h': float(ticker['priceChangePercent']),
                            'source': 'binance',
                            'timestamp': datetime.datetime.now().isoformat(),
                            'high_24h': float(ticker['highPrice']),
                            'low_24h': float(ticker['lowPrice']),
                            'symbol': symbol
                        }

                self.data_quality['binance']['success'] += 1
            else:
                self.data_quality['binance']['failures'] += 1
                logger.warning(f"Binance API returned status {response.status_code}")

            return results

        except Exception as e:
            self.data_quality['binance']['failures'] += 1
            logger.error(f"Binance API error: {e}")
            return {}

    def collect_coingecko_data(self, symbols: List[str]) -> Dict:
        """Collect data from CoinGecko API"""
        try:
            results = {}

            # Map symbols to CoinGecko IDs
            coingecko_ids = {
                'BTC': 'bitcoin',
                'ETH': 'ethereum',
                'XRP': 'ripple',
                'SUI': 'sui',
                'HBAR': 'hedera-hashgraph',
                'CRO': 'crypto-com-chain',
                'LINK': 'chainlink',
                'TAO': 'bittensor'
            }

            crypto_symbols = [s for s in symbols if s in WATCHLIST and s in coingecko_ids]
            if not crypto_symbols:
                return results

            ids_param = ','.join([coingecko_ids[s] for s in crypto_symbols])

            url = f"https://api.coingecko.com/api/v3/simple/price"
            params = {
                'ids': ids_param,
                'vs_currencies': 'usd',
                'include_24hr_change': 'true',
                'include_24hr_vol': 'true',
                'include_market_cap': 'true'
            }

            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()

                for symbol in crypto_symbols:
                    coin_id = coingecko_ids[symbol]
                    if coin_id in data:
                        coin_data = data[coin_id]

                        results[symbol] = {
                            'price': float(coin_data['usd']),
                            'volume_24h': float(coin_data.get('usd_24h_vol', 0)),
                            'change_24h': float(coin_data.get('usd_24h_change', 0)),
                            'source': 'coingecko',
                            'timestamp': datetime.datetime.now().isoformat(),
                            'market_cap': float(coin_data.get('usd_market_cap', 0)),
                            'symbol': symbol
                        }

                self.data_quality['coingecko']['success'] += 1
            else:
                self.data_quality['coingecko']['failures'] += 1
                logger.warning(f"CoinGecko API returned status {response.status_code}")

            return results

        except Exception as e:
            self.data_quality['coingecko']['failures'] += 1
            logger.error(f"CoinGecko API error: {e}")
            return {}

    def collect_alphavantage_data(self, symbols: List[str]) -> Dict:
        """Collect data from Alpha Vantage API (limited calls)"""
        if not self.alpha_vantage_key:
            return {}

        try:
            results = {}

            # Alpha Vantage is rate limited, so we'll only collect for a few key symbols
            priority_symbols = ['BTC', 'ETH']  # Most important for validation
            available_symbols = [s for s in symbols if s in priority_symbols]

            for symbol in available_symbols[:2]:  # Limit to 2 symbols per call
                try:
                    if symbol == 'BTC':
                        url = "https://www.alphavantage.co/query"
                        params = {
                            'function': 'DIGITAL_CURRENCY_DAILY',
                            'symbol': 'BTC',
                            'market': 'USD',
                            'apikey': self.alpha_vantage_key
                        }
                    elif symbol == 'ETH':
                        url = "https://www.alphavantage.co/query"
                        params = {
                            'function': 'DIGITAL_CURRENCY_DAILY',
                            'symbol': 'ETH',
                            'market': 'USD',
                            'apikey': self.alpha_vantage_key
                        }
                    else:
                        continue  # Skip other symbols for now

                    response = requests.get(url, params=params, timeout=15)

                    if response.status_code == 200:
                        data = response.json()

                        if 'Time Series (Digital Currency Daily)' in data:
                            time_series = data['Time Series (Digital Currency Daily)']
                            latest_date = max(time_series.keys())
                            latest_data = time_series[latest_date]

                            current_price = float(latest_data['4a. close (USD)'])
                            volume = float(latest_data['5. volume'])

                            # Calculate 24h change (using previous day)
                            dates = sorted(time_series.keys(), reverse=True)
                            if len(dates) >= 2:
                                prev_data = time_series[dates[1]]
                                prev_price = float(prev_data['4a. close (USD)'])
                                change_24h = ((current_price - prev_price) / prev_price) * 100
                            else:
                                change_24h = 0

                            results[symbol] = {
                                'price': current_price,
                                'volume_24h': volume,
                                'change_24h': change_24h,
                                'source': 'alphavantage',
                                'timestamp': datetime.datetime.now().isoformat(),
                                'symbol': symbol
                            }

                    # Rate limiting - Alpha Vantage allows 5 calls per minute
                    time.sleep(12)  # 12 second delay between calls

                except Exception as e:
                    logger.warning(f"Alpha Vantage error for {symbol}: {e}")
                    continue

            if results:
                self.data_quality['alphavantage']['success'] += 1
            else:
                self.data_quality['alphavantage']['failures'] += 1

            return results

        except Exception as e:
            self.data_quality['alphavantage']['failures'] += 1
            logger.error(f"Alpha Vantage API error: {e}")
            return {}

    def detect_arbitrage_opportunities(self, symbol_data: Dict[str, Dict]) -> List[Dict]:
        """Detect price differences across sources that indicate arbitrage opportunities"""
        arbitrage_opportunities = []

        for symbol, sources_data in symbol_data.items():
            if len(sources_data) < 2:
                continue  # Need at least 2 sources to compare

            prices = [(source, data['price']) for source, data in sources_data.items()]
            prices.sort(key=lambda x: x[1])  # Sort by price

            lowest_price = prices[0][1]
            highest_price = prices[-1][1]

            # Calculate percentage difference
            if lowest_price > 0:
                percentage_diff = ((highest_price - lowest_price) / lowest_price) * 100

                if percentage_diff >= ARBITRAGE_THRESHOLD:
                    opportunity = {
                        'symbol': symbol,
                        'percentage_difference': percentage_diff,
                        'lowest_price': lowest_price,
                        'highest_price': highest_price,
                        'lowest_source': prices[0][0],
                        'highest_source': prices[-1][0],
                        'timestamp': datetime.datetime.now().isoformat(),
                        'all_prices': {source: price for source, price in prices}
                    }
                    arbitrage_opportunities.append(opportunity)

                    logger.warning(f"🚨 ARBITRAGE OPPORTUNITY: {symbol} price difference {percentage_diff:.2f}% "
                                 f"({prices[0][0]}: ${lowest_price:.4f} vs {prices[-1][0]}: ${highest_price:.4f})")

        return arbitrage_opportunities

    def collect_all_sources(self, symbols: List[str]) -> Dict[str, Dict[str, Dict]]:
        """Collect data from all available sources concurrently"""
        symbol_data = {symbol: {} for symbol in symbols}

        # Submit collection tasks to thread pool
        futures = {}

        if 'yahoo' in self.data_sources:
            futures['yahoo'] = self.executor.submit(self.collect_yahoo_data, symbols)

        if 'coinbase' in self.data_sources and ENABLE_COINBASE:
            futures['coinbase'] = self.executor.submit(self.collect_coinbase_data, symbols)

        if 'binance' in self.data_sources and ENABLE_BINANCE:
            futures['binance'] = self.executor.submit(self.collect_binance_data, symbols)

        if 'coingecko' in self.data_sources:
            futures['coingecko'] = self.executor.submit(self.collect_coingecko_data, symbols)

        if 'alphavantage' in self.data_sources:
            futures['alphavantage'] = self.executor.submit(self.collect_alphavantage_data, symbols)

        # Collect results
        for source, future in futures.items():
            try:
                source_data = future.result(timeout=30)
                for symbol, data in source_data.items():
                    symbol_data[symbol][source] = data
            except Exception as e:
                logger.error(f"Error collecting from {source}: {e}")

        return symbol_data

    def create_unified_price_data(self, symbol_data: Dict[str, Dict[str, Dict]]) -> Dict:
        """Create unified current price data with source prioritization"""
        unified_data = {}

        # Source priority (most reliable first)
        if ENABLE_COINBASE and ENABLE_BINANCE:
            source_priority = ['coinbase', 'binance', 'coingecko', 'yahoo', 'alphavantage']
        elif ENABLE_COINBASE:
            source_priority = ['coinbase', 'coingecko', 'yahoo', 'alphavantage']
        elif ENABLE_BINANCE:
            source_priority = ['binance', 'coingecko', 'yahoo', 'alphavantage']
        else:
            source_priority = ['coingecko', 'yahoo', 'alphavantage']

        for symbol, sources_data in symbol_data.items():
            if not sources_data:
                continue

            # Find the best available source
            best_source = None
            for source in source_priority:
                if source in sources_data:
                    best_source = source
                    break

            if best_source:
                data = sources_data[best_source].copy()

                # Add metadata about all available sources
                data['all_sources'] = list(sources_data.keys())
                data['source_count'] = len(sources_data)
                data['primary_source'] = best_source

                # Add price comparison if multiple sources
                if len(sources_data) > 1:
                    all_prices = [sources_data[src]['price'] for src in sources_data]
                    data['price_variance'] = {
                        'min': min(all_prices),
                        'max': max(all_prices),
                        'avg': sum(all_prices) / len(all_prices),
                        'std': np.std(all_prices) if len(all_prices) > 1 else 0
                    }

                unified_data[symbol] = data

        return unified_data

    def save_current_data(self, unified_data: Dict, arbitrage_opportunities: List):
        """Save current market data to JSON files for consumers"""
        try:
            # Save current prices
            current_prices_file = os.path.join(BASE_DATA_DIR, 'current_prices.json')
            with open(current_prices_file, 'w') as f:
                json.dump(unified_data, f, indent=2)

            # Create market overview
            total_market_cap = sum([data.get('market_cap', 0) for data in unified_data.values()])
            total_volume = sum([data.get('volume_24h', 0) for data in unified_data.values()])

            market_overview = {
                'total_symbols': len(unified_data),
                'total_market_cap': total_market_cap,
                'total_volume_24h': total_volume,
                'positive_changes': len([d for d in unified_data.values() if d.get('change_24h', 0) > 0]),
                'negative_changes': len([d for d in unified_data.values() if d.get('change_24h', 0) < 0]),
                'timestamp': datetime.datetime.now().isoformat(),
                'data_sources_status': self.data_quality
            }

            market_overview_file = os.path.join(BASE_DATA_DIR, 'market_overview.json')
            with open(market_overview_file, 'w') as f:
                json.dump(market_overview, f, indent=2)

            # Save arbitrage opportunities
            if arbitrage_opportunities:
                arbitrage_file = os.path.join(BASE_DATA_DIR, 'arbitrage_alerts.json')
                with open(arbitrage_file, 'w') as f:
                    json.dump(arbitrage_opportunities, f, indent=2)

            # Save data quality metrics
            quality_file = os.path.join(BASE_DATA_DIR, 'quality_metrics.json')
            with open(quality_file, 'w') as f:
                json.dump(self.data_quality, f, indent=2)

            logger.info(f"💾 Saved data for {len(unified_data)} symbols to {BASE_DATA_DIR}")

        except Exception as e:
            logger.error(f"Error saving data: {e}")

    def display_summary(self, unified_data: Dict, arbitrage_opportunities: List):
        """Display collection summary"""
        print("\n" + "="*80)
        cprint("🌙 UNIFIED OHLCV COLLECTOR STATUS", "cyan", attrs=["bold"])
        print("="*80)

        print(f"📊 Symbols collected: {len(unified_data)}")
        print(f"🔗 Active sources: {', '.join(self.data_sources)}")

        if arbitrage_opportunities:
            cprint(f"🚨 Arbitrage opportunities detected: {len(arbitrage_opportunities)}", "red", attrs=["bold"])

        # Show data quality
        cprint("\n📈 Data Source Quality:", "yellow", attrs=["bold"])
        for source, quality in self.data_quality.items():
            total_attempts = quality['success'] + quality['failures']
            if total_attempts > 0:
                success_rate = (quality['success'] / total_attempts) * 100
                print(f"  {source.capitalize()}: {success_rate:.1f}% success ({quality['success']}/{total_attempts})")

        # Show sample prices
        if unified_data:
            cprint("\n💰 Current Prices (Sample):", "green", attrs=["bold"])
            for symbol, data in list(unified_data.items())[:5]:  # Show first 5
                price = data['price']
                change = data.get('change_24h', 0)
                source = data.get('primary_source', 'unknown')
                change_color = "green" if change > 0 else "red"
                print(f"  {symbol}: ${price:.4f} ({change:+.2f}%) [{source}]", end="")
                if 'source_count' in data and data['source_count'] > 1:
                    print(f" [✓ {data['source_count']} sources]")
                else:
                    print()

    def run(self):
        """Main collection loop"""
        self.running = True

        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        logger.info("🚀 Starting unified OHLCV data collection...")

        # Combine watchlists
        all_symbols = WATCHLIST.copy()
        if ENABLE_STOCKS:
            all_symbols.extend(STOCK_WATCHLIST)

        collection_count = 0

        try:
            while self.running:
                start_time = time.time()
                collection_count += 1

                logger.info(f"📊 Collection cycle #{collection_count} - Collecting data for {len(all_symbols)} symbols...")

                # Collect from all sources
                symbol_data = self.collect_all_sources(all_symbols)

                # Create unified dataset
                unified_data = self.create_unified_price_data(symbol_data)

                # Detect arbitrage opportunities
                arbitrage_opportunities = []
                if ENABLE_ARBITRAGE_DETECTION:
                    arbitrage_opportunities = self.detect_arbitrage_opportunities(symbol_data)

                # Save data for consumers
                self.save_current_data(unified_data, arbitrage_opportunities)

                # Display summary every 10 cycles
                if collection_count % 10 == 0:
                    self.display_summary(unified_data, arbitrage_opportunities)

                # Calculate sleep time
                elapsed_time = time.time() - start_time
                sleep_time = max(0, REFRESH_INTERVAL - elapsed_time)

                logger.info(f"✅ Collection cycle completed in {elapsed_time:.2f}s, sleeping for {sleep_time:.2f}s...")

                if self.running and sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("🛑 Shutdown requested by user")
        except Exception as e:
            logger.error(f"💥 Critical error in main loop: {e}")
        finally:
            self.running = False
            self.executor.shutdown(wait=True)
            logger.info("🔚 Unified OHLCV Collector stopped")

def main():
    """Main entry point"""
    collector = UnifiedOHLCVCollector()
    collector.run()

if __name__ == "__main__":
    main()