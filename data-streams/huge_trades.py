import asyncio
import json
import os
from datetime import datetime, timedelta
import pytz
from websockets import connect, WebSocketException, ConnectionClosed
from termcolor import cprint
import signal
import sys
import random

# list of symbols to track
symbols = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "LINKUSDT",
    "SUIUSDT",
    "HBARUSDT",
    "AAVEUSDT",
    "OPUSDT",
]

websocket_url_base = "wss://fstream.binance.com/ws/"
trades_filename = "huge_trades.csv"

# Minimum trade size to track (in USD)
MIN_TRADE_SIZE = 500000  # $500k minimum

# Display formatting thresholds
BLINK_THRESHOLD = 10000000  # $10M - trades ≥ this will blink
MILLION_THRESHOLD = 1000000  # $1M - trades ≥ this shown as millions
BILLION_THRESHOLD = 1000000000  # $1B - trades ≥ this shown as billions

# Connection settings
MAX_RECONNECT_ATTEMPTS = 10
BASE_RECONNECT_DELAY = 5  # seconds
MAX_RECONNECT_DELAY = 300  # 5 minutes
PING_INTERVAL = 20
PING_TIMEOUT = 20
CLOSE_TIMEOUT = 10
MESSAGE_TIMEOUT = 60

# check if the csv files exists
if not os.path.exists(trades_filename):
    with open(trades_filename, "w") as f:
        f.write("Event Time,Symbol,Aggregate Trade ID,Price,Quantity,First Trade ID,Trade Time,Is Buyer Maker,USD Size\n")

class TradeAggregator:
    def __init__(self, filename):
        self.filename = filename
        self.trade_buckets = {}
        self.last_cleanup = datetime.now()

    async def add_trade(self, symbol, second, usd_size, is_buyer_maker, trade_data):
        # Only process trades that meet the minimum size requirement
        if usd_size >= MIN_TRADE_SIZE:
            trade_key = (symbol, second, is_buyer_maker)
            self.trade_buckets[trade_key] = self.trade_buckets.get(trade_key, 0) + usd_size
            
            # Save individual large trade to CSV
            await self.save_trade_to_csv(trade_data, usd_size)

    async def save_trade_to_csv(self, trade_data, usd_size):
        """Save individual large trade to CSV file"""
        try:
            trade_time = datetime.fromtimestamp(trade_data["T"] / 1000, pytz.timezone("US/Central"))
            readable_trade_time = trade_time.strftime("%Y-%m-%d %H:%M:%S")
            
            csv_line = f"{readable_trade_time},{trade_data['s']},{trade_data['a']},{trade_data['p']},{trade_data['q']},{trade_data['f']},{trade_data['T']},{trade_data['m']},{usd_size:.2f}\n"
            
            with open(self.filename, "a") as f:
                f.write(csv_line)
        except Exception as e:
            print(f"Error saving trade to CSV: {e}")

    async def check_and_print_trades(self):
        current_time = datetime.now()
        # Clean up old entries (older than 10 minutes) to prevent memory leaks
        if (current_time - self.last_cleanup).seconds > 300:  # 5 minutes
            self._cleanup_old_entries()
            self.last_cleanup = current_time
        
        deletions = []
        current_time_str = current_time.strftime("%H:%M:%S")
        for trade_key, usd_size in self.trade_buckets.items():
            symbol, second, is_buyer_maker = trade_key
            # Check if trade is from a previous second and meets size threshold
            if second < current_time_str and usd_size >= MIN_TRADE_SIZE:
                attrs = ["bold"]
                back_color = "on_blue" if not is_buyer_maker else "on_magenta"
                trad_type = "BUY" if not is_buyer_maker else "SELL"
                
                # Format USD size with appropriate units
                if usd_size >= BILLION_THRESHOLD:  # ≥ $1B
                    usd_size_formatted = usd_size / BILLION_THRESHOLD
                    size_str = f"${usd_size_formatted:.2f}B"
                elif usd_size >= MILLION_THRESHOLD:  # ≥ $1M
                    usd_size_formatted = usd_size / MILLION_THRESHOLD
                    size_str = f"${usd_size_formatted:.2f}M"
                elif usd_size >= 1000:  # ≥ $1K
                    usd_size_formatted = usd_size / 1000
                    size_str = f"${usd_size_formatted:.0f}K"
                else:
                    size_str = f"${usd_size:.0f}"
                
                # Add blinking effect for very large trades (≥ $10M)
                if usd_size >= BLINK_THRESHOLD:
                    cprint(f"\033[5m{trad_type} {symbol} {second} {size_str}\033[0m", "white", back_color, attrs=attrs)
                else:
                    cprint(f"{trad_type} {symbol} {second} {size_str}", "white", back_color, attrs=attrs)

                deletions.append(trade_key)

        for trade_key in deletions:
            del self.trade_buckets[trade_key]

    def _cleanup_old_entries(self):
        """Remove entries older than 10 minutes to prevent memory leaks"""
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(minutes=10)
        deletions = []
        
        for trade_key in list(self.trade_buckets.keys()):
            symbol, second, is_buyer_maker = trade_key
            try:
                # Parse the time string to datetime for proper comparison
                trade_time = datetime.strptime(second, "%H:%M:%S").replace(
                    year=current_time.year, 
                    month=current_time.month, 
                    day=current_time.day
                )
                
                # Handle hour boundary cases
                if trade_time.hour > current_time.hour + 2:
                    trade_time = trade_time.replace(day=current_time.day - 1)
                
                if trade_time < cutoff_time:
                    deletions.append(trade_key)
            except ValueError:
                # If we can't parse the time, delete it to be safe
                deletions.append(trade_key)
        
        for trade_key in deletions:
            del self.trade_buckets[trade_key]
        
        if deletions:
            print(f"Cleaned up {len(deletions)} old trade entries (older than 10 minutes)")

trade_aggregator = TradeAggregator(trades_filename)

class WebSocketManager:
    def __init__(self, symbol, uri, aggregator):
        self.symbol = symbol
        self.uri = uri
        self.aggregator = aggregator
        self.reconnect_attempts = 0
        self.last_reconnect = None
        self.websocket = None
        self.is_connected = False
        self.should_stop = False

    def calculate_reconnect_delay(self):
        """Calculate exponential backoff delay with jitter"""
        if self.reconnect_attempts == 0:
            return BASE_RECONNECT_DELAY
        
        delay = min(BASE_RECONNECT_DELAY * (2 ** self.reconnect_attempts), MAX_RECONNECT_DELAY)
        # Add jitter (±20%) to prevent thundering herd
        jitter = delay * 0.2 * (random.random() - 0.5)
        return max(1, delay + jitter)

    async def connect(self):
        """Establish WebSocket connection with proper error handling"""
        try:
            self.websocket = await connect(
                self.uri,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=CLOSE_TIMEOUT,
                max_size=None,  # Allow large messages
                compression=None  # Disable compression for better reliability
            )
            self.is_connected = True
            self.reconnect_attempts = 0
            print(f"Connected to {self.symbol} stream successfully")
            return True
        except Exception as e:
            self.is_connected = False
            print(f"Failed to connect to {self.symbol} stream: {e}")
            return False

    async def disconnect(self):
        """Safely close WebSocket connection"""
        self.is_connected = False
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception as e:
                print(f"Error closing {self.symbol} connection: {e}")
            finally:
                self.websocket = None

    async def receive_message(self):
        """Receive and process a single message with timeout"""
        if not self.websocket or not self.is_connected:
            return None
        
        try:
            message = await asyncio.wait_for(self.websocket.recv(), timeout=MESSAGE_TIMEOUT)
            return message
        except asyncio.TimeoutError:
            print(f"Timeout on {self.symbol} stream, reconnecting...")
            return None
        except ConnectionClosed as e:
            print(f"Connection closed for {self.symbol}: {e}")
            return None
        except WebSocketException as e:
            print(f"WebSocket error for {self.symbol}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error receiving message from {self.symbol}: {e}")
            return None

    async def process_message(self, message):
        """Process a single message"""
        try:
            data = json.loads(message)
            usd_size = float(data["p"]) * float(data["q"])
            
            # Only process trades that meet the minimum size requirement
            if usd_size >= MIN_TRADE_SIZE:
                trade_time = datetime.fromtimestamp(data["T"] / 1000, pytz.timezone("US/Central"))
                readable_trade_time = trade_time.strftime("%H:%M:%S")

                await self.aggregator.add_trade(
                    self.symbol.upper().replace("USDT", ""), 
                    readable_trade_time, 
                    usd_size, 
                    data["m"], 
                    data
                )
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {self.symbol}: {e}")
        except KeyError as e:
            print(f"Missing key in {self.symbol} message: {e}")
        except ValueError as e:
            print(f"Value error processing {self.symbol} message: {e}")
        except Exception as e:
            print(f"Error processing {self.symbol} message: {e}")

    async def run(self):
        """Main connection loop with automatic reconnection"""
        while not self.should_stop:
            try:
                # Connect to WebSocket
                if not await self.connect():
                    await self.handle_reconnect()
                    continue

                # Main message processing loop
                while self.is_connected and not self.should_stop:
                    message = await self.receive_message()
                    if message is None:
                        break  # Connection lost, will reconnect
                    
                    await self.process_message(message)

            except Exception as e:
                print(f"Unexpected error in {self.symbol} stream: {e}")
            
            finally:
                await self.disconnect()
                
            # Handle reconnection
            if not self.should_stop:
                await self.handle_reconnect()

    async def handle_reconnect(self):
        """Handle reconnection with exponential backoff"""
        self.reconnect_attempts += 1
        delay = self.calculate_reconnect_delay()
        
        print(f"Reconnecting to {self.symbol} in {delay:.1f} seconds (attempt {self.reconnect_attempts})")
        await asyncio.sleep(delay)

    def stop(self):
        """Stop the WebSocket manager"""
        self.should_stop = True

async def print_aggregated_trades_every_seconds(aggregator):
    print("Starting trade aggregation monitor...")
    while True:
        try:
            await asyncio.sleep(1)
            await aggregator.check_and_print_trades()
        except Exception as e:
            print(f"Error in aggregation monitor: {e}")
            await asyncio.sleep(1)

async def main():
    filename = "huge_trades.csv"
    print("Starting Binance trade aggregator...")
    print(f"Tracking symbols: {symbols}")
    print(f"Minimum trade size: ${MIN_TRADE_SIZE:,}")
    
    # Create WebSocket managers for each symbol
    managers = []
    for symbol in symbols:
        uri = f"{websocket_url_base}{symbol.lower()}@aggTrade"
        manager = WebSocketManager(symbol, uri, trade_aggregator)
        managers.append(manager)
    
    # Create tasks for each manager
    manager_tasks = [asyncio.create_task(manager.run()) for manager in managers]
    print_task = asyncio.create_task(print_aggregated_trades_every_seconds(trade_aggregator))
    
    print("Connecting to Binance WebSocket streams...")
    
    try:
        await asyncio.gather(*manager_tasks, print_task, return_exceptions=True)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        for manager in managers:
            manager.stop()
        for task in manager_tasks:
            task.cancel()
        print_task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*manager_tasks, print_task, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
