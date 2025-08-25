import asyncio
import json
import os
from datetime import datetime, timedelta
import pytz
from websockets import connect
from termcolor import cprint
import signal
import sys

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
                if usd_size >= 3000000:
                    usd_size_formatted = usd_size / 1000000
                    cprint(f"\033[5m{trad_type} {symbol} {second} ${usd_size_formatted:.2f}m\033[0m", "white", back_color, attrs=attrs)
                else:
                    usd_size_formatted = usd_size / 100000 
                    cprint(f"{trad_type} {symbol} {second} ${usd_size_formatted:.2f}m", "white", back_color, attrs=attrs)

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

async def binance_trade_stream(uri, symbol, filename, aggregator):
    print(f"Connecting to {symbol} stream...")
    while True:
        try:
            async with connect(uri, ping_interval=30, ping_timeout=30, close_timeout=10) as websocket:
                print(f"Connected to {symbol} stream successfully")
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=60)
                        data = json.loads(message)
                        usd_size = float(data["p"]) * float(data["q"])
                        
                        # Only process trades that meet the minimum size requirement
                        if usd_size >= MIN_TRADE_SIZE:
                            trade_time = datetime.fromtimestamp(data["T"] / 1000, pytz.timezone("US/Central"))
                            readable_trade_time = trade_time.strftime("%H:%M:%S")

                            await aggregator.add_trade(symbol.upper().replace("USDT", ""), readable_trade_time, usd_size, data["m"], data)

                    except asyncio.TimeoutError:
                        print(f"Timeout on {symbol} stream, reconnecting...")
                        break
                    except Exception as e:
                        print(f"Error processing {symbol} message: {e}")
                        break
        except Exception as e:
            print(f"Failed to connect to {symbol} stream: {e}")
            await asyncio.sleep(5)

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
    
    # Create tasks for each symbol
    trade_stream_tasks = []
    for symbol in symbols:
        task = asyncio.create_task(
            binance_trade_stream(f"{websocket_url_base}{symbol.lower()}@aggTrade", symbol, filename, trade_aggregator)
        )
        trade_stream_tasks.append(task)
    
    print_task = asyncio.create_task(print_aggregated_trades_every_seconds(trade_aggregator))
    
    print("Connecting to Binance WebSocket streams...")
    try:
        await asyncio.gather(*trade_stream_tasks, print_task, return_exceptions=True)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        for task in trade_stream_tasks:
            task.cancel()
        print_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
