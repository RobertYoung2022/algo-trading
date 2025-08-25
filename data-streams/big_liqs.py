import asyncio
import json
import os
import signal
import sys
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint
import logging

# Configuration
WEBSOCKET_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
FILENAME = "big_liqs.csv"
TIMEZONE = "US/Central"

# Display thresholds (specific to big liquidations)
MIN_DISPLAY_SIZE = 100000  # $100K minimum (vs $3K in regular liqs)
BOLD_THRESHOLD = 1000000   # $1M+ gets bold
HUGE_THRESHOLD = 5000000   # $5M+ gets special treatment

# Performance settings
BATCH_SIZE = 50  # Write to file every N messages
STATS_INTERVAL = 100  # Print stats every N messages

# Connection settings
PING_INTERVAL = 20
PING_TIMEOUT = 10
RECV_TIMEOUT = 30.0
RECONNECT_DELAY = 5

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('big_liqs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BigLiquidationMonitor:
    def __init__(self):
        self.message_count = 0
        self.start_time = datetime.now()
        self.batch_buffer = []
        self.running = True
        
        # Initialize CSV file
        self._init_csv_file()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def _init_csv_file(self):
        """Initialize CSV file with headers"""
        if not os.path.exists(FILENAME):
            with open(FILENAME, "w") as f:
                f.write(",".join([
                    "symbol", "side", "order_type", "time_in_force",
                    "original_quantity", "price", "average_price", "order_status",
                    "order_last_filled_quantity", "order_last_accumulated_quantity",
                    "order_trade_time", "usd_size"
                ]) + "\n")
    
    def _format_time(self, timestamp):
        """Convert timestamp to formatted time string"""
        try:
            timezone = pytz.timezone(TIMEZONE)
            utc_time = datetime.fromtimestamp(timestamp/1000, pytz.UTC)
            return utc_time.astimezone(timezone).strftime("%H:%M:%S")
        except Exception as e:
            logger.error(f"Error formatting time: {e}")
            return "00:00:00"
    
    def _get_display_config(self, usd_size, side):
        """Get display configuration based on USD size and side"""
        # Base configuration (specific to big liquidations)
        liquidation_type = "L LIQ" if side == "SELL" else "S LIQ"
        base_color = "blue" if side == "SELL" else "magenta"  # Different from regular liqs
        attrs = []
        stars = ""
        repeat_count = 1
        
        # Enhanced display logic for BIG liquidations
        if usd_size > HUGE_THRESHOLD:  # $5M+
            stars = "*" * 5
            color = "yellow"  # Special color for massive liquidations
            attrs = ["bold", "blink"]
            repeat_count = 8  # More repeats for huge liquidations
        elif usd_size > BOLD_THRESHOLD:  # $1M+
            stars = "*" * 3
            color = base_color
            attrs = ["bold", "blink"]
            repeat_count = 4
        else:  # $100K+
            color = base_color
            attrs = ["bold"]
        
        return {
            'liquidation_type': liquidation_type,
            'color': color,
            'attrs': attrs,
            'stars': stars,
            'repeat_count': repeat_count
        }
    
    def _display_liquidation(self, order_data, usd_size):
        """Display liquidation information with enhanced formatting for BIG liquidations"""
        if usd_size < MIN_DISPLAY_SIZE:
            return
        
        try:
            symbol = order_data["s"].replace("USDT", "")
            side = order_data["S"]
            timestamp = int(order_data["T"])
            
            # Get display configuration
            config = self._get_display_config(usd_size, side)
            
            # Format output (2 decimal places for big liquidations)
            symbol_short = symbol[:4]
            time_str = self._format_time(timestamp)
            
            output = f"{config['liquidation_type']} {symbol_short} {time_str} {usd_size:,.2f}"
            
            # Add stars if any
            if config['stars']:
                output = f"{config['stars']}{output}"
            
            # Display with repeat count
            for _ in range(config['repeat_count']):
                cprint(output, "white", f"on_{config['color']}", attrs=config['attrs'])
            
            print("")
            
        except Exception as e:
            logger.error(f"Error displaying liquidation: {e}")
    
    def _process_message(self, msg):
        """Process a single liquidation message"""
        try:
            order_data = json.loads(msg)["o"]
            
            # Extract key data
            filled_quantity = float(order_data["z"])
            price = float(order_data["p"])
            usd_size = filled_quantity * price
            
            # Display if meets threshold (only $100K+ for big liquidations)
            self._display_liquidation(order_data, usd_size)
            
            # Prepare CSV data
            msg_values = [str(order_data[key]) for key in ["s", "S", "o", "f", "q", "p", "ap", "X", "l", "z", "T"]]
            msg_values.append(str(usd_size))
            
            # Add to batch buffer
            self.batch_buffer.append(msg_values)
            
            # Write batch if full
            if len(self.batch_buffer) >= BATCH_SIZE:
                self._write_batch()
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _write_batch(self):
        """Write batch of messages to CSV file"""
        if not self.batch_buffer:
            return
        
        try:
            with open(FILENAME, "a") as f:
                for msg_values in self.batch_buffer:
                    trade_info = ",".join(msg_values) + "\n"
                    trade_info = trade_info.replace("USDT", "")
                    f.write(trade_info)
            
            self.batch_buffer.clear()
            
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
    
    def _print_stats(self):
        """Print connection statistics"""
        if self.message_count % STATS_INTERVAL == 0:
            uptime = datetime.now() - self.start_time
            rate = self.message_count / max(uptime.total_seconds(), 1) * 60  # messages per minute
            
            logger.info(f"Stats: {self.message_count} messages, "
                       f"Rate: {rate:.1f} msg/min, "
                       f"Uptime: {uptime}")
    
    async def run(self):
        """Main monitoring loop"""
        logger.info("Starting Binance BIG liquidation monitor...")
        
        while self.running:
            try:
                logger.info(f"Attempting to connect to {WEBSOCKET_URL}...")
                
                async with connect(
                    WEBSOCKET_URL,
                    ping_interval=PING_INTERVAL,
                    ping_timeout=PING_TIMEOUT
                ) as websocket:
                    
                    logger.info("Connected to Binance liquidation stream...")
                    logger.info("Waiting for BIG liquidation data ($100K+)...")
                    
                    last_message_time = datetime.now()
                    
                    while self.running:
                        try:
                            # Add timeout to prevent infinite waiting
                            msg = await asyncio.wait_for(websocket.recv(), timeout=RECV_TIMEOUT)
                            self.message_count += 1
                            last_message_time = datetime.now()
                            
                            self._process_message(msg)
                            self._print_stats()
                            
                        except asyncio.TimeoutError:
                            time_since_last = (datetime.now() - last_message_time).total_seconds()
                            logger.warning(f"No messages received for {time_since_last:.1f} seconds. Reconnecting...")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            continue
                        
            except Exception as e:
                logger.error(f"Connection error: {e}")
                if self.running:
                    logger.info(f"Reconnecting in {RECONNECT_DELAY} seconds...")
                    await asyncio.sleep(RECONNECT_DELAY)
            
            # Write any remaining batch data before reconnecting
            self._write_batch()

def main():
    """Main entry point"""
    monitor = BigLiquidationMonitor()
    
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("BIG liquidation monitor stopped")

if __name__ == "__main__":
    main()
