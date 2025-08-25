import asyncio
import json
from datetime import datetime
from websockets import connect
from termcolor import cprint

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

# Store latest funding rates for each symbol
funding_rates = {}

async def binance_funding_stream(symbol):
    print(f"Connecting to {symbol} stream...")
    while True:
        try:
            # Connect to mark price stream for continuous data
            mark_price_uri = f"{websocket_url_base}{symbol.lower()}@markPrice"
            async with connect(mark_price_uri, ping_interval=20, ping_timeout=20) as websocket:
                print(f"Connected to {symbol} mark price stream...")
                
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        event_time = datetime.fromtimestamp(data["E"] / 1000).strftime("%H:%M:%S")
                        symbol_display = data["s"].replace("USDT", "")
                        mark_price = float(data["p"])  # mark price field
                        
                        # Get stored funding rate or use 0.0 if not available
                        funding_rate = funding_rates.get(symbol, 0.0)
                        
                        # Calculate yearly funding rate
                        yearly_funding_rate = (funding_rate * 3 * 365) * 100

                        # Color coding based on funding rate
                        if yearly_funding_rate > 50:
                            text_color, back_color = "black", "on_red"
                        elif yearly_funding_rate > 30:
                            text_color, back_color = "black", "on_yellow"
                        elif yearly_funding_rate > 5:
                            text_color, back_color = "black", "on_cyan"
                        elif yearly_funding_rate > -10:
                            text_color, back_color = "black", "on_green"
                        else:
                            text_color, back_color = "black", "on_light_green"

                        cprint(f"{symbol_display}: ${mark_price:.2f} | Funding: {yearly_funding_rate:.2f}% at {event_time}", text_color, back_color)
                     
                    except Exception as e:
                        print(f"Error processing {symbol} message: {e}")
                        break
        except Exception as e:
            print(f"Failed to connect to {symbol} stream: {e}")
            await asyncio.sleep(5)

async def get_funding_rates():
    """Get current funding rates for all symbols"""
    print("Fetching current funding rates...")
    try:
        # Use REST API to get current funding rates
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for item in data:
                        symbol = item['symbol']
                        if symbol in symbols:
                            funding_rate = float(item['lastFundingRate'])
                            funding_rates[symbol] = funding_rate
                            print(f"Updated {symbol} funding rate: {funding_rate:.6f}")
    except Exception as e:
        print(f"Error fetching funding rates: {e}")

async def main():
    print("Starting Binance funding rate monitor...")
    print(f"Tracking symbols: {symbols}")
    print("Showing real-time mark prices with funding rate calculations...")

    # Get initial funding rates
    await get_funding_rates()

    # Create tasks for each symbol stream
    tasks = []
    for symbol in symbols:
        task = asyncio.create_task(binance_funding_stream(symbol))
        tasks.append(task)

    print("Connecting to Binance WebSocket streams...")
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        for task in tasks:
            task.cancel()

asyncio.run(main())
                    
