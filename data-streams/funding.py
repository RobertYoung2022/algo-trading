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

# Shared counter for synchronization
shared_symbol_counter = {'count': 0}
print_lock = asyncio.Lock()

async def binance_funding_stream(symbol, shared_counter):
    global print_lock
    websocket_url = f'{websocket_url_base}{symbol.lower()}@markPrice'
    
    while True:
        try:
            async with connect(websocket_url) as websocket:
                while True:
                    try:
                        async with print_lock:
                            message = await websocket.recv()
                            data = json.loads(message)
                            event_time = datetime.fromtimestamp(data['E'] / 1000).strftime("%H:%M:%S")
                            symbol_display = data['s'].replace('USDT', '')
                            funding_rate = float(data['r'])  # Get funding rate directly from stream
                            yearly_funding_rate = (funding_rate * 3 * 365) * 100

                            # Color coding based on funding rate
                            if yearly_funding_rate > 50:
                                text_color, back_color = 'black', 'on_red'
                            elif yearly_funding_rate > 30:
                                text_color, back_color = 'black', 'on_yellow'
                            elif yearly_funding_rate > 5:
                                text_color, back_color = 'black', 'on_cyan'
                            elif yearly_funding_rate < -10:
                                text_color, back_color = 'black', 'on_green'
                            else:
                                text_color, back_color = 'black', 'on_light_green'

                            cprint(f"{symbol_display} funding: {yearly_funding_rate:.2f}%", text_color, back_color)

                            shared_counter['count'] += 1

                            if shared_counter['count'] >= len(symbols):
                                cprint(f"{event_time} yrly fund", 'white', 'on_black')
                                shared_counter['count'] = 0

                    except Exception as e:
                        print(f"Error processing {symbol} message: {e}")
                        break
        except Exception as e:
            print(f"Failed to connect to {symbol} stream: {e}")
            await asyncio.sleep(5)

async def main():
    print("Starting Binance funding rate monitor...")
    print(f"Tracking symbols: {symbols}")
    
    tasks = [binance_funding_stream(symbol, shared_symbol_counter) for symbol in symbols]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")

asyncio.run(main())
                    
