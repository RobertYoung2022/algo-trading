import asyncio
import json
import os
from datetime import datetime
import pytz
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
trades_filename = "recent_trades.csv"

# check if the csv files exists
if not os.path.exists(trades_filename):
    with open(trades_filename, "w") as f:
        f.write("Event Time,Symbol,Aggregate Trade ID,Price,Quantity,First Trade ID,Trade Time,Is Buyer Maker,USD Size\n")


async def binance_trade_stream(uri, symbol, filename):
    print(f"Connecting to {symbol} stream...")
    while True:
        try:
            async with connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                print(f"Connected to {symbol} stream successfully")
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        event_time = int(data["E"])
                        agg_trade_id = data["a"]
                        price = float(data["p"])
                        quantity = float(data["q"])
                        trade_time = int(data["T"])
                        is_buyer_maker = data["m"]
                        est = pytz.timezone("US/Central")
                        readable_trade_time = datetime.fromtimestamp(trade_time / 1000, est).strftime("%H:%M:%S")
                        usd_size = price * quantity
                        display_symbol = symbol.upper().replace("USDT", "")

                        if usd_size > 14999:
                            trade_type = "SELL" if is_buyer_maker else "BUY"
                            color = "red" if trade_type == "SELL" else "green"

                            stars = ""
                            attrs = ["bold"] if usd_size >= 50000 else []
                            repeat_count = 1
                            if usd_size >= 50000:
                                stars = "*" * 2
                                repeat_count = 1
                                if trade_type == "SELL":
                                    color = "magenta"
                                else:
                                    color = "blue"

                            elif usd_size >= 100000:
                                stars = "*" * 1
                                repeat_count = 1
                                if trade_type == "SELL":
                                    color = "red"
                                else:
                                    color = "green"

                            elif usd_size >= 500000:
                                stars = "*" * 4
                                repeat_count = 1

                            # Format price and USD size with limited decimal places
                            formatted_price = f"{price:.4f}"
                            formatted_usd_size = f"{usd_size:.4f}"
                            output = f"{stars} {trade_type} {display_symbol} {formatted_price} {readable_trade_time} ${formatted_usd_size}"
                            for _ in range(repeat_count):
                                cprint(output, "white", f"on_{color}", attrs=attrs)

                            # log to csv
                            with open(filename, "a") as f:
                                f.write(f"{event_time},{symbol.upper()},{agg_trade_id},{price},{quantity},"
                                       f"{agg_trade_id},{trade_time},{is_buyer_maker},{usd_size:.2f}\n")
                     
                    except Exception as e:
                        print(f"Error processing {symbol} message: {e}")
                        break
        except Exception as e:
            print(f"Failed to connect to {symbol} stream: {e}")
            await asyncio.sleep(5)
                

async def main():
    filename = "recent_trades.csv"
    print("Starting Binance trade monitor...")
    print(f"Tracking symbols: {symbols}")

    # Create tasks for each symbol trade stream
    tasks = []
    for symbol in symbols:
        stream_url = f"{websocket_url_base}{symbol.lower()}@aggTrade"
        task = asyncio.create_task(binance_trade_stream(stream_url, symbol, filename))
        tasks.append(task)

    print("Connecting to Binance WebSocket streams...")
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        for task in tasks:
            task.cancel()


asyncio.run(main())






































            