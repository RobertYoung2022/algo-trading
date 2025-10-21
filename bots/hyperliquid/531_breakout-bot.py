from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import my_nice_function as n
from eth_account.signers.local import LocalAccount
import eth_account
import json, time
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
import ccxt
import pandas as pd
import datetime
import schedule
import requests
from dotenv import load_dotenv

load_dotenv()

order_usd_size = 10

# Define the lookback period in hours
lookback_hours = 1 # in hours... .25, .5, .75, 1, 2, 3, 4, 6, 8, 12, 24 etc... Just not lower than 15
leverage = 3

# Fetch symbols from the API
def get_symbols():
    url = 'https://api.hyperliquid.xyz/info'
    headers = {'Content-Type': 'application/json'}
    data = {'type': 'meta'}

    response = requests. post(url, headers = headers, json = data)
    if response.status_code != 200:
        print('Error:', response.status_code)
        return []

    data = response.json()
    symbols = [symbol['name'] for symbol in data['universe']]
    return symbols

# Fetch candle snapshot for a given symbol and time range
def fetch_candle_snapshot(symbol, interval, start_time, end_time):
    url = 'https://api.hyperliquid.xyz/info'
    headers = {'Content-Type': 'application/json'}
    data = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": interval,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000)
        }
    }

    response = requests.post(url, headers = headers, json = data)
    if response.status_code == 200:
        snapshot_data = response.json()
        if 'candles' in snapshot_data:
            return snapshot_data['candles']
        else:
            return snapshot_data # Adjust if the structure if different

    else:
        print(f"Error fetching data for {symbol}: {response.status_code}")
        return None

# Fetch daily data for calculating resistance levels
def fetch_daily_data(symbol, days = 20):
    end_time = datetime.now()
    start_time = end_time - timedelta( days = days)
    return fetch_candle_snapshot(symbol, '1d', start_time, end_time)

# Calculate daily resistance levels
def calculate_daily_resistance(symbols):
    resistance_levels = {}
    for symbol in symbols:
        daily_data = fetch_daily_data(symbol)
        if daily_data:
            high_prices = [float(day['h']) for day in daily_data]
            resistance_levels[symbol] = max(high_prices)
            print(f"Resistance level for {symbol}: {resistance_levels[symbol]}")
        else:
            print(f"No daily data found for {symbol}")

    return resistance_levels

# Calculate the breakout strategy conditions
def check_breakout(symbol, hourly_snapshots, daily_resistance):
    # print (hourly_snapshots)
    current_close = float(hourly_snapshots['close'].iloc[-1])
    print(f"current close: {current_close}")

    # Get the daily resistance level
    daily_resistance_level = daily_resistance.get(symbol, None)
    print(f"this is the daily resistance {daily_resistance}")
    if not daily_resistance_level:
        print(f"Daily resistance level not found for {symbol}")
        return None

    print(f"current close: {current_close} resistance level: {daily_resistance_level}")

    if current_close > daily_resistance_level:
        entry_price = current_close
        stop_loss = entry_price * (1 - 18 / 100) # Stop loss at 18%
        take_profit = entry_price * (1 + 3 / 100) # Take profit at 3%

        # print
        print(f"breakout detected for {symbol}: Entry Price: {entry_price}, Stop Loss: {stop_loss}, Take Profit: {take_profit}")

        # Check if SL < entry price < TP
        if stop_loss < entry_price < take_profit:
            return {
                'Symbol': symbol,
                'Entry Price': entry_price,
                'Stop Loss': stop_loss,
                'Take Profit': take_profit
            }

    return None



# Main function to scan for breakout conditions
def main():

    # GET BALANCES FROM ACCOUNT 1, SIZE, POSITION, ENTRY PRICE, PNL, LONG/SHORT
    secret_key = os.getenv('PH_SECRET_KEY')
    if not secret_key:
        print("❌ Error: PH_SECRET_KEY not found in environment variables")
        print("   Please set your private key in the .env file")
        return
    
    account1 = eth_account.Account.from_key(secret_key)

    symbols = get_symbols()
    # symbols = ['ETH'] # for quickly testing
    resistance_levels = calculate_daily_resistance(symbols)
    breakout_data = []

    print(resistance_levels)

    for symbol in symbols:

        # Fetch the OHCLV data
        snapshot_data = n.get_ohlcv_hyperliquid(symbol, '1h', 2)
        hourly_snapshots = n.process_data_to_df(snapshot_data)
        # print(hourly_snapshots)

        if not hourly_snapshots.empty:
            breakout_info = check_breakout(symbol, hourly_snapshots, resistance_levels)
            if breakout_info:
                breakout_data.append(breakout_info)


    breakout_df = pd.DataFrame(breakout_data)
    print("Breakout DataFrame:")
    print(breakout_df)

    # Save the results to a CSV file
    breakout_df.to_csv('/Users/bobbyyo/Desktop/breakout_change.csv', index = False)

    # for symbol in breakout df, open orders and set tp and sl
    for index, symbol_info in breakout_df.iterrows():
        print(f"This is the symbol info for index {index}:")
        print(symbol_info)
        symbol = symbol_info['Symbol']
        print(f"passing {symbol} to adjust leverage and open order...")
        lev, size = n.adjust_leverage_size_signal(symbol, leverage, account1)
        # Convert USD size to position size based on current price
        ask, bid, l2_data = n.ask_bid_hyperliquid(symbol)
        size = order_usd_size / ask  # Convert USD to position size

        # check if we have a position of that symbol
        positions, im_in_pos, pos_size, pos_sym, entry_px, pnl_perc, long = n.get_position_hyperliquid(symbol, account1)

        if not im_in_pos:
            print(f"Not in position for {symbol}")
            # Create a simple order using the symbol info
            entry_price = symbol_info['Entry Price']
            stop_loss = symbol_info['Stop Loss']
            take_profit = symbol_info['Take Profit']
            
            # Place a buy order at entry price
            n.limit_order_hyperliquid(symbol, True, size, entry_price, False, account1)
            print(f"Order opened for {symbol} at {entry_price}")
        else:
            print(f"Already in a position for {symbol}")


    print("Running algorithm...")
    main()  # Run once immediately
    schedule.every(1).minutes.do(main)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            print(f"Encountered an error: {e}")
            time.sleep(10)