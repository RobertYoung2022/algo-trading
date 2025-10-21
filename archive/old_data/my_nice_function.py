# SUPER NICE FUNCTIONS - Combined from all nice_funcs.py files
# This file combines the best features from all versions:
# - Root nice_funcs.py: Advanced trading system with Phemex
# - hyperliquid-bots/nice_funcs.py: Hyperliquid exchange integration
# - Day 10-12 versions: Technical indicators and specialized strategies

import ccxt
import json 
import pandas as pd 
import numpy as np
import os
from dotenv import load_dotenv
from datetime import date, datetime, timezone, tzinfo, timedelta
import time, schedule
import requests
import pandas_ta as ta
from eth_account.signers.local import LocalAccount
import eth_account 
from hyperliquid.info import Info 
from hyperliquid.exchange import Exchange 
from hyperliquid.utils import constants

# Load environment variables from .env file
load_dotenv() 

# =============================================================================
# EXCHANGE CONFIGURATIONS
# =============================================================================

# Phemex Configuration - using environment variables
phemex = ccxt.phemex({
    'enableRateLimit': True, 
    'apiKey': os.getenv('PHEMEX_API_KEY', ''), 
    'secret': os.getenv('PHEMEX_SECRET', '')
})

# Hyperliquid Configuration - using environment variables
HYPERLIQUID_PRIVATE_KEY = os.getenv('HYPERLIQUID_PRIVATE_KEY', '')

# Default symbol and parameters
symbol = 'APEUSD'
index_pos = 1  # CHANGE BASED ON WHAT ASSET

# Trading parameters
pause_time = 60
vol_repeat = 11
vol_time = 5
pos_size = 100
params = {'timeInForce': 'PostOnly'}
target = 35
max_loss = -55
vol_decimal = .4

# Technical analysis parameters
timeframe = '4h'
limit = 100
sma = 20

# =============================================================================
# PHEMEX EXCHANGE FUNCTIONS (From Root nice_funcs.py)
# =============================================================================

def ask_bid_phemex(symbol=symbol):
    """
    Get ask and bid prices from Phemex exchange
    Returns: ask, bid
    """
    ob = phemex.fetch_order_book(symbol)
    bid = ob['bids'][0][0]
    ask = ob['asks'][0][0]
    print(f'Phemex ask for {symbol}: {ask}')
    return ask, bid

def df_sma(symbol=symbol, timeframe=timeframe, limit=limit, sma=sma):
    """
    Create DataFrame with SMA and trading signals
    Returns: df_sma with SMA, signals, support/resistance
    """
    print('Starting technical indicators...')
    
    bars = phemex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df_sma = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df_sma['timestamp'] = pd.to_datetime(df_sma['timestamp'], unit='ms')

    # Calculate SMA
    df_sma[f'sma{sma}_{timeframe}'] = df_sma.close.rolling(sma).mean()
    
    # Generate trading signals
    bid = ask_bid_phemex(symbol)[1]
    df_sma.loc[df_sma[f'sma{sma}_{timeframe}'] > bid, 'sig'] = 'SELL'
    df_sma.loc[df_sma[f'sma{sma}_{timeframe}'] < bid, 'sig'] = 'BUY'

    # Calculate support and resistance
    df_sma['support'] = df_sma[:-2]['close'].min()
    df_sma['resis'] = df_sma[:-2]['close'].max()
    
    # Previous close comparison
    df_sma['PC'] = df_sma['close'].shift(1)
    df_sma.loc[df_sma['close'] > df_sma['PC'], 'lcBpc'] = True
    df_sma.loc[df_sma['close'] < df_sma['PC'], 'lcBpc'] = False

    return df_sma

def open_positions_phemex(symbol=symbol):
    """
    Get open positions from Phemex
    Returns: position info including size, side, etc.
    """
    # Position index mapping
    position_map = {
        'uBTCUSD': 4,
        'APEUSD': 2,
        'ETHUSD': 3,
        'DOGEUSD': 1,
        'u100000SHIBUSD': 0
    }
    
    index_pos = position_map.get(symbol, None)
    if index_pos is None:
        print(f"Symbol {symbol} not found in position map")
        return None, False, 0, None, None, None

    params = {'type': 'swap', 'code': 'USD'}
    phe_bal = phemex.fetch_balance(params=params)
    open_positions = phe_bal['info']['data']['positions']

    openpos_side = open_positions[index_pos]['side']
    openpos_size = open_positions[index_pos]['size']

    if openpos_side == 'Buy':
        openpos_bool = True 
        long = True 
    elif openpos_side == 'Sell':
        openpos_bool = True
        long = False
    else:
        openpos_bool = False
        long = None 

    print(f'Phemex positions: open={openpos_bool}, size={openpos_size}, long={long}, index={index_pos}')
    return open_positions, openpos_bool, openpos_size, long, index_pos, phe_bal

def volume_analysis(symbol=symbol, vol_repeat=vol_repeat, vol_time=vol_time):
    """
    Advanced volume analysis from order book
    Returns: volume control analysis
    """
    print(f'Fetching order book data for {symbol}...')
    
    df = pd.DataFrame()
    temp_df = pd.DataFrame()
    
    ob = phemex.fetch_order_book(symbol)
    bids = ob['bids']
    asks = ob['asks']

    bid_vol_list = []
    ask_vol_list = []

    for x in range(vol_repeat):
        for set in bids:
            vol = set[1]
            bid_vol_list.append(vol)
            sum_bidvol = sum(bid_vol_list)
            temp_df['bid_vol'] = [sum_bidvol]

        for set in asks:
            vol = set[1]
            ask_vol_list.append(vol)
            sum_askvol = sum(ask_vol_list)
            temp_df['ask_vol'] = [sum_askvol]

        time.sleep(vol_time)
        df = df.append(temp_df)
        print(df)
        print('------')

    total_bidvol = df['bid_vol'].sum()
    total_askvol = df['ask_vol'].sum()
    seconds = vol_time * vol_repeat
    mins = round(seconds / 60, 2)
    
    print(f'Last {mins}mins for {symbol}: Bid Vol: {total_bidvol} | Ask Vol: {total_askvol}')

    if total_bidvol > total_askvol:
        control_dec = (total_askvol / total_bidvol)
        print(f'Bulls in control: {control_dec}')
        bullish = True
    else:
        control_dec = (total_bidvol / total_askvol)
        print(f'Bears in control: {control_dec}')
        bullish = False

    # Check current position and volume conditions
    open_posi = open_positions_phemex(symbol)
    openpos_tf = open_posi[1]
    long = open_posi[3]
    
    if openpos_tf == True:
        if control_dec < vol_decimal:
            vol_under_dec = True
        else:
            vol_under_dec = False
    else:
        vol_under_dec = None

    return vol_under_dec

def kill_switch_phemex(symbol=symbol):
    """
    Emergency position closure for Phemex
    """
    print(f'Starting kill switch for {symbol}')
    openposi = open_positions_phemex(symbol)[1]
    long = open_positions_phemex(symbol)[3]
    kill_size = open_positions_phemex(symbol)[2]

    while openposi == True:
        print('Starting kill switch loop...')
        phemex.cancel_all_orders(symbol)
        openposi = open_positions_phemex(symbol)[1]
        long = open_positions_phemex(symbol)[3]
        kill_size = open_positions_phemex(symbol)[2]
        kill_size = int(kill_size)
        
        ask = ask_bid_phemex(symbol)[0]
        bid = ask_bid_phemex(symbol)[1]

        if long == False:
            phemex.create_limit_buy_order(symbol, kill_size, bid, params)
            print(f'BUY to CLOSE order: {kill_size} {symbol} at ${bid}')
            time.sleep(30)
        elif long == True:
            phemex.create_limit_sell_order(symbol, kill_size, ask, params)
            print(f'SELL to CLOSE order: {kill_size} {symbol} at ${ask}')
            time.sleep(30)

        openposi = open_positions_phemex(symbol)[1]

def pnl_close_phemex(symbol=symbol, target=target, max_loss=max_loss):
    """
    PnL monitoring and position closure for Phemex
    Returns: pnlclose, in_pos, size, long
    """
    print(f'Checking PnL for {symbol}...')
    
    params = {'type': "swap", 'code': 'USD'}
    pos_dict = phemex.fetch_positions(params=params)
    index_pos = open_positions_phemex(symbol)[4]
    pos_dict = pos_dict[index_pos]
    
    side = pos_dict['side']
    size = pos_dict['contracts']
    entry_price = float(pos_dict['entryPrice'])
    leverage = float(pos_dict['leverage'])
    current_price = ask_bid_phemex(symbol)[1]

    if side == 'long':
        diff = current_price - entry_price
        long = True
    else: 
        diff = entry_price - current_price
        long = False

    try: 
        perc = round(((diff/entry_price) * leverage), 10)
    except:
        perc = 0

    perc = 100 * perc
    print(f'PnL percentage: {perc}%')

    pnlclose = False 
    in_pos = False

    if perc > 0:
        in_pos = True
        print(f'Winning position: {perc}%')
        if perc > target:
            print(f'Target hit: {target}% - checking volume...')
            pnlclose = True
            vol_under_dec = volume_analysis(symbol)
            if vol_under_dec == True:
                print(f'Volume under threshold - sleeping 30s')
                time.sleep(30)
            else:
                print(f'Starting kill switch - target hit and volume OK')
                kill_switch_phemex(symbol)
        else:
            print('Target not yet reached')

    elif perc < 0:
        in_pos = True
        if perc <= max_loss:
            print(f'Max loss hit: {perc}% - starting kill switch')
            kill_switch_phemex(symbol)
        else:
            print(f'Losing position: {perc}% - within limits')

    return pnlclose, in_pos, size, long

# =============================================================================
# HYPERLIQUID EXCHANGE FUNCTIONS (From hyperliquid-bots/nice_funcs.py)
# =============================================================================

def ask_bid_hyperliquid(symbol):
    """
    Get ask and bid prices from Hyperliquid exchange
    Returns: ask, bid, l2_data
    """
    url = 'https://api.hyperliquid.xyz/info'
    headers = {'Content-Type': 'application/json'}

    data = {
        'type': 'l2Book', 
        'coin': symbol
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    l2_data = response.json()
    l2_data = l2_data['levels']

    bid = float(l2_data[0][0]['px'])
    ask = float(l2_data[1][0]['px'])

    return ask, bid, l2_data

def get_sz_px_decimals_hyperliquid(coin):
    """
    Get size and price decimals for Hyperliquid
    Returns: sz_decimals, px_decimals
    """
    url = 'https://api.hyperliquid.xyz/info'
    headers = {'Content-Type': 'application/json'}
    data = {'type': 'meta'}

    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        data = response.json()
        symbols = data['universe']
        symbol_info = next((s for s in symbols if s['name'] == coin), None)
        if symbol_info:
            sz_decimals = symbol_info['szDecimals']
        else:
            print('Symbol not found')
            return None, None
    else:
        print('Error:', response.status_code)
        return None, None

    ask = ask_bid_hyperliquid(coin)[0]
    ask_str = str(ask)
    if '.' in ask_str:
        px_decimals = len(ask_str.split('.')[1])
    else:
        px_decimals = 0 

    print(f'{coin} size decimals: {sz_decimals}, price decimals: {px_decimals}')
    return sz_decimals, px_decimals

def limit_order_hyperliquid(coin, is_buy, sz, limit_px, reduce_only, account):
    """
    Place limit order on Hyperliquid
    Returns: order_result
    """
    exchange = Exchange(account, constants.MAINNET_API_URL)
    rounding = get_sz_px_decimals_hyperliquid(coin)[0]
    sz = round(sz, rounding)
    
    print(f'Placing limit order: {coin} {sz} @ {limit_px}')
    order_result = exchange.order(coin, is_buy, sz, limit_px, {"limit": {"tif": 'Gtc'}}, reduce_only=reduce_only)

    if is_buy == True:
        print(f"BUY order placed: {order_result['response']['data']['statuses'][0]}")
    else:
        print(f"SELL order placed: {order_result['response']['data']['statuses'][0]}")

    return order_result

def acct_bal_hyperliquid(account):
    """
    Get account balance from Hyperliquid
    Returns: account_value
    """
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    user_state = info.user_state(account.address)
    acct_value = user_state["marginSummary"]["accountValue"]
    print(f'Hyperliquid account value: {acct_value}')
    return acct_value

def get_position_hyperliquid(symbol, account):
    """
    Get position information from Hyperliquid
    Returns: positions, in_pos, size, pos_sym, entry_px, pnl_perc, long
    """
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    user_state = info.user_state(account.address)
    print(f'Account value: {user_state["marginSummary"]["accountValue"]}')
    
    positions = []
    for position in user_state["assetPositions"]:
        if (position["position"]["coin"] == symbol) and float(position["position"]["szi"]) != 0:
            positions.append(position["position"])
            in_pos = True 
            size = float(position["position"]["szi"])
            pos_sym = position["position"]["coin"]
            entry_px = float(position["position"]["entryPx"])
            pnl_perc = float(position["position"]["returnOnEquity"]) * 100
            print(f'PnL percentage: {pnl_perc}%')
            break 
    else:
        in_pos = False 
        size = 0 
        pos_sym = None 
        entry_px = 0 
        pnl_perc = 0

    if size > 0:
        long = True 
    elif size < 0:
        long = False 
    else:
        long = None 

    return positions, in_pos, size, pos_sym, entry_px, pnl_perc, long

def adjust_leverage_size_signal(symbol, leverage, account):
    """
    Adjust leverage and calculate position size (95% of balance)
    Returns: leverage, size
    """
    print(f'Adjusting leverage to: {leverage}x')
    
    exchange = Exchange(account, constants.MAINNET_API_URL)
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    
    user_state = info.user_state(account.address)
    acct_value = float(user_state["marginSummary"]["accountValue"])
    acct_val95 = acct_value * .95

    print(exchange.update_leverage(leverage, symbol))
    
    price = ask_bid_hyperliquid(symbol)[0]
    size = (acct_val95 / price) * leverage
    size = float(size)
    rounding = get_sz_px_decimals_hyperliquid(symbol)[0]
    size = round(size, rounding)
        
    return leverage, size

def cancel_all_orders_hyperliquid(account):
    """
    Cancel all open orders on Hyperliquid
    """
    exchange = Exchange(account, constants.MAINNET_API_URL)
    info = Info(constants.MAINNET_API_URL, skip_ws=True)

    open_orders = info.open_orders(account.address)
    print('Cancelling all open orders...')
    for open_order in open_orders:
        exchange.cancel(open_order['coin'], open_order['oid'])

def kill_switch_hyperliquid(symbol, account):
    """
    Emergency position closure for Hyperliquid
    """
    position, im_in_pos, pos_size, pos_sym, entry_px, pnl_perc, long = get_position_hyperliquid(symbol, account)

    while im_in_pos == True:
        cancel_all_orders_hyperliquid(account)
        ask, bid, l2 = ask_bid_hyperliquid(symbol)
        pos_size = abs(pos_size)

        if long == True:
            limit_order_hyperliquid(pos_sym, False, pos_size, ask, True, account)
            print('Kill switch - SELL TO CLOSE')
            time.sleep(5)
        elif long == False:
            limit_order_hyperliquid(pos_sym, True, pos_size, bid, True, account)
            print('Kill switch - BUY TO CLOSE')
            time.sleep(5)

        position, im_in_pos, pos_size, pos_sym, entry_px, pnl_perc, long = get_position_hyperliquid(symbol, account)

    print('Position successfully closed')

def pnl_close_hyperliquid(symbol, target, max_loss, account):
    """
    PnL monitoring and position closure for Hyperliquid
    """
    print('Starting PnL close check...')
    position, im_in_pos, pos_size, pos_sym, entry_px, pnl_perc, long = get_position_hyperliquid(symbol, account)

    if pnl_perc > target:
        print(f'Target hit: {pnl_perc}% > {target}% - closing position')
        kill_switch_hyperliquid(pos_sym, account)
    elif pnl_perc <= max_loss:
        print(f'Max loss hit: {pnl_perc}% <= {max_loss}% - closing position')
        kill_switch_hyperliquid(pos_sym, account)
    else:
        print(f'PnL: {pnl_perc}% - within limits (target: {target}%, max_loss: {max_loss}%)')

    print('PnL close check finished')

def close_all_positions_hyperliquid(account):
    """
    Close all positions on Hyperliquid
    """
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    user_state = info.user_state(account.address)
    print(f'Account value: {user_state["marginSummary"]["accountValue"]}')
    
    # Cancel all orders first
    cancel_all_orders_hyperliquid(account)
    print('All orders cancelled')

    # Find all open positions
    open_positions = []
    for position in user_state["assetPositions"]:
        if float(position["position"]["szi"]) != 0:
            open_positions.append(position["position"]["coin"])

    # Close all positions
    for position in open_positions:
        kill_switch_hyperliquid(position, account)

    print('All positions closed')

# =============================================================================
# TECHNICAL ANALYSIS FUNCTIONS (From Day 10-12 versions)
# =============================================================================

def get_ohlcv_hyperliquid(symbol, interval, lookback_days):
    """
    Get OHLCV data from Hyperliquid
    Returns: snapshot_data
    """
    end_time = datetime.now()
    start_time = end_time - timedelta(days=lookback_days)
    
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

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        snapshot_data = response.json()
        return snapshot_data
    else:
        print(f"Error fetching data for {symbol}: {response.status_code}")
        return None

def process_data_to_df(snapshot_data):
    """
    Convert snapshot data to DataFrame
    Returns: df with OHLCV data
    """
    if snapshot_data:
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        data = []
        for snapshot in snapshot_data:
            timestamp = datetime.fromtimestamp(snapshot['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            open_price = snapshot['o']
            high_price = snapshot['h']
            low_price = snapshot['l']
            close_price = snapshot['c']
            volume = snapshot['v']
            data.append([timestamp, open_price, high_price, low_price, close_price, volume])

        df = pd.DataFrame(data, columns=columns)

        # Calculate support and resistance
        if len(df) > 2:
            df['support'] = df[:-2]['close'].min()
            df['resis'] = df[:-2]['close'].max()
        else:
            df['support'] = df['close'].min()
            df['resis'] = df['close'].max()

        return df
    else:
        return pd.DataFrame()

def calculate_bollinger_bands(df, length=20, std_dev=2):
    """
    Calculate Bollinger Bands with tight/wide classification
    Returns: df, tight, wide
    """
    # Ensure 'close' is numeric
    df['close'] = pd.to_numeric(df['close'], errors='coerce')

    # Calculate Bollinger Bands using pandas_ta
    bollinger_bands = ta.bbands(df['close'], length=length, std=std_dev)
    bollinger_bands = bollinger_bands.iloc[:, [0, 1, 2]]  # BBL, BBM, BBU
    bollinger_bands.columns = ['BBL', 'BBM', 'BBU']

    # Merge into original DataFrame
    df = pd.concat([df, bollinger_bands], axis=1)

    # Calculate Band Width
    df['BandWidth'] = df['BBU'] - df['BBL']

    # Determine thresholds
    tight_threshold = df['BandWidth'].quantile(0.2)
    wide_threshold = df['BandWidth'].quantile(0.8)

    # Classify current state
    current_band_width = df['BandWidth'].iloc[-1]
    tight = current_band_width <= tight_threshold
    wide = current_band_width >= wide_threshold

    return df, tight, wide

def calculate_vwap_with_symbol(symbol):
    """
    Calculate VWAP for a symbol
    Returns: df, latest_vwap
    """
    # Fetch and process data
    snapshot_data = get_ohlcv_hyperliquid(symbol, '15m', 300)
    df = process_data_to_df(snapshot_data)

    # Convert timestamp and set index
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    # Ensure numeric columns
    numeric_columns = ['high', 'low', 'close', 'volume']
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    df.dropna(subset=numeric_columns, inplace=True)
    df.sort_index(inplace=True)

    # Calculate VWAP
    df['VWAP'] = ta.vwap(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'])
    latest_vwap = df['VWAP'].iloc[-1]

    return df, latest_vwap

def supply_demand_zones_hl(symbol, timeframe, limit):
    """
    Calculate supply and demand zones
    Returns: sd_df with zones
    """
    print('Calculating supply and demand zones...')
    
    sd_df = pd.DataFrame()
    snapshot_data = get_ohlcv_hyperliquid(symbol, timeframe, limit)
    df = process_data_to_df(snapshot_data)

    supp = df.iloc[-1]['support']
    resis = df.iloc[-1]['resis']

    df['supp_lo'] = df[:-2]['low'].min()
    supp_lo = df.iloc[-1]['supp_lo']

    df['res_hi'] = df[:-2]['high'].max()
    res_hi = df.iloc[-1]['res_hi']

    sd_df[f'{timeframe}_dz'] = [supp_lo, supp]
    sd_df[f'{timeframe}_sz'] = [res_hi, resis]

    print('Supply and demand zones calculated')
    print(sd_df)

    return sd_df

def calculate_sma(prices, window):
    """
    Calculate Simple Moving Average
    Returns: latest SMA value
    """
    sma = prices.rolling(window=window).mean()
    return sma.iloc[-1]

def get_latest_sma(symbol, interval, window, lookback_days=1):
    """
    Get latest SMA for a symbol
    Returns: latest_sma
    """
    start_time = datetime.now() - timedelta(days=lookback_days)
    end_time = datetime.now()

    snapshots = get_ohlcv_hyperliquid(symbol, interval, lookback_days)

    if snapshots:
        prices = pd.Series([float(snapshot['c']) for snapshot in snapshots])
        latest_sma = calculate_sma(prices, window)
        return latest_sma
    else:
        return None

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def sleep_on_close(symbol=symbol, pause_time=pause_time):
    """
    Sleep after closing a position to avoid rapid re-entry
    """
    closed_orders = phemex.fetch_closed_orders(symbol)
    
    for ord in closed_orders[-1::-1]:
        sincelasttrade = pause_time - 1
        filled = False
        status = ord['info']['ordStatus']
        txttime = ord['info']['transactTimeNs']
        txttime = int(txttime)
        txttime = round((txttime/1000000000))
        
        print(f'Order status: {status} with epoch {txttime}')

        if status == 'Filled':
            print('Found last filled order...')
            orderbook = phemex.fetch_order_book(symbol)
            ex_timestamp = orderbook['timestamp']
            ex_timestamp = int(ex_timestamp/1000)
            
            time_spread = (ex_timestamp - txttime)/60

            if time_spread < sincelasttrade:
                sleepy = round(sincelasttrade-time_spread)*60
                sleepy_min = sleepy/60
                print(f'Time since last trade: {time_spread}mins - sleeping for 60s...')
                time.sleep(60)
            else:
                print(f'Time since last trade: {time_spread}mins - no sleep needed')
            break 
        else:
            continue 

    print(f'Sleep on close function completed for {symbol}')

def get_position_andmaxpos(symbol, account, max_positions):
    """
    Get position info and enforce max positions limit
    Returns: position info + num_of_pos
    """
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    user_state = info.user_state(account.address)
    print(f'Account value: {user_state["marginSummary"]["accountValue"]}')
    
    positions = []
    open_positions = []

    # Check max positions first
    for position in user_state["assetPositions"]:
        if float(position["position"]["szi"]) != 0:
            open_positions.append(position["position"]["coin"])

    num_of_pos = len(open_positions)

    if len(open_positions) > max_positions:
        print(f'Max positions exceeded: {len(open_positions)} > {max_positions} - closing positions')
        for position in open_positions:
            kill_switch_hyperliquid(position, account)
    else:
        print(f'Position count OK: {len(open_positions)} <= {max_positions}')

    # Get specific position info
    for position in user_state["assetPositions"]:
        if (position["position"]["coin"] == symbol) and float(position["position"]["szi"]) != 0:
            positions.append(position["position"])
            in_pos = True 
            size = float(position["position"]["szi"])
            pos_sym = position["position"]["coin"]
            entry_px = float(position["position"]["entryPx"])
            pnl_perc = float(position["position"]["returnOnEquity"])*100
            print(f'PnL percentage: {pnl_perc}%')
            break 
    else:
        in_pos = False 
        size = 0 
        pos_sym = None 
        entry_px = 0 
        pnl_perc = 0

    if size > 0:
        long = True 
    elif size < 0:
        long = False 
    else:
        long = None 

    return positions, in_pos, size, pos_sym, entry_px, pnl_perc, long, num_of_pos

# =============================================================================
# MAIN TRADING FUNCTIONS (Combined Logic)
# =============================================================================

def universal_ask_bid(symbol, exchange='phemex'):
    """
    Universal ask/bid function for both exchanges
    Returns: ask, bid, additional_data
    """
    if exchange.lower() == 'phemex':
        ask, bid = ask_bid_phemex(symbol)
        return ask, bid, None
    elif exchange.lower() == 'hyperliquid':
        ask, bid, l2_data = ask_bid_hyperliquid(symbol)
        return ask, bid, l2_data
    else:
        raise ValueError("Exchange must be 'phemex' or 'hyperliquid'")

def universal_pnl_close(symbol, target, max_loss, account=None, exchange='phemex'):
    """
    Universal PnL close function for both exchanges
    """
    if exchange.lower() == 'phemex':
        return pnl_close_phemex(symbol, target, max_loss)
    elif exchange.lower() == 'hyperliquid':
        if account is None:
            raise ValueError("Account required for Hyperliquid")
        return pnl_close_hyperliquid(symbol, target, max_loss, account)
    else:
        raise ValueError("Exchange must be 'phemex' or 'hyperliquid'")

def universal_kill_switch(symbol, account=None, exchange='phemex'):
    """
    Universal kill switch for both exchanges
    """
    if exchange.lower() == 'phemex':
        return kill_switch_phemex(symbol)
    elif exchange.lower() == 'hyperliquid':
        if account is None:
            raise ValueError("Account required for Hyperliquid")
        return kill_switch_hyperliquid(symbol, account)
    else:
        raise ValueError("Exchange must be 'phemex' or 'hyperliquid'")

# =============================================================================
# EXAMPLE USAGE AND TESTING
# =============================================================================

def create_hyperliquid_account():
    """
    Create Hyperliquid account from environment variable
    Returns: account object
    """
    if not HYPERLIQUID_PRIVATE_KEY:
        raise ValueError("HYPERLIQUID_PRIVATE_KEY not found in environment variables")
    
    account = eth_account.Account.from_key(HYPERLIQUID_PRIVATE_KEY)
    print(f"Hyperliquid account created for address: {account.address}")
    return account

def example_usage():
    """
    Example usage of the combined functions
    """
    print("=== SUPER NICE FUNCTIONS EXAMPLE USAGE ===")
    
    # Example 1: Phemex trading
    print("\n1. Phemex Trading Example:")
    try:
        ask, bid = ask_bid_phemex('APEUSD')
        print(f"APEUSD - Ask: {ask}, Bid: {bid}")
    except Exception as e:
        print(f"Phemex error (likely missing API keys): {e}")
    
    # Example 2: Hyperliquid trading (requires account setup)
    print("\n2. Hyperliquid Trading Example:")
    try:
        account = create_hyperliquid_account()
        ask, bid, l2 = ask_bid_hyperliquid('ETH')
        print(f"ETH - Ask: {ask}, Bid: {bid}")
    except Exception as e:
        print(f"Hyperliquid error (likely missing private key): {e}")
    
    # Example 3: Technical Analysis
    print("\n3. Technical Analysis Example:")
    snapshot_data = get_ohlcv_hyperliquid('BTC', '1h', 7)
    if snapshot_data:
        df = process_data_to_df(snapshot_data)
        print(f"BTC DataFrame shape: {df.shape}")
        
        # Calculate Bollinger Bands
        df_bb, tight, wide = calculate_bollinger_bands(df)
        print(f"Bollinger Bands - Tight: {tight}, Wide: {wide}")
        
        # Calculate VWAP
        df_vwap, latest_vwap = calculate_vwap_with_symbol('BTC')
        print(f"Latest VWAP: {latest_vwap}")
    
    # Example 4: Supply/Demand Zones
    print("\n4. Supply/Demand Zones Example:")
    sd_zones = supply_demand_zones_hl('BTC', '1h', 24)
    print("Supply/Demand zones calculated")
    
    print("\n=== EXAMPLE COMPLETE ===")

if __name__ == "__main__":
    example_usage()
