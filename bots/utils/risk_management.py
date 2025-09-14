'''
this file shows how to build a kill switch and pnl close
for hyper liquid. use at your own risk. 
'''

import nice_funcs as n 
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
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("PH_API_KEY")
secret_key = os.getenv("PH_SECRET_KEY")

# Check if credentials are available
if not secret_key or secret_key == "your_private_key_here":
    print("❌ Error: Please set your API credentials in the .env file")
    print("   Edit .env and replace 'your_private_key_here' with your actual private key")
    print("   Also set PH_API_KEY if needed")
    exit(1)

symbol = 'ETH'
max_loss = -5
target = 4 
acct_min = 7 
timeframe = '4h'
size = 10 
coin = symbol 
account = LocalAccount = eth_account.Account.from_key(secret_key)

def bot():

    print('this is our bot')

    print('controlling risk with our pnl close')

    # check pnl close
    n.pnl_close(symbol, target, max_loss, account)

    # if we have over X positions 
    
    # if my account size goes under $100, and never $70
    acct_val = float(n.acct_bal(account))

    if acct_val < acct_min:
        print(f'account value is {acct_val} and closing because out low is {acct_min}')
        n.kill_switch(symbol, account)

bot()




