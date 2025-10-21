import ccxt
import os
from dotenv import load_dotenv
import time, schedule

load_dotenv()

api_key = os.getenv("PH_API_KEY")
secret_key = os.getenv("PH_SECRET_KEY")

if not api_key or not secret_key:
    raise ValueError("API keys not found. Please check your .env file.")

phemex = ccxt.phemex({
    "enableRateLimit": True,
    "apiKey": api_key,
    "secret": secret_key
})

balance = phemex.fetch_balance()
print(balance)

symbol = "uBTCUSDT"
size = 1
bid = 5
params = {
    "timeInForce": "PostOnly"
}
# # making a market order (symbol, size, bid, params)
# order = phemex.create_limit_buy_order(symbol, size, bid, params)
# print(order)

# # cancelling all orders
# phemex.cancel_all_orders(symbol)

# 1. create a limit buy order 
# 2. sleep for 10 seconds 
# 3. cancel the order

#phemex.create_limit_buy_order(symbol, size, bid, params)

# # sleep for 10 seconds
print("just made the order now sleeping for 10 seconds")
time.sleep(10)

# # cancel that order
#  phemex.cancel_order(symbol)

# # loop through orders to make and cancel
#go = True
#while go == True:
    # # make a limit buy order
    #phemex.create_limit_buy_order(symbol, size, bid, params)

    # # sleep for 5 seconds
    #time.sleep(5)

    # #cancel all orders
    #phemex.cancel_all_orders(symbol)


def bot():
    print("+++++ BOT RUNNING +++++")

    phemex.create_limit_buy_order(symbol, size, bid, params)
    time.sleep(10)
    phemex.cancel_all_orders(symbol)

# # schedule the bot to run every 28 seconds
schedule.every(28).seconds.do(bot)

# run the bot
while True:
    try:
        schedule.run_pending
    except:
        print("+++++ MAYBE AN INTERNET PROB OR SOMETHING")
    time.sleep(30)