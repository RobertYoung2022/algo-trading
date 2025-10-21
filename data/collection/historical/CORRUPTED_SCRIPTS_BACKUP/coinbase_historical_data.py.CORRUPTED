'''
STEPS TO USE
1. create a .env file that looks like this:
    COINBASE_API_KEY="your_api_key_id"
    COINBASE_API_SECRET="your_api_secret"
    COINBASE_PASSPHRASE="your_passphrase" (optional - only if your API type requires it)
2. select the symbol you want to fetch data for
3. select the timeframe you want to fetch data for
4. select the number of weeks of data to fetch
5. run the script

Note: You need a Coinbase Exchange API key (not Coinbase Pro or Advanced Trade)
'''


# ====== BobbyYo's Configuration 🌙 ======
# 🔧 MODIFY THESE SETTINGS TO CHANGE WHAT DATA YOU FETCH:

SYMBOL = 'ETH-USD'        # Trading pair - CHANGE THIS:
                         # Popular pairs: 'BTC-USD', 'ETH-USD', 'SOL-USD', 'ADA-USD', 'DOT-USD', 'MATIC-USD', 'AVAX-USD', 'LINK-USD', 'UNI-USD'
                         # More pairs: 'XRP-USD', 'LTC-USD', 'BCH-USD', 'ALGO-USD', 'ATOM-USD', 'NEAR-USD', 'FTM-USD', 'SAND-USD', 'MANA-USD'

TIMEFRAME = '1m'          # Timeframe - CHANGE THIS:
                         # Available: '1m', '5m', '15m', '1h', '6h', '1d'
                         # Note: '1m' gives more data but takes longer to fetch

WEEKS = 1000                # How many weeks of data to fetch - CHANGE THIS:
                         # Examples: 10 (2.5 months), 26 (6 months), 52 (1 year), 104 (2 years), 208 (4 years)
                         # Warning: Large values may hit API limits or take very long

SAVE_DIR = 'data/coinbase'  # Directory to save files - CHANGE IF NEEDED:
                           # Examples: 'data/coinbase', 'data/historical', 'backup_data'

# ====== Imports ======
import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from math import ceil
from pathlib import Path
import requests
import time
import hmac
import hashlib
import base64
import json
from urllib.parse import urlencode

# Create save directory if it doesn't exist
os.makedirs(SAVE_DIR, exist_ok=True)
print(f"📂 Save directory ready: {SAVE_DIR}")

# Get the project root directory (2 levels up from this file)
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'

print(f"🔍 Looking for .env file in: {project_root}")
print(f"📁 .env file exists: {'✅' if env_path.exists() else '❌'}")

# Load environment variables from the specific path
load_dotenv(env_path)

# Debug prints for API credentials (without revealing them)
api_key = os.getenv('COINBASE_API_KEY')
api_secret = os.getenv('COINBASE_API_SECRET')
print("🔑 API Key loaded:", "✅" if api_key else "❌")
print("🔒 API Secret loaded:", "✅" if api_secret else "❌")

if not api_key or not api_secret:
    print("❌ Error: API credentials not found in .env file")
    print("💡 Make sure your .env file exists and contains:")
    print("   COINBASE_API_KEY=your_api_key_id")
    print("   COINBASE_API_SECRET=your_api_secret")
    print("   COINBASE_PASSPHRASE=your_passphrase (optional - only if required by your API type)")
    raise ValueError("Missing API credentials")

print("🌙 BobbyYo's Coinbase Data Fetcher Initialized! 🚀")

def sign_request(method, path, body='', timestamp=None):
    """Sign a request using the API secret"""
    timestamp = timestamp or str(int(time.time()))
    
    # Create the message to sign
    message = f"{timestamp}{method}{path}{body}"
    
    try:
        # Print debug info without exposing secrets
        print(f"🔏 Signing request for: {method} {path}")
        
        # Create the signature using HMAC SHA256
        signature = hmac.new(
            api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # Create headers for Coinbase Exchange API
        headers = {
            'CB-ACCESS-KEY': api_key,
            'CB-ACCESS-SIGN': signature,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'accept': 'application/json',
            'content-type': 'application/json',
        }
        
        # Add passphrase only if it exists (some API types don't require it)
        passphrase = os.getenv('COINBASE_PASSPHRASE')
        if passphrase:
            headers['CB-ACCESS-PASSPHRASE'] = passphrase
        
        return headers
        
    except Exception as e:
        print(f"❌ Error generating signature: {str(e)}")
        raise

def timeframe_to_granularity(timeframe):
    """Convert timeframe to granularity in seconds"""
    if 'm' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60
    elif 'h' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60 * 60
    elif 'd' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 24 * 60 * 60

def get_historical_data(symbol, timeframe, weeks):
    print(f"🔍 BobbyYo is fetching {weeks} weeks of {timeframe} data for {symbol}")
    
    output_file = os.path.join(SAVE_DIR, f'{symbol.replace("-", "")}-{timeframe}-{weeks}wks-data.csv')
    if os.path.exists(output_file):
        print("📂 Found existing data file!")
        return pd.read_csv(output_file)

    try:
        # Test connection with a simple request
        print("🌎 Testing API connection...")
        base_url = "https://api.exchange.coinbase.com"  # Changed to exchange URL
        
        # Get product details first
        path = '/products/' + symbol
        headers = sign_request('GET', path)
        print("🔐 Generated authentication headers")
        print("🔄 Making API request...")
        
        response = requests.get(
            f"{base_url}{path}",
            headers=headers
        )
        
        if response.status_code != 200:
            print(f"❌ Response Headers: {response.headers}")
            print(f"❌ Response Body: {response.text}")
            raise Exception(f"API Error: {response.status_code} - {response.text}")
            
        print("✨ Connection test successful!")
        
        # Calculate time ranges
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(weeks=weeks)
        granularity = timeframe_to_granularity(timeframe)
        
        # Calculate appropriate chunk size based on granularity
        # Coinbase limit is 300 candles per request
        max_candles = 300
        chunk_hours = max(1, int((max_candles * granularity) / 3600))  # Convert to hours, minimum 1 hour
        print(f"📊 Using {chunk_hours} hour chunks for {timeframe} timeframe")
        
        # Fetch candles in chunks to avoid rate limits
        all_candles = []
        current_start = start_time
        
        while current_start < end_time:
            current_end = min(current_start + datetime.timedelta(hours=chunk_hours), end_time)
            
            print(f"📊 Fetching data from {current_start.strftime('%Y-%m-%d %H:%M')} to {current_end.strftime('%Y-%m-%d %H:%M')}")
            
            params = {
                'start': current_start.isoformat(),
                'end': current_end.isoformat(),
                'granularity': str(granularity)
            }
            
            path = f'/products/{symbol}/candles'
            headers = sign_request('GET', path)
            
            response = requests.get(
                f"{base_url}{path}",
                params=params,
                headers=headers
            )
            
            if response.status_code != 200:
                print(f"❌ Response Headers: {response.headers}")
                print(f"❌ Response Body: {response.text}")
                raise Exception(f"API Error: {response.status_code} - {response.text}")
                
            candles = response.json()
            all_candles.extend(candles)  # Changed to handle direct response
            current_start = current_end
            time.sleep(0.5)  # Rate limit compliance
            
        print(f"✨ Successfully fetched {len(all_candles)} candles!")
            
        # Convert to DataFrame
        df = pd.DataFrame(all_candles)
        # Map what Coinbase sends: [timestamp, low, high, open, close, volume]
        df.columns = ['datetime', 'low', 'high', 'open', 'close', 'volume']
        df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
        
        # Reorder to standard OHLC format: Open, High, Low, Close, Volume
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        df = df.set_index('datetime')
        
        # Save to file
        df.to_csv(output_file)
        print(f"💾 Data saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Tips:")
        print("   1. Make sure you're using a Coinbase Exchange API key")
        print("   2. Check if your API key has the required permissions")
        print("   3. Verify your API key is active")
        raise

# Get the data
print(get_historical_data(SYMBOL, TIMEFRAME, WEEKS))
