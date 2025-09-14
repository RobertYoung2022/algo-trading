# Coinbase API Setup Guide

## Step 1: Create the .env file

Create a `.env` file in your project root directory (`/Users/bobbyyo/Projects/algo-fun/.env`) with the following content:

```bash
# Coinbase API Credentials
COINBASE_API_KEY="your_api_key_id_here"
COINBASE_API_SECRET="your_api_secret_here"
COINBASE_PASSPHRASE="your_passphrase_here"  # Optional - only if your API type requires it
```

## Step 2: Get Your Coinbase API Credentials

You mentioned you have:
- API Key ID
- API Secret  
- Client API Key

For Coinbase Exchange API, you need:
1. **API Key ID** - This goes in `COINBASE_API_KEY`
2. **API Secret** - This goes in `COINBASE_API_SECRET`
3. **Passphrase** - This is OPTIONAL and only required for certain API types. If you created a passphrase when setting up your API key, include it in `COINBASE_PASSPHRASE`. If you didn't create one or don't remember it, you can leave this line out.

## Step 3: Important Notes

- Make sure you're using a **Coinbase Exchange** API key, not Coinbase Pro or Advanced Trade API
- The API key needs **View** permissions for market data
- Keep your `.env` file secure and never commit it to version control

## Step 4: Test the Setup

Run the script to test your setup:

```bash
cd /Users/bobbyyo/Projects/algo-fun
python coinbase_data_2025.py
```

## Step 5: Configuration Options

You can modify these settings in the script:

```python
SYMBOL = 'BTC-USD'        # Trading pair (e.g., 'BTC-USD', 'ETH-USD', 'SOL-USD')
TIMEFRAME = '5m'          # Timeframe (e.g., '1m', '5m', '1h', '6h', '1d')
WEEKS = 70                # How many weeks of data to fetch
SAVE_DIR = 'data/coinbase'  # Directory to save the data files
```

## Troubleshooting

If you get authentication errors:
1. Double-check your API credentials in the `.env` file
2. Ensure you're using Coinbase Exchange API (not Pro or Advanced Trade)
3. Verify your API key has the correct permissions
4. Make sure your passphrase is correct

The script will provide helpful error messages to guide you through any issues.
