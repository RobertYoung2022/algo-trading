# Configuration for Phemex exchange
# IMPORTANT: Never commit real API keys to version control!
# Use environment variables instead:
# export PHEMEX_API_KEY="your_key_here"
# export PHEMEX_SECRET="your_secret_here"

import os
from dotenv import load_dotenv

load_dotenv()

phemex_KEY = os.getenv('PHEMEX_API_KEY', '')
phemex_SECRET = os.getenv('PHEMEX_SECRET', '')

# Validate that keys are loaded
if not phemex_KEY or not phemex_SECRET:
    print("⚠️  Warning: Phemex API credentials not found in environment variables")
    print("   Please set PHEMEX_API_KEY and PHEMEX_SECRET in your .env file")