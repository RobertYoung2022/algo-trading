import os
from dotenv import load_dotenv
from eth_account.signers.local import LocalAccount
import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange

def check_account_activity():
    """
    Check if the account has any activity on Hyperliquid
    """
    load_dotenv()
    secret_key = os.getenv("PH_SECRET_KEY")
    
    if not secret_key:
        print("❌ No secret key found")
        return
    
    account = eth_account.Account.from_key(secret_key)
    print(f"🔍 Checking account: {account.address}")
    
    # Check if account has any activity
    info = Info()
    
    try:
        # Try to get user fills for this account
        print("\n📊 Checking for trading activity...")
        
        # Get recent fills for the account
        fills = info.user_fills(account.address)
        if fills:
            print(f"✅ Found {len(fills)} recent trades")
            print(f"   Latest trade: {fills[0]}")
        else:
            print("⚠️  No trading activity found")
            
    except Exception as e:
        print(f"❌ Could not check fills: {e}")
    
    try:
        # Check if account has any positions
        print("\n📈 Checking for open positions...")
        positions = info.user_state(account.address)
        if positions and positions.get('assetPositions'):
            print(f"✅ Found {len(positions['assetPositions'])} open positions")
        else:
            print("⚠️  No open positions found")
            
    except Exception as e:
        print(f"❌ Could not check positions: {e}")
    
    print("\n💡 If you see 'No trading activity' and 'No open positions',")
    print("   this means your account hasn't been used on Hyperliquid yet.")
    print("   You may need to:")
    print("   1. Fund your account on Hyperliquid")
    print("   2. Make your first trade")
    print("   3. Or use a different account that has been active")

if __name__ == "__main__":
    check_account_activity()