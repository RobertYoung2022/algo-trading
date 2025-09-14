# 🤖 Bots Directory

This directory contains all trading bots organized by type and functionality.

## 📁 Directory Structure

### `/hyperliquid/`
Hyperliquid-specific trading bots:
- `bollinger_bot.py` - Bollinger Bands trading strategy
- `breakout_bot.py` - Breakout trading strategy  
- `sdz_bot.py` - SDZ trading strategy
- `vwap_bot.py` - VWAP trading strategy

### `/day_based/`
Day-based trading bots (organized by development day):
- `bot_day10.py` - Day 10 trading bot
- `bot_day11.py` - Day 11 trading bot
- `bot_day12.py` - Day 12 trading bot

### `/strategies/`
Strategy-specific trading bots:
- `nadarya_watson_bot.py` - Nadarya-Watson algorithm bot

### `/utils/`
Utility files and helper functions:
- `hyperliquid_functions.py` - Hyperliquid API functions and utilities
- `optimization.py` - Bot optimization utilities
- `risk_management.py` - Risk management functions
- `test_credentials.py` - Credential testing utilities
- `bt_bo_multi.py` - Backtest optimization utilities

## 🚀 Usage

Each bot directory contains self-contained trading strategies. Check individual bot files for:
- Configuration requirements
- API credentials needed
- Trading parameters
- Risk management settings

## 📝 Notes

- All bots have been reorganized from the original scattered structure
- File names have been standardized for better clarity
- Utility functions are shared across bots where applicable
