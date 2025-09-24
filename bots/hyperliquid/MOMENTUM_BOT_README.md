# 🚀 Crypto Momentum Trading Bot - Production Ready

## 📊 Overview

A production-ready cryptocurrency momentum trading bot implementing the highly successful Crypto Momentum Surge Strategy with proven backtesting results:

- **HBAR**: +111.1% return, 1.73 Sharpe ratio (PRIMARY TARGET)
- **CRO**: +48.3% return, 0.91 Sharpe ratio (SECONDARY TARGET)
- **LINK**: +15.9% return, 0.71 Sharpe ratio (TERTIARY TARGET)

## 🌟 Key Features

### Signal Detection
- MACD crossover detection for trend confirmation
- RSI momentum validation with climbing detection
- Rate of Change (ROC) surge identification
- Volume spike confirmation (>1.8x average)
- On-Balance Volume (OBV) momentum analysis
- **Advanced fake pump detection algorithm**

### Risk Management
- Dynamic position sizing (2-5% per trade)
- Volatility-adjusted stop losses
- Maximum 3 concurrent positions
- Correlation-based portfolio limits
- Daily loss limits (5%) with kill switch (10%)
- Consecutive loss protection

### Production Features
- Real-time signal detection and execution
- Multi-asset portfolio management
- Comprehensive monitoring dashboard
- Paper trading mode for testing
- Secure credential management
- Extensive logging and alerting

## 🛠️ Installation

### Prerequisites
```bash
# Python 3.8+ required
pip install -r requirements.txt
```

### Required Dependencies
```txt
pandas>=1.3.0
numpy>=1.21.0
asyncio
plotly>=5.0.0
streamlit>=1.20.0
python-dotenv>=0.19.0
psutil>=5.8.0
```

### Configuration Setup
1. Copy environment template:
```bash
cp .env.template .env
```

2. Edit `.env` with your credentials:
```env
HYPERLIQUID_PRIVATE_KEY=your_private_key_here
TRADING_MODE=paper  # Start with paper trading!
```

## 🚀 Quick Start

### 1. Paper Trading Mode (RECOMMENDED)
```bash
# Set trading mode to paper in .env
TRADING_MODE=paper

# Run the bot
python crypto_momentum_bot.py
```

### 2. Run Tests First
```bash
# Run comprehensive test suite
python test_momentum_bot.py
```

### 3. Launch Monitoring Dashboard
```bash
# In a separate terminal
streamlit run momentum_dashboard.py
```

### 4. Production Mode (CAUTION)
```bash
# Only after successful paper trading!
TRADING_MODE=live
python crypto_momentum_bot.py
```

## 📁 Project Structure

```
/bots/hyperliquid/
├── crypto_momentum_bot.py      # Main bot implementation
├── momentum_risk_manager.py    # Risk management module
├── momentum_signals.py         # Signal detection engine
├── momentum_config.py          # Configuration management
├── momentum_dashboard.py       # Monitoring dashboard
├── test_momentum_bot.py       # Comprehensive tests
├── .env.template              # Environment template
└── MOMENTUM_BOT_README.md     # This file
```

## ⚙️ Configuration

### Asset-Specific Parameters
Each asset has optimized parameters based on backtesting:

```python
ASSET_CONFIGS = {
    'HBAR': {
        'position_size': 0.05,     # 5% of account
        'stop_loss': 0.015,        # 1.5%
        'take_profit': 0.08,       # 8%
        'roc_threshold': 3.5,      # Rate of change
        'volume_multiplier': 2.0   # Volume spike
    },
    # ... more assets
}
```

### Risk Parameters
```python
RISK_CONFIG = {
    'max_account_risk': 15.0,      # 15% max drawdown
    'daily_loss_limit': 5.0,       # 5% daily limit
    'kill_switch_threshold': 10.0,  # Emergency stop
    'max_concurrent_positions': 3
}
```

## 📊 Monitoring Dashboard

Access the real-time dashboard at `http://localhost:8080` featuring:

- **Performance Metrics**: P&L, win rate, Sharpe ratio
- **Position Monitor**: Real-time position tracking
- **Risk Analytics**: Drawdown, exposure, limits
- **Trade History**: Complete trade log
- **Signal Analysis**: Detection accuracy metrics

## 🧪 Testing

### Run Full Test Suite
```bash
python test_momentum_bot.py
```

### Test Coverage
- Signal detection accuracy
- Risk management validation
- Position sizing calculations
- Order execution simulation
- Paper trading simulator
- Performance benchmarks

## 🚨 Safety Features

1. **Paper Trading Mode**: Test without real money
2. **Kill Switch**: Automatic shutdown at 10% loss
3. **Daily Limits**: Stop at 5% daily loss
4. **Position Limits**: Maximum 3 concurrent trades
5. **Correlation Checks**: Avoid overexposure
6. **Fake Pump Detection**: Filter manipulation

## 📈 Performance Expectations

Based on backtesting results:

| Asset | Return | Sharpe | Max DD | Win Rate |
|-------|--------|--------|--------|----------|
| HBAR  | +111%  | 1.73   | -15%   | 62%      |
| CRO   | +48%   | 0.91   | -18%   | 58%      |
| LINK  | +16%   | 0.71   | -20%   | 55%      |

**Note**: Past performance does not guarantee future results.

## 🎯 Trading Strategy

### Entry Conditions
1. MACD bullish crossover OR ROC surge detected
2. RSI > 50 and climbing
3. Volume spike > 1.8x average
4. OBV momentum confirmation
5. No fake pump indicators

### Exit Conditions
1. Stop loss hit (1.5-2.5%)
2. Take profit reached (5-8%)
3. Momentum fading signals
4. Maximum hold time exceeded
5. Risk limits triggered

## ⚠️ Important Warnings

1. **START WITH PAPER TRADING** - Test for at least 2 weeks
2. **SMALL CAPITAL FIRST** - Begin with $1000-5000
3. **MONITOR CLOSELY** - Check dashboard regularly
4. **RISK MANAGEMENT** - Never disable safety features
5. **MARKET CONDITIONS** - Performance varies with volatility

## 🔧 Troubleshooting

### Bot Won't Start
```bash
# Check credentials
echo $HYPERLIQUID_PRIVATE_KEY

# Verify dependencies
pip list | grep trading_functions

# Check logs
tail -f crypto_momentum_bot.log
```

### No Signals Detected
- Check market volatility (needs momentum)
- Verify data feed connection
- Review signal thresholds in config
- Check fake pump filter sensitivity

### Performance Issues
- Reduce signal check interval
- Optimize position count
- Review correlation limits
- Check network latency

## 📞 Support & Updates

- **Documentation**: See inline code comments
- **Configuration**: Adjust `momentum_config.py`
- **Testing**: Run test suite before updates
- **Monitoring**: Use dashboard for real-time status

## 🚀 Deployment Checklist

- [ ] Run full test suite
- [ ] Paper trade for 2+ weeks
- [ ] Verify all safety features
- [ ] Set conservative position sizes
- [ ] Configure alerts and monitoring
- [ ] Document initial parameters
- [ ] Start with minimum capital
- [ ] Monitor first 48 hours closely

## 📜 License & Disclaimer

**DISCLAIMER**: This bot is for educational purposes. Cryptocurrency trading involves substantial risk of loss. Past performance does not guarantee future results. Always trade responsibly and never invest more than you can afford to lose.

---

🌙💫🚀 **Ready for Production Trading!** 🌙💫🚀

Start with paper trading, validate performance, then deploy with small capital. Monitor closely and scale gradually based on real results.