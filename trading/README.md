# 🚀 ETH Multi-Strategy Rotation Trading System

AI-enhanced automated trading system that rotates between RSI, SMA, and Breakout strategies for ETH trading on HyperLiquid.

## 📊 System Overview

This trading system:
- **Analyzes** ETH with 3 different strategies (RSI Mean Reversion, SMA Crossover, Breakout Momentum)
- **Selects** the best strategy based on highest confidence score
- **Validates** with AI-powered risk analysis (3-tier model optimization)
- **Executes** on HyperLiquid DEX with 3x leverage
- **Tracks** performance and validates against 1-2 month goals

### Key Features
- ✅ **Multi-Strategy Rotation**: Automatically selects best strategy per market conditions
- ✅ **AI Enhancement**: 3-tier AI validation (DeepSeek/Haiku/Sonnet) for cost optimization
- ✅ **Aggressive Settings**: 90% position sizing, 3x leverage, 3% SL / 9% TP
- ✅ **24/7 Operation**: Runs on DigitalOcean with auto-restart
- ✅ **Performance Tracking**: Comprehensive metrics, strategy comparison, validation reports

---

## 🎯 Goals & Validation Criteria (1-2 Months)

| Goal | Target | Status |
|------|--------|--------|
| Total Return | **>+10%** | ⏳ Testing |
| Win Rate | **>55%** | ⏳ Testing |
| Profitable Weeks | **≥3 out of 4** | ⏳ Testing |
| Max Single Loss | **<5%** | ⏳ Testing |
| AI Advantage | **>5% improvement** | ⏳ Testing |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────┐
│          STRATEGY SELECTION                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │   RSI    │ │   SMA    │ │ Breakout │   │
│  │  (1h)    │ │  (4h)    │ │   (1d)   │   │
│  └──────────┘ └──────────┘ └──────────┘   │
│       │            │            │           │
│       └────────────┴────────────┘           │
│                    ▼                        │
│          Select Highest Confidence          │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│          AI ENHANCEMENT                     │
│  ┌──────────────────────────────────────┐  │
│  │  Market Intelligence                 │  │
│  │  • Funding rates                     │  │
│  │  • Liquidation cascades              │  │
│  │  • Whale movements                   │  │
│  └──────────────────────────────────────┘  │
│                    ▼                        │
│  ┌──────────────────────────────────────┐  │
│  │  Risk Validation (AI)                │  │
│  │  • Position sizing check             │  │
│  │  • R:R ratio validation              │  │
│  │  • Auto-escalate large positions     │  │
│  └──────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│          EXECUTION (HyperLiquid)            │
│  • Entry: Limit order                       │
│  • TP/SL: Automatic                         │
│  • Leverage: 3x                             │
│  • Position: 90% of capital                 │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│          PERFORMANCE TRACKING               │
│  • CSV trade log                            │
│  • Weekly reports                           │
│  • Strategy comparison                      │
│  • Validation dashboards                    │
└─────────────────────────────────────────────┘
```

---

## 📁 File Structure

```
trading/
├── config_eth_aggressive.py     # Configuration (capital, risk, strategies)
├── strategy_selector.py          # Multi-strategy analysis & selection
├── hyperliquid_executor.py       # HyperLiquid execution wrapper
├── main_trading_loop.py          # Main orchestrator (runs 24/7)
├── strategy_validator.py         # Performance tracking & reports
├── performance_dashboard.py      # Real-time CLI dashboard
├── logs/
│   └── trades_log.csv            # All trades logged here
└── reports/
    └── weekly_report_*.txt       # Generated reports

deploy/
├── setup_digitalocean.sh         # Automated droplet setup
└── algo-trading.service          # Systemd service file
```

---

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8+
- API Keys:
  - Anthropic (Claude Sonnet 4.5 & Haiku 4.5)
  - DeepSeek
  - HyperLiquid (ETH private key)
  - Moon Dev API (optional, for market intelligence)

### 2. Configuration

Edit `trading/config_eth_aggressive.py`:

```python
# Capital & Risk
STARTING_CAPITAL = 1000.0  # Your starting capital
POSITION_SIZE_PCT = 0.90   # 90% per trade
LEVERAGE = 3               # 3x leverage
STOP_LOSS_PCT = 0.03       # 3% stop loss
TAKE_PROFIT_PCT = 0.09     # 9% take profit

# Paper trading (set to False for live trading)
PAPER_TRADING_MODE = True  # ⚠️ Start with paper mode!
```

Configure `.env.ai` in project root:

```bash
# AI Model API Keys
ANTHROPIC_API_KEY=sk-ant-YOUR_KEY_HERE
DEEPSEEK_API_KEY=sk-YOUR_KEY_HERE

# HyperLiquid
PH_SECRET_KEY=0xYOUR_ETH_PRIVATE_KEY_HERE

# Mode
PAPER_TRADING=true
```

### 3. Run Locally (Paper Trading)

```bash
# Test configuration
python3 trading/config_eth_aggressive.py

# Test strategy selector
python3 trading/strategy_selector.py

# Run trading system (paper mode)
python3 trading/main_trading_loop.py
```

### 4. Monitor Performance

```bash
# Real-time dashboard (auto-refresh every 30s)
python3 trading/performance_dashboard.py

# One-time view
python3 trading/performance_dashboard.py --once

# Generate report
python3 trading/strategy_validator.py
```

---

## ☁️ DigitalOcean Deployment (24/7 Trading)

### 1. Create Droplet

- **OS**: Ubuntu 22.04 LTS
- **Size**: 2GB RAM / 1 vCPU ($12/month)
- **Region**: Choose closest to you

### 2. Run Setup Script

```bash
# SSH into droplet
ssh root@YOUR_DROPLET_IP

# Download and run setup script
curl -O https://raw.githubusercontent.com/YOUR_USERNAME/algo-fun/feature/eth-strategy-rotation/deploy/setup_digitalocean.sh
chmod +x setup_digitalocean.sh
./setup_digitalocean.sh
```

The script will:
- Install Python & dependencies
- Clone your repository
- Setup environment variables
- Install systemd service
- Start trading system

### 3. Manage Service

```bash
# View live logs
journalctl -u algo-trading -f

# Restart service
systemctl restart algo-trading

# Stop service
systemctl stop algo-trading

# Check status
systemctl status algo-trading
```

### 4. Monitor from Droplet

```bash
cd /root/algo-fun
python3 trading/performance_dashboard.py --once
```

---

## 📊 Strategies

### 1. RSI Mean Reversion (1h timeframe)

**Logic**:
- **Entry**: RSI < 30 (oversold)
- **Exit**: RSI returns to 40 (neutral zone)
- **AI Enhancement**: Confirms with funding rates & liquidations
- **Tier**: Simple (DeepSeek) for cost efficiency

**Best For**: Oversold bounces, high liquidation events

### 2. SMA Crossover (4h timeframe)

**Logic**:
- **Entry**: Fast SMA (10) crosses above Slow SMA (30)
- **Exit**: Fast SMA crosses below Slow SMA
- **AI Enhancement**: Validates trend strength
- **Tier**: Medium (Haiku 4.5) for balance

**Best For**: Strong trending markets

### 3. Breakout Momentum (1d timeframe)

**Logic**:
- **Entry**: Price breaks daily resistance + volume > 1.5x average
- **Exit**: TP/SL hit
- **AI Enhancement**: Confirms with whale activity
- **Tier**: Medium (Haiku 4.5)

**Best For**: Volatility expansion, strong breakouts

---

## 🛡️ Safety Features

### Circuit Breakers

```python
MAX_CONSECUTIVE_LOSSES = 3  # Stop after 3 losses in a row
MAX_DAILY_LOSSES = 2        # Max 2 losses per day
COOLDOWN_AFTER_LOSS = 60    # Wait 1 hour after loss
MINIMUM_BALANCE = $100      # Stop if balance < $100
```

### Emergency Stop

Create file `trading/EMERGENCY_STOP` to halt trading immediately:

```bash
touch trading/EMERGENCY_STOP  # Stops trading
rm trading/EMERGENCY_STOP     # Resumes trading
```

### Paper Trading Mode

Always test in paper mode first:

```python
# In config_eth_aggressive.py
PAPER_TRADING_MODE = True  # Safe mode
```

---

## 📈 Performance Tracking

### Trade Log (CSV)

All trades logged to `trading/logs/trades_log.csv`:

```
timestamp,symbol,strategy,signal,entry_price,exit_price,pnl_usd,pnl_pct,win,confidence,...
2025-01-22 10:30,ETH,RSI_Mean_Reversion,BUY,3000,3270,81,9.0,True,85,...
2025-01-22 14:15,ETH,Breakout_Momentum,BUY,3200,3488,86.4,9.0,True,92,...
```

### Weekly Reports

Auto-generated in `trading/reports/`:

```
📊 WEEKLY PERFORMANCE REPORT
=====================================
Total Return: +12.7%
Win Rate: 67% (10W / 5L)

Strategy Breakdown:
  RSI Mean Reversion:  6 trades, 75% win rate, +$150
  SMA Crossover:       3 trades, 67% win rate, +$45
  Breakout Momentum:   6 trades, 83% win rate, +$180

✅ VALIDATED - All goals met!
```

### Dashboard

Real-time monitoring:

```bash
python3 trading/performance_dashboard.py
```

```
🚀 ETH MULTI-STRATEGY ROTATION - PERFORMANCE DASHBOARD
======================================================================

💰 ACCOUNT STATUS
Balance:          $1,127.50
Total P&L:        +$127.50 (+12.75%)

📈 CURRENT POSITION - ETH
Status:           IN POSITION
Entry Price:      $3,200.00
Current Price:    $3,250.00
P&L:              +$45.00 (+1.56%)

📊 PERFORMANCE METRICS
Total Trades:     15
Win Rate:         67% (10W / 5L)
Sharpe Ratio:     2.3
Max Drawdown:     -3.2%

🎯 STRATEGY COMPARISON
RSI Mean Reversion:   75% win rate, +$150
Breakout Momentum:    83% win rate, +$180
SMA Crossover:        67% win rate, +$45

✅ VALIDATION STATUS
✅ Return: 12.8% >= 10%
✅ Win Rate: 67% >= 55%
✅ Profitable Weeks: 3/4 >= 3

🎉 SYSTEM VALIDATED - All goals met!
```

---

## 💰 Cost Breakdown

### AI Costs (3-Tier Optimization)

| Tier | Model | Usage | Cost/Month |
|------|-------|-------|------------|
| Tier 3 | DeepSeek | 60% | $0.50 |
| Tier 2 | Haiku 4.5 | 30% | $0.70 |
| Tier 1 | Sonnet 4.5 | 10% | $0.50 |
| **Total** | | | **$1.70** |

### Infrastructure

| Service | Cost/Month |
|---------|------------|
| DigitalOcean VPS (2GB) | $12.00 |
| Moon Dev API (free tier) | $0.00 |
| **Total** | **$12.00** |

**Total Monthly Cost: ~$14/month** for 24/7 AI-enhanced trading!

---

## 🧪 Testing Checklist

Before going live:

- [ ] Test in paper mode for 1-2 weeks
- [ ] Verify all 3 strategies generate signals
- [ ] Check AI risk validation works
- [ ] Confirm TP/SL orders are placed correctly
- [ ] Monitor for false signals
- [ ] Validate performance tracking
- [ ] Test emergency stop functionality
- [ ] Review circuit breaker triggers

---

## 📚 Additional Resources

- **AI Integration Guide**: `../AI_INTEGRATION_GUIDE.md`
- **3-Tier AI Summary**: `../AI_3TIER_SUMMARY.md`
- **Strategy Backtests**: `../strategies/core_strategies/`
- **Moon Dev Agents**: `../moon-dev-agents/README.md`

---

## ⚠️ Disclaimer

This is an experimental trading system. Cryptocurrency trading carries significant risk. Only trade with capital you can afford to lose.

- ✅ Start with paper trading
- ✅ Test thoroughly before going live
- ✅ Use small position sizes initially
- ✅ Monitor performance closely
- ✅ Have emergency stop procedures ready

**Past performance does not guarantee future results.**

---

## 🤝 Support

Issues or questions? Create an issue in the repository.

**Happy Trading! 🚀📈**
