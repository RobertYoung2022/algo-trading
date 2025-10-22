# ⚡ Quick Start: AI Integration

## Get Started in 5 Minutes

### Step 1: Configure API Keys (2 min)

```bash
# Copy example config
cp .env.ai.example .env.ai

# Edit and add your API keys
nano .env.ai
```

**Minimum Required:**
- `ANTHROPIC_API_KEY`: Get from https://console.anthropic.com/
- `DEEPSEEK_API_KEY`: Get from https://platform.deepseek.com/

### Step 2: Test the Integration (2 min)

```bash
# Run comprehensive tests
python test_ai_integration.py
```

**Expected Output:**
```
✅ PASS: Configuration
✅ PASS: Model Factory
✅ PASS: Market Intelligence
✅ PASS: Risk Management
✅ PASS: Strategy Generation
✅ PASS: Parallel Execution

🎉 ALL TESTS PASSED!
```

### Step 3: Try It Out (1 min)

```bash
# Run parallel comparison demo
python ai_parallel_executor.py
```

---

## What You Need to Provide

### API Keys

Add these to `.env.ai`:

```bash
# REQUIRED
ANTHROPIC_API_KEY=sk-ant-YOUR_KEY_HERE
DEEPSEEK_API_KEY=sk-YOUR_KEY_HERE

# OPTIONAL (for live market data)
MOONDEV_API_KEY=YOUR_KEY_HERE
BIRDEYE_API_KEY=YOUR_KEY_HERE
COINGECKO_API_KEY=YOUR_KEY_HERE
```

### That's It!

The system uses simulated data by default, so you can test everything without live market data APIs.

---

## Integration with Your Existing System

### Option 1: Quick Integration

Add AI intelligence to your existing strategy:

```python
from ai_agents.market_intelligence import FundingAgent, WhaleAgent
from ai_agents.risk_management import RiskAgent

# Your existing strategy
traditional_signal = run_your_strategy()  # Returns "BUY", "SELL", "HOLD"

# Add AI intelligence
funding_agent = FundingAgent()
funding_result = funding_agent.execute(symbol="BTC")

risk_agent = RiskAgent()
risk_result = risk_agent.execute(
    position_data=your_position_data,
    account_balance=your_balance
)

# Combined decision
if funding_result.data['signal'] == traditional_signal and risk_result.data['approved']:
    execute_trade()
```

### Option 2: Parallel Testing

Compare your traditional system vs AI-enhanced:

```python
from ai_parallel_executor import ParallelExecutor

executor = ParallelExecutor()

# Traditional
trad = executor.execute_traditional("BTC", "BUY", {"confidence": 0.7})

# AI-Enhanced
ai = executor.execute_ai_enhanced("BTC", "BUY", {"confidence": 0.7})

# Compare
executor.compare_results(trad, ai)
executor.save_results()
```

---

## What Each Component Does

### 🔍 Market Intelligence
- **FundingAgent**: Detects overcrowded positions via funding rates
- **LiquidationAgent**: Identifies capitulation events
- **WhaleAgent**: Tracks smart money movements

### ⚠️ Risk Management
- **RiskAgent**: AI validates every trade before execution
- Circuit breakers for daily loss limits
- Position sizing validation

### 🧠 Strategy Generation
- **RBIAgent**: Generate new strategies from ideas, videos, research
- AI validation and quality scoring

### 🔄 Parallel Testing
- Run old and new systems side-by-side
- Compare performance without risk
- Track AI improvements

---

## Cost Estimate

**With DeepSeek + Claude:**
- Simple operations: ~$0.14 per 1M tokens (DeepSeek)
- Complex analysis: ~$3 per 1M tokens (Claude)
- Average cost per day: **$0.50 - $2.00** (normal trading activity)

**100% Free Option:**
- Use local Ollama models (already installed)
- No API costs
- Set `ENABLE_FUNDING_AGENT=false` etc. in `.env.ai`

---

## Next Steps

1. ✅ Run `python test_ai_integration.py`
2. ✅ Read `AI_INTEGRATION_GUIDE.md` for detailed docs
3. ✅ Start with paper trading
4. ✅ Compare results for 1-2 weeks
5. ✅ Scale up gradually

**Questions?** Check `AI_INTEGRATION_GUIDE.md` or the troubleshooting section.

---

**You're Ready! 🚀**
