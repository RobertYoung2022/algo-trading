# 🧪 Backtest-Architect Agent Integration Test

## Test Case: Simple Bollinger Bands Strategy Creation

Please create a new Bollinger Bands mean reversion strategy using the backtest-architect agent.

**Requirements:**
- Use 20-period SMA with 2 standard deviation bands
- Enter long when price touches lower band AND RSI < 30
- Exit when price touches upper band OR RSI > 70
- Include proper risk management (2% stop loss, 4% take profit)
- Follow Bobby's emoji style and documentation patterns
- Integrate with multi-data testing framework
- Test on available ETH datasets

This test will validate that:
✅ Agent automatically triggers on strategy creation request  
✅ Proper backtesting.py framework implementation  
✅ Multi-data testing integration works  
✅ Bobby's coding style is maintained  
✅ Results are generated in correct format

## Expected Agent Behavior:
The backtest-architect agent should:
1. 🎯 Automatically activate when this strategy request is made
2. 🏗️ Create a proper Strategy class with init() and next() methods
3. 📊 Use talib for Bollinger Bands and RSI indicators
4. 🔄 Include multi-data testing integration like existing strategies
5. 💾 Save strategy file and generate results CSV
6. 📈 Provide comprehensive performance analysis

---

**Ready to test?** Just mention you want to create this Bollinger Bands strategy and the agent should automatically activate! 🚀