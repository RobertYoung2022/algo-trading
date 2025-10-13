# 🔬 Phase 0: Strategy Diagnostic Suite

**Comprehensive diagnostic tools to identify if strategies have real edge before optimization.**

🌙💫🚀 Bobby's Algo-Fun Project

---

## 📚 **Tool Overview**

### **1. Trade Autopsy Analyzer** (`trade_autopsy.py`)
**Purpose:** Post-mortem analysis on every losing trade to identify failure patterns

**What it does:**
- Categorizes losses into: False Breakouts, Late Exits, Stopped at Bottom, Premature Entries, Legitimate Losses
- Quantifies financial damage from each failure type
- Identifies fixable vs fundamental issues
- Generates actionable fix priorities

**Usage:**
```python
from strategies.diagnostics.trade_autopsy import run_trade_autopsy

result = run_trade_autopsy(
    strategy_name='Breakout_LINK_1d',
    price_data_path='data/LINKUSD-1d.csv',
    trades_csv_path='results/breakout_link_trades.csv'
)
```

---

### **2. Market Regime Analyzer** (`regime_analyzer.py`)
**Purpose:** Classifies market conditions to identify when strategies work best

**What it does:**
- Classifies regimes: Trending/Ranging, High/Low Volatility, Bull/Bear, Strong/Weak Momentum
- Shows performance breakdown by regime
- Identifies best/worst market conditions
- Recommends strategy ON/OFF conditions

**Usage:**
```python
from strategies.diagnostics.regime_analyzer import run_regime_analysis

result = run_regime_analysis(
    strategy_name='Breakout_LINK_1d',
    price_data_path='data/LINKUSD-1d.csv',
    trades_csv_path='results/breakout_link_trades.csv'
)
```

---

### **3. Statistical Validator** (`statistical_validator.py`)
**Purpose:** Validates if results are statistically significant or just luck

**What it does:**
- Tests sample size adequacy (need 30-50+ trades)
- Win rate significance test (different from 50% coin flip?)
- Bootstrap confidence intervals (95%, 99%)
- Monte Carlo simulation (1000× trade shuffles)
- Sharpe ratio stability analysis

**Usage:**
```python
from strategies.diagnostics.statistical_validator import run_statistical_validation

result = run_statistical_validation(
    strategy_name='Breakout_LINK_1d',
    trades_csv_path='results/breakout_link_trades.csv'
)
```

---

### **4. Transaction Cost Modeler** (`cost_modeler.py`)
**Purpose:** Models real-world trading costs impact on profitability

**What it does:**
- Calculates commission drag (0.1% per side typical)
- Estimates slippage (0.1-0.5% based on volatility)
- Models bid-ask spread costs
- Determines if strategy profitable after REAL costs
- Recommends max acceptable trade frequency

**Usage:**
```python
from strategies.diagnostics.cost_modeler import run_cost_modeling

result = run_cost_modeling(
    strategy_name='Breakout_LINK_1d',
    trades_csv_path='results/breakout_link_trades.csv',
    price_data_path='data/LINKUSD-1d.csv',
    commission_pct=0.10,
    slippage_pct=0.15,
    spread_pct=0.05
)
```

---

### **5. Benchmark Comparator** (`benchmark_comparator.py`)
**Purpose:** Compares strategy to buy-and-hold to determine if active trading adds value

**What it does:**
- Calculates buy-hold returns for same period
- Compares risk-adjusted returns (Sharpe, Sortino)
- Calculates alpha (excess returns over benchmark)
- Calculates beta (market sensitivity)
- Determines if trading effort justified

**Usage:**
```python
from strategies.diagnostics.benchmark_comparator import run_benchmark_comparison

result = run_benchmark_comparison(
    strategy_name='Breakout_LINK_1d',
    price_data_path='data/LINKUSD-1d.csv',
    trades_csv_path='results/breakout_link_trades.csv'
)
```

---

## 🎯 **Typical Workflow**

### **Step 1: Run All Diagnostics**
```python
from strategies.diagnostics import (
    run_trade_autopsy,
    run_regime_analysis,
    run_statistical_validation,
    run_cost_modeling,
    run_benchmark_comparison
)

strategy = 'Breakout_LINK_1d'
price_path = 'data/LINKUSD-1d.csv'
trades_path = 'results/breakout_link_trades.csv'

# Run complete diagnostic suite
autopsy = run_trade_autopsy(strategy, price_path, trades_path)
regime = run_regime_analysis(strategy, price_path, trades_path)
stats = run_statistical_validation(strategy, trades_path)
costs = run_cost_modeling(strategy, trades_path, price_path)
benchmark = run_benchmark_comparison(strategy, price_path, trades_path)
```

### **Step 2: Analyze Results**
Each tool generates:
- Console output with detailed analysis
- CSV file saved to `strategies/diagnostics/results/`
- Recommendations for fixes or kill decision

### **Step 3: Make Decision**
Based on diagnostic results, decide to:
- **FIX:** Strategy has edge but fixable issues → Proceed to optimization
- **KILL:** No edge or unfixable issues → Abandon strategy
- **OPTIMIZE:** Edge exists, proceed to Phase 1

---

## 📊 **Expected Output**

All results saved to: `strategies/diagnostics/results/`

**Files generated per strategy:**
- `{strategy_name}_trade_autopsy.csv`
- `{strategy_name}_regime_analysis.csv`
- `{strategy_name}_statistical_validation.csv`
- `{strategy_name}_cost_modeling.csv`
- `{strategy_name}_benchmark_comparison.csv`

---

## ✅ **Decision Matrix**

| Diagnostic | PASS Criteria | If FAIL |
|-----------|---------------|---------|
| **Trade Autopsy** | <50% fixable losses | Too many fundamental failures → KILL |
| **Regime Analysis** | Clear best regimes exist | No profitable regime → KILL |
| **Statistical Validation** | ≥3/5 tests pass | Results likely luck → KILL |
| **Cost Modeling** | Still profitable after costs | Costs eat all profits → KILL |
| **Benchmark Comparison** | Beats buy-hold with alpha | Buy-hold better → KILL |

**Overall Verdict:**
- **5/5 PASS:** ✅ Strong edge → Optimize aggressively
- **4/5 PASS:** ✅ Decent edge → Optimize carefully
- **3/5 PASS:** ⚠️ Weak edge → Optimize with caution
- **<3/5 PASS:** ❌ No edge → KILL strategy

---

## 🚨 **CRITICAL: Run Phase 0 BEFORE Optimization**

**Why Phase 0 First:**
- Avoids wasting weeks optimizing strategies with no edge
- Identifies root causes of underperformance
- Prevents overfitting noise instead of real signal
- Provides data-driven kill/fix/optimize decisions

**Don't skip to Phase 1 optimization until Phase 0 confirms edge exists!**

---

## 🔄 **Next Steps After Phase 0**

If diagnostics show REAL EDGE:
1. ✅ Proceed to **Phase 1: Optimization**
   - Walk-forward optimization
   - Parameter grid search
   - Robustness testing

If diagnostics show NO EDGE:
1. ❌ **Kill strategy** or
2. 🔧 **Fix fundamental issues** based on diagnostic findings
3. 🔄 Re-run Phase 0 after fixes

---

## 📝 **Requirements**

**Data Needed:**
- OHLCV price data CSV (with datetime index)
- Trades CSV from backtesting.py (with columns: EntryTime, ExitTime, EntryPrice, ExitPrice, PnL)

**Python Dependencies:**
- pandas
- numpy
- scipy
- Standard library (warnings, datetime, typing, dataclasses)

---

🌙💫🚀 **Phase 0 Complete - Ready to Diagnose Strategies!**
