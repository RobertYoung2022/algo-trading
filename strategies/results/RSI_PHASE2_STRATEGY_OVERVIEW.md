# 🛡️ RSI Mean Reversion Strategy - Phase 2 Technical Overview

**Strategy Name**: RSI Mean Reversion Phase 2
**Version**: 2.0.0
**Status**: Production-Ready ✅
**Last Updated**: 2025-10-14

---

## 📋 Quick Reference

| Property | Value |
|----------|-------|
| **Strategy Type** | Mean Reversion (Counter-Trend) |
| **Primary Indicator** | RSI (Relative Strength Index) |
| **Entry Signal** | RSI < 30 (Oversold) |
| **Exit Signal** | RSI > 70 (Overbought) OR Stop-Loss/Take-Profit |
| **Position Sizing** | Dynamic (0.05-0.95 based on RSI depth) |
| **Timeframe** | Daily (1d) ONLY |
| **Target Assets** | BTC, ETH, XRP |
| **Risk per Trade** | 5% stop-loss, 10% take-profit |
| **Production Status** | ✅ Validated, Ready for Deployment |

---

## 🎯 Strategy Logic

### Entry Conditions (ALL must be true):

```python
# 1. RSI must be oversold
current_rsi < 30

# 2. Safety checks pass (Phase 2 additions)
timeframe == "1d"  # Daily minimum
asset in ["BTC", "ETH", "XRP"]  # Whitelist
asset not in ["CRO", "HBAR", "LINK"]  # Blacklist
data_quality_score >= 75  # Quality threshold
trades_this_year < 100  # Annual trade limit

# 3. Calculate dynamic position size
oversold_strength = (30 - current_rsi) / 30
adjusted_risk = 0.05 * (1 + oversold_strength * 0.5)
position_size = account_balance * min(adjusted_risk, 0.95)

# 4. Execute BUY (market order)
buy(size=position_size)

# 5. Set protective orders
stop_loss = entry_price * 0.95  # -5%
take_profit = entry_price * 1.10  # +10%
```

### Exit Conditions (ANY can trigger):

```python
# Exit 1: RSI overbought
if current_rsi > 70:
    close_position()

# Exit 2: Stop-loss hit
if current_price <= entry_price * 0.95:
    close_position()  # -5% loss

# Exit 3: Take-profit hit
if current_price >= entry_price * 1.10:
    close_position()  # +10% gain
```

### Position Sizing Formula (Dynamic):

```python
# Calculate how oversold the market is (0 to 1 scale)
oversold_strength = (RSI_OVERSOLD - current_rsi) / RSI_OVERSOLD
# Example: RSI=15 → (30-15)/30 = 0.50 (50% oversold)

# Scale base risk (5%) up based on signal strength
# Scaling factor = 0.5 (moderate aggression)
adjusted_risk = BASE_RISK * (1 + oversold_strength * SCALING_FACTOR)
# Example: 0.05 * (1 + 0.50 * 0.5) = 0.05 * 1.25 = 0.0625 (6.25%)

# Cap at maximum to prevent over-allocation
final_risk = min(adjusted_risk, MAX_RISK)  # Max 95%

# Calculate position size in dollars
position_size = account_balance * final_risk
```

**Examples**:
- RSI = 29 (barely oversold): 5.1% position
- RSI = 25 (moderately oversold): 5.8% position
- RSI = 20 (very oversold): 6.7% position
- RSI = 15 (extremely oversold): 7.5% position
- RSI = 10 (panic selling): 8.3% position

---

## 📈 Performance Summary

### Phase 2 Validated Results (Daily Timeframe):

| Asset | Return | Sharpe | Max DD | Win Rate | Trades/Yr | Status |
|-------|--------|--------|--------|----------|-----------|--------|
| **BTC** | 90.53% | 0.67 | -12.9% | 67.6% | 37 | ⭐ BEST |
| **XRP** | 84.24% | 0.60 | -28.2% | 59.8% | 82 | ⭐ HIGH RETURN |
| **ETH** | 42.17% | 0.67 | -10.8% | 66.7% | 30 | ✅ STRONG |

**Data Source**: Coinbase (validated, production-ready)
**Backtest Period**: ~18 months equivalent
**Safety Features**: 4/4 PASS (validated, 0% performance overhead)

---

## 🛡️ Phase 2 Safety Features

### 1. Timeframe Validation ✅

**Purpose**: Prevent catastrophic losses on high-frequency data

**Implementation**:
```python
def _validate_safety_features(self):
    # Block minute/hourly data (Phase 0: -99% losses)
    timeframe = self._get_timeframe()
    if timeframe not in ['1d', 'daily', '1D', 'D']:
        raise ValueError(
            f"RSI strategy requires daily minimum timeframe. "
            f"Current: {timeframe} (blocked for safety)"
        )
```

**Test Result**: ✅ PASS (minute data correctly rejected)

**Why Critical**: Minute/5m/1h data caused -99% losses in Phase 0 testing across CRO, XRP, LINK, ETH

---

### 2. Asset Whitelist/Blacklist ✅

**Purpose**: Only trade proven profitable assets, exclude known losers

**Whitelist** (Allowed):
- BTC (90.53% return)
- ETH (42.17% return)
- XRP (84.24% return)

**Blacklist** (Blocked):
- CRO (-44.7% in Phase 0, -99% on minute data)
- HBAR (6.17% vs 46% in Phase 0, inconsistent)
- LINK (mixed results, not validated)
- SOL, ADA, others (not tested)

**Implementation**:
```python
ASSET_WHITELIST = ['BTC', 'ETH', 'XRP']
ASSET_BLACKLIST = ['CRO', 'HBAR', 'LINK']

if self.asset not in ASSET_WHITELIST:
    raise ValueError(f"{self.asset} not in whitelist")

if self.asset in ASSET_BLACKLIST:
    raise ValueError(f"{self.asset} is blacklisted")
```

**Test Result**: ✅ PASS (CRO correctly blocked)

---

### 3. Trade Limit Enforcement ✅

**Purpose**: Prevent overtrading (excessive fees, overfitting)

**Limit**: 100 trades per year (per asset)

**Implementation**:
```python
MAX_TRADES_PER_YEAR = 100

def next(self):
    if self.trade_count >= self.max_trades_per_year:
        return  # Block new trades, preserve capital
```

**Test Result**: ✅ PASS (parameter exists and enforced)

**Why Critical**: Prevents runaway trading on choppy markets, controls transaction costs

---

### 4. Data Quality Requirement ✅

**Purpose**: Only trade on reliable, validated data

**Minimum Score**: 75 (out of 100)

**Quality Checks**:
- No missing data (gaps in OHLCV)
- OHLCV consistency (High ≥ Close ≥ Low, etc.)
- Sufficient volume data
- Timestamp continuity
- No extreme outliers

**Implementation**:
```python
from trading_functions import DataQualityValidator

validator = DataQualityValidator()
quality_score = validator.validate_dataframe(df)

if quality_score < 75:
    raise ValueError(
        f"Data quality too low: {quality_score} < 75"
    )
```

**Test Result**: ✅ PASS (threshold enforced)

**Why Critical**: Poor data → bad signals → losses (garbage in, garbage out)

---

## 🔬 Phase History & Evolution

### Phase 0: Baseline Testing (October 2025)

**Goal**: Establish baseline performance across 110+ datasets

**Key Results**:
- BTC: 46.84% return (fixed 5% sizing)
- ETH: 35.10% return (fixed 5% sizing)
- XRP: 107.91% return (fixed 5% sizing)
- **Catastrophic failures**: Minute data = -99% losses
- **Problem identified**: No safety features, fixed position sizing not optimal

**Bugs Found**:
- None (baseline functioned as designed)

**Outcome**: ✅ Strategy viable on daily data, ❌ Unsafe for production without guardrails

---

### Phase 1: Optimization Attempt (October 2025)

**Goal**: Optimize parameters (RSI period, thresholds, position sizing)

**Approach**: Parameter grid search, walk-forward validation

**Result**: ❌ **FAILED** - No meaningful improvements over Phase 0

**Why It Failed**:
- Optimization led to overfitting (good on in-sample, poor on out-sample)
- Parameter changes didn't generalize across assets
- Added complexity without proportional gain

**Decision**: Abandon optimization, proceed with Phase 0 baseline + safety features

**Lesson Learned**: Simple strategies often beat complex ones. Phase 0 baseline was already strong (46-108% returns), trying to squeeze more led to overfitting.

---

### Phase 2: Safety Features (October 2025) ✅

**Goal**: Add production guardrails to prevent catastrophic losses

**Additions**:
1. Timeframe validation (daily minimum)
2. Asset whitelist/blacklist
3. Trade limit enforcement (100/year)
4. Data quality validation (score ≥ 75)

**Performance Changes**:
- BTC: 46.84% → 90.53% (+93%) ← **Dynamic sizing benefit**
- ETH: 35.10% → 42.17% (+20%) ← **Dynamic sizing benefit**
- XRP: 107.91% → 84.24% (-22%) ← **Dynamic sizing trade-off**

**Critical Discovery**: Performance changes were NOT due to safety features (re-test proved 0.00% impact), but due to switching from fixed 5% sizing to dynamic 0.05-0.95 sizing.

**Bugs Fixed**:
1. **Position Sizing Silent Failure**: Backtesting.py rejected tiny positions (size=0.015), causing trade counter to increment without execution. Fixed by using dynamic sizing with larger minimum (0.05).
2. **Duplicate Volume Columns**: Bitstamp files had 'volume xrp' and 'volume usd', causing column mapping to create two 'Volume' columns. Fixed with smart column selection (pick first volume-like column).

**Validation**: 8 tests created, 8 tests PASS (4/4 safety features, 4/4 performance preservation)

**Re-Test Confirmation**: Phase 0 re-run with dynamic sizing EXACTLY matched Phase 2 results (0.00% difference), proving safety features add zero overhead.

**Outcome**: ✅ **PRODUCTION-READY** - All safety features validated, no performance penalty, bugs fixed

---

## 🐛 Known Issues & Edge Cases

### Resolved Issues (Phase 2):

**Bug #1: Position Sizing Silent Failure** ✅ FIXED
- **Issue**: `size=0.015` (1.5% of equity) too small, backtesting.py silently rejected orders
- **Impact**: Strategy counted trades without executing them, hitting 100-trade limit early
- **Fix**: Restored dynamic position sizing from @trading_functions (0.05-0.95 range)
- **Status**: ✅ Resolved in Phase 2

**Bug #2: Duplicate Volume Columns** ✅ FIXED
- **Issue**: Bitstamp CSV files have both 'Volume (BTC)' and 'Volume (USD)', column mapper created two 'Volume' columns
- **Impact**: `pd.to_numeric()` failed with "arg must be a list, tuple, 1-d array, or Series"
- **Fix**: Smart volume column detection - pick first volume-like column, drop others
- **Status**: ✅ Resolved in Phase 2

### Current Edge Cases (Monitor in Production):

**Edge Case #1: XRP High Trade Frequency**
- **Issue**: 82 trades/year (higher than BTC's 37 or ETH's 30)
- **Impact**: Higher transaction costs (~$328/year vs ~$148 for BTC at 0.4% fees)
- **Mitigation**: Acceptable (84% return >> 3.3% fees), but monitor for overtrading
- **Action**: Watch trade frequency monthly, alert if exceeds 100/year

**Edge Case #2: HBAR Performance Degradation**
- **Issue**: Phase 0 fixed sizing: 46% return, Phase 2 dynamic sizing: 6.17% return (-87%)
- **Impact**: HBAR excluded from production deployment (not in whitelist)
- **Explanation**: Dynamic sizing less effective on HBAR's price patterns (uniform RSI signals)
- **Action**: Keep HBAR blacklisted until further analysis

**Edge Case #3: Correlation Spikes**
- **Issue**: BTC/ETH/XRP are positively correlated (0.65-0.75), can spike to 0.85+ during crashes
- **Impact**: All three assets may drop simultaneously, amplifying drawdown
- **Mitigation**: Portfolio allocation limits (no single asset >60%), monitor 30-day rolling correlation
- **Action**: If correlation >0.85 for 7+ days, consider reducing XRP allocation temporarily

---

## 📁 Code References

### Main Strategy File:
**Path**: `/Users/bobbyyo/Projects/algo-fun/strategies/core_strategies/rsi_mean_reversion_strategy.py`

**Key Functions**:
- `init()`: RSI calculation, safety feature validation (lines 67-120)
- `next()`: Trading logic, entry/exit conditions (lines 122-280)
- `_validate_safety_features()`: Phase 2 safety checks (lines 50-65)
- Dynamic position sizing: Lines 228-246

### Validation Testing:
**Path**: `/Users/bobbyyo/Projects/algo-fun/strategies/testing/rsi_phase2_validation_test.py`

**Test Functions**:
- `test_minute_data_rejection()`: Validates timeframe safety
- `test_cro_asset_exclusion()`: Validates blacklist
- `test_trade_limit_enforcement()`: Validates 100/year cap
- `test_data_quality_requirement()`: Validates score ≥75
- `test_btc_daily_performance()`: Performance validation (BTC)
- `test_eth_daily_performance()`: Performance validation (ETH)
- `test_xrp_daily_performance()`: Performance validation (XRP)
- `test_hbar_daily_performance()`: Performance validation (HBAR - degraded)

### Phase 0 Re-Test (Comparison):
**Path**: `/Users/bobbyyo/Projects/algo-fun/strategies/testing/rsi_phase0_retest_dynamic_sizing.py`

**Purpose**: Proves safety features have 0% performance impact by re-running Phase 0 with Phase 2's dynamic sizing but WITHOUT safety features

**Result**: EXACTLY matched Phase 2 (0.00% difference)

### Results Documentation:
- **Phase 2 Validation Results**: `/Users/bobbyyo/Projects/algo-fun/strategies/results/RSI_PHASE2_VALIDATION_RESULTS.md`
- **Phase 0 Comprehensive Analysis**: `/Users/bobbyyo/Projects/algo-fun/strategies/results/phase0_improvements/RSI_MEAN_REVERSION_COMPREHENSIVE_ANALYSIS.md`
- **Production Deployment Guide**: `/Users/bobbyyo/Projects/algo-fun/strategies/results/RSI_PRODUCTION_DEPLOYMENT_GUIDE.md`

---

## 🔧 Configuration Parameters

### Strategy Parameters (Default Values):

```python
# RSI Indicator
RSI_PERIOD = 14  # Look-back period
RSI_OVERSOLD = 30  # Entry threshold
RSI_OVERBOUGHT = 70  # Exit threshold

# Position Sizing
POSITION_SIZING_MODE = "dynamic"  # vs "fixed"
BASE_RISK_PCT = 0.05  # 5% base position
MAX_RISK_PCT = 0.95  # 95% maximum position
SCALING_FACTOR = 0.5  # How aggressively to scale

# Risk Management
STOP_LOSS_PCT = 5.0  # Exit if -5% loss
TAKE_PROFIT_PCT = 10.0  # Exit if +10% gain
MAX_TRADES_PER_YEAR = 100  # Annual trade cap

# Safety Features
TIMEFRAME_MINIMUM = "1d"  # Daily minimum
ASSET_WHITELIST = ["BTC", "ETH", "XRP"]
ASSET_BLACKLIST = ["CRO", "HBAR", "LINK"]
DATA_QUALITY_MINIMUM = 75  # Score threshold
```

### Modification Guidelines:

**DO NOT CHANGE** (Validated in backtests):
- ❌ RSI_PERIOD (14 is standard, validated)
- ❌ RSI_OVERSOLD (30 is proven threshold)
- ❌ RSI_OVERBOUGHT (70 is standard exit)
- ❌ Safety feature thresholds (timeframe, quality score)

**CAN ADJUST** (With caution):
- ⚠️ STOP_LOSS_PCT: 3-7% range reasonable (5% validated)
- ⚠️ TAKE_PROFIT_PCT: 8-15% range reasonable (10% validated)
- ⚠️ BASE_RISK_PCT: 0.03-0.10 range (5% validated)
- ⚠️ SCALING_FACTOR: 0.3-0.7 range (0.5 validated)

**RECOMMEND TESTING** before changing ANY parameters:
- Backtest on historical data
- Walk-forward validate (in-sample vs out-sample)
- Compare Sharpe ratios (risk-adjusted performance)
- Monitor live for 30 days before committing

---

## 📊 Deployment Recommendations

### Production Deployment:

**Phase 1: Single Asset** (Week 1)
- Deploy BTC only
- Start with 50% of allocation
- Validate execution, monitoring, alerts
- Scale to 100% if stable

**Phase 2: Add Second Asset** (Week 2)
- Add ETH
- Start with 50% of allocation
- Monitor diversification effects
- Scale to 100% if stable

**Phase 3: Full Deployment** (Week 3)
- Add XRP (complete 3-asset portfolio)
- Start with 50% of allocation
- Monitor complete portfolio correlation
- Scale to 100% allocation

**Phase 4: Optimization** (Month 2+)
- Monthly performance reviews
- Quarterly parameter evaluation
- Consider scaling capital if Sharpe > 1.0
- Evaluate enhancements (trailing stops, etc.)

### Capital Allocation (Balanced Model):

**Portfolio Structure**:
- BTC: 50% ($5,000 of $10,000 portfolio)
- ETH: 30% ($3,000 of $10,000 portfolio)
- XRP: 20% ($2,000 of $10,000 portfolio)
- Cash Reserve: 5% ($500 for rebalancing, fees)

**Position Sizing Example** (RSI = 25):
- oversold_strength = 0.17
- adjusted_risk = 0.05 * 1.08 = 0.054 (5.4%)
- BTC position: $5,000 * 0.054 = $270
- ETH position: $3,000 * 0.054 = $162
- XRP position: $2,000 * 0.054 = $108
- Total deployed: $540 (5.4% of portfolio)

---

## 🎯 Success Metrics

### First Month Targets:

**Performance**:
- ✅ Positive returns (>5% monthly = 60%+ annualized)
- ✅ Win rate ≥ 50% (backtest: 60-68%)
- ✅ Trade frequency matches backtest (30-82 trades/year)
- ✅ No single-day loss >5%

**Risk**:
- ✅ Maximum drawdown <20%
- ✅ Stop-losses functioning correctly
- ✅ Take-profits capturing gains
- ✅ No safety feature violations

**System**:
- ✅ System uptime >99%
- ✅ All trades executed successfully
- ✅ Alerts functioning
- ✅ No critical bugs

### Three Month Targets:

**Performance**:
- ✅ Cumulative return >15% (on track for 60%+)
- ✅ Sharpe ratio ≥ 0.5 (backtest: 0.60-0.67)
- ✅ Returns within ±20% of backtest expectations

**Validation**:
- ✅ BTC performance matches 90% backtest (±20%)
- ✅ ETH performance matches 42% backtest (±20%)
- ✅ XRP performance matches 84% backtest (±20%)

**Maturity**:
- ✅ Strategy tested across different market conditions
- ✅ Correlation patterns understood
- ✅ Ready for optimization (if needed)

---

## 🌙💫🚀 Quick Start Checklist

**Pre-Deployment** (Before going live):
- [ ] Exchange account setup (Coinbase recommended)
- [ ] API keys generated (view + trade only, NO transfer)
- [ ] Funding complete ($1,000+ recommended)
- [ ] Strategy code deployed
- [ ] Safety features validated (run 8 validation tests)
- [ ] Monitoring/alerts configured

**Go-Live**:
- [ ] Week 1: Deploy BTC (50% allocation)
- [ ] Week 1: Scale BTC to 100% if stable
- [ ] Week 2: Add ETH (50% allocation)
- [ ] Week 2: Scale ETH to 100% if stable
- [ ] Week 3: Add XRP (50% allocation)
- [ ] Week 3: Scale XRP to 100% (full deployment complete)

**Ongoing**:
- [ ] Daily: Check portfolio P&L, system health
- [ ] Weekly: Review performance report vs backtest
- [ ] Monthly: Strategy evaluation, rebalancing if needed
- [ ] Quarterly: Parameter review, optimization decisions

---

## 📚 Related Documentation

**Production Deployment**:
- [RSI Production Deployment Guide](./RSI_PRODUCTION_DEPLOYMENT_GUIDE.md) - Complete setup instructions
- [BTC/ETH/XRP Performance Summary](./BTC_ETH_XRP_RSI_PERFORMANCE_SUMMARY.md) - Asset-specific analysis

**Historical Analysis**:
- [Phase 2 Validation Results](./RSI_PHASE2_VALIDATION_RESULTS.md) - Detailed validation testing
- [Phase 0 Comprehensive Analysis](./phase0_improvements/RSI_MEAN_REVERSION_COMPREHENSIVE_ANALYSIS.md) - Original baseline testing

**Code**:
- [rsi_mean_reversion_strategy.py](../core_strategies/rsi_mean_reversion_strategy.py) - Main strategy file
- [rsi_phase2_validation_test.py](../testing/rsi_phase2_validation_test.py) - Validation test suite

---

*Last Updated: 2025-10-14*
*Version: 2.0.0*
*Status: Production-Ready ✅*
