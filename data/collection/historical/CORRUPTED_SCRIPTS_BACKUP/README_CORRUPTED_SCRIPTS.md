# 🚨 CORRUPTED SCRIPTS - DO NOT USE

**Date Quarantined**: September 15, 2025
**Reason**: Data corruption issues discovered during Phase 1 diagnostic

---

## ❌ **SCRIPTS MOVED TO BACKUP (DO NOT USE)**

### **1. `coinbase_historical_data.py.CORRUPTED`**
- **Issue**: Systematic HIGH/LOW column swap
- **Impact**: 99% of BTC data corrupted with invalid OHLC relationships
- **Severity**: CRITICAL - makes all backtesting unreliable
- **Root Cause**: Incorrect API response mapping (lines 206-211)

### **2. `hyperliquid_historical_data.py.FUTURE_DATES_ISSUE`**
- **Issue**: Generates data with future timestamps (2025 dates)
- **Impact**: Look-ahead bias in backtesting
- **Severity**: CRITICAL - invalidates backtest results
- **Root Cause**: API returning test/synthetic data

### **3. `enhanced_hyperliquid_historical.py.QUALITY_ISSUES`**
- **Issue**: Quality score 61/100 (below 75 threshold) + validation errors
- **Impact**: Unreliable data quality for backtesting
- **Severity**: CRITICAL - fails quality requirements
- **Root Cause**: Volume anomalies and internal validation errors

---

## ✅ **REPLACEMENT SCRIPTS TO USE**

### **For Coinbase Data Collection:**
**Use**: `enhanced_coinbase_historical.py`
- ✅ Correct OHLC mapping
- ✅ Proper API response handling
- ✅ Validated output format
- ✅ Quality score: Production ready

### **For Hyperliquid Data Collection:**
**Status**: ❌ **NOT RECOMMENDED**
- All Hyperliquid scripts fail quality requirements (score <75)
- Use validated alternatives: Coinbase or Yahoo Finance

---

## 🛡️ **VALIDATION REQUIREMENTS**

Before using ANY data collection script:

1. **Run validation system** on output
2. **Check quality score** ≥ 75 for development, ≥ 90 for production
3. **Verify no future dates** in historical data
4. **Confirm OHLC relationships** are mathematically valid
5. **Test with small sample** before large data collection

---

## 📋 **SAFE DATA COLLECTION CHECKLIST**

- [ ] Script produces OHLC data where High ≥ Open, Close ≥ Low
- [ ] No future dates in historical data
- [ ] Filename matches actual data content
- [ ] Quality score ≥ 75 through validation system
- [ ] No critical issues reported by validator

---

**⚠️ WARNING**: Using these corrupted scripts will produce unreliable backtesting results that could lead to significant trading losses in live markets.

**✅ SOLUTION**: Use only validated scripts with quality scores ≥ 75 and no critical issues.