# 🗂️ Failed Strategies Archive

This directory contains strategies that performed poorly during backtesting and are not suitable for live trading in their current form.

## 📊 Classification Criteria for Failed Strategies:

### ❌ Performance Thresholds:
- **Negative Sharpe Ratio** (< 0)
- **Low Win Rate** (< 35%)
- **Negative Total Returns** while market gained significantly
- **Excessive Drawdowns** (> 20%)
- **No Trade Generation** (0 trades across multiple assets/timeframes)

### 🗂️ Current Failed Strategies:

#### 1. ClucMay72018 Momentum Reversal
- **Date Added:** 2025-09-17
- **Reason:** Catastrophic performance across all configurations
- **Key Issues:**
  - Negative returns (-1.5% to -73%) while crypto market gained 70-300%
  - Terrible Sharpe ratios (-1.6 to -22.3)
  - Low win rates (14-31%)
  - Zero trades generated on BTC (complete failure on primary asset)
  - Over-restrictive entry conditions
  - Mean reversion approach failed in trending crypto markets
- **Files:** 12 strategy and test files + 4 result CSVs
- **Status:** Abandoned - fundamental approach flawed

## 💡 Purpose of This Archive:

1. **Learning Reference:** Understand what doesn't work and why
2. **Future Research:** Patterns or components might be useful in other strategies
3. **Avoid Repetition:** Don't waste time re-testing known failed approaches
4. **Documentation:** Complete record of testing methodology and results

## 🔄 Potential Recovery Actions:

Strategies in this folder could potentially be:
- **Completely redesigned** with different core logic
- **Stripped for parts** - individual indicators might be useful
- **Used as negative examples** for training/research
- **Permanently archived** if fundamentally flawed

## 📋 Organization:

Each failed strategy gets its own subfolder containing:
- Original strategy files
- All test frameworks and variants
- Complete result CSVs
- Documentation of why it failed

**Note:** Strategies are moved here after comprehensive testing confirms poor performance across multiple assets, timeframes, and parameter configurations.