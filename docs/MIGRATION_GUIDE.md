# 🔄 Repository Migration Guide

**Date**: October 21, 2025  
**Action**: Complete Repository Reorganization

---

## 📋 **What Changed**

### **New Directory Structure**
The repository has been completely reorganized for better maintainability and clarity:

```
OLD STRUCTURE                    NEW STRUCTURE
├── main.py                     → core/main.py
├── trading_functions/          → core/trading_functions/
├── data-scripts/               → data/collection/historical/
├── dataset_files/              → data/storage/dataset_files/
├── strategies/                 → strategies/ (organized)
├── bots/                       → bots/ (organized)
├── *.md files                  → docs/
├── *.html files                → docs/
├── *.log files                 → monitoring/logs/
├── test_*.py                   → tools/testing/
├── archive/                    → archive/ (reorganized)
└── stress-test/                → tools/testing/
```

---

## 🚀 **How to Use the New Structure**

### **Main Entry Point**
```bash
# OLD
python main.py

# NEW
python core/main.py
```

### **Strategy Testing**
```bash
# OLD
cd strategies/testing
python universal_strategy_tester.py SMAStrategy

# NEW (same location, but clearer organization)
cd strategies/testing
python universal_strategy_tester.py SMAStrategy
```

### **Data Collection**
```bash
# OLD
python data-scripts/coinbase_historical_data.py

# NEW
python data/collection/historical/coinbase_historical_data.py
```

### **Bot Deployment**
```bash
# OLD
cd strategies/production
python strategy_to_bot_converter.py

# NEW (same location)
cd strategies/production
python strategy_to_bot_converter.py
```

---

## 📁 **Directory Purposes**

### **`core/`** - Core Production Systems
- `main.py` - Main entry point
- `trading_functions/` - Modern function library
- `config/` - Configuration files

### **`strategies/`** - Trading Strategies
- `core_strategies/` - Production-ready strategies
- `testing/` - Testing framework
- `production/` - Deployment tools
- `results/` - Backtest results (organized by type)

### **`bots/`** - Live Trading Bots
- `hyperliquid/` - Hyperliquid exchange bots
- `universal/` - Universal bot templates
- `utils/` - Bot utilities

### **`data/`** - Data Management
- `collection/` - Data collection scripts
- `storage/` - Data files
- `validation/` - Data quality tools

### **`monitoring/`** - System Monitoring
- `logs/` - All log files
- `health/` - Health check scripts
- `alerts/` - Alert systems

### **`docs/`** - Documentation
- `guides/` - User guides
- `api/` - API documentation
- `reports/` - Analysis reports

### **`archive/`** - Archived Files
- `old_strategies/` - Archived strategies
- `old_bots/` - Archived bots
- `old_data/` - Archived data
- `old_reports/` - Archived reports

### **`tools/`** - Development Tools
- `testing/` - Test utilities
- `maintenance/` - Maintenance scripts

---

## 🔧 **Updated Import Paths**

### **Core Functions**
```python
# OLD
from trading_functions import universal_get_ask_bid

# NEW (same import, but from core/)
from trading_functions import universal_get_ask_bid
```

### **Strategy Imports**
```python
# OLD
sys.path.append('/Users/bobbyyo/Projects/algo-fun')

# NEW (updated in main.py)
sys.path.append('/Users/bobbyyo/Projects/algo-fun')
sys.path.append('/Users/bobbyyo/Projects/algo-fun/core')
```

---

## 📊 **File Movements Summary**

### **Moved to `docs/`:**
- All `.md` files (documentation)
- All `.html` files (backtest results)

### **Moved to `monitoring/logs/`:**
- All `.log` files
- Monitor log files

### **Moved to `tools/testing/`:**
- `test_*.py` files
- `validate_*.py` files
- `investigate_*.py` files
- `stress-test/` contents

### **Moved to `archive/`:**
- Legacy `my_nice_function.py`
- Legacy `sma.py`
- Old strategy backups
- Old data streams

### **Reorganized:**
- `data-scripts/` → `data/collection/historical/`
- `dataset_files/` → `data/storage/dataset_files/`
- Results organized by type (performance, optimization, trades)

---

## ✅ **Verification Checklist**

After migration, verify these systems work:

1. **Main Entry Point**
   ```bash
   python core/main.py
   ```

2. **Strategy Testing**
   ```bash
   cd strategies/testing
   python universal_strategy_tester.py SMAStrategy
   ```

3. **Data Collection**
   ```bash
   python data/collection/historical/coinbase_historical_data.py
   ```

4. **Bot Deployment**
   ```bash
   cd strategies/production
   python strategy_to_bot_converter.py
   ```

---

## 🎯 **Benefits of New Structure**

### **✅ Improved Organization**
- Clear separation of concerns
- Logical grouping of related files
- Easy navigation and maintenance

### **✅ Better Scalability**
- Modular structure supports growth
- Clear entry points for each component
- Organized archive for historical reference

### **✅ Enhanced Maintainability**
- Centralized configuration
- Organized documentation
- Clear testing and monitoring separation

### **✅ Production Readiness**
- Clear production vs development separation
- Organized monitoring and logging
- Streamlined deployment process

---

## 🚀 **Next Steps**

1. **Test All Systems** - Verify everything works with new structure
2. **Update Scripts** - Update any custom scripts to use new paths
3. **Update Documentation** - Reference new structure in guides
4. **Clean Up** - Remove any remaining old references

---

**Your repository is now organized for maximum efficiency and maintainability!** 🌙💫🚀
