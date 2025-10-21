# Security Guidelines for Algo Trading Project

## 🔒 Security Overview

This document outlines security best practices for the algo trading project to prevent sensitive information from being exposed in version control.

## 🚨 Critical Security Rules

### 1. Never Commit API Keys or Secrets
- **NEVER** hardcode API keys, secrets, or private keys in source code
- **ALWAYS** use environment variables for sensitive data
- **ALWAYS** use `.env` files for local development (and ensure they're in `.gitignore`)

### 2. Environment Variables Required
The following environment variables must be set in your `.env` file:

```bash
# CoinMarketCap API
CMC_API_KEY=your_coinmarketcap_api_key_here

# Coinbase API
COINBASE_API_KEY=your_coinbase_api_key_here
COINBASE_API_SECRET=your_coinbase_api_secret_here
COINBASE_PASSPHRASE=your_coinbase_passphrase_here

# Alpha Vantage API
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key_here

# Phemex Exchange
PHEMEX_API_KEY=your_phemex_api_key_here
PHEMEX_SECRET=your_phemex_secret_here

# Hyperliquid Exchange
PH_SECRET_KEY=your_hyperliquid_secret_key_here
PH_API_KEY=your_hyperliquid_api_key_here
HYPERLIQUID_PRIVATE_KEY=your_hyperliquid_private_key_here
```

### 3. Files Automatically Excluded from Git
The following file types are automatically excluded by `.gitignore`:
- `.env*` files (all environment files)
- `*.csv` files (trading data)
- `*.json` files (market data)
- `*.log` files (logs that might contain sensitive info)
- `*_secret.py`, `*_keys.py`, `*_credentials.py` files
- Trading-related directories: `trades/`, `positions/`, `orders/`, `balances/`

## 🛡️ Security Checklist

Before committing code, ensure:

- [ ] No hardcoded API keys or secrets
- [ ] All sensitive data uses environment variables
- [ ] `.env` file is not tracked by git
- [ ] No sensitive trading data in CSV/JSON files
- [ ] No log files containing sensitive information
- [ ] All config files use environment variables

## 🔍 Security Audit Commands

### Check for potential security violations:
```bash
# Search for hardcoded API keys
grep -r "api_key.*=" --include="*.py" .
grep -r "secret.*=" --include="*.py" .

# Check for environment variable usage
grep -r "os.getenv" --include="*.py" .

# Verify .gitignore is working
git status --ignored
```

### Remove sensitive files from git history (if needed):
```bash
# Remove files from git tracking
git rm --cached sensitive_file.csv
git rm --cached sensitive_file.log

# Add to .gitignore to prevent future tracking
echo "sensitive_file.csv" >> .gitignore
echo "sensitive_file.log" >> .gitignore
```

## 📋 Environment Setup

1. **Create `.env` file** in project root:
   ```bash
   cp .env.example .env  # if example exists
   # OR create new .env file with required variables
   ```

2. **Install python-dotenv**:
   ```bash
   pip install python-dotenv
   ```

3. **Load environment variables** in Python:
   ```python
   from dotenv import load_dotenv
   import os
   
   load_dotenv()
   api_key = os.getenv('YOUR_API_KEY')
   ```

## 🚨 Emergency Response

If you accidentally commit sensitive data:

1. **Immediately** remove the file from git:
   ```bash
   git rm --cached sensitive_file
   ```

2. **Add to .gitignore**:
   ```bash
   echo "sensitive_file" >> .gitignore
   ```

3. **Rotate/regenerate** any exposed API keys or secrets

4. **Consider** using `git filter-branch` or BFG Repo-Cleaner to remove from git history

## 📞 Support

If you discover a security vulnerability:
1. **DO NOT** create a public issue
2. **DO** contact the project maintainer privately
3. **DO** follow responsible disclosure practices

---

**Remember**: Security is everyone's responsibility. When in doubt, ask before committing sensitive data!
