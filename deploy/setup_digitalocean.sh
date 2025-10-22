#!/bin/bash

# ============================================================================
# DigitalOcean Droplet Setup Script
# ============================================================================
# Automated setup for ETH Multi-Strategy Rotation trading system
#
# Usage:
#   1. Create Ubuntu 22.04 droplet on DigitalOcean (2GB RAM minimum)
#   2. SSH into droplet: ssh root@YOUR_DROPLET_IP
#   3. Run this script: bash setup_digitalocean.sh
#
# This script will:
#   - Install Python 3 and required packages
#   - Clone your repository
#   - Setup environment variables
#   - Install systemd service
#   - Start trading system
# ============================================================================

set -e  # Exit on error

echo "============================================================================"
echo "🚀 ETH Multi-Strategy Rotation - DigitalOcean Setup"
echo "============================================================================"

# ============================================================================
# 1. SYSTEM UPDATES
# ============================================================================
echo ""
echo "📦 Updating system packages..."
apt update && apt upgrade -y

# ============================================================================
# 2. INSTALL DEPENDENCIES
# ============================================================================
echo ""
echo "🔧 Installing Python 3 and Git..."
apt install -y python3 python3-pip git python3-venv

# ============================================================================
# 3. CLONE REPOSITORY
# ============================================================================
echo ""
echo "📂 Cloning repository..."

# Prompt for repository URL
read -p "Enter your GitHub repository URL (e.g., https://github.com/user/algo-fun.git): " REPO_URL

if [ ! -d "/root/algo-fun" ]; then
    cd /root
    git clone "$REPO_URL"
    cd algo-fun
    git checkout feature/eth-strategy-rotation
else
    echo "⚠️  Repository already exists, pulling latest changes..."
    cd /root/algo-fun
    git pull
fi

# ============================================================================
# 4. SETUP PYTHON ENVIRONMENT
# ============================================================================
echo ""
echo "🐍 Setting up Python environment..."

# Install main requirements
if [ -f "requirements.txt" ]; then
    pip3 install -r requirements.txt
fi

# Install moon-dev-agents submodule requirements
if [ -f "moon-dev-agents/requirements.txt" ]; then
    cd moon-dev-agents
    pip3 install -r requirements.txt
    cd ..
fi

# ============================================================================
# 5. CONFIGURE ENVIRONMENT VARIABLES
# ============================================================================
echo ""
echo "🔐 Configuring environment variables..."

# Check if .env.ai exists
if [ ! -f ".env.ai" ]; then
    echo "⚠️  .env.ai not found. Creating from template..."

    if [ -f ".env.ai.example" ]; then
        cp .env.ai.example .env.ai
    else
        # Create .env.ai file
        cat > .env.ai << 'EOF'
# AI Model API Keys
ANTHROPIC_API_KEY=sk-ant-YOUR_ANTHROPIC_KEY_HERE
DEEPSEEK_API_KEY=sk-YOUR_DEEPSEEK_KEY_HERE

# HyperLiquid (ETH Private Key)
PH_SECRET_KEY=0xYOUR_ETH_PRIVATE_KEY_HERE

# Market Data APIs (Optional)
MOONDEV_API_KEY=
BIRDEYE_API_KEY=
COINGECKO_API_KEY=

# Trading Mode
PAPER_TRADING=true
EOF
    fi

    echo ""
    echo "⚠️  IMPORTANT: Edit .env.ai and add your API keys!"
    echo "   File location: /root/algo-fun/.env.ai"
    echo ""
    read -p "Press Enter when you've added your API keys..."
fi

# ============================================================================
# 6. CREATE SYSTEMD SERVICE
# ============================================================================
echo ""
echo "⚙️  Creating systemd service..."

cat > /etc/systemd/system/algo-trading.service << 'EOF'
[Unit]
Description=ETH Multi-Strategy Rotation Trading Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/algo-fun
ExecStart=/usr/bin/python3 -u trading/main_trading_loop.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# ============================================================================
# 7. ENABLE AND START SERVICE
# ============================================================================
echo ""
echo "🚀 Starting trading system..."

systemctl daemon-reload
systemctl enable algo-trading.service
systemctl start algo-trading.service

# ============================================================================
# 8. SETUP COMPLETE
# ============================================================================
echo ""
echo "============================================================================"
echo "✅ SETUP COMPLETE!"
echo "============================================================================"
echo ""
echo "📊 Service Status:"
systemctl status algo-trading.service --no-pager
echo ""
echo "📝 Useful Commands:"
echo "  • View logs:        journalctl -u algo-trading -f"
echo "  • Restart service:  systemctl restart algo-trading"
echo "  • Stop service:     systemctl stop algo-trading"
echo "  • Service status:   systemctl status algo-trading"
echo "  • View dashboard:   cd /root/algo-fun && python3 trading/performance_dashboard.py"
echo ""
echo "📁 Important Files:"
echo "  • Config:           /root/algo-fun/trading/config_eth_aggressive.py"
echo "  • Environment:      /root/algo-fun/.env.ai"
echo "  • Trade log:        /root/algo-fun/trading/logs/trades_log.csv"
echo "  • Reports:          /root/algo-fun/trading/reports/"
echo ""
echo "============================================================================"
