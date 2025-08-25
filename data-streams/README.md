# Algo Trading Data Streams

A comprehensive algorithmic trading data collection and monitoring system for cryptocurrency markets using Binance WebSocket streams.

## 🚀 Features

### Data Streams (`data-streams/`)
- **Real-time trade monitoring** with customizable size thresholds
- **Large trade aggregation** for identifying significant market movements
- **Funding rate tracking** for perpetual futures
- **Multi-symbol support** for major cryptocurrencies
- **CSV data logging** for analysis and backtesting

### Tools (`tools/`)
- Additional trading utilities and analysis tools

## 📊 Components

### 1. Recent Trades Monitor (`recent_trades.py`)
- Monitors all trades above $15,000 USD
- Real-time display with color-coded trade types
- CSV logging with complete trade data
- Supports 9 major cryptocurrency pairs

### 2. Huge Trades Aggregator (`huge_trades.py`)
- Tracks trades above $500,000 USD
- Aggregates trades by second and direction
- Provides summary statistics for large market movements
- Memory-efficient with automatic cleanup

### 3. Funding Rate Monitor (`funding.py`)
- Real-time funding rate tracking for perpetual futures
- Color-coded display based on funding rate levels
- Yearly funding rate calculations
- Mark price monitoring

## 🛠️ Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/RobertYoung2022/algo-trading.git
   cd algo-trading
   ```

2. **Set up conda environment:**
   ```bash
   conda create -n algo python=3.9
   conda activate algo
   ```

3. **Install dependencies:**
   ```bash
   pip install websockets termcolor pytz aiohttp
   ```

## 🚀 Usage

### Start Recent Trades Monitor
```bash
cd data-streams
conda activate algo
python recent_trades.py
```

### Start Huge Trades Aggregator
```bash
cd data-streams
conda activate algo
python huge_trades.py
```

### Start Funding Rate Monitor
```bash
cd data-streams
conda activate algo
python funding.py
```

## 📈 Supported Symbols

- BTCUSDT (Bitcoin)
- ETHUSDT (Ethereum)
- SOLUSDT (Solana)
- XRPUSDT (Ripple)
- LINKUSDT (Chainlink)
- SUIUSDT (Sui)
- HBARUSDT (Hedera)
- AAVEUSDT (Aave)
- OPUSDT (Optimism)

## 📁 Data Output

### CSV Files
- `recent_trades.csv`: All trades above $15,000
- `huge_trades.csv`: All trades above $500,000

### Data Fields
- Event Time
- Symbol
- Aggregate Trade ID
- Price
- Quantity
- First Trade ID
- Trade Time
- Is Buyer Maker
- USD Size

## 🔧 Configuration

### Trade Size Thresholds
- **Recent Trades**: $15,000 minimum
- **Huge Trades**: $500,000 minimum
- **Display Thresholds**: $50k, $100k, $500k with different formatting

### Timezone
- All timestamps are in US/Central timezone

## 📝 Notes

- Requires active internet connection for Binance WebSocket streams
- CSV files grow over time - consider periodic cleanup
- Extended attributes on CSV files are normal macOS behavior
- Use Ctrl+C to gracefully stop any monitoring script

## 🤝 Contributing

Feel free to submit issues and enhancement requests!

## 📄 License

This project is for educational and research purposes.
