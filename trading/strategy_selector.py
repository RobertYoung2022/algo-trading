"""
Strategy Selector - Multi-Strategy Rotation Logic
==================================================
Analyzes ETH with all 3 strategies and selects the best opportunity

Strategies:
1. RSI Mean Reversion (1h timeframe) - Oversold bounces
2. SMA Crossover (4h timeframe) - Trend following
3. Breakout Momentum (1d timeframe) - Volatility expansion

Selection Logic:
- Runs all strategies in parallel on their respective timeframes
- Enhances signals with AI market intelligence (funding, liquidations, whales)
- Selects strategy with highest confidence score
- Returns None if no strategy has >MIN_CONFIDENCE_THRESHOLD
"""

import sys
import os
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime
import pandas as pd
from termcolor import cprint

# Add parent directory to path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import configuration
from trading.config_eth_aggressive import (
    SYMBOLS,
    MIN_CONFIDENCE_THRESHOLD,
    ENABLE_RSI_STRATEGY,
    ENABLE_SMA_STRATEGY,
    ENABLE_BREAKOUT_STRATEGY,
    RSI_CONFIG,
    SMA_CONFIG,
    BREAKOUT_CONFIG,
    USE_AI_VALIDATION,
    USE_FUNDING_AGENT,
    USE_LIQUIDATION_AGENT,
    USE_WHALE_AGENT,
)

# Import AI agents
from ai_agents.market_intelligence.funding_agent import FundingAgent
from ai_agents.market_intelligence.liquidation_agent import LiquidationAgent
from ai_agents.market_intelligence.whale_agent import WhaleAgent
from ai_model_factory import get_factory

# Import strategies (we'll need to adapt these)
# Note: The existing strategies use backtesting.py format, we'll need to extract their logic


class StrategySignal:
    """Standardized strategy signal format"""
    def __init__(self,
                 strategy_name: str,
                 signal: str,  # "BUY", "SELL", or "HOLD"
                 confidence: float,  # 0-100
                 reasoning: str,
                 timeframe: str,
                 entry_price: float,
                 stop_loss: Optional[float] = None,
                 take_profit: Optional[float] = None,
                 metadata: Optional[Dict] = None):
        self.strategy_name = strategy_name
        self.signal = signal
        self.confidence = confidence
        self.reasoning = reasoning
        self.timeframe = timeframe
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.metadata = metadata or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict:
        """Convert signal to dictionary"""
        return {
            "strategy_name": self.strategy_name,
            "signal": self.signal,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "timeframe": self.timeframe,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat()
        }

    def __repr__(self):
        return f"StrategySignal({self.strategy_name}: {self.signal} @ {self.confidence:.0f}%)"


class StrategySelector:
    """
    Multi-Strategy Rotation Selector

    Analyzes ETH with multiple strategies and selects the best opportunity
    """

    def __init__(self):
        """Initialize strategy selector"""
        self.symbol = SYMBOLS[0]  # ETH
        self.min_confidence = MIN_CONFIDENCE_THRESHOLD

        # Initialize AI agents if enabled
        self.funding_agent = FundingAgent() if USE_FUNDING_AGENT else None
        self.liquidation_agent = LiquidationAgent() if USE_LIQUIDATION_AGENT else None
        self.whale_agent = WhaleAgent() if USE_WHALE_AGENT else None
        self.ai_factory = get_factory() if USE_AI_VALIDATION else None

        cprint(f"✅ Strategy Selector initialized for {self.symbol}", "green")

    def get_ohlcv_data(self, symbol: str, timeframe: str, lookback: int) -> pd.DataFrame:
        """
        Fetch OHLCV data from HyperLiquid

        Args:
            symbol: Trading symbol (e.g., "ETH")
            timeframe: Timeframe (e.g., "1h", "4h", "1d")
            lookback: Number of bars to fetch

        Returns:
            DataFrame with OHLCV data
        """
        try:
            # Import HyperLiquid functions
            from moon_dev_agents.src.nice_funcs_hl import _get_ohlcv

            # Convert timeframe to HyperLiquid format
            timeframe_map = {
                "1h": "1h",
                "4h": "4h",
                "1d": "1d"
            }
            hl_timeframe = timeframe_map.get(timeframe, timeframe)

            # Calculate time range
            from datetime import timedelta
            end_time = datetime.now()

            if timeframe == "1h":
                start_time = end_time - timedelta(hours=lookback)
            elif timeframe == "4h":
                start_time = end_time - timedelta(hours=lookback * 4)
            else:  # 1d
                start_time = end_time - timedelta(days=lookback)

            # Fetch candles
            candles = _get_ohlcv(symbol, hl_timeframe, start_time, end_time)

            if not candles:
                cprint(f"⚠️  No data returned for {symbol} {timeframe}", "yellow")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(candles)

            # Ensure required columns
            required_cols = ["time", "open", "high", "low", "close", "volume"]
            if not all(col in df.columns for col in required_cols):
                cprint(f"⚠️  Missing required columns in {symbol} data", "yellow")
                return pd.DataFrame()

            # Rename columns to match backtesting format
            df = df.rename(columns={
                "time": "Datetime",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume"
            })

            # Convert timestamp to datetime
            df["Datetime"] = pd.to_datetime(df["Datetime"], unit="ms")
            df.set_index("Datetime", inplace=True)

            return df

        except Exception as e:
            cprint(f"❌ Error fetching {symbol} data: {e}", "red")
            return pd.DataFrame()

    def analyze_rsi_strategy(self, data: pd.DataFrame) -> Optional[StrategySignal]:
        """
        Analyze RSI Mean Reversion strategy

        Args:
            data: OHLCV DataFrame (1h timeframe)

        Returns:
            StrategySignal or None
        """
        if data.empty:
            return None

        try:
            # Calculate RSI
            def calculate_rsi(prices, period=14):
                delta = prices.diff()
                gain = delta.where(delta > 0, 0).rolling(window=period).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
                rs = gain / loss
                return 100 - (100 / (1 + rs))

            rsi = calculate_rsi(data["Close"], period=RSI_CONFIG["rsi_period"])
            current_rsi = rsi.iloc[-1]
            current_price = data["Close"].iloc[-1]

            # Check for oversold condition
            if current_rsi < RSI_CONFIG["rsi_oversold"]:
                # Calculate strength of oversold condition
                oversold_strength = (RSI_CONFIG["rsi_oversold"] - current_rsi) / RSI_CONFIG["rsi_oversold"]
                confidence = min(60 + (oversold_strength * 40), 95)  # 60-95% confidence

                return StrategySignal(
                    strategy_name="RSI_Mean_Reversion",
                    signal="BUY",
                    confidence=confidence,
                    reasoning=f"RSI oversold at {current_rsi:.1f} (threshold {RSI_CONFIG['rsi_oversold']})",
                    timeframe=RSI_CONFIG["timeframe"],
                    entry_price=current_price,
                    metadata={"rsi": current_rsi, "oversold_strength": oversold_strength}
                )

            # Check for overbought condition (potential short, but we're long-only)
            elif current_rsi > RSI_CONFIG["rsi_overbought"]:
                return None  # Skip overbought (no shorting in this system)

            return None  # Neutral zone

        except Exception as e:
            cprint(f"⚠️  RSI strategy error: {e}", "yellow")
            return None

    def analyze_sma_strategy(self, data: pd.DataFrame) -> Optional[StrategySignal]:
        """
        Analyze SMA Crossover strategy

        Args:
            data: OHLCV DataFrame (4h timeframe)

        Returns:
            StrategySignal or None
        """
        if data.empty or len(data) < SMA_CONFIG["slow_period"] + 1:
            return None

        try:
            # Calculate SMAs
            fast_sma = data["Close"].rolling(window=SMA_CONFIG["fast_period"]).mean()
            slow_sma = data["Close"].rolling(window=SMA_CONFIG["slow_period"]).mean()

            current_fast = fast_sma.iloc[-1]
            current_slow = slow_sma.iloc[-1]
            prev_fast = fast_sma.iloc[-2]
            prev_slow = slow_sma.iloc[-2]
            current_price = data["Close"].iloc[-1]

            # Check for bullish crossover
            if prev_fast <= prev_slow and current_fast > current_slow:
                # Calculate strength of crossover
                crossover_strength = (current_fast - current_slow) / current_slow
                confidence = min(55 + (crossover_strength * 1000), 90)  # 55-90% confidence

                return StrategySignal(
                    strategy_name="SMA_Crossover",
                    signal="BUY",
                    confidence=confidence,
                    reasoning=f"Fast SMA ({current_fast:.2f}) crossed above Slow SMA ({current_slow:.2f})",
                    timeframe=SMA_CONFIG["timeframe"],
                    entry_price=current_price,
                    metadata={
                        "fast_sma": current_fast,
                        "slow_sma": current_slow,
                        "crossover_strength": crossover_strength
                    }
                )

            # Check for bearish crossover
            elif prev_fast >= prev_slow and current_fast < current_slow:
                return None  # Bearish crossover, no action (long-only system)

            return None  # No crossover

        except Exception as e:
            cprint(f"⚠️  SMA strategy error: {e}", "yellow")
            return None

    def analyze_breakout_strategy(self, data: pd.DataFrame) -> Optional[StrategySignal]:
        """
        Analyze Breakout Momentum strategy

        Args:
            data: OHLCV DataFrame (1d timeframe)

        Returns:
            StrategySignal or None
        """
        if data.empty or len(data) < BREAKOUT_CONFIG["resistance_lookback_days"]:
            return None

        try:
            lookback = BREAKOUT_CONFIG["resistance_lookback_days"]

            # Calculate resistance level (highest high in lookback period)
            resistance = data["High"].iloc[-lookback:-1].max()  # Exclude current bar

            # Current values
            current_price = data["Close"].iloc[-1]
            current_volume = data["Volume"].iloc[-1]
            avg_volume = data["Volume"].iloc[-lookback:-1].mean()

            # Check for breakout
            if current_price > resistance:
                # Check volume confirmation
                volume_ratio = current_volume / avg_volume
                has_volume = volume_ratio >= BREAKOUT_CONFIG["volume_threshold"]

                if has_volume:
                    # Calculate breakout strength
                    breakout_strength = (current_price - resistance) / resistance
                    volume_strength = min((volume_ratio - 1) / 2, 1)  # Normalize to 0-1

                    confidence = min(
                        65 + (breakout_strength * 200) + (volume_strength * 25),
                        95
                    )  # 65-95% confidence

                    return StrategySignal(
                        strategy_name="Breakout_Momentum",
                        signal="BUY",
                        confidence=confidence,
                        reasoning=f"Price broke resistance ${resistance:.2f} with {volume_ratio:.1f}x volume",
                        timeframe=BREAKOUT_CONFIG["timeframe"],
                        entry_price=current_price,
                        metadata={
                            "resistance": resistance,
                            "breakout_strength": breakout_strength,
                            "volume_ratio": volume_ratio
                        }
                    )

            return None  # No breakout

        except Exception as e:
            cprint(f"⚠️  Breakout strategy error: {e}", "yellow")
            return None

    def enhance_signal_with_ai(self, signal: StrategySignal,
                               market_data: Dict) -> StrategySignal:
        """
        Enhance strategy signal with AI market intelligence

        Args:
            signal: Original strategy signal
            market_data: Market intelligence data (funding, liquidations, whales)

        Returns:
            Enhanced signal with adjusted confidence
        """
        if not USE_AI_VALIDATION:
            return signal

        try:
            # Collect AI intelligence
            ai_signals = []

            # Funding rate analysis
            if self.funding_agent and "funding" in market_data:
                funding_result = self.funding_agent.execute(
                    symbol=self.symbol,
                    **market_data["funding"]
                )
                ai_signals.append(funding_result)

            # Liquidation analysis
            if self.liquidation_agent and "liquidations" in market_data:
                liq_result = self.liquidation_agent.execute(
                    symbol=self.symbol,
                    **market_data["liquidations"]
                )
                ai_signals.append(liq_result)

            # Whale activity analysis
            if self.whale_agent and "whales" in market_data:
                whale_result = self.whale_agent.execute(
                    symbol=self.symbol,
                    **market_data["whales"]
                )
                ai_signals.append(whale_result)

            # Calculate AI confidence adjustment
            if ai_signals:
                # Average confidence from AI agents
                ai_confidences = [s.get("confidence", 50) for s in ai_signals if s]
                avg_ai_confidence = sum(ai_confidences) / len(ai_confidences)

                # Adjust original confidence (weighted average: 70% strategy, 30% AI)
                original_confidence = signal.confidence
                enhanced_confidence = (original_confidence * 0.7) + (avg_ai_confidence * 0.3)

                # Add AI reasoning to metadata
                ai_reasoning = " | ".join([s.get("reasoning", "") for s in ai_signals if s])
                signal.metadata["ai_enhancement"] = {
                    "original_confidence": original_confidence,
                    "ai_confidence": avg_ai_confidence,
                    "enhanced_confidence": enhanced_confidence,
                    "ai_reasoning": ai_reasoning
                }

                signal.confidence = enhanced_confidence
                signal.reasoning += f" | AI: {ai_reasoning[:100]}"  # Truncate if too long

            return signal

        except Exception as e:
            cprint(f"⚠️  AI enhancement error: {e}", "yellow")
            return signal  # Return original signal on error

    def select_best_strategy(self) -> Optional[StrategySignal]:
        """
        Analyze ETH with all strategies and select the best opportunity

        Returns:
            Best strategy signal or None if no good opportunities
        """
        cprint(f"\n{'='*60}", "cyan")
        cprint(f"🔍 Analyzing {self.symbol} with all strategies...", "cyan")
        cprint(f"{'='*60}", "cyan")

        signals: List[StrategySignal] = []

        # Strategy 1: RSI Mean Reversion (1h)
        if ENABLE_RSI_STRATEGY:
            cprint("\n📊 Running RSI Mean Reversion (1h)...", "cyan")
            data_1h = self.get_ohlcv_data(self.symbol, RSI_CONFIG["timeframe"],
                                          RSI_CONFIG["lookback_bars"])
            rsi_signal = self.analyze_rsi_strategy(data_1h)
            if rsi_signal:
                cprint(f"  {rsi_signal}", "green")
                signals.append(rsi_signal)
            else:
                cprint("  No RSI signal", "yellow")

        # Strategy 2: SMA Crossover (4h)
        if ENABLE_SMA_STRATEGY:
            cprint("\n📊 Running SMA Crossover (4h)...", "cyan")
            data_4h = self.get_ohlcv_data(self.symbol, SMA_CONFIG["timeframe"],
                                          SMA_CONFIG["lookback_bars"])
            sma_signal = self.analyze_sma_strategy(data_4h)
            if sma_signal:
                cprint(f"  {sma_signal}", "green")
                signals.append(sma_signal)
            else:
                cprint("  No SMA signal", "yellow")

        # Strategy 3: Breakout Momentum (1d)
        if ENABLE_BREAKOUT_STRATEGY:
            cprint("\n📊 Running Breakout Momentum (1d)...", "cyan")
            data_1d = self.get_ohlcv_data(self.symbol, BREAKOUT_CONFIG["timeframe"],
                                          BREAKOUT_CONFIG["lookback_bars"])
            breakout_signal = self.analyze_breakout_strategy(data_1d)
            if breakout_signal:
                cprint(f"  {breakout_signal}", "green")
                signals.append(breakout_signal)
            else:
                cprint("  No Breakout signal", "yellow")

        # No signals found
        if not signals:
            cprint("\n⏸️  No BUY signals from any strategy", "yellow")
            return None

        # Enhance signals with AI (if enabled)
        if USE_AI_VALIDATION:
            cprint("\n🤖 Enhancing signals with AI market intelligence...", "cyan")
            # TODO: Fetch market data (funding, liquidations, whales)
            market_data = {}  # Placeholder
            enhanced_signals = [self.enhance_signal_with_ai(s, market_data) for s in signals]
            signals = enhanced_signals

        # Select best signal (highest confidence)
        best_signal = max(signals, key=lambda s: s.confidence)

        # Check if best signal meets minimum threshold
        if best_signal.confidence < self.min_confidence:
            cprint(f"\n⏸️  Best signal ({best_signal.strategy_name}) has low confidence: "
                   f"{best_signal.confidence:.1f}% < {self.min_confidence}%", "yellow")
            return None

        # Print selection summary
        cprint(f"\n{'='*60}", "green")
        cprint(f"🎯 SELECTED STRATEGY: {best_signal.strategy_name}", "green")
        cprint(f"{'='*60}", "green")
        cprint(f"  Signal: {best_signal.signal}", "green")
        cprint(f"  Confidence: {best_signal.confidence:.1f}%", "green")
        cprint(f"  Timeframe: {best_signal.timeframe}", "green")
        cprint(f"  Entry Price: ${best_signal.entry_price:.2f}", "green")
        cprint(f"  Reasoning: {best_signal.reasoning}", "green")

        if len(signals) > 1:
            cprint(f"\n  Other signals:", "cyan")
            for sig in signals:
                if sig != best_signal:
                    cprint(f"    - {sig.strategy_name}: {sig.confidence:.1f}%", "cyan")

        return best_signal


if __name__ == "__main__":
    """Test strategy selector"""
    print("🧪 Testing Strategy Selector...\n")

    selector = StrategySelector()
    best_strategy = selector.select_best_strategy()

    if best_strategy:
        print(f"\n✅ Best Strategy Found:")
        print(f"   {best_strategy.to_dict()}")
    else:
        print("\n⏸️  No good opportunities at this time")
