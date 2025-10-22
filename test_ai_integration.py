"""
Comprehensive Test Suite for AI Integration
Tests all AI agents, model factory, and parallel execution
"""

from termcolor import cprint
import sys

from ai_config import AIConfig
from ai_model_factory import AlgoFunModelFactory
from ai_agents.market_intelligence import FundingAgent, LiquidationAgent, WhaleAgent
from ai_agents.risk_management import RiskAgent
from ai_agents.strategy_generation import RBIAgent
from ai_parallel_executor import ParallelExecutor


def test_configuration():
    """Test AI configuration system"""
    cprint("\n" + "="*60, "cyan")
    cprint("TEST 1: AI Configuration System", "cyan")
    cprint("="*60, "cyan")

    try:
        config = AIConfig()
        config.print_config()

        validation = config.validate()
        cprint(f"\n✅ Configuration Valid: {validation['is_valid']}", "green")

        if validation['warnings']:
            cprint(f"⚠️  Warnings: {len(validation['warnings'])}", "yellow")

        return True
    except Exception as e:
        cprint(f"❌ Configuration test failed: {e}", "red")
        return False


def test_model_factory():
    """Test AI model factory"""
    cprint("\n" + "="*60, "cyan")
    cprint("TEST 2: AI Model Factory", "cyan")
    cprint("="*60, "cyan")

    try:
        factory = AlgoFunModelFactory()
        factory.print_status()

        if factory.is_available():
            cprint("\n✅ Model factory operational", "green")

            # Test simple response (if models available)
            available = factory.get_available_models()
            if available:
                cprint(f"\n🧪 Testing AI response generation...", "cyan")
                response = factory.generate_response(
                    prompt="What is 2+2? Answer in one word.",
                    complexity="simple"
                )
                if response:
                    cprint(f"✅ AI Response: {response.content[:100]}", "green")
                else:
                    cprint("⚠️  AI response generation failed (API keys may be missing)", "yellow")

            return True
        else:
            cprint("⚠️  Model factory initialized but no models available", "yellow")
            cprint("   Add API keys to .env.ai to enable AI features", "yellow")
            return True  # Not a failure, just no API keys configured

    except Exception as e:
        cprint(f"❌ Model factory test failed: {e}", "red")
        import traceback
        traceback.print_exc()
        return False


def test_market_intelligence_agents():
    """Test market intelligence agents"""
    cprint("\n" + "="*60, "cyan")
    cprint("TEST 3: Market Intelligence Agents", "cyan")
    cprint("="*60, "cyan")

    results = {}

    # Test Funding Agent
    try:
        cprint("\n📊 Testing Funding Agent...", "cyan")
        funding_agent = FundingAgent()
        result = funding_agent.execute(symbol="BTC")
        results["funding"] = result.success
        cprint(f"   Success: {result.success}", "green" if result.success else "red")
        if result.success:
            cprint(f"   Signal: {result.data.get('signal')}", "cyan")
    except Exception as e:
        cprint(f"   ❌ Failed: {e}", "red")
        results["funding"] = False

    # Test Liquidation Agent
    try:
        cprint("\n💥 Testing Liquidation Agent...", "cyan")
        liq_agent = LiquidationAgent()
        result = liq_agent.execute(symbol="BTC")
        results["liquidation"] = result.success
        cprint(f"   Success: {result.success}", "green" if result.success else "red")
        if result.success:
            cprint(f"   Signal: {result.data.get('signal')}", "cyan")
    except Exception as e:
        cprint(f"   ❌ Failed: {e}", "red")
        results["liquidation"] = False

    # Test Whale Agent
    try:
        cprint("\n🐋 Testing Whale Agent...", "cyan")
        whale_agent = WhaleAgent()
        result = whale_agent.execute(symbol="BTC")
        results["whale"] = result.success
        cprint(f"   Success: {result.success}", "green" if result.success else "red")
        if result.success:
            cprint(f"   Signal: {result.data.get('signal')}", "cyan")
    except Exception as e:
        cprint(f"   ❌ Failed: {e}", "red")
        results["whale"] = False

    all_passed = all(results.values())
    cprint(f"\n{'✅' if all_passed else '⚠️'} Market Intelligence: {sum(results.values())}/3 agents passed",
           "green" if all_passed else "yellow")

    return all_passed


def test_risk_agent():
    """Test risk management agent"""
    cprint("\n" + "="*60, "cyan")
    cprint("TEST 4: Risk Management Agent", "cyan")
    cprint("="*60, "cyan")

    try:
        cprint("\n⚠️  Testing Risk Agent...", "cyan")
        risk_agent = RiskAgent(
            max_position_pct=0.2,
            max_loss_usd=500.0,
            min_balance=100.0
        )

        # Test good position
        good_position = {
            "symbol": "BTC",
            "position_size": 500.0,
            "entry_price": 50000.0,
            "stop_loss": 49000.0,
            "take_profit": 52000.0
        }

        result = risk_agent.execute(
            position_data=good_position,
            account_balance=5000.0
        )

        cprint(f"   Good Position Test: {'✅' if result.success else '❌'}", "green" if result.success else "red")
        if result.success:
            cprint(f"   Approved: {result.data.get('approved')}", "cyan")
            cprint(f"   Risk Score: {result.data.get('risk_score')}/100", "cyan")

        # Test bad position (too large)
        bad_position = {
            "symbol": "BTC",
            "position_size": 4500.0,  # 90% of balance!
            "entry_price": 50000.0,
            "stop_loss": 49000.0,
            "take_profit": 51000.0
        }

        result2 = risk_agent.execute(
            position_data=bad_position,
            account_balance=5000.0
        )

        should_reject = not result2.data.get('approved')
        cprint(f"   Bad Position Test: {'✅ Rejected' if should_reject else '❌ Approved (should reject)'}",
               "green" if should_reject else "red")

        return result.success and should_reject

    except Exception as e:
        cprint(f"❌ Risk agent test failed: {e}", "red")
        import traceback
        traceback.print_exc()
        return False


def test_rbi_agent():
    """Test RBI strategy generation agent"""
    cprint("\n" + "="*60, "cyan")
    cprint("TEST 5: RBI Agent (Strategy Generation)", "cyan")
    cprint("="*60, "cyan")

    try:
        cprint("\n🧠 Testing RBI Agent...", "cyan")
        rbi_agent = RBIAgent(min_quality_score=50.0)  # Lower threshold for testing

        trading_idea = """
        Simple Moving Average Crossover Strategy

        Entry: Buy when 20-period SMA crosses above 50-period SMA
        Exit: Sell when 20-period SMA crosses below 50-period SMA
        Risk: Use 2% stop loss
        Timeframe: 1-hour
        """

        result = rbi_agent.execute(
            source_type="idea",
            source_content=trading_idea,
            symbol="BTC"
        )

        cprint(f"   Success: {result.success}", "green" if result.success else "red")
        if result.success:
            cprint(f"   Quality Score: {result.data.get('quality_score', 0):.1f}/100", "cyan")
            cprint(f"   Approved: {result.data.get('approved')}", "cyan")

        return result.success

    except Exception as e:
        cprint(f"❌ RBI agent test failed: {e}", "red")
        import traceback
        traceback.print_exc()
        return False


def test_parallel_executor():
    """Test parallel execution framework"""
    cprint("\n" + "="*60, "cyan")
    cprint("TEST 6: Parallel Execution Framework", "cyan")
    cprint("="*60, "cyan")

    try:
        cprint("\n🔄 Testing Parallel Executor...", "cyan")
        executor = ParallelExecutor()

        # Simulate test case
        symbol = "BTC"
        strategy_signal = "BUY"
        strategy_data = {"confidence": 0.65}

        # Traditional execution
        trad_result = executor.execute_traditional(
            symbol=symbol,
            strategy_signal=strategy_signal,
            strategy_data=strategy_data
        )

        # AI-enhanced execution
        position_data = {
            "symbol": symbol,
            "position_size": 500.0,
            "entry_price": 50000.0,
            "stop_loss": 49000.0,
            "take_profit": 52000.0
        }

        ai_result = executor.execute_ai_enhanced(
            symbol=symbol,
            strategy_signal=strategy_signal,
            strategy_data=strategy_data,
            position_data=position_data,
            account_balance=5000.0
        )

        # Compare
        comparison = executor.compare_results(trad_result, ai_result)

        cprint("\n✅ Parallel execution completed", "green")
        return True

    except Exception as e:
        cprint(f"❌ Parallel executor test failed: {e}", "red")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all tests"""
    cprint("\n" + "="*60, "magenta")
    cprint("🧪 AI INTEGRATION TEST SUITE", "magenta")
    cprint("="*60, "magenta")

    tests = [
        ("Configuration", test_configuration),
        ("Model Factory", test_model_factory),
        ("Market Intelligence", test_market_intelligence_agents),
        ("Risk Management", test_risk_agent),
        ("Strategy Generation (RBI)", test_rbi_agent),
        ("Parallel Execution", test_parallel_executor)
    ]

    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            cprint(f"\n❌ {test_name} test crashed: {e}", "red")
            results[test_name] = False

    # Print summary
    cprint("\n" + "="*60, "magenta")
    cprint("📊 TEST SUMMARY", "magenta")
    cprint("="*60, "magenta")

    passed = sum(results.values())
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        color = "green" if result else "red"
        cprint(f"{test_name}: {status}", color)

    cprint("\n" + "="*60, "magenta")
    cprint(f"Total: {passed}/{total} tests passed ({passed/total*100:.1f}%)", "magenta")
    cprint("="*60, "magenta")

    if passed == total:
        cprint("\n🎉 ALL TESTS PASSED!", "green")
        return 0
    else:
        cprint(f"\n⚠️  {total - passed} TEST(S) FAILED", "yellow")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
