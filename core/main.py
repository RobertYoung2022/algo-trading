#!/usr/bin/env python3
"""
🚀 Bobby's Simplified Algo Trading System - Main Entry Point
===========================================================
Single entry point to prevent multiple processes and provide clear workflow.
Replaces scattered scripts with unified control system.

🌟 Workflow:
    Data → Strategy → Backtest → Optimize → Deploy → Monitor

💫 Features:
    - No competing processes (sequential execution)
    - Clear menu-driven interface
    - Production readiness checks
    - Comprehensive logging
    - Error handling and recovery

🔧 Usage:
    python main.py

    Select from menu:
    1. Test Strategy (comprehensive multi-asset)
    2. Quick Single Asset Test
    3. System Status & Health Check
    4. View Recent Results
    5. Archive Management
"""

import os
import sys
import subprocess
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 🚀 Add project path
sys.path.append('/Users/bobbyyo/Projects/algo-fun')
sys.path.append('/Users/bobbyyo/Projects/algo-fun/core')

# 🎯 Import core components
try:
    from trading_functions import production_readiness_check, TRADING_CONFIG
    TRADING_FUNCTIONS_AVAILABLE = True
except ImportError:
    TRADING_FUNCTIONS_AVAILABLE = False

class AlgoTradingSystem:
    """
    🎯 Main system controller - prevents multiple processes
    Single point of control for all trading operations
    """

    def __init__(self):
        self.project_root = '/Users/bobbyyo/Projects/algo-fun'
        self.strategies = ['SMAStrategy', 'RSIMeanReversionStrategy', 'BreakoutMomentumStrategy']
        self.session_log = []

    def show_banner(self):
        """🎨 Display system banner"""
        print("🚀" + "=" * 60 + "🚀")
        print("🌟    Bobby's SIMPLIFIED Algo Trading System v2.0     🌟")
        print("💫" + "=" * 60 + "💫")
        print("📊 Status: Single Process, No Conflicts, Ultra Clean")
        print(f"🕒 Session: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🚀" + "=" * 60 + "🚀")

    def check_system_health(self):
        """🛡️ Comprehensive system health check"""
        print("\n🛡️ SYSTEM HEALTH CHECK")
        print("=" * 30)

        issues = []

        # Check for running Python processes
        try:
            result = subprocess.run(['pgrep', '-f', 'python'], capture_output=True, text=True)
            if result.stdout.strip():
                running_processes = len(result.stdout.strip().split('\n'))
                if running_processes > 2:  # Allow for this script + maybe one other
                    issues.append(f"⚠️ {running_processes} Python processes running")
                else:
                    print("✅ Process count normal")
            else:
                print("✅ No competing Python processes")
        except:
            print("⚠️ Cannot check process count")

        # Check @trading_functions
        if TRADING_FUNCTIONS_AVAILABLE:
            print("✅ @trading_functions library available")
            try:
                if production_readiness_check():
                    print("✅ Production readiness: PASS")
                else:
                    issues.append("⚠️ Production readiness check failed")
            except:
                issues.append("⚠️ Production readiness check error")
        else:
            issues.append("❌ @trading_functions library not available")

        # Check data directory
        data_dir = f"{self.project_root}/dataset_files"
        if os.path.exists(data_dir):
            csv_count = len([f for f in os.listdir(data_dir) if f.endswith('.csv')])
            if csv_count > 0:
                print(f"✅ Data files available: {csv_count} CSV files")
            else:
                issues.append("⚠️ No CSV data files found")
        else:
            issues.append("❌ Dataset directory not found")

        # Check core strategies
        strategy_dir = f"{self.project_root}/strategies/core_strategies"
        if os.path.exists(strategy_dir):
            strategy_files = [f for f in os.listdir(strategy_dir) if f.endswith('.py')]
            print(f"✅ Core strategies available: {len(strategy_files)} files")
        else:
            issues.append("❌ Core strategies directory not found")

        # Check results directory
        results_dir = f"{self.project_root}/strategies/results"
        if os.path.exists(results_dir):
            print("✅ Results directory available")
        else:
            issues.append("⚠️ Results directory not found")

        # Summary
        if issues:
            print(f"\n⚠️ Issues found: {len(issues)}")
            for issue in issues:
                print(f"   {issue}")
        else:
            print("\n✅ System health: EXCELLENT")

        return len(issues) == 0

    def show_main_menu(self):
        """📋 Display main menu options"""
        print("\n📋 SELECT OPERATION:")
        print("=" * 30)
        print("1. 🎯 Test Strategy (Multi-Asset)")
        print("2. ⚡ Quick Single Asset Test")
        print("3. 🛡️ System Status & Health")
        print("4. 📊 View Recent Results")
        print("5. 🗄️ Archive Management")
        print("6. 📚 Help & Documentation")
        print("7. 🚪 Exit")
        print("=" * 30)

    def test_strategy_comprehensive(self):
        """🎯 Run comprehensive multi-asset strategy testing"""
        print("\n🎯 COMPREHENSIVE STRATEGY TESTING")
        print("=" * 40)

        print("Available strategies:")
        for i, strategy in enumerate(self.strategies, 1):
            print(f"{i}. {strategy}")

        try:
            choice = input("\nSelect strategy (1-3): ").strip()
            strategy_idx = int(choice) - 1

            if 0 <= strategy_idx < len(self.strategies):
                strategy_name = self.strategies[strategy_idx]
                print(f"\n🚀 Launching comprehensive test for {strategy_name}")
                print("⚠️ This will test across ALL available assets...")

                confirm = input("Continue? (y/n): ").lower().strip()
                if confirm == 'y':
                    # Run universal tester
                    cmd = [
                        'python3',
                        f'{self.project_root}/strategies/testing/universal_tester.py',
                        strategy_name
                    ]

                    print("🚀 Executing comprehensive test...")
                    result = subprocess.run(cmd, cwd=self.project_root)

                    if result.returncode == 0:
                        print("✅ Comprehensive test completed successfully")
                        self.session_log.append(f"Tested {strategy_name} comprehensively")
                    else:
                        print("❌ Test failed - check output above")
                else:
                    print("Test cancelled")
            else:
                print("❌ Invalid selection")

        except ValueError:
            print("❌ Invalid input - please enter a number")
        except KeyboardInterrupt:
            print("\n⚠️ Test interrupted by user")

    def quick_single_test(self):
        """⚡ Quick single asset test"""
        print("\n⚡ QUICK SINGLE ASSET TEST")
        print("=" * 30)

        # Show available CSV files
        data_dir = f"{self.project_root}/dataset_files"
        csv_files = []

        if os.path.exists(data_dir):
            for root, dirs, files in os.walk(data_dir):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))

        if not csv_files:
            print("❌ No CSV data files found")
            return

        print(f"Found {len(csv_files)} data files")
        print("Using first available file for quick test...")

        # Select strategy
        print("\nSelect strategy:")
        for i, strategy in enumerate(self.strategies, 1):
            print(f"{i}. {strategy}")

        try:
            choice = input("Strategy (1-3): ").strip()
            strategy_idx = int(choice) - 1

            if 0 <= strategy_idx < len(self.strategies):
                strategy_name = self.strategies[strategy_idx]

                print(f"\n🚀 Quick testing {strategy_name} on sample data...")

                # Create a minimal test script call
                test_script = f"""
import sys
sys.path.append('{self.project_root}')
sys.path.append('{self.project_root}/strategies/core_strategies')

from strategies.testing.universal_tester import UniversalStrategyTester

tester = UniversalStrategyTester()
if tester.data_sources:
    result = tester.test_strategy_single_asset('{strategy_name}', tester.data_sources[0])
    if result:
        print("✅ Quick test completed successfully")
    else:
        print("❌ Quick test failed")
else:
    print("❌ No data sources available")
"""

                with open('/tmp/quick_test.py', 'w') as f:
                    f.write(test_script)

                result = subprocess.run(['python3', '/tmp/quick_test.py'], cwd=self.project_root)

                if result.returncode == 0:
                    self.session_log.append(f"Quick tested {strategy_name}")

        except ValueError:
            print("❌ Invalid input")
        except Exception as e:
            print(f"❌ Error: {e}")

    def view_recent_results(self):
        """📊 View recent test results"""
        print("\n📊 RECENT RESULTS")
        print("=" * 20)

        results_dir = f"{self.project_root}/strategies/results"
        if not os.path.exists(results_dir):
            print("❌ Results directory not found")
            return

        # Get recent CSV files
        csv_files = []
        for file in os.listdir(results_dir):
            if file.endswith('.csv'):
                filepath = os.path.join(results_dir, file)
                mtime = os.path.getmtime(filepath)
                csv_files.append((file, mtime))

        # Sort by modification time (newest first)
        csv_files.sort(key=lambda x: x[1], reverse=True)

        if csv_files:
            print(f"📈 Found {len(csv_files)} result files:")
            for i, (filename, mtime) in enumerate(csv_files[:10], 1):
                date_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                print(f"{i:2}. {filename} ({date_str})")

            try:
                choice = input(f"\nView file (1-{min(10, len(csv_files))}) or Enter to skip: ").strip()
                if choice and choice.isdigit():
                    idx = int(choice) - 1
                    if 0 <= idx < len(csv_files):
                        filepath = os.path.join(results_dir, csv_files[idx][0])
                        print(f"\n📋 Preview of {csv_files[idx][0]}:")

                        import pandas as pd
                        df = pd.read_csv(filepath)
                        print(df.head())

            except Exception as e:
                print(f"❌ Error reading file: {e}")
        else:
            print("📭 No result files found")

    def archive_management(self):
        """🗄️ Archive management"""
        print("\n🗄️ ARCHIVE MANAGEMENT")
        print("=" * 25)

        archive_dir = f"{self.project_root}/archive"
        if os.path.exists(archive_dir):
            size_mb = sum(os.path.getsize(os.path.join(dirpath, filename))
                         for dirpath, dirnames, filenames in os.walk(archive_dir)
                         for filename in filenames) / (1024 * 1024)

            print(f"📦 Archive size: {size_mb:.1f} MB")
            print("📁 Archive contains non-essential files")
            print("   • data-streams/ (8.7GB real-time data)")
            print("   • strategies_backup/ (redundant strategies)")
            print("   • old analysis and test files")

            print("\n🗑️ Management options:")
            print("1. View archive contents")
            print("2. Clean old real-time data (recommended)")
            print("3. Return to main menu")

            choice = input("Select (1-3): ").strip()

            if choice == "1":
                print("\n📁 Archive contents:")
                for item in os.listdir(archive_dir):
                    item_path = os.path.join(archive_dir, item)
                    if os.path.isdir(item_path):
                        print(f"   📁 {item}/")
                    else:
                        print(f"   📄 {item}")

            elif choice == "2":
                print("\n🗑️ This would remove old real-time data logs")
                print("⚠️ Archive management coming soon...")

        else:
            print("📭 No archive directory found")

    def show_help(self):
        """📚 Show help and documentation"""
        print("\n📚 HELP & DOCUMENTATION")
        print("=" * 30)
        print("🎯 Strategy Types:")
        print("   • SMA Crossover: Trend following with moving averages")
        print("   • RSI Mean Reversion: Counter-trend using RSI levels")
        print("   • Breakout Momentum: Volume-confirmed breakouts")

        print("\n📊 Testing Process:")
        print("   1. Data quality validation (≥75 score required)")
        print("   2. Strategy execution on historical data")
        print("   3. Complete backtesting.py stats display")
        print("   4. Performance ranking across assets")
        print("   5. Results saved to CSV with timestamp")

        print("\n🔧 System Features:")
        print("   • Single process execution (no conflicts)")
        print("   • Comprehensive multi-asset testing")
        print("   • Production readiness validation")
        print("   • Automated results archiving")

        print("\n📁 Directory Structure:")
        print("   • strategies/core_strategies/ - 3 proven strategies")
        print("   • strategies/testing/ - universal tester")
        print("   • strategies/results/ - test results")
        print("   • dataset_files/ - validated data (BTC, ETH, etc.)")
        print("   • trading_functions/ - production library")

        print("\n💡 Tips:")
        print("   • Start with quick single asset test")
        print("   • Use comprehensive testing for full evaluation")
        print("   • Check system health if issues occur")
        print("   • Results are timestamped for tracking")

    def run(self):
        """🚀 Main system loop"""
        self.show_banner()

        # Initial health check
        if not self.check_system_health():
            print("\n⚠️ System health issues detected")
            print("Continuing anyway, but some features may not work properly")

        while True:
            try:
                self.show_main_menu()
                choice = input("\nSelect option (1-7): ").strip()

                if choice == "1":
                    self.test_strategy_comprehensive()
                elif choice == "2":
                    self.quick_single_test()
                elif choice == "3":
                    self.check_system_health()
                elif choice == "4":
                    self.view_recent_results()
                elif choice == "5":
                    self.archive_management()
                elif choice == "6":
                    self.show_help()
                elif choice == "7":
                    print("\n👋 Goodbye! Session log:")
                    for log_entry in self.session_log:
                        print(f"   ✅ {log_entry}")
                    break
                else:
                    print("❌ Invalid choice. Please select 1-7.")

                input("\nPress Enter to continue...")

            except KeyboardInterrupt:
                print("\n\n👋 Interrupted by user. Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Unexpected error: {e}")
                print("Continuing...")

def main():
    """🎯 Entry point"""
    system = AlgoTradingSystem()
    system.run()

if __name__ == "__main__":
    main()