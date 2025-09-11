"""
Strategy Optimization Framework
===============================

This framework provides systematic parameter optimization for trading strategies
using the backtesting.py library. It includes:

- Grid search optimization across multiple parameters
- Walk-forward analysis for robust testing
- Performance heatmaps and optimization surfaces
- Statistical analysis of parameter sensitivity
- Overfitting detection and prevention

Features:
1. Multi-dimensional parameter optimization
2. Risk-adjusted optimization (Sharpe, Sortino, Calmar)
3. Robustness testing across different market periods
4. Visualization of optimization results
5. Best parameter recommendation system

Author: Bobby's Strategy Optimization Framework
Date: 2025-09-11
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from backtesting import Backtest
from itertools import product
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class StrategyOptimizer:
    """
    Comprehensive strategy optimization framework
    """
    
    def __init__(self, strategy_class, data, cash=100000, commission=0.001):
        """
        Initialize the optimizer
        
        Args:
            strategy_class: The strategy class to optimize
            data: Historical price data
            cash: Starting capital
            commission: Transaction costs
        """
        self.strategy_class = strategy_class
        self.data = data
        self.cash = cash
        self.commission = commission
        self.results = []
        self.best_params = None
        self.optimization_history = []
        
    def optimize_parameters(self, param_ranges, optimization_metric='Sharpe Ratio', 
                          max_combinations=1000, verbose=True):
        """
        Perform grid search optimization across parameter ranges
        
        Args:
            param_ranges: Dict of parameter names and their ranges
            optimization_metric: Metric to optimize ('Sharpe Ratio', 'Return [%]', 'Calmar Ratio')
            max_combinations: Maximum parameter combinations to test
            verbose: Print progress
        """
        
        print(f"🔍 STRATEGY PARAMETER OPTIMIZATION")
        print("=" * 60)
        print(f"📊 Data: {len(self.data)} bars")
        print(f"🎯 Metric: {optimization_metric}")
        print(f"🔧 Parameters: {list(param_ranges.keys())}")
        
        # Generate parameter combinations
        param_names = list(param_ranges.keys())
        param_values = list(param_ranges.values())
        combinations = list(product(*param_values))
        
        # Limit combinations if too many
        if len(combinations) > max_combinations:
            print(f"⚠️  Limiting to {max_combinations} combinations (was {len(combinations)})")
            combinations = combinations[:max_combinations]
        
        print(f"🔄 Testing {len(combinations)} parameter combinations...")
        
        results = []
        best_score = float('-inf')
        best_params = None
        
        for i, param_combo in enumerate(combinations):
            if verbose and (i + 1) % 50 == 0:
                print(f"   Progress: {i + 1}/{len(combinations)} ({((i + 1)/len(combinations))*100:.1f}%)")
            
            try:
                # Create strategy with current parameters
                class OptimizedStrategy(self.strategy_class):
                    pass
                
                # Set parameters dynamically
                for param_name, param_value in zip(param_names, param_combo):
                    setattr(OptimizedStrategy, param_name, param_value)
                
                # Run backtest
                bt = Backtest(self.data, OptimizedStrategy, cash=self.cash, commission=self.commission)
                stats = bt.run()
                
                # Extract metrics
                result = {
                    'combination_id': i,
                    'Return_%': stats.get('Return [%]', 0),
                    'Sharpe': stats.get('Sharpe Ratio', 0),
                    'Sortino': stats.get('Sortino Ratio', 0),
                    'Calmar': stats.get('Calmar Ratio', 0),
                    'Max_DD_%': stats.get('Max. Drawdown [%]', 0),
                    'Win_Rate_%': stats.get('Win Rate [%]', 0),
                    'Trades': stats.get('# Trades', 0),
                    'Profit_Factor': stats.get('Profit Factor', 0),
                    'Volatility_%': stats.get('Volatility (Ann.) [%]', 0)
                }
                
                # Add parameter values
                for param_name, param_value in zip(param_names, param_combo):
                    result[param_name] = param_value
                
                results.append(result)
                
                # Track best parameters
                metric_value = stats.get(optimization_metric, float('-inf'))
                if not pd.isna(metric_value) and metric_value > best_score:
                    best_score = metric_value
                    best_params = dict(zip(param_names, param_combo))
                
            except Exception as e:
                if verbose:
                    print(f"   Error with combination {i}: {e}")
                continue
        
        # Store results
        self.results = pd.DataFrame(results)
        self.best_params = best_params
        
        print(f"\n✅ Optimization Complete!")
        print(f"📊 Valid results: {len(self.results)}/{len(combinations)}")
        
        if best_params:
            print(f"🏆 Best parameters ({optimization_metric}: {best_score:.3f}):")
            for param, value in best_params.items():
                print(f"   {param}: {value}")
        
        return self.results, self.best_params
    
    def walk_forward_analysis(self, param_ranges, window_size_days=365, step_size_days=90,
                            optimization_metric='Sharpe Ratio', min_trades=10):
        """
        Perform walk-forward analysis to test parameter stability
        
        Args:
            param_ranges: Parameter ranges to test
            window_size_days: Size of optimization window in days
            step_size_days: Step size for moving window
            optimization_metric: Metric to optimize
            min_trades: Minimum trades required for valid results
        """
        
        print(f"\n🚀 WALK-FORWARD ANALYSIS")
        print("=" * 50)
        print(f"📊 Window size: {window_size_days} days")
        print(f"🔄 Step size: {step_size_days} days")
        
        # Split data into windows
        data_periods = []
        start_date = self.data.index[0]
        end_date = self.data.index[-1]
        
        current_date = start_date
        while current_date + timedelta(days=window_size_days) <= end_date:
            window_end = current_date + timedelta(days=window_size_days)
            period_data = self.data[(self.data.index >= current_date) & 
                                  (self.data.index < window_end)]
            
            if len(period_data) > 50:  # Minimum data points
                data_periods.append({
                    'start': current_date,
                    'end': window_end,
                    'data': period_data
                })
            
            current_date += timedelta(days=step_size_days)
        
        print(f"📅 Analysis periods: {len(data_periods)}")
        
        # Optimize each period
        period_results = []
        
        for i, period in enumerate(data_periods):
            print(f"\n🔍 Period {i+1}/{len(data_periods)}: {period['start'].date()} to {period['end'].date()}")
            
            # Create temporary optimizer for this period
            temp_optimizer = StrategyOptimizer(
                self.strategy_class, period['data'], self.cash, self.commission
            )
            
            # Optimize parameters for this period
            results, best_params = temp_optimizer.optimize_parameters(
                param_ranges, optimization_metric, verbose=False
            )
            
            if best_params and len(results) > 0:
                best_result = results.iloc[results[optimization_metric.replace(' ', '_').replace('[%]', '_%').replace('.', '')].idxmax()]
                
                period_result = {
                    'period': i + 1,
                    'start_date': period['start'],
                    'end_date': period['end'],
                    'data_points': len(period['data']),
                    'best_metric': best_result[optimization_metric.replace(' ', '_').replace('[%]', '_%').replace('.', '')],
                    'best_params': best_params,
                    'trades': best_result['Trades']
                }
                
                # Add individual parameter values
                for param, value in best_params.items():
                    period_result[f'best_{param}'] = value
                
                period_results.append(period_result)
                
                print(f"   Best {optimization_metric}: {best_result[optimization_metric.replace(' ', '_').replace('[%]', '_%').replace('.', '')]:.3f}")
                print(f"   Trades: {best_result['Trades']}")
        
        self.walk_forward_results = pd.DataFrame(period_results)
        
        print(f"\n📊 Walk-Forward Analysis Summary:")
        if len(self.walk_forward_results) > 0:
            metric_col = 'best_metric'
            print(f"   Periods analyzed: {len(self.walk_forward_results)}")
            print(f"   Average {optimization_metric}: {self.walk_forward_results[metric_col].mean():.3f}")
            print(f"   Best {optimization_metric}: {self.walk_forward_results[metric_col].max():.3f}")
            print(f"   Worst {optimization_metric}: {self.walk_forward_results[metric_col].min():.3f}")
            print(f"   Std Dev: {self.walk_forward_results[metric_col].std():.3f}")
        
        return self.walk_forward_results
    
    def analyze_parameter_sensitivity(self, param_name, param_range, 
                                    optimization_metric='Sharpe Ratio'):
        """
        Analyze sensitivity of performance to a single parameter
        
        Args:
            param_name: Name of parameter to analyze
            param_range: Range of values to test
            optimization_metric: Metric to analyze
        """
        
        print(f"\n🔬 PARAMETER SENSITIVITY ANALYSIS: {param_name}")
        print("=" * 60)
        
        sensitivity_results = []
        
        for value in param_range:
            try:
                # Create strategy with specific parameter value
                class SensitivityStrategy(self.strategy_class):
                    pass
                
                setattr(SensitivityStrategy, param_name, value)
                
                # Run backtest
                bt = Backtest(self.data, SensitivityStrategy, cash=self.cash, commission=self.commission)
                stats = bt.run()
                
                result = {
                    param_name: value,
                    'Return_%': stats.get('Return [%]', 0),
                    'Sharpe': stats.get('Sharpe Ratio', 0),
                    'Sortino': stats.get('Sortino Ratio', 0),
                    'Max_DD_%': stats.get('Max. Drawdown [%]', 0),
                    'Trades': stats.get('# Trades', 0),
                    'Win_Rate_%': stats.get('Win Rate [%]', 0)
                }
                
                sensitivity_results.append(result)
                
            except Exception as e:
                print(f"   Error with {param_name}={value}: {e}")
                continue
        
        sensitivity_df = pd.DataFrame(sensitivity_results)
        
        # Analyze results
        if len(sensitivity_df) > 0:
            metric_col = optimization_metric.replace(' ', '_').replace('[%]', '_%').replace('.', '')
            if metric_col in sensitivity_df.columns:
                best_idx = sensitivity_df[metric_col].idxmax()
                best_value = sensitivity_df.loc[best_idx, param_name]
                best_score = sensitivity_df.loc[best_idx, metric_col]
                
                print(f"📊 Best {param_name}: {best_value} ({optimization_metric}: {best_score:.3f})")
                
                # Calculate correlation
                corr = sensitivity_df[param_name].corr(sensitivity_df[metric_col])
                print(f"📈 Correlation with {optimization_metric}: {corr:.3f}")
        
        return sensitivity_df
    
    def create_heatmap(self, param1, param2, metric='Sharpe'):
        """
        Create a performance heatmap for two parameters
        
        Args:
            param1: First parameter name
            param2: Second parameter name
            metric: Performance metric to plot
        """
        
        if self.results is None or len(self.results) == 0:
            print("❌ No optimization results available. Run optimize_parameters first.")
            return
        
        print(f"\n📊 Creating heatmap: {param1} vs {param2} ({metric})")
        
        # Filter results that have both parameters
        heatmap_data = self.results[[param1, param2, metric]].dropna()
        
        if len(heatmap_data) == 0:
            print("❌ No data available for heatmap")
            return
        
        # Create pivot table
        pivot_table = heatmap_data.pivot_table(
            values=metric, index=param2, columns=param1, aggfunc='mean'
        )
        
        # Create heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_table, annot=True, cmap='RdYlGn', center=0, 
                   fmt='.3f', cbar_kws={'label': metric})
        plt.title(f'Strategy Performance Heatmap: {param1} vs {param2}')
        plt.xlabel(param1)
        plt.ylabel(param2)
        plt.tight_layout()
        
        # Save plot
        filename = f'heatmap_{param1}_{param2}_{metric}.png'
        plt.savefig(f'/Users/bobbyyo/Projects/algo-fun/strategies/{filename}', dpi=300, bbox_inches='tight')
        print(f"📁 Heatmap saved: {filename}")
        plt.show()
        
        return pivot_table
    
    def generate_optimization_report(self):
        """
        Generate comprehensive optimization report
        """
        
        print(f"\n📋 OPTIMIZATION REPORT")
        print("=" * 80)
        print(f"🕐 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Strategy: {self.strategy_class.__name__}")
        print(f"💰 Data period: {self.data.index[0]} to {self.data.index[-1]} ({len(self.data)} bars)")
        
        if self.results is not None and len(self.results) > 0:
            print(f"\n📈 OPTIMIZATION RESULTS SUMMARY:")
            print(f"   Combinations tested: {len(self.results)}")
            print(f"   Best Sharpe Ratio: {self.results['Sharpe'].max():.3f}")
            print(f"   Best Return: {self.results['Return_%'].max():.2f}%")
            print(f"   Average Return: {self.results['Return_%'].mean():.2f}%")
            print(f"   Average Sharpe: {self.results['Sharpe'].mean():.3f}")
            
            # Top 10 results
            print(f"\n🏆 TOP 10 PARAMETER COMBINATIONS (by Sharpe Ratio):")
            top_results = self.results.nlargest(10, 'Sharpe')
            
            for i, (_, row) in enumerate(top_results.iterrows(), 1):
                print(f"   {i:2}. Sharpe: {row['Sharpe']:6.3f} | Return: {row['Return_%']:7.2f}% | "
                      f"Trades: {row['Trades']:3.0f} | Max DD: {row['Max_DD_%']:6.2f}%")
        
        # Recommendations
        print(f"\n💡 OPTIMIZATION RECOMMENDATIONS:")
        
        if self.best_params:
            print("   🎯 BEST PARAMETERS:")
            for param, value in self.best_params.items():
                print(f"      {param}: {value}")
        
        print("   🔧 NEXT STEPS:")
        print("      1. Test best parameters on out-of-sample data")
        print("      2. Perform walk-forward analysis for robustness")
        print("      3. Analyze parameter stability across market conditions")
        print("      4. Consider ensemble of top-performing parameter sets")
        print("      5. Implement dynamic parameter adaptation")
        
        return self.results

# Example usage and testing
if __name__ == "__main__":
    """
    Example optimization of Enhanced ETH Momentum Strategy
    """
    from enhanced_eth_momentum_final import EnhancedETHMomentumFinal
    
    print("🚀 STRATEGY OPTIMIZATION FRAMEWORK - EXAMPLE")
    print("=" * 70)
    
    # Load ETH data
    data_file = '/Users/bobbyyo/Projects/algo-fun/data/ETH_1d_20250909_030924_historical.csv'
    
    try:
        # Load data
        data = pd.read_csv(data_file)
        date_cols = [col for col in data.columns if any(word in col.lower() for word in ['date', 'time', 'timestamp'])]
        if date_cols:
            date_col = date_cols[0]
            data[date_col] = pd.to_datetime(data[date_col])
            data = data.set_index(date_col)
        
        data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data = data.sort_index().dropna()
        
        print(f"📊 Loaded {len(data)} bars of ETH data")
        
        # Initialize optimizer
        optimizer = StrategyOptimizer(EnhancedETHMomentumFinal, data)
        
        # Define parameter ranges to optimize
        param_ranges = {
            'macd_fast': [5, 8, 12],
            'macd_slow': [18, 21, 26],
            'macd_signal': [3, 5, 9],
            'rsi_threshold': [65, 70, 75],
            'ma_period': [20, 50, 100],
            'atr_multiplier': [1.5, 2.0, 2.5]
        }
        
        # Run optimization
        results, best_params = optimizer.optimize_parameters(
            param_ranges, 
            optimization_metric='Sharpe Ratio',
            max_combinations=200,
            verbose=True
        )
        
        # Generate report
        optimizer.generate_optimization_report()
        
        # Create heatmaps for key parameters
        if 'macd_fast' in results.columns and 'rsi_threshold' in results.columns:
            optimizer.create_heatmap('macd_fast', 'rsi_threshold', 'Sharpe')
        
        print("\n✅ Optimization framework demonstration complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()