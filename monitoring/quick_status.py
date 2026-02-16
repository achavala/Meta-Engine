#!/usr/bin/env python3
"""
Quick Status Check
==================
Quick overview of current system performance vs. baseline.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from monitoring.validation_monitor import ValidationMonitor, BASELINE_METRICS


def main():
    """Quick status check."""
    monitor = ValidationMonitor()
    
    print("=" * 60)
    print("QUICK STATUS CHECK")
    print("=" * 60)
    print()
    
    # Trade Execution
    execution = monitor.analyze_trade_execution(days=7)
    exec_rate = execution["execution_rate"] * 100
    exec_vs_baseline = execution["vs_baseline"] * 100
    
    print(f"📊 Trade Execution Rate: {exec_rate:.1f}%")
    print(f"   Baseline: {BASELINE_METRICS['trade_execution_rate']*100:.1f}%")
    print(f"   Change: {exec_vs_baseline:+.1f}%")
    if exec_vs_baseline > 20:
        print(f"   Status: ✅ Excellent improvement!")
    elif exec_vs_baseline > 0:
        print(f"   Status: ✅ Improving")
    else:
        print(f"   Status: ⚠️  Needs attention")
    print()
    
    # Performance
    performance = monitor.analyze_performance_metrics(days=7)
    if performance["executed_trades"] > 0:
        win_rate = performance["win_rate"] * 100
        win_vs_baseline = performance["vs_baseline_win_rate"] * 100
        
        print(f"🎯 Win Rate: {win_rate:.1f}%")
        print(f"   Baseline: {BASELINE_METRICS['win_rate']*100:.1f}%")
        print(f"   Change: {win_vs_baseline:+.1f}%")
        if win_vs_baseline > 3:
            print(f"   Status: ✅ Excellent improvement!")
        elif win_vs_baseline > 0:
            print(f"   Status: ✅ Improving")
        else:
            print(f"   Status: ⚠️  Needs attention")
        print()
        
        print(f"💰 Average Returns:")
        print(f"   Winners: {performance['avg_winner_return']:+.1f}%")
        print(f"   Losers: {performance['avg_loser_return']:.1f}%")
        print()
    else:
        print("⚠️  No executed trades in last 7 days")
        print()
    
    # Selection Gates
    gates = monitor.analyze_selection_gates(days=7)
    print(f"🚫 Selection Gates:")
    print(f"   Filtered: {gates['total_filtered']} candidates")
    if gates['total_filtered'] > 0:
        print(f"   Status: ✅ Active")
    else:
        print(f"   Status: ⚠️  No filtering detected")
    print()
    
    print("=" * 60)
    print()
    print("For detailed analysis, run:")
    print("  python3 monitoring/validation_monitor.py")
    print("  python3 monitoring/compare_performance.py")


if __name__ == "__main__":
    main()
