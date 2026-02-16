#!/usr/bin/env python3
"""
Performance Comparison Tool
==========================
Compares current performance vs. baseline metrics and generates
detailed comparison reports with recommendations.
"""

import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from monitoring.validation_monitor import ValidationMonitor, BASELINE_METRICS


def generate_comparison_report(days: int = 7) -> Dict[str, Any]:
    """Generate detailed comparison report."""
    monitor = ValidationMonitor()
    
    # Get current metrics
    execution = monitor.analyze_trade_execution(days)
    gates = monitor.analyze_selection_gates(days)
    orm = monitor.analyze_orm_scores(days)
    performance = monitor.analyze_performance_metrics(days)
    
    # Calculate improvements
    execution_improvement = execution["execution_rate"] - BASELINE_METRICS["trade_execution_rate"]
    win_rate_improvement = performance["win_rate"] - BASELINE_METRICS["win_rate"]
    avg_winner_improvement = performance["avg_winner_return"] - BASELINE_METRICS["avg_winner_return"]
    avg_loser_improvement = performance["avg_loser_return"] - BASELINE_METRICS["avg_loser_return"]
    
    # Generate recommendations
    recommendations = []
    
    # Execution rate recommendations
    if execution_improvement < 0.10:  # Less than 10% improvement
        recommendations.append({
            "category": "Trade Execution",
            "priority": "HIGH",
            "issue": f"Execution rate only improved by {execution_improvement*100:.1f}% (target: +20-30%)",
            "action": "Investigate Alpaca API connection, check retry logic logs, verify account status",
        })
    elif execution_improvement >= 0.20:
        recommendations.append({
            "category": "Trade Execution",
            "priority": "INFO",
            "issue": f"Execution rate improved by {execution_improvement*100:.1f}% - Excellent!",
            "action": "Continue monitoring, consider optimizing further",
        })
    
    # Win rate recommendations
    if win_rate_improvement < 0.02:  # Less than 2% improvement
        recommendations.append({
            "category": "Win Rate",
            "priority": "MEDIUM",
            "issue": f"Win rate only improved by {win_rate_improvement*100:.1f}% (target: +3-5%)",
            "action": "Consider tightening selection gates (ORM ≥ 0.50, 3+ signals, base score ≥ 0.70)",
        })
    elif win_rate_improvement >= 0.04:
        recommendations.append({
            "category": "Win Rate",
            "priority": "INFO",
            "issue": f"Win rate improved by {win_rate_improvement*100:.1f}% - Excellent!",
            "action": "Selection gates are working well, continue monitoring",
        })
    
    # Selection gates recommendations
    if gates["total_filtered"] == 0:
        recommendations.append({
            "category": "Selection Gates",
            "priority": "HIGH",
            "issue": "No selection gate filtering detected",
            "action": "Verify selection gates are active in logs, check adapter code",
        })
    elif gates["total_filtered"] < 3:
        recommendations.append({
            "category": "Selection Gates",
            "priority": "MEDIUM",
            "issue": f"Only {gates['total_filtered']} candidates filtered (expected: 5-10)",
            "action": "Consider tightening thresholds or verify gates are working correctly",
        })
    
    # ORM recommendations
    if orm["orm_ge_070_pct"] < 50:
        recommendations.append({
            "category": "ORM Scores",
            "priority": "LOW",
            "issue": f"Only {orm['orm_ge_070_pct']:.1f}% of picks have ORM ≥ 0.70",
            "action": "Monitor if ORM enhancements are improving scores over time",
        })
    
    # Average return recommendations
    if avg_winner_improvement < -20:  # Decreased significantly
        recommendations.append({
            "category": "Average Returns",
            "priority": "MEDIUM",
            "issue": f"Average winner return decreased by {abs(avg_winner_improvement):.1f}%",
            "action": "Review ORM weight adjustments, may need to revert some changes",
        })
    
    # Compile report
    report = {
        "timestamp": datetime.now().isoformat(),
        "analysis_period_days": days,
        "baseline": BASELINE_METRICS,
        "current": {
            "trade_execution_rate": execution["execution_rate"],
            "win_rate": performance["win_rate"],
            "avg_winner_return": performance["avg_winner_return"],
            "avg_loser_return": performance["avg_loser_return"],
            "selection_gates_filtered": gates["total_filtered"],
            "orm_ge_070_pct": orm["orm_ge_070_pct"],
        },
        "improvements": {
            "execution_rate": execution_improvement,
            "win_rate": win_rate_improvement,
            "avg_winner_return": avg_winner_improvement,
            "avg_loser_return": avg_loser_improvement,
        },
        "recommendations": recommendations,
    }
    
    return report


def print_comparison_report(report: Dict[str, Any]):
    """Print formatted comparison report."""
    print("=" * 80)
    print("PERFORMANCE COMPARISON REPORT")
    print("=" * 80)
    print()
    
    print("📊 METRICS COMPARISON")
    print("-" * 80)
    
    # Execution Rate
    exec_imp = report["improvements"]["execution_rate"] * 100
    exec_status = "✅" if exec_imp > 0 else "⚠️"
    print(f"{exec_status} Trade Execution Rate:")
    print(f"   Baseline: {report['baseline']['trade_execution_rate']*100:.1f}%")
    print(f"   Current:  {report['current']['trade_execution_rate']*100:.1f}%")
    print(f"   Change:   {exec_imp:+.1f}%")
    print()
    
    # Win Rate
    win_imp = report["improvements"]["win_rate"] * 100
    win_status = "✅" if win_imp > 0 else "⚠️"
    print(f"{win_status} Win Rate:")
    print(f"   Baseline: {report['baseline']['win_rate']*100:.1f}%")
    print(f"   Current:  {report['current']['win_rate']*100:.1f}%")
    print(f"   Change:   {win_imp:+.1f}%")
    print()
    
    # Average Winner Return
    winner_imp = report["improvements"]["avg_winner_return"]
    winner_status = "✅" if winner_imp > -10 else "⚠️"
    print(f"{winner_status} Average Winner Return:")
    print(f"   Baseline: {report['baseline']['avg_winner_return']:+.1f}%")
    print(f"   Current:  {report['current']['avg_winner_return']:+.1f}%")
    print(f"   Change:   {winner_imp:+.1f}%")
    print()
    
    # Selection Gates
    print(f"🚫 Selection Gates:")
    print(f"   Candidates Filtered: {report['current']['selection_gates_filtered']}")
    print()
    
    # ORM Scores
    print(f"📈 ORM Scores:")
    print(f"   ORM ≥ 0.70: {report['current']['orm_ge_070_pct']:.1f}%")
    print()
    
    # Recommendations
    if report["recommendations"]:
        print("=" * 80)
        print("RECOMMENDATIONS")
        print("=" * 80)
        print()
        
        for i, rec in enumerate(report["recommendations"], 1):
            priority_emoji = {
                "HIGH": "🔴",
                "MEDIUM": "🟡",
                "LOW": "🟢",
                "INFO": "ℹ️",
            }.get(rec["priority"], "•")
            
            print(f"{i}. {priority_emoji} [{rec['priority']}] {rec['category']}")
            print(f"   Issue: {rec['issue']}")
            print(f"   Action: {rec['action']}")
            print()
    else:
        print("✅ No recommendations - all metrics are performing well!")
        print()
    
    print("=" * 80)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Performance Comparison Tool")
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to analyze (default: 7)",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save report to file",
    )
    
    args = parser.parse_args()
    
    report = generate_comparison_report(days=args.days)
    print_comparison_report(report)
    
    if args.save:
        monitor = ValidationMonitor()
        report_file = monitor.reports_dir / f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)
        print(f"✅ Report saved: {report_file.name}")


if __name__ == "__main__":
    main()
