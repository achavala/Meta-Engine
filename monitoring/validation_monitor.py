#!/usr/bin/env python3
"""
Validation Monitor for New Code Performance
===========================================
Tracks and validates the impact of new code fixes:
- Trade execution rate (retry logic)
- Selection gate filtering
- ORM score improvements
- Win rate comparisons
- Average return comparisons

Runs automatically after each scan or can be run manually.
"""

import sys
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import pytz

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from trading.trade_db import TradeDB
from config import MetaConfig

EST = pytz.timezone("US/Eastern")

# Baseline metrics from Feb 9-12, 2026 analysis
BASELINE_METRICS = {
    "trade_execution_rate": 0.367,  # 36.7%
    "win_rate": 0.083,  # 8.3%
    "avg_winner_return": 189.6,  # +189.6%
    "avg_loser_return": -12.3,  # -12.3%
    "total_picks": 60,
    "winners": 5,
    "losers": 34,
    "no_trade_executed": 38,
}


class ValidationMonitor:
    """Monitors and validates new code performance against baseline."""

    def __init__(self):
        self.db = TradeDB()
        self.log_dir = Path(__file__).parent.parent / "logs"
        self.output_dir = Path(__file__).parent.parent / "output"
        self.reports_dir = Path(__file__).parent.parent / "monitoring" / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def get_recent_scans(self, days: int = 7) -> List[Dict]:
        """Get recent scan data from cross_analysis files."""
        scans = []
        cutoff = datetime.now(EST) - timedelta(days=days)
        
        for cross_file in sorted(self.output_dir.glob("cross_analysis_*.json")):
            try:
                with open(cross_file) as f:
                    data = json.load(f)
                
                ts = data.get("timestamp", "")
                if not ts:
                    continue
                
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00") if "Z" in ts else ts)
                if dt.tzinfo:
                    dt = dt.astimezone(EST)
                else:
                    dt = EST.localize(dt)
                
                if dt >= cutoff:
                    scans.append({
                        "file": cross_file.name,
                        "timestamp": ts,
                        "datetime": dt,
                        "data": data,
                    })
            except Exception as e:
                print(f"  ⚠️  Error loading {cross_file.name}: {e}")
        
        return sorted(scans, key=lambda x: x["datetime"])

    def analyze_trade_execution(self, days: int = 7) -> Dict[str, Any]:
        """Analyze trade execution rate and retry attempts."""
        cutoff = (datetime.now(EST) - timedelta(days=days)).date()
        
        # Get all trades from recent period
        all_trades = []
        for date_offset in range(days):
            scan_date = (cutoff + timedelta(days=date_offset)).isoformat()
            trades = self.db.get_trades_by_date(scan_date)
            all_trades.extend(trades)
        
        if not all_trades:
            return {
                "total_trades": 0,
                "execution_rate": 0.0,
                "filled": 0,
                "pending": 0,
                "cancelled": 0,
                "retry_attempts": 0,
                "vs_baseline": 0.0,
            }
        
        # Count by status
        filled = sum(1 for t in all_trades if t.get("status") == "filled")
        pending = sum(1 for t in all_trades if t.get("status") == "pending")
        cancelled = sum(1 for t in all_trades if t.get("status") == "cancelled")
        total = len(all_trades)
        
        execution_rate = (filled / total) if total > 0 else 0.0
        
        # Check for retry attempts in exit_reason
        retry_attempts = sum(
            1 for t in all_trades
            if "retry" in str(t.get("exit_reason", "")).lower() or
               "attempt" in str(t.get("exit_reason", "")).lower()
        )
        
        return {
            "total_trades": total,
            "execution_rate": execution_rate,
            "filled": filled,
            "pending": pending,
            "cancelled": cancelled,
            "retry_attempts": retry_attempts,
            "vs_baseline": execution_rate - BASELINE_METRICS["trade_execution_rate"],
        }

    def analyze_selection_gates(self, days: int = 7) -> Dict[str, Any]:
        """Analyze selection gate filtering from logs."""
        gate_stats = {
            "total_filtered": 0,
            "filtered_by_orm": 0,
            "filtered_by_signals": 0,
            "filtered_by_score": 0,
            "scans_analyzed": 0,
        }
        
        # Parse log files for selection gate messages
        log_files = sorted(self.log_dir.glob("*.log"), reverse=True)
        cutoff = datetime.now(EST) - timedelta(days=days)
        
        for log_file in log_files[:10]:  # Check last 10 log files
            try:
                # Check file modification time
                if datetime.fromtimestamp(log_file.stat().st_mtime, EST) < cutoff:
                    continue
                
                with open(log_file) as f:
                    content = f.read()
                    
                    # Look for selection gate messages
                    if "Top 10 Selection Gates" in content:
                        gate_stats["scans_analyzed"] += 1
                        
                        # Count filtered candidates
                        lines = content.split("\n")
                        for line in lines:
                            if "filtered out" in line.lower():
                                gate_stats["total_filtered"] += 1
                            if "orm" in line.lower() and ("filtered" in line.lower() or "<" in line):
                                gate_stats["filtered_by_orm"] += 1
                            if "signals" in line.lower() and ("filtered" in line.lower() or "<" in line):
                                gate_stats["filtered_by_signals"] += 1
                            if "base score" in line.lower() and ("filtered" in line.lower() or "<" in line):
                                gate_stats["filtered_by_score"] += 1
            except Exception as e:
                continue
        
        return gate_stats

    def analyze_orm_scores(self, days: int = 7) -> Dict[str, Any]:
        """Analyze ORM scores for recent picks."""
        scans = self.get_recent_scans(days)
        
        orm_scores = []
        orm_scores_winners = []
        orm_scores_losers = []
        
        for scan in scans:
            data = scan["data"]
            
            # Get picks from both engines
            puts_picks = data.get("puts_through_moonshot", [])[:10]
            moonshot_picks = data.get("moonshot_through_puts", [])[:10]
            
            for pick in puts_picks + moonshot_picks:
                orm = pick.get("_orm_score", 0)
                if orm > 0:
                    orm_scores.append(orm)
                    
                    # Check if this pick was a winner or loser
                    symbol = pick.get("symbol", "")
                    option_type = "put" if pick in puts_picks else "call"
                    
                    # Get trade data
                    scan_date = scan["datetime"].date().isoformat()
                    trades = self.db.get_trades_by_date(scan_date)
                    
                    for trade in trades:
                        if (trade.get("symbol") == symbol and
                            trade.get("option_type") == option_type):
                            pnl_pct = float(trade.get("pnl_pct", 0) or 0)
                            if pnl_pct >= 50:
                                orm_scores_winners.append(orm)
                            elif pnl_pct < 0:
                                orm_scores_losers.append(orm)
                            break
        
        if not orm_scores:
            return {
                "avg_orm": 0.0,
                "avg_orm_winners": 0.0,
                "avg_orm_losers": 0.0,
                "orm_ge_070_pct": 0.0,
            }
        
        avg_orm = sum(orm_scores) / len(orm_scores)
        avg_orm_winners = sum(orm_scores_winners) / len(orm_scores_winners) if orm_scores_winners else 0.0
        avg_orm_losers = sum(orm_scores_losers) / len(orm_scores_losers) if orm_scores_losers else 0.0
        orm_ge_070 = sum(1 for o in orm_scores if o >= 0.70) / len(orm_scores) * 100
        
        return {
            "avg_orm": avg_orm,
            "avg_orm_winners": avg_orm_winners,
            "avg_orm_losers": avg_orm_losers,
            "orm_ge_070_pct": orm_ge_070,
            "total_picks_with_orm": len(orm_scores),
        }

    def analyze_performance_metrics(self, days: int = 7) -> Dict[str, Any]:
        """Analyze win rate and average returns."""
        cutoff = (datetime.now(EST) - timedelta(days=days)).date()
        
        all_trades = []
        for date_offset in range(days):
            scan_date = (cutoff + timedelta(days=date_offset)).isoformat()
            trades = self.db.get_trades_by_date(scan_date)
            all_trades.extend(trades)
        
        if not all_trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "avg_winner_return": 0.0,
                "avg_loser_return": 0.0,
                "winners": 0,
                "losers": 0,
            }
        
        # Filter to filled/closed trades only
        executed_trades = [t for t in all_trades if t.get("status") in ["filled", "open", "closed"]]
        
        if not executed_trades:
            return {
                "total_trades": len(all_trades),
                "executed_trades": 0,
                "win_rate": 0.0,
                "avg_winner_return": 0.0,
                "avg_loser_return": 0.0,
                "winners": 0,
                "losers": 0,
            }
        
        winners = [t for t in executed_trades if float(t.get("pnl_pct", 0) or 0) >= 50]
        losers = [t for t in executed_trades if float(t.get("pnl_pct", 0) or 0) < 0]
        
        win_rate = len(winners) / len(executed_trades) if executed_trades else 0.0
        
        avg_winner_return = (
            sum(float(t.get("pnl_pct", 0) or 0) for t in winners) / len(winners)
            if winners else 0.0
        )
        
        avg_loser_return = (
            sum(float(t.get("pnl_pct", 0) or 0) for t in losers) / len(losers)
            if losers else 0.0
        )
        
        return {
            "total_trades": len(all_trades),
            "executed_trades": len(executed_trades),
            "win_rate": win_rate,
            "avg_winner_return": avg_winner_return,
            "avg_loser_return": avg_loser_return,
            "winners": len(winners),
            "losers": len(losers),
            "vs_baseline_win_rate": win_rate - BASELINE_METRICS["win_rate"],
            "vs_baseline_avg_winner": avg_winner_return - BASELINE_METRICS["avg_winner_return"],
        }

    def generate_validation_report(self, days: int = 7) -> Dict[str, Any]:
        """Generate comprehensive validation report."""
        print("=" * 80)
        print("VALIDATION MONITOR - NEW CODE PERFORMANCE")
        print("=" * 80)
        print()
        
        # 1. Trade Execution Analysis
        print("📊 Trade Execution Analysis...")
        execution = self.analyze_trade_execution(days)
        print(f"   Execution Rate: {execution['execution_rate']*100:.1f}% "
              f"(Baseline: {BASELINE_METRICS['trade_execution_rate']*100:.1f}%)")
        print(f"   Filled: {execution['filled']} | Pending: {execution['pending']} | "
              f"Cancelled: {execution['cancelled']}")
        print(f"   Retry Attempts: {execution['retry_attempts']}")
        print()
        
        # 2. Selection Gates Analysis
        print("🚫 Selection Gates Analysis...")
        gates = self.analyze_selection_gates(days)
        print(f"   Total Filtered: {gates['total_filtered']}")
        print(f"   By ORM: {gates['filtered_by_orm']} | "
              f"By Signals: {gates['filtered_by_signals']} | "
              f"By Score: {gates['filtered_by_score']}")
        print(f"   Scans Analyzed: {gates['scans_analyzed']}")
        print()
        
        # 3. ORM Scores Analysis
        print("📈 ORM Scores Analysis...")
        orm = self.analyze_orm_scores(days)
        print(f"   Average ORM: {orm['avg_orm']:.3f}")
        print(f"   Winners ORM: {orm['avg_orm_winners']:.3f} | "
              f"Losers ORM: {orm['avg_orm_losers']:.3f}")
        print(f"   ORM ≥ 0.70: {orm['orm_ge_070_pct']:.1f}%")
        print()
        
        # 4. Performance Metrics
        print("🎯 Performance Metrics...")
        performance = self.analyze_performance_metrics(days)
        print(f"   Win Rate: {performance['win_rate']*100:.1f}% "
              f"(Baseline: {BASELINE_METRICS['win_rate']*100:.1f}%)")
        print(f"   Avg Winner Return: {performance['avg_winner_return']:+.1f}% "
              f"(Baseline: {BASELINE_METRICS['avg_winner_return']:+.1f}%)")
        print(f"   Avg Loser Return: {performance['avg_loser_return']:.1f}% "
              f"(Baseline: {BASELINE_METRICS['avg_loser_return']:.1f}%)")
        print(f"   Winners: {performance['winners']} | Losers: {performance['losers']}")
        print()
        
        # Compile report
        report = {
            "timestamp": datetime.now(EST).isoformat(),
            "analysis_period_days": days,
            "baseline_metrics": BASELINE_METRICS,
            "trade_execution": execution,
            "selection_gates": gates,
            "orm_scores": orm,
            "performance": performance,
        }
        
        # Save report
        report_file = self.reports_dir / f"validation_report_{datetime.now(EST).strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)
        
        print(f"✅ Report saved: {report_file.name}")
        print()
        
        # Summary
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        
        improvements = []
        if execution["vs_baseline"] > 0:
            improvements.append(f"✅ Execution rate improved by {execution['vs_baseline']*100:.1f}%")
        else:
            improvements.append(f"⚠️  Execution rate decreased by {abs(execution['vs_baseline'])*100:.1f}%")
        
        if performance["vs_baseline_win_rate"] > 0:
            improvements.append(f"✅ Win rate improved by {performance['vs_baseline_win_rate']*100:.1f}%")
        else:
            improvements.append(f"⚠️  Win rate decreased by {abs(performance['vs_baseline_win_rate'])*100:.1f}%")
        
        if gates["total_filtered"] > 0:
            improvements.append(f"✅ Selection gates filtered {gates['total_filtered']} candidates")
        else:
            improvements.append(f"⚠️  No selection gate filtering detected")
        
        for imp in improvements:
            print(f"   {imp}")
        
        print()
        print("=" * 80)
        
        return report


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validation Monitor for New Code Performance")
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to analyze (default: 7)",
    )
    
    args = parser.parse_args()
    
    monitor = ValidationMonitor()
    report = monitor.generate_validation_report(days=args.days)
    
    return report


if __name__ == "__main__":
    main()
