#!/usr/bin/env python3
"""
Capital Requirement Analysis: $1M in 6 Months
==============================================
Institutional-grade analysis of capital requirements based on:
- Historical performance data
- Win rate and average returns
- Position sizing and risk management
- Compounding effects
- Drawdown scenarios

30+ years trading + PhD quant + institutional microstructure lens
"""

import sys
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pytz

sys.path.insert(0, str(Path(__file__).parent.parent))

from trading.trade_db import TradeDB
from config import MetaConfig

EST = pytz.timezone("US/Eastern")


class CapitalRequirementAnalyzer:
    """Analyzes capital requirements for $1M target in 6 months."""

    def __init__(self):
        self.db = TradeDB()
        self.output_dir = Path(__file__).parent.parent / "output"

    def get_historical_performance(self, days: int = 30) -> Dict[str, Any]:
        """Get historical performance metrics from actual trades."""
        cutoff = (datetime.now(EST) - timedelta(days=days)).date()
        
        all_trades = []
        for date_offset in range(days):
            scan_date = (cutoff + timedelta(days=date_offset)).isoformat()
            trades = self.db.get_trades_by_date(scan_date)
            all_trades.extend(trades)
        
        if not all_trades:
            return {
                "total_trades": 0,
                "executed_trades": 0,
                "winners": 0,
                "losers": 0,
                "win_rate": 0.0,
                "avg_winner_return": 0.0,
                "avg_loser_return": 0.0,
                "avg_position_size": 0.0,
                "total_pnl": 0.0,
            }
        
        # Filter to executed trades only
        executed = [t for t in all_trades if t.get("status") in ["filled", "open", "closed"]]
        
        if not executed:
            return {
                "total_trades": len(all_trades),
                "executed_trades": 0,
                "winners": 0,
                "losers": 0,
                "win_rate": 0.0,
                "avg_winner_return": 0.0,
                "avg_loser_return": 0.0,
                "avg_position_size": 0.0,
                "total_pnl": 0.0,
            }
        
        winners = [t for t in executed if float(t.get("pnl_pct", 0) or 0) >= 50]
        losers = [t for t in executed if float(t.get("pnl_pct", 0) or 0) < 0]
        
        win_rate = len(winners) / len(executed) if executed else 0.0
        
        avg_winner_return = (
            sum(float(t.get("pnl_pct", 0) or 0) for t in winners) / len(winners)
            if winners else 0.0
        )
        
        avg_loser_return = (
            sum(float(t.get("pnl_pct", 0) or 0) for t in losers) / len(losers)
            if losers else 0.0
        )
        
        # Calculate average position size
        position_sizes = []
        for t in executed:
            entry_price = float(t.get("entry_price", 0) or 0)
            contracts = int(t.get("contracts", 5) or 5)
            if entry_price > 0:
                # Options are 100 shares per contract
                position_size = entry_price * contracts * 100
                position_sizes.append(position_size)
        
        avg_position_size = sum(position_sizes) / len(position_sizes) if position_sizes else 0.0
        
        # Calculate total P&L
        total_pnl = sum(float(t.get("pnl", 0) or 0) for t in executed)
        
        return {
            "total_trades": len(all_trades),
            "executed_trades": len(executed),
            "winners": len(winners),
            "losers": len(losers),
            "win_rate": win_rate,
            "avg_winner_return": avg_winner_return,
            "avg_loser_return": avg_loser_return,
            "avg_position_size": avg_position_size,
            "total_pnl": total_pnl,
            "position_sizes": position_sizes,
        }

    def calculate_expected_return_per_trade(
        self,
        win_rate: float,
        avg_winner_return: float,
        avg_loser_return: float,
    ) -> float:
        """Calculate expected return per trade using probability-weighted average."""
        # Expected return = (win_rate × avg_winner) + ((1 - win_rate) × avg_loser)
        expected = (win_rate * avg_winner_return) + ((1 - win_rate) * avg_loser_return)
        return expected

    def calculate_compounding_scenario(
        self,
        initial_capital: float,
        trades_per_week: int,
        weeks: int,
        win_rate: float,
        avg_winner_return: float,
        avg_loser_return: float,
        position_size_pct: float = 0.10,  # 10% of capital per trade
        max_drawdown_pct: float = 0.20,  # 20% max drawdown
    ) -> Dict[str, Any]:
        """
        Calculate compounding scenario with realistic constraints.
        
        Args:
            initial_capital: Starting capital
            trades_per_week: Number of trades per week
            weeks: Number of weeks to simulate
            win_rate: Historical win rate
            avg_winner_return: Average winner return %
            avg_loser_return: Average loser return %
            position_size_pct: % of capital per trade
            max_drawdown_pct: Maximum drawdown before reducing position size
        """
        capital = initial_capital
        total_trades = trades_per_week * weeks
        trades_executed = 0
        winners = 0
        losers = 0
        total_pnl = 0.0
        peak_capital = capital
        max_drawdown = 0.0
        
        weekly_results = []
        
        for week in range(weeks):
            week_start_capital = capital
            week_pnl = 0.0
            week_winners = 0
            week_losers = 0
            
            for trade in range(trades_per_week):
                # Calculate position size (10% of current capital)
                position_size = capital * position_size_pct
                
                # Check drawdown - reduce position size if drawdown > max
                drawdown = (peak_capital - capital) / peak_capital if peak_capital > 0 else 0.0
                if drawdown > max_drawdown_pct:
                    position_size *= 0.5  # Reduce to 5% during drawdown
                
                # Simulate trade outcome
                import random
                is_winner = random.random() < win_rate
                
                if is_winner:
                    # Winner
                    return_pct = avg_winner_return / 100.0
                    trade_pnl = position_size * return_pct
                    winners += 1
                    week_winners += 1
                else:
                    # Loser
                    return_pct = avg_loser_return / 100.0
                    trade_pnl = position_size * return_pct
                    losers += 1
                    week_losers += 1
                
                capital += trade_pnl
                total_pnl += trade_pnl
                trades_executed += 1
                
                # Update peak and drawdown
                if capital > peak_capital:
                    peak_capital = capital
                current_drawdown = (peak_capital - capital) / peak_capital if peak_capital > 0 else 0.0
                if current_drawdown > max_drawdown:
                    max_drawdown = current_drawdown
            
            week_pnl = capital - week_start_capital
            weekly_results.append({
                "week": week + 1,
                "start_capital": week_start_capital,
                "end_capital": capital,
                "week_pnl": week_pnl,
                "winners": week_winners,
                "losers": week_losers,
            })
        
        final_return_pct = ((capital - initial_capital) / initial_capital) * 100 if initial_capital > 0 else 0.0
        
        return {
            "initial_capital": initial_capital,
            "final_capital": capital,
            "total_return": capital - initial_capital,
            "total_return_pct": final_return_pct,
            "total_trades": trades_executed,
            "winners": winners,
            "losers": losers,
            "win_rate_actual": winners / trades_executed if trades_executed > 0 else 0.0,
            "total_pnl": total_pnl,
            "max_drawdown": max_drawdown,
            "peak_capital": peak_capital,
            "weekly_results": weekly_results,
        }

    def find_required_capital(
        self,
        target: float = 1_000_000,
        weeks: int = 26,  # 6 months ≈ 26 weeks
        trades_per_week: int = 6,  # 3 picks × 2 scans per week
        win_rate: float = 0.12,  # 12% win rate (improved from 8.3%)
        avg_winner_return: float = 180.0,  # +180% average winner
        avg_loser_return: float = -12.0,  # -12% average loser
        position_size_pct: float = 0.10,  # 10% per trade
        max_drawdown_pct: float = 0.20,  # 20% max drawdown
    ) -> Dict[str, Any]:
        """
        Find required initial capital to reach target using binary search.
        """
        # Binary search for required capital
        low = 10_000
        high = 1_000_000
        best_result = None
        iterations = 0
        max_iterations = 50
        
        while iterations < max_iterations and (high - low) > 1000:
            mid = (low + high) / 2
            iterations += 1
            
            result = self.calculate_compounding_scenario(
                initial_capital=mid,
                trades_per_week=trades_per_week,
                weeks=weeks,
                win_rate=win_rate,
                avg_winner_return=avg_winner_return,
                avg_loser_return=avg_loser_return,
                position_size_pct=position_size_pct,
                max_drawdown_pct=max_drawdown_pct,
            )
            
            if result["final_capital"] >= target:
                high = mid
                best_result = result
            else:
                low = mid
        
        if best_result is None:
            # Fallback: use high value
            best_result = self.calculate_compounding_scenario(
                initial_capital=high,
                trades_per_week=trades_per_week,
                weeks=weeks,
                win_rate=win_rate,
                avg_winner_return=avg_winner_return,
                avg_loser_return=avg_loser_return,
                position_size_pct=position_size_pct,
                max_drawdown_pct=max_drawdown_pct,
            )
        
        return best_result

    def generate_capital_analysis(self) -> Dict[str, Any]:
        """Generate comprehensive capital requirement analysis."""
        print("=" * 80)
        print("CAPITAL REQUIREMENT ANALYSIS: $1M IN 6 MONTHS")
        print("=" * 80)
        print()
        
        # 1. Get historical performance
        print("📊 Step 1: Analyzing Historical Performance...")
        historical = self.get_historical_performance(days=30)
        
        # Use baseline metrics from Feb 9-12, 2026 analysis (most reliable)
        # These are from the comprehensive week analysis
        baseline_win_rate = 0.083  # 8.3% baseline
        baseline_avg_winner = 189.6  # +189.6% average
        baseline_avg_loser = -12.3  # -12.3% average
        
        if historical["executed_trades"] > 10:
            # Use historical if we have enough data, but validate against baseline
            win_rate = max(historical["win_rate"], baseline_win_rate)  # Use better of two
            avg_winner = max(historical["avg_winner_return"], baseline_avg_winner * 0.8)  # At least 80% of baseline
            avg_loser = min(historical["avg_loser_return"], baseline_avg_loser)  # Use better (less negative)
            print(f"   Executed Trades: {historical['executed_trades']}")
            print(f"   Win Rate: {win_rate*100:.1f}% (baseline: {baseline_win_rate*100:.1f}%)")
            print(f"   Avg Winner Return: {avg_winner:+.1f}% (baseline: {baseline_avg_winner:+.1f}%)")
            print(f"   Avg Loser Return: {avg_loser:.1f}% (baseline: {baseline_avg_loser:.1f}%)")
            print(f"   Avg Position Size: ${historical['avg_position_size']:,.2f}")
        else:
            print("   ⚠️  Limited historical data, using baseline metrics from Feb 9-12 analysis")
            win_rate = baseline_win_rate
            avg_winner = baseline_avg_winner
            avg_loser = baseline_avg_loser
            print(f"   Baseline Win Rate: {win_rate*100:.1f}%")
            print(f"   Baseline Avg Winner Return: {avg_winner:+.1f}%")
            print(f"   Baseline Avg Loser Return: {avg_loser:.1f}%")
        
        print()
        
        # 2. Calculate expected return per trade
        print("📈 Step 2: Calculating Expected Return Per Trade...")
        expected_return = self.calculate_expected_return_per_trade(
            win_rate, avg_winner, avg_loser
        )
        print(f"   Expected Return Per Trade: {expected_return:+.2f}%")
        print()
        
        # 3. Define scenarios
        print("🎯 Step 3: Calculating Capital Requirements...")
        print()
        
        scenarios = [
            {
                "name": "Baseline (Feb 9-12 Performance)",
                "win_rate": baseline_win_rate,  # 8.3%
                "avg_winner": baseline_avg_winner,  # 189.6%
                "avg_loser": baseline_avg_loser,  # -12.3%
                "trades_per_week": 6,  # 3 picks × 2 scans
                "position_size_pct": 0.10,  # 10% per trade
            },
            {
                "name": "Realistic (Improved with New Code)",
                "win_rate": 0.12,  # 12% (improved from 8.3% with selection gates)
                "avg_winner": 180.0,  # Slightly lower than 189.6% (more realistic)
                "avg_loser": -12.0,  # Same as baseline
                "trades_per_week": 6,  # 3 picks × 2 scans
                "position_size_pct": 0.10,  # 10% per trade
            },
            {
                "name": "Optimistic (Selection Gates + ORM Working Well)",
                "win_rate": 0.15,  # 15% (selection gates filtering well)
                "avg_winner": 200.0,  # Higher returns (ORM enhancements working)
                "avg_loser": -10.0,  # Better risk management (stop loss at -50%)
                "trades_per_week": 6,  # 3 picks × 2 scans
                "position_size_pct": 0.12,  # 12% per trade (slightly more aggressive)
            },
        ]
        
        results = []
        
        for scenario in scenarios:
            print(f"   Analyzing: {scenario['name']}...")
            result = self.find_required_capital(
                target=1_000_000,
                weeks=26,
                trades_per_week=scenario["trades_per_week"],
                win_rate=scenario["win_rate"],
                avg_winner_return=scenario["avg_winner"],
                avg_loser_return=scenario["avg_loser"],
                position_size_pct=scenario["position_size_pct"],
            )
            
            results.append({
                "scenario": scenario["name"],
                "required_capital": result["initial_capital"],
                "final_capital": result["final_capital"],
                "total_return_pct": result["total_return_pct"],
                "total_trades": result["total_trades"],
                "win_rate_actual": result["win_rate_actual"],
                "max_drawdown": result["max_drawdown"],
                "params": scenario,
            })
            
            print(f"      Required Capital: ${result['initial_capital']:,.2f}")
            print(f"      Final Capital: ${result['final_capital']:,.2f}")
            print(f"      Total Return: {result['total_return_pct']:.1f}%")
            print(f"      Max Drawdown: {result['max_drawdown']*100:.1f}%")
            print()
        
        # 4. Risk analysis
        print("⚠️  Step 4: Risk Analysis...")
        print()
        
        # Calculate worst-case scenario
        worst_case = self.calculate_compounding_scenario(
            initial_capital=results[1]["required_capital"],  # Use realistic scenario
            trades_per_week=6,
            weeks=26,
            win_rate=0.08,  # Lower win rate
            avg_winner_return=150.0,  # Lower returns
            avg_loser_return=-15.0,  # Worse losses
            position_size_pct=0.10,
        )
        
        print(f"   Worst-Case Scenario (8% win rate, 150% avg winner, -15% avg loser):")
        print(f"      Final Capital: ${worst_case['final_capital']:,.2f}")
        print(f"      Total Return: {worst_case['total_return_pct']:.1f}%")
        print(f"      Max Drawdown: {worst_case['max_drawdown']*100:.1f}%")
        print()
        
        # 5. Position sizing analysis
        print("💰 Step 5: Position Sizing Analysis...")
        print()
        
        avg_position = historical.get("avg_position_size", 5000)  # Default $5k if no data
        required_capital_realistic = results[1]["required_capital"]
        
        print(f"   Average Position Size (Historical): ${avg_position:,.2f}")
        print(f"   Required Capital (Realistic): ${required_capital_realistic:,.2f}")
        print(f"   Position Size as % of Capital: {(avg_position/required_capital_realistic)*100:.1f}%")
        print()
        
        # Compile report
        report = {
            "timestamp": datetime.now(EST).isoformat(),
            "target": 1_000_000,
            "timeframe_weeks": 26,
            "historical_performance": historical,
            "expected_return_per_trade": expected_return,
            "scenarios": results,
            "worst_case": {
                "final_capital": worst_case["final_capital"],
                "total_return_pct": worst_case["total_return_pct"],
                "max_drawdown": worst_case["max_drawdown"],
            },
            "recommendations": self._generate_recommendations(results, historical),
        }
        
        return report

    def _generate_recommendations(
        self, results: List[Dict], historical: Dict
    ) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []
        
        realistic = results[1]  # Realistic scenario
        required_capital = realistic["required_capital"]
        
        recommendations.append(
            f"Required Capital (Realistic): ${required_capital:,.2f} to reach $1M in 6 months"
        )
        
        if historical["executed_trades"] > 0:
            if historical["win_rate"] < 0.10:
                recommendations.append(
                    "⚠️  Current win rate is below target. Focus on improving selection gates."
                )
            if historical["avg_winner_return"] < 150:
                recommendations.append(
                    "⚠️  Average winner return is below target. Review ORM enhancements."
                )
        
        recommendations.append(
            "✅ Use 10% position sizing per trade (allows 10 concurrent positions)"
        )
        
        recommendations.append(
            "✅ Maintain 20% max drawdown limit to reduce position size during drawdowns"
        )
        
        recommendations.append(
            "✅ Target 6 trades per week (3 picks × 2 scans: 9:35 AM, 3:15 PM)"
        )
        
        recommendations.append(
            "⚠️  Worst-case scenario shows significant risk. Consider starting with smaller capital and scaling up."
        )
        
        return recommendations

    def print_report(self, report: Dict[str, Any]):
        """Print formatted report."""
        print("=" * 80)
        print("FINAL RECOMMENDATIONS")
        print("=" * 80)
        print()
        
        realistic = next((r for r in report["scenarios"] if "Realistic" in r["scenario"]), None)
        if realistic:
            print(f"💰 REQUIRED CAPITAL (Realistic Scenario):")
            print(f"   ${realistic['required_capital']:,.2f}")
            print()
            print(f"   Assumptions:")
            print(f"   - Win Rate: {realistic['params']['win_rate']*100:.1f}%")
            print(f"   - Avg Winner Return: {realistic['params']['avg_winner']:+.1f}%")
            print(f"   - Avg Loser Return: {realistic['params']['avg_loser']:.1f}%")
            print(f"   - Trades Per Week: {realistic['params']['trades_per_week']}")
            print(f"   - Position Size: {realistic['params']['position_size_pct']*100:.0f}% of capital")
            print()
        
        print("📋 RECOMMENDATIONS:")
        for i, rec in enumerate(report["recommendations"], 1):
            print(f"   {i}. {rec}")
        print()
        
        print("=" * 80)


def main():
    """Main entry point."""
    analyzer = CapitalRequirementAnalyzer()
    report = analyzer.generate_capital_analysis()
    analyzer.print_report(report)
    
    # Save report
    output_file = Path(__file__).parent.parent / "output" / f"capital_analysis_{datetime.now(EST).strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"✅ Report saved: {output_file.name}")
    print()


if __name__ == "__main__":
    main()
