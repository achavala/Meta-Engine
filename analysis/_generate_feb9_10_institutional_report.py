"""
Generate Institutional-Grade Analysis Report for Feb 9-10 Backtest
==================================================================
30+ years trading + PhD quant + institutional microstructure lens
"""

import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

OUTPUT_DIR = Path(__file__).parent.parent / "output"

def load_backtest_data() -> Dict[str, Any]:
    """Load filtered backtest results."""
    backtest_file = OUTPUT_DIR / "backtest_feb9_13_real_data.json"
    with open(backtest_file) as f:
        data = json.load(f)
    
    all_results = data.get("all_results", [])
    feb9_10_results = [
        r for r in all_results 
        if r.get("scan_date") in ["2026-02-09", "2026-02-10"]
    ]
    
    return {
        "all_results": feb9_10_results,
        "puts": [r for r in feb9_10_results if r.get("option_type") == "put"],
        "moonshot": [r for r in feb9_10_results if r.get("option_type") == "call"],
    }

def analyze_winners_losers(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Deep analysis of winners and losers."""
    winners = [r for r in results if r.get("win") == True]
    losers = [r for r in results if r.get("win") == False]
    
    # Aggregate metrics
    winner_metrics = {
        "count": len(winners),
        "avg_return": sum(r.get("options_pnl_net", 0) for r in winners) / len(winners) if winners else 0,
        "avg_orm": sum(r.get("orm_score", 0) for r in winners) / len(winners) if winners else 0,
        "avg_signals": sum(r.get("signal_count", 0) for r in winners) / len(winners) if winners else 0,
        "avg_base_score": sum(r.get("base_score", 0) for r in winners) / len(winners) if winners else 0,
        "orm_computed_pct": sum(1 for r in winners if r.get("orm_status") == "computed") / len(winners) * 100 if winners else 0,
    }
    
    loser_metrics = {
        "count": len(losers),
        "avg_return": sum(r.get("options_pnl_net", 0) for r in losers) / len(losers) if losers else 0,
        "avg_orm": sum(r.get("orm_score", 0) for r in losers) / len(losers) if losers else 0,
        "avg_signals": sum(r.get("signal_count", 0) for r in losers) / len(losers) if losers else 0,
        "avg_base_score": sum(r.get("base_score", 0) for r in losers) / len(losers) if losers else 0,
        "orm_computed_pct": sum(1 for r in losers if r.get("orm_status") == "computed") / len(losers) * 100 if losers else 0,
    }
    
    # Top performers
    top_winners = sorted(winners, key=lambda x: x.get("options_pnl_net", 0), reverse=True)[:5]
    worst_losers = sorted(losers, key=lambda x: x.get("options_pnl_net", 0))[:5]
    
    return {
        "winners": winners,
        "losers": losers,
        "winner_metrics": winner_metrics,
        "loser_metrics": loser_metrics,
        "top_winners": top_winners,
        "worst_losers": worst_losers,
    }

def generate_recommendations(analysis: Dict[str, Any], data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate institutional-grade recommendations."""
    recommendations = []
    
    puts_analysis = analysis["puts"]
    moonshot_analysis = analysis["moonshot"]
    
    # 1. ORM Analysis
    puts_winners_orm = [r.get("orm_score", 0) for r in puts_analysis["winners"] if r.get("orm_status") == "computed"]
    puts_losers_orm = [r.get("orm_score", 0) for r in puts_analysis["losers"] if r.get("orm_status") == "computed"]
    
    if puts_winners_orm and puts_losers_orm:
        avg_winner_orm = sum(puts_winners_orm) / len(puts_winners_orm)
        avg_loser_orm = sum(puts_losers_orm) / len(puts_losers_orm)
        
        if avg_winner_orm > avg_loser_orm + 0.10:
            recommendations.append({
                "category": "ORM Gate",
                "priority": "HIGH",
                "finding": f"Winners averaged ORM {avg_winner_orm:.2f} vs losers {avg_loser_orm:.2f}",
                "recommendation": f"Consider raising ORM gate threshold to {avg_loser_orm + 0.05:.2f} to filter more losers",
                "impact": "Would filter low-ORM picks that tend to lose"
            })
    
    # 2. Signal Count Analysis
    puts_winners_sigs = [r.get("signal_count", 0) for r in puts_analysis["winners"]]
    puts_losers_sigs = [r.get("signal_count", 0) for r in puts_analysis["losers"]]
    
    if puts_winners_sigs and puts_losers_sigs:
        avg_winner_sigs = sum(puts_winners_sigs) / len(puts_winners_sigs)
        avg_loser_sigs = sum(puts_losers_sigs) / len(puts_losers_sigs)
        
        if avg_winner_sigs > avg_loser_sigs + 1.0:
            recommendations.append({
                "category": "Signal Quality",
                "priority": "MODERATE",
                "finding": f"Winners averaged {avg_winner_sigs:.1f} signals vs losers {avg_loser_sigs:.1f}",
                "recommendation": f"Require minimum {int(avg_loser_sigs + 1)} signals for Top 10 inclusion",
                "impact": "Would filter picks with insufficient signal confluence"
            })
    
    # 3. Base Score Analysis
    puts_winners_score = [r.get("base_score", 0) for r in puts_analysis["winners"]]
    puts_losers_score = [r.get("base_score", 0) for r in puts_analysis["losers"]]
    
    if puts_winners_score and puts_losers_score:
        avg_winner_score = sum(puts_winners_score) / len(puts_winners_score)
        avg_loser_score = sum(puts_losers_score) / len(puts_losers_score)
        
        if avg_winner_score > avg_loser_score + 0.05:
            recommendations.append({
                "category": "Score Threshold",
                "priority": "MODERATE",
                "finding": f"Winners averaged base score {avg_winner_score:.2f} vs losers {avg_loser_score:.2f}",
                "recommendation": f"Consider minimum base score gate of {avg_loser_score + 0.02:.2f}",
                "impact": "Would filter lower-conviction picks"
            })
    
    # 4. Moonshot Performance Gap
    moonshot_total = len(data["moonshot"])
    puts_total = len(data["puts"])
    moonshot_win_rate = len(moonshot_analysis["winners"]) / moonshot_total * 100 if moonshot_total > 0 else 0
    puts_win_rate = len(puts_analysis["winners"]) / puts_total * 100 if puts_total > 0 else 0
    
    if puts_win_rate > moonshot_win_rate + 20:
        recommendations.append({
            "category": "Engine Allocation",
            "priority": "LOW",
            "finding": f"PUTS win rate {puts_win_rate:.1f}% vs MOONSHOT {moonshot_win_rate:.1f}%",
            "recommendation": "Consider allocating more capital to PUTS engine in current regime",
            "impact": "Regime-dependent — monitor over longer period before reallocating"
        })
    
    # 5. ORM Missing Impact
    puts_winners_orm_missing = sum(1 for r in puts_analysis["winners"] if r.get("orm_status") == "missing")
    puts_losers_orm_missing = sum(1 for r in puts_analysis["losers"] if r.get("orm_status") == "missing")
    
    if puts_winners_orm_missing > 0 or puts_losers_orm_missing > 0:
        recommendations.append({
            "category": "ORM Calculation",
            "priority": "HIGH",
            "finding": f"{puts_winners_orm_missing} winners and {puts_losers_orm_missing} losers had missing ORM",
            "recommendation": "Enhance ORM calculation to ensure all picks have computed ORM scores",
            "impact": "Would improve gate effectiveness and pick quality"
        })
    
    return recommendations

def generate_report() -> str:
    """Generate comprehensive institutional report."""
    data = load_backtest_data()
    
    puts_analysis = analyze_winners_losers(data["puts"])
    moonshot_analysis = analyze_winners_losers(data["moonshot"])
    
    recommendations = generate_recommendations({
        "puts": puts_analysis,
        "moonshot": moonshot_analysis,
    }, data)
    
    report = []
    report.append("# INSTITUTIONAL-GRADE BACKTEST ANALYSIS")
    report.append("## Feb 9-10, 2026 | 9:35 AM & 3:15 PM Top 10 Picks")
    report.append("")
    report.append("**Analysis Lens:** 30+ years trading + PhD quant + institutional microstructure")
    report.append("")
    report.append("---")
    report.append("")
    
    # Executive Summary
    report.append("## EXECUTIVE SUMMARY")
    report.append("")
    total_picks = len(data["all_results"])
    total_winners = len(puts_analysis["winners"]) + len(moonshot_analysis["winners"])
    total_losers = len(puts_analysis["losers"]) + len(moonshot_analysis["losers"])
    win_rate = (total_winners / total_picks * 100) if total_picks > 0 else 0
    
    report.append(f"- **Total Picks:** {total_picks}")
    report.append(f"- **Winners:** {total_winners} ({win_rate:.1f}%)")
    report.append(f"- **Losers:** {total_losers}")
    report.append(f"- **PUTS Engine:** {len(data['puts'])} picks ({len(puts_analysis['winners'])} winners, {len(puts_analysis['losers'])} losers)")
    report.append(f"- **MOONSHOT Engine:** {len(data['moonshot'])} picks ({len(moonshot_analysis['winners'])} winners, {len(moonshot_analysis['losers'])} losers)")
    report.append("")
    
    # PUTS Analysis
    report.append("## PUTS ENGINE ANALYSIS")
    report.append("")
    report.append(f"**Win Rate:** {len(puts_analysis['winners'])}/{len(data['puts'])} ({len(puts_analysis['winners'])/len(data['puts'])*100:.1f}%)")
    report.append("")
    report.append("### Winner Metrics")
    report.append(f"- Average Return: {puts_analysis['winner_metrics']['avg_return']:+.1f}%")
    report.append(f"- Average ORM: {puts_analysis['winner_metrics']['avg_orm']:.2f}")
    report.append(f"- Average Signals: {puts_analysis['winner_metrics']['avg_signals']:.1f}")
    report.append(f"- Average Base Score: {puts_analysis['winner_metrics']['avg_base_score']:.2f}")
    report.append(f"- ORM Computed: {puts_analysis['winner_metrics']['orm_computed_pct']:.1f}%")
    report.append("")
    report.append("### Loser Metrics")
    report.append(f"- Average Return: {puts_analysis['loser_metrics']['avg_return']:+.1f}%")
    report.append(f"- Average ORM: {puts_analysis['loser_metrics']['avg_orm']:.2f}")
    report.append(f"- Average Signals: {puts_analysis['loser_metrics']['avg_signals']:.1f}")
    report.append(f"- Average Base Score: {puts_analysis['loser_metrics']['avg_base_score']:.2f}")
    report.append(f"- ORM Computed: {puts_analysis['loser_metrics']['orm_computed_pct']:.1f}%")
    report.append("")
    
    # Top PUTS Winners
    report.append("### Top 5 PUTS Winners")
    for i, winner in enumerate(puts_analysis["top_winners"], 1):
        report.append(f"**#{i} {winner.get('symbol')} PUT** — {winner.get('options_pnl_net', 0):+.1f}%")
        report.append(f"- Stock Move: {winner.get('stock_move_pct', 0):+.1f}%")
        report.append(f"- ORM: {winner.get('orm_score', 0):.2f} ({winner.get('orm_status', 'N/A')})")
        report.append(f"- Signals: {winner.get('signal_count', 0)}")
        report.append(f"- Base Score: {winner.get('base_score', 0):.2f}")
        report.append(f"- Analysis: {winner.get('analysis', 'N/A')}")
        report.append("")
    
    # Worst PUTS Losers
    report.append("### Worst 5 PUTS Losers")
    for i, loser in enumerate(puts_analysis["worst_losers"], 1):
        report.append(f"**#{i} {loser.get('symbol')} PUT** — {loser.get('options_pnl_net', 0):+.1f}%")
        report.append(f"- Stock Move: {loser.get('stock_move_pct', 0):+.1f}%")
        report.append(f"- ORM: {loser.get('orm_score', 0):.2f} ({loser.get('orm_status', 'N/A')})")
        report.append(f"- Signals: {loser.get('signal_count', 0)}")
        report.append(f"- Base Score: {loser.get('base_score', 0):.2f}")
        report.append(f"- Analysis: {loser.get('analysis', 'N/A')}")
        report.append("")
    
    # MOONSHOT Analysis
    report.append("## MOONSHOT ENGINE ANALYSIS")
    report.append("")
    report.append(f"**Win Rate:** {len(moonshot_analysis['winners'])}/{len(data['moonshot'])} ({len(moonshot_analysis['winners'])/len(data['moonshot'])*100:.1f}%)")
    report.append("")
    report.append("### Winner Metrics")
    report.append(f"- Average Return: {moonshot_analysis['winner_metrics']['avg_return']:+.1f}%")
    report.append(f"- Average ORM: {moonshot_analysis['winner_metrics']['avg_orm']:.2f}")
    report.append(f"- Average Signals: {moonshot_analysis['winner_metrics']['avg_signals']:.1f}")
    report.append(f"- Average Base Score: {moonshot_analysis['winner_metrics']['avg_base_score']:.2f}")
    report.append("")
    report.append("### Loser Metrics")
    report.append(f"- Average Return: {moonshot_analysis['loser_metrics']['avg_return']:+.1f}%")
    report.append(f"- Average ORM: {moonshot_analysis['loser_metrics']['avg_orm']:.2f}")
    report.append(f"- Average Signals: {moonshot_analysis['loser_metrics']['avg_signals']:.1f}")
    report.append(f"- Average Base Score: {moonshot_analysis['loser_metrics']['avg_base_score']:.2f}")
    report.append("")
    
    # Recommendations
    report.append("## RECOMMENDATIONS")
    report.append("")
    for i, rec in enumerate(recommendations, 1):
        report.append(f"### {i}. {rec['category']} ({rec['priority']} Priority)")
        report.append(f"**Finding:** {rec['finding']}")
        report.append(f"**Recommendation:** {rec['recommendation']}")
        report.append(f"**Impact:** {rec['impact']}")
        report.append("")
    
    return "\n".join(report)

if __name__ == "__main__":
    report = generate_report()
    
    output_file = OUTPUT_DIR / "BACKTEST_FEB9_10_INSTITUTIONAL_REPORT.md"
    with open(output_file, "w") as f:
        f.write(report)
    
    print("=" * 80)
    print("INSTITUTIONAL REPORT GENERATED")
    print("=" * 80)
    print(f"\n✅ Report saved to: {output_file}")
    print("\n" + report[:2000] + "...")
