#!/usr/bin/env python3
"""
Institutional-Grade Analysis: Top 10 Picks Performance
========================================================
Analyzes all Top 10 picks from Monday 9:35 AM EST to Thursday 3:15 PM EST.

Provides:
- Complete pick inventory (no picks missed)
- Peak return analysis for each pick
- Winner vs. loser classification
- Root cause analysis for losers
- Success factor analysis for winners
- Institutional recommendations

No fixes applied — recommendations only.
"""

import sys
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pytz

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

EST = pytz.timezone("US/Eastern")

# ============================================================================
# Data Collection
# ============================================================================

def get_analysis_period():
    """Determine Monday to Thursday analysis period."""
    today = datetime.now(EST).date()
    # Find most recent Monday
    monday = today - timedelta(days=today.weekday())
    if monday > today:
        monday = monday - timedelta(days=7)
    thursday = monday + timedelta(days=3)
    
    return monday, thursday


def load_all_top10_picks(monday: datetime.date, thursday: datetime.date) -> List[Dict]:
    """Load all Top 10 picks from cross_analysis files in the period."""
    output_dir = Path("output")
    all_picks = []
    
    # Load all cross_analysis files
    for cross_file in sorted(output_dir.glob("cross_analysis_*.json")):
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
            
            file_date = dt.date()
            hour = dt.hour
            minute = dt.minute
            
            # Filter: Monday 9:35 AM to Thursday 3:15 PM
            if monday <= file_date <= thursday:
                # Monday 9:35 AM or later
                if file_date == monday:
                    if not (hour == 9 and minute >= 35):
                        continue
                # Thursday 3:15 PM or earlier
                elif file_date == thursday:
                    if not (hour == 15 and minute == 15):
                        continue
                # Tuesday-Wednesday: any 9:35 AM or 3:15 PM scan
                elif monday < file_date < thursday:
                    if not ((hour == 9 and minute >= 35) or (hour == 15 and minute == 15)):
                        continue
                
                # Extract Top 10 from each engine
                puts_top10 = data.get("puts_through_moonshot", [])[:10]
                moonshot_top10 = data.get("moonshot_through_puts", [])[:10]
                
                for pick in puts_top10 + moonshot_top10:
                    pick["scan_timestamp"] = ts
                    pick["scan_date"] = file_date.isoformat()
                    pick["scan_time"] = dt.strftime("%Y-%m-%d %H:%M:%S %Z")
                    pick["session"] = "AM" if hour < 12 else "PM"
                    all_picks.append(pick)
        
        except Exception as e:
            print(f"  ⚠️  Error loading {cross_file.name}: {e}")
            continue
    
    return all_picks


def get_trade_data_for_picks(picks: List[Dict]) -> Dict[str, Dict]:
    """Get trade data from database for all picks."""
    from trading.trade_db import TradeDB
    
    db = TradeDB()
    trade_map = {}
    
    # Get all trades from the period
    monday, thursday = get_analysis_period()
    for date_offset in range(4):  # Mon-Thu
        scan_date = (monday + timedelta(days=date_offset)).isoformat()
        trades = db.get_trades_by_date(scan_date)
        
        for trade in trades:
            sym = trade.get("symbol", "")
            option_type = trade.get("option_type", "")
            # Match by symbol and option type
            key = f"{sym}_{option_type}"
            if key not in trade_map:
                trade_map[key] = []
            trade_map[key].append(trade)
    
    return trade_map


def get_peak_return_for_pick(pick: Dict, trades: List[Dict]) -> Tuple[float, str]:
    """
    Calculate peak return for a pick.
    Returns: (peak_return_pct, analysis)
    """
    if not trades:
        return 0.0, "No trade executed"
    
    # Use the best trade if multiple
    best_trade = max(trades, key=lambda t: float(t.get("pnl_pct", 0) or 0))
    
    entry_px = float(best_trade.get("entry_price", 0) or 0)
    current_px = float(best_trade.get("current_price", 0) or 0)
    exit_px = float(best_trade.get("exit_price", 0) or 0)
    pnl_pct = float(best_trade.get("pnl_pct", 0) or 0)
    status = best_trade.get("status", "")
    
    # Peak return logic
    if status == "closed":
        # Use exit price for closed trades
        if exit_px > 0:
            peak = ((exit_px / entry_px) - 1) * 100 if entry_px > 0 else 0
            return peak, f"Closed at {exit_px:.2f}"
        else:
            return pnl_pct, "Closed (no exit price)"
    else:
        # For open trades, use current price
        if current_px > 0:
            peak = ((current_px / entry_px) - 1) * 100 if entry_px > 0 else 0
            return peak, f"Open (current: {current_px:.2f})"
        else:
            return pnl_pct, "Open (no current price)"


# ============================================================================
# Analysis Functions
# ============================================================================

def analyze_pick_performance(pick: Dict, trades: List[Dict]) -> Dict[str, Any]:
    """Comprehensive institutional analysis of a single pick."""
    sym = pick.get("symbol", "?")
    score = pick.get("score", 0)
    price = pick.get("price", 0)
    signals = pick.get("signals", [])
    engine = pick.get("engine", "")
    orm_score = pick.get("_orm_score", 0)
    orm_factors = pick.get("_orm_factors", {})
    
    peak_return, return_status = get_peak_return_for_pick(pick, trades)
    
    # Classification
    if peak_return >= 50:
        category = "WINNER"
        emoji = "✅"
    elif peak_return >= 0:
        category = "BREAKEVEN"
        emoji = "➖"
    else:
        category = "LOSER"
        emoji = "❌"
    
    analysis = {
        "symbol": sym,
        "scan_time": pick.get("scan_time", ""),
        "session": pick.get("session", ""),
        "engine": engine,
        "score": score,
        "orm_score": orm_score,
        "price_at_scan": price,
        "signals": signals,
        "peak_return_pct": peak_return,
        "return_status": return_status,
        "category": category,
        "emoji": emoji,
        "trades": trades,
    }
    
    # Root cause analysis for losers
    if category == "LOSER":
        reasons = []
        
        # Check if trade was executed
        if not trades:
            reasons.append("NO_TRADE_EXECUTED")
        else:
            trade = trades[0]
            entry_px = float(trade.get("entry_price", 0) or 0)
            current_px = float(trade.get("current_price", 0) or 0)
            
            # Price moved against thesis
            if engine == "PutsEngine" and current_px > entry_px * 1.1:
                reasons.append("PRICE_MOVED_UP (put thesis failed)")
            elif engine == "Moonshot" and current_px < entry_px * 0.9:
                reasons.append("PRICE_MOVED_DOWN (call thesis failed)")
            
            # Low ORM score
            if orm_score < 0.4:
                reasons.append("LOW_ORM_SCORE (poor options setup)")
            
            # Weak signals
            if len(signals) < 2:
                reasons.append("WEAK_SIGNAL_COUNT")
            
            # Low base score
            if score < 0.6:
                reasons.append("LOW_BASE_SCORE")
        
        analysis["loser_reasons"] = reasons
    
    # Success factors for winners
    elif category == "WINNER":
        factors = []
        
        if orm_score >= 0.7:
            factors.append("HIGH_ORM_SCORE")
        
        if len(signals) >= 3:
            factors.append("STRONG_SIGNAL_COUNT")
        
        if score >= 0.7:
            factors.append("HIGH_BASE_SCORE")
        
        # Check top ORM factors
        if orm_factors:
            top_factor = max(orm_factors.items(), key=lambda x: x[1])
            factors.append(f"TOP_ORM_FACTOR: {top_factor[0]} ({top_factor[1]:.2f})")
        
        analysis["winner_factors"] = factors
    
    return analysis


# ============================================================================
# Main Analysis
# ============================================================================

def main():
    print("=" * 80)
    print("INSTITUTIONAL-GRADE TOP 10 PICKS ANALYSIS")
    print("Monday 9:35 AM EST to Thursday 3:15 PM EST")
    print("=" * 80)
    print()
    
    # Get analysis period
    monday, thursday = get_analysis_period()
    print(f"📅 Analysis Period: {monday} 9:35 AM EST to {thursday} 3:15 PM EST")
    print()
    
    # Load all Top 10 picks
    print("📊 Loading Top 10 picks from cross_analysis files...")
    all_picks = load_all_top10_picks(monday, thursday)
    print(f"   Found {len(all_picks)} total picks across all scans")
    print()
    
    # Get trade data
    print("💰 Loading trade data from database...")
    trade_map = get_trade_data_for_picks(all_picks)
    print(f"   Found trades for {len(trade_map)} unique symbol+type combinations")
    print()
    
    # Analyze each pick
    print("🔍 Analyzing each pick...")
    print()
    
    analyses = []
    for pick in all_picks:
        sym = pick.get("symbol", "?")
        option_type = "put" if "PutsEngine" in pick.get("engine", "") else "call"
        key = f"{sym}_{option_type}"
        trades = trade_map.get(key, [])
        
        analysis = analyze_pick_performance(pick, trades)
        analyses.append(analysis)
    
    # ============================================================================
    # Summary Statistics
    # ============================================================================
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print()
    
    winners = [a for a in analyses if a["category"] == "WINNER"]
    losers = [a for a in analyses if a["category"] == "LOSER"]
    breakeven = [a for a in analyses if a["category"] == "BREAKEVEN"]
    no_trade = [a for a in analyses if not a["trades"]]
    
    print(f"📈 Total Picks Analyzed: {len(analyses)}")
    print(f"   ✅ Winners (≥50%): {len(winners)} ({len(winners)/len(analyses)*100:.1f}%)")
    print(f"   ❌ Losers (<0%): {len(losers)} ({len(losers)/len(analyses)*100:.1f}%)")
    print(f"   ➖ Breakeven (0-50%): {len(breakeven)} ({len(breakeven)/len(analyses)*100:.1f}%)")
    print(f"   ⚠️  No Trade Executed: {len(no_trade)} ({len(no_trade)/len(analyses)*100:.1f}%)")
    print()
    
    if winners:
        avg_winner_return = sum(w["peak_return_pct"] for w in winners) / len(winners)
        best_winner = max(winners, key=lambda x: x["peak_return_pct"])
        print(f"🏆 Winners:")
        print(f"   Average Return: +{avg_winner_return:.1f}%")
        print(f"   Best: {best_winner['symbol']} +{best_winner['peak_return_pct']:.1f}%")
        print()
    
    if losers:
        avg_loser_return = sum(l["peak_return_pct"] for l in losers) / len(losers)
        worst_loser = min(losers, key=lambda x: x["peak_return_pct"])
        print(f"📉 Losers:")
        print(f"   Average Return: {avg_loser_return:.1f}%")
        print(f"   Worst: {worst_loser['symbol']} {worst_loser['peak_return_pct']:.1f}%")
        print()
    
    # ============================================================================
    # Detailed Pick-by-Pick Analysis
    # ============================================================================
    print("=" * 80)
    print("DETAILED PICK-BY-PICK ANALYSIS")
    print("=" * 80)
    print()
    
    # Sort by peak return descending
    analyses_sorted = sorted(analyses, key=lambda x: x["peak_return_pct"], reverse=True)
    
    for i, analysis in enumerate(analyses_sorted, 1):
        sym = analysis["symbol"]
        peak = analysis["peak_return_pct"]
        category = analysis["category"]
        emoji = analysis["emoji"]
        scan_time = analysis["scan_time"]
        session = analysis["session"]
        engine = analysis["engine"]
        score = analysis["score"]
        orm_score = analysis["orm_score"]
        
        print(f"{i}. {emoji} {sym} — {category} ({peak:+.1f}%)")
        print(f"   Scan: {scan_time} ({session}) | Engine: {engine}")
        print(f"   Score: {score:.3f} | ORM: {orm_score:.3f}")
        
        if analysis.get("trades"):
            trade = analysis["trades"][0]
            entry = trade.get("entry_price", 0)
            current = trade.get("current_price", 0)
            status = trade.get("status", "")
            print(f"   Trade: Entry ${entry:.2f} → Current ${current:.2f} ({status})")
        else:
            print(f"   Trade: NOT EXECUTED")
        
        # Loser analysis
        if category == "LOSER" and analysis.get("loser_reasons"):
            print(f"   ❌ Loser Reasons:")
            for reason in analysis["loser_reasons"]:
                print(f"      • {reason}")
        
        # Winner analysis
        if category == "WINNER" and analysis.get("winner_factors"):
            print(f"   ✅ Winner Factors:")
            for factor in analysis["winner_factors"]:
                print(f"      • {factor}")
        
        print()
    
    # ============================================================================
    # Root Cause Analysis: Losers
    # ============================================================================
    if losers:
        print("=" * 80)
        print("ROOT CAUSE ANALYSIS: LOSERS")
        print("=" * 80)
        print()
        
        # Count loser reasons
        reason_counts = {}
        for loser in losers:
            reasons = loser.get("loser_reasons", [])
            for reason in reasons:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
        
        print("Top Loser Reasons:")
        for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {reason}: {count} picks")
        print()
        
        # Analyze by engine
        puts_losers = [l for l in losers if "Puts" in l.get("engine", "")]
        moonshot_losers = [l for l in losers if "Moonshot" in l.get("engine", "")]
        
        print(f"Losers by Engine:")
        print(f"   PutsEngine: {len(puts_losers)} losers")
        print(f"   Moonshot: {len(moonshot_losers)} losers")
        print()
    
    # ============================================================================
    # Success Factor Analysis: Winners
    # ============================================================================
    if winners:
        print("=" * 80)
        print("SUCCESS FACTOR ANALYSIS: WINNERS")
        print("=" * 80)
        print()
        
        # Count winner factors
        factor_counts = {}
        for winner in winners:
            factors = winner.get("winner_factors", [])
            for factor in factors:
                factor_counts[factor] = factor_counts.get(factor, 0) + 1
        
        print("Top Winner Factors:")
        for factor, count in sorted(factor_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {factor}: {count} picks")
        print()
        
        # Analyze by engine
        puts_winners = [w for w in winners if "Puts" in w.get("engine", "")]
        moonshot_winners = [w for w in winners if "Moonshot" in w.get("engine", "")]
        
        print(f"Winners by Engine:")
        print(f"   PutsEngine: {len(puts_winners)} winners")
        print(f"   Moonshot: {len(moonshot_winners)} winners")
        print()
    
    # ============================================================================
    # Recommendations
    # ============================================================================
    print("=" * 80)
    print("INSTITUTIONAL RECOMMENDATIONS")
    print("=" * 80)
    print()
    
    recommendations = []
    
    # Recommendation 1: ORM Score Threshold
    low_orm_losers = [l for l in losers if l.get("orm_score", 0) < 0.4]
    if low_orm_losers:
        recommendations.append(
            f"1. ORM Score Gate: {len(low_orm_losers)} losers had ORM < 0.40. "
            f"Consider minimum ORM threshold of 0.45 for Top 10 selection."
        )
    
    # Recommendation 2: Signal Count
    weak_signal_losers = [l for l in losers if len(l.get("signals", [])) < 2]
    if weak_signal_losers:
        recommendations.append(
            f"2. Signal Count Gate: {len(weak_signal_losers)} losers had < 2 signals. "
            f"Consider requiring minimum 2-3 signals for Top 10."
        )
    
    # Recommendation 3: Base Score
    low_score_losers = [l for l in losers if l.get("score", 0) < 0.6]
    if low_score_losers:
        recommendations.append(
            f"3. Base Score Gate: {len(low_score_losers)} losers had base score < 0.60. "
            f"Consider minimum base score of 0.65 for Top 10."
        )
    
    # Recommendation 4: Winner Patterns
    if winners:
        high_orm_winners = [w for w in winners if w.get("orm_score", 0) >= 0.7]
        if high_orm_winners:
            recommendations.append(
                f"4. ORM Success Pattern: {len(high_orm_winners)}/{len(winners)} winners "
                f"had ORM ≥ 0.70. ORM is a strong predictor of success."
            )
    
    # Recommendation 5: Trade Execution
    if no_trade:
        recommendations.append(
            f"5. Trade Execution Gap: {len(no_trade)} picks had no trades executed. "
            f"Review order placement logic and Alpaca connection."
        )
    
    if recommendations:
        for rec in recommendations:
            print(f"   {rec}")
    else:
        print("   No specific recommendations at this time.")
    
    print()
    print("=" * 80)
    print("Analysis Complete")
    print("=" * 80)


if __name__ == "__main__":
    main()
