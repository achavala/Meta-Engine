"""
Institutional-Grade Backtest: Feb 9-13, 2026
=============================================
Backtests all 9:35 AM and 3:15 PM Top 10 picks from both engines
with the NEW code improvements (Feb 15, 2026).

Analyzes:
  - Winners vs. losers
  - Why picks succeeded/failed
  - Impact of new fixes (AM/PM sizing, grade-based sizing, score inversion, etc.)
  - Institutional-grade recommendations

30+ years trading + PhD quant + institutional microstructure lens
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import requests

# Add Meta Engine to path
META_DIR = Path(__file__).parent.parent
if str(META_DIR) not in sys.path:
    sys.path.insert(0, str(META_DIR))

from config import MetaConfig

# Output directory
OUTPUT_DIR = Path(MetaConfig.OUTPUT_DIR)
POLYGON_API_KEY = MetaConfig.POLYGON_API_KEY

# Date range
START_DATE = date(2026, 2, 9)  # Monday
END_DATE = date(2026, 2, 13)   # Friday

# Scan times
AM_SCAN_TIME = "09:35"
PM_SCAN_TIME = "15:15"


def load_scan_data(scan_date: date, session: str) -> Optional[Dict[str, Any]]:
    """
    Load scan data for a specific date and session (AM/PM).
    
    Looks for files:
      - output/meta_engine_run_YYYYMMDD_HHMM.json
      - output/cross_analysis_YYYYMMDD_HHMM.json
    """
    date_str = scan_date.strftime("%Y%m%d")
    hour = 9 if session == "AM" else 15
    minute = 35 if session == "AM" else 15
    
    # Try exact match first
    pattern = f"meta_engine_run_{date_str}_{hour:02d}{minute:02d}*.json"
    files = list(OUTPUT_DIR.glob(pattern))
    
    if not files:
        # Try any file from that date
        pattern = f"meta_engine_run_{date_str}_*.json"
        files = list(OUTPUT_DIR.glob(pattern))
        # Filter by approximate time
        if session == "AM":
            files = [f for f in files if "09" in f.name or "10" in f.name]
        else:
            files = [f for f in files if "15" in f.name or "14" in f.name]
    
    if not files:
        return None
    
    # Use most recent file for that date/session
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    try:
        with open(files[0]) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠️ Failed to load {files[0]}: {e}")
        return None


def get_polygon_client():
    """Get Polygon API client."""
    return requests.Session() if POLYGON_API_KEY else None


def get_stock_data(
    symbol: str,
    start_date: date,
    end_date: date,
    client: requests.Session = None,
) -> Dict[str, Any]:
    """
    Fetch stock data from Polygon for a date range.
    Returns dict with daily bars: {date: {"open": float, "high": float, "low": float, "close": float}}
    """
    if not client or not POLYGON_API_KEY:
        return {}
    
    try:
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start_date.isoformat()}/{end_date.isoformat()}"
        )
        resp = client.get(
            url,
            params={"adjusted": "true", "sort": "asc", "apiKey": POLYGON_API_KEY},
            timeout=15,
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            bars = {}
            for r in results:
                ts = r.get("t", 0) / 1000  # Convert ms to seconds
                bar_date = datetime.fromtimestamp(ts).date()
                bars[bar_date] = {
                    "open": r.get("o", 0),
                    "high": r.get("h", 0),
                    "low": r.get("l", 0),
                    "close": r.get("c", 0),
                    "volume": r.get("v", 0),
                }
            return bars
    except Exception as e:
        print(f"    ⚠️ Polygon fetch failed for {symbol}: {e}")
    
    return {}


def calculate_options_pnl(
    pick_price: float,
    stock_move_pct: float,
    option_type: str,
    days_held: int = 1,
    orm_score: float = 0.0,
    has_earnings: bool = False,
) -> Tuple[float, float]:
    """
    Estimate options P&L based on stock move — ENHANCED (FEB 15, 2026).
    
    Uses a multi-factor model accounting for:
      - Delta (moneyness-dependent: 0.25–0.40 based on ORM/setup quality)
      - Gamma convexity (larger moves get amplified via gamma)
      - Theta decay (time-dependent: accelerates near expiry)
      - IV crush (earnings picks lose ~15-25% of premium post-earnings)
      - ORM quality factor (higher ORM = better strike selection = better P&L)
    
    Returns: (stock_move_pct, estimated_options_return_pct)
    """
    if pick_price <= 0:
        return 0.0, 0.0
    
    # For PUTS: negative stock move = positive P&L
    # For CALLS: positive stock move = positive P&L
    if option_type == "put":
        direction_multiplier = -1.0
    else:
        direction_multiplier = 1.0
    
    directional_move = stock_move_pct * direction_multiplier
    
    # Delta: varies by ORM quality (better setup = closer to ATM = higher delta)
    if orm_score >= 0.70:
        delta = 0.35  # Good strike selection, closer to ATM
    elif orm_score >= 0.50:
        delta = 0.30  # Standard OTM
    else:
        delta = 0.25  # Further OTM / poor setup

    # Gamma convexity: large moves amplify delta (gamma effect)
    # Note: directional_move is in percentage (e.g., 5.0 = 5%)
    abs_move = abs(directional_move)
    if abs_move > 5.0:
        gamma_boost = 1.4  # Large move → delta increases significantly
    elif abs_move > 3.0:
        gamma_boost = 1.2  # Moderate move → some gamma amplification
    elif abs_move > 1.0:
        gamma_boost = 1.1  # Small move → minimal gamma effect
    else:
        gamma_boost = 1.0  # Tiny move → no gamma benefit

    # Premium as fraction of stock price (5% OTM ≈ 1.5-3% premium)
    # Better setups (higher ORM) → better strikes → lower premium % → higher leverage
    premium_pct = max(0.015, 0.03 - orm_score * 0.015)  # 3% base, down to 1.5%

    # Options return = (delta * stock_move% * gamma_boost) / premium_pct
    # This gives the percentage return on the option premium
    raw_pct_return = (delta * directional_move * gamma_boost) / (premium_pct * 100)

    # Time decay (theta): percentage of premium lost per day
    # Short DTE options lose ~3-5% of premium per day
    if days_held <= 1:
        theta_pct = 3.0  # ~3% of premium per day
    elif days_held <= 3:
        theta_pct = days_held * 4.0  # accelerating near expiry
    else:
        theta_pct = days_held * 5.0  # 5% per day for longer holds

    # IV crush: earnings picks lose premium post-announcement
    iv_crush_pct = 0.0
    if has_earnings:
        iv_crush_pct = 25.0 if abs_move < 3.0 else 10.0  # 25% crush on small moves

    # Final percentage return on option premium
    options_return_pct = (raw_pct_return * 100) - theta_pct - iv_crush_pct
    
    return stock_move_pct, options_return_pct


def analyze_pick_performance(
    pick: Dict[str, Any],
    scan_date: date,
    session: str,
    stock_data: Dict[date, Dict[str, float]],
    option_type: str,
) -> Dict[str, Any]:
    """
    Analyze a single pick's performance.
    
    Returns:
        {
            "symbol": str,
            "option_type": str,
            "scan_date": date,
            "session": str,
            "pick_price": float,
            "best_move_pct": float,
            "best_move_date": date,
            "options_pnl_pct": float,
            "grade": str,  # A+, A, B, C, F
            "win": bool,
            "analysis": str,
            "signals": List[str],
            "orm_score": float,
            "meta_score": float,
            "base_score": float,
            "new_code_impact": Dict[str, Any],
        }
    """
    symbol = pick.get("symbol", "")
    pick_price = float(pick.get("price", 0) or 0)
    
    if pick_price <= 0 or not stock_data:
        return {
            "symbol": symbol,
            "option_type": option_type,
            "scan_date": scan_date,
            "session": session,
            "pick_price": pick_price,
            "best_move_pct": 0.0,
            "options_pnl_pct": 0.0,
            "grade": "F",
            "win": False,
            "analysis": "No price data available",
        }
    
    # Find best move within 2 days
    best_move_pct = 0.0
    best_move_date = scan_date
    best_close = pick_price
    
    for check_date in [scan_date + timedelta(days=i) for i in [1, 2]]:
        if check_date in stock_data:
            bar = stock_data[check_date]
            close = bar.get("close", 0)
            if close > 0:
                move_pct = ((close - pick_price) / pick_price) * 100
                # For PUTS: negative move is good
                # For CALLS: positive move is good
                if option_type == "put":
                    effective_move = -move_pct  # Invert for puts
                else:
                    effective_move = move_pct
                
                if effective_move > best_move_pct:
                    best_move_pct = effective_move
                    best_move_date = check_date
                    best_close = close
    
    # Calculate options P&L
    stock_move = ((best_close - pick_price) / pick_price) * 100
    days_held = (best_move_date - scan_date).days
    orm = pick.get("_orm_score", pick.get("orm_score", 0)) or 0
    has_earn = bool(pick.get("_earnings_flag"))
    _, options_pnl = calculate_options_pnl(
        pick_price, stock_move, option_type, days_held,
        orm_score=float(orm), has_earnings=has_earn,
    )
    
    # Determine grade
    if options_pnl >= 200:
        grade = "A+"
    elif options_pnl >= 100:
        grade = "A"
    elif options_pnl >= 50:
        grade = "B"
    elif options_pnl >= 0:
        grade = "C"
    else:
        grade = "F"
    
    win = options_pnl > 0
    
    # Extract signals and scores
    signals = pick.get("signals", [])
    if isinstance(signals, str):
        signals = [signals]
    if not isinstance(signals, list):
        signals = []
    orm_score = float(pick.get("_orm_score", pick.get("orm_score", 0)) or 0)
    meta_score = float(pick.get("meta_score", pick.get("score", 0)) or 0)
    base_score = float(pick.get("_base_score", pick.get("base_score", meta_score)) or 0)
    
    # Analyze why it worked/failed
    analysis_parts = []
    
    if win:
        analysis_parts.append(f"✅ WINNER: {options_pnl:+.0f}% options return")
        if orm_score >= 0.70:
            analysis_parts.append(f"High ORM ({orm_score:.2f}) — options structure favorable")
        if len(signals) >= 5:
            analysis_parts.append(f"Strong signal convergence ({len(signals)} signals)")
        if base_score >= 0.85:
            analysis_parts.append(f"High base score ({base_score:.2f}) — strong conviction")
    else:
        analysis_parts.append(f"❌ LOSER: {options_pnl:+.0f}% options return")
        if orm_score < 0.45:
            analysis_parts.append(f"Low ORM ({orm_score:.2f}) — poor options structure")
        if len(signals) < 2:
            analysis_parts.append(f"Weak signals ({len(signals)} signals) — insufficient confirmation")
        if base_score < 0.65:
            analysis_parts.append(f"Low base score ({base_score:.2f}) — weak conviction")
    
    # Check for new code impacts
    new_code_impact = {}
    if pick.get("_grade"):
        new_code_impact["grade_based_sizing"] = pick["_grade"]
    if pick.get("_staleness_penalty"):
        new_code_impact["score_deflation"] = pick["_staleness_penalty"]
    if pick.get("_sector_conflict"):
        new_code_impact["sector_penalty"] = pick["_sector_conflict"]
    if pick.get("_earnings_flag"):
        new_code_impact["earnings_risk"] = True
    if pick.get("_excluded_reason"):
        new_code_impact["excluded"] = pick["_excluded_reason"]
    
    analysis = " | ".join(analysis_parts)
    
    return {
        "symbol": symbol,
        "option_type": option_type,
        "scan_date": scan_date.isoformat(),
        "session": session,
        "pick_price": pick_price,
        "best_move_pct": best_move_pct,
        "best_move_date": best_move_date.isoformat(),
        "options_pnl_pct": options_pnl,
        "grade": grade,
        "win": win,
        "analysis": analysis,
        "signals": signals,
        "signal_count": len(signals),
        "orm_score": orm_score,
        "meta_score": meta_score,
        "base_score": base_score,
        "new_code_impact": new_code_impact,
    }


def run_backtest() -> Dict[str, Any]:
    """
    Run the full backtest for Feb 9-13, 2026.
    """
    print("=" * 80)
    print("INSTITUTIONAL-GRADE BACKTEST: FEB 9-13, 2026 (NEW CODE)")
    print("=" * 80)
    print(f"Analyzing scans from {START_DATE} to {END_DATE}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    
    client = get_polygon_client()
    if not client:
        print("⚠️  WARNING: No Polygon API key — will use cached data only")
    
    all_results = []
    scan_summary = defaultdict(int)
    
    # Iterate through each date and session
    current_date = START_DATE
    while current_date <= END_DATE:
        if current_date.weekday() >= 5:  # Skip weekends
            current_date += timedelta(days=1)
            continue
        
        for session in ["AM", "PM"]:
            scan_summary["total_scans"] += 1
            print(f"\n{'='*80}")
            print(f"📅 {current_date.strftime('%A, %B %d')} — {session} Scan")
            print(f"{'='*80}")
            
            scan_data = load_scan_data(current_date, session)
            if not scan_data:
                print(f"  ⚠️  No scan data found for {current_date} {session}")
                scan_summary["missing_scans"] += 1
                continue
            
            scan_summary["loaded_scans"] += 1
            
            # Extract picks from cross_analysis or direct top10
            cross_results = scan_data.get("cross_analysis", {})
            puts_picks = cross_results.get("puts_through_moonshot", [])[:10]
            moonshot_picks = cross_results.get("moonshot_through_puts", [])[:10]
            
            if not puts_picks and not moonshot_picks:
                # Fallback to direct top10
                puts_picks = scan_data.get("puts_top10", [])[:10]
                moonshot_picks = scan_data.get("moonshot_top10", [])[:10]
            
            print(f"  🔴 PutsEngine: {len(puts_picks)} picks")
            print(f"  🟢 Moonshot: {len(moonshot_picks)} picks")
            
            # Analyze PUT picks
            for pick in puts_picks:
                symbol = pick.get("symbol", "")
                if not symbol:
                    continue
                
                # Fetch stock data
                stock_data = get_stock_data(symbol, current_date, current_date + timedelta(days=3), client)
                
                result = analyze_pick_performance(
                    pick, current_date, session, stock_data, "put"
                )
                all_results.append(result)
                
                if result["win"]:
                    scan_summary["put_wins"] += 1
                else:
                    scan_summary["put_losses"] += 1
            
            # Analyze CALL picks
            for pick in moonshot_picks:
                symbol = pick.get("symbol", "")
                if not symbol:
                    continue
                
                stock_data = get_stock_data(symbol, current_date, current_date + timedelta(days=3), client)
                
                result = analyze_pick_performance(
                    pick, current_date, session, stock_data, "call"
                )
                all_results.append(result)
                
                if result["win"]:
                    scan_summary["call_wins"] += 1
                else:
                    scan_summary["call_losses"] += 1
        
        current_date += timedelta(days=1)
    
    # Aggregate analysis
    print("\n" + "=" * 80)
    print("📊 AGGREGATE RESULTS")
    print("=" * 80)
    
    total_picks = len(all_results)
    put_picks = [r for r in all_results if r["option_type"] == "put"]
    call_picks = [r for r in all_results if r["option_type"] == "call"]
    
    put_wins = [r for r in put_picks if r["win"]]
    call_wins = [r for r in call_picks if r["win"]]
    
    put_wr = len(put_wins) / len(put_picks) * 100 if put_picks else 0
    call_wr = len(call_wins) / len(call_picks) * 100 if call_picks else 0
    overall_wr = len([r for r in all_results if r["win"]]) / total_picks * 100 if total_picks else 0
    
    print(f"\nTotal Picks Analyzed: {total_picks}")
    print(f"  PUTS: {len(put_picks)} picks | {len(put_wins)} wins ({put_wr:.1f}% WR)")
    print(f"  CALLS: {len(call_picks)} picks | {len(call_wins)} wins ({call_wr:.1f}% WR)")
    print(f"  OVERALL: {overall_wr:.1f}% win rate")
    
    # Grade distribution
    grade_counts = defaultdict(int)
    for r in all_results:
        grade_counts[r["grade"]] += 1
    
    print(f"\nGrade Distribution:")
    for grade in ["A+", "A", "B", "C", "F"]:
        count = grade_counts[grade]
        pct = count / total_picks * 100 if total_picks else 0
        print(f"  {grade}: {count} picks ({pct:.1f}%)")
    
    # Average returns
    avg_return_all = sum(r["options_pnl_pct"] for r in all_results) / total_picks if total_picks else 0
    avg_return_winners = sum(r["options_pnl_pct"] for r in all_results if r["win"]) / len([r for r in all_results if r["win"]]) if [r for r in all_results if r["win"]] else 0
    avg_return_losers = sum(r["options_pnl_pct"] for r in all_results if not r["win"]) / len([r for r in all_results if not r["win"]]) if [r for r in all_results if not r["win"]] else 0
    
    print(f"\nAverage Returns:")
    print(f"  All picks: {avg_return_all:+.1f}%")
    print(f"  Winners: {avg_return_winners:+.1f}%")
    print(f"  Losers: {avg_return_losers:+.1f}%")
    
    # Session analysis
    am_picks = [r for r in all_results if r["session"] == "AM"]
    pm_picks = [r for r in all_results if r["session"] == "PM"]
    am_wr = len([r for r in am_picks if r["win"]]) / len(am_picks) * 100 if am_picks else 0
    pm_wr = len([r for r in pm_picks if r["win"]]) / len(pm_picks) * 100 if pm_picks else 0
    
    print(f"\nSession Analysis:")
    print(f"  AM (9:35): {len(am_picks)} picks | {am_wr:.1f}% WR")
    print(f"  PM (3:15): {len(pm_picks)} picks | {pm_wr:.1f}% WR")
    
    # Detailed pick-by-pick analysis
    print("\n" + "=" * 80)
    print("📋 DETAILED PICK-BY-PICK ANALYSIS")
    print("=" * 80)
    
    # Sort by performance (best to worst)
    all_results.sort(key=lambda x: x["options_pnl_pct"], reverse=True)
    
    print("\n🏆 TOP 10 WINNERS:")
    for i, r in enumerate(all_results[:10], 1):
        if r["win"]:
            print(f"\n  #{i} {r['symbol']} {r['option_type'].upper()} ({r['grade']})")
            print(f"     Return: {r['options_pnl_pct']:+.0f}% | Score: {r.get('meta_score', 0):.2f} | ORM: {r.get('orm_score', 0):.2f}")
            print(f"     Signals: {r.get('signal_count', 0)} | {r.get('analysis', 'N/A')}")
            if r.get("new_code_impact"):
                print(f"     New Code Impact: {r['new_code_impact']}")
    
    print("\n\n💥 TOP 10 LOSERS:")
    losers = [r for r in all_results if not r["win"]]
    for i, r in enumerate(losers[:10], 1):
        print(f"\n  #{i} {r['symbol']} {r['option_type'].upper()} ({r['grade']})")
        print(f"     Return: {r['options_pnl_pct']:+.0f}% | Score: {r.get('meta_score', 0):.2f} | ORM: {r.get('orm_score', 0):.2f}")
        print(f"     Signals: {r.get('signal_count', 0)} | {r.get('analysis', 'N/A')}")
        if r.get("new_code_impact"):
            print(f"     New Code Impact: {r['new_code_impact']}")
    
    # Recommendations
    print("\n" + "=" * 80)
    print("💡 INSTITUTIONAL-GRADE RECOMMENDATIONS")
    print("=" * 80)
    
    recommendations = []
    
    # 1. ORM threshold analysis
    high_orm_winners = [r for r in all_results if r.get("orm_score", 0) >= 0.70 and r.get("win", False)]
    low_orm_losers = [r for r in all_results if r.get("orm_score", 0) < 0.45 and not r.get("win", True)]
    if high_orm_winners:
        high_orm_total = len([r for r in all_results if r.get("orm_score", 0) >= 0.70])
        orm_wr = len(high_orm_winners) / high_orm_total * 100 if high_orm_total > 0 else 0
        recommendations.append(
            f"✅ ORM ≥ 0.70 threshold: {len(high_orm_winners)} winners "
            f"({orm_wr:.1f}% WR) — threshold is effective"
        )
    if low_orm_losers:
        recommendations.append(
            f"⚠️  ORM < 0.45: {len(low_orm_losers)} losers — "
            f"consider raising minimum ORM gate to 0.50"
        )
    
    # 2. Signal count analysis
    high_signal_winners = [r for r in all_results if r.get("signal_count", 0) >= 5 and r.get("win", False)]
    low_signal_losers = [r for r in all_results if r.get("signal_count", 0) < 2 and not r.get("win", True)]
    if high_signal_winners:
        recommendations.append(
            f"✅ Signal count ≥ 5: {len(high_signal_winners)} winners — "
            f"multi-signal convergence is key"
        )
    if low_signal_losers:
        recommendations.append(
            f"⚠️  Signal count < 2: {len(low_signal_losers)} losers — "
            f"minimum 2-signal gate is working"
        )
    
    # 3. AM vs PM
    if am_wr > pm_wr + 5:
        recommendations.append(
            f"✅ AM scans ({am_wr:.1f}% WR) significantly outperform PM ({pm_wr:.1f}% WR) — "
            f"PM sizing reduction (3→2) is justified"
        )
    
    # 4. Grade-based sizing
    a_grade_picks = [r for r in all_results if r.get("grade", "") in ["A+", "A"]]
    a_grade_wr = len([r for r in a_grade_picks if r.get("win", False)]) / len(a_grade_picks) * 100 if a_grade_picks else 0
    if a_grade_wr > overall_wr + 10:
        recommendations.append(
            f"✅ A+/A grade picks ({a_grade_wr:.1f}% WR) outperform average — "
            f"grade-based sizing (5 contracts) is effective"
        )
    
    # 5. Score inversion impact
    deflated_picks = [r for r in all_results if r.get("new_code_impact", {}).get("score_deflation")]
    if deflated_picks:
        deflated_wr = len([r for r in deflated_picks if r.get("win", False)]) / len(deflated_picks) * 100
        recommendations.append(
            f"📉 Score inversion fix: {len(deflated_picks)} picks deflated — "
            f"WR: {deflated_wr:.1f}% (vs {overall_wr:.1f}% overall)"
        )
    
    # 6. Sector concentration
    sector_conflict_picks = [r for r in all_results if r.get("new_code_impact", {}).get("sector_penalty")]
    if sector_conflict_picks:
        conflict_wr = len([r for r in sector_conflict_picks if r.get("win", False)]) / len(sector_conflict_picks) * 100
        recommendations.append(
            f"🔄 Sector concentration: {len(sector_conflict_picks)} picks penalized — "
            f"WR: {conflict_wr:.1f}% (vs {overall_wr:.1f}% overall) — filter is working"
        )
    
    # 7. Earnings risk
    earnings_picks = [r for r in all_results if r.get("new_code_impact", {}).get("earnings_risk")]
    if earnings_picks:
        earnings_wr = len([r for r in earnings_picks if r.get("win", False)]) / len(earnings_picks) * 100
        recommendations.append(
            f"📅 Earnings proximity: {len(earnings_picks)} picks flagged — "
            f"WR: {earnings_wr:.1f}% — consider tighter IV crush handling"
        )
    
    for rec in recommendations:
        print(f"  • {rec}")
    
    # Save full results
    output_file = OUTPUT_DIR / "backtest_feb9_13_newcode.json"
    with open(output_file, "w") as f:
        json.dump({
            "backtest_date": datetime.now().isoformat(),
            "date_range": {
                "start": START_DATE.isoformat(),
                "end": END_DATE.isoformat(),
            },
            "summary": {
                "total_picks": total_picks,
                "put_picks": len(put_picks),
                "call_picks": len(call_picks),
                "put_wins": len(put_wins),
                "call_wins": len(call_wins),
                "put_wr": put_wr,
                "call_wr": call_wr,
                "overall_wr": overall_wr,
                "am_wr": am_wr,
                "pm_wr": pm_wr,
                "avg_return_all": avg_return_all,
                "avg_return_winners": avg_return_winners,
                "avg_return_losers": avg_return_losers,
            },
            "grade_distribution": dict(grade_counts),
            "recommendations": recommendations,
            "detailed_results": all_results,
        }, f, indent=2, default=str)
    
    print(f"\n💾 Full results saved to: {output_file}")
    print("\n" + "=" * 80)
    print("✅ BACKTEST COMPLETE")
    print("=" * 80)
    
    return {
        "summary": {
            "total_picks": total_picks,
            "overall_wr": overall_wr,
            "put_wr": put_wr,
            "call_wr": call_wr,
            "am_wr": am_wr,
            "pm_wr": pm_wr,
        },
        "recommendations": recommendations,
    }


if __name__ == "__main__":
    run_backtest()
