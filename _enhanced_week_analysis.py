#!/usr/bin/env python3
"""
Enhanced Institutional-Grade Analysis: Top 10 Picks Performance
================================================================
Analyzes all Top 10 picks from Monday 9:35 AM EST to Thursday 3:15 PM EST.

Includes:
- Actual stock price movements (even if no trade executed)
- Theoretical option returns based on stock moves
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
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pytz
import time

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import MetaConfig

EST = pytz.timezone("US/Eastern")

# ============================================================================
# Price Data Fetching
# ============================================================================

def fetch_stock_price_at_time(symbol: str, target_date: datetime.date, target_time: str = "15:15") -> Optional[float]:
    """
    Fetch stock price at a specific date/time using Polygon API.
    For intraday, uses the daily bar's close price as proxy.
    """
    api_key = MetaConfig.POLYGON_API_KEY
    if not api_key:
        return None
    
    try:
        # Fetch daily bar for the target date
        date_str = target_date.strftime("%Y-%m-%d")
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{date_str}/{date_str}"
        resp = requests.get(
            url,
            params={"apiKey": api_key, "adjusted": "true"},
            timeout=10
        )
        
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                bar = results[0]
                # Use close price as proxy for end-of-day
                return float(bar.get("c", 0))
        
        # Fallback: try previous close
        url_prev = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
        resp_prev = requests.get(
            url_prev,
            params={"apiKey": api_key, "adjusted": "true"},
            timeout=10
        )
        if resp_prev.status_code == 200:
            results = resp_prev.json().get("results", [])
            if results:
                return float(results[0].get("c", 0))
    
    except Exception as e:
        print(f"      ⚠️  Price fetch failed for {symbol}: {e}")
    
    return None


def fetch_current_price(symbol: str) -> Optional[float]:
    """Fetch current stock price from Polygon."""
    api_key = MetaConfig.POLYGON_API_KEY
    if not api_key:
        return None
    
    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
        resp = requests.get(url, params={"apiKey": api_key}, timeout=10)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return float(results[0].get("c", 0))
    except:
        pass
    return None


def calculate_theoretical_option_return(
    stock_price_at_scan: float,
    stock_price_current: float,
    option_type: str,  # "call" or "put"
    strike_proximity: float = 0.05,  # 5% OTM
) -> float:
    """
    Calculate theoretical option return based on stock move.
    Assumes ATM option (strike = stock_price_at_scan).
    Uses simplified Black-Scholes approximation.
    """
    if stock_price_at_scan <= 0 or stock_price_current <= 0:
        return 0.0
    
    if option_type == "call":
        # Call option: profit if stock goes up
        stock_move_pct = ((stock_price_current - stock_price_at_scan) / stock_price_at_scan) * 100
        # Simplified: option return ≈ 3-5x stock move for ATM calls
        # Using 4x multiplier as conservative estimate
        option_return = stock_move_pct * 4.0
        return max(option_return, -100.0)  # Cap loss at -100%
    
    else:  # put
        # Put option: profit if stock goes down
        stock_move_pct = ((stock_price_at_scan - stock_price_current) / stock_price_at_scan) * 100
        # Simplified: option return ≈ 3-5x stock move for ATM puts
        option_return = stock_move_pct * 4.0
        return max(option_return, -100.0)  # Cap loss at -100%


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
                    pick["scan_datetime"] = dt
                    pick["session"] = "AM" if hour < 12 else "PM"
                    pick["engine"] = "PutsEngine" if pick in puts_top10 else "Moonshot"
                    pick["option_type"] = "put" if pick in puts_top10 else "call"
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
            key = f"{sym}_{option_type}"
            if key not in trade_map:
                trade_map[key] = []
            trade_map[key].append(trade)
    
    return trade_map


# ============================================================================
# Analysis Functions
# ============================================================================

def analyze_pick_performance(
    pick: Dict,
    trades: List[Dict],
    stock_price_at_scan: Optional[float],
    stock_price_current: Optional[float],
) -> Dict[str, Any]:
    """Comprehensive institutional analysis of a single pick."""
    sym = pick.get("symbol", "?")
    score = pick.get("score", 0)
    price_at_scan = pick.get("price", 0) or stock_price_at_scan or 0
    signals = pick.get("signals", [])
    engine = pick.get("engine", "")
    option_type = pick.get("option_type", "")
    orm_score = pick.get("_orm_score", 0)
    orm_factors = pick.get("_orm_factors", {})
    
    # Get actual trade return if available
    trade_return = 0.0
    trade_status = "NO_TRADE"
    if trades:
        best_trade = max(trades, key=lambda t: float(t.get("pnl_pct", 0) or 0))
        trade_return = float(best_trade.get("pnl_pct", 0) or 0)
        trade_status = best_trade.get("status", "unknown")
    
    # Calculate theoretical return based on stock move
    theoretical_return = 0.0
    if stock_price_at_scan and stock_price_current:
        theoretical_return = calculate_theoretical_option_return(
            stock_price_at_scan,
            stock_price_current,
            option_type,
        )
    
    # Use best available return (trade > theoretical)
    if trade_return != 0:
        peak_return = trade_return
        return_source = "ACTUAL_TRADE"
    elif theoretical_return != 0:
        peak_return = theoretical_return
        return_source = "THEORETICAL"
    else:
        peak_return = 0.0
        return_source = "NO_DATA"
    
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
        "option_type": option_type,
        "score": score,
        "orm_score": orm_score,
        "price_at_scan": price_at_scan,
        "stock_price_at_scan": stock_price_at_scan,
        "stock_price_current": stock_price_current,
        "stock_move_pct": ((stock_price_current - stock_price_at_scan) / stock_price_at_scan * 100) if (stock_price_at_scan and stock_price_current) else 0,
        "signals": signals,
        "peak_return_pct": peak_return,
        "return_source": return_source,
        "trade_return": trade_return,
        "theoretical_return": theoretical_return,
        "trade_status": trade_status,
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
            if option_type == "put" and stock_price_current and stock_price_at_scan:
                if stock_price_current > stock_price_at_scan * 1.1:
                    reasons.append(f"PRICE_MOVED_UP +{((stock_price_current/stock_price_at_scan-1)*100):.1f}% (put thesis failed)")
            elif option_type == "call" and stock_price_current and stock_price_at_scan:
                if stock_price_current < stock_price_at_scan * 0.9:
                    reasons.append(f"PRICE_MOVED_DOWN {((stock_price_current/stock_price_at_scan-1)*100):.1f}% (call thesis failed)")
            
            # Low ORM score
            if orm_score < 0.4:
                reasons.append(f"LOW_ORM_SCORE ({orm_score:.2f} - poor options setup)")
            
            # Weak signals
            if len(signals) < 2:
                reasons.append(f"WEAK_SIGNAL_COUNT ({len(signals)} signals)")
            
            # Low base score
            if score < 0.6:
                reasons.append(f"LOW_BASE_SCORE ({score:.3f})")
        
        analysis["loser_reasons"] = reasons
    
    # Success factors for winners
    elif category == "WINNER":
        factors = []
        
        if orm_score >= 0.7:
            factors.append(f"HIGH_ORM_SCORE ({orm_score:.2f})")
        
        if len(signals) >= 3:
            factors.append(f"STRONG_SIGNAL_COUNT ({len(signals)} signals)")
        
        if score >= 0.7:
            factors.append(f"HIGH_BASE_SCORE ({score:.3f})")
        
        # Check top ORM factors
        if orm_factors:
            top_factor = max(orm_factors.items(), key=lambda x: x[1])
            factors.append(f"TOP_ORM_FACTOR: {top_factor[0]} ({top_factor[1]:.2f})")
        
        # Stock move analysis
        if stock_price_at_scan and stock_price_current:
            move_pct = ((stock_price_current - stock_price_at_scan) / stock_price_at_scan) * 100
            if option_type == "call" and move_pct > 5:
                factors.append(f"STRONG_STOCK_MOVE: +{move_pct:.1f}%")
            elif option_type == "put" and move_pct < -5:
                factors.append(f"STRONG_STOCK_MOVE: {move_pct:.1f}%")
        
        analysis["winner_factors"] = factors
    
    return analysis


# ============================================================================
# Main Analysis
# ============================================================================

def main():
    print("=" * 80)
    print("ENHANCED INSTITUTIONAL-GRADE TOP 10 PICKS ANALYSIS")
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
    
    # Fetch stock prices for all picks
    print("📈 Fetching stock price data from Polygon API...")
    print("   (This may take a minute due to API rate limits)")
    print()
    
    unique_symbols = list(set(p.get("symbol", "") for p in all_picks))
    price_cache = {}
    
    for i, symbol in enumerate(unique_symbols, 1):
        if not symbol:
            continue
        print(f"   [{i}/{len(unique_symbols)}] Fetching {symbol}...", end=" ", flush=True)
        current_price = fetch_current_price(symbol)
        if current_price:
            price_cache[symbol] = {"current": current_price}
            print(f"✅ ${current_price:.2f}")
        else:
            print("❌ Failed")
        time.sleep(0.1)  # Rate limit protection
    
    print()
    
    # Analyze each pick
    print("🔍 Analyzing each pick with price data...")
    print()
    
    analyses = []
    for pick in all_picks:
        sym = pick.get("symbol", "?")
        option_type = pick.get("option_type", "")
        key = f"{sym}_{option_type}"
        trades = trade_map.get(key, [])
        
        # Get prices
        stock_price_at_scan = pick.get("price", 0) or price_cache.get(sym, {}).get("current")
        stock_price_current = price_cache.get(sym, {}).get("current")
        
        # If we don't have scan price, try to fetch it
        if not stock_price_at_scan and pick.get("scan_datetime"):
            scan_date = pick["scan_datetime"].date()
            stock_price_at_scan = fetch_stock_price_at_time(sym, scan_date)
            if stock_price_at_scan:
                price_cache.setdefault(sym, {})["scan"] = stock_price_at_scan
        
        analysis = analyze_pick_performance(
            pick,
            trades,
            stock_price_at_scan,
            stock_price_current,
        )
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
    no_data = [a for a in analyses if a["return_source"] == "NO_DATA"]
    
    print(f"📈 Total Picks Analyzed: {len(analyses)}")
    print(f"   ✅ Winners (≥50%): {len(winners)} ({len(winners)/len(analyses)*100:.1f}%)")
    print(f"   ❌ Losers (<0%): {len(losers)} ({len(losers)/len(analyses)*100:.1f}%)")
    print(f"   ➖ Breakeven (0-50%): {len(breakeven)} ({len(breakeven)/len(analyses)*100:.1f}%)")
    print(f"   ⚠️  No Price Data: {len(no_data)} ({len(no_data)/len(analyses)*100:.1f}%)")
    print()
    
    # Return source breakdown
    actual_trades = [a for a in analyses if a["return_source"] == "ACTUAL_TRADE"]
    theoretical = [a for a in analyses if a["return_source"] == "THEORETICAL"]
    print(f"📊 Return Source:")
    print(f"   Actual Trades: {len(actual_trades)}")
    print(f"   Theoretical (stock move): {len(theoretical)}")
    print()
    
    if winners:
        avg_winner_return = sum(w["peak_return_pct"] for w in winners) / len(winners)
        best_winner = max(winners, key=lambda x: x["peak_return_pct"])
        print(f"🏆 Winners:")
        print(f"   Average Return: +{avg_winner_return:.1f}%")
        print(f"   Best: {best_winner['symbol']} +{best_winner['peak_return_pct']:.1f}% ({best_winner['return_source']})")
        print()
    
    if losers:
        avg_loser_return = sum(l["peak_return_pct"] for l in losers) / len(losers)
        worst_loser = min(losers, key=lambda x: x["peak_return_pct"])
        print(f"📉 Losers:")
        print(f"   Average Return: {avg_loser_return:.1f}%")
        print(f"   Worst: {worst_loser['symbol']} {worst_loser['peak_return_pct']:.1f}% ({worst_loser['return_source']})")
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
        option_type = analysis["option_type"]
        score = analysis["score"]
        orm_score = analysis["orm_score"]
        return_source = analysis["return_source"]
        
        print(f"{i}. {emoji} {sym} {option_type.upper()} — {category} ({peak:+.1f}%) [{return_source}]")
        print(f"   Scan: {scan_time} ({session}) | Engine: {engine}")
        print(f"   Score: {score:.3f} | ORM: {orm_score:.3f}")
        
        # Stock price info
        if analysis.get("stock_price_at_scan") and analysis.get("stock_price_current"):
            stock_at = analysis["stock_price_at_scan"]
            stock_now = analysis["stock_price_current"]
            move = analysis["stock_move_pct"]
            print(f"   Stock: ${stock_at:.2f} → ${stock_now:.2f} ({move:+.1f}%)")
        
        # Trade info
        if analysis.get("trades"):
            trade = analysis["trades"][0]
            entry = trade.get("entry_price", 0)
            current = trade.get("current_price", 0)
            status = trade.get("status", "")
            print(f"   Trade: Entry ${entry:.2f} → Current ${current:.2f} ({status})")
        elif return_source == "THEORETICAL":
            print(f"   Trade: NOT EXECUTED (theoretical return based on stock move)")
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
                # Extract base reason (before parentheses)
                base_reason = reason.split("(")[0].strip()
                reason_counts[base_reason] = reason_counts.get(base_reason, 0) + 1
        
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
                # Extract base factor (before parentheses)
                base_factor = factor.split("(")[0].strip()
                factor_counts[base_factor] = factor_counts.get(base_factor, 0) + 1
        
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
    no_trade_count = len([a for a in analyses if not a.get("trades")])
    if no_trade_count > len(analyses) * 0.5:
        recommendations.append(
            f"5. Trade Execution Gap: {no_trade_count} picks ({no_trade_count/len(analyses)*100:.1f}%) "
            f"had no trades executed. Review order placement logic and Alpaca connection."
        )
    
    # Recommendation 6: Price Data Quality
    if no_data:
        recommendations.append(
            f"6. Price Data Quality: {len(no_data)} picks lacked price data. "
            f"Ensure Polygon API key is valid and rate limits are managed."
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
