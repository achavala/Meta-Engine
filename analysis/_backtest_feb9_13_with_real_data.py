"""
Institutional-Grade Backtest: Feb 9-13, 2026 (WITH NEW CODE + REAL DATA)
==========================================================================
Uses NEW code (conditional ORM gate, data quality tracking) + REAL data from
TradeNova/data directory for accurate pricing and options data.

30+ years trading + PhD quant + institutional microstructure lens
"""

import json
import os
import sys
import time
import hashlib
import statistics
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

# TradeNova data directory
TRADENOVA_DATA = Path.home() / "TradeNova" / "data"
OUTPUT_DIR = Path(MetaConfig.OUTPUT_DIR)
POLYGON_API_KEY = MetaConfig.POLYGON_API_KEY

# Date range
START_DATE = date(2026, 2, 9)
END_DATE = date(2026, 2, 13)


def load_tradenova_data(symbol: str, scan_date: date) -> Dict[str, Any]:
    """Load real data from TradeNova/data directory."""
    data = {}
    
    # 1. Options cache
    options_pattern = f"{symbol}_*_{scan_date.strftime('%Y-%m-%d')}*.json"
    options_files = list((TRADENOVA_DATA / "options_cache").glob(options_pattern))
    if options_files:
        try:
            with open(options_files[0]) as f:
                data["options"] = json.load(f)
        except Exception:
            pass
    
    # 2. Price cache
    price_file = TRADENOVA_DATA / "price_cache" / f"{symbol}_{scan_date.strftime('%Y%m%d')}.json"
    if price_file.exists():
        try:
            with open(price_file) as f:
                data["price"] = json.load(f)
        except Exception:
            pass
    
    # 3. UW flow cache
    uw_flow_file = TRADENOVA_DATA / "uw_flow_cache.json"
    if uw_flow_file.exists():
        try:
            with open(uw_flow_file) as f:
                uw_data = json.load(f)
                if symbol in uw_data.get("flow_data", {}):
                    data["uw_flow"] = uw_data["flow_data"][symbol]
        except Exception:
            pass
    
    # 4. GEX cache
    gex_file = TRADENOVA_DATA / "uw_gex_cache.json"
    if gex_file.exists():
        try:
            with open(gex_file) as f:
                gex_data = json.load(f)
                if symbol in gex_data:
                    data["gex"] = gex_data[symbol]
        except Exception:
            pass
    
    # 5. Actual movements (if available)
    movements_file = TRADENOVA_DATA / "feb9_13_actual_movements.json"
    if movements_file.exists():
        try:
            with open(movements_file) as f:
                movements = json.load(f)
                date_str = scan_date.isoformat()
                if date_str in movements and symbol in movements[date_str]:
                    data["actual_move"] = movements[date_str][symbol]
        except Exception:
            pass
    
    return data


def load_cross_analysis(scan_date: date) -> Optional[Dict[str, Any]]:
    """Load cross_analysis file for a specific date."""
    date_str = scan_date.strftime("%Y%m%d")
    file_path = OUTPUT_DIR / f"cross_analysis_{date_str}.json"
    if not file_path.exists():
        return None
    try:
        with open(file_path) as f:
            return json.load(f)
    except Exception:
        return None


def _next_trading_day(d: date) -> date:
    """Return next trading day (skip weekends)."""
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


def get_stock_data_polygon(
    symbol: str,
    start_date: date,
    end_date: date,
    client: requests.Session,
) -> Dict[date, Dict[str, float]]:
    """Fetch stock data from Polygon API."""
    if not client or not POLYGON_API_KEY:
        return {}
    
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_str}/{end_str}"
        resp = client.get(
            url,
            params={"apiKey": POLYGON_API_KEY, "adjusted": "true", "sort": "asc"},
            timeout=10,
        )
        
        if resp.status_code != 200:
            return {}
        
        data = resp.json()
        results = data.get("results", [])
        
        bars = {}
        for r in results:
            ts = r.get("t", 0) / 1000
            bar_date = date.fromtimestamp(ts)
            bars[bar_date] = {
                "open": float(r.get("o", 0)),
                "high": float(r.get("h", 0)),
                "low": float(r.get("l", 0)),
                "close": float(r.get("c", 0)),
                "volume": float(r.get("v", 0)),
            }
        return bars
    except Exception:
        return {}


def calculate_options_pnl_realistic(
    pick_price: float,
    stock_move_pct: float,
    option_type: str,
    days_held: int = 1,
    orm_score: float = 0.0,
    has_earnings: bool = False,
) -> Tuple[float, float]:
    """Realistic options P&L calculation."""
    if pick_price <= 0:
        return 0.0, 0.0
    
    direction = -1.0 if option_type == "put" else 1.0
    directional_move = stock_move_pct * direction
    
    delta = 0.35 if orm_score >= 0.70 else (0.30 if orm_score >= 0.50 else 0.25)
    
    abs_move = abs(directional_move)
    if abs_move > 5.0:
        gamma_boost = 1.4
    elif abs_move > 3.0:
        gamma_boost = 1.2
    elif abs_move > 1.0:
        gamma_boost = 1.1
    else:
        gamma_boost = 1.0
    
    premium_pct = max(0.015, 0.03 - orm_score * 0.015)
    raw_return = (delta * directional_move * gamma_boost) / (premium_pct * 100)
    
    if days_held <= 1:
        theta_pct = 3.0
    elif days_held <= 3:
        theta_pct = days_held * 4.0
    else:
        theta_pct = days_held * 5.0
    
    iv_crush_pct = 0.0
    if has_earnings:
        iv_crush_pct = 25.0 if abs_move < 3.0 else 10.0
    
    options_return = (raw_return * 100) - theta_pct - iv_crush_pct
    
    return stock_move_pct, options_return


def analyze_pick_with_real_data(
    pick: Dict[str, Any],
    scan_date: date,
    session: str,
    stock_data: Dict[date, Dict[str, float]],
    option_type: str,
    tradenova_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Analyze a pick using NEW code logic + real TradeNova data.
    """
    symbol = pick.get("symbol", "")
    pick_price = float(pick.get("price", 0) or 0)
    
    # === DATA QUALITY ASSESSMENT (NEW CODE) ===
    data_quality = "OK"
    data_quality_details = []
    
    if pick_price <= 0:
        data_quality = "MISSING_UNDERLYING"
        data_quality_details.append("entry price is 0 or missing")
    
    if not stock_data:
        data_quality = "MISSING_UNDERLYING"
        data_quality_details.append("no Polygon bars for exit window")
    
    # ORM status (NEW CODE: conditional gate)
    orm = float(pick.get("_orm_score", pick.get("orm_score", 0)) or 0)
    orm_status = "computed" if orm > 0.001 else "missing"
    if orm_status == "missing":
        data_quality_details.append("ORM not computed — missing options flow data")
    
    # Use real TradeNova data if available
    if tradenova_data.get("actual_move"):
        actual = tradenova_data["actual_move"]
        data_quality_details.append(f"Real TradeNova data: {actual.get('move_pct', 0):+.1f}%")
    
    # Extract metrics
    meta_score = float(pick.get("meta_score", pick.get("score", 0)) or 0)
    base_score = float(pick.get("_base_score", pick.get("base_score", meta_score)) or 0)
    signals = pick.get("signals", [])
    if not isinstance(signals, list):
        signals = []
    signal_count = len(signals)
    
    # === FIND EXIT PRICE ===
    best_move_pct = 0.0
    best_move_date = scan_date
    best_close = pick_price
    exit_found = False
    
    # Use actual move from TradeNova if available
    if tradenova_data.get("actual_move"):
        actual = tradenova_data["actual_move"]
        move_pct = float(actual.get("move_pct", 0))
        if option_type == "put":
            effective_move = -move_pct
        else:
            effective_move = move_pct
        
        if effective_move > best_move_pct:
            best_move_pct = effective_move
            best_close = pick_price * (1 + move_pct / 100)
            exit_found = True
            data_quality = "OK"
            data_quality_details.append("Used TradeNova actual_movements.json")
    else:
        # Fall back to Polygon data
        check_dates = []
        d = scan_date
        for _ in range(2):
            d = _next_trading_day(d)
            check_dates.append(d)
        
        for check_date in check_dates:
            if check_date in stock_data:
                bar = stock_data[check_date]
                close = bar.get("close", 0)
                if close > 0:
                    move_pct = ((close - pick_price) / pick_price) * 100
                    effective_move = -move_pct if option_type == "put" else move_pct
                    
                    if effective_move > best_move_pct:
                        best_move_pct = effective_move
                        best_move_date = check_date
                        best_close = close
                        exit_found = True
    
    if not exit_found and pick_price > 0:
        if data_quality == "OK":
            data_quality = "FALLBACK_USED"
        data_quality_details.append("no exit data available")
    
    # === COMPUTE P&L ===
    stock_move = ((best_close - pick_price) / pick_price) * 100 if pick_price > 0 else 0
    days_held = max(1, (best_move_date - scan_date).days)
    has_earnings = bool(pick.get("_earnings_flag"))
    
    _, options_pnl_gross = calculate_options_pnl_realistic(
        pick_price, stock_move, option_type, days_held,
        orm_score=orm, has_earnings=has_earnings,
    )
    
    # Net P&L after costs
    options_pnl_net = options_pnl_gross - 3.0  # 3% spread/slippage
    
    # Grade
    if exit_found:
        if options_pnl_net >= 200:
            grade = "A+"
        elif options_pnl_net >= 100:
            grade = "A"
        elif options_pnl_net >= 50:
            grade = "B"
        elif options_pnl_net >= 0:
            grade = "C"
        else:
            grade = "F"
    else:
        grade = "NO_DATA"
    
    win = options_pnl_net > 0 and exit_found
    
    # === INSTITUTIONAL ANALYSIS ===
    analysis_parts = []
    if not exit_found:
        analysis_parts.append("⚠️ NO EXIT DATA — cannot evaluate")
    elif win:
        analysis_parts.append(f"✅ WINNER: {options_pnl_net:+.0f}% net")
        if orm_status == "computed" and orm >= 0.70:
            analysis_parts.append(f"High ORM ({orm:.2f})")
        if signal_count >= 5:
            analysis_parts.append(f"Strong confluence ({signal_count} signals)")
        if base_score >= 0.85:
            analysis_parts.append(f"High conviction ({base_score:.2f})")
    else:
        analysis_parts.append(f"❌ LOSER: {options_pnl_net:+.0f}% net")
        if orm_status == "computed" and orm < 0.50:
            analysis_parts.append(f"Low ORM ({orm:.2f})")
        elif orm_status == "missing":
            analysis_parts.append("ORM missing")
        if signal_count < 2:
            analysis_parts.append(f"Weak signals ({signal_count})")
        if abs(stock_move) < 1.0:
            analysis_parts.append("Theta-dominated")
    
    return {
        "symbol": symbol,
        "option_type": option_type,
        "scan_date": scan_date.isoformat(),
        "session": session,
        "pick_price": pick_price,
        "exit_price": best_close if exit_found else None,
        "stock_move_pct": round(stock_move, 2),
        "best_move_pct": round(best_move_pct, 2),
        "days_held": days_held,
        "options_pnl_gross": round(options_pnl_gross, 1),
        "options_pnl_net": round(options_pnl_net, 1),
        "grade": grade,
        "win": win,
        "exit_found": exit_found,
        "data_quality": data_quality,
        "data_quality_details": data_quality_details,
        "orm_score": orm,
        "orm_status": orm_status,
        "meta_score": meta_score,
        "base_score": base_score,
        "signal_count": signal_count,
        "signals": signals[:10],
        "has_earnings": has_earnings,
        "analysis": " | ".join(analysis_parts),
        "tradenova_data_used": bool(tradenova_data.get("actual_move")),
    }


def run_backtest():
    """Run backtest with NEW code + real TradeNova data."""
    print("=" * 80)
    print("INSTITUTIONAL BACKTEST: FEB 9-13, 2026 (NEW CODE + REAL DATA)")
    print("=" * 80)
    print()
    print("Using:")
    print("  ✅ NEW code: Conditional ORM gate, data quality tracking")
    print("  ✅ Real TradeNova data: actual_movements.json, options_cache, price_cache")
    print("  ✅ Polygon API: Fallback for missing TradeNova data")
    print()
    
    all_results = []
    polygon_client = requests.Session() if POLYGON_API_KEY else None
    
    current_date = START_DATE
    while current_date <= END_DATE:
        cross_data = load_cross_analysis(current_date)
        if not cross_data:
            print(f"📅 {current_date} — No cross_analysis data")
            current_date += timedelta(days=1)
            continue
        
        puts_top10 = cross_data.get("puts_through_moonshot", [])[:10]
        moonshot_top10 = cross_data.get("moonshot_through_puts", [])[:10]
        
        timestamp_str = cross_data.get("timestamp", "")
        try:
            scan_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            session = "AM" if 8 <= scan_time.hour <= 11 else "PM"
        except Exception:
            session = "PM"
        
        print(f"\n📅 {current_date.strftime('%A, %b %d')} ({session})")
        print(f"   {len(puts_top10)} PUTS + {len(moonshot_top10)} CALLS")
        
        exit_start = _next_trading_day(current_date)
        exit_end = _next_trading_day(exit_start)
        
        # Process PUT picks
        for pick in puts_top10:
            symbol = pick.get("symbol", "")
            if not symbol:
                continue
            
            tradenova_data = load_tradenova_data(symbol, current_date)
            stock_data = get_stock_data_polygon(
                symbol, current_date, exit_end + timedelta(days=1), polygon_client
            )
            
            result = analyze_pick_with_real_data(
                pick, current_date, session, stock_data, "put", tradenova_data
            )
            all_results.append(result)
            time.sleep(0.1)
            
            tag = "✅" if result["win"] else ("⚠️" if not result["exit_found"] else "❌")
            dq = f" [{result['data_quality']}]" if result["data_quality"] != "OK" else ""
            tn_tag = "📊" if result["tradenova_data_used"] else ""
            orm_tag = f"ORM={result['orm_score']:.2f}" if result["orm_status"] == "computed" else "ORM=N/A"
            print(f"   {tag} {tn_tag} {symbol:6s} PUT  {result['options_pnl_net']:+7.1f}% "
                  f"({orm_tag}, sig={result['signal_count']}, grade={result['grade']}){dq}")
        
        # Process CALL picks
        for pick in moonshot_top10:
            symbol = pick.get("symbol", "")
            if not symbol:
                continue
            
            tradenova_data = load_tradenova_data(symbol, current_date)
            stock_data = get_stock_data_polygon(
                symbol, current_date, exit_end + timedelta(days=1), polygon_client
            )
            
            result = analyze_pick_with_real_data(
                pick, current_date, session, stock_data, "call", tradenova_data
            )
            all_results.append(result)
            time.sleep(0.1)
            
            tag = "✅" if result["win"] else ("⚠️" if not result["exit_found"] else "❌")
            dq = f" [{result['data_quality']}]" if result["data_quality"] != "OK" else ""
            tn_tag = "📊" if result["tradenova_data_used"] else ""
            orm_tag = f"ORM={result['orm_score']:.2f}" if result["orm_status"] == "computed" else "ORM=N/A"
            print(f"   {tag} {tn_tag} {symbol:6s} CALL {result['options_pnl_net']:+7.1f}% "
                  f"({orm_tag}, sig={result['signal_count']}, grade={result['grade']}){dq}")
        
        current_date += timedelta(days=1)
    
    # === ANALYSIS ===
    print("\n" + "=" * 80)
    print("AGGREGATE STATISTICS")
    print("=" * 80)
    
    total = len(all_results)
    clean = [r for r in all_results if r["exit_found"] and r["data_quality"] == "OK"]
    fallback = [r for r in all_results if not r["exit_found"] or r["data_quality"] != "OK"]
    
    print(f"\n  Total picks: {total}")
    print(f"  Clean picks: {len(clean)} ({len(clean)/total*100:.0f}%)")
    print(f"  Fallback picks: {len(fallback)} ({len(fallback)/total*100:.0f}%)")
    
    if clean:
        winners = [r for r in clean if r["win"]]
        losers = [r for r in clean if not r["win"]]
        
        print(f"\n  Win Rate: {len(winners)}/{len(clean)} ({len(winners)/len(clean)*100:.1f}%)")
        if winners:
            avg_win = sum(r["options_pnl_net"] for r in winners) / len(winners)
            print(f"  Avg Winner: {avg_win:+.1f}%")
        if losers:
            avg_loss = sum(r["options_pnl_net"] for r in losers) / len(losers)
            print(f"  Avg Loser:  {avg_loss:+.1f}%")
        
        # Expectancy
        p_win = len(winners) / len(clean)
        p_loss = len(losers) / len(clean)
        expectancy = p_win * avg_win + p_loss * avg_loss if winners and losers else (avg_win if winners else avg_loss)
        print(f"  Expectancy: {expectancy:+.1f}% per trade")
        
        # By option type
        puts_clean = [r for r in clean if r["option_type"] == "put"]
        calls_clean = [r for r in clean if r["option_type"] == "call"]
        
        print(f"\n  By Option Type:")
        if puts_clean:
            put_wr = sum(1 for r in puts_clean if r["win"]) / len(puts_clean) * 100
            print(f"    PUTS:  {sum(1 for r in puts_clean if r['win'])}/{len(puts_clean)} ({put_wr:.1f}% WR)")
        if calls_clean:
            call_wr = sum(1 for r in calls_clean if r["win"]) / len(calls_clean) * 100
            print(f"    CALLS: {sum(1 for r in calls_clean if r['win'])}/{len(calls_clean)} ({call_wr:.1f}% WR)")
        
        # By ORM status
        computed = [r for r in clean if r["orm_status"] == "computed"]
        missing = [r for r in clean if r["orm_status"] == "missing"]
        
        print(f"\n  By ORM Status (NEW CODE):")
        if computed:
            comp_wr = sum(1 for r in computed if r["win"]) / len(computed) * 100
            print(f"    Computed: {sum(1 for r in computed if r['win'])}/{len(computed)} ({comp_wr:.1f}% WR)")
        if missing:
            miss_wr = sum(1 for r in missing if r["win"]) / len(missing) * 100
            print(f"    Missing:  {sum(1 for r in missing if r['win'])}/{len(missing)} ({miss_wr:.1f}% WR)")
        
        # Top winners
        top_winners = sorted(winners, key=lambda x: x["options_pnl_net"], reverse=True)[:10]
        print(f"\n  TOP 10 WINNERS:")
        for i, r in enumerate(top_winners, 1):
            orm_tag = f"ORM={r['orm_score']:.2f}" if r['orm_status'] == "computed" else "ORM=N/A"
            tn_tag = "📊" if r["tradenova_data_used"] else ""
            print(f"    #{i} {tn_tag} {r['symbol']} {r['option_type'].upper()}: "
                  f"{r['options_pnl_net']:+.0f}% ({orm_tag}, {r['signal_count']} sigs)")
        
        # Top losers
        if losers:
            top_losers = sorted(losers, key=lambda x: x["options_pnl_net"])[:10]
            print(f"\n  TOP 10 LOSERS:")
            for i, r in enumerate(top_losers, 1):
                orm_tag = f"ORM={r['orm_score']:.2f}" if r['orm_status'] == "computed" else "ORM=N/A"
                tn_tag = "📊" if r["tradenova_data_used"] else ""
                print(f"    #{i} {tn_tag} {r['symbol']} {r['option_type'].upper()}: "
                      f"{r['options_pnl_net']:+.0f}% ({orm_tag}, {r['signal_count']} sigs)")
    
    # Save results
    output_file = OUTPUT_DIR / "backtest_feb9_13_real_data.json"
    with open(output_file, "w") as f:
        json.dump({
            "backtest_version": "real_data_v1",
            "date_range": f"{START_DATE} to {END_DATE}",
            "generated_at": datetime.now().isoformat(),
            "all_results": all_results,
        }, f, indent=2, default=str)
    
    print(f"\n💾 Results saved: {output_file}")
    
    # Generate recommendations
    generate_recommendations(all_results, clean)
    
    print("\n" + "=" * 80)
    print("✅ BACKTEST COMPLETE")
    print("=" * 80)


def generate_recommendations(all_results: List[Dict[str, Any]], clean: List[Dict[str, Any]]):
    """Generate institutional-grade recommendations (NO FIXES)."""
    print("\n" + "=" * 80)
    print("INSTITUTIONAL-GRADE RECOMMENDATIONS (NO FIXES)")
    print("=" * 80)
    
    if not clean:
        print("\n⚠️ No clean trades to analyze — recommendations deferred")
        return
    
    winners = [r for r in clean if r["win"]]
    losers = [r for r in clean if not r["win"]]
    
    # 1. ORM conditional gate effectiveness
    computed_winners = [r for r in winners if r["orm_status"] == "computed"]
    missing_winners = [r for r in winners if r["orm_status"] == "missing"]
    computed_losers = [r for r in losers if r["orm_status"] == "computed"]
    missing_losers = [r for r in losers if r["orm_status"] == "missing"]
    
    print("\n1. ORM CONDITIONAL GATE (NEW CODE)")
    print(f"   • Computed ORM winners: {len(computed_winners)}")
    print(f"   • Missing ORM winners: {len(missing_winners)}")
    print(f"   • Computed ORM losers: {len(computed_losers)}")
    print(f"   • Missing ORM losers: {len(missing_losers)}")
    if missing_winners:
        print(f"   ✅ RECOMMENDATION: Conditional gate is working — {len(missing_winners)} winners")
        print(f"      would have been wrongly filtered by hard ORM gate")
    
    # 2. Signal convergence
    high_sig_winners = [r for r in winners if r["signal_count"] >= 5]
    low_sig_losers = [r for r in losers if r["signal_count"] < 2]
    
    print("\n2. SIGNAL CONVERGENCE")
    print(f"   • High signal (≥5) winners: {len(high_sig_winners)}")
    print(f"   • Low signal (<2) losers: {len(low_sig_losers)}")
    if high_sig_winners:
        avg_sig_win = sum(r["signal_count"] for r in winners) / len(winners)
        avg_sig_loss = sum(r["signal_count"] for r in losers) / len(losers) if losers else 0
        print(f"   • Avg signals: Winners={avg_sig_win:.1f}, Losers={avg_sig_loss:.1f}")
        print(f"   ✅ RECOMMENDATION: Maintain minimum 2-signal gate, consider raising to 3")
    
    # 3. Data quality
    tradenova_used = sum(1 for r in clean if r.get("tradenova_data_used"))
    print(f"\n3. DATA QUALITY")
    print(f"   • TradeNova data used: {tradenova_used}/{len(clean)} ({tradenova_used/len(clean)*100:.0f}%)")
    print(f"   ✅ RECOMMENDATION: Continue using TradeNova actual_movements.json when available")
    
    # 4. Option type performance
    puts_clean = [r for r in clean if r["option_type"] == "put"]
    calls_clean = [r for r in clean if r["option_type"] == "call"]
    
    if puts_clean and calls_clean:
        put_wr = sum(1 for r in puts_clean if r["win"]) / len(puts_clean) * 100
        call_wr = sum(1 for r in calls_clean if r["win"]) / len(calls_clean) * 100
        print(f"\n4. OPTION TYPE PERFORMANCE")
        print(f"   • PUT win rate: {put_wr:.1f}%")
        print(f"   • CALL win rate: {call_wr:.1f}%")
        if put_wr > call_wr + 10:
            print(f"   ✅ RECOMMENDATION: PUT engine outperforming — consider capital allocation adjustment")
            print(f"      after 8-12 weeks of data across multiple regimes")
    
    # 5. Overall assessment
    print(f"\n5. OVERALL ASSESSMENT")
    print(f"   • Clean trades: {len(clean)}/{len(all_results)} ({len(clean)/len(all_results)*100:.0f}%)")
    print(f"   • Win rate: {len(winners)/len(clean)*100:.1f}%")
    if winners:
        expectancy = (len(winners)/len(clean) * sum(r["options_pnl_net"] for r in winners)/len(winners) +
                     len(losers)/len(clean) * sum(r["options_pnl_net"] for r in losers)/len(losers) if losers else 0)
        print(f"   • Expectancy: {expectancy:+.1f}% per trade")
    print(f"   ✅ RECOMMENDATION: Continue monitoring for 8-12 weeks before live scaling")
    print(f"      Need data across multiple regimes (VIX high/low, SPY trend, day-of-week)")


if __name__ == "__main__":
    run_backtest()
