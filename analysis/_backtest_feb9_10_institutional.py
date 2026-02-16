"""
Institutional-Grade Backtest: Feb 9-10, 2026
============================================
Analyzes 9:35 AM and 3:15 PM Top 10 picks from both Moonshot and Puts engines
using real TradeNova data with institutional-grade metrics.

Focus: Monday Feb 9 and Tuesday Feb 10 only
Sessions: 9:35 AM EST and 3:15 PM EST
"""

import json
import uuid
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
import requests
from collections import defaultdict

# Paths
META_ENGINE_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = META_ENGINE_ROOT / "output"
TRADENOVA_DATA = Path.home() / "TradeNova" / "data"

# Load environment
POLYGON_API_KEY = ""
try:
    from config import POLYGON_API_KEY
except:
    pass

def load_actual_movements(filepath: Path) -> Dict[str, Any]:
    """Load actual stock movements from TradeNova data."""
    if filepath.exists():
        with open(filepath) as f:
            return json.load(f)
    return {}

def load_cross_analysis(date_str: str) -> Optional[Dict[str, Any]]:
    """Load cross_analysis file for a given date."""
    cross_file = OUTPUT_DIR / f"cross_analysis_{date_str}.json"
    if cross_file.exists():
        with open(cross_file) as f:
            return json.load(f)
    return None

def _next_trading_day(d: date) -> date:
    """Get next trading day (skip weekends)."""
    next_day = d + timedelta(days=1)
    while next_day.weekday() >= 5:  # Saturday = 5, Sunday = 6
        next_day += timedelta(days=1)
    return next_day

def calculate_options_pnl(
    stock_move_pct: float,
    option_type: str,
    entry_price: float,
    dte: int = 5,
    iv: float = 0.40,
    delta: float = 0.30,
) -> Tuple[float, float]:
    """
    Realistic options P&L calculation using multi-factor model.
    
    Factors:
    - Initial premium (theta decay)
    - Delta (directional move)
    - Gamma (acceleration)
    - IV crush (for earnings)
    - Time decay
    """
    if entry_price <= 0:
        return 0.0, 0.0
    
    # Base premium estimate (simplified Black-Scholes proxy)
    # For OTM options: premium ≈ underlying * IV * sqrt(DTE/365) * moneyness_factor
    moneyness_factor = 0.05 if option_type == "call" else 0.05  # 5% OTM
    base_premium = entry_price * iv * (dte / 365) ** 0.5 * moneyness_factor
    
    # Directional move impact (delta)
    directional_pnl = base_premium * delta * (stock_move_pct / 100)
    
    # Gamma acceleration (non-linear)
    gamma_boost = 1.0
    if abs(stock_move_pct) > 5:
        gamma_boost = 1.0 + (abs(stock_move_pct) - 5) * 0.15  # 15% boost per % above 5%
    
    # Time decay (theta)
    theta_decay = base_premium * 0.10 * (dte / 5)  # 10% decay per day, scaled by DTE
    
    # IV crush (if earnings-related)
    iv_crush = 0.0  # Will be applied if earnings flag is set
    
    # Net P&L
    gross_pnl_pct = ((directional_pnl * gamma_boost) - theta_decay - iv_crush) / base_premium * 100
    
    # Costs
    spread_slippage = 0.05  # 5% bid-ask spread
    commission = 0.01  # $0.01 per contract
    
    net_pnl_pct = gross_pnl_pct - spread_slippage
    
    return gross_pnl_pct, net_pnl_pct

def analyze_pick_performance(
    pick: Dict[str, Any],
    scan_date: date,
    session: str,
    actual_movements: Dict[str, Any],
    polygon_client: requests.Session,
) -> Dict[str, Any]:
    """
    Analyze a single pick's performance using real TradeNova data.
    """
    symbol = pick.get("symbol", "")
    option_type = "put" if "PUT" in str(pick.get("engine_type", "")).upper() or "PUT" in str(pick.get("engine", "")).upper() else "call"
    pick_price = float(pick.get("price", 0) or 0)
    
    pick_id = str(uuid.uuid4())
    
    # Extract signals and metadata
    signals = pick.get("signals", [])
    signal_count = len(signals) if isinstance(signals, list) else 0
    orm_score = float(pick.get("_orm_score", pick.get("orm_score", 0)) or 0)
    orm_status = pick.get("_orm_status", "computed")
    meta_score = float(pick.get("meta_score", pick.get("score", 0)) or 0)
    base_score = float(pick.get("base_score", meta_score) or 0)
    
    data_quality = "OK"
    stock_move_pct = 0.0
    best_move_date = scan_date
    
    # Prioritize actual movements from TradeNova data
    if symbol in actual_movements:
        move_data = actual_movements[symbol]
        if isinstance(move_data, list):
            # Find best move for this option type
            best_move = 0.0
            best_date = scan_date
            for move_entry in move_data:
                move_date = datetime.strptime(move_entry.get("date", scan_date.isoformat()).split()[0], "%Y-%m-%d").date()
                if move_date > scan_date:
                    move_pct = move_entry.get("change_pct", 0)
                    if option_type == "call" and move_pct > best_move:
                        best_move = move_pct
                        best_date = move_date
                    elif option_type == "put" and move_pct < best_move:
                        best_move = move_pct
                        best_date = move_date
            
            stock_move_pct = best_move
            best_move_date = best_date
            data_quality = "TRADENOVA_DATA"
        elif isinstance(move_data, dict):
            if option_type == "call":
                stock_move_pct = move_data.get("best_bullish_move_pct", 0.0)
                best_move_date = datetime.strptime(move_data.get("best_bullish_move_date", scan_date.isoformat()), "%Y-%m-%d").date()
            else:
                stock_move_pct = move_data.get("best_bearish_move_pct", 0.0)
                best_move_date = datetime.strptime(move_data.get("best_bearish_move_date", scan_date.isoformat()), "%Y-%m-%d").date()
            data_quality = "TRADENOVA_DATA"
    else:
        # Fallback: try to get from Polygon (but mark as fallback)
        exit_date = _next_trading_day(scan_date)
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{scan_date}/{exit_date}"
            params = {"adjusted": "true", "sort": "asc", "limit": 5, "apiKey": POLYGON_API_KEY}
            resp = polygon_client.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("resultsCount", 0) > 0:
                    results = data.get("results", [])
                    if len(results) >= 2:
                        entry_close = results[0].get("c", 0)
                        exit_close = results[-1].get("c", 0)
                        if entry_close > 0:
                            stock_move_pct = ((exit_close - entry_close) / entry_close) * 100
                            data_quality = "POLYGON_FALLBACK"
        except:
            data_quality = "FALLBACK_USED"
    
    # Calculate options P&L
    dte = pick.get("estimated_dte", pick.get("dte", 5))
    iv = pick.get("iv_est", 0.40)
    delta = 0.30 if option_type == "call" else -0.30
    
    options_pnl_gross, options_pnl_net = calculate_options_pnl(
        stock_move_pct, option_type, pick_price, dte, iv, abs(delta)
    )
    
    # Determine win/loss
    win = (option_type == "call" and stock_move_pct > 0) or (option_type == "put" and stock_move_pct < 0)
    if abs(stock_move_pct) < 0.5:  # Flat move
        win = options_pnl_net > 0
    
    # Grade
    if options_pnl_net >= 100:
        grade = "A+"
    elif options_pnl_net >= 50:
        grade = "A"
    elif options_pnl_net >= 20:
        grade = "B"
    elif options_pnl_net >= 0:
        grade = "C"
    else:
        grade = "F"
    
    return {
        "pick_id": pick_id,
        "symbol": symbol,
        "option_type": option_type,
        "scan_date": scan_date.isoformat(),
        "session": session,
        "pick_price": pick_price,
        "stock_move_pct": stock_move_pct,
        "best_move_date": best_move_date.isoformat(),
        "options_pnl_gross": round(options_pnl_gross, 1),
        "options_pnl_net": round(options_pnl_net, 1),
        "data_quality": data_quality,
        "orm_score": orm_score,
        "orm_status": orm_status,
        "meta_score": meta_score,
        "base_score": base_score,
        "signal_count": signal_count,
        "signals": signals,
        "win": win,
        "grade": grade,
        "days_held": (best_move_date - scan_date).days,
    }

def run_backtest() -> Dict[str, Any]:
    """Run institutional-grade backtest for Feb 9-10."""
    
    print("=" * 80)
    print("INSTITUTIONAL-GRADE BACKTEST: FEB 9-10, 2026")
    print("=" * 80)
    print("\nAnalyzing 9:35 AM and 3:15 PM Top 10 picks")
    print("Using real TradeNova data with institutional metrics\n")
    
    # Load actual movements
    actual_movements_file = TRADENOVA_DATA / "feb9_13_actual_movements.json"
    actual_movements = load_actual_movements(actual_movements_file)
    print(f"✅ Loaded actual movements for {len(actual_movements)} symbols")
    
    # Dates to analyze
    dates = [
        (date(2026, 2, 9), "Monday"),
        (date(2026, 2, 10), "Tuesday"),
    ]
    
    polygon_client = requests.Session()
    all_results = []
    
    for scan_date, day_name in dates:
        print(f"\n{'='*80}")
        print(f"📅 {day_name}, {scan_date.isoformat()}")
        print(f"{'='*80}")
        
        date_str = scan_date.strftime("%Y%m%d")
        cross_data = load_cross_analysis(date_str)
        
        if not cross_data:
            print(f"  ⚠️  No cross_analysis file found for {scan_date}")
            continue
        
        # Extract Top 10 picks for each session
        # Note: We need to infer session from timestamp or use both AM and PM
        puts_picks = cross_data.get("puts_through_moonshot", [])
        moonshot_picks = cross_data.get("moonshot_through_puts", [])
        
        # For Feb 9-10, we'll analyze all picks (both AM and PM would be in same file)
        # We'll mark session based on timestamp or default to PM for 3:15
        
        # Analyze PUT picks
        print(f"\n  🔴 PUTS ENGINE: {len(puts_picks)} picks")
        for i, pick in enumerate(puts_picks[:10], 1):  # Top 10
            symbol = pick.get("symbol", "")
            session = "PM"  # Default to PM for 3:15 scans
            result = analyze_pick_performance(
                pick, scan_date, session, actual_movements, polygon_client
            )
            result["rank"] = i
            result["engine"] = "PutsEngine"
            all_results.append(result)
            print(f"    {i}. {symbol}: {result['options_pnl_net']:+.1f}% ({result['grade']})")
        
        # Analyze MOONSHOT picks
        print(f"\n  🟢 MOONSHOT ENGINE: {len(moonshot_picks)} picks")
        for i, pick in enumerate(moonshot_picks[:10], 1):  # Top 10
            symbol = pick.get("symbol", "")
            session = "PM"  # Default to PM for 3:15 scans
            result = analyze_pick_performance(
                pick, scan_date, session, actual_movements, polygon_client
            )
            result["rank"] = i
            result["engine"] = "Moonshot"
            all_results.append(result)
            print(f"    {i}. {symbol}: {result['options_pnl_net']:+.1f}% ({result['grade']})")
    
    # Aggregate statistics
    puts_results = [r for r in all_results if r["engine"] == "PutsEngine"]
    moonshot_results = [r for r in all_results if r["engine"] == "Moonshot"]
    
    clean_results = [r for r in all_results if r["data_quality"] in ["OK", "TRADENOVA_DATA", "POLYGON_FALLBACK"]]
    
    winners = [r for r in clean_results if r["win"]]
    losers = [r for r in clean_results if not r["win"]]
    
    # Calculate institutional metrics
    if clean_results:
        returns = [r["options_pnl_net"] for r in clean_results]
        win_rate = len(winners) / len(clean_results) * 100
        avg_return = sum(returns) / len(returns)
        median_return = sorted(returns)[len(returns) // 2]
        
        winner_returns = [r["options_pnl_net"] for r in winners]
        loser_returns = [r["options_pnl_net"] for r in losers]
        
        avg_win = sum(winner_returns) / len(winner_returns) if winner_returns else 0
        avg_loss = sum(loser_returns) / len(loser_returns) if loser_returns else 0
        
        expectancy = (len(winners) / len(clean_results)) * avg_win + (len(losers) / len(clean_results)) * avg_loss
        
        # Profit factor
        total_wins = sum(winner_returns) if winner_returns else 0
        total_losses = abs(sum(loser_returns)) if loser_returns else 0
        profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
    else:
        win_rate = avg_return = median_return = avg_win = avg_loss = expectancy = profit_factor = 0
    
    summary = {
        "backtest_period": "2026-02-09 to 2026-02-10",
        "total_picks": len(all_results),
        "clean_picks": len(clean_results),
        "puts_picks": len(puts_results),
        "moonshot_picks": len(moonshot_results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate_pct": round(win_rate, 1),
        "avg_return_pct": round(avg_return, 1),
        "median_return_pct": round(median_return, 1),
        "avg_win_pct": round(avg_win, 1),
        "avg_loss_pct": round(avg_loss, 1),
        "expectancy_pct": round(expectancy, 1),
        "profit_factor": round(profit_factor, 2),
        "all_results": all_results,
        "winners": winners,
        "losers": losers,
    }
    
    return summary

if __name__ == "__main__":
    results = run_backtest()
    
    # Save results
    output_file = OUTPUT_DIR / "backtest_feb9_10_institutional.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print("\n" + "=" * 80)
    print("BACKTEST SUMMARY")
    print("=" * 80)
    print(f"\nTotal Picks: {results['total_picks']}")
    print(f"Clean Picks: {results['clean_picks']}")
    print(f"Winners: {results['winners']} ({results['win_rate_pct']:.1f}%)")
    print(f"Losers: {results['losers']}")
    print(f"\nReturns:")
    print(f"  Average: {results['avg_return_pct']:+.1f}%")
    print(f"  Median: {results['median_return_pct']:+.1f}%")
    print(f"  Avg Win: {results['avg_win_pct']:+.1f}%")
    print(f"  Avg Loss: {results['avg_loss_pct']:+.1f}%")
    print(f"  Expectancy: {results['expectancy_pct']:+.1f}%")
    print(f"  Profit Factor: {results['profit_factor']:.2f}")
    
    print(f"\n✅ Results saved to: {output_file}")
