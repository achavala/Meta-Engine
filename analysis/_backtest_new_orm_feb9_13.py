#!/usr/bin/env python3
"""
INSTITUTIONAL-GRADE BACKTEST — Feb 9-13, 2026
=============================================
Takes the ACTUAL picks from each day's scan, applies the NEW ORM
computation + gates, re-ranks, and evaluates against real Polygon prices.

This answers: "Given the same inputs the system detected each day,
how would the NEW ORM code have changed the rankings and filtering?"

Methodology:
- Load actual puts_top10 + moonshot_top10 from each scan day
- Apply NEW ORM (from UW microstructure cache) to every candidate
- Re-rank using new blending: original_score × 0.55 + ORM × 0.45
- Apply new gates (ORM ≥ 0.50 for computed, penalty for missing/default)
- Evaluate against actual next-day prices from Polygon API + TradeNova data
- Full institutional metrics (expectancy, median, trimmed mean, profit factor)
"""

import sys, os, json, logging, time, math, statistics
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("backtest_orm")

# ── Configuration ──────────────────────────────────────────────
POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")
if not POLYGON_KEY:
    # Search multiple .env locations
    for env_path in [
        Path(__file__).resolve().parent.parent / ".env",
        Path.home() / "PutsEngine" / ".env",
        Path.home() / "TradeNova" / ".env",
    ]:
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line.startswith("POLYGON_API_KEY=") and not line.startswith("#"):
                    POLYGON_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if POLYGON_KEY:
                        break
        if POLYGON_KEY:
            break

TRADENOVA_DATA = Path.home() / "TradeNova" / "data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"

# Scan days with actual data
SCAN_DAYS = [
    ("2026-02-09", "20260209", "PM"),  # Sunday evening (pre-market for Monday)
    ("2026-02-10", "20260210", "PM"),  # Monday 3:27 PM
    ("2026-02-11", "20260211", "PM"),  # Tuesday 3:15 PM
    ("2026-02-12", "20260212", "PM"),  # Wednesday 3:15 PM
    ("2026-02-13", "20260213", "PM"),  # Thursday 7:14 PM (delayed)
]

# Evaluation: entry on scan day close, exit on next 1-3 trading days
# Sunday scan → enter Monday close, exit Tue-Thu
# Mon PM scan → enter Monday close, exit Tue-Thu
# etc.


# ═══════════════════════════════════════════════════════════════
# STEP 1: Load UW microstructure data for ORM computation
# ═══════════════════════════════════════════════════════════════
def load_uw_data():
    """Load all 5 UW cache files for ORM computation."""
    def _load(fname, inner_key=None):
        for base_dir in [TRADENOVA_DATA, Path(__file__).resolve().parent.parent / "data"]:
            fpath = base_dir / fname
            if fpath.exists():
                with open(fpath) as f:
                    raw = json.load(f)
                data = raw.get(inner_key, raw) if inner_key else raw
                if not isinstance(data, dict):
                    continue
                result = {k: v for k, v in data.items()
                          if k not in ("timestamp", "generated_at", "scan_time")}
                if result:
                    return result
        return {}

    gex = _load("uw_gex_cache.json", "data")
    iv = _load("uw_iv_term_cache.json", "data")
    oi = _load("uw_oi_change_cache.json", "data")
    flow = _load("uw_flow_cache.json", "flow_data")
    dp = _load("darkpool_cache.json")
    print(f"  UW data: GEX={len(gex)} | IV={len(iv)} | OI={len(oi)} | Flow={len(flow)} | DP={len(dp)}")
    return gex, iv, oi, flow, dp


# ═══════════════════════════════════════════════════════════════
# STEP 2: Load actual movements from TradeNova + Polygon
# ═══════════════════════════════════════════════════════════════
def load_actual_movements():
    """Load the actual stock movements from TradeNova data."""
    am_path = TRADENOVA_DATA / "feb9_13_actual_movements.json"
    if am_path.exists():
        with open(am_path) as f:
            return json.load(f)
    return {}


_price_cache: Dict[str, Dict] = {}


def batch_fetch_polygon_prices(symbols: List[str], from_date: str, to_date: str):
    """Fetch daily OHLCV from Polygon for all symbols."""
    global _price_cache
    for sym in symbols:
        cache_key = sym
        if cache_key in _price_cache:
            continue

        try:
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
                f"{from_date}/{to_date}"
                f"?adjusted=true&sort=asc&apiKey={POLYGON_KEY}"
            )
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                prices = {}
                for bar in data.get("results", []):
                    bar_date = datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d")
                    prices[bar_date] = {
                        "open": bar.get("o", 0),
                        "high": bar.get("h", 0),
                        "low": bar.get("l", 0),
                        "close": bar.get("c", 0),
                        "volume": bar.get("v", 0),
                    }
                _price_cache[sym] = prices
            else:
                _price_cache[sym] = {}
            time.sleep(0.13)  # Rate limit ~8/sec for paid tier
        except Exception as e:
            logger.warning(f"Polygon error {sym}: {e}")
            _price_cache[sym] = {}


def get_price(symbol: str, date_str: str) -> Optional[Dict]:
    """Get price data for a symbol on a specific date."""
    if symbol in _price_cache and date_str in _price_cache[symbol]:
        return _price_cache[symbol][date_str]
    return None


def next_trading_day(d: date) -> date:
    """Get next trading day (skip weekends AND NYSE holidays)."""
    try:
        from trading.nyse_calendar import next_trading_day as _ntd
        return _ntd(d)
    except ImportError:
        pass
    # Fallback: NYSE holidays for Feb 2026 hardcoded
    NYSE_HOLIDAYS_2026 = {
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
        date(2026, 4, 3), date(2026, 5, 25), date(2026, 6, 19),
        date(2026, 7, 3), date(2026, 9, 7), date(2026, 11, 26),
        date(2026, 12, 25),
    }
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5 or nd in NYSE_HOLIDAYS_2026:
        nd += timedelta(days=1)
    return nd


# ═══════════════════════════════════════════════════════════════
# STEP 3: Compute ORM for each candidate (NEW code)
# ═══════════════════════════════════════════════════════════════
def compute_orm_for_pick(pick: Dict, gex, iv, oi, flow, dp, engine: str):
    """
    Apply the NEW ORM computation to a pick.
    Uses the actual _compute_options_return_multiplier from the adapters.
    """
    sym = pick["symbol"]
    stock_price = pick.get("price", 0)

    try:
        if engine == "PUTS":
            from engine_adapters.puts_adapter import _compute_options_return_multiplier
            orm, factors, has_real = _compute_options_return_multiplier(
                sym, gex, iv, oi, flow, dp, stock_price=stock_price
            )
        else:
            from engine_adapters.moonshot_adapter import _compute_call_options_return_multiplier
            orm, factors, has_real = _compute_call_options_return_multiplier(
                sym, gex, iv, oi, flow, dp, stock_price=stock_price
            )

        has_uw_data = any(d for d in [gex, iv, oi, flow, dp])
        if not has_uw_data:
            orm_status = "missing"
        elif not has_real:
            orm_status = "default"
        else:
            orm_status = "computed"

        return orm, factors, orm_status
    except Exception as e:
        logger.warning(f"ORM computation error for {sym}: {e}")
        return 0.35, {}, "missing"


# ═══════════════════════════════════════════════════════════════
# STEP 4: Apply new gates and re-rank
# ═══════════════════════════════════════════════════════════════
def apply_new_orm_and_gates(picks: List[Dict], gex, iv, oi, flow, dp, engine: str):
    """
    Apply NEW ORM computation + gates to a list of picks.
    Returns: (surviving_picks, filtered_picks)
    """
    MIN_ORM_COMPUTED = 0.50
    ORM_MISSING_PENALTY = 0.08

    enriched = []
    for pick in picks:
        p = dict(pick)  # Copy
        orm, factors, orm_status = compute_orm_for_pick(p, gex, iv, oi, flow, dp, engine)

        p["_orm_score"] = orm
        p["_orm_factors"] = factors
        p["_orm_status"] = orm_status

        # New blending formula: original × 0.55 + ORM × 0.45
        original_score = p.get("score", 0)
        p["_original_score"] = original_score
        blended = original_score * 0.55 + orm * 0.45
        p["_blended_score"] = max(0.0, min(blended, 1.5))  # Allow >1 for moonshot scores

        enriched.append(p)

    # Sort by blended score
    enriched.sort(key=lambda x: x["_blended_score"], reverse=True)

    # Apply gates
    surviving = []
    filtered_out = []

    for p in enriched:
        orm = p["_orm_score"]
        orm_status = p["_orm_status"]
        signals = p.get("signals", [])
        n_signals = len(signals) if isinstance(signals, list) else 0
        base = p["_original_score"]

        gate_reason = None

        if orm_status == "computed" and orm < MIN_ORM_COMPUTED:
            gate_reason = f"ORM {orm:.3f} < {MIN_ORM_COMPUTED} (computed)"

        if orm_status in ("missing", "default"):
            # Penalty but don't hard-filter unless weak signals
            p["_blended_score"] = max(p["_blended_score"] - ORM_MISSING_PENALTY, 0.10)
            if n_signals < 3 and base < 0.50:
                gate_reason = f"ORM missing + weak signals ({n_signals}) + low base ({base:.2f})"

        if gate_reason:
            p["_gate_reason"] = gate_reason
            filtered_out.append(p)
        else:
            surviving.append(p)

    # Re-sort survivors
    surviving.sort(key=lambda x: x["_blended_score"], reverse=True)

    return surviving, filtered_out


# ═══════════════════════════════════════════════════════════════
# STEP 5: Evaluate picks against actual prices
# ═══════════════════════════════════════════════════════════════
def compute_options_pnl(stock_move_pct: float, option_type: str, dte: int = 5) -> float:
    """Realistic options P&L estimation with delta/gamma/theta model."""
    abs_move = abs(stock_move_pct) / 100.0

    favorable = (option_type == "put" and stock_move_pct < 0) or \
                (option_type == "call" and stock_move_pct > 0)

    if not favorable:
        loss = -abs_move * 3.5
        theta_cost = min(dte, 5) * 0.008
        loss -= theta_cost
        return max(loss * 100, -100.0)

    # Favorable move
    delta = 0.35
    gamma = 0.03
    premium_pct = 0.03

    delta_pnl = delta * abs_move
    gamma_pnl = 0.5 * gamma * (abs_move ** 2) * 100
    total_option_move = delta_pnl + gamma_pnl

    if abs_move > 0.03:
        iv_boost = (abs_move - 0.03) * 0.5
        total_option_move += iv_boost

    option_return_pct = (total_option_move / premium_pct) * 100
    theta_cost = min(dte, 5) * 0.008
    option_return_pct -= theta_cost * 100 / premium_pct
    option_return_pct -= 5.0  # Spread/slippage

    # FEB 16 FIX: Long options cannot lose more than 100% of premium paid.
    # Enforce invariant: trade_return = max(return, -100%) AFTER all adjustments.
    return max(min(option_return_pct, 2000.0), -100.0)


def evaluate_pick(pick: Dict, scan_date: date, actual_movements: Dict) -> Dict:
    """Evaluate a single pick against actual market movements."""
    sym = pick["symbol"]
    option_type = pick.get("option_type", "put")
    if pick.get("engine") == "MOONSHOT" or pick.get("source_engine") == "Moonshot":
        option_type = "call"
    elif pick.get("engine") == "PUTS" or pick.get("source_engine") == "Puts":
        option_type = "put"

    entry_date = next_trading_day(scan_date)
    eval_dates = []
    d = entry_date
    for _ in range(3):
        eval_dates.append(d)
        d = next_trading_day(d)

    # Get entry price
    entry_price_data = get_price(sym, entry_date.isoformat())
    if not entry_price_data:
        # Try scan date
        entry_price_data = get_price(sym, scan_date.isoformat())

    if not entry_price_data or entry_price_data.get("close", 0) <= 0:
        return {
            **pick,
            "option_type": option_type,
            "entry_price": 0,
            "entry_date": entry_date.isoformat(),
            "stock_move_pct": 0,
            "best_move_pct": 0,
            "options_pnl_pct": 0,
            "data_quality": "MISSING_ENTRY",
            "outcome": "NO_DATA",
            "best_date": "",
            "eval_note": f"No Polygon data for {sym} on {entry_date.isoformat()}",
        }

    entry_price = entry_price_data["close"]

    # Try to use pick price for entry if available and reasonable
    pick_price = pick.get("price", 0)
    if pick_price > 0 and abs(pick_price - entry_price) / entry_price < 0.10:
        entry_price = pick_price  # Use the actual pick price if within 10%

    # Find best favorable move in eval window
    best_move = 0
    best_date = ""
    eod_move = 0  # Close-to-close for next trading day

    for eval_d in eval_dates:
        p = get_price(sym, eval_d.isoformat())
        if not p:
            continue

        close_move = (p["close"] - entry_price) / entry_price * 100

        if eval_d == entry_date:
            eod_move = close_move

        if option_type == "put":
            # Best downward move
            intraday_low = (p["low"] - entry_price) / entry_price * 100
            if close_move < best_move:
                best_move = close_move
                best_date = eval_d.isoformat()
            if intraday_low < best_move:
                best_move = intraday_low
                best_date = f"{eval_d.isoformat()} (intraday low)"
        else:
            # Best upward move
            intraday_high = (p["high"] - entry_price) / entry_price * 100
            if close_move > best_move:
                best_move = close_move
                best_date = eval_d.isoformat()
            if intraday_high > best_move:
                best_move = intraday_high
                best_date = f"{eval_d.isoformat()} (intraday high)"

    # Also check actual_movements data
    if sym in actual_movements:
        for mv in actual_movements[sym]:
            mv_date = mv.get("date", "")[:10]
            mv_pct = mv.get("change_pct", 0)
            if option_type == "put" and mv_pct < best_move:
                best_move = mv_pct
                best_date = f"{mv_date} (TradeNova)"
            elif option_type == "call" and mv_pct > best_move:
                best_move = mv_pct
                best_date = f"{mv_date} (TradeNova)"

    data_quality = "OK" if best_date else "MISSING_EXIT"

    # Compute options P&L
    options_pnl = compute_options_pnl(best_move, option_type)
    # FEB 16 INVARIANT: Long options max loss = -100% of premium paid
    options_pnl = max(options_pnl, -100.0)

    # Outcome classification
    if options_pnl > 50:
        outcome = "BIG_WINNER"
    elif options_pnl > 20:
        outcome = "WINNER"
    elif options_pnl > 0:
        outcome = "MARGINAL"
    elif options_pnl > -20:
        outcome = "SCRATCH"
    else:
        outcome = "LOSER"

    return {
        **pick,
        "option_type": option_type,
        "entry_price": entry_price,
        "entry_date": entry_date.isoformat(),
        "stock_move_pct": best_move,
        "best_move_pct": best_move,
        "eod_move_pct": eod_move,
        "options_pnl_pct": options_pnl,
        "data_quality": data_quality,
        "outcome": outcome,
        "best_date": best_date,
    }


# ═══════════════════════════════════════════════════════════════
# STEP 6: Generate the institutional report
# ═══════════════════════════════════════════════════════════════
def generate_report(all_results: List[Dict], daily_data: Dict) -> str:
    """Generate comprehensive markdown report."""
    clean = [r for r in all_results if r["data_quality"] == "OK"]
    no_data = [r for r in all_results if r["data_quality"] != "OK"]

    puts = [r for r in clean if r["option_type"] == "put"]
    calls = [r for r in clean if r["option_type"] == "call"]

    big_winners = [r for r in clean if r["outcome"] == "BIG_WINNER"]
    winners = [r for r in clean if r["outcome"] in ("BIG_WINNER", "WINNER")]
    marginals = [r for r in clean if r["outcome"] == "MARGINAL"]
    scratches = [r for r in clean if r["outcome"] == "SCRATCH"]
    losers = [r for r in clean if r["outcome"] == "LOSER"]

    all_returns = [r["options_pnl_pct"] for r in clean]
    win_returns = [r["options_pnl_pct"] for r in winners]
    loss_returns = [r["options_pnl_pct"] for r in losers]

    # Core metrics
    win_rate = len(winners) / max(len(clean), 1) * 100
    avg_win = statistics.mean(win_returns) if win_returns else 0
    avg_loss = statistics.mean(loss_returns) if loss_returns else 0
    median_ret = statistics.median(all_returns) if all_returns else 0
    expectancy = (len(winners)/max(len(clean),1)) * avg_win + \
                 (len(losers)/max(len(clean),1)) * avg_loss

    if len(all_returns) >= 6:
        trim_n = max(1, len(all_returns) // 10)
        trimmed = sorted(all_returns)[trim_n:-trim_n]
        trimmed_mean = statistics.mean(trimmed) if trimmed else 0
    else:
        trimmed_mean = statistics.mean(all_returns) if all_returns else 0

    profit_factor = abs(sum(win_returns)) / max(abs(sum(loss_returns)), 0.01)

    # ORM segmentation
    orm_computed = [r for r in clean if r.get("_orm_status") == "computed"]
    orm_default = [r for r in clean if r.get("_orm_status") in ("default", "missing")]

    # ── Build report ──
    rpt = []
    rpt.append("# 🏛️ INSTITUTIONAL BACKTEST REPORT — Feb 9-13, 2026")
    rpt.append("## NEW CODE (ORM Fixes Applied) — Re-simulation of Actual Picks")
    rpt.append(f"\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} EST*")
    rpt.append(f"*Methodology: PhD quant + 30yr institutional microstructure lens*\n")
    rpt.append("---\n")

    # ── Executive Summary ──
    rpt.append("## 📊 Executive Summary\n")
    rpt.append("| Metric | Value |")
    rpt.append("|--------|-------|")
    rpt.append(f"| Scan Days | 5 (Feb 9-13) |")
    rpt.append(f"| Total Picks Processed | {len(all_results)} |")
    rpt.append(f"| Clean Trades (data OK) | {len(clean)} |")
    rpt.append(f"| No Data (Polygon missing) | {len(no_data)} |")
    rpt.append(f"| **Big Winners (>50%)** | **{len(big_winners)}** |")
    rpt.append(f"| Winners (>20%) | {len(winners)} |")
    rpt.append(f"| Marginals (0-20%) | {len(marginals)} |")
    rpt.append(f"| Scratches (-20% to 0%) | {len(scratches)} |")
    rpt.append(f"| Losers (<-20%) | {len(losers)} |")
    rpt.append(f"| **Win Rate (>20%)** | **{win_rate:.1f}%** |")
    rpt.append(f"| Avg Winner | +{avg_win:.1f}% |")
    rpt.append(f"| Avg Loser | {avg_loss:.1f}% |")
    rpt.append(f"| **Expectancy** | **{expectancy:+.1f}%** |")
    rpt.append(f"| Median Return | {median_ret:+.1f}% |")
    rpt.append(f"| Trimmed Mean (10%) | {trimmed_mean:+.1f}% |")
    rpt.append(f"| **Profit Factor** | **{profit_factor:.2f}** |")

    # ── ORM Impact ──
    rpt.append("\n---\n")
    rpt.append("## 🎯 ORM Impact Analysis\n")
    rpt.append("*Key question: Does having computed ORM improve pick quality?*\n")
    rpt.append("| ORM Status | Picks | Winners | Win Rate | Avg Return | Expectancy |")
    rpt.append("|------------|-------|---------|----------|------------|------------|")
    for label, group in [("Computed (real UW)", orm_computed), ("Default/Missing", orm_default)]:
        g_returns = [r["options_pnl_pct"] for r in group]
        g_winners = [r for r in group if r["outcome"] in ("BIG_WINNER", "WINNER")]
        g_losers = [r for r in group if r["outcome"] == "LOSER"]
        wr = len(g_winners) / max(len(group), 1) * 100
        avg_r = statistics.mean(g_returns) if g_returns else 0
        g_win_r = [r["options_pnl_pct"] for r in g_winners]
        g_loss_r = [r["options_pnl_pct"] for r in g_losers]
        g_avg_win = statistics.mean(g_win_r) if g_win_r else 0
        g_avg_loss = statistics.mean(g_loss_r) if g_loss_r else 0
        g_exp = (len(g_winners)/max(len(group),1)) * g_avg_win + \
                (len(g_losers)/max(len(group),1)) * g_avg_loss
        rpt.append(f"| {label} | {len(group)} | {len(g_winners)} | {wr:.1f}% | "
                   f"{avg_r:+.1f}% | {g_exp:+.1f}% |")

    # ── ORM Score Buckets ──
    rpt.append("\n### ORM Score Distribution vs Win Rate\n")
    rpt.append("| ORM Range | Picks | Winners | Win Rate | Avg P&L |")
    rpt.append("|-----------|-------|---------|----------|---------|")
    for lo, hi, label in [(0.80, 1.01, "Elite (0.80+)"), (0.70, 0.80, "Strong (0.70-0.80)"),
                           (0.60, 0.70, "Good (0.60-0.70)"), (0.50, 0.60, "Marginal (0.50-0.60)"),
                           (0.00, 0.50, "Below Gate (<0.50)")]:
        bucket = [r for r in clean if lo <= r.get("_orm_score", 0) < hi]
        if bucket:
            bw = [r for r in bucket if r["outcome"] in ("BIG_WINNER", "WINNER")]
            br = [r["options_pnl_pct"] for r in bucket]
            wr = len(bw) / len(bucket) * 100
            avg_r = statistics.mean(br)
            rpt.append(f"| {label} | {len(bucket)} | {len(bw)} | {wr:.0f}% | {avg_r:+.1f}% |")

    # ── Engine Comparison ──
    rpt.append("\n---\n")
    rpt.append("## 🔴🟢 Engine Performance\n")
    rpt.append("| Engine | Picks | Winners | Win Rate | Avg Winner | Avg Loser | Expectancy |")
    rpt.append("|--------|-------|---------|----------|------------|-----------|------------|")
    for label, eng_picks in [("PUTS 🔴", puts), ("MOONSHOT 🟢", calls)]:
        ew = [r for r in eng_picks if r["outcome"] in ("BIG_WINNER", "WINNER")]
        el = [r for r in eng_picks if r["outcome"] == "LOSER"]
        wr = len(ew) / max(len(eng_picks), 1) * 100
        aw = statistics.mean([r["options_pnl_pct"] for r in ew]) if ew else 0
        al = statistics.mean([r["options_pnl_pct"] for r in el]) if el else 0
        exp = (len(ew)/max(len(eng_picks),1)) * aw + (len(el)/max(len(eng_picks),1)) * al
        rpt.append(f"| {label} | {len(eng_picks)} | {len(ew)} | {wr:.1f}% | "
                   f"+{aw:.1f}% | {al:.1f}% | {exp:+.1f}% |")

    # ── Daily Breakdown ──
    rpt.append("\n---\n")
    rpt.append("## 📅 Daily Breakdown\n")
    for scan_str, date_key, session in SCAN_DAYS:
        dd = daily_data.get(date_key, {})
        day_clean = dd.get("clean_results", [])
        day_surv = dd.get("surviving", [])
        day_filt = dd.get("filtered", [])

        scan_d = date.fromisoformat(scan_str)
        day_name = scan_d.strftime("%a %b %d")

        dw = [r for r in day_clean if r["outcome"] in ("BIG_WINNER", "WINNER")]
        dl = [r for r in day_clean if r["outcome"] == "LOSER"]

        rpt.append(f"### {day_name} ({session} scan)\n")
        rpt.append(f"- Surviving picks (passed gates): {len(day_surv)}")
        rpt.append(f"- Filtered out by ORM gates: {len(day_filt)}")
        rpt.append(f"- Clean evaluations: {len(day_clean)}")
        rpt.append(f"- Winners: {len(dw)} | Losers: {len(dl)}")

        if day_clean:
            rpt.append(f"\n| # | Symbol | Type | ORM | Status | Score→Blended | Stock Move | Options P&L | Outcome |")
            rpt.append(f"|---|--------|------|-----|--------|---------------|------------|-------------|---------|")
            for i, r in enumerate(sorted(day_clean, key=lambda x: x["_blended_score"], reverse=True), 1):
                outcome_emoji = {"BIG_WINNER": "🏆", "WINNER": "✅", "MARGINAL": "📊",
                                 "SCRATCH": "⚪", "LOSER": "❌"}.get(r["outcome"], "?")
                rpt.append(
                    f"| {i} | **{r['symbol']}** | {r['option_type'].upper()} | "
                    f"{r.get('_orm_score',0):.3f} | {r.get('_orm_status','')} | "
                    f"{r.get('_original_score',0):.3f}→{r.get('_blended_score',0):.3f} | "
                    f"{r['stock_move_pct']:+.2f}% | {r['options_pnl_pct']:+.1f}% | "
                    f"{outcome_emoji} {r['outcome']} |"
                )

        if day_filt:
            rpt.append(f"\n**Filtered out by gates:**")
            for f in day_filt:
                rpt.append(f"- {f['symbol']} ({f.get('option_type','?').upper()}): "
                           f"ORM={f.get('_orm_score',0):.3f} [{f.get('_orm_status','')}] — "
                           f"{f.get('_gate_reason', 'gate filter')}")
        rpt.append("")

    # ── Top Winners Detail ──
    rpt.append("\n---\n")
    rpt.append("## 🏆 Top 10 Winners — Detailed Analysis\n")
    sorted_winners = sorted(winners, key=lambda x: x["options_pnl_pct"], reverse=True)
    for i, w in enumerate(sorted_winners[:10], 1):
        rpt.append(f"### #{i} {w['symbol']} ({w['option_type'].upper()}) — "
                   f"**{w['options_pnl_pct']:+.1f}% options return**\n")
        rpt.append(f"- **Scan date:** {w.get('scan_date', w.get('entry_date', '?'))}")
        rpt.append(f"- **Entry price:** ${w.get('entry_price', 0):.2f}")
        rpt.append(f"- **Best stock move:** {w['stock_move_pct']:+.2f}% on {w.get('best_date','?')}")
        rpt.append(f"- **ORM Score:** {w.get('_orm_score',0):.3f} ({w.get('_orm_status','?')})")
        rpt.append(f"- **Original → Blended score:** {w.get('_original_score',0):.3f} → "
                   f"{w.get('_blended_score',0):.3f}")
        sigs = w.get("signals", [])
        if sigs:
            rpt.append(f"- **Signals ({len(sigs)}):** {', '.join(sigs[:8])}")
        factors = w.get("_orm_factors", {})
        if factors:
            top_factors = sorted(factors.items(), key=lambda x: x[1], reverse=True)[:3]
            factor_str = ", ".join(f"{k}={v:.2f}" for k, v in top_factors)
            rpt.append(f"- **Top ORM factors:** {factor_str}")
        _add_winner_analysis(rpt, w)
        rpt.append("")

    # ── Top Losers Detail ──
    rpt.append("\n---\n")
    rpt.append("## 📉 Top 10 Losers — Detailed Analysis\n")
    sorted_losers = sorted(losers, key=lambda x: x["options_pnl_pct"])
    for i, l in enumerate(sorted_losers[:10], 1):
        rpt.append(f"### #{i} {l['symbol']} ({l['option_type'].upper()}) — "
                   f"**{l['options_pnl_pct']:+.1f}% options return**\n")
        rpt.append(f"- **Scan date:** {l.get('scan_date', l.get('entry_date', '?'))}")
        rpt.append(f"- **Entry price:** ${l.get('entry_price', 0):.2f}")
        rpt.append(f"- **Stock move:** {l['stock_move_pct']:+.2f}%")
        rpt.append(f"- **ORM Score:** {l.get('_orm_score',0):.3f} ({l.get('_orm_status','?')})")
        rpt.append(f"- **Original → Blended score:** {l.get('_original_score',0):.3f} → "
                   f"{l.get('_blended_score',0):.3f}")
        sigs = l.get("signals", [])
        if sigs:
            rpt.append(f"- **Signals ({len(sigs)}):** {', '.join(sigs[:8])}")
        _add_loser_analysis(rpt, l)
        rpt.append("")

    # ── Signal Effectiveness Matrix ──
    rpt.append("\n---\n")
    rpt.append("## 🔍 Signal Effectiveness Matrix\n")
    sig_data = defaultdict(lambda: {"wins": 0, "losses": 0, "total_pnl": 0, "count": 0})
    for r in clean:
        for s in r.get("signals", []):
            sig_data[s]["count"] += 1
            sig_data[s]["total_pnl"] += r["options_pnl_pct"]
            if r["outcome"] in ("BIG_WINNER", "WINNER"):
                sig_data[s]["wins"] += 1
            elif r["outcome"] == "LOSER":
                sig_data[s]["losses"] += 1

    rpt.append("| Signal | Count | Wins | Losses | Win Rate | Avg P&L | Edge |")
    rpt.append("|--------|-------|------|--------|----------|---------|------|")
    sorted_sigs = sorted(sig_data.items(),
                         key=lambda x: x[1]["total_pnl"] / max(x[1]["count"], 1),
                         reverse=True)
    for sig, d in sorted_sigs[:20]:
        total = d["wins"] + d["losses"]
        wr = d["wins"] / max(total, 1) * 100
        avg_pnl = d["total_pnl"] / max(d["count"], 1)
        edge = "✅" if avg_pnl > 20 else ("⚠️" if avg_pnl > 0 else "❌")
        rpt.append(f"| {sig} | {d['count']} | {d['wins']} | {d['losses']} | "
                   f"{wr:.0f}% | {avg_pnl:+.1f}% | {edge} |")

    # ── ORM Factor Analysis ──
    rpt.append("\n---\n")
    rpt.append("## 🎯 ORM Factor Analysis: Winners vs Losers\n")
    factor_names = ["gamma_leverage", "iv_expansion", "oi_positioning", "delta_sweet",
                    "short_dte", "vol_regime", "dealer_position", "liquidity"]
    rpt.append("| Factor | Winners Avg | Losers Avg | Delta | Predictive? |")
    rpt.append("|--------|-------------|------------|-------|-------------|")
    for fn in factor_names:
        wv = [r.get("_orm_factors", {}).get(fn, 0) for r in winners if r.get("_orm_factors")]
        lv = [r.get("_orm_factors", {}).get(fn, 0) for r in losers if r.get("_orm_factors")]
        wa = statistics.mean(wv) if wv else 0
        la = statistics.mean(lv) if lv else 0
        delta = wa - la
        pred = "✅ Strong" if delta > 0.10 else ("⚠️ Weak" if delta > 0 else "❌ Inverted")
        rpt.append(f"| {fn} | {wa:.3f} | {la:.3f} | {delta:+.3f} | {pred} |")

    # ── Recommendations ──
    rpt.append("\n---\n")
    rpt.append("## 💡 Recommendations (No Fixes Applied)\n")
    rpt.append(_generate_recommendations(all_results, clean, winners, losers, puts, calls,
                                          orm_computed, orm_default, daily_data))

    return "\n".join(rpt)


def _add_winner_analysis(rpt, w):
    """Add contextual analysis for a winner."""
    move = abs(w["stock_move_pct"])
    orm = w.get("_orm_score", 0)
    signals = w.get("signals", [])

    reasons = []
    if move > 5:
        reasons.append(f"large stock move ({w['stock_move_pct']:+.1f}%) amplified by options leverage")
    elif move > 3:
        reasons.append(f"solid {w['stock_move_pct']:+.1f}% move with good delta capture")
    else:
        reasons.append(f"moderate move but OTM gamma acceleration helped")

    if orm >= 0.80:
        reasons.append("elite ORM score indicated optimal options structure")
    elif orm >= 0.70:
        reasons.append("strong ORM score — good gamma/IV setup")

    if len(signals) >= 5:
        reasons.append(f"high signal confluence ({len(signals)} signals)")

    analysis = " | ".join(reasons) if reasons else "Multiple factors aligned"
    rpt.append(f"- **Why it worked:** {analysis}")


def _add_loser_analysis(rpt, l):
    """Add contextual analysis for a loser."""
    move = l["stock_move_pct"]
    option_type = l["option_type"]

    reasons = []
    if (option_type == "put" and move > 0) or (option_type == "call" and move < 0):
        reasons.append(f"stock moved AGAINST thesis ({move:+.1f}%)")
    elif abs(move) < 1:
        reasons.append(f"stock barely moved ({move:+.1f}%), theta ate premium")
    else:
        reasons.append(f"move not large enough to overcome premium decay")

    signals = l.get("signals", [])
    if len(signals) < 3:
        reasons.append(f"low signal count ({len(signals)}) — weak conviction")

    orm = l.get("_orm_score", 0)
    if orm < 0.50:
        reasons.append(f"low ORM ({orm:.3f}) — poor options structure")

    analysis = " | ".join(reasons) if reasons else "Direction thesis invalidated"
    rpt.append(f"- **Why it failed:** {analysis}")


def _generate_recommendations(all_results, clean, winners, losers, puts, calls,
                               orm_computed, orm_default, daily_data):
    """Generate actionable recommendations."""
    lines = []

    # 1. ORM gate effectiveness
    lines.append("### 1. ORM Gate Effectiveness\n")
    if orm_computed:
        oc_wr = len([r for r in orm_computed if r["outcome"] in ("BIG_WINNER","WINNER")]) / len(orm_computed) * 100
        lines.append(f"- Computed ORM picks: {len(orm_computed)}, win rate: {oc_wr:.0f}%")
    if orm_default:
        od_wr = len([r for r in orm_default if r["outcome"] in ("BIG_WINNER","WINNER")]) / len(orm_default) * 100
        lines.append(f"- Default/Missing ORM picks: {len(orm_default)}, win rate: {od_wr:.0f}%")
    if orm_computed and orm_default:
        delta = oc_wr - od_wr
        if delta > 10:
            lines.append(f"- ✅ **ORM provides {delta:.0f}pp edge** — gate is working")
        elif delta > 0:
            lines.append(f"- ⚠️ ORM shows marginal {delta:.0f}pp edge — more data needed")
        else:
            lines.append(f"- ❌ ORM not showing edge — review factor weights")

    # 2. ORM threshold sweep
    lines.append("\n### 2. ORM Threshold Sensitivity\n")
    for threshold in [0.50, 0.60, 0.70, 0.80]:
        above = [r for r in clean if r.get("_orm_score", 0) >= threshold]
        if above:
            aw = [r for r in above if r["outcome"] in ("BIG_WINNER", "WINNER")]
            wr = len(aw) / len(above) * 100
            avg_r = statistics.mean([r["options_pnl_pct"] for r in above])
            lines.append(f"- ORM ≥ {threshold}: {len(above)} picks, "
                         f"{len(aw)} winners ({wr:.0f}%), avg {avg_r:+.1f}%")

    # 3. Engine comparison
    lines.append("\n### 3. Engine-Specific Insights\n")
    p_wr = len([r for r in puts if r["outcome"] in ("BIG_WINNER","WINNER")]) / max(len(puts),1) * 100
    c_wr = len([r for r in calls if r["outcome"] in ("BIG_WINNER","WINNER")]) / max(len(calls),1) * 100
    lines.append(f"- PUTS engine: {p_wr:.0f}% win rate ({len(puts)} picks)")
    lines.append(f"- MOONSHOT engine: {c_wr:.0f}% win rate ({len(calls)} picks)")
    if p_wr > c_wr + 15:
        lines.append(f"- 🔴 **PUT engine significantly outperforming** — bearish week bias likely")
    elif c_wr > p_wr + 15:
        lines.append(f"- 🟢 **CALL engine significantly outperforming** — bullish week bias likely")

    # 4. Signal count
    lines.append("\n### 4. Signal Count Threshold\n")
    for min_sig in [2, 3, 4, 5]:
        above = [r for r in clean if len(r.get("signals", [])) >= min_sig]
        if above:
            aw = [r for r in above if r["outcome"] in ("BIG_WINNER", "WINNER")]
            wr = len(aw) / len(above) * 100
            lines.append(f"- ≥{min_sig} signals: {len(above)} picks, {wr:.0f}% win rate")

    # 5. Day-of-week
    lines.append("\n### 5. Day-of-Week Patterns\n")
    day_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    for scan_str, date_key, _ in SCAN_DAYS:
        d = date.fromisoformat(scan_str)
        dd = daily_data.get(date_key, {})
        dc = dd.get("clean_results", [])
        dw = len([r for r in dc if r["outcome"] in ("BIG_WINNER","WINNER")])
        dl = len([r for r in dc if r["outcome"] == "LOSER"])
        wr = dw / max(len(dc), 1) * 100
        lines.append(f"- {day_names.get(d.weekday(), '?')} {scan_str}: "
                     f"{dw}W/{dl}L from {len(dc)} clean ({wr:.0f}%)")

    # 6. Critical improvements
    lines.append("\n### 6. Critical Improvement Opportunities\n")

    # Check filtered picks that would have been winners
    all_filtered = []
    for dd in daily_data.values():
        all_filtered.extend(dd.get("filtered_results", []))
    filtered_winners = [r for r in all_filtered if r.get("outcome") in ("BIG_WINNER", "WINNER")]
    if filtered_winners:
        lines.append(f"- ⚠️ **{len(filtered_winners)} winners were filtered by gates!**")
        for fw in filtered_winners:
            lines.append(f"  - {fw['symbol']} ({fw['option_type'].upper()}): "
                         f"{fw['options_pnl_pct']:+.1f}% — {fw.get('_gate_reason','filtered')}")
        lines.append(f"  → Consider relaxing gate thresholds for high-signal picks")

    # Data quality
    all_nodata = [r for r in all_results if r["data_quality"] != "OK"]
    if all_nodata:
        lines.append(f"- ℹ️ {len(all_nodata)} picks had missing price data from Polygon")

    # Biggest edge: ORM factor with highest predictive power
    factor_edges = {}
    for fn in ["gamma_leverage", "iv_expansion", "oi_positioning", "delta_sweet",
               "short_dte", "vol_regime", "dealer_position", "liquidity"]:
        wv = [r.get("_orm_factors", {}).get(fn, 0) for r in winners if r.get("_orm_factors")]
        lv = [r.get("_orm_factors", {}).get(fn, 0) for r in losers if r.get("_orm_factors")]
        if wv and lv:
            factor_edges[fn] = statistics.mean(wv) - statistics.mean(lv)
    if factor_edges:
        best = max(factor_edges, key=factor_edges.get)
        worst = min(factor_edges, key=factor_edges.get)
        lines.append(f"- 🎯 **Most predictive ORM factor:** {best} "
                     f"(+{factor_edges[best]:.3f} winner edge)")
        lines.append(f"- ⚠️ **Least predictive:** {worst} "
                     f"({factor_edges[worst]:+.3f}) — consider reducing weight")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    print("=" * 80)
    print("🏛️  INSTITUTIONAL BACKTEST — Feb 9-13, 2026")
    print("    NEW CODE with ORM Fixes — Re-simulation of Actual Picks")
    print("=" * 80)

    # Load UW data
    print("\n📊 Loading UW microstructure data...")
    gex, iv, oi, flow, dp = load_uw_data()

    # Load actual movements
    print("📈 Loading actual price movements...")
    actual_movements = load_actual_movements()
    print(f"  TradeNova movements: {len(actual_movements)} symbols")

    # Collect all unique symbols to fetch from Polygon
    all_symbols = set()
    day_picks_raw = {}

    for scan_str, date_key, session in SCAN_DAYS:
        puts_path = OUTPUT_DIR / f"puts_top10_{date_key}.json"
        moon_path = OUTPUT_DIR / f"moonshot_top10_{date_key}.json"

        puts_picks = []
        calls_picks = []

        if puts_path.exists():
            data = json.load(open(puts_path))
            puts_picks = data.get("picks", [])
            for p in puts_picks:
                p["option_type"] = "put"
                p["scan_date"] = scan_str
                all_symbols.add(p["symbol"])

        if moon_path.exists():
            data = json.load(open(moon_path))
            calls_picks = data.get("picks", [])
            for c in calls_picks:
                c["option_type"] = c.get("option_type", "call")
                c["scan_date"] = scan_str
                all_symbols.add(c["symbol"])

        day_picks_raw[date_key] = {
            "puts": puts_picks,
            "calls": calls_picks,
            "session": session,
            "scan_date": scan_str,
        }

    # Batch fetch Polygon prices for all symbols
    all_symbols = sorted(all_symbols)
    print(f"\n🌐 Fetching Polygon prices for {len(all_symbols)} symbols...")
    batch_fetch_polygon_prices(all_symbols, "2026-02-07", "2026-02-21")
    fetched = sum(1 for s in all_symbols if _price_cache.get(s))
    print(f"  ✅ {fetched}/{len(all_symbols)} symbols with price data")

    # Process each day
    all_results = []
    daily_data = {}

    for scan_str, date_key, session in SCAN_DAYS:
        scan_date = date.fromisoformat(scan_str)
        day_name = scan_date.strftime("%A %b %d")
        print(f"\n{'─'*60}")
        print(f"📅 {day_name} ({session})")
        print(f"{'─'*60}")

        raw = day_picks_raw[date_key]
        puts_picks = raw["puts"]
        calls_picks = raw["calls"]

        # Apply NEW ORM to puts
        print(f"  🔴 PUTS: {len(puts_picks)} candidates → applying ORM...")
        puts_surviving, puts_filtered = apply_new_orm_and_gates(
            puts_picks, gex, iv, oi, flow, dp, "PUTS"
        )
        print(f"     Surviving: {len(puts_surviving)} | Filtered: {len(puts_filtered)}")
        for i, p in enumerate(puts_surviving[:5], 1):
            print(f"     #{i} {p['symbol']:6s} "
                  f"orig={p['_original_score']:.3f} → blended={p['_blended_score']:.3f} "
                  f"ORM={p['_orm_score']:.3f}[{p['_orm_status']}]")

        # Apply NEW ORM to calls
        print(f"  🟢 CALLS: {len(calls_picks)} candidates → applying ORM...")
        calls_surviving, calls_filtered = apply_new_orm_and_gates(
            calls_picks, gex, iv, oi, flow, dp, "MOONSHOT"
        )
        print(f"     Surviving: {len(calls_surviving)} | Filtered: {len(calls_filtered)}")
        for i, c in enumerate(calls_surviving[:5], 1):
            print(f"     #{i} {c['symbol']:6s} "
                  f"orig={c['_original_score']:.3f} → blended={c['_blended_score']:.3f} "
                  f"ORM={c['_orm_score']:.3f}[{c['_orm_status']}]")

        # Evaluate all picks against actual prices
        print(f"  📈 Evaluating against actual prices...")
        day_clean = []
        day_filtered_results = []

        for pick in puts_surviving + calls_surviving:
            result = evaluate_pick(pick, scan_date, actual_movements)
            all_results.append(result)
            if result["data_quality"] == "OK":
                day_clean.append(result)

        # Also evaluate filtered picks (to check if gates helped)
        for pick in puts_filtered + calls_filtered:
            result = evaluate_pick(pick, scan_date, actual_movements)
            result["_was_filtered"] = True
            day_filtered_results.append(result)

        day_winners = [r for r in day_clean if r["outcome"] in ("BIG_WINNER", "WINNER")]
        day_losers = [r for r in day_clean if r["outcome"] == "LOSER"]

        daily_data[date_key] = {
            "clean_results": day_clean,
            "filtered_results": day_filtered_results,
            "surviving": puts_surviving + calls_surviving,
            "filtered": puts_filtered + calls_filtered,
        }

        print(f"  📊 {len(day_clean)} clean: {len(day_winners)}W / {len(day_losers)}L "
              f"({len(day_winners)/max(len(day_clean),1)*100:.0f}% WR)")

    # Generate report
    print(f"\n\n{'='*80}")
    print("📝 GENERATING INSTITUTIONAL REPORT...")
    print(f"{'='*80}")

    report = generate_report(all_results, daily_data)

    output_path = OUTPUT_DIR / "BACKTEST_NEW_ORM_FEB9_13.md"
    with open(output_path, "w") as f:
        f.write(report)
    print(f"\n✅ Report saved: {output_path}")

    # Save raw JSON
    json_path = OUTPUT_DIR / "backtest_new_orm_feb9_13_results.json"
    json_safe = []
    for r in all_results:
        jr = {}
        for k, v in r.items():
            if isinstance(v, (date, datetime)):
                jr[k] = v.isoformat()
            elif isinstance(v, (int, float, str, bool, list, dict, type(None))):
                jr[k] = v
            else:
                jr[k] = str(v)
        json_safe.append(jr)
    with open(json_path, "w") as f:
        json.dump(json_safe, f, indent=2, default=str)
    print(f"✅ Raw data: {json_path}")

    # Quick summary
    clean = [r for r in all_results if r["data_quality"] == "OK"]
    w = [r for r in clean if r["outcome"] in ("BIG_WINNER", "WINNER")]
    l = [r for r in clean if r["outcome"] == "LOSER"]
    print(f"\n🎯 FINAL: {len(clean)} clean trades | "
          f"{len(w)} winners ({len(w)/max(len(clean),1)*100:.0f}%) | "
          f"{len(l)} losers")


if __name__ == "__main__":
    main()
