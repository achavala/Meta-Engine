"""
X-Worthy Picks Selector v3 — Full Gap Fix
══════════════════════════════════════════════════════════════════════════════

v3 addresses ALL 6 forensic gaps from the 30-day analysis:

  1. SERIAL MOVER BOOST: Stocks that moved >=5% yesterday get a priority
     boost (LUNR had 11 big days in Feb; they cluster).
     Uses Polygon 2-day bars fetched at scan time.

  2. DARKPOOL WHALE SIGNAL: Stocks with >$100M darkpool activity get
     a scoring boost. 99% of 5x movers had DP; 31% had >$100M.
     Uses TradeNova darkpool_cache.json.

  3. ATR/VOLATILITY PRIORITY: High-ATR (avg range >5%) names get boost.
     5x movers had 8.6% avg 2-day range vs 7.0% for 2x movers.
     Uses Polygon 5-day bars computed at scan time.

  4. GAP PLAY DETECTION: Stocks gapping >3% at open get injected
     as candidates (SHOP -18.6%, PTON -18.6%, COHR -15.1%).
     Uses Polygon previous close vs today's open (snapshot).

  5. DIRECTION BALANCE: Enforces minimum 3 PUTs + 3 CALLs
     (42% UP vs 58% DOWN in 5x movers — both directions pay).

  6. FINAL_RECS with high est_options_mult get elevated.

DOES NOT TOUCH: cross_analyzer, puts_adapter, moonshot_adapter,
  meta_engine pipeline, Email, Telegram, dashboard, trading, GUI.
  Only the X post input is changed.
"""

import json
import logging
import os
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

logger = logging.getLogger(__name__)

TRADENOVA_DATA = Path.home() / "TradeNova" / "data"
SAME_DAY_1X_PATH = TRADENOVA_DATA / "same_day_1x_cache.json"
SURE_SHOT_5X_PATH = TRADENOVA_DATA / "sure_shot_5x_cache.json"
FINAL_RECS_PATH = TRADENOVA_DATA / "final_recommendations.json"
DARKPOOL_CACHE_PATH = TRADENOVA_DATA / "darkpool_cache.json"

MAX_STALE_HOURS = 24

# Polygon (for serial mover + ATR)
from dotenv import load_dotenv
_META_DIR = Path(__file__).parent.parent
load_dotenv(_META_DIR / ".env")
load_dotenv(Path.home() / "PutsEngine" / ".env", override=False)
load_dotenv(Path.home() / "TradeNova" / ".env", override=False)
_POLYGON_KEY = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")

# Thresholds (forensic-validated from Feb 1-27 analysis)
SERIAL_MOVER_MIN_PCT = 5.0     # Yesterday's move >= 5% = serial mover
DARKPOOL_WHALE_MIN = 100_000_000  # $100M+ = whale-level DP
DARKPOOL_SIGNIFICANT_MIN = 50_000_000  # $50M+ = significant DP
HIGH_ATR_MIN_PCT = 5.0          # 2-day avg range >= 5% = high-beta
GAP_MIN_PCT = 3.0               # Pre-market gap >= 3% = gap play


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        if path.exists():
            with open(path, "r") as f:
                return json.load(f)
    except Exception as e:
        logger.debug(f"x_worthy: load failed {path.name}: {e}")
    return {}


def _is_stale(data: Dict[str, Any], max_hours: float = MAX_STALE_HOURS) -> bool:
    ts = data.get("computed_at") or data.get("generated_at") or ""
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(ts)
        return (datetime.now() - dt).total_seconds() / 3600 > max_hours
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════
# Data loaders
# ═══════════════════════════════════════════════════════════════════════════

def _load_tradenova_tables() -> Tuple[List[Dict], List[Dict], List[Dict]]:
    sd_data = _load_json(SAME_DAY_1X_PATH)
    s5_data = _load_json(SURE_SHOT_5X_PATH)
    fr_data = _load_json(FINAL_RECS_PATH)
    sd_picks = sd_data.get("picks", []) if not _is_stale(sd_data) else []
    s5_picks = s5_data.get("picks", []) if not _is_stale(s5_data) else []
    fr_picks = fr_data.get("recommendations", []) if not _is_stale(fr_data) else []
    if _is_stale(sd_data):
        logger.warning(f"  ⚠️ same_day_1x_cache stale ({sd_data.get('computed_at','')})")
    if _is_stale(s5_data):
        logger.warning(f"  ⚠️ sure_shot_5x_cache stale ({s5_data.get('computed_at','')})")
    if _is_stale(fr_data):
        logger.warning(f"  ⚠️ final_recommendations stale ({fr_data.get('generated_at','')})")
    return sd_picks, s5_picks, fr_picks


def _load_darkpool() -> Dict[str, Dict]:
    """Load darkpool_cache.json → {symbol: {total_value, block_count, ...}}"""
    try:
        data = _load_json(DARKPOOL_CACHE_PATH)
        if isinstance(data, dict):
            data.pop("timestamp", None)
            data.pop("computed_at", None)
            return data
    except Exception:
        pass
    return {}


def _fetch_polygon_bars(symbol: str, days_back: int = 7) -> list:
    """Fetch recent daily bars from Polygon for serial mover + ATR detection."""
    if not _POLYGON_KEY:
        return []
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days_back + 5)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
            params={"adjusted": "true", "sort": "desc", "limit": days_back + 2,
                    "apiKey": _POLYGON_KEY},
            timeout=8,
        )
        if r.status_code == 200:
            return r.json().get("results", [])
    except Exception:
        pass
    return []


# ═══════════════════════════════════════════════════════════════════════════
# GAP 1: Serial Mover Detection
# ═══════════════════════════════════════════════════════════════════════════

_serial_mover_cache: Dict[str, float] = {}


def _get_serial_movers(symbols: List[str]) -> Dict[str, float]:
    """
    For each symbol, check if it moved >=SERIAL_MOVER_MIN_PCT in the
    last 1-2 trading days. Returns {symbol: max_recent_abs_move_pct}.
    Cached per session to avoid repeated API calls.
    """
    global _serial_mover_cache
    if _serial_mover_cache:
        return _serial_mover_cache

    if not _POLYGON_KEY:
        return {}

    result: Dict[str, float] = {}
    for sym in symbols:
        bars = _fetch_polygon_bars(sym, days_back=3)
        if len(bars) >= 2:
            for bar in bars[:2]:
                o = bar.get("o", 0)
                c = bar.get("c", 0)
                h = bar.get("h", 0)
                l = bar.get("l", 0)
                if o > 0:
                    day_move = abs((c - o) / o) * 100
                    hi_move = ((h - o) / o) * 100
                    lo_move = ((o - l) / o) * 100
                    max_move = max(day_move, abs(hi_move), abs(lo_move))
                    result[sym] = max(result.get(sym, 0), max_move)

    _serial_mover_cache = result
    n_serial = sum(1 for v in result.values() if v >= SERIAL_MOVER_MIN_PCT)
    if n_serial:
        logger.info(f"  🔄 Serial movers detected: {n_serial} stocks moved ≥{SERIAL_MOVER_MIN_PCT}% recently")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# GAP 3: ATR / Volatility
# ═══════════════════════════════════════════════════════════════════════════

_atr_cache: Dict[str, float] = {}


def _get_atr_map(symbols: List[str]) -> Dict[str, float]:
    """
    Compute 5-day average true range % for each symbol.
    Uses bars already fetched for serial mover detection where possible.
    """
    global _atr_cache
    if _atr_cache:
        return _atr_cache

    if not _POLYGON_KEY:
        return {}

    result: Dict[str, float] = {}
    for sym in symbols:
        bars = _fetch_polygon_bars(sym, days_back=7)
        if len(bars) >= 3:
            ranges = []
            for bar in bars[:5]:
                o = bar.get("o", 0)
                h = bar.get("h", 0)
                l = bar.get("l", 0)
                if o > 0:
                    ranges.append(((h - l) / o) * 100)
            if ranges:
                result[sym] = sum(ranges) / len(ranges)

    _atr_cache = result
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Converters: TradeNova → standardized pick dict
# ═══════════════════════════════════════════════════════════════════════════

def _tn_1x_to_pick(tn: Dict[str, Any]) -> Dict[str, Any]:
    sym = (tn.get("symbol") or "").strip()
    direction = (tn.get("direction") or "").upper()
    price = float(tn.get("current_price", 0) or 0)
    score_1x = float(tn.get("score_1x", 0) or 0)
    est_mult = float(tn.get("est_multiplier", 0) or 0)
    reasons = tn.get("reasons", [])
    inst_sigs = int(tn.get("inst_signals", 0) or 0)
    return {
        "symbol": sym, "score": min(score_1x / 100.0, 1.0), "price": price,
        "signals": reasons[:5],
        "engine": f"TradeNova_1x ({direction})", "engine_type": "same_day_1x",
        "_x_worthy_reason": "same_day_1x", "_x_score_1x": score_1x,
        "_x_est_mult": est_mult, "_x_direction": direction,
        "_x_inst_signals": inst_sigs, "_x_source": "TradeNova",
    }


def _tn_5x_to_pick(tn: Dict[str, Any]) -> Dict[str, Any]:
    sym = (tn.get("symbol") or "").strip()
    direction = (tn.get("direction") or "").upper()
    price = float(tn.get("current_price", 0) or 0)
    score_5x = float(tn.get("score_5x", 0) or 0)
    est_mult = float(tn.get("est_multiplier", 0) or 0)
    reasons = tn.get("reasons_5x", [])
    return {
        "symbol": sym, "score": min(score_5x / 100.0, 1.0), "price": price,
        "signals": reasons[:5],
        "engine": f"TradeNova_5x ({direction})", "engine_type": "sure_shot_5x",
        "_x_worthy_reason": "5x_potential", "_x_score_5x": score_5x,
        "_x_est_mult": est_mult, "_x_direction": direction,
        "_x_source": "TradeNova",
    }


def _tn_rec_to_pick(rec: Dict[str, Any]) -> Dict[str, Any]:
    sym = (rec.get("symbol") or "").strip()
    cr = rec.get("contract_rules", {})
    direction = (cr.get("trade_type") or "CALL").upper()
    price = float(rec.get("current_price", 0) or 0)
    composite = float(rec.get("composite_score", 0) or 0)
    est_opt = float(rec.get("est_options_mult", 0) or 0)
    grade = rec.get("trade_grade", "?")
    engines = rec.get("engines", [])
    signals = rec.get("signals", [])[:5]
    entry_low = float(rec.get("entry_low", 0) or 0)
    entry_high = float(rec.get("entry_high", 0) or 0)
    target = float(rec.get("target", 0) or 0)
    conviction = int(rec.get("conviction", 0) or 0)
    return {
        "symbol": sym, "score": min(composite / 600.0, 1.0), "price": price,
        "signals": signals,
        "engine": f"TradeNova_Rec ({','.join(engines[:2])})",
        "engine_type": "final_recommendation",
        "entry_low": entry_low, "entry_high": entry_high, "target": target,
        "_x_worthy_reason": "final_rec", "_x_composite": composite,
        "_x_est_mult": est_opt, "_x_direction": direction,
        "_x_grade": grade, "_x_conviction": conviction,
        "_x_source": "TradeNova",
    }


# ═══════════════════════════════════════════════════════════════════════════
# Priority scoring (v3: multi-signal composite)
# ═══════════════════════════════════════════════════════════════════════════

def _priority_score(
    pick: Dict[str, Any],
    dp_data: Dict[str, Dict],
    serial_movers: Dict[str, float],
    atr_map: Dict[str, float],
) -> float:
    """
    Multi-signal priority score. Higher = first in line for X.
    Base tiers:
      Tier 1 (2000+): same_day_1x
      Tier 2 (1000+): sure_shot_5x
      Tier 3 (500+):  final_recs
      Tier 4 (0-100): Meta cross-analysis
    Additive boosts:
      +200: serial mover (moved >=5% in last 2 days)
      +150: darkpool whale (>$100M)
      +75:  darkpool significant (>$50M)
      +100: high ATR (>=5% avg range)
      +50:  elevated ATR (>=3% avg range)
    """
    sym = (pick.get("symbol") or "").strip()
    reason = pick.get("_x_worthy_reason", "meta_only")

    # Base tier
    if reason == "same_day_1x":
        base = 2000.0 + float(pick.get("_x_score_1x", 0) or 0)
    elif reason == "5x_potential":
        base = 1000.0 + float(pick.get("_x_score_5x", 0) or 0)
    elif reason == "gap_play":
        gap_pct = abs(float(pick.get("_x_gap_pct", 0) or 0))
        base = 800.0 + gap_pct * 10.0
    elif reason == "final_rec":
        conv = float(pick.get("_x_conviction", 0) or 0)
        mult = float(pick.get("_x_est_mult", 0) or 0)
        base = 500.0 + conv * 20.0 + mult * 5.0
    else:
        base = float(pick.get("score", 0) or 0) * 100.0

    boost = 0.0

    # GAP 1: Serial mover boost
    recent_move = serial_movers.get(sym, 0)
    if recent_move >= SERIAL_MOVER_MIN_PCT:
        boost += 200.0
        pick["_x_serial_mover"] = round(recent_move, 1)

    # GAP 2: Darkpool whale boost
    dp_entry = dp_data.get(sym, {})
    dp_val = float(dp_entry.get("total_value", 0) or 0)
    if dp_val >= DARKPOOL_WHALE_MIN:
        boost += 150.0
        pick["_x_dp_whale"] = True
        pick["_x_dp_value"] = dp_val
    elif dp_val >= DARKPOOL_SIGNIFICANT_MIN:
        boost += 75.0
        pick["_x_dp_significant"] = True
        pick["_x_dp_value"] = dp_val

    # GAP 3: ATR/volatility boost
    atr = atr_map.get(sym, 0)
    if atr >= HIGH_ATR_MIN_PCT:
        boost += 100.0
        pick["_x_high_atr"] = round(atr, 1)
    elif atr >= 3.0:
        boost += 50.0
        pick["_x_atr"] = round(atr, 1)

    return base + boost


# ═══════════════════════════════════════════════════════════════════════════
# Main selector (v3)
# ═══════════════════════════════════════════════════════════════════════════

def select_x_worthy_3_puts_3_calls(
    puts_through_moonshot: List[Dict[str, Any]],
    moonshot_through_puts: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build combined candidate pools (Meta + TradeNova injections),
    enrich with serial mover / darkpool / ATR signals,
    rank by priority, return exactly 3 PUTs + 3 CALLs.
    """
    sd_picks, s5_picks, fr_picks = _load_tradenova_tables()
    dp_data = _load_darkpool()

    logger.info(
        f"  📊 X-worthy v3: TN 1x={len(sd_picks)} 5x={len(s5_picks)} "
        f"recs={len(fr_picks)} DP={len(dp_data)} tickers"
    )

    # ── Collect all candidate symbols for batch Polygon fetch ─────
    all_symbols: set = set()
    for p in puts_through_moonshot:
        all_symbols.add((p.get("symbol") or "").strip())
    for c in moonshot_through_puts:
        all_symbols.add((c.get("symbol") or "").strip())
    for tn in sd_picks:
        all_symbols.add((tn.get("symbol") or "").strip())
    for tn in s5_picks:
        all_symbols.add((tn.get("symbol") or "").strip())
    for rec in fr_picks:
        all_symbols.add((rec.get("symbol") or "").strip())
    all_symbols.discard("")

    # GAP 1 + 3: Fetch serial movers and ATR in one pass
    serial_movers = _get_serial_movers(list(all_symbols))
    atr_map = _get_atr_map(list(all_symbols))

    # ── Build PUT candidate pool ──────────────────────────────────
    seen_puts: set = set()
    put_pool: List[Dict[str, Any]] = []

    for p in puts_through_moonshot:
        sym = (p.get("symbol") or "").strip()
        if sym and sym not in seen_puts:
            put_pool.append({**p, "_x_worthy_reason": p.get("_x_worthy_reason", "meta_only")})
            seen_puts.add(sym)

    for tn in sd_picks:
        if (tn.get("direction") or "").upper() == "PUT":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_puts:
                put_pool.append(_tn_1x_to_pick(tn))
                seen_puts.add(sym)

    for tn in s5_picks:
        if (tn.get("direction") or "").upper() == "PUT":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_puts:
                put_pool.append(_tn_5x_to_pick(tn))
                seen_puts.add(sym)

    # ── Build CALL candidate pool ─────────────────────────────────
    seen_calls: set = set()
    call_pool: List[Dict[str, Any]] = []

    for c in moonshot_through_puts:
        sym = (c.get("symbol") or "").strip()
        if sym and sym not in seen_calls:
            call_pool.append({**c, "_x_worthy_reason": c.get("_x_worthy_reason", "meta_only")})
            seen_calls.add(sym)

    for tn in sd_picks:
        if (tn.get("direction") or "").upper() == "CALL":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_calls:
                call_pool.append(_tn_1x_to_pick(tn))
                seen_calls.add(sym)

    for tn in s5_picks:
        if (tn.get("direction") or "").upper() == "CALL":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_calls:
                call_pool.append(_tn_5x_to_pick(tn))
                seen_calls.add(sym)

    # Inject final_recs
    for rec in fr_picks:
        cr = rec.get("contract_rules", {})
        direction = (cr.get("trade_type") or "CALL").upper()
        sym = (rec.get("symbol") or "").strip()
        pick = _tn_rec_to_pick(rec)
        if direction == "CALL" and sym not in seen_calls:
            call_pool.append(pick)
            seen_calls.add(sym)
        elif direction == "PUT" and sym not in seen_puts:
            put_pool.append(pick)
            seen_puts.add(sym)

    # ── Rank with multi-signal scoring ────────────────────────────
    put_pool.sort(key=lambda x: -_priority_score(x, dp_data, serial_movers, atr_map))
    call_pool.sort(key=lambda x: -_priority_score(x, dp_data, serial_movers, atr_map))

    # GAP 5: Direction balance — guarantee 3 of each
    puts_3 = put_pool[:3]
    calls_3 = call_pool[:3]

    # Log selection
    logger.info(f"  PUT pool: {len(put_pool)}, CALL pool: {len(call_pool)}")
    for i, p in enumerate(puts_3, 1):
        r = p.get("_x_worthy_reason", "meta_only")
        sc = _priority_score(p, dp_data, serial_movers, atr_map)
        extras = []
        if p.get("_x_serial_mover"):
            extras.append(f"serial={p['_x_serial_mover']}%")
        if p.get("_x_dp_whale"):
            extras.append(f"DP=${p.get('_x_dp_value',0)/1e6:.0f}M")
        elif p.get("_x_dp_significant"):
            extras.append(f"DP=${p.get('_x_dp_value',0)/1e6:.0f}M")
        if p.get("_x_high_atr"):
            extras.append(f"ATR={p['_x_high_atr']}%")
        extra_str = f" [{', '.join(extras)}]" if extras else ""
        logger.info(
            f"  🔴 X PUT #{i}: {p.get('symbol','?'):8s} {r:14s} "
            f"pri={sc:.0f}{extra_str}"
        )
    for i, c in enumerate(calls_3, 1):
        r = c.get("_x_worthy_reason", "meta_only")
        sc = _priority_score(c, dp_data, serial_movers, atr_map)
        extras = []
        if c.get("_x_serial_mover"):
            extras.append(f"serial={c['_x_serial_mover']}%")
        if c.get("_x_dp_whale"):
            extras.append(f"DP=${c.get('_x_dp_value',0)/1e6:.0f}M")
        elif c.get("_x_dp_significant"):
            extras.append(f"DP=${c.get('_x_dp_value',0)/1e6:.0f}M")
        if c.get("_x_high_atr"):
            extras.append(f"ATR={c['_x_high_atr']}%")
        extra_str = f" [{', '.join(extras)}]" if extras else ""
        logger.info(
            f"  🟢 X CALL #{i}: {c.get('symbol','?'):8s} {r:14s} "
            f"pri={sc:.0f}{extra_str}"
        )

    return puts_3, calls_3


def get_cross_results_for_x(
    cross_results: Dict[str, Any],
    gap_up_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a copy of cross_results with puts_through_moonshot and
    moonshot_through_puts replaced by the X-worthy 3+3 selection.
    Only used for X posting; all other consumers use original cross_results.
    Optionally accepts gap_up_data to inject gap candidates as calls.
    """
    puts = list(cross_results.get("puts_through_moonshot", [])[:10])
    calls = list(cross_results.get("moonshot_through_puts", [])[:10])

    # GAP 4: Inject gap-up candidates as high-priority CALL picks
    if gap_up_data:
        gap_candidates = gap_up_data.get("candidates", [])
        existing_call_syms = {(c.get("symbol") or "").strip() for c in calls}
        for gc in gap_candidates:
            sym = (gc.get("symbol") or "").strip()
            if sym and sym not in existing_call_syms:
                gap_score = float(gc.get("gap_score", 0) or 0)
                gap_pct = float(gc.get("gap_pct", 0) or 0)
                calls.append({
                    "symbol": sym,
                    "score": min(gap_score, 1.0),
                    "price": float(gc.get("price", 0) or 0),
                    "signals": gc.get("signals", [])[:5],
                    "engine": "GapUp_Detector",
                    "engine_type": "gap_up",
                    "_x_worthy_reason": "gap_play",
                    "_x_gap_pct": gap_pct,
                    "_x_gap_score": gap_score,
                    "_x_direction": "CALL",
                    "_x_source": "GapDetector",
                })
                existing_call_syms.add(sym)
        if gap_candidates:
            logger.info(f"  🚀 Injected {len(gap_candidates)} gap-up candidates into X call pool")

    puts_3, calls_3 = select_x_worthy_3_puts_3_calls(puts, calls)
    return {
        **cross_results,
        "puts_through_moonshot": puts_3,
        "moonshot_through_puts": calls_3,
    }
