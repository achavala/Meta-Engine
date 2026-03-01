"""
X-Worthy Picks Selector v2 — Injection + Ranking for X/Twitter
══════════════════════════════════════════════════════════════════════════════

PROBLEM (v1):
  Meta's cross-analysis top 10 and TradeNova's 1x/5x tables share almost
  ZERO overlap.  Feb 27: Meta had 12 symbols; TradeNova 1x had 15 symbols;
  overlap was 1 (SMCI).  Reordering within Meta's list is useless.

FIX (v2):
  1. Load TradeNova's 3 tables: same_day_1x, sure_shot_5x, final_recs.
  2. INJECT the best 1x/5x picks into the candidate pool for X posting.
  3. Rank combined pool: 1x same-day > 5x potential > final_recs > Meta score.
  4. Return exactly 3 PUTs + 3 CALLs (no duplicates).

  The injected picks get a standardized pick dict so x_poster.py can
  format them identically to Meta picks (symbol, score, price, signals).

DOES NOT TOUCH:
  - cross_analyzer, puts_adapter, moonshot_adapter, meta_engine pipeline
  - Email, Telegram, dashboard, trading — only the X post input is changed.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

TRADENOVA_DATA = Path.home() / "TradeNova" / "data"
SAME_DAY_1X_PATH = TRADENOVA_DATA / "same_day_1x_cache.json"
SURE_SHOT_5X_PATH = TRADENOVA_DATA / "sure_shot_5x_cache.json"
FINAL_RECS_PATH = TRADENOVA_DATA / "final_recommendations.json"

MAX_STALE_HOURS = 24  # Ignore TradeNova caches older than this


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
        return False  # No timestamp = use anyway
    try:
        dt = datetime.fromisoformat(ts)
        age_h = (datetime.now() - dt).total_seconds() / 3600
        return age_h > max_hours
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════
# Load all 3 TradeNova tables
# ═══════════════════════════════════════════════════════════════════════════

def _load_tradenova_tables() -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Returns:
        (same_day_1x_picks, sure_shot_5x_picks, final_recs_picks)
        Each is a list of raw TradeNova pick dicts.
        Stale data (>24h) is dropped with a warning.
    """
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


# ═══════════════════════════════════════════════════════════════════════════
# Convert TradeNova pick → standardized Meta-like pick dict
# ═══════════════════════════════════════════════════════════════════════════

def _tn_1x_to_pick(tn: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a same_day_1x pick to a standardized pick dict for x_poster."""
    sym = (tn.get("symbol") or "").strip()
    direction = (tn.get("direction") or "").upper()
    price = float(tn.get("current_price", 0) or 0)
    score_1x = float(tn.get("score_1x", 0) or 0)
    est_mult = float(tn.get("est_multiplier", 0) or 0)
    reasons = tn.get("reasons", [])
    inst_sigs = int(tn.get("inst_signals", 0) or 0)

    return {
        "symbol": sym,
        "score": min(score_1x / 100.0, 1.0),
        "price": price,
        "signals": reasons[:5],
        "engine": f"TradeNova_1x ({direction})",
        "engine_type": "same_day_1x",
        "_x_worthy_reason": "same_day_1x",
        "_x_score_1x": score_1x,
        "_x_est_mult": est_mult,
        "_x_direction": direction,
        "_x_inst_signals": inst_sigs,
        "_x_source": "TradeNova",
    }


def _tn_5x_to_pick(tn: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a sure_shot_5x pick to a standardized pick dict."""
    sym = (tn.get("symbol") or "").strip()
    direction = (tn.get("direction") or "").upper()
    price = float(tn.get("current_price", 0) or 0)
    score_5x = float(tn.get("score_5x", 0) or 0)
    est_mult = float(tn.get("est_multiplier", 0) or 0)
    reasons = tn.get("reasons_5x", [])

    return {
        "symbol": sym,
        "score": min(score_5x / 100.0, 1.0),
        "price": price,
        "signals": reasons[:5],
        "engine": f"TradeNova_5x ({direction})",
        "engine_type": "sure_shot_5x",
        "_x_worthy_reason": "5x_potential",
        "_x_score_5x": score_5x,
        "_x_est_mult": est_mult,
        "_x_direction": direction,
        "_x_source": "TradeNova",
    }


def _tn_rec_to_pick(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a final_recommendation to a standardized pick dict."""
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
        "symbol": sym,
        "score": min(composite / 600.0, 1.0),
        "price": price,
        "signals": signals,
        "engine": f"TradeNova_Rec ({','.join(engines[:2])})",
        "engine_type": "final_recommendation",
        "entry_low": entry_low,
        "entry_high": entry_high,
        "target": target,
        "_x_worthy_reason": "final_rec",
        "_x_composite": composite,
        "_x_est_mult": est_opt,
        "_x_direction": direction,
        "_x_grade": grade,
        "_x_conviction": conviction,
        "_x_source": "TradeNova",
    }


# ═══════════════════════════════════════════════════════════════════════════
# Priority scoring for combined pool
# ═══════════════════════════════════════════════════════════════════════════

def _priority_score(pick: Dict[str, Any]) -> float:
    """
    Combined priority score for ranking. Higher = first in line for X.
    Tier 1 (2000+): same_day_1x — minimum 1x same-day, ideal for X.
    Tier 2 (1000+): sure_shot_5x — 5x in 1-2 days, strong for X.
    Tier 3 (500+):  final_recs with est_opt >= 4x and conviction 5.
    Tier 4 (0-100): Meta cross-analysis by score.
    """
    reason = pick.get("_x_worthy_reason", "meta_only")
    if reason == "same_day_1x":
        return 2000.0 + float(pick.get("_x_score_1x", 0) or 0)
    if reason == "5x_potential":
        return 1000.0 + float(pick.get("_x_score_5x", 0) or 0)
    if reason == "final_rec":
        conv = float(pick.get("_x_conviction", 0) or 0)
        mult = float(pick.get("_x_est_mult", 0) or 0)
        return 500.0 + conv * 20.0 + mult * 5.0
    return float(pick.get("score", 0) or 0) * 100.0


# ═══════════════════════════════════════════════════════════════════════════
# Main selector
# ═══════════════════════════════════════════════════════════════════════════

def select_x_worthy_3_puts_3_calls(
    puts_through_moonshot: List[Dict[str, Any]],
    moonshot_through_puts: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build combined candidate pools (Meta + TradeNova injections),
    rank by priority, return exactly 3 PUTs + 3 CALLs.
    """
    sd_picks, s5_picks, fr_picks = _load_tradenova_tables()

    logger.info(
        f"  📊 X-worthy v2: TradeNova 1x={len(sd_picks)}, "
        f"5x={len(s5_picks)}, recs={len(fr_picks)}"
    )

    # ── Build PUT candidate pool ──────────────────────────────────
    seen_puts: set = set()
    put_pool: List[Dict[str, Any]] = []

    # Meta puts first (keep original data)
    for p in puts_through_moonshot:
        sym = (p.get("symbol") or "").strip()
        if sym and sym not in seen_puts:
            put_pool.append({**p, "_x_worthy_reason": p.get("_x_worthy_reason", "meta_only")})
            seen_puts.add(sym)

    # Inject TradeNova 1x PUTs
    for tn in sd_picks:
        if (tn.get("direction") or "").upper() == "PUT":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_puts:
                put_pool.append(_tn_1x_to_pick(tn))
                seen_puts.add(sym)

    # Inject TradeNova 5x PUTs
    for tn in s5_picks:
        if (tn.get("direction") or "").upper() == "PUT":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_puts:
                put_pool.append(_tn_5x_to_pick(tn))
                seen_puts.add(sym)

    # ── Build CALL candidate pool ─────────────────────────────────
    seen_calls: set = set()
    call_pool: List[Dict[str, Any]] = []

    # Meta calls first
    for c in moonshot_through_puts:
        sym = (c.get("symbol") or "").strip()
        if sym and sym not in seen_calls:
            call_pool.append({**c, "_x_worthy_reason": c.get("_x_worthy_reason", "meta_only")})
            seen_calls.add(sym)

    # Inject TradeNova 1x CALLs
    for tn in sd_picks:
        if (tn.get("direction") or "").upper() == "CALL":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_calls:
                call_pool.append(_tn_1x_to_pick(tn))
                seen_calls.add(sym)

    # Inject TradeNova 5x CALLs
    for tn in s5_picks:
        if (tn.get("direction") or "").upper() == "CALL":
            sym = (tn.get("symbol") or "").strip()
            if sym and sym not in seen_calls:
                call_pool.append(_tn_5x_to_pick(tn))
                seen_calls.add(sym)

    # Inject TradeNova final_recs (direction from contract_rules.trade_type)
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

    # ── Rank and select top 3 each ────────────────────────────────
    put_pool.sort(key=lambda x: -_priority_score(x))
    call_pool.sort(key=lambda x: -_priority_score(x))

    puts_3 = put_pool[:3]
    calls_3 = call_pool[:3]

    # Log selection
    logger.info(f"  PUT pool size: {len(put_pool)}, CALL pool size: {len(call_pool)}")
    for i, p in enumerate(puts_3, 1):
        r = p.get("_x_worthy_reason", "meta_only")
        sc = _priority_score(p)
        logger.info(
            f"  🔴 X PUT #{i}: {p.get('symbol','?'):8s} reason={r:14s} "
            f"priority={sc:.0f} score={p.get('score',0):.3f}"
        )
    for i, c in enumerate(calls_3, 1):
        r = c.get("_x_worthy_reason", "meta_only")
        sc = _priority_score(c)
        logger.info(
            f"  🟢 X CALL #{i}: {c.get('symbol','?'):8s} reason={r:14s} "
            f"priority={sc:.0f} score={c.get('score',0):.3f}"
        )

    return puts_3, calls_3


def get_cross_results_for_x(
    cross_results: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build a copy of cross_results with puts_through_moonshot and
    moonshot_through_puts replaced by the X-worthy 3+3 selection.
    Only used for X posting; all other consumers use original cross_results.
    """
    puts = list(cross_results.get("puts_through_moonshot", [])[:10])
    calls = list(cross_results.get("moonshot_through_puts", [])[:10])
    puts_3, calls_3 = select_x_worthy_3_puts_3_calls(puts, calls)
    return {
        **cross_results,
        "puts_through_moonshot": puts_3,
        "moonshot_through_puts": calls_3,
    }
