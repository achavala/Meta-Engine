"""
Moonshot/TradeNova Engine Adapter
==================================
Interface to get Top 10 moonshot candidates from the TradeNova Moonshot Engine.
Imports Moonshot modules directly without modifying the original codebase.

Data Sources (priority order for fallback):
  0. eod_interval_picks.json — Broadest pool: 10 picks per interval scan,
     captures intraday momentum names (MSFT, HIMS, BILL, etc.) that
     final_recommendations.json often misses.
  1. final_recommendations.json — Curated ranked picks with UW sentiment.
  2. final_recommendations_history.json — Historical picks (supplement).
  3. moonshot CSV data — Oldest fallback.

FEB 11 ADDITION — Call Options Return Multiplier (ORM):
  Just as the PutsEngine ORM ranks puts by "will the PUT OPTION pay 3x–10x?",
  the Call ORM ranks moonshot picks by "will the CALL OPTION pay 3x–10x?"

  A stock can rally +5% and:
    • With HIGH gamma leverage + LOW IV  → call pays 10x
    • With LOW gamma + HIGH IV (crush)   → call barely breaks even

  Call ORM sub-factors (8 total):
    1. Gamma Leverage         (20%) — near call-wall + negative GEX = squeeze
    2. IV Expansion Potential  (15%) — low IV = cheap options, room to expand
    3. OI Positioning          (15%) — call OI build-up, aggressive call buying
    4. Delta Sweet Spot        (10%) — 0.20–0.40 delta = maximum leverage
    5. Short DTE               (10%) — 0–5 DTE = maximum gamma leverage
    6. Volatility Regime       (10%) — trending + volatile = best for calls
    7. Dealer Positioning      (10%) — call wall proximity, gamma squeeze
    8. Liquidity & Spread      (10%) — tight spreads + high volume
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# Add TradeNova to path
TRADENOVA_PATH = str(Path.home() / "TradeNova")
if TRADENOVA_PATH not in sys.path:
    sys.path.insert(0, TRADENOVA_PATH)


def get_top_moonshots(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Get the Top N moonshot candidates from the TradeNova Moonshot Engine.
    
    Uses the Moonshot Engine's scanning pipeline to find the best
    lottery-ticket / high-momentum candidates.
    
    Args:
        top_n: Number of top picks to return (default 10)
        
    Returns:
        List of dicts with keys: symbol, score, price, signals, etc.
    """
    try:
        from moonshot.core.engine import MoonshotEngine
        from moonshot.config import MoonshotConfig
        
        logger.info("🟢 Moonshot Engine: Running full scan...")
        
        engine = MoonshotEngine(paper_mode=True)
        
        # Run the moonshot scan
        candidates = engine.scan_for_moonshots()
        
        results = []
        for c in candidates:
            result = {
                "symbol": c.symbol if hasattr(c, 'symbol') else str(c),
                "score": c.score if hasattr(c, 'score') else 0,
                "price": c.price if hasattr(c, 'price') else 0,
                "signals": [],
                "signal_types": [],
                "option_type": getattr(c, 'option_type', 'call'),
                "target_return": getattr(c, 'target_return', 0),
                "engine": "Moonshot",
                "sector": getattr(c, 'sector', ''),
                "volume_ratio": getattr(c, 'volume_ratio', 0),
                "short_interest": getattr(c, 'short_interest', 0),
            }
            
            # Extract signals from candidate
            if hasattr(c, 'signals') and c.signals:
                if isinstance(c.signals, list):
                    result["signals"] = [str(s) for s in c.signals]
                elif isinstance(c.signals, dict):
                    result["signals"] = list(c.signals.keys())
            
            if hasattr(c, 'signal_types') and c.signal_types:
                result["signal_types"] = [str(st) for st in c.signal_types]
            
            results.append(result)
        
        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)
        
        # If live scan returned nothing (e.g. weekend/holiday), fall back to cached data
        if not results:
            logger.info("🟢 Live scan returned 0 candidates — falling back to cached TradeNova data...")
            return _fallback_from_cached_moonshots(top_n)

        # ── Apply Call Options Return Multiplier (ORM) ──────────────
        # Enriches ALL candidates (not just top_n) so that stocks with
        # superior options microstructure can rise to the top.
        results = _enrich_moonshots_with_orm(results, top_n)
        
        top_picks = results[:top_n]
        logger.info(f"🟢 Moonshot Engine: Top {len(top_picks)} picks selected")
        for i, p in enumerate(top_picks, 1):
            logger.info(f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — ${p['price']:.2f}")
        
        return top_picks
        
    except Exception as e:
        logger.error(f"Moonshot scan failed: {e}")
        return _fallback_from_cached_moonshots(top_n)


def _fallback_from_cached_moonshots(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Fallback: Read from TradeNova's cached results.
    Priority order:
      0. eod_interval_picks.json (broadest pool — captures MSFT, HIMS, etc.)
      1. final_recommendations.json (most recent ranked picks)
      2. final_recommendations_history.json (historical picks)
      3. moonshot/data_collection/*.csv (raw moonshot scans)
    """
    results = []
    data_timestamps: Dict[str, str] = {}  # Track data freshness per source

    # --- Source 0: eod_interval_picks.json (broadest candidate pool) ---
    # This file contains 10 picks per interval scan throughout the day.
    # It captures high-momentum intraday names that final_recommendations
    # may miss entirely (e.g., MSFT +3.1%, HIMS +12.8%, BILL +3.7%).
    try:
        eod_file = Path(TRADENOVA_PATH) / "data" / "eod_interval_picks.json"
        if eod_file.exists():
            with open(eod_file) as f:
                eod_data = json.load(f)

            eod_date = eod_data.get("date", "")
            intervals = eod_data.get("intervals", {})
            data_timestamps["eod_interval_picks"] = eod_date

            # Determine data freshness — only use if from today or yesterday
            today_str = datetime.now().strftime("%Y-%m-%d")
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            # Also accept 2-day-old data for Monday mornings (Friday's data)
            three_days_ago_str = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

            if eod_date >= three_days_ago_str:
                # Aggregate all unique symbols across all intervals.
                # Track how many intervals each symbol appears in (persistence)
                # — symbols in more intervals have stronger conviction.
                eod_candidates: Dict[str, Dict[str, Any]] = {}
                interval_counts: Dict[str, int] = {}
                for interval_key, interval_data in intervals.items():
                    for pick in interval_data.get("picks", []):
                        sym = pick.get("symbol", "")
                        if not sym:
                            continue
                        score = pick.get("score", 0)
                        interval_counts[sym] = interval_counts.get(sym, 0) + 1

                        existing = eod_candidates.get(sym)
                        if existing is None or score > existing.get("score", 0):
                            eod_candidates[sym] = {
                                "symbol": sym,
                                "score": score,
                                "price": pick.get("current_price", 0) or pick.get("entry_price", 0),
                                "signals": pick.get("signals", []),
                                "signal_types": [pick.get("engine", "")],
                                "option_type": "call",
                                "target_return": 0,
                                "engine": f"Moonshot (interval:{interval_key})",
                                "sector": "",
                                "volume_ratio": pick.get("weighted_rvol", 0) or pick.get("rvol", 0),
                                "short_interest": pick.get("short_float", 0),
                                "action": pick.get("action", ""),
                                "entry_low": pick.get("entry_price", 0),
                                "entry_high": pick.get("entry_price", 0),
                                "target": 0,
                                "stop": 0,
                                "rsi": 50,
                                "uw_sentiment": "",
                                "data_source": f"eod_interval_picks ({eod_date})",
                                "data_age_days": (datetime.now() - datetime.strptime(eod_date, "%Y-%m-%d")).days if eod_date else -1,
                                "velocity_score": pick.get("velocity_score", 0),
                                "rs_acceleration": pick.get("rs_acceleration", 0),
                            }

                # Attach interval persistence count to each candidate
                for sym, cand in eod_candidates.items():
                    cand["interval_persistence"] = interval_counts.get(sym, 1)

                results.extend(eod_candidates.values())
                logger.info(
                    f"🟢 TradeNova eod_interval_picks: {len(eod_candidates)} unique symbols "
                    f"from {len(intervals)} intervals (date: {eod_date})"
                )
            else:
                logger.info(
                    f"🟢 eod_interval_picks.json skipped — data from {eod_date} is too old "
                    f"(today: {today_str})"
                )
    except Exception as e:
        logger.debug(f"Failed to read eod_interval_picks.json: {e}")

    # --- Source 1: final_recommendations.json (best curated source) ---
    try:
        recs_file = Path(TRADENOVA_PATH) / "data" / "final_recommendations.json"
        if recs_file.exists():
            with open(recs_file) as f:
                data = json.load(f)
            recs = data.get("recommendations", [])
            scan_label = data.get("scan_label", "")
            generated = data.get("generated_at", "")
            data_timestamps["final_recommendations"] = generated
            logger.info(f"🟢 TradeNova cached: {len(recs)} recommendations from '{scan_label}' ({generated})")

            for rec in recs:
                results.append({
                    "symbol": rec.get("symbol", ""),
                    "score": rec.get("composite_score", 0) / 100.0,  # Normalize 0-100 → 0-1
                    "price": rec.get("current_price", 0),
                    "signals": rec.get("signals", []),
                    "signal_types": rec.get("engines", []),
                    "option_type": "call",
                    "target_return": 0,
                    "engine": "Moonshot",
                    "sector": "",
                    "volume_ratio": 0,
                    "short_interest": 0,
                    "conviction": rec.get("conviction", 0),
                    "entry_low": rec.get("entry_low", 0),
                    "entry_high": rec.get("entry_high", 0),
                    "target": rec.get("target", 0),
                    "stop": rec.get("stop", 0),
                    "why": rec.get("why", ""),
                    "rsi": rec.get("rsi", 50),
                    "uw_sentiment": rec.get("uw_sentiment", ""),
                    "data_source": f"final_recommendations ({generated})",
                    "data_age_days": _calc_data_age_days(generated),
                })
    except Exception as e:
        logger.debug(f"Failed to read final_recommendations.json: {e}")

    # --- Source 2: final_recommendations_history.json (supplement) ---
    if len(results) < top_n:
        try:
            hist_file = Path(TRADENOVA_PATH) / "data" / "final_recommendations_history.json"
            if hist_file.exists():
                with open(hist_file) as f:
                    history = json.load(f)
                # Get unique symbols from history that aren't already in results
                existing_symbols = {r["symbol"] for r in results}
                for entry in reversed(history):  # Most recent first
                    hist_gen = entry.get("generated_at", "")
                    for rec in entry.get("recommendations", []):
                        sym = rec.get("symbol", "")
                        if sym and sym not in existing_symbols:
                            results.append({
                                "symbol": sym,
                                "score": rec.get("composite_score", 0) / 100.0,
                                "price": rec.get("current_price", 0),
                                "signals": rec.get("signals", []),
                                "signal_types": rec.get("engines", []),
                                "option_type": "call",
                                "target_return": 0,
                                "engine": "Moonshot",
                                "sector": "",
                                "volume_ratio": 0,
                                "short_interest": 0,
                                "conviction": rec.get("conviction", 0),
                                "why": rec.get("why", ""),
                                "rsi": rec.get("rsi", 50),
                                "uw_sentiment": rec.get("uw_sentiment", ""),
                                "data_source": f"final_recommendations_history ({hist_gen})",
                                "data_age_days": _calc_data_age_days(hist_gen),
                            })
                            existing_symbols.add(sym)
        except Exception as e:
            logger.debug(f"Failed to read history: {e}")

    # --- Source 3: moonshot CSV data (oldest fallback) ---
    if len(results) < top_n:
        try:
            import pandas as pd
            data_dir = Path(TRADENOVA_PATH) / "moonshot" / "data_collection"
            csv_files = sorted(data_dir.glob("moonshot_daily_*.csv"), reverse=True)
            if csv_files:
                csv_name = csv_files[0].name
                df = pd.read_csv(csv_files[0])
                existing_symbols = {r["symbol"] for r in results}
                for _, row in df.iterrows():
                    sym = row.get("symbol", "")
                    if sym and sym not in existing_symbols:
                        results.append({
                            "symbol": sym,
                            "score": row.get("moonshot_score", 0) / 100.0,
                            "price": row.get("stock_price", 0),
                            "signals": [],
                            "signal_types": [],
                            "option_type": row.get("option_type", "call"),
                            "target_return": 0,
                            "engine": "Moonshot (csv)",
                            "sector": "",
                            "volume_ratio": 0,
                            "short_interest": 0,
                            "data_source": f"moonshot CSV ({csv_name})",
                            "data_age_days": -1,
                        })
                        existing_symbols.add(sym)
        except Exception as e:
            logger.debug(f"Failed to read CSV fallback: {e}")

    # Deduplicate — keep the entry with the highest score per symbol
    seen = {}
    for r in results:
        sym = r["symbol"]
        if sym not in seen or r["score"] > seen[sym]["score"]:
            seen[sym] = r

    # Sort with multi-factor tie-breaker:
    #   1. Score (primary — higher is better)
    #   2. Interval persistence (how many scans flagged this symbol — higher = stronger conviction)
    #   3. Conviction from final_recommendations (if available)
    #   4. Volume ratio (higher = more interesting)
    deduped = sorted(
        seen.values(),
        key=lambda x: (
            x.get("score", 0) or 0,
            x.get("interval_persistence", 0) or 0,
            x.get("conviction", 0) or 0,
            x.get("volume_ratio", 0) or 0,
        ),
        reverse=True,
    )

    # ── Apply Call Options Return Multiplier (ORM) ──────────────
    # Enriches ALL candidates (not just top_n) so that stocks with
    # superior options microstructure can rise to the top.
    deduped = _enrich_moonshots_with_orm(deduped, top_n)

    top_picks = deduped[:top_n]

    # Log data freshness for transparency
    sources_used = set()
    for p in top_picks:
        ds = p.get("data_source", "unknown")
        sources_used.add(ds.split(" (")[0] if " (" in ds else ds)
    logger.info(
        f"🟢 Moonshot (cached): {len(deduped)} total candidates, "
        f"returning top {top_n}. Sources: {', '.join(sorted(sources_used))}"
    )
    for i, p in enumerate(top_picks, 1):
        age = p.get("data_age_days", -1)
        age_tag = f" [DATA AGE: {age}d]" if age >= 0 else ""
        logger.info(
            f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — "
            f"${p.get('price', 0):.2f}{age_tag}"
        )

    return top_picks


def _calc_data_age_days(timestamp_str: str) -> int:
    """
    Calculate how many days old a timestamp string is.
    Accepts ISO format or common date formats.
    Returns -1 if parsing fails.
    """
    if not timestamp_str:
        return -1
    try:
        # Try ISO format first
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        age = (datetime.now() - ts.replace(tzinfo=None)).days
        return max(0, age)
    except (ValueError, TypeError):
        pass
    # Try date-only format
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            ts = datetime.strptime(timestamp_str[:10], fmt)
            age = (datetime.now() - ts).days
            return max(0, age)
        except (ValueError, TypeError):
            continue
    return -1


# ═══════════════════════════════════════════════════════════════════════
# CALL OPTIONS RETURN MULTIPLIER (ORM)  —  FEB 11
# ═══════════════════════════════════════════════════════════════════════
# Ranks call/moonshot candidates by expected CALL OPTIONS return (3x–10x),
# not just by composite score or interval persistence.  Uses 8 factors
# sourced from UW GEX, IV term structure, OI changes, flow, and dark pool.
#
# Key difference vs. Put ORM:
#   • Put ORM looks for put OI build-up, put wall proximity
#   • Call ORM looks for CALL OI build-up, call wall proximity,
#     gamma squeeze potential, bullish vanna regime
# ═══════════════════════════════════════════════════════════════════════

# Paths to TradeNova UW cache files
_TRADENOVA_DATA = Path.home() / "TradeNova" / "data"

# Module-level UW cache (loaded once per pipeline run)
_call_uw_gex: Optional[Dict[str, Any]] = None
_call_uw_iv: Optional[Dict[str, Any]] = None
_call_uw_oi: Optional[Dict[str, Any]] = None
_call_uw_flow: Optional[Dict[str, Any]] = None
_call_uw_dp: Optional[Dict[str, Any]] = None


def _load_uw_options_data() -> Tuple[
    Dict[str, Any],  # gex
    Dict[str, Any],  # iv_term
    Dict[str, Any],  # oi_change
    Dict[str, Any],  # flow
    Dict[str, Any],  # darkpool
]:
    """
    Load ALL Unusual-Whales options-microstructure caches once.

    Returns five dicts keyed by symbol.  Each cache is read from
    ``~/TradeNova/data/`` and stripped of metadata keys.  The data is
    cached at module level so repeated calls within the same pipeline
    run are free.

    Files consumed (all READ-ONLY):
      - uw_gex_cache.json        (291 symbols — GEX / vanna / charm)
      - uw_iv_term_cache.json    (290 symbols — IV term structure)
      - uw_oi_change_cache.json  (290 symbols — OI build-up / new pos.)
      - uw_flow_cache.json       (261 symbols — individual trades w/ greeks)
      - darkpool_cache.json      (286 symbols — block / dark prints)
    """
    global _call_uw_gex, _call_uw_iv, _call_uw_oi, _call_uw_flow, _call_uw_dp

    def _load(fname: str, inner_key: Optional[str] = None) -> Dict[str, Any]:
        try:
            fpath = _TRADENOVA_DATA / fname
            if not fpath.exists():
                return {}
            with open(fpath) as f:
                raw = json.load(f)
            data = raw.get(inner_key, raw) if inner_key else raw
            if not isinstance(data, dict):
                return {}
            return {k: v for k, v in data.items()
                    if k not in ("timestamp", "generated_at")}
        except Exception as exc:
            logger.debug(f"  Call-ORM: failed to load {fname}: {exc}")
            return {}

    if _call_uw_gex is None:
        _call_uw_gex = _load("uw_gex_cache.json", "data")
    if _call_uw_iv is None:
        _call_uw_iv = _load("uw_iv_term_cache.json", "data")
    if _call_uw_oi is None:
        _call_uw_oi = _load("uw_oi_change_cache.json", "data")
    if _call_uw_flow is None:
        _call_uw_flow = _load("uw_flow_cache.json", "flow_data")
    if _call_uw_dp is None:
        _call_uw_dp = _load("darkpool_cache.json")

    loaded = sum(1 for d in [_call_uw_gex, _call_uw_iv, _call_uw_oi,
                              _call_uw_flow, _call_uw_dp] if d)
    logger.debug(f"  Call-ORM: Loaded {loaded}/5 UW cache files "
                 f"(GEX={len(_call_uw_gex)}, IV={len(_call_uw_iv)}, "
                 f"OI={len(_call_uw_oi)}, Flow={len(_call_uw_flow)}, "
                 f"DP={len(_call_uw_dp)} symbols)")

    return _call_uw_gex, _call_uw_iv, _call_uw_oi, _call_uw_flow, _call_uw_dp


def _compute_call_options_return_multiplier(
    symbol: str,
    gex: Dict[str, Any],
    iv: Dict[str, Any],
    oi: Dict[str, Any],
    flow: Dict[str, Any],
    dp: Dict[str, Any],
    stock_price: float = 0,
) -> Tuple[float, Dict[str, float]]:
    """
    Compute the Options Return Multiplier (ORM) for a CALL candidate.

    Institutional-grade scoring across 8 factors that predict
    **how much a CALL OPTION will pay**, not just whether the stock rallies.

    Philosophy (30 yr quant / PhD microstructure lens):
    ─────────────────────────────────────────────────────
    A stock can rally +5% and:
      • With gamma squeeze + LOW IV    → call pays 10x
      • With LOW gamma + HIGH IV crush → call barely breaks even

    KEY DIFFERENCES vs. Put ORM:
    ─────────────────────────────────────────────────────
    1. OI Positioning: looks for CALL OI build-up (not put OI)
    2. Dealer Positioning: CALL WALL proximity = squeeze potential
       (stock breaking through call wall forces dealers to delta-hedge
       by buying shares, amplifying the rally = GAMMA SQUEEZE)
    3. Flow: filters for CALL trades, not put trades
    4. Vanna Regime: VOL_CRUSH_BULLISH = positive for calls
       (declining vol + rising stock = dealers buy shares)

    Returns: (orm_score, {factor_name: factor_score})
    """
    factors: Dict[str, float] = {}
    sym_gex = gex.get(symbol, {})
    sym_iv  = iv.get(symbol, {})
    sym_oi  = oi.get(symbol, {})
    sym_flow = flow.get(symbol, [])
    sym_dp  = dp.get(symbol, {})
    if not isinstance(sym_flow, list):
        sym_flow = []

    # ── NO DATA FALLBACK ────────────────────────────────────────────
    has_any_data = bool(sym_gex or sym_iv or sym_oi or sym_flow or sym_dp)
    if not has_any_data:
        default = 0.35
        for f_name in ["gamma_leverage", "iv_expansion", "oi_positioning",
                        "delta_sweet", "short_dte", "vol_regime",
                        "dealer_position", "liquidity"]:
            factors[f_name] = default
        return default, factors

    # ── 1. GAMMA LEVERAGE (weight 0.20) ─────────────────────────────
    # For CALLS, two regimes create 10x potential:
    #   A) NEGATIVE GEX + stock breaking UP through levels
    #      → dealers chase the move (short gamma amplifies ALL moves)
    #   B) POSITIVE GEX near CALL WALL + stock approaching wall
    #      → dealers are long gamma from calls; as stock rises toward
    #        call wall, they must buy more shares to stay hedged
    #        → GAMMA SQUEEZE (like GME)
    #
    # The key metric: magnitude of gamma exposure + wall proximity
    gamma_score = 0.0
    if sym_gex:
        regime = sym_gex.get("regime", "UNKNOWN")
        net_gex = sym_gex.get("net_gex", 0)
        call_wall = sym_gex.get("call_wall", 0)

        if regime == "NEGATIVE":
            # Negative GEX = amplified moves in BOTH directions
            gamma_score = 0.55
            magnitude = abs(net_gex)
            if magnitude > 2_000_000:
                gamma_score += 0.35
            elif magnitude > 500_000:
                gamma_score += 0.20 + 0.15 * min((magnitude - 500_000) / 1_500_000, 1.0)
            elif magnitude > 100_000:
                gamma_score += 0.10
        elif regime == "POSITIVE":
            # Positive GEX with stock near call wall = squeeze setup
            gamma_score = 0.30
            magnitude = abs(net_gex)
            if magnitude > 2_000_000:
                gamma_score += 0.20  # Massive call gamma = squeeze fuel
            # Call wall proximity check
            if call_wall > 0 and stock_price > 0:
                dist_to_wall = (call_wall - stock_price) / stock_price * 100
                if 0 < dist_to_wall < 3:
                    gamma_score += 0.30  # Within 3% of call wall = imminent squeeze
                elif 0 < dist_to_wall < 5:
                    gamma_score += 0.15  # Approaching call wall
                elif dist_to_wall <= 0:
                    gamma_score += 0.20  # Already above call wall = breakout
            # Recent flip = volatility
            days_since = sym_gex.get("days_since_flip", 999)
            if days_since <= 3:
                gamma_score = max(gamma_score, 0.55)  # Just flipped — volatile
        else:
            gamma_score = 0.30  # Unknown / neutral
    factors["gamma_leverage"] = min(gamma_score, 1.0)

    # ── 2. IV EXPANSION POTENTIAL (weight 0.15) ─────────────────────
    # Same principle as puts: LOW/MODERATE IV = cheap calls + room to expand.
    # HIGH IV = expensive, IV crush on catalyst = kills returns.
    #
    # For calls: Inverted term structure can indicate near-term event
    # (earnings, catalyst) which could be bullish too — small bonus.
    iv_score = 0.0
    if sym_iv:
        front_iv = sym_iv.get("front_iv", 0)
        inverted = sym_iv.get("inverted", False)
        impl_move = sym_iv.get("implied_move_pct", 0)

        if front_iv > 0:
            if front_iv < 0.25:
                iv_score = 0.70  # Very low IV — cheap but low expected move
            elif front_iv < 0.40:
                iv_score = 1.00  # OPTIMAL: cheap options + moderate vol
            elif front_iv < 0.60:
                iv_score = 0.80  # Good — still room to expand
            elif front_iv < 0.80:
                iv_score = 0.40  # Getting expensive — IV crush risk
            else:
                iv_score = 0.15  # High IV — IV crush will eat returns

        # Bonus for inverted term structure (near-term catalyst)
        if inverted:
            iv_score = min(iv_score + 0.10, 1.0)

        # Bonus for high implied move (market expects big move)
        if impl_move > 0.04:
            iv_score = min(iv_score + 0.10, 1.0)
    factors["iv_expansion"] = min(iv_score, 1.0)

    # ── 3. OI POSITIONING (weight 0.15) ─────────────────────────────
    # CALL OI BUILDING = institutions loading up on calls.
    # This is the "smart money footprint" for imminent rallies.
    #
    # Key signals for CALLS (opposite of puts):
    #   - call_oi_pct_change > 20%     → aggressive call building
    #   - vol_gt_oi_count > 3          → new positions opening
    #   - contracts_3plus_days_oi_increase > 10 → PERSISTENT positioning
    #   - call_oi_pct > put_oi_pct * 1.5 → bullish skew
    oi_score = 0.0
    if sym_oi:
        call_oi_pct = sym_oi.get("call_oi_pct_change", 0)
        vol_gt_oi = sym_oi.get("vol_gt_oi_count", 0)
        persistent = sym_oi.get("contracts_3plus_days_oi_increase", 0)
        put_oi_pct = sym_oi.get("put_oi_pct_change", 0)

        # Call OI growth — stronger = more institutional conviction
        if call_oi_pct > 40:
            oi_score += 0.40
        elif call_oi_pct > 20:
            oi_score += 0.25
        elif call_oi_pct > 10:
            oi_score += 0.15

        # New positions (volume > OI) — aggressive new entries
        if vol_gt_oi > 5:
            oi_score += 0.25
        elif vol_gt_oi > 2:
            oi_score += 0.15

        # Persistent OI build (3+ days increasing)
        if persistent > 15:
            oi_score += 0.25
        elif persistent > 8:
            oi_score += 0.15
        elif persistent > 3:
            oi_score += 0.10

        # Call/Put OI skew — more calls than puts = bullish consensus
        if call_oi_pct > put_oi_pct * 1.5 and call_oi_pct > 15:
            oi_score += 0.10

    factors["oi_positioning"] = min(oi_score, 1.0)

    # ── 4. DELTA SWEET SPOT (weight 0.10) ───────────────────────────
    # For 3x–10x returns, OTM calls with delta 0.20–0.40 are optimal.
    # Same logic as puts — look at CALL flow instead.
    delta_score = 0.0
    call_trades = [t for t in sym_flow if t.get("put_call") == "C"]
    if call_trades:
        deltas = [abs(float(t.get("delta", 0) or 0)) for t in call_trades
                  if t.get("delta")]
        if deltas:
            avg_delta = sum(deltas) / len(deltas)
            sweet_count = sum(1 for d in deltas if 0.15 <= d <= 0.45)
            sweet_pct = sweet_count / len(deltas) if len(deltas) > 0 else 0

            if 0.20 <= avg_delta <= 0.40:
                delta_score = 1.0   # Perfect sweet spot
            elif 0.15 <= avg_delta <= 0.45:
                delta_score = 0.80  # Close to sweet spot
            elif 0.10 <= avg_delta <= 0.55:
                delta_score = 0.50  # Acceptable range
            elif avg_delta < 0.10:
                delta_score = 0.20  # Lottery territory
            else:
                delta_score = 0.30  # Too deep ITM

            if sweet_pct > 0.5:
                delta_score = min(delta_score + 0.10, 1.0)
    factors["delta_sweet"] = min(delta_score, 1.0)

    # ── 5. SHORT DTE (weight 0.10) ──────────────────────────────────
    # Same logic as puts — max gamma leverage at DTE 0–5.
    dte_score = 0.0
    if sym_iv:
        front_dte = sym_iv.get("front_dte", 30)
        if front_dte <= 2:
            dte_score = 1.0
        elif front_dte <= 5:
            dte_score = 0.90
        elif front_dte <= 10:
            dte_score = 0.70
        elif front_dte <= 14:
            dte_score = 0.50
        elif front_dte <= 21:
            dte_score = 0.30
        else:
            dte_score = 0.15
    elif call_trades:
        dtes = [int(t.get("dte", 30) or 30) for t in call_trades if t.get("dte")]
        if dtes:
            min_dte = min(dtes)
            if min_dte <= 5:
                dte_score = 0.90
            elif min_dte <= 14:
                dte_score = 0.60
            else:
                dte_score = 0.25
    factors["short_dte"] = min(dte_score, 1.0)

    # ── 6. VOLATILITY REGIME (weight 0.10) ──────────────────────────
    # For CALLS: trending + volatile = best.
    # HIGH implied_move + any amplifying regime = explosive.
    #
    # Key difference from puts:
    #   VOL_CRUSH_BULLISH vanna regime = POSITIVE for calls
    #   (declining vol + rising stock = dealers buy shares = amplifies rally)
    vol_score = 0.0
    if sym_iv:
        impl_move = sym_iv.get("implied_move_pct", 0)
        if impl_move > 0.05:
            vol_score = 1.0
        elif impl_move > 0.03:
            vol_score = 0.70
        elif impl_move > 0.02:
            vol_score = 0.40
        else:
            vol_score = 0.20
    # For calls: negative GEX amplifies, VOL_CRUSH_BULLISH amplifies
    if sym_gex:
        if sym_gex.get("regime") == "NEGATIVE":
            vol_score = min(vol_score + 0.15, 1.0)
        vanna = sym_gex.get("vanna_regime", "")
        if vanna == "VOL_CRUSH_BULLISH":
            vol_score = min(vol_score + 0.15, 1.0)
    factors["vol_regime"] = min(vol_score, 1.0)

    # ── 7. DEALER POSITIONING / GAMMA SQUEEZE (weight 0.10) ──────────
    # For CALLS: the key is CALL WALL proximity.
    # When stock approaches/breaks through the call wall, dealers who
    # sold calls must delta-hedge by BUYING shares → GAMMA SQUEEZE UP.
    #
    # This is the mechanic behind GME, AMC, and other squeeze plays.
    #
    # Also: recent gamma flip = maximum instability
    # Vanna: VOL_CRUSH_BULLISH = vol declining pushes dealers to buy
    dealer_score = 0.0
    if sym_gex:
        flip_today = sym_gex.get("gex_flip_today", False)
        days_since_flip = sym_gex.get("days_since_flip", 999)
        vanna_regime = sym_gex.get("vanna_regime", "NEUTRAL")
        call_wall = sym_gex.get("call_wall", 0)

        if flip_today:
            dealer_score = 0.90  # Maximum instability
        elif days_since_flip <= 2:
            dealer_score = 0.75
        elif days_since_flip <= 5:
            dealer_score = 0.55
        elif days_since_flip <= 10:
            dealer_score = 0.35
        else:
            dealer_score = 0.15

        # Vanna: VOL_CRUSH_BULLISH = dealers buy shares as vol declines
        if vanna_regime == "VOL_CRUSH_BULLISH":
            dealer_score = min(dealer_score + 0.20, 1.0)
        elif vanna_regime == "NEUTRAL":
            dealer_score = min(dealer_score + 0.05, 1.0)

        # Call wall proximity — THE squeeze trigger
        if call_wall > 0 and stock_price > 0:
            wall_dist_pct = (call_wall - stock_price) / stock_price * 100
            if wall_dist_pct <= 0:
                # Already ABOVE call wall — breakout territory
                dealer_score = min(dealer_score + 0.15, 1.0)
            elif wall_dist_pct < 2:
                # Within 2% of call wall — imminent squeeze
                dealer_score = min(dealer_score + 0.20, 1.0)
            elif wall_dist_pct < 5:
                # Approaching call wall
                dealer_score = min(dealer_score + 0.10, 1.0)
    factors["dealer_position"] = min(dealer_score, 1.0)

    # ── 8. LIQUIDITY & SPREAD QUALITY (weight 0.10) ─────────────────
    # Same principle as puts: tight spreads + high volume + urgency.
    # Uses CALL flow instead of put flow.
    liq_score = 0.0
    if call_trades:
        spreads = []
        for t in call_trades:
            bid = float(t.get("bid_price", 0) or 0)
            ask = float(t.get("ask_price", 0) or 0)
            if bid > 0 and ask > 0:
                spread_pct = (ask - bid) / ask * 100
                spreads.append(spread_pct)

        if spreads:
            avg_spread = sum(spreads) / len(spreads)
            if avg_spread < 3:
                liq_score += 0.40
            elif avg_spread < 6:
                liq_score += 0.30
            elif avg_spread < 10:
                liq_score += 0.15

        total_vol = sum(int(t.get("volume", 0) or 0) for t in call_trades)
        if total_vol > 5000:
            liq_score += 0.20
        elif total_vol > 1000:
            liq_score += 0.10

        sweeps = sum(1 for t in call_trades if t.get("is_sweep"))
        blocks = sum(1 for t in call_trades if t.get("is_block"))
        aggressive = sum(1 for t in call_trades
                         if t.get("aggressiveness") == "AGGRESSIVE_BUY")
        if sweeps > 0 or blocks > 2:
            liq_score += 0.15
        if len(call_trades) > 0 and aggressive > len(call_trades) * 0.4:
            liq_score += 0.10

    # Dark pool liquidity
    if sym_dp:
        dp_blocks = sym_dp.get("dark_block_count", 0)
        dp_pct_adv = sym_dp.get("pct_adv", 0)
        if dp_blocks > 50:
            liq_score += 0.15
        elif dp_blocks > 20:
            liq_score += 0.10
        if dp_pct_adv > 10:
            liq_score += 0.10

    factors["liquidity"] = min(liq_score, 1.0)

    # ── WEIGHTED COMBINATION ──────────────────────────────────────────
    orm = (
        factors["gamma_leverage"]  * 0.20 +
        factors["iv_expansion"]    * 0.15 +
        factors["oi_positioning"]  * 0.15 +
        factors["delta_sweet"]     * 0.10 +
        factors["short_dte"]       * 0.10 +
        factors["vol_regime"]      * 0.10 +
        factors["dealer_position"] * 0.10 +
        factors["liquidity"]       * 0.10
    )
    return max(0.0, min(orm, 1.0)), factors


def _enrich_moonshots_with_orm(
    candidates: List[Dict[str, Any]],
    top_n: int = 10,
) -> List[Dict[str, Any]]:
    """
    Enrich moonshot candidates with the Call Options Return Multiplier.

    Blends the existing score (momentum/interval persistence) with ORM
    to surface stocks with the highest expected CALL OPTIONS return.

    Blend: final_score = base_score × 0.55 + ORM × 0.45

    This ensures momentum/conviction still matters (don't buy calls on a
    stock with no catalyst), but among equally strong momentum names,
    the one with better gamma leverage, cheaper IV, institutional call
    building, and tighter spreads ranks higher.
    """
    if not candidates:
        return candidates

    try:
        gex, iv_data, oi, flow_data, dp = _load_uw_options_data()
        has_uw = any(d for d in [gex, iv_data, oi, flow_data, dp])
    except Exception as e:
        logger.warning(f"  ⚠️ Call-ORM: Failed to load UW options data: {e}")
        return candidates

    if not has_uw:
        logger.info("  ℹ️ Call-ORM: No UW options data available — "
                     "using base score only for ranking")
        return candidates

    logger.info("  🎯 Computing CALL OPTIONS RETURN MULTIPLIER...")
    orm_count = 0
    orm_scores = []
    enrich_count = min(max(top_n * 3, 30), len(candidates))

    for c in candidates[:enrich_count]:
        sym = c["symbol"]
        stock_px = c.get("price", 0)
        orm, factors = _compute_call_options_return_multiplier(
            sym, gex, iv_data, oi, flow_data, dp,
            stock_price=stock_px,
        )
        c["_orm_score"] = orm
        c["_orm_factors"] = factors
        orm_count += 1
        orm_scores.append(orm)

        # Blend: final_score = base × 0.55 + ORM × 0.45
        base_score = c.get("score", 0)
        c["_base_score"] = base_score
        final = base_score * 0.55 + orm * 0.45
        c["score"] = max(0.0, min(final, 1.0))

    # Re-sort by final blended score
    candidates.sort(key=lambda x: x["score"], reverse=True)

    if orm_scores:
        avg_orm = sum(orm_scores) / len(orm_scores)
        max_orm = max(orm_scores)
        min_orm = min(orm_scores)
        logger.info(
            f"  ✅ Call-ORM applied to {orm_count} candidates "
            f"(avg={avg_orm:.3f}, range={min_orm:.3f}–{max_orm:.3f})"
        )

    # Log final top picks with ORM breakdown
    logger.info(f"  📊 FINAL Top {min(top_n, len(candidates))} "
                 f"(base × 0.55 + Call-ORM × 0.45):")
    for i, c in enumerate(candidates[:top_n], 1):
        base = c.get("_base_score", 0)
        orm = c.get("_orm_score", 0)
        fcts = c.get("_orm_factors", {})
        top_factors = sorted(fcts.items(), key=lambda x: x[1], reverse=True)[:3]
        factor_str = " ".join(f"{k[:3]}={v:.2f}" for k, v in top_factors)
        logger.info(
            f"    #{i:2d} {c['symbol']:6s} "
            f"final={c['score']:.3f} "
            f"(base={base:.3f} orm={orm:.3f}) "
            f"[{factor_str}]"
        )

    return candidates


def get_moonshot_universe() -> List[str]:
    """Get the Moonshot Engine's default symbol universe."""
    try:
        from moonshot.run_daily_scan import DEFAULT_SYMBOLS
        return DEFAULT_SYMBOLS
    except ImportError:
        # Fallback default symbols
        return [
            'NVDA', 'AAPL', 'TSLA', 'MSFT', 'META', 'AMZN', 'GOOG', 'AMD', 'AVGO', 'MU',
            'SMCI', 'COIN', 'MSTR', 'PLTR', 'HOOD',
            'GME', 'AMC', 'BBBY', 'RKLB', 'IONQ',
            'SOFI', 'RIVN', 'LCID', 'NIO', 'SNOW',
        ]
