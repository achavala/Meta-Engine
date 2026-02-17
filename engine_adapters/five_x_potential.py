"""
🔥 5x POTENTIAL DETECTOR — Catching High-Leverage Movers EARLY
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
  The existing Top 10 pipeline optimizes for win rate (Policy B v4: 80% WR).
  This module adds a SEPARATE track that surfaces stocks with ≥5x OPTIONS
  RETURN POTENTIAL — the volatile, sector-wave, high-beta names that the
  score-ceiling + regime-gate pipeline systematically filters out.

  This runs ALONGSIDE existing Top 10 — it does NOT replace or modify
  any existing picks, scoring, or regime gates.

ROOT CAUSE (Feb 9-13, 2026 analysis):
  - 73 stocks had ≥5x options potential (31 calls, 42 puts)
  - Trinity detected 56/73 (77%) with scores ≥0.80
  - Cross Analysis picked 27/73 (37%) — mega-caps crowded out volatile names
  - Final Recommendations picked 0/73 (0%) — regime gates + mega-cap bias

THE FIX — 5 Scoring Layers:
  1. VOLATILITY WEIGHT: Lower-priced, higher-IV stocks get 2-5x multiplier
     (RIVN at $15 → ~15x leverage vs NVDA at $800 → ~3x leverage)
  2. SECTOR WAVE DETECTION: When 3+ stocks in same sector have bullish
     signals, that's a sector wave — massive boost to all sector members
  3. MULTI-DAY PERSISTENCE: RIVN appeared 97 times Mon-Fri in Trinity —
     persistent signals = highest conviction → escalating boost
  4. UW FLOW DIRECTIONAL ALIGNMENT: Options flow confirms thesis direction
     (call premium dominant for calls, put premium for puts)
  5. SIGNAL QUALITY: "Heavy call buying / +GEX" and dark pool signals
     are the highest-conviction institutional indicators

OUTPUT:
  Separate "🔥 5x POTENTIAL" section with:
  - Top 25 CALL potential (sorted by 5x_score)
  - Top 25 PUT potential (sorted by 5x_score)
  - Regime warnings (not blocks) for risk management
"""

import json
import logging
import re
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple, Set

logger = logging.getLogger(__name__)

TRADENOVA_PATH = str(Path.home() / "TradeNova")
TN_DATA = Path(TRADENOVA_PATH) / "data"

# ═══════════════════════════════════════════════════════════════════════════
# SECTOR MAP + STATIC UNIVERSE — Build from PutsEngine config
# ═══════════════════════════════════════════════════════════════════════════
_SECTOR_MAP: Dict[str, str] = {}
_STATIC_UNIVERSE: Set[str] = set()  # 104-ticker static universe gate
try:
    _pe_path = str(Path.home() / "PutsEngine")
    if _pe_path not in sys.path:
        sys.path.insert(0, _pe_path)
    from putsengine.config import EngineConfig
    for _sector_name, _tickers in EngineConfig.UNIVERSE_SECTORS.items():
        for _t in _tickers:
            _SECTOR_MAP[_t] = _sector_name
    _STATIC_UNIVERSE = set(EngineConfig.get_all_tickers())
    logger.debug(f"5x Potential: universe={len(_STATIC_UNIVERSE)} tickers")
except (ImportError, AttributeError):
    pass

# ═══════════════════════════════════════════════════════════════════════════
# PRICE-BASED VOLATILITY WEIGHT
# ═══════════════════════════════════════════════════════════════════════════
# Options leverage is INVERSELY related to stock price.
# A $15 stock that moves 10% → call could pay 15x
# A $500 stock that moves 10% → call pays ~3x
# This weight corrects the mega-cap bias.

def _price_volatility_weight(price: float) -> float:
    """
    Returns a multiplier [1.0, 5.0] based on stock price.
    Lower prices get higher weight (more options leverage).
    """
    if price <= 0:
        return 2.0    # Unknown price: assume mid-range (NOT mega-cap default)
    if price <= 5:
        return 4.0    # Micro-caps: extreme leverage but risky
    if price <= 15:
        return 3.5    # RIVN ($14.92), CIFR ($14.70): sweet spot
    if price <= 30:
        return 3.0    # UPST ($38.40), HIMS ($32.95)
    if price <= 50:
        return 2.5    # AFRM ($57.15)
    if price <= 100:
        return 2.0    # HOOD ($85.15), ROKU ($86.00)
    if price <= 200:
        return 1.5    # NET ($173.22), COIN ($161.38)
    if price <= 400:
        return 1.2    # SHOP ($112.11), AMAT ($322.52)
    return 1.0         # Mega-caps: UNH, NVDA, etc.


# ═══════════════════════════════════════════════════════════════════════════
# SECTOR WAVE DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def _detect_sector_waves(candidates: List[Dict]) -> Dict[str, Dict]:
    """
    Detect sector waves: when 3+ stocks in same sector have strong signals.
    Returns: {sector_name: {count, symbols, wave_strength}}
    """
    sector_counts = defaultdict(list)
    for c in candidates:
        sym = c.get("symbol", "")
        sector = _SECTOR_MAP.get(sym, c.get("sector", ""))
        if sector:
            sector_counts[sector].append(sym)
    
    waves = {}
    for sector, syms in sector_counts.items():
        if len(syms) >= 3:
            wave_strength = min(len(syms) / 5.0, 1.0)  # 5+ = maximum wave
            waves[sector] = {
                "count": len(syms),
                "symbols": syms,
                "wave_strength": wave_strength,
                "boost": 0.15 + (0.15 * wave_strength),  # 0.15 for 3, up to 0.30 for 5+
            }
    
    return waves


# ═══════════════════════════════════════════════════════════════════════════
# MULTI-DAY PERSISTENCE TRACKING
# ═══════════════════════════════════════════════════════════════════════════

def _load_persistence_data() -> Dict[str, int]:
    """
    Load multi-day persistence from trinity_interval_scans.json.
    Returns: {symbol: num_days_appeared}
    """
    persistence = defaultdict(set)
    try:
        with open(TN_DATA / "trinity_interval_scans.json") as f:
            tri = json.load(f)
        
        for d in sorted(tri.keys()):
            try:
                day_data = tri[d]
                scans = day_data.get("scans", [])
                for s in scans:
                    for eng in ["moonshot", "catalyst", "coiled_spring", "top_10"]:
                        for pick in s.get(eng, []):
                            sym = pick.get("symbol", "")
                            if sym:
                                persistence[sym].add(d)
            except (TypeError, AttributeError):
                continue
    except Exception as e:
        logger.warning(f"5x Potential: Failed to load persistence data: {e}")
    
    return {sym: len(days) for sym, days in persistence.items()}


def _persistence_boost(days: int) -> float:
    """Boost for multi-day signal persistence."""
    if days >= 5:
        return 0.25  # All-week persistence (highest conviction)
    if days >= 4:
        return 0.20
    if days >= 3:
        return 0.15
    if days >= 2:
        return 0.10
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# UW FLOW DIRECTIONAL ALIGNMENT
# ═══════════════════════════════════════════════════════════════════════════

def _load_uw_flow() -> Dict[str, Dict]:
    """
    Load UW options flow data.
    Returns: {symbol: {call_pct, put_pct, total_premium, num_trades}}
    """
    flow_summary = {}
    try:
        with open(TN_DATA / "uw_flow_cache.json") as f:
            raw = json.load(f)
        
        flow_data = raw.get("flow_data", raw) if isinstance(raw, dict) else raw
        if not isinstance(flow_data, dict):
            return {}
        
        for sym, trades in flow_data.items():
            if not isinstance(trades, list):
                continue
            call_prem = sum(t.get("premium", 0) for t in trades 
                          if isinstance(t, dict) and t.get("put_call") == "C")
            put_prem = sum(t.get("premium", 0) for t in trades 
                         if isinstance(t, dict) and t.get("put_call") == "P")
            total = call_prem + put_prem
            if total > 0:
                flow_summary[sym] = {
                    "call_pct": round(call_prem / total, 3),
                    "put_pct": round(put_prem / total, 3),
                    "total_premium": total,
                    "num_trades": len(trades),
                }
    except Exception as e:
        logger.warning(f"5x Potential: Failed to load UW flow: {e}")
    
    return flow_summary


# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL QUALITY ASSESSMENT
# ═══════════════════════════════════════════════════════════════════════════

PREMIUM_CALL_SIGNALS = {
    "heavy call buying", "positive gex", "call buying", "neg_gex_explosive",
    "iv_inverted", "dark_pool_massive", "institutional accumulation",
    "gamma squeeze", "short squeeze",
}

PREMIUM_PUT_SIGNALS = {
    "put_buying_at_ask", "call_selling_at_bid", "multi_day_weakness",
    "flat_price_rising_volume", "gap_down_no_recovery", "dark_pool_violence",
    "repeated_sell_blocks", "distribution",
}

def _signal_quality_score(signals: list, catalysts: list, direction: str) -> float:
    """
    Score signal quality for a specific direction (CALL or PUT).
    Premium signals get higher scores.
    """
    sig_set = set()
    for s in (signals or []):
        sig_set.add(str(s).lower())
    for c in (catalysts or []):
        sig_set.add(str(c).lower())
    
    # Join all for keyword matching
    sig_str = " ".join(sig_set)
    
    premium_hits = 0
    if direction == "CALL":
        for ps in PREMIUM_CALL_SIGNALS:
            if ps in sig_str:
                premium_hits += 1
    else:  # PUT
        for ps in PREMIUM_PUT_SIGNALS:
            if ps in sig_str:
                premium_hits += 1
    
    # Total signal density
    total_signals = len(signals or [])
    density = min(total_signals / 8.0, 1.0)
    
    # Combined quality
    quality = min(premium_hits * 0.20, 0.60) + density * 0.40
    return min(quality, 1.0)


# ═══════════════════════════════════════════════════════════════════════════
# MARKET REGIME (informational only — NOT used for blocking)
# ═══════════════════════════════════════════════════════════════════════════

def _get_market_regime() -> Dict[str, Any]:
    """Get current market regime for informational warnings."""
    try:
        regime_path = TN_DATA / "market_regime_cache.json"
        if regime_path.exists():
            with open(regime_path) as f:
                data = json.load(f)
            return {
                "regime": data.get("regime_label", "UNKNOWN"),
                "score": data.get("regime_score", 0),
            }
    except Exception:
        pass
    
    # Fallback: use market direction from PutsEngine
    try:
        md_path = TN_DATA / "market_direction.json"
        if md_path.exists():
            with open(md_path) as f:
                data = json.load(f)
            direction = data.get("direction", "")
            if "bull" in direction.lower():
                return {"regime": "LEAN_BULL", "score": 0.2}
            elif "bear" in direction.lower():
                return {"regime": "LEAN_BEAR", "score": -0.2}
    except Exception:
        pass
    
    return {"regime": "UNKNOWN", "score": 0}


# ═══════════════════════════════════════════════════════════════════════════
# SAFE PRICE PARSER (used by logging + report formatting)
# ═══════════════════════════════════════════════════════════════════════════

def _safe_price(val) -> float:
    """Parse price for logging (handles str/float/None)."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        nums = re.findall(r'[\d.]+', val)
        if nums:
            try:
                return float(nums[0])
            except ValueError:
                pass
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# MAIN: COMPUTE 5x POTENTIAL
# ═══════════════════════════════════════════════════════════════════════════

def compute_5x_potential(
    moonshot_candidates: List[Dict] = None,
    puts_candidates: List[Dict] = None,
    top_n: int = 25,  # Validated: 56/65 (86%) 5x mover coverage
) -> Dict[str, Any]:
    """
    Compute 5x options potential for ALL candidates from both engines.
    
    This runs ALONGSIDE existing pipeline — does NOT modify existing picks.
    
    Returns:
        {
            "call_potential": [top_n sorted by 5x_score],
            "put_potential": [top_n sorted by 5x_score],
            "sector_waves": {detected sector waves},
            "regime_warning": str or None,
            "stats": {coverage stats},
        }
    """
    logger.info("🔥 Computing 5x OPTIONS POTENTIAL (separate track)...")
    
    # Load enrichment data
    persistence = _load_persistence_data()
    uw_flow = _load_uw_flow()
    regime = _get_market_regime()
    
    # Load forecast for catalyst data
    forecasts = {}
    try:
        with open(TN_DATA / "tomorrows_forecast.json") as f:
            fc_data = json.load(f)
        forecasts = {fc["symbol"]: fc for fc in fc_data.get("forecasts", []) if fc.get("symbol")}
    except Exception:
        pass
    
    # Also load from Trinity scans for broader coverage
    trinity_candidates = _load_trinity_candidates()
    
    # Build combined candidate pools — cast the WIDEST possible net
    # The whole point of 5x module is to catch movers the adapter gates filter out
    all_call_candidates = {}  # sym -> candidate
    all_put_candidates = {}
    
    # 1. Add moonshot candidates (CALL direction)
    for c in (moonshot_candidates or []):
        sym = c.get("symbol", "")
        if sym:
            all_call_candidates[sym] = {**c, "_source": "moonshot_adapter"}
    
    # 2. Add puts candidates (PUT direction)
    for c in (puts_candidates or []):
        sym = c.get("symbol", "")
        if sym:
            all_put_candidates[sym] = {**c, "_source": "puts_adapter"}
    
    # 3. Add Trinity candidates that weren't in adapter output
    for tc in trinity_candidates:
        sym = tc.get("symbol", "")
        if sym and sym not in all_call_candidates:
            all_call_candidates[sym] = {**tc, "_source": "trinity_scanner"}
        if sym and sym not in all_put_candidates:
            all_put_candidates[sym] = {**tc, "_source": "trinity_scanner"}
    
    # 4. CRITICAL: Add ALL symbols from persistence data (multi-day signals)
    # These appeared in Trinity scans over multiple days — highest conviction
    for sym, days in persistence.items():
        if days >= 2 and sym not in all_call_candidates:
            all_call_candidates[sym] = {
                "symbol": sym, "score": min(days / 10.0, 0.8),
                "price": 0, "signals": [], "sector": _SECTOR_MAP.get(sym, ""),
                "_source": f"persistence_{days}d",
            }
        if days >= 2 and sym not in all_put_candidates:
            all_put_candidates[sym] = {
                "symbol": sym, "score": min(days / 10.0, 0.8),
                "price": 0, "signals": [], "sector": _SECTOR_MAP.get(sym, ""),
                "_source": f"persistence_{days}d",
            }
    
    # 5. Add ALL symbols from UW flow (options market is already watching them)
    for sym, flow in uw_flow.items():
        if sym not in all_call_candidates:
            all_call_candidates[sym] = {
                "symbol": sym, "score": 0.3,
                "price": 0, "signals": [], "sector": _SECTOR_MAP.get(sym, ""),
                "_source": "uw_flow",
            }
        if sym not in all_put_candidates:
            all_put_candidates[sym] = {
                "symbol": sym, "score": 0.3,
                "price": 0, "signals": [], "sector": _SECTOR_MAP.get(sym, ""),
                "_source": "uw_flow",
            }
    
    # 6. Add ALL forecast symbols (MWS 7-layer analysis flagged them)
    for sym, fc in forecasts.items():
        if sym not in all_call_candidates:
            bull_prob = fc.get("bullish_probability", 0)
            if isinstance(bull_prob, str):
                try:
                    bull_prob = float(bull_prob.strip("%")) / 100
                except (ValueError, TypeError):
                    bull_prob = 0
            all_call_candidates[sym] = {
                "symbol": sym, "score": min(bull_prob, 1.0) if bull_prob else 0.4,
                "price": 0, "signals": [], "sector": _SECTOR_MAP.get(sym, ""),
                "catalysts": fc.get("catalysts", []),
                "_source": "forecast",
            }
        if sym not in all_put_candidates:
            all_put_candidates[sym] = {
                "symbol": sym, "score": 0.4,
                "price": 0, "signals": [], "sector": _SECTOR_MAP.get(sym, ""),
                "catalysts": fc.get("catalysts", []),
                "_source": "forecast",
            }
    
    # ══════════════════════════════════════════════════════════════════════
    # UNIVERSE GATE — ONLY allow tickers in 104-ticker static universe
    # This ensures the 5x module NEVER surfaces tickers outside the heatmap.
    # ══════════════════════════════════════════════════════════════════════
    if _STATIC_UNIVERSE:
        call_before = len(all_call_candidates)
        all_call_candidates = {
            s: c for s, c in all_call_candidates.items() if s in _STATIC_UNIVERSE
        }
        put_before = len(all_put_candidates)
        all_put_candidates = {
            s: c for s, c in all_put_candidates.items() if s in _STATIC_UNIVERSE
        }
        call_removed = call_before - len(all_call_candidates)
        put_removed = put_before - len(all_put_candidates)
        if call_removed or put_removed:
            logger.info(
                f"  🚫 Universe filter (5x): {call_removed} calls + "
                f"{put_removed} puts removed "
                f"(not in {len(_STATIC_UNIVERSE)}-ticker static universe)"
            )

    logger.info(f"  📊 5x Pool: {len(all_call_candidates)} call candidates, "
                f"{len(all_put_candidates)} put candidates (universe-gated)")
    
    # Detect sector waves across ALL candidates
    all_candidates = list(all_call_candidates.values()) + list(all_put_candidates.values())
    sector_waves = _detect_sector_waves(all_candidates)
    
    if sector_waves:
        logger.info(f"  🌊 Sector waves detected:")
        for sector, wave in sorted(sector_waves.items(), key=lambda x: -x[1]["count"]):
            logger.info(f"    {sector}: {wave['count']} stocks ({', '.join(wave['symbols'][:5])})")
    
    # Score CALL candidates
    call_scored = []
    for sym, c in all_call_candidates.items():
        score_5x = _compute_5x_score(
            sym, c, "CALL", persistence, uw_flow, forecasts, sector_waves, regime
        )
        c["_5x_score"] = round(score_5x, 4)
        c["_5x_direction"] = "CALL"
        c["_5x_persistence_days"] = persistence.get(sym, 0)
        c["_5x_sector"] = _SECTOR_MAP.get(sym, c.get("sector", ""))
        
        # Flow alignment info
        flow = uw_flow.get(sym, {})
        c["_5x_call_pct"] = flow.get("call_pct", 0.5)
        c["_5x_total_premium"] = flow.get("total_premium", 0)
        
        if score_5x >= 0.30:  # Minimum threshold
            call_scored.append(c)
    
    # Score PUT candidates
    put_scored = []
    for sym, c in all_put_candidates.items():
        score_5x = _compute_5x_score(
            sym, c, "PUT", persistence, uw_flow, forecasts, sector_waves, regime
        )
        c["_5x_score"] = round(score_5x, 4)
        c["_5x_direction"] = "PUT"
        c["_5x_persistence_days"] = persistence.get(sym, 0)
        c["_5x_sector"] = _SECTOR_MAP.get(sym, c.get("sector", ""))
        
        flow = uw_flow.get(sym, {})
        c["_5x_put_pct"] = flow.get("put_pct", 0.5)
        c["_5x_total_premium"] = flow.get("total_premium", 0)
        
        if score_5x >= 0.30:
            put_scored.append(c)
    
    # Sort by 5x_score and take top N
    call_scored.sort(key=lambda x: x["_5x_score"], reverse=True)
    put_scored.sort(key=lambda x: x["_5x_score"], reverse=True)
    
    call_top = call_scored[:top_n]
    put_top = put_scored[:top_n]
    
    # Add regime warnings (NOT blocks)
    regime_warning = None
    regime_label = regime.get("regime", "UNKNOWN")
    if regime_label in ("STRONG_BEAR", "LEAN_BEAR"):
        regime_warning = (
            f"⚠️ REGIME: {regime_label} — CALL 5x plays carry elevated risk. "
            f"Prefer same-day plays or tight stops. PUT 5x plays are regime-aligned."
        )
    elif regime_label in ("STRONG_BULL", "LEAN_BULL"):
        regime_warning = (
            f"⚠️ REGIME: {regime_label} — PUT 5x plays carry elevated risk. "
            f"Prefer same-day plays or tight stops. CALL 5x plays are regime-aligned."
        )
    
    # Log results
    logger.info(f"\n  🔥 5x CALL POTENTIAL: {len(call_top)} picks")
    for i, c in enumerate(call_top, 1):
        sym = c.get("symbol", "?")
        s5x = c["_5x_score"]
        price = _safe_price(c.get("price", c.get("current_price", 0)) or 0)
        sector = c.get("_5x_sector", "")
        persist = c.get("_5x_persistence_days", 0)
        cpct = c.get("_5x_call_pct", 0.5)
        logger.info(f"    #{i:2d} {sym:7s} 5x={s5x:.3f} ${price:>8.2f} {sector:16s} "
                    f"P={persist}d Flow={cpct:.0%}C")
    
    logger.info(f"\n  🔥 5x PUT POTENTIAL: {len(put_top)} picks")
    for i, c in enumerate(put_top, 1):
        sym = c.get("symbol", "?")
        s5x = c["_5x_score"]
        price = _safe_price(c.get("price", c.get("current_price", 0)) or 0)
        sector = c.get("_5x_sector", "")
        persist = c.get("_5x_persistence_days", 0)
        ppct = c.get("_5x_put_pct", 0.5)
        logger.info(f"    #{i:2d} {sym:7s} 5x={s5x:.3f} ${price:>8.2f} {sector:16s} "
                    f"P={persist}d Flow={ppct:.0%}P")
    
    if regime_warning:
        logger.info(f"\n  {regime_warning}")
    
    # ── Build Sector Wave Watch List ────────────────────────────────
    # For each detected sector wave, list ALL stocks with their 5x scores.
    # This ensures sector-wave movers aren't lost to the flat top-N cut.
    sector_wave_watchlist = {}
    if sector_waves:
        # Build a sym→score map for quick lookup
        all_scored_map = {}
        for c in call_scored:
            sym = c.get("symbol", "")
            if sym:
                all_scored_map[sym] = {
                    "call_5x": c.get("_5x_score", 0),
                    "persist": c.get("_5x_persistence_days", 0),
                    "sector": c.get("_5x_sector", ""),
                }
        for c in put_scored:
            sym = c.get("symbol", "")
            if sym:
                existing = all_scored_map.get(sym, {})
                existing["put_5x"] = c.get("_5x_score", 0)
                if not existing.get("persist"):
                    existing["persist"] = c.get("_5x_persistence_days", 0)
                if not existing.get("sector"):
                    existing["sector"] = c.get("_5x_sector", "")
                all_scored_map[sym] = existing
        
        for sector, wave in sorted(sector_waves.items(), key=lambda x: -x[1]["count"]):
            wave_stocks = []
            for sym in wave["symbols"]:
                info = all_scored_map.get(sym, {})
                wave_stocks.append({
                    "symbol": sym,
                    "call_5x": round(info.get("call_5x", 0), 3),
                    "put_5x": round(info.get("put_5x", 0), 3),
                    "persist": info.get("persist", 0),
                })
            # Deduplicate (sector_waves may have duplicates from call+put pools)
            seen = set()
            unique_stocks = []
            for s in wave_stocks:
                if s["symbol"] not in seen:
                    seen.add(s["symbol"])
                    unique_stocks.append(s)
            # Sort by max 5x score
            unique_stocks.sort(
                key=lambda x: max(x["call_5x"], x["put_5x"]), reverse=True
            )
            sector_wave_watchlist[sector] = {
                "count": wave["count"],
                "boost": wave["boost"],
                "stocks": unique_stocks,
            }
        
        logger.info(f"\n  📋 Sector Wave Watch List: {len(sector_wave_watchlist)} sectors")
        for sector, wl in sector_wave_watchlist.items():
            unique_count = len(wl["stocks"])
            logger.info(f"    🌊 {sector}: {unique_count} stocks (boost +{wl['boost']:.2f})")
    
    result = {
        "call_potential": _serialize_picks(call_top),
        "put_potential": _serialize_picks(put_top),
        "sector_waves": {k: {**v, "symbols": v["symbols"][:10]} for k, v in sector_waves.items()},
        "sector_wave_watchlist": sector_wave_watchlist,
        "regime_warning": regime_warning,
        "regime": regime,
        "stats": {
            "call_pool_size": len(all_call_candidates),
            "put_pool_size": len(all_put_candidates),
            "call_above_threshold": len(call_scored),
            "put_above_threshold": len(put_scored),
            "sector_waves_count": len(sector_waves),
            "persistence_symbols": sum(1 for v in persistence.values() if v >= 3),
        },
        "generated_at": datetime.now().isoformat(),
    }
    
    return result


def _compute_5x_score(
    sym: str,
    candidate: Dict,
    direction: str,  # "CALL" or "PUT"
    persistence: Dict[str, int],
    uw_flow: Dict[str, Dict],
    forecasts: Dict[str, Dict],
    sector_waves: Dict[str, Dict],
    regime: Dict[str, Any],
) -> float:
    """
    Compute composite 5x potential score.
    
    Formula:
      5x_score = base_quality × volatility_weight × (1 + boosts)
      
    Where:
      base_quality = 0.40 × base_score + 0.30 × signal_quality + 0.30 × flow_alignment
      boosts = sector_wave_boost + persistence_boost + forecast_boost
    """
    def _parse_price(val) -> float:
        """Safely parse price from various formats (float, str, range)."""
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            # Handle "$1.56 - $1.90" → take first number
            nums = re.findall(r'[\d.]+', val)
            if nums:
                try:
                    return float(nums[0])
                except ValueError:
                    pass
        return 0.0
    
    price = _parse_price(candidate.get("price") or candidate.get("current_price") or candidate.get("entry_price") or 0)
    base_score_raw = candidate.get("score") or candidate.get("_base_score") or 0
    base_score = float(base_score_raw) if isinstance(base_score_raw, (int, float)) else 0.0
    signals = candidate.get("signals", [])
    if not isinstance(signals, list):
        signals = []
    
    # Get catalyst data from forecast
    fc = forecasts.get(sym, {})
    catalysts = fc.get("catalysts", [])
    if not isinstance(catalysts, list):
        catalysts = [str(catalysts)] if catalysts else []
    
    # ── Layer 1: BASE QUALITY ──────────────────────────────────────
    # Signal quality (premium signals for the specific direction)
    sig_quality = _signal_quality_score(signals, catalysts, direction)
    
    # Flow alignment (does UW flow confirm the direction?)
    flow = uw_flow.get(sym, {})
    flow_alignment = 0.0
    if flow:
        if direction == "CALL":
            call_pct = flow.get("call_pct", 0.5)
            if call_pct > 0.70:
                flow_alignment = 1.0   # Strong call dominance
            elif call_pct > 0.55:
                flow_alignment = 0.6   # Moderate call lean
            elif call_pct < 0.40:
                flow_alignment = -0.3  # Contradicting flow (bearish for calls)
            else:
                flow_alignment = 0.2   # Neutral
        else:  # PUT
            put_pct = flow.get("put_pct", 0.5)
            if put_pct > 0.70:
                flow_alignment = 1.0
            elif put_pct > 0.55:
                flow_alignment = 0.6
            elif put_pct < 0.40:
                flow_alignment = -0.3
            else:
                flow_alignment = 0.2
    
    # ── Layer 1: BASE QUALITY (additive, not multiplicative) ──────
    # Changed from multiplicative to additive to prevent zero-signal candidates
    # from scoring near zero despite having strong flow/persistence/sector data.
    base_quality = (
        0.25 * min(base_score, 1.0)
        + 0.25 * sig_quality
        + 0.25 * max(flow_alignment, 0)  # Don't let negative flow kill the score
    )
    
    # ── Layer 2: VOLATILITY WEIGHT ─────────────────────────────────
    vol_weight = _price_volatility_weight(price)
    
    # ── Layer 3: INSTITUTIONAL INTEREST SIGNALS ────────────────────
    # These are treated as ADDITIVE components (not just multiplier boosts)
    # because persistence + sector wave + flow = institutional conviction
    institutional_score = 0.0
    
    # Persistence score (multi-day signal = highest-conviction early indicator)
    days = persistence.get(sym, 0)
    if days >= 5:
        institutional_score += 0.30  # All-week persistence: very strong
    elif days >= 4:
        institutional_score += 0.25
    elif days >= 3:
        institutional_score += 0.20
    elif days >= 2:
        institutional_score += 0.15
    elif days >= 1:
        institutional_score += 0.08
    
    # Sector wave score (cluster momentum)
    sector = _SECTOR_MAP.get(sym, candidate.get("sector", ""))
    if sector and sector in sector_waves:
        wave = sector_waves[sector]
        institutional_score += wave["boost"]  # 0.15 to 0.30
    
    # Forecast alignment
    if fc:
        bull_prob = fc.get("bullish_probability", 0)
        bear_prob = fc.get("bearish_probability", 0)
        if isinstance(bull_prob, str):
            try:
                bull_prob = float(bull_prob.strip("%")) / 100
            except (ValueError, TypeError):
                bull_prob = 0
        if isinstance(bear_prob, str):
            try:
                bear_prob = float(bear_prob.strip("%")) / 100
            except (ValueError, TypeError):
                bear_prob = 0
        
        if direction == "CALL" and bull_prob > 0.55:
            institutional_score += 0.10
        elif direction == "PUT" and bear_prob > 0.55:
            institutional_score += 0.10
    
    # Flow contradiction penalty (reduce, don't kill)
    if flow_alignment < 0:
        institutional_score += flow_alignment * 0.3  # Small reduction
    
    # ── Final Score ────────────────────────────────────────────────
    # ADDITIVE formula: base_quality + institutional_score, then scale by vol_weight
    # This ensures persistence + sector wave alone can produce actionable scores
    combined = base_quality + institutional_score  # Range [0, ~1.5]
    raw = combined * vol_weight  # vol_weight in [1.0, 4.0]
    
    # Normalize to [0, 1] range
    # max theoretical: 1.5 × 4.0 = 6.0
    normalized = min(raw / 3.0, 1.0)
    
    return normalized


def _load_trinity_candidates() -> List[Dict]:
    """
    Load the latest Trinity scan candidates to expand the pool
    beyond what the adapters already selected.
    """
    candidates = []
    try:
        with open(TN_DATA / "trinity_interval_scans.json") as f:
            tri = json.load(f)
        
        # Get the most recent day's data
        latest_day = max(tri.keys()) if tri else None
        if not latest_day:
            return []
        
        day_data = tri[latest_day]
        scans = day_data.get("scans", [])
        
        # Get the most recent scan
        if not scans:
            return []
        
        latest_scan = scans[-1]
        seen = set()
        
        for eng in ["moonshot", "catalyst", "coiled_spring"]:
            for pick in latest_scan.get(eng, []):
                sym = pick.get("symbol", "")
                if sym and sym not in seen:
                    seen.add(sym)
                    candidates.append({
                        "symbol": sym,
                        "score": pick.get("score", 0),
                        "price": pick.get("entry_price", 0) or pick.get("current_price", 0),
                        "signals": pick.get("signals", []),
                        "engine": f"Trinity ({eng})",
                        "sector": "",
                        "action": pick.get("action", ""),
                        "win_probability": pick.get("win_probability", 0),
                    })
    except Exception as e:
        logger.warning(f"5x Potential: Failed to load Trinity candidates: {e}")
    
    return candidates


def _serialize_picks(picks: List[Dict]) -> List[Dict]:
    """Serialize picks for JSON output (remove non-serializable fields)."""
    result = []
    for p in picks:
        result.append({
            "symbol": p.get("symbol", ""),
            "score": p.get("score", 0),
            "price": p.get("price", 0) or p.get("current_price", 0) or p.get("entry_price", 0),
            "signals": p.get("signals", []),
            "engine": p.get("engine", ""),
            "sector": p.get("_5x_sector", ""),
            "_5x_score": p.get("_5x_score", 0),
            "_5x_direction": p.get("_5x_direction", ""),
            "_5x_persistence_days": p.get("_5x_persistence_days", 0),
            "_5x_call_pct": p.get("_5x_call_pct", 0),
            "_5x_put_pct": p.get("_5x_put_pct", 0),
            "_5x_total_premium": p.get("_5x_total_premium", 0),
            "_source": p.get("_source", ""),
        })
    return result


# ═══════════════════════════════════════════════════════════════════════════
# FORMAT: Human-readable report for email/Telegram
# ═══════════════════════════════════════════════════════════════════════════

def format_5x_potential_report(result: Dict[str, Any]) -> str:
    """Format 5x potential results for email/Telegram."""
    lines = []
    lines.append("🔥 5x OPTIONS POTENTIAL")
    lines.append("━" * 40)
    
    regime_warning = result.get("regime_warning")
    if regime_warning:
        lines.append(f"\n{regime_warning}\n")
    
    # Sector waves
    waves = result.get("sector_waves", {})
    if waves:
        for sector, wave in sorted(waves.items(), key=lambda x: -x[1]["count"]):
            syms_str = ", ".join(wave["symbols"][:5])
            lines.append(f"⚡ SECTOR WAVE: {sector} ({wave['count']} stocks: {syms_str})")
        lines.append("")
    
    def _fmt_price(val) -> str:
        """Format price safely (handles str/float/None)."""
        if isinstance(val, (int, float)) and val > 0:
            return f"${val:>8.2f}"
        if isinstance(val, str):
            nums = re.findall(r'[\d.]+', val)
            if nums:
                try:
                    return f"${float(nums[0]):>8.2f}"
                except ValueError:
                    pass
        return "    N/A "
    
    # Call potential
    calls = result.get("call_potential", [])
    if calls:
        lines.append(f"📈 TOP {len(calls)} CALL POTENTIAL:")
        for i, c in enumerate(calls, 1):
            sym = c.get("symbol", "?")
            s5x = c.get("_5x_score", 0)
            price_str = _fmt_price(c.get("price", 0))
            sector = c.get("sector", "")
            persist = c.get("_5x_persistence_days", 0)
            cpct = c.get("_5x_call_pct", 0.5)
            
            flow_tag = ""
            if isinstance(cpct, (int, float)):
                if cpct > 0.70:
                    flow_tag = "🟢 Heavy Calls"
                elif cpct > 0.55:
                    flow_tag = "🟡 Call Lean"
                elif cpct < 0.40:
                    flow_tag = "🔴 Put Heavy"
            
            persist_tag = f"[{persist}d]" if persist >= 3 else ""
            
            lines.append(
                f"  #{i:2d} {sym:7s} 5x={s5x:.2f} {price_str} "
                f"{sector:14s} {persist_tag:5s} {flow_tag}"
            )
        lines.append("")
    
    # Put potential
    puts = result.get("put_potential", [])
    if puts:
        lines.append(f"📉 TOP {len(puts)} PUT POTENTIAL:")
        for i, c in enumerate(puts, 1):
            sym = c.get("symbol", "?")
            s5x = c.get("_5x_score", 0)
            price_str = _fmt_price(c.get("price", 0))
            sector = c.get("sector", "")
            persist = c.get("_5x_persistence_days", 0)
            ppct = c.get("_5x_put_pct", 0.5)
            
            flow_tag = ""
            if isinstance(ppct, (int, float)):
                if ppct > 0.70:
                    flow_tag = "🟢 Heavy Puts"
                elif ppct > 0.55:
                    flow_tag = "🟡 Put Lean"
                elif ppct < 0.40:
                    flow_tag = "🔴 Call Heavy"
            
            persist_tag = f"[{persist}d]" if persist >= 3 else ""
            
            lines.append(
                f"  #{i:2d} {sym:7s} 5x={s5x:.2f} {price_str} "
                f"{sector:14s} {persist_tag:5s} {flow_tag}"
            )
        lines.append("")
    
    # Sector Wave Watch List
    watchlist = result.get("sector_wave_watchlist", {})
    if watchlist:
        lines.append("🌊 SECTOR WAVE WATCH LIST")
        lines.append("━" * 40)
        for sector, wl in sorted(watchlist.items(), key=lambda x: -x[1]["count"]):
            stocks = wl["stocks"]
            if not stocks:
                continue
            lines.append(f"\n  ⚡ {sector} ({len(stocks)} stocks, boost +{wl['boost']:.0%}):")
            for s in stocks[:8]:  # Show top 8 per sector
                sym = s["symbol"]
                cs = s.get("call_5x", 0)
                ps = s.get("put_5x", 0)
                persist = s.get("persist", 0)
                best = max(cs, ps)
                direction = "C" if cs >= ps else "P"
                persist_tag = f"[{persist}d]" if persist >= 2 else ""
                lines.append(
                    f"    {sym:7s} best={best:.2f}{direction} {persist_tag}"
                )
        lines.append("")
    
    # Stats
    stats = result.get("stats", {})
    lines.append(f"📊 Pool: {stats.get('call_pool_size', 0)} calls, "
                f"{stats.get('put_pool_size', 0)} puts | "
                f"Sector waves: {stats.get('sector_waves_count', 0)} | "
                f"Multi-day signals: {stats.get('persistence_symbols', 0)}")
    
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC ENTRY POINT (used by meta_engine.py)
# ═══════════════════════════════════════════════════════════════════════════

def get_five_x_potential_picks(
    moonshot_candidates: List[Dict] = None,
    puts_candidates: List[Dict] = None,
    polygon_api_key: str = None,
) -> Dict[str, Any]:
    """
    Public entry point called by meta_engine.py.
    Wraps compute_5x_potential with the standard interface.
    """
    return compute_5x_potential(
        moonshot_candidates=moonshot_candidates,
        puts_candidates=puts_candidates,
        top_n=25,
    )
