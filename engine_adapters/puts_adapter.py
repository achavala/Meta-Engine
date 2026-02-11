"""
PutsEngine Adapter
==================
Interface to get Top 10 PUT candidates from the PutsEngine system.
Imports PutsEngine modules directly without modifying the original codebase.
"""

import sys
import os
import asyncio
import json
import signal as _signal
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import logging

# Timeout for PutsEngine live scan (seconds).
# The scan of 361 tickers can take 25+ minutes via Polygon API.
# If it exceeds this, fall back to cached data so the pipeline continues.
# FEB 10 FIX: Reduced from 300s (5 min) to 30s.
# Rationale: PutsEngine's scheduler already scans all 361 tickers at 9:00 AM
# and saves fresh results to scheduled_scan_results.json by ~9:21 AM.
# At 9:35 AM, Meta Engine should quickly fall back to that cached data (only 14 min old)
# instead of doing a redundant 25-minute live scan.
# Similarly, the 2:45 PM market_pulse scan feeds the 3:15 PM Meta Engine run.
LIVE_SCAN_TIMEOUT_SEC = 30  # 30 seconds — then fall back to cached data

logger = logging.getLogger(__name__)

# Add PutsEngine to path
PUTSENGINE_PATH = str(Path.home() / "PutsEngine")
if PUTSENGINE_PATH not in sys.path:
    sys.path.insert(0, PUTSENGINE_PATH)

# Load PutsEngine's .env for API keys (needed for live scan)
_putsengine_env = Path(PUTSENGINE_PATH) / ".env"
if _putsengine_env.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_putsengine_env, override=False)  # Don't override existing vars
    except ImportError:
        pass


def get_top_puts(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Get the Top N PUT candidates from PutsEngine.
    
    Uses PutsEngine's internal scanning pipeline to find the best
    put candidates sorted by composite score.
    
    Args:
        top_n: Number of top picks to return (default 10)
        
    Returns:
        List of dicts with keys: symbol, score, price, signals, engine_type, etc.
    """
    try:
        from putsengine.config import EngineConfig, get_settings
        from putsengine.engine import PutsEngine
        
        logger.info(f"🔴 PutsEngine: Scanning {len(EngineConfig.get_all_tickers())} tickers...")
        
        settings = get_settings()
        engine = PutsEngine(settings)
        
        # Get all tickers from PutsEngine universe
        all_tickers = sorted(EngineConfig.get_all_tickers())
        
        results = []
        
        async def _scan_all():
            """Async scan of all tickers."""
            nonlocal results
            scanned = 0
            errors = 0
            
            for symbol in all_tickers:
                # FEB 10 FIX: Yield control to the event loop between tickers
                # so that asyncio.wait_for() can check the timeout.
                # Without this, the loop blocks and the timeout never fires.
                await asyncio.sleep(0)
                try:
                    candidate = await engine.run_single_symbol(symbol)
                    scanned += 1
                    
                    if candidate and candidate.composite_score > 0:
                        result = {
                            "symbol": symbol,
                            "score": candidate.composite_score,
                            "price": candidate.current_price,
                            "passed_gates": candidate.passed_all_gates,
                            "distribution_score": candidate.distribution_score,
                            "dealer_score": candidate.dealer_score,
                            "liquidity_score": candidate.liquidity_score,
                            "signals": [],
                            "block_reasons": [r.value for r in candidate.block_reasons] if candidate.block_reasons else [],
                            "engine": "PutsEngine",
                            "engine_type": "N/A",
                        }
                        
                        # Extract signals
                        if candidate.distribution and hasattr(candidate.distribution, 'signals'):
                            result["signals"] = list(candidate.distribution.signals.keys()) if isinstance(candidate.distribution.signals, dict) else candidate.distribution.signals
                        
                        # Get engine type
                        if candidate.acceleration:
                            et = getattr(candidate.acceleration, 'engine_type', 'N/A')
                            result["engine_type"] = et.value if hasattr(et, 'value') else str(et)
                        
                        results.append(result)
                        
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        logger.debug(f"  Skip {symbol}: {str(e)[:60]}")
            
            logger.info(f"  Scanned {scanned}/{len(all_tickers)}, errors: {errors}")
        
        # Run async scan with timeout
        async def _scan_with_timeout():
            """Run the scan with a hard timeout."""
            await asyncio.wait_for(_scan_all(), timeout=LIVE_SCAN_TIMEOUT_SEC)
        
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_scan_with_timeout())
        except asyncio.TimeoutError:
            logger.warning(
                f"  ⏱️ PutsEngine live scan timed out after "
                f"{LIVE_SCAN_TIMEOUT_SEC}s — using {len(results)} partial "
                f"results + cached fallback"
            )
            if len(results) < top_n:
                # Supplement partial results with cached data
                partial_syms = {r["symbol"] for r in results}
                cached = _fallback_from_cached_results(top_n)
                for c in cached:
                    if c["symbol"] not in partial_syms:
                        results.append(c)
                        partial_syms.add(c["symbol"])
        finally:
            loop.close()
        
        # Sort by composite score descending
        results.sort(key=lambda x: x["score"], reverse=True)
        
        top_picks = results[:top_n]
        
        # ──────────────────────────────────────────────────────────
        # CRITICAL FIX: If live scan returned 0 picks (all tickers
        # had composite_score <= 0), fall back to cached data.
        # This commonly happens on the PM scan when PutsEngine's
        # real-time analysis doesn't find passing candidates due to
        # different intraday conditions or API exhaustion.
        # ──────────────────────────────────────────────────────────
        if not top_picks:
            logger.warning(
                "  ⚠️ PutsEngine live scan returned 0 picks "
                "(all tickers scored ≤ 0) — falling back to cached data"
            )
            fallback_picks = _fallback_from_cached_results(top_n)
            if fallback_picks:
                _validate_picks(fallback_picks, "cached fallback (live scan empty)")
                return fallback_picks
            logger.warning("  ⚠️ Cached fallback also empty — returning empty list")
        
        logger.info(f"🔴 PutsEngine: Top {len(top_picks)} picks selected")
        for i, p in enumerate(top_picks, 1):
            logger.info(f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — ${p['price']:.2f}")
        
        # Data quality validation
        _validate_picks(top_picks, "live scan")
        
        return top_picks
        
    except Exception as e:
        logger.error(f"PutsEngine scan failed: {e}")
        fallback_picks = _fallback_from_cached_results(top_n)
        _validate_picks(fallback_picks, "cached fallback")
        return fallback_picks


def _fallback_from_cached_results(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Multi-tier fallback: Read from PutsEngine's cached data files.
    
    Tier 1: scheduled_scan_results.json (latest market_open scan)
    Tier 2: scan_history.json (most recent scan with actual picks)
    Tier 3: logs/convergence/latest_top9.json (convergence pipeline - richest data)
    Tier 4: pattern_scan_results.json (pattern-based scans)
    
    Used when the live scan fails (e.g., pydantic conflict, API rate limits).
    """
    all_candidates = []
    source_used = "none"
    data_age_days = -1  # Track freshness of the source file
    
    # ── Tier 1: scheduled_scan_results.json ──────────────────────────
    try:
        results_file = Path(PUTSENGINE_PATH) / "scheduled_scan_results.json"
        if results_file.exists():
            with open(results_file) as f:
                data = json.load(f)

            # Calculate data age from file modification time
            file_mtime = datetime.fromtimestamp(results_file.stat().st_mtime)
            data_age_days = (datetime.now() - file_mtime).days
            scan_ts = data.get("scan_time", data.get("timestamp", ""))
            data_source_label = f"scheduled_scan_results ({scan_ts or file_mtime.strftime('%Y-%m-%d %H:%M')})"

            for engine_key in ["gamma_drain", "distribution", "liquidity"]:
                for c in data.get(engine_key, []):
                    all_candidates.append({
                        "symbol": c.get("symbol", ""),
                        "score": c.get("score", c.get("composite_score", 0)),
                        "price": c.get("current_price", 0) or c.get("close", 0),
                        "passed_gates": True,
                        "distribution_score": c.get("distribution_score", 0),
                        "dealer_score": c.get("dealer_score", 0),
                        "liquidity_score": c.get("liquidity_score", 0),
                        "signals": c.get("signals", []),
                        "block_reasons": [],
                        "engine": f"PutsEngine ({engine_key})",
                        "engine_type": engine_key,
                        # Preserve pattern data for score enrichment
                        "pattern_boost": c.get("pattern_boost", 0),
                        "pattern_enhanced": c.get("pattern_enhanced", False),
                        "vol_ratio": c.get("vol_ratio", 0),
                        "tier": c.get("tier", ""),
                        "data_source": data_source_label,
                        "data_age_days": data_age_days,
                        # FEB 11: Preserve rich metadata for meta-score ranking
                        "pre_signals": c.get("pre_signals", []),
                        "post_signals": c.get("post_signals", []),
                        "is_predictive": c.get("is_predictive", False),
                        "signal_count": c.get("signal_count", 0),
                        "is_dui": c.get("is_dui", False),
                        "batch": c.get("batch", 5),
                        "timing_recommendation": c.get("timing_recommendation", ""),
                    })
            
            if all_candidates:
                source_used = "scheduled_scan_results.json"
                age_tag = f" [{data_age_days}d old]" if data_age_days > 0 else " [fresh]"
                logger.info(f"  Tier 1 (scheduled_scan): {len(all_candidates)} candidates{age_tag}")
    except Exception as e:
        logger.debug(f"  Tier 1 failed: {e}")
    
    # ── Tier 2: scan_history.json (most recent scan with picks) ──────
    if not all_candidates:
        try:
            history_file = Path(PUTSENGINE_PATH) / "scan_history.json"
            if history_file.exists():
                with open(history_file) as f:
                    history = json.load(f)
                
                scans = history.get("scans", [])
                # Find the most recent scan with actual picks
                for scan in reversed(scans):
                    gamma = scan.get("gamma_drain", [])
                    dist = scan.get("distribution", [])
                    liq = scan.get("liquidity", [])
                    total_picks = len(gamma) + len(dist) + len(liq)
                    
                    if total_picks > 0:
                        scan_ts = scan.get("timestamp", "unknown")
                        logger.info(f"  Tier 2: Found scan with {total_picks} picks from {scan_ts}")
                        
                        # Calculate age from scan timestamp
                        try:
                            scan_dt = datetime.fromisoformat(scan_ts) if scan_ts and scan_ts != "unknown" else None
                            tier2_age = (datetime.now() - scan_dt).days if scan_dt else -1
                        except (ValueError, TypeError):
                            tier2_age = -1

                        for engine_key, picks in [("gamma_drain", gamma), ("distribution", dist), ("liquidity", liq)]:
                            for c in picks:
                                all_candidates.append({
                                    "symbol": c.get("symbol", ""),
                                    "score": c.get("score", c.get("composite_score", 0)),
                                    "price": c.get("current_price", 0) or c.get("close", 0),
                                    "passed_gates": True,
                                    "distribution_score": c.get("distribution_score", 0),
                                    "dealer_score": c.get("dealer_score", 0),
                                    "liquidity_score": c.get("liquidity_score", 0),
                                    "signals": c.get("signals", []),
                                    "block_reasons": [],
                                    "engine": f"PutsEngine ({engine_key})",
                                    "engine_type": engine_key,
                                    "scan_timestamp": scan_ts,
                                    "pattern_boost": c.get("pattern_boost", 0),
                                    "pattern_enhanced": c.get("pattern_enhanced", False),
                                    "vol_ratio": c.get("vol_ratio", 0),
                                    "tier": c.get("tier", ""),
                                    "data_source": f"scan_history ({scan_ts})",
                                    "data_age_days": tier2_age,
                                })
                        
                        source_used = f"scan_history.json ({scan_ts})"
                        break
        except Exception as e:
            logger.debug(f"  Tier 2 failed: {e}")
    
    # ── Tier 3: convergence/latest_top9.json (richest data) ──────────
    if not all_candidates:
        try:
            conv_file = Path(PUTSENGINE_PATH) / "logs" / "convergence" / "latest_top9.json"
            if conv_file.exists():
                with open(conv_file) as f:
                    conv = json.load(f)
                
                generated = conv.get("generated_at_et", "unknown")
                for c in conv.get("top9", []):
                    gamma_sigs = c.get("gamma_signals", [])
                    all_candidates.append({
                        "symbol": c.get("symbol", ""),
                        "score": c.get("convergence_score", 0),
                        "price": c.get("current_price", 0),
                        "passed_gates": True,
                        "distribution_score": c.get("ews_score", 0),
                        "dealer_score": c.get("gamma_score", 0),
                        "liquidity_score": 0,
                        "signals": gamma_sigs,
                        "block_reasons": [],
                        "engine": f"PutsEngine (convergence)",
                        "engine_type": c.get("gamma_engine", "convergence"),
                        "convergence_data": {
                            "sources_agreeing": c.get("sources_agreeing", 0),
                            "source_list": c.get("source_list", []),
                            "weather_forecast": c.get("weather_forecast", ""),
                            "expected_drop": c.get("expected_drop", ""),
                            "timing": c.get("timing", ""),
                            "recommendation": c.get("ews_recommendation", ""),
                            "days_on_list": c.get("days_on_list", 0),
                        },
                    })
                
                if all_candidates:
                    source_used = f"convergence/latest_top9.json ({generated})"
                    logger.info(f"  Tier 3 (convergence): {len(all_candidates)} picks from {generated}")
        except Exception as e:
            logger.debug(f"  Tier 3 failed: {e}")
    
    # ── Tier 4: pattern_scan_results.json ────────────────────────────
    if not all_candidates:
        try:
            pattern_file = Path(PUTSENGINE_PATH) / "pattern_scan_results.json"
            if pattern_file.exists():
                with open(pattern_file) as f:
                    patterns = json.load(f)
                
                for pat_type in ["pump_reversal", "two_day_rally", "high_vol_run"]:
                    for c in patterns.get(pat_type, []):
                        sym = c.get("symbol", c.get("ticker", ""))
                        if sym:
                            all_candidates.append({
                                "symbol": sym,
                                "score": c.get("score", c.get("confidence", 0.5)),
                                "price": c.get("current_price", 0) or c.get("close", 0) or c.get("price", 0),
                                "passed_gates": True,
                                "distribution_score": 0,
                                "dealer_score": 0,
                                "liquidity_score": 0,
                                "signals": [pat_type],
                                "block_reasons": [],
                                "engine": f"PutsEngine ({pat_type})",
                                "engine_type": pat_type,
                            })
                
                if all_candidates:
                    scan_time = patterns.get("scan_time", "unknown")
                    source_used = f"pattern_scan_results.json ({scan_time})"
                    logger.info(f"  Tier 4 (patterns): {len(all_candidates)} candidates")
        except Exception as e:
            logger.debug(f"  Tier 4 failed: {e}")
    
    # ── Deduplicate and return ───────────────────────────────────────
    if not all_candidates:
        logger.warning("No PutsEngine data found across all tiers")
        return []
    
    seen = {}
    for c in all_candidates:
        sym = c["symbol"]
        if sym and (sym not in seen or c["score"] > seen[sym]["score"]):
            seen[sym] = c
    
    deduped = sorted(seen.values(), key=lambda x: x["score"], reverse=True)
    
    # ── Enrich missing prices / uniform scores from supplementary data ──
    deduped = _enrich_candidates(deduped, top_n)
    
    logger.info(f"🔴 PutsEngine (cached via {source_used}): {len(deduped)} candidates, returning top {top_n}")
    for i, p in enumerate(deduped[:top_n], 1):
        logger.info(f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — ${p.get('price', 0):.2f}")
    
    return deduped[:top_n]


def _load_ews_scores() -> Dict[str, Dict[str, Any]]:
    """
    Load EWS (Early Warning System) IPI scores for all scanned tickers.
    
    Returns dict: {symbol: {"ipi": float, "level": str, "unique_footprints": int}}
    Used to boost ranking of tickers with institutional pressure signals.
    """
    ews_file = Path(PUTSENGINE_PATH) / "logs" / "ews_last_results.json"
    try:
        if ews_file.exists():
            with open(ews_file) as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) > 10:
                logger.info(f"  📊 Loaded EWS scores for {len(data)} tickers")
                return data
    except Exception as e:
        logger.debug(f"  EWS load failed: {e}")
    return {}


# ─── Tier quality weights for meta-scoring ────────────────────────
# Based on PutsEngine's tier classification of bearish signal strength.
# EXPLOSIVE = strongest institutional distribution evidence
TIER_WEIGHTS = {
    "\U0001f525 EXPLOSIVE":       1.00,
    "\U0001f3db\ufe0f CLASS A":   0.90,
    "\U0001f4aa STRONG":          0.80,
    "\U0001f440 MONITORING":      0.65,
    "\U0001f4ca WATCHING":        0.55,
    "\U0001f7e1 CLASS B":         0.50,
    "\u274c BELOW THRESHOLD":     0.40,
}


def _compute_meta_score(
    candidate: Dict[str, Any],
    ews_data: Dict[str, Dict[str, Any]],
    live_price: Optional[float] = None,
    ews_percentiles: Optional[Dict[str, float]] = None,
    sector_boost_set: Optional[set] = None,
    earnings_set: Optional[set] = None,
    is_pm_scan: bool = False,
) -> float:
    """
    Compute a differentiated META-SCORE for ranking puts candidates.
    
    This solves the critical 0.950 score compression problem where 67% of
    tickers get identical composite scores, making Top 10 selection random.
    
    FEB 11 OVERHAUL + FEB 11 PM-SCAN UPGRADE — Key improvements:
    
    1. BUG FIX: Gap detection now uses _cached_price (not price, which gets
       overwritten with live price in _enrich_candidates before this runs).
    2. EWS IPI now uses PERCENTILE ranking instead of raw IPI value.
       When 66% of tickers have IPI >= 0.7, raw IPI can't differentiate.
       Percentile ranking ensures the top 10% of IPI scores actually stand out.
    3. Sector rotation boost: when 3+ tickers in the same sector have
       distribution signals, all tickers in that sector get a boost.
       This catches sector-wide selloffs (e.g., 9 semiconductors crashed together).
    4. Gap weight INCREASED to 40% — this is the most impactful predictor.
       A stock gapping down -10% pre-market is virtually certain to be bearish.
    5. Stronger penalty for rallying stocks (gap > +2% = -0.15 penalty).
    6. EARNINGS PROXIMITY BOOST: Stocks reporting earnings within 2 trading
       days AND showing dark_pool_violence/put_buying_at_ask get +15% boost.
       This catches BDX (-20%), UPWK (-20.2%) style crashes.
    7. MULTI-SIGNAL CONVERGENCE: When 3+ predictive signals agree AND EWS
       IPI >= 0.80, add convergence premium for highest-conviction picks.
    8. PM SCAN DYNAMIC WEIGHTS: When running at 3:15 PM (no overnight gap
       yet), redistribute the 40% gap weight to earnings/convergence/tier
       since gap data is unavailable and would waste 40% of scoring power.
    
    AM Scan Components (total weight = 1.0):
      1. TIER QUALITY (15%): EXPLOSIVE tier has strongest bearish evidence
      2. SIGNAL QUALITY (15%): pre-signals (predictive) count more
      3. INTRADAY GAP (40%): THE MOST IMPACTFUL — confirmed bearish moves
      4. EWS INSTITUTIONAL PRESSURE (20%): IPI percentile from EWS
      5. DUI & BATCH & SECTOR (10%): Priority + sector rotation boost
    
    PM Scan Components (total weight = 1.0) — gap weight redistributed:
      1. TIER QUALITY (20%): Increased — EXPLOSIVE tier is key differentiator
      2. SIGNAL QUALITY (15%): pre-signals remain critical
      3. INTRADAY MOMENTUM (15%): Same-day price change as gap proxy
      4. EWS INSTITUTIONAL PRESSURE (20%): IPI percentile (unchanged)
      5. EARNINGS CATALYST (15%): Upcoming earnings + dark pool = smoking gun
      6. CONVERGENCE PREMIUM (5%): Multi-signal agreement
      7. DUI & BATCH & SECTOR (10%): Priority + sector rotation boost
    
    Returns: float 0.0-1.0 meta-score for ranking
    """
    sym = candidate.get("symbol", "")
    
    # ── 1. TIER QUALITY (0.0 - 1.0) ──────────────────────────────
    tier = candidate.get("tier", "")
    tier_score = TIER_WEIGHTS.get(tier, 0.50)
    
    # ── 2. SIGNAL QUALITY (0.0 - 1.0) ────────────────────────────
    pre_sigs = len(candidate.get("pre_signals", []))  if isinstance(candidate.get("pre_signals"), list) else 0
    post_sigs = len(candidate.get("post_signals", [])) if isinstance(candidate.get("post_signals"), list) else 0
    is_predictive = 1.0 if candidate.get("is_predictive", False) else 0.0
    sig_count = candidate.get("signal_count", 0)
    
    signal_raw = (pre_sigs * 3.0 + post_sigs * 1.0 + is_predictive * 2.0 + min(sig_count, 5)) / 12.0
    signal_score = min(signal_raw, 1.0)
    
    # ── 3. INTRADAY/PRE-MARKET GAP (0.0 - 1.0) ──────────────────
    # CRITICAL BUG FIX: Use _cached_price (the ORIGINAL scan price) not
    # "price" which gets overwritten with the live Polygon price in
    # _enrich_candidates BEFORE this function is called.
    # Without this fix, gap_pct = (live - live) / live = 0 ALWAYS.
    cached_price = candidate.get("_cached_price", candidate.get("price", 0))
    gap_score = 0.0
    if live_price and cached_price and cached_price > 0:
        gap_pct = ((live_price - cached_price) / cached_price) * 100
        if gap_pct < 0:
            # Stronger scaling: -3% = 0.375, -8% = 1.0, -10%+ = 1.0
            gap_score = min(abs(gap_pct) / 8.0, 1.0)
        elif gap_pct > 2:
            gap_score = -0.15  # stronger penalty for rallying stocks
    
    # ── 4. EWS INSTITUTIONAL PRESSURE (0.0 - 1.0) ────────────────
    ews_entry = ews_data.get(sym, {})
    if not isinstance(ews_entry, dict):
        ews_entry = {}
    ews_ipi = ews_entry.get("ipi", 0)
    ews_footprints = ews_entry.get("unique_footprints", 0)
    
    # FEB 11 FIX: Use PERCENTILE-BASED IPI instead of raw IPI.
    # When 66% of tickers have IPI >= 0.7 (act level), raw IPI
    # gives nearly identical scores to most tickers — useless for ranking.
    # Percentile ranking ensures the top IPI scores actually differentiate.
    if ews_percentiles and sym in ews_percentiles:
        ipi_pct = ews_percentiles[sym]
    else:
        ipi_pct = ews_ipi  # fallback to raw IPI
    
    ews_score = ipi_pct * 0.6 + (ews_footprints / 8.0) * 0.4
    ews_score = min(ews_score, 1.0)
    
    # ── 5. DUI & BATCH & SECTOR ROTATION (0.0 - 1.0) ─────────────
    is_dui = 0.5 if candidate.get("is_dui", False) else 0.0
    batch = candidate.get("batch", 5)
    batch_s = max(0, (5 - batch) / 4.0) * 0.3
    # Sector rotation boost: if 3+ tickers in same sector have signals,
    # this ticker gets a sector boost (catches sector-wide selloffs)
    sector_s = 0.3 if (sector_boost_set and sym in sector_boost_set) else 0.0
    priority_score = min(is_dui + batch_s + sector_s, 1.0)
    
    # ── 6. EARNINGS PROXIMITY BOOST (0.0 - 1.0) ──────────────────
    # NEW: If stock reports earnings within next 2 trading days AND
    # has dark_pool_violence or put_buying_at_ask signals, this is
    # the strongest predictive signal for overnight crashes.
    # BDX (-20%), UPWK (-20.2%) were earnings-driven crashes.
    #
    # IMPORTANT: The boost is CONDITIONAL on bearish positioning evidence.
    # Earnings alone is NOT bearish — many stocks rally after earnings.
    # The boost only fires when dark_pool_violence + put_buying_at_ask
    # indicate institutions are POSITIONING for a post-earnings drop.
    # Without these signals, earnings proximity gets minimal weight.
    earnings_score = 0.0
    if earnings_set and sym in earnings_set:
        # Has upcoming earnings — check for institutional positioning
        sigs = candidate.get("signals", [])
        sig_str = str(sigs) if isinstance(sigs, list) else str(sigs)
        has_dp = "dark_pool_violence" in sig_str
        has_pb = "put_buying_at_ask" in sig_str
        has_sells = "repeated_sell_blocks" in sig_str
        has_weakness = "multi_day_weakness" in sig_str
        bearish_signals = sum([has_dp, has_pb, has_sells, has_weakness])
        
        if bearish_signals >= 3:
            earnings_score = 1.0  # Maximum: multiple bearish signals + earnings
        elif has_pb and (has_dp or has_sells):
            earnings_score = 0.8  # High: put buying + institutional selling
        elif bearish_signals >= 2:
            earnings_score = 0.6  # Moderate: 2 bearish signals
        elif bearish_signals >= 1 and tier_score >= 0.80:
            earnings_score = 0.4  # Conditional: 1 signal but high tier
        else:
            earnings_score = 0.1  # Minimal: earnings alone isn't bearish
    
    # ── 7. MULTI-SIGNAL CONVERGENCE (0.0 - 1.0) ──────────────────
    # NEW: When multiple HIGH-QUALITY signals agree AND EWS IPI is high,
    # this represents multi-source confirmation of institutional selling.
    # GEV had 4 pre-signals + IPI=1.0 and dropped -5.4%.
    #
    # IMPORTANT: dark_pool_violence appears in 95% of ALL candidates,
    # so it has ZERO discriminating power. We use a tiered approach:
    # - HIGH_QUALITY: Rare, specific institutional signals (strongest)
    # - STANDARD: Common signals that add context when combined
    HIGH_QUALITY_SIGNALS = {
        "put_buying_at_ask",        # Direct put buying — strongest
        "call_selling_at_bid",      # Hedging via call selling
        "multi_day_weakness",       # Multi-day selling pattern
        "flat_price_rising_volume", # Distribution divergence
        "gap_down_no_recovery",     # Failed bounce
    }
    STANDARD_SIGNALS = {
        "repeated_sell_blocks",     # Block selling
        "dark_pool_violence",       # Ubiquitous but adds context
    }
    sigs_list = candidate.get("signals", [])
    if isinstance(sigs_list, list):
        hq_count = sum(1 for s in sigs_list if s in HIGH_QUALITY_SIGNALS)
        std_count = sum(1 for s in sigs_list if s in STANDARD_SIGNALS)
    else:
        hq_count = 0
        std_count = 0
    
    convergence_score = 0.0
    if hq_count >= 3 and ews_ipi >= 0.80:
        convergence_score = 1.0   # Maximum: 3+ high-quality + institutional pressure
    elif hq_count >= 2 and std_count >= 1 and ews_ipi >= 0.80:
        convergence_score = 0.8   # Strong: 2 HQ + 1 standard + IPI
    elif hq_count >= 2 and ews_ipi >= 0.60:
        convergence_score = 0.6   # Moderate: 2 HQ + decent IPI
    elif hq_count >= 2:
        convergence_score = 0.4   # Moderate: 2 HQ signals alone
    elif hq_count >= 1 and std_count >= 1 and ews_ipi >= 0.90:
        convergence_score = 0.3   # Partial: 1 HQ + standard + high IPI
    
    # ── WEIGHTED COMBINATION (Dynamic based on scan time) ─────────
    if is_pm_scan:
        # PM SCAN (3:15 PM): No overnight gap available yet.
        # Redistribute the 40% gap weight to predictive factors.
        # gap_score here represents INTRADAY momentum (same-day drop),
        # which is less impactful than overnight gap but still useful.
        meta = (
            tier_score          * 0.20 +   # ↑ increased from 15%
            signal_score        * 0.15 +   # unchanged
            gap_score           * 0.15 +   # ↓ intraday momentum (was 40% for AM gap)
            ews_score           * 0.20 +   # unchanged
            earnings_score      * 0.15 +   # NEW: earnings catalyst
            convergence_score   * 0.05 +   # NEW: multi-signal convergence
            priority_score      * 0.10     # unchanged
        )
    else:
        # AM SCAN (9:35 AM): Overnight gap is the strongest signal.
        # Earnings and convergence still contribute but less weight
        # since gap data is available and more predictive.
        meta = (
            tier_score          * 0.12 +   # slightly reduced for AM
            signal_score        * 0.12 +   # slightly reduced for AM
            gap_score           * 0.35 +   # ↓ from 40% to make room for new factors
            ews_score           * 0.18 +   # slightly reduced for AM
            earnings_score      * 0.08 +   # NEW: still relevant post-earnings
            convergence_score   * 0.05 +   # NEW: multi-signal convergence
            priority_score      * 0.10     # unchanged
        )
    
    return max(0.0, min(meta, 1.0))


def _load_earnings_proximity() -> set:
    """
    Load tickers with earnings reports within the next 2 trading days.
    
    Stocks reporting earnings are HIGH RISK for overnight gaps. When combined
    with dark_pool_violence or put_buying_at_ask signals (institutional
    pre-positioning), this is the strongest predictive signal for crashes.
    
    Data source: PutsEngine's earnings_calendar_cache.json
    
    Example: BDX reported after Monday close → crashed -20% Tuesday morning.
    If we had detected BDX with upcoming earnings + dark_pool_violence on
    Monday 3:15 PM, we could have ranked it much higher.
    
    Returns: set of symbols with upcoming earnings
    """
    try:
        ec_file = Path(PUTSENGINE_PATH) / "earnings_calendar_cache.json"
        if not ec_file.exists():
            return set()
        
        with open(ec_file) as f:
            data = json.load(f)
        
        events = data.get("events", data if isinstance(data, dict) else {})
        if not isinstance(events, dict):
            return set()
        
        today = date.today()
        # Next 3 calendar days covers 2 trading days (accounts for weekends)
        cutoff = today + timedelta(days=4)
        
        upcoming = set()
        for sym, info in events.items():
            if not isinstance(info, dict):
                continue
            report_date_str = info.get("report_date", "")
            try:
                report_date = date.fromisoformat(report_date_str)
                if today <= report_date <= cutoff:
                    upcoming.add(sym)
            except (ValueError, TypeError):
                continue
        
        if upcoming:
            logger.info(
                f"  📅 Earnings proximity: {len(upcoming)} tickers reporting "
                f"within next 2 trading days: {sorted(upcoming)[:10]}..."
            )
        
        return upcoming
    except Exception as e:
        logger.debug(f"  Earnings calendar load failed: {e}")
        return set()


def _is_pm_scan() -> bool:
    """
    Determine if the current scan is a PM scan (after 2:00 PM ET).
    
    PM scans (3:15 PM) have different characteristics than AM scans (9:35 AM):
    - No overnight gap data available (gap will happen tonight)
    - Intraday momentum IS available (stock already weak today?)
    - Earnings proximity is most impactful (reporting tonight/tomorrow)
    
    This allows dynamic weight adjustment in the meta-score algorithm.
    """
    try:
        from zoneinfo import ZoneInfo
        et = datetime.now(ZoneInfo("America/New_York"))
        return et.hour >= 14  # 2:00 PM ET or later
    except ImportError:
        # Fallback: use local time heuristic
        now = datetime.now()
        return now.hour >= 14


def _fetch_intraday_changes(symbols: List[str]) -> Dict[str, float]:
    """
    Fetch same-day intraday price changes for PM scans.
    
    At 3:15 PM, overnight gap data is NOT available (the gap happens tonight).
    But we CAN check if the stock is already WEAK today:
    - Compare current price to today's open
    - A stock dropping -2% intraday on high volume signals weakness
    - This serves as a "gap proxy" for PM scans
    
    This is critical for Monday PM scans to detect stocks that will
    crash overnight (earnings, news, etc.).
    
    Returns: {symbol: intraday_change_pct} (negative = dropping)
    """
    changes = {}
    api_key = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")
    if not api_key or not symbols:
        return changes
    
    try:
        import requests
    except ImportError:
        return changes
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    for sym in symbols:
        try:
            # Fetch today's OHLC to compare open vs. current
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
                f"{today}/{today}"
            )
            resp = requests.get(
                url,
                params={"adjusted": "true", "apiKey": api_key},
                timeout=5,
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    bar = results[0]
                    open_price = bar.get("o", 0)
                    close_price = bar.get("c", 0)  # Current/latest price
                    if open_price > 0 and close_price > 0:
                        change_pct = ((close_price - open_price) / open_price) * 100
                        changes[sym] = change_pct
        except Exception:
            pass  # Non-critical — just skip
    
    if changes:
        drops = {s: p for s, p in changes.items() if p < -1.0}
        if drops:
            logger.info(
                f"  📉 Intraday drops detected: "
                f"{', '.join(f'{s}({p:+.1f}%)' for s, p in sorted(drops.items(), key=lambda x: x[1])[:10])}"
            )
    
    return changes


def _compute_ews_percentiles(ews_data: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """
    Compute PERCENTILE-BASED IPI rankings from EWS data.
    
    When 66% of tickers have IPI >= 0.7 (act level), raw IPI values
    cluster at the top and can't differentiate bearish from bullish stocks.
    Percentile ranking distributes scores 0.0-1.0 evenly, ensuring the
    top 10% of IPI scores actually stand out in the meta-score ranking.
    
    Returns: {symbol: percentile_rank (0.0-1.0)} where 1.0 = highest IPI
    """
    if not ews_data:
        return {}
    
    # Collect all (symbol, ipi) pairs
    ipi_pairs = []
    for sym, entry in ews_data.items():
        if isinstance(entry, dict) and "ipi" in entry:
            ipi_pairs.append((sym, entry["ipi"]))
    
    if not ipi_pairs:
        return {}
    
    # Sort by IPI ascending → percentile = position / total
    ipi_pairs.sort(key=lambda x: x[1])
    total = len(ipi_pairs)
    
    return {sym: rank / total for rank, (sym, _) in enumerate(ipi_pairs)}


def _detect_sector_rotation(candidates: List[Dict[str, Any]]) -> set:
    """
    Detect sector rotation: when 3+ tickers in the same sector have
    distribution signals, ALL tickers in that sector should be boosted.
    
    This catches sector-wide selloffs. For example, on Feb 10-11:
    - 9 semiconductors crashed together (ALAB -10%, WDC -8%, STX -7%...)
    - 5 nuclear energy stocks dropped (LEU -9%, OKLO -7%, NNE -7%...)
    - 4 space/aero stocks sold off (RDW -7%, ASTS -6%, PL -5%...)
    
    Without sector detection, individual stocks may not rank high enough
    (their tier/signal scores are average), but the SECTOR pattern is
    a very strong bearish signal that should boost all stocks in it.
    
    Returns: set of symbols that should get a sector rotation boost
    """
    try:
        # Build reverse mapping: symbol -> sector
        from putsengine.config import EngineConfig
        sym_to_sector = {}
        for sector_name, tickers in EngineConfig.UNIVERSE_SECTORS.items():
            for t in tickers:
                sym_to_sector[t] = sector_name
    except ImportError:
        logger.debug("  Cannot import EngineConfig for sector mapping")
        return set()
    
    # Count how many candidates per sector — but only among the
    # TOP QUARTILE of candidates (by score/meta-score). When ALL 275
    # candidates are passed in, every sector with 3+ tickers would
    # trivially qualify, making the boost meaningless.
    # Restricting to the top quartile ensures sector rotation is
    # detected among the STRONGEST bearish candidates, not all.
    from collections import Counter
    
    # Use top quartile of candidates for sector counting
    top_quartile_size = max(50, len(candidates) // 4)
    top_candidates = candidates[:top_quartile_size]
    
    sector_counts = Counter()
    sector_universe = Counter()  # Total tickers per sector
    
    for c in top_candidates:
        sym = c.get("symbol", "")
        sector = sym_to_sector.get(sym)
        if sector:
            sector_counts[sector] += 1
    
    # Count total universe per sector (for concentration ratio)
    for sym, sector in sym_to_sector.items():
        sector_universe[sector] += 1
    
    # Find sectors with 3+ candidates IN THE TOP QUARTILE
    # AND where that represents a meaningful concentration
    # (at least 20% of the sector's tickers are in the top quartile)
    rotating_sectors = set()
    for sector, count in sector_counts.items():
        universe_size = sector_universe.get(sector, 1)
        concentration = count / universe_size if universe_size > 0 else 0
        if count >= 3 and concentration >= 0.15:  # 3+ stocks AND 15%+ concentration
            rotating_sectors.add(sector)
    
    if rotating_sectors:
        logger.info(
            f"  🔄 Sector rotation detected in: "
            f"{', '.join(f'{s}({sector_counts[s]})' for s in rotating_sectors)}"
        )
    
    # Build boost set: all tickers in rotating sectors
    boost_set = set()
    for c in candidates:
        sym = c.get("symbol", "")
        sector = sym_to_sector.get(sym)
        if sector in rotating_sectors:
            boost_set.add(sym)
    
    return boost_set


def _validate_picks(picks: List[Dict[str, Any]], source: str) -> None:
    """
    Log detailed data quality warnings for the final set of picks.

    Checks for:
      - Zero prices (no market data)
      - Uniform scores (ranking is meaningless)
      - All sub-scores zero (minimal data source)
      - Score range health (how well-differentiated the picks are)
    """
    if not picks:
        logger.warning(f"  ⚠️ DATA QUALITY ({source}): 0 picks returned — report will be empty")
        return

    zero_prices = sum(1 for p in picks if p.get("price", 0) == 0)
    if zero_prices > 0:
        logger.warning(
            f"  ⚠️ DATA QUALITY ({source}): {zero_prices}/{len(picks)} picks "
            f"still have price=$0.00"
        )

    scores = [p["score"] for p in picks]
    unique_scores = len(set(scores))
    if unique_scores == 1 and len(scores) > 1:
        logger.warning(
            f"  ⚠️ DATA QUALITY ({source}): All {len(scores)} picks have "
            f"identical score={scores[0]:.3f} — ranking is degraded"
        )
    elif unique_scores <= 2 and len(scores) > 3:
        logger.warning(
            f"  ⚠️ DATA QUALITY ({source}): Only {unique_scores} unique scores "
            f"across {len(scores)} picks — limited differentiation "
            f"(range {min(scores):.3f}–{max(scores):.3f})"
        )
    else:
        logger.info(
            f"  ✅ DATA QUALITY ({source}): {unique_scores} unique scores, "
            f"range {min(scores):.3f}–{max(scores):.3f} — "
            f"good differentiation"
        )

    if all(p.get("distribution_score", 0) == 0 and
           p.get("dealer_score", 0) == 0 and
           p.get("liquidity_score", 0) == 0
           for p in picks):
        logger.warning(
            f"  ⚠️ DATA QUALITY ({source}): All sub-scores are zero — "
            f"data came from a minimal source"
        )

    # Score threshold gate: warn about low-conviction puts
    low_conviction = [p for p in picks if p["score"] < 0.20]
    if low_conviction:
        syms = ", ".join(p["symbol"] for p in low_conviction)
        logger.warning(
            f"  ⚠️ LOW CONVICTION ({source}): {len(low_conviction)}/{len(picks)} "
            f"picks have score < 0.20 — [{syms}]. "
            f"These should be treated as noise, not actionable signals."
        )

    # Log the final pick summary for audit trail
    for i, p in enumerate(picks, 1):
        conv_tag = " ⚠️LOW" if p["score"] < 0.20 else ""
        logger.info(
            f"    #{i:2d} {p['symbol']:6s} "
            f"score={p['score']:.3f} "
            f"price=${p.get('price', 0):.2f} "
            f"signals={len(p.get('signals', []))} "
            f"engine={p.get('engine_type', '?')}"
            f"{conv_tag}"
        )


def _build_supplementary_lookup() -> Dict[str, Dict[str, Any]]:
    """
    Build a comprehensive symbol lookup from ALL PutsEngine data files.
    Used to enrich fallback candidates that may have missing prices or scores.

    Reads (in priority order):
      1. scheduled_scan_results.json — best quality (has price + score + signals)
      2. pattern_scan_results.json   — has price under 'price' key
      3. convergence/latest_top9.json — may have current_price
    """
    lookup: Dict[str, Dict[str, Any]] = {}

    # ── Source 1: scheduled_scan_results.json (highest fidelity) ───────
    try:
        results_file = Path(PUTSENGINE_PATH) / "scheduled_scan_results.json"
        if results_file.exists():
            with open(results_file) as f:
                data = json.load(f)
            for engine_key in ["gamma_drain", "distribution", "liquidity"]:
                for c in data.get(engine_key, []):
                    sym = c.get("symbol", "")
                    if not sym:
                        continue
                    price = c.get("current_price", 0) or c.get("close", 0) or c.get("price", 0)
                    score = c.get("score", c.get("composite_score", 0))
                    if sym not in lookup or price > lookup[sym].get("price", 0):
                        lookup[sym] = {
                            "price": price,
                            "score": score,
                            "engine_type": engine_key,
                            "signals": c.get("signals", []),
                            "distribution_score": c.get("distribution_score", 0),
                            "dealer_score": c.get("dealer_score", 0),
                            "liquidity_score": c.get("liquidity_score", 0),
                        }
    except Exception as e:
        logger.debug(f"  Supplementary lookup: scheduled_scan failed: {e}")

    # ── Source 2: pattern_scan_results.json ────────────────────────────
    try:
        pattern_file = Path(PUTSENGINE_PATH) / "pattern_scan_results.json"
        if pattern_file.exists():
            with open(pattern_file) as f:
                patterns = json.load(f)
            for pat_type in ["pump_reversal", "two_day_rally", "high_vol_run"]:
                for c in patterns.get(pat_type, []):
                    sym = c.get("symbol", c.get("ticker", ""))
                    if not sym:
                        continue
                    price = c.get("price", 0) or c.get("current_price", 0) or c.get("close", 0)
                    if price > 0 and (sym not in lookup or lookup[sym].get("price", 0) == 0):
                        if sym not in lookup:
                            lookup[sym] = {
                                "price": price,
                                "score": c.get("score", c.get("confidence", 0)),
                                "engine_type": pat_type,
                                "signals": c.get("signals", []),
                            }
                        else:
                            lookup[sym]["price"] = price
    except Exception as e:
        logger.debug(f"  Supplementary lookup: pattern_scan failed: {e}")

    # ── Source 3: convergence/latest_top9.json ─────────────────────────
    try:
        conv_file = Path(PUTSENGINE_PATH) / "logs" / "convergence" / "latest_top9.json"
        if conv_file.exists():
            with open(conv_file) as f:
                conv = json.load(f)
            for c in conv.get("top9", []):
                sym = c.get("symbol", "")
                if not sym:
                    continue
                price = c.get("current_price", 0)
                if price > 0 and (sym not in lookup or lookup[sym].get("price", 0) == 0):
                    if sym not in lookup:
                        lookup[sym] = {
                            "price": price,
                            "score": c.get("convergence_score", 0),
                            "engine_type": c.get("gamma_engine", "convergence"),
                            "signals": c.get("gamma_signals", []),
                        }
                    else:
                        lookup[sym]["price"] = price
    except Exception as e:
        logger.debug(f"  Supplementary lookup: convergence failed: {e}")

    return lookup


def _fetch_polygon_prices(symbols: List[str]) -> Dict[str, float]:
    """
    Fetch the most recent close prices from Polygon API.

    Uses the last 7 days of daily bars (sorted desc) so we get the most
    recent trading day's close — which is far more current than the
    prev-close endpoint, especially during/after market hours.

    Falls back to the /prev endpoint if daily bars fail.
    """
    prices = {}
    api_key = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")
    if not api_key or not symbols:
        return prices

    try:
        import requests
    except ImportError:
        return prices

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    for sym in symbols:
        try:
            # Primary: latest daily bar (includes today's close if market open/closed)
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
                f"{start_date}/{end_date}"
            )
            resp = requests.get(
                url,
                params={"adjusted": "true", "sort": "desc", "limit": 5, "apiKey": api_key},
                timeout=10,
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    close_price = results[0].get("c", 0)
                    if close_price > 0:
                        prices[sym] = close_price
                        logger.debug(f"    {sym}: Polygon latest bar ${close_price:.2f}")
                        continue

            # Fallback: prev-close endpoint
            url = f"https://api.polygon.io/v2/aggs/ticker/{sym}/prev"
            resp = requests.get(url, params={"apiKey": api_key}, timeout=10)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    close_price = results[0].get("c", 0)
                    if close_price > 0:
                        prices[sym] = close_price
                        logger.debug(f"    {sym}: Polygon prev-close ${close_price:.2f}")
        except Exception as e:
            logger.debug(f"    {sym}: Polygon API failed: {e}")

    return prices


def _enrich_candidates(candidates: List[Dict[str, Any]], top_n: int) -> List[Dict[str, Any]]:
    """
    Enrich candidates with real-time prices and META-SCORE based ranking.

    FEB 11 OVERHAUL — Three critical improvements:

    1. **PRICES — Fetch real-time from Polygon for WIDER candidate pool**
       Instead of just top 10, fetch for top 50 candidates so we can
       detect price gaps BEFORE ranking (catching UPWK -20%, BDX -20%).

    2. **META-SCORE — Multi-factor ranking replaces raw composite score**
       The raw PutsEngine composite score is compressed to 0.950 for 67%
       of tickers (183/275), making Top 10 selection essentially random.
       The meta-score uses tier quality, signal quality, intraday gap,
       EWS institutional pressure, and DUI priority for differentiation.

    3. **GAP DETECTION — The most impactful new factor**
       Detects negative price gaps (pre-market or intraday) by comparing
       the Polygon live price to the cached scan price. Tickers with large
       gaps (-5%+) get massive score boosts, catching earnings reactions
       and overnight news that the distribution layer can't predict.

    Called after dedup and sort, before returning results from fallback.
    """
    if not candidates:
        return candidates

    supp_lookup = None  # lazy-loaded if needed

    # ==================================================================
    # STEP 1: DETECT SCORE COMPRESSION
    # ==================================================================
    from collections import Counter
    all_scores = [round(c["score"], 3) for c in candidates]
    score_counts = Counter(all_scores)
    most_common_score, most_common_count = score_counts.most_common(1)[0]
    compression_ratio = most_common_count / len(candidates)
    
    score_compressed = compression_ratio > 0.40  # 40%+ at same score = compressed
    if score_compressed:
        logger.warning(
            f"  ⚠️ SCORE COMPRESSION DETECTED: {most_common_count}/{len(candidates)} "
            f"({compression_ratio:.0%}) candidates at score={most_common_score:.3f} — "
            f"activating multi-factor META-SCORE ranking"
        )

    # ==================================================================
    # STEP 2: FETCH REAL-TIME PRICES FROM POLYGON (wider pool)
    # ==================================================================
    # When scores are compressed, fetch prices for a WIDER pool (top 50)
    # so we can use gap detection as a ranking factor.
    # When scores are differentiated, just fetch for top_n.
    price_fetch_count = min(50, len(candidates)) if score_compressed else min(top_n, len(candidates))
    logger.info(
        f"  📡 Fetching real-time prices from Polygon for "
        f"top {price_fetch_count} candidates..."
    )
    fetch_symbols = [c["symbol"] for c in candidates[:price_fetch_count]]
    polygon_prices = _fetch_polygon_prices(fetch_symbols)

    # CRITICAL: Store the ORIGINAL cached price for ALL candidates BEFORE
    # any Polygon price updates. _compute_meta_score uses _cached_price
    # to compute the gap vs live price. Without this, the gap detection
    # produces gap_pct = 0 because both prices are the same (live).
    for c in candidates[:price_fetch_count]:
        c["_cached_price"] = c.get("price", 0)

    updated_prices = 0
    gap_detected = 0
    for c in candidates[:price_fetch_count]:
        sym = c["symbol"]
        if sym in polygon_prices and polygon_prices[sym] > 0:
            new_price = polygon_prices[sym]
            old_price = c["_cached_price"]  # Use the preserved cached price
            # Store live price for gap detection
            c["_live_price"] = new_price
            if old_price > 0 and old_price != new_price:
                pct_diff = ((new_price - old_price) / old_price) * 100
                c["_gap_pct"] = pct_diff
                if pct_diff < -3.0:
                    gap_detected += 1
                    logger.info(
                        f"    🔻 {sym}: ${old_price:.2f} → ${new_price:.2f} "
                        f"({pct_diff:+.1f}% GAP DOWN)"
                    )
                elif abs(pct_diff) > 1:
                    logger.info(
                        f"    {sym}: ${old_price:.2f} → ${new_price:.2f} "
                        f"({pct_diff:+.1f}% stale)"
                    )
            c["price"] = new_price
            updated_prices += 1

    if updated_prices > 0:
        logger.info(f"  ✅ Updated {updated_prices}/{len(fetch_symbols)} prices from Polygon API")
    if gap_detected > 0:
        logger.info(f"  🔻 {gap_detected} tickers with significant gap-down (>3%) detected")

    # For any remaining $0 prices, try supplementary data files
    still_zero = [c["symbol"] for c in candidates[:price_fetch_count] if c.get("price", 0) == 0]
    if still_zero:
        supp_lookup = _build_supplementary_lookup()
        for c in candidates[:price_fetch_count]:
            if c.get("price", 0) == 0:
                sym = c["symbol"]
                if sym in supp_lookup and supp_lookup[sym].get("price", 0) > 0:
                    c["price"] = supp_lookup[sym]["price"]
                    logger.debug(f"    {sym}: price from supplementary data ${supp_lookup[sym]['price']:.2f}")
        final_zero = sum(1 for c in candidates[:price_fetch_count] if c.get("price", 0) == 0)
        if final_zero > 0:
            logger.warning(f"  ⚠️ {final_zero} picks still at $0.00 after all enrichment")

    # Enrich sub-scores from supplementary data if missing
    if any(
        c.get("distribution_score", 0) == 0 and
        c.get("dealer_score", 0) == 0 and
        c.get("liquidity_score", 0) == 0
        for c in candidates[:top_n]
    ):
        if supp_lookup is None:
            supp_lookup = _build_supplementary_lookup()
        for c in candidates[:top_n]:
            sym = c["symbol"]
            if sym in supp_lookup:
                supp = supp_lookup[sym]
                for key in ["distribution_score", "dealer_score", "liquidity_score"]:
                    if c.get(key, 0) == 0 and supp.get(key, 0) > 0:
                        c[key] = supp[key]

    # ==================================================================
    # STEP 3: MULTI-FACTOR META-SCORE RANKING
    # ==================================================================
    # Always apply when score compression is detected.
    # This replaces the old pattern_boost / tie-breaker approach which
    # only fired when ALL top 10 had identical scores (often masked by
    # 1-2 tickers with slightly different scores like AAPL=0.965).
    if score_compressed:
        logger.info("  🧠 Computing multi-factor META-SCORES...")
        ews_data = _load_ews_scores()
        
        # ── FEB 11 FIX: Compute EWS IPI percentile ranks ─────────
        # When 66% of tickers have IPI >= 0.7, raw IPI can't differentiate.
        # Percentile ranking ensures the top 10% of IPI scores stand out.
        ews_percentiles = _compute_ews_percentiles(ews_data)
        if ews_percentiles:
            # Count how many are at each level
            act_count = sum(1 for s, e in ews_data.items() if isinstance(e, dict) and e.get("level") == "act")
            total_ews = sum(1 for s, e in ews_data.items() if isinstance(e, dict))
            logger.info(
                f"  📊 EWS IPI percentile normalization: {total_ews} tickers, "
                f"{act_count} at 'act' ({act_count/total_ews*100:.0f}%) — "
                f"using percentile ranking for differentiation"
            )
        
        # ── FEB 11 FIX: Sector rotation detection ────────────────
        # When 3+ tickers in the same sector have distribution signals,
        # boost ALL tickers in that sector. This catches sector-wide
        # selloffs (e.g., 9 semiconductors crashed together on Feb 10-11).
        sector_boost_set = _detect_sector_rotation(candidates[:price_fetch_count])
        if sector_boost_set:
            logger.info(
                f"  🔄 SECTOR ROTATION: {len(sector_boost_set)} tickers "
                f"in rotating sectors get boost"
            )
        
        # ── FEB 11 NEW: Earnings proximity boost ──────────────────
        # Stocks reporting earnings within next 2 trading days AND
        # showing dark_pool_violence = highest conviction for overnight crash.
        # This alone would have caught BDX (-20%) and UPWK (-20.2%).
        earnings_set = _load_earnings_proximity()
        
        # ── FEB 11 NEW: Detect AM vs PM scan ──────────────────────
        # PM scans (3:15 PM) have NO overnight gap data. The 40% gap weight
        # is wasted. Dynamic weights redistribute to earnings/convergence.
        pm_scan = _is_pm_scan()
        if pm_scan:
            logger.info(
                "  🌆 PM SCAN MODE: Activating earnings catalyst + "
                "convergence premium + intraday momentum weights "
                "(overnight gap not yet available)"
            )
            # Fetch intraday price changes (today's open vs current)
            # This serves as a "gap proxy" for PM scans
            intraday_syms = [c["symbol"] for c in candidates[:price_fetch_count]]
            intraday_changes = _fetch_intraday_changes(intraday_syms)
            # Inject intraday changes as _cached_price adjustments
            # so gap detection uses today's open vs. current price
            for c in candidates[:price_fetch_count]:
                sym = c["symbol"]
                if sym in intraday_changes:
                    c["_intraday_change"] = intraday_changes[sym]
        else:
            logger.info("  🌅 AM SCAN MODE: Using overnight gap as primary signal")
        
        meta_scored = 0
        for c in candidates[:price_fetch_count]:
            live_price = c.get("_live_price")
            meta = _compute_meta_score(
                c, ews_data, live_price, ews_percentiles,
                sector_boost_set, earnings_set, pm_scan,
            )
            c["meta_score"] = meta
            # Replace raw score with meta-score for ranking
            c["_raw_score"] = c["score"]
            c["score"] = meta
            meta_scored += 1
        
        # For candidates beyond the price-fetch pool, use metadata-only scoring
        for c in candidates[price_fetch_count:]:
            meta = _compute_meta_score(
                c, ews_data, None, ews_percentiles,
                sector_boost_set, earnings_set, pm_scan,
            )
            c["meta_score"] = meta
            c["_raw_score"] = c["score"]
            c["score"] = meta
        
        # Re-sort by meta-score
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        # Log the new top 10
        logger.info(f"  ✅ META-SCORE ranking applied to {meta_scored}+ candidates")
        scan_type = "PM" if pm_scan else "AM"
        logger.info(f"  📊 New Top {min(top_n, len(candidates))} after meta-scoring ({scan_type} mode):")
        for i, c in enumerate(candidates[:top_n], 1):
            gap = c.get("_gap_pct", 0)
            gap_tag = f" gap={gap:+.1f}%" if gap else ""
            intra = c.get("_intraday_change", 0)
            intra_tag = f" intra={intra:+.1f}%" if intra else ""
            ews_ipi_raw = 0
            ews_entry = ews_data.get(c["symbol"], {})
            if isinstance(ews_entry, dict):
                ews_ipi_raw = ews_entry.get("ipi", 0)
            sector_tag = " SECTOR-BOOST" if (sector_boost_set and c["symbol"] in sector_boost_set) else ""
            earn_tag = " 📅EARNINGS" if (earnings_set and c["symbol"] in earnings_set) else ""
            logger.info(
                f"    #{i:2d} {c['symbol']:6s} "
                f"meta={c['score']:.3f} "
                f"(raw={c.get('_raw_score', 0):.3f}) "
                f"tier={c.get('tier', '?'):20s} "
                f"ipi={ews_ipi_raw:.2f}"
                f"{gap_tag}{intra_tag}{sector_tag}{earn_tag}"
            )
    else:
        # Scores are already differentiated — use existing logic
        logger.info("  ✅ Scores well-differentiated — using raw PutsEngine ranking")

    # ==================================================================
    # Deduplicate signals (scan_history may have repeats)
    # ==================================================================
    for c in candidates:
        if c.get("signals"):
            seen_sigs: List[str] = []
            for s in c["signals"]:
                if s not in seen_sigs:
                    seen_sigs.append(s)
            c["signals"] = seen_sigs

    return candidates


def get_puts_universe() -> List[str]:
    """Get the full PutsEngine ticker universe."""
    try:
        from putsengine.config import EngineConfig
        return sorted(EngineConfig.get_all_tickers())
    except ImportError:
        logger.warning("Cannot import PutsEngine config, returning empty universe")
        return []
