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
    Enrich candidates with real-time prices and differentiated scores.

    Three critical fixes applied here:

    1. **PRICES — Always fetch real-time from Polygon API**
       Cached prices from scheduled_scan_results.json can be hours old.
       For trading decisions we need current market prices, not stale ones.
       Even non-zero cached prices may be wildly wrong (e.g. RBLX $66 vs $73).

    2. **SCORES — Apply pattern_boost if not yet integrated**
       PutsEngine runs pattern integration asynchronously. If the Meta Engine
       reads the file before integration, all scores are uniform base values
       (e.g. 0.65). The pattern_boost field tells us what each pick's
       boost should be, so we apply it ourselves.

    3. **TIE-BREAKING — Use signal count + vol_ratio + sub-scores**
       When scores remain tied after boosting, differentiate by number
       of signals (more = stronger conviction), vol_ratio (higher = more
       distribution activity), and sub-scores.

    Called after dedup and sort, before returning results from fallback.
    """
    if not candidates:
        return candidates

    top_slice = candidates[:top_n]
    top_scores = [c["score"] for c in top_slice]
    uniform_scores = len(set(top_scores)) == 1 and len(top_scores) > 1
    supp_lookup = None  # lazy-loaded if needed

    # ==================================================================
    # FIX 1: ALWAYS fetch real-time prices from Polygon
    # ==================================================================
    # Cached prices can be hours old; for volatile names like RBLX, WULF
    # the difference between cached ($66) and live ($73) is >10%.
    logger.info(
        f"  📡 Fetching real-time prices from Polygon for "
        f"top {min(top_n, len(candidates))} picks..."
    )
    all_symbols = [c["symbol"] for c in candidates[:top_n]]
    polygon_prices = _fetch_polygon_prices(all_symbols)

    updated_prices = 0
    for c in candidates[:top_n]:
        sym = c["symbol"]
        if sym in polygon_prices and polygon_prices[sym] > 0:
            new_price = polygon_prices[sym]
            old_price = c.get("price", 0)
            if old_price > 0 and old_price != new_price:
                pct_diff = ((new_price - old_price) / old_price) * 100
                if abs(pct_diff) > 1:
                    logger.info(
                        f"    {sym}: ${old_price:.2f} → ${new_price:.2f} "
                        f"({pct_diff:+.1f}% stale)"
                    )
            c["price"] = new_price
            updated_prices += 1

    if updated_prices > 0:
        logger.info(f"  ✅ Updated {updated_prices}/{len(all_symbols)} prices from Polygon API")

    # For any remaining $0 prices, try supplementary data files
    still_zero = [c["symbol"] for c in candidates[:top_n] if c.get("price", 0) == 0]
    if still_zero:
        supp_lookup = _build_supplementary_lookup()
        for c in candidates[:top_n]:
            if c.get("price", 0) == 0:
                sym = c["symbol"]
                if sym in supp_lookup and supp_lookup[sym].get("price", 0) > 0:
                    c["price"] = supp_lookup[sym]["price"]
                    logger.debug(f"    {sym}: price from supplementary data ${supp_lookup[sym]['price']:.2f}")
        final_zero = sum(1 for c in candidates[:top_n] if c.get("price", 0) == 0)
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
    # FIX 2: Score differentiation — apply pattern_boost if needed
    # ==================================================================
    if uniform_scores:
        logger.warning(
            f"  ⚠️ All top {len(top_scores)} picks have identical "
            f"score={top_scores[0]:.3f} — attempting score differentiation..."
        )

        # STEP A: Apply pattern_boost to base scores
        # PutsEngine's pattern integration adds pattern_boost to the base score.
        # If integration hasn't run yet, we do it ourselves.
        boosted = 0
        for c in candidates:
            boost = c.get("pattern_boost", 0)
            if boost > 0 and c.get("pattern_enhanced", False):
                c["score"] = min(c["score"] + boost, 1.0)
                boosted += 1

        if boosted > 0:
            logger.info(
                f"  ✅ Applied pattern_boost to {boosted} picks — "
                f"scores now differentiated"
            )
            candidates.sort(key=lambda x: x["score"], reverse=True)

        # STEP B: Re-check if still uniform after pattern_boost
        rechecked = [c["score"] for c in candidates[:top_n]]
        if len(set(rechecked)) == 1 and len(rechecked) > 1:
            # Try supplementary data scores (different file may have varied scores)
            if supp_lookup is None:
                supp_lookup = _build_supplementary_lookup()
            enriched_count = 0
            for c in candidates:
                sym = c["symbol"]
                if sym in supp_lookup and supp_lookup[sym].get("score", 0) > 0:
                    supp_score = supp_lookup[sym]["score"]
                    if supp_score != c["score"]:
                        c["score"] = supp_score
                        enriched_count += 1
            if enriched_count > 0:
                logger.info(
                    f"  ✅ Scores enriched for {enriched_count} symbols "
                    f"from supplementary data"
                )
                candidates.sort(key=lambda x: x["score"], reverse=True)

        # STEP C: Final tie-breaker for any remaining ties
        rechecked2 = [c["score"] for c in candidates[:top_n]]
        if len(set(rechecked2)) == 1 and len(rechecked2) > 1:
            logger.info(
                "  ℹ️ Scores still tied — applying multi-factor "
                "tie-breaker for ranking"
            )
            candidates.sort(
                key=lambda x: (
                    x["score"],
                    len(x.get("signals", [])),
                    x.get("vol_ratio", 0),
                    x.get("distribution_score", 0)
                    + x.get("dealer_score", 0)
                    + x.get("liquidity_score", 0),
                ),
                reverse=True,
            )

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
