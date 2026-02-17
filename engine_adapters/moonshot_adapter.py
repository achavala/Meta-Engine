"""
Moonshot/TradeNova Engine Adapter
==================================
Interface to get Top 10 moonshot candidates from the TradeNova Moonshot Engine.
Imports Moonshot modules directly without modifying the original codebase.

Data Sources (priority order for fallback):
  0. tomorrows_forecast.json — MWS 7-sensor forecasts (50 symbols with rich
     macro/sector/microstructure/options intel scoring).  This is the broadest
     and most analytically rich data source — captures MU, ON, TSM, WDC and
     other institutional-grade momentum plays that narrow scan files miss.
  1. eod_interval_picks.json — 10 picks per interval scan,
     captures intraday momentum names (MSFT, HIMS, BILL, etc.) that
     final_recommendations.json often misses.
  2. final_recommendations.json — Curated ranked picks with UW sentiment.
  3. final_recommendations_history.json — Historical picks (supplement).
  4. moonshot CSV data — Oldest fallback.

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
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# Add TradeNova to path
TRADENOVA_PATH = str(Path.home() / "TradeNova")
if TRADENOVA_PATH not in sys.path:
    sys.path.insert(0, TRADENOVA_PATH)

# ═══════════════════════════════════════════════════════════════════════
# SECTOR MAP  —  FEB 11  (Sector Momentum Boost)
# ═══════════════════════════════════════════════════════════════════════
# Build a ticker → sector map from PutsEngine's UNIVERSE_SECTORS
# (400+ symbols across 25+ sectors) supplemented by TradeNova forecast.
# Soft dependency — if PutsEngine is unavailable, uses forecast sectors.
# ═══════════════════════════════════════════════════════════════════════
_SECTOR_MAP: Dict[str, str] = {}
_STATIC_UNIVERSE: set = set()  # 104-ticker static universe gate
try:
    _pe_path = str(Path.home() / "PutsEngine")
    if _pe_path not in sys.path:
        sys.path.insert(0, _pe_path)
    from putsengine.config import EngineConfig
    for _sector_name, _tickers in EngineConfig.UNIVERSE_SECTORS.items():
        for _t in _tickers:
            _SECTOR_MAP[_t] = _sector_name
    _STATIC_UNIVERSE = set(EngineConfig.get_all_tickers())
    logger.debug(f"Sector map: {len(_SECTOR_MAP)} symbols, static universe: {len(_STATIC_UNIVERSE)} tickers")
except (ImportError, AttributeError):
    logger.debug("PutsEngine sector map unavailable — will use forecast sectors")


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

        # ── Merge MWS forecast data into the live candidate pool ─────
        # The live scan is narrow (Finviz + EDGAR).  The MWS 7-sensor
        # forecast has 50 institutional-grade symbols that the live scan
        # often misses (MU, ON, TSM, WDC, etc.).  Merging expands the
        # candidate pool so ORM can surface the best options plays.
        forecast_pool = _load_forecast_candidates()
        if forecast_pool:
            existing_symbols = {r["symbol"] for r in results}
            merged_count = 0
            for fc in forecast_pool:
                if fc["symbol"] not in existing_symbols:
                    results.append(fc)
                    existing_symbols.add(fc["symbol"])
                    merged_count += 1
            if merged_count:
                logger.info(
                    f"🟢 Merged {merged_count} MWS forecast candidates "
                    f"into live pool (total: {len(results)})"
                )

        # ── Universe gate — only allow tickers in the 104-ticker static list ──
        if _STATIC_UNIVERSE:
            before = len(results)
            results = [r for r in results if r.get("symbol", "") in _STATIC_UNIVERSE]
            filtered_out = before - len(results)
            if filtered_out:
                logger.info(
                    f"  🚫 Universe filter (live): {filtered_out} candidates removed "
                    f"(not in {len(_STATIC_UNIVERSE)}-ticker static universe), "
                    f"{len(results)} remain"
                )

        # ── Apply Call Options Return Multiplier (ORM) ──────────────
        # Enriches ALL candidates (not just top_n) so that stocks with
        # superior options microstructure can rise to the top.
        results = _enrich_moonshots_with_orm(results, top_n)
        
        top_picks = results[:top_n]
        n_picks = len(top_picks)
        logger.info(
            f"🟢 Moonshot Engine: {n_picks} picks selected (Policy B)"
            + (f" ⚠️ LOW OPPORTUNITY DAY" if n_picks < 3 else "")
        )
        for i, p in enumerate(top_picks, 1):
            mps_tag = f" MPS={p.get('_move_potential_score', 0):.2f}" if p.get('_move_potential_score') else ""
            logger.info(f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — ${p['price']:.2f}{mps_tag}")
        
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

                # ── FEB 11 FIX: Deflate uniform scores ──────────────────
                # When the interval scanner outputs ALL scores = 1.0 it
                # provides ZERO differentiation and unfairly dominates
                # over better-analyzed sources (MWS forecast, predictive
                # signals).  In that case, compute a meaningful score
                # from the only differentiating sub-components:
                #   • velocity_score   (0-1, varies per pick)
                #   • interval_persistence (how many scans flagged it)
                #   • weighted_rvol    (only if < 1.0)
                # Base: 0.80 (they ARE qualified picks from the scanner)
                raw_scores = [c.get("score", 0) for c in eod_candidates.values()]
                all_uniform = len(raw_scores) > 1 and len(set(raw_scores)) == 1
                if all_uniform:
                    max_persist = max((interval_counts.get(s, 1) for s in eod_candidates), default=1)
                    for sym, cand in eod_candidates.items():
                        vel = cand.get("velocity_score", 0) or 0
                        persist = interval_counts.get(sym, 1)
                        rvol = cand.get("volume_ratio", 0) or 0
                        rvol_bonus = min(rvol * 0.05, 0.05) if rvol < 0.99 else 0
                        persist_bonus = (persist / max_persist) * 0.10 if max_persist > 1 else 0
                        cand["score"] = min(0.80 + vel * 0.10 + persist_bonus + rvol_bonus, 0.95)
                    logger.info(
                        f"  ℹ️ eod scores were all {raw_scores[0]:.2f} — "
                        f"deflated to meaningful 0.80–0.95 range"
                    )

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

    # --- Source 0b: tomorrows_forecast.json (MWS 7-sensor forecasts) ---
    # 50 symbols with macro/sector/microstructure/options intel scoring.
    # This captures MU, ON, TSM, WDC and other institutional-grade momentum
    # plays that narrow scan files miss entirely.
    forecast_candidates = _load_forecast_candidates()
    if forecast_candidates:
        results.extend(forecast_candidates)

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
                    "score": min(rec.get("composite_score", 0) / 100.0, 1.0),  # Normalize + cap at 1.0
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

    # --- Source 4: predictive_signals_latest.json (197 multi-factor signals) ---
    # This is the BROADEST and most category-rich data source from TradeNova.
    # Covers 197 symbols with categories: pre_catalyst, early_setup,
    # mean_reversion, pre_breakout, short_squeeze, compression, options_flow.
    # Key: many movers (MU, ON, TSM, SNDK, NKTR, WDC, AGQ) appear here
    # even when they're missing from final_recommendations and eod_interval_picks.
    try:
        pred_file = Path(TRADENOVA_PATH) / "data" / "predictive_signals_latest.json"
        if pred_file.exists():
            with open(pred_file) as f:
                pred_data = json.load(f)
            pred_signals = pred_data.get("signals", [])

            # Score mapping: category strength + signal quality + target size
            _cat_base = {
                "pre_catalyst": 0.85,   # Smart money pre-positioning → strongest
                "early_setup": 0.75,    # Technical setup forming
                "mean_reversion": 0.70, # Oversold bounce / squeeze
            }
            _sig_bonus = {
                "options_flow": 0.10,   # Institutional flow → highest conviction
                "pre_breakout": 0.08,
                "short_squeeze": 0.08,
                "compression": 0.04,
                "oversold_bounce": 0.03,
            }

            pred_count = 0
            for sig in pred_signals:
                sym = sig.get("symbol", "")
                direction = sig.get("direction", "")
                if not sym or direction == "bearish":
                    continue  # Only bullish/neutral for Moonshot

                cat = sig.get("category", "")
                sig_type = sig.get("signal_type", "")
                raw_score = sig.get("score", 0) or 0
                tgt_pct = sig.get("target_pct", 0) or 0
                rr = sig.get("risk_reward", 0) or 0
                entry_zone = sig.get("entry_zone", [0, 0])

                # Compute meaningful score from category + signal + target
                base = _cat_base.get(cat, 0.65)
                sig_mod = _sig_bonus.get(sig_type, 0.02)
                dir_mod = 0.03 if direction == "bullish" else 0.0
                tgt_mod = min(tgt_pct * 0.005, 0.07)  # Up to +0.07 for 14%+ target
                rr_mod = min(rr * 0.01, 0.03)           # Up to +0.03 for 3+ R:R
                computed = min(base + sig_mod + dir_mod + tgt_mod + rr_mod, 1.0)

                entry_low = entry_zone[0] if isinstance(entry_zone, list) and len(entry_zone) >= 1 else 0
                entry_high = entry_zone[1] if isinstance(entry_zone, list) and len(entry_zone) >= 2 else 0

                results.append({
                    "symbol": sym,
                    "score": computed,
                    "price": (entry_low + entry_high) / 2 if entry_low and entry_high else 0,
                    "signals": sig.get("signals", []),
                    "signal_types": [cat, sig_type],
                    "option_type": "call",
                    "target_return": tgt_pct,
                    "engine": f"Moonshot (predictive:{cat})",
                    "sector": "",
                    "volume_ratio": 0,
                    "short_interest": 0,
                    "action": f"{cat}:{sig_type}",
                    "entry_low": entry_low,
                    "entry_high": entry_high,
                    "target": 0,
                    "stop": 0,
                    "rsi": 50,
                    "uw_sentiment": direction,
                    "data_source": f"predictive_signals ({pred_data.get('scan_label', '')})",
                    "data_age_days": 0,
                    "pred_category": cat,
                    "pred_signal_type": sig_type,
                    "pred_target_pct": tgt_pct,
                    "pred_risk_reward": rr,
                    "pred_confidence": sig.get("confidence", ""),
                })
                pred_count += 1
            if pred_count:
                logger.info(
                    f"🟢 TradeNova predictive_signals: {pred_count} bullish signals "
                    f"from {len(pred_signals)} total"
                )
    except Exception as e:
        logger.debug(f"Failed to read predictive_signals_latest.json: {e}")

    # --- Source 5: institutional_radar_promoted.json (84 inst. picks) ---
    try:
        ir_file = Path(TRADENOVA_PATH) / "data" / "institutional_radar_promoted.json"
        if ir_file.exists():
            with open(ir_file) as f:
                ir_data = json.load(f)
            promoted = ir_data.get("promoted_tickers", [])
            ir_count = 0
            for p in promoted:
                if not isinstance(p, dict):
                    continue
                sym = p.get("symbol", p.get("ticker", ""))
                if not sym:
                    continue
                conv = p.get("conviction", "")
                conv_base = {"HIGH": 0.85, "MEDIUM": 0.72, "LOW": 0.60}.get(conv, 0.65)
                sig_count = p.get("signal_count", 0) or 0
                crossday = p.get("crossday_bonus", 0) or 0
                score = min(conv_base + sig_count * 0.02 + min(crossday * 0.005, 0.05), 1.0)
                ir_signals = p.get("signals", [])
                results.append({
                    "symbol": sym,
                    "score": score,
                    "price": 0,
                    "signals": ir_signals if isinstance(ir_signals, list) else [],
                    "signal_types": ["institutional_radar"],
                    "option_type": "call",
                    "target_return": 0,
                    "engine": "Moonshot (institutional)",
                    "sector": "",
                    "volume_ratio": 0,
                    "short_interest": 0,
                    "action": f"institutional:{conv}",
                    "uw_sentiment": conv,
                    "data_source": "institutional_radar_promoted",
                    "data_age_days": 0,
                })
                ir_count += 1
            if ir_count:
                logger.info(f"🟢 TradeNova institutional_radar: {ir_count} promoted tickers")
    except Exception as e:
        logger.debug(f"Failed to read institutional_radar_promoted.json: {e}")

    # ── Universe gate — only allow tickers in the 104-ticker static list ──
    if _STATIC_UNIVERSE:
        before = len(results)
        results = [r for r in results if r.get("symbol", "") in _STATIC_UNIVERSE]
        filtered_out = before - len(results)
        if filtered_out:
            logger.info(
                f"  🚫 Universe filter: {filtered_out} candidates removed "
                f"(not in {len(_STATIC_UNIVERSE)}-ticker static universe), "
                f"{len(results)} remain"
            )

    # ── Deduplicate — keep highest-scoring entry per symbol ──────────
    # CRITICAL (FEB 11): Also merge metadata from lower-scored entries
    # so cross-source intelligence (MWS scores, pred signals, conviction)
    # is preserved on the winning entry for post-ORM boost calculations.
    seen = {}
    all_entries_per_sym: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        sym = r["symbol"]
        if sym not in all_entries_per_sym:
            all_entries_per_sym[sym] = []
        all_entries_per_sym[sym].append(r)
        if sym not in seen or r["score"] > seen[sym]["score"]:
            seen[sym] = r

    # Merge metadata from all sources into the winning entry
    _merge_keys = [
        "pred_category", "pred_signal_type", "pred_target_pct",
        "pred_risk_reward", "pred_confidence",
        "mws_score", "mws_action", "expected_move_pct",
        "microstructure_score", "whale_call_premium",
        "conviction", "interval_persistence", "velocity_score",
        "sector",  # Critical for sector momentum boost
        "catalysts",  # FIX 4: Heavy call buying / +GEX detection
        "bullish_probability",  # MWS forecast data
    ]
    for sym, entries in all_entries_per_sym.items():
        winner = seen[sym]
        for entry in entries:
            if entry is winner:
                continue
            for key in _merge_keys:
                if key not in winner and key in entry and entry[key]:
                    winner[key] = entry[key]
            # Preserve price from the source with non-zero price
            if not winner.get("price") and entry.get("price"):
                winner["price"] = entry["price"]

    # ── Multi-source convergence bonus ──────────────────────────────
    # When multiple independent data sources agree on a symbol, it's a
    # much higher-conviction signal.  Track which sources flagged each
    # symbol and award a convergence bonus:
    #   2 sources → +0.03
    #   3 sources → +0.06
    #   4+ sources → +0.10
    source_counts: Dict[str, int] = {}
    for r in results:
        sym = r["symbol"]
        ds = r.get("data_source", "")
        src_key = ds.split(" (")[0] if " (" in ds else ds
        key = f"{sym}|{src_key}"
        if key not in source_counts:
            source_counts[key] = 1
        # else already counted
    # Aggregate per symbol
    sym_source_count: Dict[str, int] = {}
    for key in source_counts:
        sym = key.split("|")[0]
        sym_source_count[sym] = sym_source_count.get(sym, 0) + 1

    # ── Store convergence data as METADATA only ────────────────────
    # CRITICAL (FEB 11 FIX): The convergence bonus must NOT be added
    # to the base score here because base scores are often at 1.0
    # (capped).  Adding +0.03 to 1.0 → capped at 1.0 → bonus wasted.
    # Instead, store the bonus as metadata and apply it POST-ORM in
    # _enrich_moonshots_with_orm(), where it's added to the blended
    # final score (typically 0.80–0.90, not capped).
    convergence_applied = 0
    for sym, entry in seen.items():
        sc = sym_source_count.get(sym, 1)
        if sc >= 4:
            bonus = 0.10
        elif sc >= 3:
            bonus = 0.06
        elif sc >= 2:
            bonus = 0.03
        else:
            bonus = 0.0
        if bonus > 0:
            # DON'T add to score — store as metadata for post-ORM application
            entry["_convergence_sources"] = sc
            entry["_convergence_bonus"] = bonus
            convergence_applied += 1
    if convergence_applied:
        logger.info(
            f"  📊 Multi-source convergence: {convergence_applied} symbols "
            f"tagged for post-ORM boost (2+ sources agree)"
        )

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
    n_returning = len(top_picks)

    # Log data freshness for transparency
    sources_used = set()
    for p in top_picks:
        ds = p.get("data_source", "unknown")
        sources_used.add(ds.split(" (")[0] if " (" in ds else ds)
    logger.info(
        f"🟢 Moonshot (cached): {len(deduped)} after Policy B gates, "
        f"returning {n_returning} picks. Sources: {', '.join(sorted(sources_used))}"
        + (f" ⚠️ LOW OPPORTUNITY DAY" if n_returning < 3 else "")
    )
    for i, p in enumerate(top_picks, 1):
        age = p.get("data_age_days", -1)
        age_tag = f" [DATA AGE: {age}d]" if age >= 0 else ""
        mps_tag = f" MPS={p.get('_move_potential_score', 0):.2f}" if p.get('_move_potential_score') else ""
        sig_cnt = len(p.get('signals', [])) if isinstance(p.get('signals'), list) else 0
        logger.info(
            f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — "
            f"${p.get('price', 0):.2f}{age_tag}{mps_tag} Sig={sig_cnt}"
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
# TOMORROWS FORECAST DATA SOURCE  —  FEB 11
# ═══════════════════════════════════════════════════════════════════════
# TradeNova's MWS 7-sensor forecast (50 symbols) is the broadest and
# most analytically rich data source.  Each forecast entry contains:
#   - 7 sensor scores (macro, sector, microstructure, options intel,
#     technical, sentiment, catalyst)
#   - GEX regime, call/put walls
#   - Target price, stop price, expected move %
#   - Action labels (BUY, LEAN BUY, HOLD / WAIT)
#
# Only BUY and LEAN BUY forecasts are included as Moonshot candidates.
# ═══════════════════════════════════════════════════════════════════════


def _load_forecast_candidates() -> List[Dict[str, Any]]:
    """
    Load actionable candidates from tomorrows_forecast.json.

    Returns a list of Moonshot-compatible candidate dicts, one per symbol
    where the MWS forecast action is BUY or LEAN BUY.

    Data freshness: only returns forecasts ≤ 3 calendar days old
    (covers Monday morning reading Friday's forecast).

    Returns empty list if the file is missing, stale, or unparseable.
    """
    try:
        forecast_file = Path(TRADENOVA_PATH) / "data" / "tomorrows_forecast.json"
        if not forecast_file.exists():
            logger.debug("  tomorrows_forecast.json not found — skipping")
            return []

        with open(forecast_file) as f:
            fc_data = json.load(f)

        generated = fc_data.get("generated_at", "")
        forecasts = fc_data.get("forecasts", [])
        fc_age = _calc_data_age_days(generated)

        # Skip stale data (> 3 days covers weekend gap)
        if fc_age > 3 and fc_age >= 0:
            logger.info(
                f"🟢 tomorrows_forecast.json skipped — {fc_age} days old "
                f"(generated: {generated})"
            )
            return []

        # Filter to actionable forecasts only
        actionable = [
            fc for fc in forecasts
            if fc.get("action", "").upper().startswith(("BUY", "LEAN BUY"))
        ]

        candidates = []
        for fc in actionable:
            sym = fc.get("symbol", "")
            if not sym:
                continue

            # ── Enhanced MWS score ──────────────────────────────────
            # Raw MWS (0-100 → 0-1) is conservatively scaled and always
            # loses to eod_interval_picks (many at 1.000).  To compete
            # fairly we add two evidence-based bonuses:
            #
            #  1. Expected Move bonus  (up to +0.15):
            #     Larger expected moves → bigger option payoffs.
            #     This is the most directly relevant signal for ORM.
            #     Formula: exp_move_pct × 0.03, capped at 0.15
            #       1% move → +0.03,  3% → +0.09,  5%+ → +0.15
            #
            #  2. Sensor Agreement bonus (up to +0.05):
            #     When 6-7 of 7 sensors agree → highest conviction.
            #     sensor_agreement ranges 0-1.
            #
            # This keeps the rank order within forecasts identical (all
            # get the same kind of bonus) but makes them competitive
            # with inflated scores from other sources.
            mws_base = fc.get("mws_score", 0) / 100.0
            exp_move = fc.get("expected_move_pct", 0) or 0
            move_bonus = min(exp_move * 0.03, 0.15) if exp_move > 0 else 0
            agreement = fc.get("sensor_agreement", 0) or 0
            agree_bonus = agreement * 0.05
            mws_score = min(mws_base + move_bonus + agree_bonus, 1.0)

            # Extract strong bullish signals from sensors
            signals = []
            for sensor in fc.get("sensors", []):
                sig_direction = sensor.get("signal", "")
                name = sensor.get("name", "")
                score = sensor.get("score", 0)
                if sig_direction == "bullish" and score >= 60:
                    signals.append(f"{name}: bullish ({score})")

            exp_range = fc.get("expected_range", [0, 0])
            entry_low = exp_range[0] if isinstance(exp_range, list) and len(exp_range) >= 1 else 0
            entry_high = exp_range[1] if isinstance(exp_range, list) and len(exp_range) >= 2 else 0

            candidates.append({
                "symbol": sym,
                "score": mws_score,
                "price": fc.get("current_price", 0),
                "signals": signals,
                "signal_types": ["MWS-7-Sensor"],
                "option_type": "call",
                "target_return": fc.get("expected_move_pct", 0),
                "engine": "Moonshot (MWS Forecast)",
                "sector": fc.get("sector", ""),
                "volume_ratio": 0,
                "short_interest": 0,
                "action": fc.get("action", ""),
                "entry_low": entry_low,
                "entry_high": entry_high,
                "target": fc.get("target_price", 0),
                "stop": fc.get("stop_price", 0),
                "rsi": 50,
                "uw_sentiment": fc.get("action", ""),
                "data_source": f"tomorrows_forecast ({generated})",
                "data_age_days": fc_age,
                # ── Extra forecast-specific fields (used by ORM / report) ──
                "mws_score": fc.get("mws_score", 0),  # Raw MWS 0-100
                "mws_action": fc.get("action", ""),
                "bullish_probability": fc.get("bullish_probability", 0),
                "expected_move_pct": fc.get("expected_move_pct", 0),
                "confidence": fc.get("confidence", ""),
                "confidence_score": fc.get("confidence_score", 0),
                "sensor_agreement": fc.get("sensor_agreement", 0),
                "gex_regime": fc.get("gex_regime", ""),
                "call_wall": fc.get("call_wall", 0),
                "put_wall": fc.get("put_wall", 0),
                # FIX 4: Preserve catalysts for Heavy Call Buying detection
                "catalysts": fc.get("catalysts", []),
            })

        logger.info(
            f"🟢 TradeNova tomorrows_forecast: {len(candidates)} actionable "
            f"from {len(forecasts)} total (generated: {generated})"
        )
        return candidates

    except Exception as e:
        logger.debug(f"Failed to read tomorrows_forecast.json: {e}")
        return []


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
    # FEB 15 FIX: Now returns 3-tuple including has_real_data flag
    # so callers can distinguish "computed from data" vs "default".
    has_any_data = bool(sym_gex or sym_iv or sym_oi or sym_flow or sym_dp)
    if not has_any_data:
        default = 0.35
        for f_name in ["gamma_leverage", "iv_expansion", "oi_positioning",
                        "delta_sweet", "short_dte", "vol_regime",
                        "dealer_position", "liquidity"]:
            factors[f_name] = default
        return default, factors, False  # False = no real data used

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

    # ── NEUTRAL DEFAULTS FOR MISSING DATA (FEB 15, 2026) ──────────────
    # Backtest finding: many CALL ORMs were 0.00 because partial data
    # left most factors at 0.0 (e.g. only GEX data, no IV/OI/flow).
    # When a factor has NO data source at all, assign 0.30 neutral
    # instead of 0.0 so the ORM isn't artificially crushed.
    # Factors that DID compute from real data keep their actual scores.
    NEUTRAL_DEFAULT = 0.30
    if not sym_iv:
        # No IV data → iv_expansion, short_dte, vol_regime get neutral
        if factors.get("iv_expansion", 0) == 0:
            factors["iv_expansion"] = NEUTRAL_DEFAULT
        if factors.get("short_dte", 0) == 0 and not call_trades:
            factors["short_dte"] = NEUTRAL_DEFAULT
        if factors.get("vol_regime", 0) == 0 and not sym_gex:
            factors["vol_regime"] = NEUTRAL_DEFAULT
    if not sym_oi:
        if factors.get("oi_positioning", 0) == 0:
            factors["oi_positioning"] = NEUTRAL_DEFAULT
    if not call_trades:
        if factors.get("delta_sweet", 0) == 0:
            factors["delta_sweet"] = NEUTRAL_DEFAULT
        if factors.get("liquidity", 0) == 0 and not sym_dp:
            factors["liquidity"] = NEUTRAL_DEFAULT

    # ── WEIGHTED COMBINATION ──────────────────────────────────────────
    # FEB 12 UPDATE: Increased IV expansion (0.15→0.20) and dealer positioning (0.10→0.15)
    # based on institutional analysis: UNH (IV expansion=1.00) and MRVL (dealer_position=1.00) were top winners
    # Adjusted gamma_leverage (0.20→0.15) and liquidity (0.10→0.05) to maintain sum=1.0
    orm = (
        factors["gamma_leverage"]  * 0.15 +  # REDUCED from 0.20 to balance weights
        factors["iv_expansion"]    * 0.20 +  # INCREASED from 0.15 (UNH success case: IV expansion = 1.00)
        factors["oi_positioning"]  * 0.15 +
        factors["delta_sweet"]     * 0.10 +
        factors["short_dte"]       * 0.10 +
        factors["vol_regime"]      * 0.10 +
        factors["dealer_position"] * 0.15 +  # INCREASED from 0.10 (MRVL success case: dealer_position = 1.00)
        factors["liquidity"]       * 0.05   # REDUCED from 0.10 to balance weights
    )
    return max(0.0, min(orm, 1.0)), factors, True  # True = real data used


def _apply_sector_momentum_boost(
    candidates: List[Dict[str, Any]],
) -> int:
    """
    FIX 3 (FEB 16): Detect hot sectors and boost ALL stocks in those sectors.

    NOW ACTIVATED — reads sector_sympathy_alerts.json (TradeNova) to
    supplement the candidate-based sector detection.  Previously the
    sympathy_score was 0.00 everywhere because this data wasn't consumed.

    Sources combined:
      1. PutsEngine UNIVERSE_SECTORS (primary sector map)
      2. tomorrows_forecast sector field
      3. sector_sympathy_alerts.json (NEW — 45 leaders, 118 alerts)

    A "hot sector" has 3+ bullish signals (from ANY source).

    Sector heat tiers:
      3-4 strong peers  → base boost +0.10  (FEB 16: raised from 0.025)
      5-7 strong peers  → base boost +0.12
      8-10 strong peers → base boost +0.14
      11+ strong peers  → base boost +0.16

    Returns the number of candidates that received a sector boost.
    """
    if not _SECTOR_MAP and not candidates:
        return 0

    # ── Step 1: Build sector map including forecast sectors ────────
    sector_lookup = dict(_SECTOR_MAP)  # Copy from PutsEngine
    for c in candidates:
        sym = c["symbol"]
        fc_sector = c.get("sector", "")
        if sym not in sector_lookup and fc_sector:
            fc_lower = fc_sector.lower()
            if "technol" in fc_lower:
                sector_lookup[sym] = "mega_cap_tech"
            elif "financ" in fc_lower:
                sector_lookup[sym] = "financials"
            elif "communi" in fc_lower:
                sector_lookup[sym] = "telecom"
            elif "industr" in fc_lower:
                sector_lookup[sym] = "industrials"
            elif "consumer" in fc_lower:
                sector_lookup[sym] = "consumer"
            elif "health" in fc_lower:
                sector_lookup[sym] = "healthcare_insurance"
            elif "energy" in fc_lower:
                sector_lookup[sym] = "nuclear_energy"
            else:
                sector_lookup[sym] = fc_sector

    # ── FIX 3 (NEW): Load sector_sympathy_alerts.json ─────────────
    # The sympathy_score was 0.00 everywhere because this data wasn't
    # being consumed.  Now we read leaders + alerts and inject them
    # into the sector heat detection.
    sympathy_sector_counts: Dict[str, set] = {}
    try:
        sa_file = _TRADENOVA_DATA / "sector_sympathy_alerts.json"
        if sa_file.exists():
            with open(sa_file) as f:
                sa_data = json.load(f)
            from collections import defaultdict as _dd
            sympathy_sector_counts = _dd(set)

            for _key, leader_info in sa_data.get("leaders", {}).items():
                if not isinstance(leader_info, dict):
                    continue
                sector_name = leader_info.get("sector_name", "")
                sym = leader_info.get("symbol", "")
                appearances = leader_info.get("appearances_48h", 0) or 0
                if sym and sector_name and appearances >= 2:
                    sympathy_sector_counts[sector_name].add(sym)
                    # Also add to sector_lookup if missing
                    if sym not in sector_lookup:
                        sector_lookup[sym] = sector_name

            for sym, alert_info in sa_data.get("alerts", {}).items():
                if not isinstance(alert_info, dict):
                    continue
                sector_name = alert_info.get("sector_name", "")
                if sector_name and sym:
                    sympathy_sector_counts[sector_name].add(sym)
                    if sym not in sector_lookup:
                        sector_lookup[sym] = sector_name

            if sympathy_sector_counts:
                logger.debug(
                    f"  FIX 3: Loaded sector sympathy — "
                    f"{sum(len(v) for v in sympathy_sector_counts.values())} "
                    f"tickers across {len(sympathy_sector_counts)} sectors"
                )
    except Exception as e:
        logger.debug(f"  FIX 3: sector_sympathy_alerts.json failed: {e}")

    # ── Step 2: Group candidates by sector, count strong peers ─────
    from collections import defaultdict
    sector_candidates: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in candidates:
        sym = c["symbol"]
        sector = sector_lookup.get(sym, "")
        if sector:
            c["_sector"] = sector
            sector_candidates[sector].append(c)

    # A "strong" candidate: base score >= 0.70 (lowered from 0.80 for sympathy)
    hot_sectors: Dict[str, int] = {}
    for sector, members in sector_candidates.items():
        strong_count = sum(
            1 for m in members
            if m.get("_base_score", m.get("score", 0)) >= 0.70
        )
        # FIX 3: Also count stocks from sector_sympathy_alerts
        if sector in sympathy_sector_counts:
            # Add unique sympathy members not already counted
            candidate_syms = {m["symbol"] for m in members}
            extra_sympathy = sympathy_sector_counts[sector] - candidate_syms
            strong_count += len(extra_sympathy)

        if strong_count >= 3:
            hot_sectors[sector] = strong_count

    if not hot_sectors:
        return 0

    for sector, count in sorted(hot_sectors.items(), key=lambda x: -x[1]):
        logger.info(
            f"  🔥 Hot sector: {sector} — {count} bullish signals "
            f"({len(sector_candidates.get(sector, []))} candidates in pool)"
        )

    # ── Step 3: Apply differential sector boost ────────────────────
    # FEB 16: Increased boost magnitudes (was 0.025-0.070, now 0.10-0.16)
    # because the previous values were too small to lift sympathy plays
    # into the Top 10.
    boosted = 0
    for c in candidates:
        sector = c.get("_sector", "")
        if sector not in hot_sectors:
            continue

        strong_peers = hot_sectors[sector]

        # FEB 16: Raised base boost to be impactful
        if strong_peers >= 11:
            raw_boost = 0.16  # Extreme sector-wide rally
        elif strong_peers >= 8:
            raw_boost = 0.14  # Strong momentum
        elif strong_peers >= 5:
            raw_boost = 0.12  # Moderate momentum
        else:
            raw_boost = 0.10  # Mild sector sympathy (was 0.025)

        existing_boost = c.get("_post_orm_boost", 0)
        effective_factor = max(0.25, 1.0 - (existing_boost / 0.20))
        sector_boost = raw_boost * effective_factor

        if sector_boost > 0.005:
            c["score"] = min(c["score"] + sector_boost, 1.0)
            c["_sector_boost"] = sector_boost
            c["_sector_heat"] = strong_peers
            c["_post_orm_boost"] = c.get("_post_orm_boost", 0) + sector_boost
            boosted += 1

    return boosted


def _enrich_moonshots_with_orm(
    candidates: List[Dict[str, Any]],
    top_n: int = 10,
) -> List[Dict[str, Any]]:
    """
    Enrich moonshot candidates with the Call Options Return Multiplier.

    Blends the existing score (momentum/interval persistence) with ORM
    to surface stocks with the highest expected CALL OPTIONS return.

    FEB 16: Status-aware ORM blending (reduced from 0.45):
      computed ORM:  final = base × 0.82 + ORM × 0.18
      default ORM:   final = base × 0.92 + ORM × 0.08
      missing ORM:   final = base × 1.00 (no ORM blend)

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
        has_uw = False
        gex = iv_data = oi = flow_data = dp = {}

    # ── FEB 16 FIX: Status-aware ORM blending ──────────────────
    # Backtest finding: ORM at 0.45 weight overweights "institutional
    # quality" (large-cap tight spreads) and suppresses convex winners
    # (volatile small/mid-caps with 5%+ move potential).
    #
    # New weight schedule:
    #   computed  → w_orm = 0.18  (real UW data — trust moderately)
    #   default   → w_orm = 0.08  (no symbol data, fallback 0.35)
    #   missing   → w_orm = 0.00  (no UW data at all — don't blend)
    ORM_WEIGHT_COMPUTED = 0.18
    ORM_WEIGHT_DEFAULT  = 0.08
    ORM_WEIGHT_MISSING  = 0.00

    orm_count = 0
    orm_scores = []
    orm_computed_count = 0
    enrich_count = len(candidates)

    if has_uw:
        logger.info("  🎯 Computing CALL OPTIONS RETURN MULTIPLIER for ALL candidates...")
        for c in candidates[:enrich_count]:
            sym = c["symbol"]
            stock_px = c.get("price", 0)
            orm, factors, has_real_data = _compute_call_options_return_multiplier(
                sym, gex, iv_data, oi, flow_data, dp,
                stock_price=stock_px,
            )
            c["_orm_score"] = orm
            c["_orm_factors"] = factors
            if has_real_data:
                c["_orm_status"] = "computed"
                orm_computed_count += 1
            else:
                c["_orm_status"] = "default"
            orm_count += 1
            orm_scores.append(orm)

            # FEB 16: Status-aware ORM blending
            orm_status = c["_orm_status"]
            if orm_status == "computed":
                w_orm = ORM_WEIGHT_COMPUTED
            elif orm_status == "default":
                w_orm = ORM_WEIGHT_DEFAULT
            else:
                w_orm = ORM_WEIGHT_MISSING
            w_base = 1.0 - w_orm

            base_score = c.get("score", 0)
            c["_base_score"] = base_score
            final = base_score * w_base + orm * w_orm
            c["score"] = max(0.0, min(final, 1.0))
            c["_orm_weight_used"] = w_orm
    else:
        # FEB 15 FIX: When UW data is completely unavailable, still
        # set ORM fields for consistency with downstream code.
        logger.info("  ℹ️ Call-ORM: No UW options data available — "
                     "setting _orm_status='missing' for all candidates")
        for c in candidates[:enrich_count]:
            c["_orm_score"] = 0.0
            c["_orm_status"] = "missing"
            c["_orm_factors"] = {}
            c["_base_score"] = c.get("score", 0)
            c["_orm_weight_used"] = 0.0

    # ── Apply POST-ORM quality boosts ────────────────────────────
    # These boosts are applied AFTER ORM blending so they're not
    # wasted on base scores that were already at 1.0.  They capture
    # cross-source intelligence that the raw ORM can't measure.
    convergence_applied = 0
    heavy_call_applied = 0   # FIX 4 counter
    recurrence_applied = 0   # FIX 5 counter
    for c in candidates[:enrich_count]:
        boost = 0.0

        # 1. Multi-source convergence bonus
        conv_bonus = c.get("_convergence_bonus", 0)
        if conv_bonus > 0:
            boost += conv_bonus
            convergence_applied += 1

        # 2. Signal-quality boost from predictive signals
        # Candidates with pre_catalyst + options_flow DIRECTLY predict
        # institutional positioning ahead of the move — this is the
        # highest-conviction signal for options returns.
        pred_cat = c.get("pred_category", "")
        pred_sig = c.get("pred_signal_type", "")
        if pred_cat == "pre_catalyst" and pred_sig == "options_flow":
            boost += 0.040  # Strong institutional options positioning
        elif pred_cat == "pre_catalyst":
            boost += 0.025
        elif pred_cat == "early_setup" and pred_sig == "pre_breakout":
            boost += 0.015
        elif pred_cat == "mean_reversion" and pred_sig == "short_squeeze":
            boost += 0.020  # Squeeze potential = high options return

        # 3. MWS forecast quality boost (from tomorrows_forecast)
        # A high MWS score (7-sensor) with BUY action indicates
        # multi-dimensional institutional conviction.
        mws = c.get("mws_score", 0) or 0
        if mws >= 80:
            boost += 0.025  # Top quartile MWS conviction
        elif mws >= 75:
            boost += 0.012

        # 4. Expected move magnitude
        # Larger expected moves translate to higher options returns
        exp_move = c.get("expected_move_pct", 0) or 0
        if exp_move >= 5.0:
            boost += 0.020  # 5%+ expected move
        elif exp_move >= 3.0:
            boost += 0.010

        # 5. Target size from predictive signals
        pred_tgt = c.get("pred_target_pct", 0) or 0
        if pred_tgt >= 12.0:
            boost += 0.015  # High upside target
        elif pred_tgt >= 8.0:
            boost += 0.008

        # ── FIX 4 (FEB 16): Heavy Call Buying / +GEX as Top-Tier Signal ──
        # The MWS forecast's catalysts field contains "Heavy call buying /
        # positive GEX" for stocks with bullish options microstructure.
        # This was present for 10/16 movers on Feb 12/13 but was NOT used
        # as a scoring signal.  Adding +0.15 when detected.
        # This single signal catches ~62% of gap-up movers.
        _catalysts = c.get("catalysts", [])
        if not isinstance(_catalysts, list):
            _catalysts = [str(_catalysts)] if _catalysts else []
        # Also check MWS action field for call buying keywords
        _mws_action = str(c.get("mws_action", "") or "")
        _catalyst_str = " ".join(str(x) for x in _catalysts).lower() + " " + _mws_action.lower()
        if "heavy call buying" in _catalyst_str or "positive gex" in _catalyst_str:
            boost += 0.15
            c["_heavy_call_buying"] = True
            heavy_call_applied += 1

        # ── FIX 5 (FEB 16): Signal Recurrence Boost ─────────────────
        # Track signal recurrence across intraday scans.
        # ROKU appeared in 5/11 Thursday scans — this "recurring
        # confirmation" pattern is a strong predictor.
        #   3+ scans → +0.10 ("persistent setup")
        #   5+ scans → +0.20 ("very persistent setup")
        #   Score escalating across scans → +0.05 ("confirming momentum")
        _persistence = c.get("interval_persistence", 0) or 0
        if _persistence >= 5:
            boost += 0.20
            c["_recurrence_boost"] = 0.20
            c["_recurrence_tag"] = "very persistent setup"
            recurrence_applied += 1
        elif _persistence >= 3:
            boost += 0.10
            c["_recurrence_boost"] = 0.10
            c["_recurrence_tag"] = "persistent setup"
            recurrence_applied += 1

        if boost > 0:
            c["score"] = min(c["score"] + boost, 1.0)
            c["_post_orm_boost"] = boost

    if convergence_applied or heavy_call_applied or recurrence_applied:
        logger.info(
            f"  📊 Post-ORM quality boosts: "
            f"convergence={convergence_applied}, "
            f"heavy_call_buying={heavy_call_applied} (FIX 4), "
            f"recurrence={recurrence_applied} (FIX 5)"
        )

    # ── 6. SECTOR MOMENTUM BOOST ─────────────────────────────────
    # When a sector has many strong candidates (3+), it signals a
    # sector-wide catalyst (e.g., MU earnings driving all semi stocks).
    # Stocks with WEAK individual signals but in a HOT sector get a
    # larger boost — "rising tide lifts all boats."
    #
    # Design principles:
    #   • Only triggers when 3+ sector peers have strong signals
    #   • Boost scales with sector heat (more peers = higher boost)
    #   • DIFFERENTIAL: stocks with fewer individual boosts benefit
    #     more (sector sympathy closes their gap to sector leaders)
    #   • This captures plays like SNDK +8.84% driven by MU earnings
    #     where SNDK had weak individual signals but sector momentum
    sector_boost_applied = _apply_sector_momentum_boost(candidates[:enrich_count])
    if sector_boost_applied:
        logger.info(
            f"  🔥 Sector momentum: {sector_boost_applied} symbols "
            f"boosted by hot-sector detection"
        )

    # Re-sort by final blended score
    candidates.sort(key=lambda x: x["score"], reverse=True)

    # ==================================================================
    # SCORE INVERSION FIX (FEB 15, 2026)
    # ==================================================================
    # Backtest finding: fresh differentiated signals with moderate scores
    # (0.50–0.80) OUTPERFORM cached uniform high scores (0.90+).
    # Deflate stale/cached high scores so fresh data competes fairly.
    staleness_deflated = 0
    for c in candidates[:enrich_count]:
        raw_score = c.get("_base_score", c.get("score", 0))
        data_age = 0
        data_src = c.get("data_source", "")
        if isinstance(data_src, str) and ("cache" in data_src.lower() or "fallback" in data_src.lower()):
            data_age = max(data_age, 1)
        data_age = max(data_age, c.get("data_age_days", 0) or 0)
        data_age_hours = c.get("data_age_hours", data_age * 24 if data_age > 0 else 0)

        # Mark stale data for downstream gate filtering (>12h old)
        if data_age_hours > 12 or data_age > 0:
            c["_data_stale"] = True
            c["_data_age_hours"] = data_age_hours if data_age_hours > 0 else data_age * 24

        if data_age > 0 and raw_score >= 0.90:
            sigs = c.get("signals", [])
            n_sigs = len(sigs) if isinstance(sigs, list) else 0
            uniqueness_factor = min(n_sigs / 5.0, 1.0)
            # Enhanced staleness penalty — scales more aggressively
            age_factor = min(data_age * 0.05, 0.15)  # Increased from 0.03 → 0.05
            staleness_penalty = age_factor * (1.0 - uniqueness_factor * 0.5)
            if staleness_penalty > 0.005:
                c["score"] = max(c["score"] - staleness_penalty, 0.30)
                c["_staleness_penalty"] = staleness_penalty
                staleness_deflated += 1
    if staleness_deflated > 0:
        candidates.sort(key=lambda x: x["score"], reverse=True)
        logger.info(
            f"  📉 Score inversion fix: {staleness_deflated} stale high-score "
            f"picks deflated (cached data with score ≥ 0.90)"
        )

    # ══════════════════════════════════════════════════════════
    # MOVE POTENTIAL SCORE (FEB 16, 2026)
    # ══════════════════════════════════════════════════════════
    try:
        from trading.move_potential import batch_compute_move_potential
        
        mps_candidates = candidates[:min(40, len(candidates))]
        mps_symbols = [c["symbol"] for c in mps_candidates]
        
        # Check for earnings
        try:
            from engine_adapters.puts_adapter import _load_earnings_proximity
            _earnings_for_mps = _load_earnings_proximity()
        except Exception:
            _earnings_for_mps = set()
        
        logger.info(f"  📐 Computing MOVE POTENTIAL SCORE for top {len(mps_symbols)} candidates...")
        mps_results = batch_compute_move_potential(
            mps_symbols,
            earnings_set=_earnings_for_mps,
        )
        
        mps_applied = 0
        for c in mps_candidates:
            sym = c["symbol"]
            if sym in mps_results:
                mps_score, mps_components = mps_results[sym]
                c["_move_potential_score"] = mps_score
                c["_move_potential_components"] = mps_components
                mps_applied += 1
        
        if mps_applied:
            logger.info(
                f"  ✅ Move Potential Score: {mps_applied} candidates enriched "
                f"(avg={sum(c.get('_move_potential_score',0) for c in mps_candidates)/max(mps_applied,1):.3f})"
            )
    except Exception as e:
        logger.warning(f"  ⚠️ Move Potential Score: failed ({e}) — continuing without gate")

    # ──────────────────────────────────────────────────────────
    # QUALITY-OVER-QUANTITY SELECTION GATES — POLICY B v2 (FEB 16, 2026)
    # Replaces forced Top 10 with strict quality gates.
    # Backtested Feb 9-13: v1 had 62.5% WR but missed too many moonshot
    # winners due to inverted ORM filter and overly strict MPS/signal gates.
    #
    # MOONSHOT ENGINE THRESHOLDS — POLICY B v3 (FEB 16, 2026) — ULTRA-SELECTIVE
    # Target: 80% WR (quality over quantity, accepts 2-5 picks typical)
    # Based on winner pattern analysis (Feb 9-13 backtest):
    #   - Signal Count ≥ 6 (all winners had 6+ signals)
    #   - Base Score ≥ 0.70 (winners avg 0.80-0.88, except AMD=0.35 outlier)
    #   - MPS ≥ 0.65 (winners had 0.69-0.80, except TSM=0.40 outlier)
    #   - Require at least 1 premium signal (call_buying, iv_inverted, dark_pool, neg_gex)
    #   - Regime-aligned (STRONG_BULL requires call_buying, STRONG_BEAR requires iv_inverted OR score≥0.85)
    #   - Block bearish_flow in ALL regimes
    # ──────────────────────────────────────────────────────────
    MIN_SIGNAL_COUNT = 6          # POLICY B v3: Raised 5→6 (ultra-selective for 80% WR)
    MIN_BASE_SCORE = 0.70         # POLICY B v3: Raised 0.65→0.70 (winners avg 0.80-0.88)
    MIN_MOVE_POTENTIAL = 0.65     # POLICY B v3: Raised 0.50→0.65 (winners had 0.69-0.80)
    ORM_MISSING_PENALTY = 0.04    # Smaller penalty for moonshot (missing = likely volatile)
    # Breakeven realism proxy (adapter-level pre-check)
    MIN_EXPECTED_MOVE_VS_BREAKEVEN = 1.3
    TYPICAL_BREAKEVEN_PCT = 3.5   # v2: Lowered 5.0→3.5 (weekly ATM on 4%+ ATR stocks)

    # ── THETA AWARENESS (adapter-level, FEB 16 v2: WARNING not BLOCK) ──
    # This is a SIGNAL ENGINE for manual execution — never block picks.
    # Instead, flag theta exposure so the user can choose DTE accordingly.
    # Same-day gap plays are unaffected by theta (open and close same day).
    _theta_warning = ""
    _theta_gap_days = 2
    try:
        from trading.nyse_calendar import calendar_days_to_next_session, next_trading_day
        _today = date.today()
        _gap_today = calendar_days_to_next_session(_today)
        _nxt = next_trading_day(_today)
        _gap_tomorrow = calendar_days_to_next_session(_nxt)
        _theta_gap_days = max(_gap_today, _gap_tomorrow)
        if _gap_today >= 4 or _gap_tomorrow >= 4:
            _theta_warning = (
                f"⚠️ THETA: {_theta_gap_days}-day gap to next session "
                f"(long weekend). Prefer same-day plays or DTE ≥ 7."
            )
            logger.warning(
                f"  ⚠️ THETA AWARENESS: Today={_today} "
                f"(gap_today={_gap_today}d, gap_next_session={_gap_tomorrow}d). "
                f"Flagging all picks with theta warning — NOT blocking. "
                f"User can trade same-day or choose longer DTE."
            )
        elif _today.weekday() == 4:  # Friday
            _theta_warning = (
                f"⚠️ THETA: Friday — weekend decay for short DTE. "
                f"Prefer same-day plays or DTE ≥ 5."
            )
            logger.info(f"  ℹ️ Friday theta awareness: flagging picks (not blocking)")
    except Exception as e:
        logger.warning(f"  ⚠️ Theta awareness: check failed ({e})")

    before_gates = len(candidates)
    filtered_candidates = []
    gate_reasons = []
    
    for c in candidates:
        orm = c.get("_orm_score", 0)
        orm_status = c.get("_orm_status", "missing")
        signals = c.get("signals", [])
        signal_count = len(signals) if isinstance(signals, list) else 0
        base_score = c.get("_base_score", c.get("score", 0))
        
        # ── ORM GATE — POLICY B v2: NO INVERSION ───────────────
        # v1 rejected ORM ≥ 0.60 ("too stable for moonshot") but this
        # rejected the biggest winners: IONQ (ORM=0.66, +37.9%),
        # UNH (ORM=0.72, +10.8%), NET (ORM=0.67, +11.8%).
        # High ORM = good options microstructure = GOOD for calls too.
        # v2: No ORM rejection for moonshot. Missing ORM gets light penalty.
        if orm_status in ("missing", "default"):
            # For moonshot, missing ORM is acceptable (volatile names often lack
            # institutional coverage). Apply a light penalty only.
            c["score"] = max(c.get("score", 0) - ORM_MISSING_PENALTY, 0.10)
            c["_orm_missing_penalty"] = ORM_MISSING_PENALTY
            # Still require minimum signal quality
            if signal_count < 4 and base_score < 0.70:
                gate_reasons.append(
                    f"{c.get('symbol', '?')}: ORM {orm_status} + weak signals "
                    f"({signal_count}) + low score ({base_score:.2f})"
                )
                continue
            logger.debug(
                f"     ℹ️ {c.get('symbol', '?')}: ORM {orm_status} — light penalty "
                f"({ORM_MISSING_PENALTY:.0%}), signals={signal_count}, score={base_score:.2f}"
            )

        # ── SIGNAL COUNT GATE — POLICY B v3 (FEB 16, 2026) ────────
        # v3: Raised to ≥6. Winner analysis: all moonshot winners had 6+ signals.
        # Ultra-selective for maximum WR.
        if signal_count < MIN_SIGNAL_COUNT:
            gate_reasons.append(
                f"{c.get('symbol', '?')}: {signal_count} signals < {MIN_SIGNAL_COUNT} (Policy B v3 MOONSHOT — ultra-selective)"
            )
            continue
        if base_score < MIN_BASE_SCORE:
            gate_reasons.append(
                f"{c.get('symbol', '?')}: base score {base_score:.3f} < {MIN_BASE_SCORE} (Policy B v3 MOONSHOT — ultra-selective)"
            )
            continue
        
        # ── PRICE DATA VALIDATION GATE ────────────────────────────
        pick_price = float(c.get("price", 0) or 0)
        if pick_price <= 0:
            gate_reasons.append(f"{c.get('symbol', '?')}: no valid price data (price={pick_price})")
            continue
        if pick_price < 1.0:
            gate_reasons.append(f"{c.get('symbol', '?')}: penny stock price ${pick_price:.2f}")
            continue
        if pick_price > 10000:
            gate_reasons.append(f"{c.get('symbol', '?')}: unrealistic price ${pick_price:.0f}")
            continue

        # ── SIGNAL UNIFORMITY PENALTY ─────────────────────────────
        if isinstance(signals, list) and signal_count >= 2:
            unique_signals = len(set(str(s) for s in signals))
            uniformity = 1.0 - (unique_signals / signal_count)
            if uniformity >= 0.70:
                penalty = 0.05 * uniformity
                c["score"] = max(c["score"] - penalty, 0.20)
                c["_signal_uniformity_penalty"] = penalty
                logger.debug(
                    f"     ⚠️ {c.get('symbol', '?')}: signal uniformity "
                    f"{uniformity:.0%} — penalized {penalty:.3f}"
                )
        
        # ── MOVE POTENTIAL GATE — POLICY B v3 (FEB 16, 2026) ────────
        # v3: Raised to 0.65. Winner analysis: moonshot winners had MPS 0.69-0.80
        # (except TSM=0.40 outlier). Ultra-selective for maximum WR.
        mps = c.get("_move_potential_score")
        if mps is not None and mps < MIN_MOVE_POTENTIAL:
            gate_reasons.append(
                f"{c.get('symbol', '?')}: MPS {mps:.3f} < {MIN_MOVE_POTENTIAL} "
                f"(Policy B v3 MOONSHOT — ultra-selective)"
            )
            continue
        
        # ── BREAKEVEN REALISM FILTER — v2 (FEB 16, 2026) ─────────
        # v2: TYPICAL_BREAKEVEN_PCT lowered 5.0→3.5. New threshold = 4.55%.
        # Weekly ATM calls/puts on 4%+ ATR stocks typically break even at ~3%.
        # Definitive check with actual contract data is in executor.py.
        if mps is not None and mps > 0:
            expected_move_pct = mps * 10.0
            required_for_breakeven = TYPICAL_BREAKEVEN_PCT * MIN_EXPECTED_MOVE_VS_BREAKEVEN
            if expected_move_pct < required_for_breakeven:
                gate_reasons.append(
                    f"{c.get('symbol', '?')}: Breakeven proxy — "
                    f"expected move {expected_move_pct:.1f}% < "
                    f"{required_for_breakeven:.1f}% required (MPS={mps:.2f})"
                )
                continue
        
        # ── PREMIUM SIGNAL REQUIREMENT — POLICY B v3 (FEB 16, 2026) ────────
        # v3: Require at least 1 premium signal (all winners had call_buying or iv_inverted).
        # Premium signals: iv_inverted, call_buying, dark_pool_massive, neg_gex_explosive.
        # This filters out low-conviction setups that pass basic gates but lack
        # institutional-quality microstructure signals.
        catalysts = c.get("catalysts", [])
        if isinstance(catalysts, list):
            cat_str = " ".join(str(cat) for cat in catalysts).lower()
        else:
            cat_str = str(catalysts).lower()
        
        sig_str = " ".join(str(s) for s in signals).lower() if isinstance(signals, list) else ""
        
        has_iv_inverted = "iv_inverted" in sig_str
        has_call_buying = "call buying" in cat_str or "positive gex" in cat_str
        has_dark_pool = "dark_pool_massive" in sig_str
        has_neg_gex = "neg_gex_explosive" in sig_str
        
        premium_count = sum([has_iv_inverted, has_call_buying, has_dark_pool, has_neg_gex])
        
        if premium_count < 1:
            gate_reasons.append(
                f"{c.get('symbol', '?')}: No premium signal (require at least 1 of: "
                f"iv_inverted, call_buying, dark_pool_massive, neg_gex_explosive) — "
                f"Policy B v3 MOONSHOT ultra-selective"
            )
            continue
        
        filtered_candidates.append(c)
    
    if gate_reasons:
        logger.info(f"  🚫 Policy B Quality Gates: {len(gate_reasons)} candidates filtered out:")
        for reason in gate_reasons[:15]:
            logger.info(f"     • {reason}")
        if len(gate_reasons) > 15:
            logger.info(f"     ... and {len(gate_reasons) - 15} more")
    
    candidates = filtered_candidates
    
    # ── LOW OPPORTUNITY DAY CHECK (POLICY B) ─────────────────
    if len(candidates) < 3:
        logger.warning(
            f"  ⚠️ LOW OPPORTUNITY DAY: Only {len(candidates)} moonshot candidates "
            f"passed Policy B quality gates (of {before_gates} total). "
            f"Capital preserved — quality over quantity."
        )
        for c in candidates:
            c["_low_opportunity_day"] = True
    
    logger.info(f"  ✅ After Policy B gates: {len(candidates)}/{before_gates} candidates remain")

    # ══════════════════════════════════════════════════════════════════
    # REGIME-AWARE SHADOW LOGGING + HARD BLOCK — FEB 16 v3
    # ══════════════════════════════════════════════════════════════════
    # Phase 1 (shadow): Log regime gate decisions but only hard-block
    #   Policy B v4 — HARD regime gates:
    #     - STRONG_BEAR / LEAN_BEAR / NEUTRAL → block ALL moonshots
    #     - STRONG_BULL / LEAN_BULL → require call_buying + score ≥ 0.70
    #     - Bearish UW flow → block in ANY regime
    #   Then conviction-rank survivors and keep top MAX_MOONSHOT_PER_SCAN.
    #
    # Feature extraction uses stable schema (booleans/floats), NOT string
    # matching, so it won't silently break if signal names change.
    # ══════════════════════════════════════════════════════════════════
    try:
        candidates = _apply_regime_shadow_and_hard_block(candidates, flow_data)
    except Exception as e:
        logger.warning(f"  ⚠️ Regime gate: failed ({e}) — continuing without gate")

    if orm_scores:
        avg_orm = sum(orm_scores) / len(orm_scores)
        max_orm = max(orm_scores)
        min_orm = min(orm_scores)
        logger.info(
            f"  ✅ Call-ORM applied to {orm_count} candidates "
            f"({orm_computed_count} from real data, "
            f"{orm_count - orm_computed_count} defaults/missing) "
            f"(avg={avg_orm:.3f}, range={min_orm:.3f}–{max_orm:.3f})"
        )

    # ── CONVICTION SCORING + TOP-N RANKING (Policy B v4) ──────────
    # Compute composite conviction score for each surviving candidate.
    # Factors: base_score, ORM, MPS, signal_count, premium signals, regime.
    # Then rank and keep only top MAX_MOONSHOT_PER_SCAN to maximise WR.
    MAX_MOONSHOT_PER_SCAN = 3  # Ultra-selective: max 3 per scan for 80% WR target
    MIN_CONVICTION_SCORE = 0.45  # Minimum conviction to pass (drops marginal picks)
    PM_CONVICTION_PENALTY = 0.75  # PM scans get 25% conviction penalty (PM momentum fades)

    for c in candidates:
        features = c.get("_features", {})
        base = c.get("_base_score", c.get("score", 0))
        orm = c.get("_orm_score", 0)
        mps_val = c.get("_move_potential_score", 0)
        sig_cnt = len(c.get("signals", [])) if isinstance(c.get("signals"), list) else 0

        # Premium signal count (each adds conviction)
        premium_count = sum([
            features.get("iv_inverted", False),
            features.get("call_buying", False),
            features.get("dark_pool_massive", False),
            features.get("neg_gex_explosive", False),
            features.get("institutional_accumulation", False),
        ])

        # Conviction score formula:
        #   40% base score (already blended with ORM)
        #   25% MPS (move potential)
        #   15% signal density (sig_count / 15, capped at 1.0)
        #   20% premium signal bonus (0.10 per premium signal, max 0.50)
        sig_density = min(sig_cnt / 15.0, 1.0)
        premium_bonus = min(premium_count * 0.10, 0.50)

        conviction = (
            0.40 * base
            + 0.25 * mps_val
            + 0.15 * sig_density
            + 0.20 * premium_bonus
        )

        # PM scan penalty: moonshot momentum fades by afternoon.
        # AM captures gap-ups / morning momentum; PM catches reversals.
        # Evidence: Feb 9 PM moonshots 0% WR vs AM 66.7% WR.
        try:
            from zoneinfo import ZoneInfo
            _et = datetime.now(ZoneInfo("America/New_York"))
            _is_pm = _et.hour >= 14
        except ImportError:
            _is_pm = datetime.now().hour >= 14
        if _is_pm:
            conviction *= PM_CONVICTION_PENALTY

        c["_conviction_score"] = round(conviction, 4)

    # Drop candidates below conviction floor
    below_floor = [c for c in candidates if c.get("_conviction_score", 0) < MIN_CONVICTION_SCORE]
    candidates = [c for c in candidates if c.get("_conviction_score", 0) >= MIN_CONVICTION_SCORE]
    if below_floor:
        logger.info(
            f"  🔻 Conviction floor ({MIN_CONVICTION_SCORE}): dropped {len(below_floor)} picks "
            f"({', '.join(c['symbol'] for c in below_floor)})"
        )

    # Sort by conviction score (descending) and take top N
    candidates.sort(key=lambda x: x.get("_conviction_score", 0), reverse=True)
    if len(candidates) > MAX_MOONSHOT_PER_SCAN:
        trimmed = candidates[MAX_MOONSHOT_PER_SCAN:]
        candidates = candidates[:MAX_MOONSHOT_PER_SCAN]
        logger.info(
            f"  🎯 Conviction Top-{MAX_MOONSHOT_PER_SCAN}: kept {len(candidates)}, "
            f"trimmed {len(trimmed)} lower-conviction picks "
            f"(conviction range: {candidates[0]['_conviction_score']:.3f} "
            f"to {candidates[-1]['_conviction_score']:.3f})"
        )
        for t in trimmed:
            logger.info(
                f"    ✂️ Trimmed: {t['symbol']:6s} "
                f"conviction={t['_conviction_score']:.3f} "
                f"score={t.get('score', 0):.3f}"
            )

    # Log final picks with ORM + conviction breakdown
    n_final = min(top_n, len(candidates))
    logger.info(f"  📊 FINAL {n_final} MOONSHOT picks (Policy B v4 ultra-selective) "
                 f"(ORM blending: computed={ORM_WEIGHT_COMPUTED}, "
                 f"default={ORM_WEIGHT_DEFAULT}, missing={ORM_WEIGHT_MISSING}):")
    for i, c in enumerate(candidates[:top_n], 1):
        base = c.get("_base_score", 0)
        orm = c.get("_orm_score", 0)
        status = c.get("_orm_status", "?")
        mps_val = c.get("_move_potential_score", 0)
        sig_cnt = len(c.get("signals", [])) if isinstance(c.get("signals"), list) else 0
        conv = c.get("_conviction_score", 0)
        fcts = c.get("_orm_factors", {})
        top_factors = sorted(fcts.items(), key=lambda x: x[1], reverse=True)[:3]
        factor_str = " ".join(f"{k[:3]}={v:.2f}" for k, v in top_factors)
        logger.info(
            f"    #{i:2d} {c['symbol']:6s} "
            f"final={c['score']:.3f} conv={conv:.3f} "
            f"(base={base:.3f} orm={orm:.3f} [{status}] mps={mps_val:.2f} sig={sig_cnt}) "
            f"[{factor_str}]"
        )

    # ── THETA AWARENESS FLAGS (FEB 16 v2) ─────────────────────
    # Attach theta warning to every pick so email/telegram/X can show it.
    if _theta_warning:
        for c in candidates:
            c["_theta_warning"] = _theta_warning
            c["_theta_gap_days"] = _theta_gap_days
            c["_theta_prefer_dte"] = 7 if _theta_gap_days >= 4 else 5

    return candidates


# ══════════════════════════════════════════════════════════════════════════
# REGIME-AWARE SHADOW LOGGING + HARD BLOCK — FEB 16 v3
# ══════════════════════════════════════════════════════════════════════════
# Tasks 2-5 from institutional review feedback.
# ══════════════════════════════════════════════════════════════════════════

def _extract_pick_features(candidate: Dict[str, Any],
                           flow_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a STABLE boolean/float feature dict from a moonshot candidate.

    Returns a fixed schema — no string matching downstream.
    Uses only data that is already loaded in _enrich_moonshots_with_orm.

    Schema:
        iv_inverted: bool
        neg_gex_explosive: bool
        dark_pool_massive: bool
        institutional_accumulation: bool
        call_buying: bool
        support_test: bool
        oversold: bool
        momentum: bool
        vanna_crush: bool
        sweep_urgency: bool
        bullish_flow: bool  (UW call premium > 60%)
        bearish_flow: bool  (UW put premium > 60%)
        call_pct: float     (0.0-1.0)
        mps: float
        signal_count: int
    """
    sym = candidate.get("symbol", "")

    # ── Signals (from candidate's signals list) ─────────────
    signals = candidate.get("signals", [])
    sig_set: set = set()
    if isinstance(signals, list):
        sig_set = {str(s).lower() for s in signals}

    # ── Catalysts (from MWS forecast, already attached to candidate) ──
    catalysts = candidate.get("catalysts", [])
    if isinstance(catalysts, list):
        cat_str = " ".join(str(c) for c in catalysts).lower()
    else:
        cat_str = str(catalysts).lower()

    # ── UW flow data (already loaded) ───────────────────────
    call_prem = 0.0
    put_prem = 0.0
    sym_flow = flow_data.get(sym, []) if isinstance(flow_data, dict) else []
    if isinstance(sym_flow, list):
        for trade in sym_flow:
            if isinstance(trade, dict):
                prem = trade.get("premium", 0) or 0
                if trade.get("put_call") == "C":
                    call_prem += prem
                elif trade.get("put_call") == "P":
                    put_prem += prem
    total_prem = call_prem + put_prem
    call_pct = call_prem / total_prem if total_prem > 0 else 0.50

    return {
        "iv_inverted": any("iv_inverted" in s for s in sig_set),
        "neg_gex_explosive": any("neg_gex_explosive" in s for s in sig_set),
        "dark_pool_massive": any("dark_pool_massive" in s for s in sig_set),
        "institutional_accumulation": "institutional accumulation" in cat_str,
        "call_buying": "call buying" in cat_str or "positive gex" in cat_str,
        "support_test": any("support" in s for s in sig_set),
        "oversold": any("oversold" in s for s in sig_set),
        "momentum": any("momentum" in s for s in sig_set),
        "vanna_crush": any("vanna_crush" in s for s in sig_set),
        "sweep_urgency": any("sweep" in s for s in sig_set),
        "bullish_flow": call_pct > 0.60,
        "bearish_flow": call_pct < 0.40,
        "call_pct": round(call_pct, 3),
        "mps": candidate.get("_move_potential_score", 0) or 0,
        "signal_count": len(signals) if isinstance(signals, list) else 0,
    }


def _get_regime_with_timestamp() -> Dict[str, Any]:
    """
    Get current market regime from MarketDirectionPredictor.

    Returns regime label + composite score + the timestamp at which
    the regime was computed (for leakage auditing).

    If MarketDirectionPredictor is unavailable, falls back to the
    PutsEngine market_direction.json file.
    """
    regime_result = {
        "regime_label": "UNKNOWN",
        "regime_score": 0.0,
        "regime_asof_timestamp": datetime.now().isoformat(),
        "regime_source": "none",
    }

    # Try 1: MarketDirectionPredictor (preferred — full 10-signal fusion)
    try:
        from analysis.market_direction_predictor import get_market_direction_for_scan
        prediction = get_market_direction_for_scan(session_label="AM")
        if prediction:
            composite = prediction.get("composite_score", 0)
            # Classify using same thresholds as analysis
            if composite >= 0.30:
                label = "STRONG_BULL"
            elif composite >= 0.10:
                label = "LEAN_BULL"
            elif composite <= -0.30:
                label = "STRONG_BEAR"
            elif composite <= -0.10:
                label = "LEAN_BEAR"
            else:
                label = "NEUTRAL"

            regime_result.update({
                "regime_label": label,
                "regime_score": round(composite, 4),
                "regime_asof_timestamp": prediction.get("timestamp",
                                                        datetime.now().isoformat()),
                "regime_source": "MarketDirectionPredictor",
                "regime_direction": prediction.get("direction", "N/A"),
                "regime_confidence": prediction.get("confidence", "N/A"),
            })
            return regime_result
    except Exception as e:
        logger.debug(f"  Regime: MarketDirectionPredictor unavailable ({e})")

    # Try 2: PutsEngine market_direction.json (fallback)
    try:
        md_path = Path.home() / "PutsEngine" / "logs" / "market_direction.json"
        if md_path.exists():
            with open(md_path) as f:
                md = json.load(f)
            regime_result.update({
                "regime_label": md.get("regime", "UNKNOWN"),
                "regime_score": md.get("regime_score", 0),
                "regime_asof_timestamp": md.get("timestamp",
                                                datetime.now().isoformat()),
                "regime_source": "PutsEngine_market_direction",
            })
    except Exception:
        pass

    return regime_result


def _apply_regime_shadow_and_hard_block(
    candidates: List[Dict[str, Any]],
    flow_data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Policy B v4 Regime Gate — hard block for 80% WR target.

    Rules (based on Feb 9–13 forward backtest):
      - STRONG_BEAR / LEAN_BEAR → BLOCK ALL moonshots (11.1% WR)
      - NEUTRAL → BLOCK ALL moonshots (no edge without regime tailwind)
      - STRONG_BULL / LEAN_BULL → ALLOW only with call_buying + score ≥ 0.70
      - Bearish UW flow (call_pct < 40%) → BLOCK in ANY regime

    Fields added to each candidate:
      _regime_label, _regime_score, _regime_asof_timestamp,
      _regime_gate_decision, _regime_gate_reasons,
      _features (full feature dict)
    """
    # Get regime (with timestamp)
    regime_info = _get_regime_with_timestamp()
    regime_label = regime_info["regime_label"]
    regime_score = regime_info["regime_score"]

    logger.info(
        f"  🌤️ Regime: {regime_label} (score={regime_score:+.3f}, "
        f"source={regime_info['regime_source']}, "
        f"as_of={regime_info['regime_asof_timestamp'][:19]})"
    )

    bear_regimes = {"STRONG_BEAR", "LEAN_BEAR"}
    hard_blocked = []
    passed = []

    for c in candidates:
        sym = c.get("symbol", "")

        # ── Task 5: Stable feature extraction ────────────────
        features = _extract_pick_features(c, flow_data)
        c["_features"] = features

        # ── Task 2: Regime timestamp on every pick ───────────
        c["_regime_label"] = regime_label
        c["_regime_score"] = regime_score
        c["_regime_asof_timestamp"] = regime_info["regime_asof_timestamp"]

        # ── Task 3: Shadow Smart Gate decision (logged, not enforced) ──
        premium_count = sum([
            features["iv_inverted"],
            features["call_buying"],
            features["dark_pool_massive"],
            features["neg_gex_explosive"],
        ])
        
        base_score = c.get("_base_score", c.get("score", 0))

        gate_decision = "ALLOW"
        gate_reasons = []

        # ── POLICY B v4: Regime-Aligned Hard Gate (Target: 80% WR) ──
        # Based on Feb 9-13 forward backtest (institutional analysis):
        #   - STRONG_BEAR moonshots: 1/9 = 11.1% WR → BLOCK ALL
        #   - LEAN_BEAR moonshots: 0% WR historically → BLOCK ALL
        #   - NEUTRAL moonshots: No edge → BLOCK ALL
        #   - STRONG_BULL/LEAN_BULL: Allow ONLY with call_buying + score ≥ 0.70
        #   - Bearish flow: Block in ALL regimes
        #
        # KEY INSIGHT: Even with iv_inverted + call_buying + dark_pool + score 0.93,
        # moonshots in STRONG_BEAR fail 89% of the time. The market direction
        # overwhelms individual stock signals. Deploy moonshots ONLY in bull markets.
        
        if regime_label in ("STRONG_BEAR", "LEAN_BEAR"):
            # v4: TOTAL HARD BLOCK in bear regimes
            # Evidence: 1/9 = 11.1% WR in STRONG_BEAR (even with premium signals)
            # Single winner (NET +11.8%) cannot justify 8 losers
            gate_decision = "HARD_BLOCK"
            gate_reasons.append(
                f"{regime_label}: ALL moonshots blocked (11.1% WR in bear — "
                f"market direction overwhelms individual signals)"
            )
        elif regime_label == "NEUTRAL":
            # v4: BLOCK in neutral (no reliable edge without regime tailwind)
            gate_decision = "HARD_BLOCK"
            gate_reasons.append(
                "NEUTRAL: Moonshots blocked (no edge without bullish regime)"
            )
        elif regime_label in ("STRONG_BULL", "LEAN_BULL"):
            # Allow ONLY with call_buying confirmation
            if not features["call_buying"]:
                gate_decision = "HARD_BLOCK"
                gate_reasons.append(
                    f"{regime_label} requires call_buying (all bull winners had it)"
                )
            elif base_score < 0.70:
                gate_decision = "HARD_BLOCK"
                gate_reasons.append(
                    f"{regime_label} + call_buying but score={base_score:.2f} < 0.70"
                )
            else:
                gate_reasons.append(f"{regime_label} + call_buying + score≥0.70 — allow")
        else:
            # UNKNOWN regime — block to be safe
            gate_decision = "HARD_BLOCK"
            gate_reasons.append(f"UNKNOWN regime '{regime_label}' — blocked for safety")

        # ── Additional v4 override: bearish flow in bull regimes ──
        # (Bear/neutral already blocked above; this catches edge case
        # where call_buying=True but overall flow is bearish in bull regime)
        if gate_decision == "ALLOW" and features["bearish_flow"]:
            gate_decision = "HARD_BLOCK"
            gate_reasons.append(
                f"Bearish UW flow override (call_pct={features['call_pct']:.0%}) "
                f"in {regime_label}"
            )

        c["_regime_gate_decision"] = gate_decision
        c["_regime_gate_reasons"] = gate_reasons

        # ── Apply gate decision ──
        if gate_decision == "HARD_BLOCK":
            hard_blocked.append(c)
            logger.info(
                f"  🔴 HARD BLOCK: {sym} — {gate_reasons[0][:100]} → removed"
            )
            continue

        passed.append(c)

    if hard_blocked:
        logger.info(
            f"  🛡️ Policy B v4 Regime Gate: {len(hard_blocked)} moonshots blocked, "
            f"{len(passed)} survive (regime={regime_label})"
        )

    # Save shadow artifact for post-session analysis
    try:
        shadow_path = Path(os.environ.get("META_ENGINE_OUTPUT",
                                          str(Path(__file__).parent.parent / "output")))
        shadow_file = shadow_path / f"regime_shadow_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        shadow_data = {
            "timestamp": datetime.now().isoformat(),
            "regime": regime_info,
            "candidates_before": len(candidates),
            "hard_blocked": [{
                "symbol": c["symbol"],
                "features": c.get("_features", {}),
                "gate_reasons": c.get("_regime_gate_reasons", []),
            } for c in hard_blocked],
            "passed": [{
                "symbol": c["symbol"],
                "gate_decision": c.get("_regime_gate_decision", ""),
                "score": c.get("score", 0),
            } for c in passed],
        }
        with open(shadow_file, "w") as f:
            json.dump(shadow_data, f, indent=2, default=str)
        logger.info(f"  💾 Regime shadow artifact: {shadow_file}")
    except Exception as e:
        logger.debug(f"  Regime shadow save failed: {e}")

    return passed


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
