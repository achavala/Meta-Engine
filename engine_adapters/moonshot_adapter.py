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
from datetime import datetime, timedelta
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
try:
    _pe_path = str(Path.home() / "PutsEngine")
    if _pe_path not in sys.path:
        sys.path.insert(0, _pe_path)
    from putsengine.config import EngineConfig
    for _sector_name, _tickers in EngineConfig.UNIVERSE_SECTORS.items():
        for _t in _tickers:
            _SECTOR_MAP[_t] = _sector_name
    logger.debug(f"Sector map: {len(_SECTOR_MAP)} symbols from PutsEngine")
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


def _apply_sector_momentum_boost(
    candidates: List[Dict[str, Any]],
) -> int:
    """
    Detect hot sectors and boost ALL stocks in those sectors.

    A "hot sector" has 3+ candidates with strong base scores (≥ 0.80),
    indicating a sector-wide catalyst.  Examples:
      • Semiconductor sector with MU, TSM, ON, AVGO, AMD, LRCX, etc.
        all showing strong signals → SNDK (weak individual signal)
        gets lifted by sector sympathy.
      • Healthcare sector after policy announcement → even weaker names
        in the sector benefit from the tailwind.

    The boost is DIFFERENTIAL: stocks that already accumulated large
    individual boosts (convergence, signal quality, MWS) get a smaller
    sector boost because they don't need sector sympathy.  Stocks with
    fewer individual boosts get the maximum sector boost — this is the
    key mechanism that lifts "sympathy plays" like SNDK.

    Sector heat tiers:
      3-4 strong peers  → base boost 0.025 (mild sector sympathy)
      5-7 strong peers  → base boost 0.040 (moderate momentum)
      8-10 strong peers → base boost 0.055 (strong momentum)
      11+ strong peers  → base boost 0.070 (extreme sector-wide rally)

    Returns the number of candidates that received a sector boost.
    """
    if not _SECTOR_MAP and not candidates:
        return 0

    # ── Step 1: Build sector map including forecast sectors ────────
    # PutsEngine UNIVERSE_SECTORS is the primary source (400+ symbols).
    # Supplement with tomorrows_forecast sector field for coverage.
    sector_lookup = dict(_SECTOR_MAP)  # Copy
    for c in candidates:
        sym = c["symbol"]
        fc_sector = c.get("sector", "")
        if sym not in sector_lookup and fc_sector:
            # Map forecast sector names to PutsEngine-style names
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
                sector_lookup[sym] = fc_sector  # Use as-is

    # ── Step 2: Group candidates by sector, count strong peers ─────
    from collections import defaultdict
    sector_candidates: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in candidates:
        sym = c["symbol"]
        sector = sector_lookup.get(sym, "")
        if sector:
            c["_sector"] = sector
            sector_candidates[sector].append(c)

    # A "strong" candidate: base score >= 0.80 (indicating quality signals)
    hot_sectors: Dict[str, int] = {}
    for sector, members in sector_candidates.items():
        strong_count = sum(
            1 for m in members
            if m.get("_base_score", m.get("score", 0)) >= 0.80
        )
        if strong_count >= 3:
            hot_sectors[sector] = strong_count

    if not hot_sectors:
        return 0

    for sector, count in sorted(hot_sectors.items(), key=lambda x: -x[1]):
        logger.debug(
            f"  🔥 Hot sector: {sector} — {count} strong candidates "
            f"({len(sector_candidates[sector])} total)"
        )

    # ── Step 3: Apply differential sector boost ────────────────────
    boosted = 0
    for c in candidates:
        sector = c.get("_sector", "")
        if sector not in hot_sectors:
            continue

        strong_peers = hot_sectors[sector]

        # Base boost scales with sector heat
        if strong_peers >= 11:
            raw_boost = 0.070  # Extreme sector-wide rally
        elif strong_peers >= 8:
            raw_boost = 0.055  # Strong momentum
        elif strong_peers >= 5:
            raw_boost = 0.040  # Moderate momentum
        else:
            raw_boost = 0.025  # Mild sector sympathy

        # Differential: stocks with fewer individual boosts get
        # the FULL sector boost; stocks with large individual boosts
        # get a reduced sector boost (they're already well-scored).
        #
        # Example: SNDK has _post_orm_boost=0.085 (mostly convergence)
        #          → effective_factor ≈ 0.72 → sector_boost = 0.070 × 0.72 = 0.050
        #          MU has _post_orm_boost=0.120 (convergence+signal+MWS)
        #          → effective_factor ≈ 0.36 → sector_boost = 0.070 × 0.36 = 0.025
        existing_boost = c.get("_post_orm_boost", 0)
        # Factor: 1.0 when existing_boost=0, diminishes as boost grows
        # At existing_boost=0.15, factor=0.25 (minimum)
        effective_factor = max(0.25, 1.0 - (existing_boost / 0.20))
        sector_boost = raw_boost * effective_factor

        if sector_boost > 0.005:  # Skip negligible boosts
            c["score"] = min(c["score"] + sector_boost, 1.0)
            c["_sector_boost"] = sector_boost
            c["_sector_heat"] = strong_peers
            # Update total boost tracker
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
    # Enrich ALL candidates so the re-sort is fair.  Without this,
    # un-enriched candidates retain raw base scores and can leapfrog
    # ORM-blended candidates (see FEB-11 tomorrows_forecast bug).
    enrich_count = len(candidates)

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

    # ── Apply POST-ORM quality boosts ────────────────────────────
    # These boosts are applied AFTER ORM blending so they're not
    # wasted on base scores that were already at 1.0.  They capture
    # cross-source intelligence that the raw ORM can't measure.
    convergence_applied = 0
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

        if boost > 0:
            c["score"] = min(c["score"] + boost, 1.0)
            c["_post_orm_boost"] = boost

    if convergence_applied:
        logger.info(
            f"  📊 Post-ORM quality boosts: {convergence_applied} symbols "
            f"with convergence + signal/MWS/target boosts applied"
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
