"""
🚀 Gap-Up Detector Module
===========================
Runs at 9:21 AM (pre-market) and 9:35 AM (open), producing a separate
"🚀 Gap-Up Alerts" section in email/Telegram alongside existing Top 10 picks.

Combines 5 independent data sources — ALL already available:
  1. MWS Forecast: bullish_probability > 55% AND catalyst "Heavy call buying / +GEX"
  2. Sector Sympathy: 3+ stocks in same sector with bullish signals → flag sector
  3. Predictive Signals: Recurring bullish signals across multiple scans
  4. UW Options Flow: Call/Put premium ratio > 2x with short-term call accumulation
  5. Pre-Market Price Gap: Compare pre-market bid/ask vs previous close (Polygon)

Scoring:
  gap_score = (0.30 × call_buying_flag)
            + (0.25 × sector_sympathy_flag)
            + (0.20 × predictive_signal_flag)
            + (0.15 × uw_flow_bullish_flag)
            + (0.10 × premarket_gap_flag)

  Require gap_score ≥ 0.40 (any 2+ signals) to make the gap-up alert list.

Report format:
  🚀 GAP-UP ALERTS (Same-Day Plays)
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ⚡ SECTOR ALERT: Bitcoin Proxies (5 stocks bullish)
    MSTR — 5 signals | Heavy call buying | Support test
    COIN — 3 signals | Heavy call buying | Sector sympathy
  ⚡ INDIVIDUAL:
    RIVN — 3 signals | Support test | EV sector | UW flow

CRITICAL: This module does NOT modify PutsEngine, TradeNova, or any
existing engine logic.  It READS cached JSON files and Polygon API
to produce an additive section for the daily report.
"""

import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

TRADENOVA_DATA = Path.home() / "TradeNova" / "data"
PUTSENGINE_PATH = Path.home() / "PutsEngine"

# ─── Scoring weights ────────────────────────────────────────────────
W_CALL_BUYING = 0.30
W_SECTOR_SYMPATHY = 0.25
W_PREDICTIVE_SIGNAL = 0.20
W_UW_FLOW = 0.15
W_PREMARKET_GAP = 0.10

MIN_GAP_SCORE = 0.40  # Require 2+ signals to qualify

# ─── Sector groupings (supplements PutsEngine UNIVERSE_SECTORS) ─────
# Key manual groupings for gap-up sympathy detection
_CRYPTO_PROXIES = {"MSTR", "COIN", "MARA", "RIOT", "CLSK", "HUT", "BITF", "CIFR", "WULF"}
_QUANTUM_COMPUTING = {"IONQ", "RGTI", "QBTS"}
_EV_MAKERS = {"RIVN", "LCID", "NIO", "XPEV", "LI"}
_BIOTECH = {"MRNA", "NVAX", "CRSP", "NTLA", "HIMS", "SAVA", "IBRX", "VKTX", "DNA", "IOVA", "MDGL"}
_FINTECH = {"HOOD", "SOFI", "AFRM", "SQ", "UPST", "PYPL"}
_SEMICONDUCTORS = {"NVDA", "AMD", "SMCI", "ARM", "MU", "AVGO", "MRVL", "TSM", "ASML", "LRCX", "KLAC", "INTC", "QCOM"}
_MEGA_TECH = {"META", "NFLX", "AMZN", "GOOG", "MSFT", "AAPL"}
_MEME_VOLATILE = {"GME", "AMC", "DJT", "RDDT"}

_SECTOR_GROUPS: Dict[str, Set[str]] = {
    "Bitcoin Proxies": _CRYPTO_PROXIES,
    "Quantum Computing": _QUANTUM_COMPUTING,
    "EV Makers": _EV_MAKERS,
    "Biotech": _BIOTECH,
    "Fintech": _FINTECH,
    "Semiconductors": _SEMICONDUCTORS,
    "Mega-Cap Tech": _MEGA_TECH,
    "Meme / High-Vol": _MEME_VOLATILE,
}

# Reverse lookup: ticker → sector name
_TICKER_TO_SECTOR: Dict[str, str] = {}
for _sec_name, _tickers in _SECTOR_GROUPS.items():
    for _t in _tickers:
        _TICKER_TO_SECTOR[_t] = _sec_name


# ═══════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════

def detect_gap_ups(
    polygon_api_key: str = "",
    static_universe: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    Main entry point — detect gap-up candidates for same-day plays.

    Returns:
        {
            "candidates": [
                {
                    "symbol": "MSTR",
                    "gap_score": 0.75,
                    "signals": ["Heavy call buying", "Sector sympathy", ...],
                    "signal_count": 5,
                    "sector": "Bitcoin Proxies",
                    "premarket_gap_pct": 3.2,
                    ...
                },
                ...
            ],
            "sector_alerts": {
                "Bitcoin Proxies": {"count": 5, "symbols": [...]},
                ...
            },
            "theta_note": "⚠️ THETA NOTE: ...",
            "timestamp": "...",
        }
    """
    logger.info("🚀 Gap-Up Detector: Scanning for same-day plays...")

    # Load all data sources
    mws_data = _load_mws_call_buying_signals()
    sector_hot = _load_sector_sympathy_signals()
    pred_data = _load_predictive_recurrence_signals()
    uw_flow_data = _load_uw_flow_bullish_signals()
    premarket_data = _load_premarket_gaps(polygon_api_key)

    # Get universe gate
    universe = static_universe or _get_static_universe()

    # Combine all tickers from all sources
    all_tickers: Set[str] = set()
    all_tickers.update(mws_data.keys())
    all_tickers.update(uw_flow_data.keys())
    all_tickers.update(pred_data.keys())
    all_tickers.update(premarket_data.keys())
    for sec_info in sector_hot.values():
        all_tickers.update(sec_info.get("symbols", []))

    # FEB 16 v5.2: ALSO add ALL pre-market gap tickers (>3%) from the
    # FULL universe — not just tickers already in other sources.
    # This catches VKTX/CVNA-type movers that gap up with zero prior signals.
    if premarket_data:
        for sym, pm_info in premarket_data.items():
            gap_pct = pm_info.get("gap_pct", 0)
            if abs(gap_pct) >= 3.0:
                all_tickers.add(sym)

    # Filter to universe if available
    if universe:
        all_tickers = all_tickers & universe

    logger.info(
        f"  📡 Data sources: MWS={len(mws_data)} | "
        f"Sector={len(sector_hot)} hot sectors | "
        f"Predictive={len(pred_data)} | "
        f"UW Flow={len(uw_flow_data)} | "
        f"PreMarket={len(premarket_data)} | "
        f"Universe tickers: {len(all_tickers)}"
    )

    # Score each ticker
    candidates = []
    for sym in sorted(all_tickers):
        signals = []
        score_parts = {}

        # 1. MWS Heavy Call Buying / +GEX
        if sym in mws_data:
            mws_info = mws_data[sym]
            score_parts["call_buying"] = W_CALL_BUYING
            signals.append("Heavy call buying")
            if mws_info.get("bullish_probability", 0) > 65:
                signals.append(f"BP={mws_info['bullish_probability']:.0f}%")
            for cat in mws_info.get("catalysts", []):
                if "positive GEX" in str(cat):
                    signals.append("+GEX")
                    break

        # 2. Sector Sympathy
        ticker_sector = _TICKER_TO_SECTOR.get(sym, "")
        if ticker_sector and ticker_sector in sector_hot:
            sec_info = sector_hot[ticker_sector]
            score_parts["sector_sympathy"] = W_SECTOR_SYMPATHY
            signals.append(f"Sector sympathy ({ticker_sector})")

        # 3. Predictive Signal Recurrence
        if sym in pred_data:
            pred_info = pred_data[sym]
            score_parts["predictive_signal"] = W_PREDICTIVE_SIGNAL
            recur = pred_info.get("recurrence_count", 1)
            cat = pred_info.get("category", "")
            sig_type = pred_info.get("signal_type", "")
            if recur >= 3:
                signals.append(f"{sig_type or cat} ({recur}x recurring)")
            else:
                signals.append(sig_type or cat or "Predictive signal")

        # 4. UW Options Flow Bullish
        if sym in uw_flow_data:
            uw_info = uw_flow_data[sym]
            score_parts["uw_flow"] = W_UW_FLOW
            ratio = uw_info.get("call_put_ratio", 0)
            signals.append(f"UW flow (C/P={ratio:.1f}x)")

        # 5. Pre-Market Gap
        if sym in premarket_data:
            pm_info = premarket_data[sym]
            gap_pct = pm_info.get("gap_pct", 0)
            if gap_pct >= 2.0:
                score_parts["premarket_gap"] = W_PREMARKET_GAP
                if gap_pct >= 5.0:
                    signals.append(f"🔥🔥 MAJOR pre-market gap +{gap_pct:.1f}%")
                else:
                    signals.append(f"🔥 Pre-market gap +{gap_pct:.1f}%")

        # Compute gap score
        gap_score = sum(score_parts.values())

        if gap_score >= MIN_GAP_SCORE:
            candidates.append({
                "symbol": sym,
                "gap_score": gap_score,
                "signals": signals,
                "signal_count": len(signals),
                "sector": ticker_sector,
                "score_parts": score_parts,
                "premarket_gap_pct": premarket_data.get(sym, {}).get("gap_pct", 0),
                "bullish_probability": mws_data.get(sym, {}).get("bullish_probability", 0),
                "call_put_ratio": uw_flow_data.get(sym, {}).get("call_put_ratio", 0),
                "recurrence_count": pred_data.get(sym, {}).get("recurrence_count", 0),
            })

    # Sort by gap_score descending
    candidates.sort(key=lambda x: (-x["gap_score"], -x["signal_count"]))

    # Build sector alerts
    sector_alerts = {}
    for c in candidates:
        sec = c.get("sector", "")
        if sec:
            if sec not in sector_alerts:
                sector_alerts[sec] = {"count": 0, "symbols": []}
            sector_alerts[sec]["count"] += 1
            sector_alerts[sec]["symbols"].append(c["symbol"])

    # Theta note
    theta_note = _get_theta_note()

    logger.info(
        f"🚀 Gap-Up Detector: {len(candidates)} candidates passed "
        f"(score ≥ {MIN_GAP_SCORE}), {len(sector_alerts)} sector alerts"
    )
    for i, c in enumerate(candidates[:15], 1):
        logger.info(
            f"  #{i:2d} {c['symbol']:6s} score={c['gap_score']:.2f} "
            f"sigs={c['signal_count']} [{', '.join(c['signals'][:3])}]"
        )

    return {
        "candidates": candidates,
        "sector_alerts": sector_alerts,
        "theta_note": theta_note,
        "timestamp": datetime.now().isoformat(),
        "sources_loaded": {
            "mws_forecast": len(mws_data),
            "sector_sympathy": len(sector_hot),
            "predictive_signals": len(pred_data),
            "uw_flow": len(uw_flow_data),
            "premarket_gaps": len(premarket_data),
        },
    }


def format_gap_up_report(gap_data: Dict[str, Any]) -> str:
    """
    Format gap-up alerts as a human-readable text block for
    email/Telegram/X integration.

    Returns a styled multi-line string.
    """
    candidates = gap_data.get("candidates", [])
    sector_alerts = gap_data.get("sector_alerts", {})
    theta_note = gap_data.get("theta_note", "")

    if not candidates:
        return ""

    lines = [
        "🚀 GAP-UP ALERTS (Same-Day Plays)",
        "━" * 36,
    ]

    # Group candidates by sector for sector alerts
    sector_candidates: Dict[str, List[Dict]] = defaultdict(list)
    individual_candidates = []

    for c in candidates:
        sec = c.get("sector", "")
        if sec and sec in sector_alerts and sector_alerts[sec]["count"] >= 2:
            sector_candidates[sec].append(c)
        else:
            individual_candidates.append(c)

    # Sector alerts first (sorted by count descending)
    for sec_name in sorted(
        sector_candidates.keys(),
        key=lambda s: -sector_alerts.get(s, {}).get("count", 0),
    ):
        members = sector_candidates[sec_name]
        lines.append(f"")
        lines.append(f"⚡ SECTOR ALERT: {sec_name} ({len(members)} stocks bullish)")
        for c in members:
            sig_str = " | ".join(c["signals"][:4])
            lines.append(f"  {c['symbol']:6s} — {c['signal_count']} signals | {sig_str}")

    # Individual candidates
    if individual_candidates:
        lines.append("")
        lines.append("⚡ INDIVIDUAL:")
        for c in individual_candidates[:10]:
            sig_str = " | ".join(c["signals"][:4])
            lines.append(f"  {c['symbol']:6s} — {c['signal_count']} signals | {sig_str}")

    # Theta note
    if theta_note:
        lines.append("")
        lines.append(theta_note)

    return "\n".join(lines)


def format_gap_up_html(gap_data: Dict[str, Any]) -> str:
    """
    Format gap-up alerts as HTML for email body inclusion.
    """
    candidates = gap_data.get("candidates", [])
    sector_alerts = gap_data.get("sector_alerts", {})
    theta_note = gap_data.get("theta_note", "")

    if not candidates:
        return ""

    html = [
        '<div style="background:#1a1a2e; padding:16px; border-radius:8px; '
        'margin:20px 0; font-family:monospace;">',
        '<h3 style="color:#00ff88; margin:0 0 12px 0;">🚀 GAP-UP ALERTS (Same-Day Plays)</h3>',
        '<hr style="border-color:#333;">',
    ]

    # Group by sector
    sector_candidates: Dict[str, List[Dict]] = defaultdict(list)
    individual_candidates = []
    for c in candidates:
        sec = c.get("sector", "")
        if sec and sec in sector_alerts and sector_alerts[sec]["count"] >= 2:
            sector_candidates[sec].append(c)
        else:
            individual_candidates.append(c)

    for sec_name in sorted(
        sector_candidates.keys(),
        key=lambda s: -sector_alerts.get(s, {}).get("count", 0),
    ):
        members = sector_candidates[sec_name]
        html.append(
            f'<p style="color:#ffd700; font-weight:bold; margin:12px 0 4px 0;">'
            f'⚡ SECTOR ALERT: {sec_name} ({len(members)} stocks bullish)</p>'
        )
        for c in members:
            sig_str = " | ".join(c["signals"][:4])
            html.append(
                f'<p style="color:#e0e0e0; margin:2px 0 2px 16px;">'
                f'<b>{c["symbol"]}</b> — {c["signal_count"]} signals | {sig_str}</p>'
            )

    if individual_candidates:
        html.append(
            '<p style="color:#ffd700; font-weight:bold; margin:12px 0 4px 0;">'
            '⚡ INDIVIDUAL:</p>'
        )
        for c in individual_candidates[:10]:
            sig_str = " | ".join(c["signals"][:4])
            html.append(
                f'<p style="color:#e0e0e0; margin:2px 0 2px 16px;">'
                f'<b>{c["symbol"]}</b> — {c["signal_count"]} signals | {sig_str}</p>'
            )

    if theta_note:
        html.append(
            f'<p style="color:#ff9800; margin:12px 0 0 0;">{theta_note}</p>'
        )

    html.append("</div>")
    return "\n".join(html)


# ═══════════════════════════════════════════════════════════════════
# DATA SOURCE LOADERS
# ═══════════════════════════════════════════════════════════════════

def _load_mws_call_buying_signals() -> Dict[str, Dict[str, Any]]:
    """
    Source 1: MWS 7-Sensor Forecast — "Heavy call buying / +GEX" catalysts.

    Loads tomorrows_forecast.json and returns tickers where:
      - bullish_probability > 55%
      - catalysts contains "Heavy call buying" or "positive GEX"

    Returns: {symbol: {bullish_probability, catalysts, action, mws_score}}
    """
    result = {}
    try:
        forecast_file = TRADENOVA_DATA / "tomorrows_forecast.json"
        if not forecast_file.exists():
            logger.debug("  Gap-Up MWS: tomorrows_forecast.json not found")
            return result

        with open(forecast_file) as f:
            data = json.load(f)

        # Check freshness (≤ 3 days)
        generated = data.get("generated_at", "")
        if generated:
            try:
                gen_dt = datetime.fromisoformat(generated.replace("Z", "+00:00"))
                age_days = (datetime.now() - gen_dt.replace(tzinfo=None)).days
                if age_days > 3:
                    logger.info(f"  Gap-Up MWS: forecast {age_days}d old — skipping")
                    return result
            except (ValueError, TypeError):
                pass

        for fc in data.get("forecasts", []):
            sym = fc.get("symbol", "")
            bp = fc.get("bullish_probability", 0) or 0
            catalysts = fc.get("catalysts", [])
            if not isinstance(catalysts, list):
                catalysts = [str(catalysts)] if catalysts else []

            # Check for heavy call buying / positive GEX in catalysts
            has_call_buying = any(
                "heavy call buying" in str(c).lower() or "positive gex" in str(c).lower()
                for c in catalysts
            )

            if bp > 55 and has_call_buying:
                result[sym] = {
                    "bullish_probability": bp,
                    "catalysts": catalysts,
                    "action": fc.get("action", ""),
                    "mws_score": fc.get("mws_score", 0),
                    "expected_move_pct": fc.get("expected_move_pct", 0),
                    "sector": fc.get("sector", ""),
                }

        logger.info(f"  Gap-Up MWS: {len(result)} stocks with heavy call buying & BP > 55%")

    except Exception as e:
        logger.warning(f"  Gap-Up MWS: failed — {e}")
    return result


def _load_sector_sympathy_signals() -> Dict[str, Dict[str, Any]]:
    """
    Source 2: Sector Sympathy — detect hot sectors with 3+ bullish stocks.

    Combines:
      a) sector_sympathy_alerts.json (TradeNova sector leader/alert data)
      b) Internal sector groupings (_SECTOR_GROUPS)
      c) MWS forecast sector data

    Returns: {sector_name: {count, symbols, leader}}
    """
    # ── Step 1: Load sector_sympathy_alerts.json ──
    sector_bullish_counts: Dict[str, Set[str]] = defaultdict(set)

    try:
        alerts_file = TRADENOVA_DATA / "sector_sympathy_alerts.json"
        if alerts_file.exists():
            with open(alerts_file) as f:
                sa_data = json.load(f)

            leaders = sa_data.get("leaders", {})
            alerts = sa_data.get("alerts", {})

            # Leaders are keyed like "MSTR_CRYPTO" — extract sector
            for key, leader_info in leaders.items():
                if not isinstance(leader_info, dict):
                    continue
                sector_id = leader_info.get("sector_id", "")
                sector_name = leader_info.get("sector_name", sector_id)
                sym = leader_info.get("symbol", "")
                appearances = leader_info.get("appearances_48h", 0) or 0
                if sym and appearances >= 3:
                    sector_bullish_counts[sector_name].add(sym)

            # Alerts map individual tickers to their sector leader
            for sym, alert_info in alerts.items():
                if not isinstance(alert_info, dict):
                    continue
                sector_name = alert_info.get("sector_name", "")
                sympathy_score = alert_info.get("sympathy_score", 0)
                # Even if sympathy_score is 0, the fact that the alert exists
                # means the sector detection engine flagged this ticker
                if sector_name and sym:
                    sector_bullish_counts[sector_name].add(sym)

            logger.debug(f"  Gap-Up Sector: loaded {len(leaders)} leaders, {len(alerts)} alerts")

    except Exception as e:
        logger.debug(f"  Gap-Up Sector: sector_sympathy_alerts.json failed — {e}")

    # ── Step 2: Supplement with MWS forecast bullish tickers ──
    try:
        forecast_file = TRADENOVA_DATA / "tomorrows_forecast.json"
        if forecast_file.exists():
            with open(forecast_file) as f:
                fc_data = json.load(f)
            for fc in fc_data.get("forecasts", []):
                sym = fc.get("symbol", "")
                bp = fc.get("bullish_probability", 0) or 0
                if sym and bp > 55:
                    # Use internal sector grouping
                    sec = _TICKER_TO_SECTOR.get(sym, "")
                    if sec:
                        sector_bullish_counts[sec].add(sym)
    except Exception:
        pass

    # ── Step 3: Filter to hot sectors (3+ stocks) ──
    hot_sectors = {}
    for sec_name, members in sector_bullish_counts.items():
        if len(members) >= 3:
            hot_sectors[sec_name] = {
                "count": len(members),
                "symbols": sorted(members),
            }

    if hot_sectors:
        for sec, info in sorted(hot_sectors.items(), key=lambda x: -x[1]["count"]):
            logger.info(
                f"  Gap-Up Sector: 🔥 {sec} — {info['count']} stocks "
                f"({', '.join(info['symbols'][:5])})"
            )

    return hot_sectors


def _load_predictive_recurrence_signals() -> Dict[str, Dict[str, Any]]:
    """
    Source 3: Predictive Signal Recurrence — stocks that appear in
    multiple scans with the same signal type.

    Loads predictive_signals_latest.json and eod_interval_picks.json.
    Tracks which symbols appear repeatedly (3+ times across scans).

    Returns: {symbol: {recurrence_count, category, signal_type, score}}
    """
    result = {}

    # ── Source A: eod_interval_picks.json (interval persistence) ──
    try:
        eod_file = TRADENOVA_DATA / "eod_interval_picks.json"
        if eod_file.exists():
            with open(eod_file) as f:
                eod_data = json.load(f)

            intervals = eod_data.get("intervals", {})
            sym_interval_count: Dict[str, int] = {}
            sym_best_score: Dict[str, float] = {}
            for _interval_key, interval_data in intervals.items():
                for pick in interval_data.get("picks", []):
                    sym = pick.get("symbol", "")
                    if sym:
                        sym_interval_count[sym] = sym_interval_count.get(sym, 0) + 1
                        score = pick.get("score", 0) or 0
                        if score > sym_best_score.get(sym, 0):
                            sym_best_score[sym] = score

            for sym, count in sym_interval_count.items():
                if count >= 3:
                    result[sym] = {
                        "recurrence_count": count,
                        "category": "interval_persistence",
                        "signal_type": "recurring_scan",
                        "score": sym_best_score.get(sym, 0),
                    }

    except Exception as e:
        logger.debug(f"  Gap-Up Predictive: eod_interval_picks failed — {e}")

    # ── Source B: predictive_signals_latest.json (signal recurrence) ──
    try:
        pred_file = TRADENOVA_DATA / "predictive_signals_latest.json"
        if pred_file.exists():
            with open(pred_file) as f:
                pred_data = json.load(f)

            sig_count: Dict[str, int] = {}
            sig_info: Dict[str, Dict] = {}
            for sig in pred_data.get("signals", []):
                sym = sig.get("symbol", "")
                direction = sig.get("direction", "")
                if not sym or direction == "bearish":
                    continue
                sig_count[sym] = sig_count.get(sym, 0) + 1
                # Keep best info
                if sym not in sig_info or (sig.get("score", 0) or 0) > (sig_info[sym].get("score", 0) or 0):
                    sig_info[sym] = {
                        "category": sig.get("category", ""),
                        "signal_type": sig.get("signal_type", ""),
                        "score": sig.get("score", 0),
                    }

            for sym, count in sig_count.items():
                existing = result.get(sym)
                if existing:
                    # Merge: take max recurrence
                    existing["recurrence_count"] = max(existing["recurrence_count"], count)
                    if sym in sig_info:
                        existing["category"] = sig_info[sym].get("category", existing.get("category", ""))
                        existing["signal_type"] = sig_info[sym].get("signal_type", existing.get("signal_type", ""))
                elif count >= 2:  # Lower threshold for predictive signals (they're already filtered)
                    info = sig_info.get(sym, {})
                    result[sym] = {
                        "recurrence_count": count,
                        "category": info.get("category", ""),
                        "signal_type": info.get("signal_type", ""),
                        "score": info.get("score", 0),
                    }

    except Exception as e:
        logger.debug(f"  Gap-Up Predictive: predictive_signals failed — {e}")

    logger.info(f"  Gap-Up Predictive: {len(result)} recurring bullish signals")
    return result


def _load_uw_flow_bullish_signals() -> Dict[str, Dict[str, Any]]:
    """
    Source 4: UW Options Flow — Call/Put premium ratio > 2x.

    Loads uw_flow_cache.json and computes call vs put premium ratio.
    Flags tickers with heavy call accumulation (ratio > 2x).

    Returns: {symbol: {call_premium, put_premium, call_put_ratio, call_count}}
    """
    result = {}
    try:
        flow_file = TRADENOVA_DATA / "uw_flow_cache.json"
        if not flow_file.exists():
            logger.debug("  Gap-Up UW: uw_flow_cache.json not found")
            return result

        with open(flow_file) as f:
            raw = json.load(f)

        flow_data = raw.get("flow_data", raw)
        if not isinstance(flow_data, dict):
            return result

        for sym, trades in flow_data.items():
            if sym in ("timestamp", "generated_at") or not isinstance(trades, list):
                continue

            call_premium = 0.0
            put_premium = 0.0
            call_count = 0
            short_term_calls = 0  # DTE ≤ 7

            for t in trades:
                prem = float(t.get("premium", 0) or 0)
                pc = t.get("put_call", "")
                dte = int(t.get("dte", 30) or 30)

                if pc == "C":
                    call_premium += prem
                    call_count += 1
                    if dte <= 7:
                        short_term_calls += 1
                elif pc == "P":
                    put_premium += prem

            # Require call/put ratio > 2x AND meaningful call premium
            if put_premium > 0:
                ratio = call_premium / put_premium
            elif call_premium > 0:
                ratio = 10.0  # All calls, no puts
            else:
                ratio = 0

            if ratio >= 2.0 and call_premium >= 50000:
                result[sym] = {
                    "call_premium": call_premium,
                    "put_premium": put_premium,
                    "call_put_ratio": ratio,
                    "call_count": call_count,
                    "short_term_calls": short_term_calls,
                }

        logger.info(f"  Gap-Up UW: {len(result)} stocks with C/P ratio ≥ 2x")

    except Exception as e:
        logger.warning(f"  Gap-Up UW: failed — {e}")
    return result


def _load_premarket_gaps(polygon_api_key: str = "") -> Dict[str, Dict[str, Any]]:
    """
    Source 5: Pre-Market Price Gap Detection via Polygon API.

    Compares current pre-market price vs previous close.
    Flags gaps > 2%.

    Returns: {symbol: {gap_pct, prev_close, premarket_price}}
    """
    result = {}

    if not polygon_api_key:
        # Try to load from environment
        polygon_api_key = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")

    if not polygon_api_key:
        logger.debug("  Gap-Up PreMarket: No Polygon API key — skipping pre-market data")
        return result

    # Load static universe for targeted API calls
    universe = _get_static_universe()
    if not universe:
        return result

    # Only check top volatile tickers (minimize API calls)
    # Use the tickers we already know from other sources
    try:
        import requests

        # Polygon snapshot for all tickers (single API call)
        url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {
            "apiKey": polygon_api_key,
        }

        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            tickers_data = data.get("tickers", [])

            for td_item in tickers_data:
                sym = td_item.get("ticker", "")
                if sym not in universe:
                    continue

                prev_close = td_item.get("prevDay", {}).get("c", 0)
                # Pre-market data from todaysChange
                todays_change_pct = td_item.get("todaysChangePerc", 0)
                current_price = td_item.get("lastTrade", {}).get("p", 0) or td_item.get("min", {}).get("c", 0)

                # Also check pre-market specific data
                pm_open = td_item.get("day", {}).get("o", 0)

                if prev_close > 0 and current_price > 0:
                    gap_pct = ((current_price - prev_close) / prev_close) * 100
                elif prev_close > 0 and pm_open > 0:
                    gap_pct = ((pm_open - prev_close) / prev_close) * 100
                elif todays_change_pct:
                    gap_pct = todays_change_pct
                else:
                    continue

                if gap_pct >= 2.0:
                    result[sym] = {
                        "gap_pct": gap_pct,
                        "prev_close": prev_close,
                        "premarket_price": current_price or pm_open,
                    }

            logger.info(f"  Gap-Up PreMarket: {len(result)} stocks with gap ≥ 2%")
        else:
            logger.debug(
                f"  Gap-Up PreMarket: Polygon snapshot API returned {resp.status_code}"
            )

    except Exception as e:
        logger.warning(f"  Gap-Up PreMarket: Polygon API failed — {e}")

    return result


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def _get_static_universe() -> Set[str]:
    """Load the static universe from PutsEngine."""
    try:
        import sys
        pe_path = str(PUTSENGINE_PATH)
        if pe_path not in sys.path:
            sys.path.insert(0, pe_path)
        from putsengine.config import EngineConfig
        return set(EngineConfig.get_all_tickers())
    except (ImportError, AttributeError):
        logger.debug("  Gap-Up: PutsEngine universe unavailable — no universe filter")
        return set()


def _get_theta_note() -> str:
    """Generate theta awareness note based on current calendar."""
    try:
        from trading.nyse_calendar import (
            calendar_days_to_next_session,
            next_trading_day,
            NYSE_HOLIDAYS,
        )

        today = date.today()
        gap = calendar_days_to_next_session(today)
        nxt = next_trading_day(today)

        if gap >= 4:
            # Find which holiday
            holiday_name = ""
            check = today + timedelta(days=1)
            while check < nxt:
                if check in NYSE_HOLIDAYS:
                    # Simple holiday name lookup
                    month_day = (check.month, check.day)
                    names = {
                        (1, 1): "New Year's Day",
                        (1, 19): "MLK Day",
                        (1, 20): "MLK Day",
                        (2, 15): "Presidents' Day",
                        (2, 16): "Presidents' Day",
                        (2, 17): "Presidents' Day",
                        (7, 3): "Independence Day",
                        (7, 4): "Independence Day",
                        (11, 26): "Thanksgiving",
                        (11, 27): "Thanksgiving",
                        (12, 24): "Christmas Eve",
                        (12, 25): "Christmas",
                    }
                    holiday_name = names.get(month_day, f"holiday ({check})")
                    break
                check += timedelta(days=1)

            return (
                f"⚠️ THETA NOTE: {holiday_name or 'Long weekend'} "
                f"{nxt.strftime('%A')} — {gap}-day gap. "
                f"Prefer same-day plays or DTE ≥ 7"
            )

        elif today.weekday() == 4:  # Friday
            return "⚠️ THETA NOTE: Friday — weekend decay for short DTE. Prefer same-day or DTE ≥ 5"

        return ""

    except Exception:
        return ""
