"""
Smart Money Scanner v3 — Multi-Source Predictive Future Mover Detection

Backtested against 104 tickers × 2 weeks (Feb 3-18, 2026).
v2 backtest results: 45% bullish hit rate, 26% bearish hit rate, 50% wrong bearish.

Root cause of v2 failures:
  - UW flow is REACTIVE: after a stock drops -10%, flow shows 95% puts because
    retail/momentum traders pile in AFTER the move. This is not predictive.
  - 55 of 104 tickers had BOTH big up AND big down days in 2 weeks.
  - Feature comparison showed NO statistical difference between correct and wrong
    predictions on conviction, call_pct, or premium. The signal was noise.

v3 fix — PREDICTIVE FILTER:
  Only count flow as predictive when it's FRESH positioning, not reactive:
  1. Short-DTE premium (≤7d) in weekly options = urgent new bet, not hedging
  2. Vol/OI > 2x = NEW positions being opened, not closing old ones
  3. OI change asymmetry: one-sided OI build-up (calls OR puts, not both)
  4. GEX flip = regime change (forward-looking structural shift)
  5. Convergence of 3+ INDEPENDENT sources pointing same direction

Deprioritized (reactive, not predictive):
  - Raw call/put flow percentage (follows price, doesn't lead it)
  - Total premium magnitude (correlated with market cap, not direction)
  - Dark pool total value (accumulation, but no directional signal)

Fed by: ~/TradeNova/data/*.json
Feeds into: moonshot_adapter.py (calls) and puts_adapter.py (puts)
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_TRADENOVA_DATA = Path.home() / "TradeNova" / "data"

# ═══════════════════════════════════════════════════════════════════
# THRESHOLDS (calibrated against 104-ticker × 2-week backtest)
# ═══════════════════════════════════════════════════════════════════

MIN_TRADES = 5
MIN_PREMIUM_INSTITUTIONAL = 300_000
STRONG_DIRECTIONAL_PCT = 0.70
SHORT_DTE_DAYS = 7
MIN_VOL_OI_RATIO = 1.5
MIN_CONVICTION_THRESHOLD = 0.22


# ═══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

def scan_smart_money(
    universe: Optional[set] = None,
) -> Dict[str, Any]:
    """
    Multi-source smart money scan for future movers.

    Returns:
        {
            "bullish_candidates": [...],
            "bearish_candidates": [...],
            "scan_timestamp": "...",
            "flow_tickers_scanned": N,
            "sources_loaded": [...],
        }
    """
    # Load ALL data sources
    flow_data = _load_uw_flow()
    dp_data = _load_dark_pool()
    forecast_data = _load_forecast()
    oi_data = _load_oi_changes()
    gex_data = _load_gex()
    iv_data = _load_iv_term()
    skew_data = _load_skew()
    inst_radar = _load_institutional_radar()
    insider_data = _load_insider_data()
    recs_data = _load_tradenova_recs()
    congress_data = _load_congress_trades()

    sources_loaded = []
    if flow_data:
        sources_loaded.append("uw_flow({})".format(len(flow_data)))
    if dp_data:
        sources_loaded.append("dark_pool({})".format(len(dp_data)))
    if oi_data:
        sources_loaded.append("oi_change({})".format(len(oi_data)))
    if gex_data:
        sources_loaded.append("gex({})".format(len(gex_data)))
    if iv_data:
        sources_loaded.append("iv_term({})".format(len(iv_data)))
    if skew_data:
        sources_loaded.append("skew({})".format(len(skew_data)))
    if inst_radar:
        sources_loaded.append("inst_radar({})".format(len(inst_radar)))
    if insider_data:
        sources_loaded.append("insider({})".format(len(insider_data)))
    if recs_data:
        sources_loaded.append("tradenova_recs({})".format(len(recs_data)))
    if congress_data:
        sources_loaded.append("congress({})".format(len(congress_data)))

    logger.info("  🧠 Smart Money v3: loaded {} sources — {}".format(
        len(sources_loaded), ", ".join(sources_loaded)))

    if universe is None:
        universe = _get_universe()

    # Build a superset of all tickers that appear in ANY source
    all_tickers = set(universe)
    all_tickers.update(flow_data.keys())
    all_tickers.update(oi_data.keys())
    all_tickers.update(gex_data.keys())

    bullish = []
    bearish = []
    scanned = 0

    for sym in all_tickers:
        trades = flow_data.get(sym, [])
        if not isinstance(trades, list):
            trades = []

        scanned += 1
        analysis = _analyze_ticker_multi_source(
            sym=sym,
            trades=trades,
            dp_info=dp_data.get(sym),
            forecast_info=forecast_data.get(sym),
            oi_info=oi_data.get(sym),
            gex_info=gex_data.get(sym),
            iv_info=iv_data.get(sym),
            skew_info=skew_data.get(sym),
            inst_info=inst_radar.get(sym),
            insider_info=insider_data.get(sym),
            rec_info=recs_data.get(sym),
            congress_info=congress_data.get(sym),
        )

        if analysis["direction"] == "BULLISH" and analysis["conviction"] >= MIN_CONVICTION_THRESHOLD:
            bullish.append(analysis)
        elif analysis["direction"] == "BEARISH" and analysis["conviction"] >= MIN_CONVICTION_THRESHOLD:
            bearish.append(analysis)

    bullish.sort(key=lambda x: x["conviction"], reverse=True)
    bearish.sort(key=lambda x: x["conviction"], reverse=True)

    # ── ANSWER STABILITY: compare against previous scan ──────────────
    # If a ticker changed direction since last scan, flag it and reduce
    # conviction unless the new signal is significantly stronger.
    prev_scan_path = Path.home() / "Meta Engine" / "output" / "smart_money_last_scan.json"
    try:
        if prev_scan_path.exists():
            with open(prev_scan_path) as f:
                prev = json.load(f)
            prev_bull_syms = {b["symbol"]: b["conviction"] for b in prev.get("bullish_candidates", [])}
            prev_bear_syms = {b["symbol"]: b["conviction"] for b in prev.get("bearish_candidates", [])}

            for b in bullish:
                if b["symbol"] in prev_bear_syms:
                    old_conv = prev_bear_syms[b["symbol"]]
                    if b["conviction"] <= old_conv * 1.3:
                        b["conviction"] *= 0.70
                        b["signals"].append("FLIPPED_from_BEAR_penalized")
                    else:
                        b["signals"].append("FLIPPED_from_BEAR_stronger_signal")
            for b in bearish:
                if b["symbol"] in prev_bull_syms:
                    old_conv = prev_bull_syms[b["symbol"]]
                    if b["conviction"] <= old_conv * 1.3:
                        b["conviction"] *= 0.70
                        b["signals"].append("FLIPPED_from_BULL_penalized")
                    else:
                        b["signals"].append("FLIPPED_from_BULL_stronger_signal")

            bullish.sort(key=lambda x: x["conviction"], reverse=True)
            bearish.sort(key=lambda x: x["conviction"], reverse=True)
    except Exception:
        pass

    # Save current scan for next comparison + data fingerprint for debugging
    # "why different answers?" — log what data changed between scans
    try:
        import hashlib
        data_fingerprint = hashlib.md5(
            json.dumps(sorted(flow_data.keys()), default=str).encode()
        ).hexdigest()[:8]

        scan_output = {
            "bullish_candidates": bullish[:20],
            "bearish_candidates": bearish[:20],
            "scan_timestamp": datetime.now().isoformat(),
            "data_fingerprint": data_fingerprint,
            "sources_loaded": sources_loaded,
        }
        with open(prev_scan_path, "w") as f:
            json.dump(scan_output, f, indent=2, default=str)

        # Append to scan history log for audit trail
        history_path = Path.home() / "Meta Engine" / "output" / "smart_money_scan_history.jsonl"
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "fingerprint": data_fingerprint,
            "n_bullish": len(bullish),
            "n_bearish": len(bearish),
            "top5_bull": [b["symbol"] for b in bullish[:5]],
            "top5_bear": [b["symbol"] for b in bearish[:5]],
        }
        with open(history_path, "a") as f:
            f.write(json.dumps(history_entry, default=str) + "\n")
    except Exception:
        pass

    logger.info(
        "  🧠 Smart Money v3: {} tickers scanned, "
        "{} bullish, {} bearish candidates".format(scanned, len(bullish), len(bearish))
    )
    if bullish:
        top_desc = ", ".join(
            "{} {:.0f}%C ${:,.0f} conv={:.2f}".format(
                b["symbol"], b["call_pct"] * 100, b["total_premium"], b["conviction"])
            for b in bullish[:5]
        )
        logger.info("  📈 Top bullish: {}".format(top_desc))
    if bearish:
        top_desc = ", ".join(
            "{} {:.0f}%P ${:,.0f} conv={:.2f}".format(
                b["symbol"], (1 - b["call_pct"]) * 100, b["total_premium"], b["conviction"])
            for b in bearish[:5]
        )
        logger.info("  📉 Top bearish: {}".format(top_desc))

    return {
        "bullish_candidates": bullish,
        "bearish_candidates": bearish,
        "scan_timestamp": datetime.now().isoformat(),
        "flow_tickers_scanned": scanned,
        "sources_loaded": sources_loaded,
    }


# ═══════════════════════════════════════════════════════════════════
# MULTI-SOURCE TICKER ANALYSIS
# ═══════════════════════════════════════════════════════════════════

def _analyze_ticker_multi_source(
    sym: str,
    trades: List[Dict],
    dp_info: Optional[Dict],
    forecast_info: Optional[Dict],
    oi_info: Optional[Dict],
    gex_info: Optional[Dict],
    iv_info: Optional[Dict],
    skew_info: Optional[Dict],
    inst_info: Optional[Dict],
    insider_info: Optional[Dict],
    rec_info: Optional[Dict],
    congress_info: Optional[Dict],
) -> Dict[str, Any]:
    """
    Compute multi-source directional conviction for a single ticker.
    Each source contributes independently; convergence gets a multiplier.
    """

    signals = []
    bullish_sources = 0
    bearish_sources = 0
    conviction = 0.0
    total_prem = 0
    call_pct = 0.5

    # ── SOURCE 1: UW Options Flow (0-0.30) ─────────────────────────
    flow_score, flow_signals, flow_dir, flow_meta = _score_uw_flow(trades)
    conviction += flow_score
    signals.extend(flow_signals)
    total_prem = flow_meta.get("total_premium", 0)
    call_pct = flow_meta.get("call_pct", 0.5)
    if flow_dir == "BULLISH":
        bullish_sources += 1
    elif flow_dir == "BEARISH":
        bearish_sources += 1

    # ── SOURCE 2: OI Changes (0-0.12) ──────────────────────────────
    oi_score, oi_signals, oi_dir = _score_oi_change(oi_info)
    conviction += oi_score
    signals.extend(oi_signals)
    if oi_dir == "BULLISH":
        bullish_sources += 1
    elif oi_dir == "BEARISH":
        bearish_sources += 1

    # ── SOURCE 3: GEX / Gamma Exposure (0-0.10) ────────────────────
    gex_score, gex_signals, gex_amplifier = _score_gex(gex_info)
    conviction += gex_score
    signals.extend(gex_signals)

    # ── SOURCE 4: IV Term Structure (0-0.10) ────────────────────────
    iv_score, iv_signals = _score_iv_term(iv_info)
    conviction += iv_score
    signals.extend(iv_signals)

    # ── SOURCE 5: Skew Analysis (0-0.10) ────────────────────────────
    skew_score, skew_signals, skew_dir = _score_skew(skew_info)
    conviction += skew_score
    signals.extend(skew_signals)
    if skew_dir == "BULLISH":
        bullish_sources += 1
    elif skew_dir == "BEARISH":
        bearish_sources += 1

    # ── SOURCE 6: Dark Pool (0-0.04) ───────────────────────────────
    # v3.5: Dark pool above_ask/below_bid gives weak directionality
    dp_score, dp_signals, dp_dir = _score_dark_pool(dp_info)
    conviction += dp_score
    signals.extend(dp_signals)
    if dp_dir == "BULLISH":
        bullish_sources += 1
    elif dp_dir == "BEARISH":
        bearish_sources += 1

    # ── SOURCE 7: Institutional Radar (0-0.08) ─────────────────────
    inst_score, inst_signals, inst_dir = _score_institutional_radar(inst_info)
    conviction += inst_score
    signals.extend(inst_signals)
    if inst_dir == "BULLISH":
        bullish_sources += 1
    elif inst_dir == "BEARISH":
        bearish_sources += 1

    # ── SOURCE 8: Insider Buying (0-0.06) ──────────────────────────
    insider_score, insider_signals = _score_insider(insider_info)
    conviction += insider_score
    signals.extend(insider_signals)
    if insider_score > 0.01:
        bullish_sources += 1  # Any insider net buying = genuinely bullish signal

    # ── SOURCE 9: TradeNova Recs (0-0.06) ──────────────────────────
    rec_score, rec_signals = _score_tradenova_rec(rec_info)
    conviction += rec_score
    signals.extend(rec_signals)

    # ── SOURCE 10: Congress Trades (0-0.04) ─────────────────────────
    cong_score, cong_signals, cong_dir = _score_congress(congress_info)
    conviction += cong_score
    signals.extend(cong_signals)
    if cong_dir == "BULLISH":
        bullish_sources += 1
    elif cong_dir == "BEARISH":
        bearish_sources += 1

    # ── CONVERGENCE MULTIPLIER ──────────────────────────────────────
    # Sources counted: flow, OI, skew (reversals only), inst radar, insider, congress
    max_directional = max(bullish_sources, bearish_sources)
    if max_directional >= 4:
        conviction *= 1.40
        signals.append("STRONG_CONVERGENCE_{}_sources".format(max_directional))
    elif max_directional >= 3:
        conviction *= 1.25
        signals.append("CONVERGENCE_{}_sources".format(max_directional))
    elif max_directional >= 2 and iv_score > 0:
        conviction *= 1.15
        signals.append("IV+{}_source_convergence".format(max_directional))

    # GEX amplifier: negative GEX means dealer short gamma — any move
    # will be amplified. This is a risk-reward enhancer, not directional.
    if gex_amplifier > 1.0:
        conviction *= gex_amplifier

    # VOLATILITY CATALYST: IV inverted + other signals = big move expected
    # OI + IV convergence is the #1 bullish predictor from backtest (16/30 big UP had it)
    if iv_score > 0 and oi_score > 0:
        conviction += 0.10
        signals.append("vol_catalyst_strong")
    elif iv_score > 0 and (gex_score > 0 or dp_score > 0 or flow_score >= 0.10):
        conviction += 0.06
        signals.append("vol_catalyst")

    # ── DETERMINE DIRECTION (Asymmetric Tiered) ──────────────────────
    # v3 backtest revealed a STRUCTURAL ASYMMETRY in how signals work:
    #
    # BEARISH: Short-DTE puts concentrate 95-100% → Tier 1 fires reliably
    # BULLISH: Short-DTE calls are scattered/absent for most up-movers.
    #          Bullish signal instead comes from OI asymmetry + IV inversion
    #          + total call flow combined (16/30 big UP had OI bull, 25/30 IV)
    #
    # Solution: Different detection paths for bullish vs bearish.
    #
    # BEARISH PATH: Tier 1 (short-DTE puts) → Tier 2 (OI put asym) → Tier 3 (total put flow)
    # BULLISH PATH: Tier 1 (short-DTE calls) → Tier 2A (OI call asym + IV) →
    #               Tier 2B (OI call asym alone) → Tier 3 (total call flow)
    direction = "NEUTRAL"
    direction_tier = 0

    short_call_pct = flow_meta.get("short_call_pct", 0.5)
    short_dte_prem = flow_meta.get("short_dte_premium", 0)
    short_dte_ratio = flow_meta.get("short_dte_ratio", 0)

    # TIER 1: Short-DTE flow (works great for bearish, sometimes for bullish)
    has_short_flow = short_dte_prem >= 100_000 and short_dte_ratio >= 0.15
    if has_short_flow:
        if short_call_pct >= 0.60:
            direction = "BULLISH"
            direction_tier = 1
        elif short_call_pct <= 0.40:
            direction = "BEARISH"
            direction_tier = 1

    # TIER 2A (BULLISH): OI call asymmetry + IV inversion — the dominant
    # bullish predictive pattern. 16/30 big UP movers had this combination.
    if direction == "NEUTRAL" and oi_dir == "BULLISH" and iv_score > 0:
        direction = "BULLISH"
        direction_tier = 2
        conviction += 0.04  # Bonus for IV+OI convergence
        signals.append("OI_bull+IV_convergence")

    # TIER 2B: OI asymmetry alone (either direction)
    if direction == "NEUTRAL" and oi_dir != "NEUTRAL":
        direction = oi_dir
        direction_tier = 2

    # TIER 3A (BULLISH): Total call flow ≥ 65% + any supporting signal
    # This catches stocks with consistent call buying across all expirations
    if direction == "NEUTRAL" and total_prem >= MIN_PREMIUM_INSTITUTIONAL:
        if call_pct >= 0.65 and (iv_score > 0 or bullish_sources >= 1):
            direction = "BULLISH"
            direction_tier = 3

    # TIER 3B: Strong total flow direction (either side)
    if direction == "NEUTRAL" and total_prem >= MIN_PREMIUM_INSTITUTIONAL:
        if call_pct >= 0.80:
            direction = "BULLISH"
            direction_tier = 3
        elif call_pct <= 0.20:
            direction = "BEARISH"
            direction_tier = 3
        elif call_pct <= 0.35 and bearish_sources >= 2:
            direction = "BEARISH"
            direction_tier = 3

    # TIER 4: Multi-source consensus
    if direction == "NEUTRAL":
        if bullish_sources >= 3:
            direction = "BULLISH"
            direction_tier = 4
        elif bearish_sources >= 3:
            direction = "BEARISH"
            direction_tier = 4
        elif bullish_sources == 2 and bearish_sources == 0 and iv_score > 0:
            direction = "BULLISH"
            direction_tier = 4
        elif bearish_sources == 2 and bullish_sources == 0 and iv_score > 0:
            direction = "BEARISH"
            direction_tier = 4

    # TIER 5: Low-Liquidity Catalyst — catches mid/small-cap movers that
    # have <5 flow trades but show strong non-flow signals.
    # Root cause: DNA(+8.3%), IOVA(+4.8%), TDOC(+4.7%), HROW(-3.1%),
    # FUBO(-3.1%) were all missed because MIN_TRADES gate blocked them,
    # despite having OI accumulation, IV inversion, or dark pool activity.
    if direction == "NEUTRAL" and len(trades) < MIN_TRADES:
        non_flow_score = oi_score + iv_score + dp_score + skew_score + inst_score
        if non_flow_score >= 0.10 and oi_dir != "NEUTRAL":
            direction = oi_dir
            direction_tier = 5
        elif non_flow_score >= 0.08 and iv_score > 0 and (bullish_sources > 0 or bearish_sources > 0):
            direction = "BULLISH" if bullish_sources >= bearish_sources else "BEARISH"
            direction_tier = 5
        elif bullish_sources >= 2 and bearish_sources == 0:
            direction = "BULLISH"
            direction_tier = 5
        elif bearish_sources >= 2 and bullish_sources == 0:
            direction = "BEARISH"
            direction_tier = 5

    # TIER 5b: Sustained OI accumulation with flow (≥5 trades) but
    # no directional tier fired due to mixed/weak signals.
    # If OI has been building for 7+ days with 10+ contracts AND
    # a directional source exists, use it.
    if direction == "NEUTRAL" and oi_info:
        max_days_inc = oi_info.get("max_days_oi_increasing", 0)
        contracts_3d = oi_info.get("contracts_3plus_days_oi_increase", 0)
        if max_days_inc >= 7 and contracts_3d >= 10:
            if dp_dir != "NEUTRAL":
                direction = dp_dir
                direction_tier = 5
                signals.append("sustained_OI+dp_direction")
            elif bullish_sources > bearish_sources:
                direction = "BULLISH"
                direction_tier = 5
                signals.append("sustained_OI+source_lean")
            elif bearish_sources > bullish_sources:
                direction = "BEARISH"
                direction_tier = 5
                signals.append("sustained_OI+source_lean")

    # ── TIER-BASED CONVICTION SCALING ─────────────────────────────────
    # Lower tiers = less confident = scale down conviction
    if direction_tier == 3:
        conviction *= 0.85
    elif direction_tier == 4:
        conviction *= 0.70
    elif direction_tier == 5:
        conviction *= 0.65  # Low-liquidity picks carry more uncertainty

    # ── FLOW vs ACCUMULATION CONFLICT DETECTION ─────────────────────
    # Root cause: APP was put in PUTS (flow=0% calls, reactive) despite
    # CALL_OI_DOMINANT + DARK_POOL_MASSIVE (accumulation, predictive).
    # When flow direction contradicts OI + institutional radar direction,
    # the flow is likely REACTIVE (following price) not PREDICTIVE.
    if direction_tier <= 2 and flow_dir != "NEUTRAL" and oi_dir != "NEUTRAL":
        if flow_dir != oi_dir:
            if inst_dir == oi_dir and inst_dir != "NEUTRAL":
                old_dir = direction
                direction = oi_dir
                conviction *= 0.85
                signals.append("FLOW_REACTIVE:oi+inst_override_{}->{}".format(
                    old_dir, direction))
                direction_tier = 2
            elif inst_dir != flow_dir and inst_dir != "NEUTRAL":
                conviction *= 0.75
                signals.append("CONFLICTING:flow={}_oi={}_inst={}".format(
                    flow_dir, oi_dir, inst_dir))

    # ── CONFLICT PENALTY ──────────────────────────────────────────────
    # If Tier 1/2 direction disagrees with source consensus
    if direction_tier <= 2 and direction == "BEARISH" and bullish_sources > bearish_sources + 1:
        conviction *= 0.80
        signals.append("CAUTION:bear_vs_bull_sources")
    elif direction_tier <= 2 and direction == "BULLISH" and bearish_sources > bullish_sources + 1:
        conviction *= 0.80
        signals.append("CAUTION:bull_vs_bear_sources")

    signals.append("dir_tier={}".format(direction_tier))

    # ── LEAPS PENALTY ───────────────────────────────────────────────
    if trades:
        leaps_trades = [t for t in trades if t.get("dte", 0) > 180]
        leaps_prem = sum(t.get("premium", 0) for t in leaps_trades)
        short_dte_trades = [t for t in trades if t.get("dte", 999) <= SHORT_DTE_DAYS]
        short_dte_prem = sum(t.get("premium", 0) for t in short_dte_trades)
        short_dte_ratio = short_dte_prem / total_prem if total_prem > 0 else 0
        if total_prem > 0 and leaps_prem / total_prem > 0.80 and short_dte_ratio < 0.10:
            conviction *= 0.60
            signals.append("LEAPS_heavy_possible_hedge")
    else:
        short_dte_ratio = 0

    # ── HIGH-BETA AMPLIFIER ──────────────────────────────────────────
    # Volatile stocks with a directional signal are more likely to make 5%+
    # moves. Boost conviction for known high-beta names when IV is inverted
    # (confirming the market expects a big move).
    HIGH_BETA = {
        "MSTR", "CLSK", "MARA", "RIOT", "BITF", "WULF", "HUT", "CIFR",  # crypto
        "QBTS", "RGTI", "IONQ", "OKLO",  # quantum/nuclear
        "LUNR", "RKLB", "ASTS", "SPCE",  # space
        "GME", "AMC", "DJT", "BYND",  # meme/high-short
        "IBRX", "SAVA", "NTLA", "CRSP", "DNA", "IOVA", "NVAX", "CRWV",  # biotech
        "SMCI", "APP", "UPST", "AFRM", "HIMS", "CVNA", "LCID", "PLUG",  # high-vol tech
        "RDDT", "SNAP", "U", "FUBO", "PTON", "TDOC",  # volatile tech
        "ENPH", "SEDG", "FSLR",  # solar
        "ARM", "INOD", "HROW", "MDGL", "VKTX",  # volatile mid-cap
    }
    if sym in HIGH_BETA and iv_score > 0 and direction != "NEUTRAL":
        conviction *= 1.15
        signals.append("high_beta_amplified")

    conviction = min(conviction, 1.0)

    return {
        "symbol": sym,
        "direction": direction,
        "conviction": round(conviction, 4),
        "call_pct": round(call_pct, 4),
        "total_premium": total_prem,
        "call_premium": flow_meta.get("call_premium", 0),
        "put_premium": flow_meta.get("put_premium", 0),
        "short_dte_ratio": round(flow_meta.get("short_dte_ratio", short_dte_ratio), 3),
        "short_dte_premium": flow_meta.get("short_dte_premium", 0),
        "avg_vol_oi_ratio": round(flow_meta.get("avg_vol_oi", 0), 2),
        "trade_count": len(trades),
        "bullish_source_count": bullish_sources,
        "bearish_source_count": bearish_sources,
        "signals": signals,
        "_is_smart_money_pick": True,
    }


# ═══════════════════════════════════════════════════════════════════
# INDIVIDUAL SOURCE SCORERS
# ═══════════════════════════════════════════════════════════════════

def _score_uw_flow(trades: List[Dict]) -> Tuple[float, List[str], str, Dict]:
    """
    Score UW options flow with PREDICTIVE filter.

    v3 insight: Raw call/put % is reactive (follows price, doesn't lead it).
    Predictive signals are:
      - Short-DTE concentration: money in ≤7d options = urgent NEW bet
      - Vol/OI > 2x: these are NEW positions, not closing old ones
      - Directional SHORT-DTE flow: the urgency-weighted direction

    We score short-DTE flow direction separately from total flow direction.
    When they disagree, short-DTE wins (it's the fresh money).
    """
    if not trades or len(trades) < MIN_TRADES:
        return 0.0, [], "NEUTRAL", {"call_pct": 0.5, "total_premium": 0}

    calls = [t for t in trades if t.get("put_call") == "C"]
    puts = [t for t in trades if t.get("put_call") == "P"]
    call_prem = sum(t.get("premium", 0) for t in calls)
    put_prem = sum(t.get("premium", 0) for t in puts)
    total_prem = call_prem + put_prem

    if total_prem <= 0:
        return 0.0, [], "NEUTRAL", {"call_pct": 0.5, "total_premium": 0}

    call_pct = call_prem / total_prem
    signals = []
    score = 0.0

    # ── PREDICTIVE SIGNAL 1: Short-DTE directional flow (0-0.12) ───
    # This is the MOST predictive flow signal — money in weeklies is urgent
    short_dte_trades = [t for t in trades if t.get("dte", 999) <= SHORT_DTE_DAYS]
    short_dte_prem = sum(t.get("premium", 0) for t in short_dte_trades)
    short_dte_ratio = short_dte_prem / total_prem if total_prem > 0 else 0

    short_call_prem = sum(t.get("premium", 0) for t in short_dte_trades if t.get("put_call") == "C")
    short_put_prem = sum(t.get("premium", 0) for t in short_dte_trades if t.get("put_call") == "P")
    short_total = short_call_prem + short_put_prem
    short_call_pct = short_call_prem / short_total if short_total > 0 else 0.5

    # Use short-DTE direction as the PRIMARY flow direction signal
    short_direction = "NEUTRAL"
    if short_total >= 200_000 and short_dte_ratio >= 0.30:
        if short_call_pct >= 0.65:
            score += 0.12
            short_direction = "BULLISH"
            signals.append("short_dte_calls_{:.0f}%_${:,.0f}".format(short_call_pct * 100, short_total))
        elif short_call_pct <= 0.35:
            score += 0.12
            short_direction = "BEARISH"
            signals.append("short_dte_puts_{:.0f}%_${:,.0f}".format((1 - short_call_pct) * 100, short_total))
        elif short_dte_ratio >= 0.50:
            score += 0.04
            signals.append("high_short_dte_{:.0f}%_mixed".format(short_dte_ratio * 100))

    # ── PREDICTIVE SIGNAL 2: New positions (Vol/OI > 2x) (0-0.08) ──
    # High Vol/OI = traders opening NEW positions, not closing old ones
    vol_oi_ratios = []
    for t in trades:
        oi = t.get("open_interest", 0)
        vol = t.get("volume", 0)
        if oi > 0:
            vol_oi_ratios.append(vol / oi)
    avg_vol_oi = sum(vol_oi_ratios) / len(vol_oi_ratios) if vol_oi_ratios else 0

    if avg_vol_oi >= 3.0:
        score += 0.08
        signals.append("fresh_positions_vol/oi={:.1f}x".format(avg_vol_oi))
    elif avg_vol_oi >= 2.0:
        score += 0.04
        signals.append("new_positions_vol/oi={:.1f}x".format(avg_vol_oi))

    # ── REACTIVE SIGNAL: Total flow direction (0-0.05) ──────────────
    # Downweighted because this follows price. Only counts if it AGREES
    # with short-DTE direction or if short-DTE has no signal.
    direction = short_direction  # Short-DTE is primary
    dir_pct = max(call_pct, 1 - call_pct)
    if dir_pct >= STRONG_DIRECTIONAL_PCT:
        total_dir = "BULLISH" if call_pct >= STRONG_DIRECTIONAL_PCT else "BEARISH"
        if short_direction == total_dir or short_direction == "NEUTRAL":
            score += 0.05
            if call_pct >= STRONG_DIRECTIONAL_PCT:
                signals.append("total_call_flow_{:.0f}%".format(call_pct * 100))
            else:
                signals.append("total_put_flow_{:.0f}%".format((1 - call_pct) * 100))
            if direction == "NEUTRAL":
                direction = total_dir
        else:
            # Short-DTE disagrees with total flow — this is a RED FLAG
            # Total flow is likely reactive (chasing the move)
            signals.append("FLOW_CONFLICT:total_{}_vs_shortDTE_{}".format(total_dir, short_direction))

    # Premium urgency bonus (0-0.08): SHORT-DTE premium magnitude
    # Only short-DTE premium matters — it's urgent money
    if short_total >= 10_000_000:
        score += 0.08
        signals.append("urgent_premium_${:,.0f}".format(short_total))
    elif short_total >= 1_000_000:
        score += 0.05
        signals.append("significant_short_dte_${:,.0f}".format(short_total))
    elif short_total >= 200_000:
        score += 0.02

    meta = {
        "call_pct": call_pct, "total_premium": total_prem,
        "call_premium": call_prem, "put_premium": put_prem,
        "short_dte_ratio": short_dte_ratio, "short_dte_premium": short_dte_prem,
        "short_call_pct": short_call_pct,
        "avg_vol_oi": avg_vol_oi,
    }
    return min(score, 0.35), signals, direction, meta


def _score_oi_change(oi_info: Optional[Dict]) -> Tuple[float, List[str], str]:
    """
    Score OI changes — new positions being opened.

    v3 fix: ONLY score ASYMMETRIC OI builds. When both call and put OI
    surge together, it's hedging/straddling, not a directional signal.
    The predictive signal is ONE-SIDED OI accumulation.

    v3.5 fix (Feb 19): Enrich with contract-level intelligence.
    The OI cache has rich sub-fields that were unused:
      - max_days_oi_increasing: persistent accumulation (≥3 days = institutional)
      - contracts_3plus_days_oi_increase: breadth of sustained build
      - vol_gt_oi_count: high vol/OI contracts (new aggressive positions)
      - top_contracts[].prev_direction: what direction the flow was on those contracts
    These signals catch mid/small-cap movers with low absolute OI but
    strong RELATIVE patterns (e.g. IOVA +4.8% today, TDOC +4.7%, FUBO -3.1%).
    """
    if not oi_info or not isinstance(oi_info, dict):
        return 0.0, [], "NEUTRAL"

    call_chg = oi_info.get("call_oi_change", 0)
    put_chg = oi_info.get("put_oi_change", 0)
    call_pct_chg = oi_info.get("call_oi_pct_change", 0)
    put_pct_chg = oi_info.get("put_oi_pct_change", 0)

    signals = []
    score = 0.0
    direction = "NEUTRAL"

    # ── PRIMARY: Absolute OI asymmetry (unchanged logic) ─────────────
    both_positive = call_chg > 1000 and put_chg > 1000
    if both_positive:
        ratio = call_chg / max(put_chg, 1)
        if ratio > 2.5:
            score += 0.10
            signals.append("OI_asymmetry_calls_{:.1f}x_puts".format(ratio))
            direction = "BULLISH"
        elif ratio < 0.40:
            score += 0.10
            signals.append("OI_asymmetry_puts_{:.1f}x_calls".format(1 / ratio))
            direction = "BEARISH"
        elif ratio > 1.7:
            score += 0.05
            signals.append("OI_lean_calls_{:.1f}x".format(ratio))
            direction = "BULLISH"
        elif ratio < 0.59:
            score += 0.05
            signals.append("OI_lean_puts_{:.1f}x".format(1 / ratio))
            direction = "BEARISH"
        else:
            signals.append("OI_mixed_no_signal")
    elif call_chg > 5000 and put_chg <= 500:
        score += 0.10
        signals.append("pure_call_OI_build_+{:,}".format(call_chg))
        direction = "BULLISH"
    elif put_chg > 5000 and call_chg <= 500:
        score += 0.10
        signals.append("pure_put_OI_build_+{:,}".format(put_chg))
        direction = "BEARISH"
    elif call_chg > 2000 and call_pct_chg > 20 and put_chg < call_chg * 0.5:
        score += 0.05
        signals.append("call_OI_dominant_+{:,}_{:+.0f}%".format(call_chg, call_pct_chg))
        direction = "BULLISH"
    elif put_chg > 2000 and put_pct_chg > 20 and call_chg < put_chg * 0.5:
        score += 0.05
        signals.append("put_OI_dominant_+{:,}_{:+.0f}%".format(put_chg, put_pct_chg))
        direction = "BEARISH"

    # ── SECONDARY: Percentage-based asymmetry for smaller tickers ────
    # Catches mid/small-cap names where absolute OI is low but the
    # RELATIVE change is extreme (e.g. put OI jumps 50%+ vs calls 3%)
    if direction == "NEUTRAL" and score == 0:
        if call_pct_chg > 15 and put_pct_chg < call_pct_chg * 0.3 and call_chg > 500:
            score += 0.05
            signals.append("pct_OI_skew_calls_{:+.0f}%_vs_puts_{:+.0f}%".format(
                call_pct_chg, put_pct_chg))
            direction = "BULLISH"
        elif put_pct_chg > 15 and call_pct_chg < put_pct_chg * 0.3 and put_chg > 500:
            score += 0.05
            signals.append("pct_OI_skew_puts_{:+.0f}%_vs_calls_{:+.0f}%".format(
                put_pct_chg, call_pct_chg))
            direction = "BEARISH"

    # ── ENRICHMENT: Contract-level accumulation intelligence ─────────
    max_days_inc = oi_info.get("max_days_oi_increasing", 0)
    contracts_3d = oi_info.get("contracts_3plus_days_oi_increase", 0)
    vol_gt_oi = oi_info.get("vol_gt_oi_count", 0)

    # Multi-day sustained OI accumulation = institutional, not retail day-trade
    if max_days_inc >= 5 and contracts_3d >= 5:
        score += 0.04
        signals.append("sustained_OI_build_{}d_{}contracts".format(max_days_inc, contracts_3d))
    elif max_days_inc >= 3 and contracts_3d >= 3:
        score += 0.02
        signals.append("OI_accumulating_{}d".format(max_days_inc))

    # High vol/OI contracts = aggressive new positioning
    if vol_gt_oi >= 5:
        score += 0.03
        signals.append("aggressive_new_positions_{}_contracts".format(vol_gt_oi))

    # Top contract direction consensus — analyze prev_direction of top OI builds
    top_contracts = oi_info.get("top_contracts", [])
    if top_contracts and direction == "NEUTRAL":
        bull_top = sum(1 for c in top_contracts[:10]
                       if str(c.get("prev_direction", "")).upper() == "BULLISH")
        bear_top = sum(1 for c in top_contracts[:10]
                       if str(c.get("prev_direction", "")).upper() == "BEARISH")
        n_top = min(len(top_contracts), 10)
        if n_top >= 3:
            if bull_top >= n_top * 0.6:
                score += 0.03
                direction = "BULLISH"
                signals.append("top_contract_flow_bullish_{}/{}".format(bull_top, n_top))
            elif bear_top >= n_top * 0.6:
                score += 0.03
                direction = "BEARISH"
                signals.append("top_contract_flow_bearish_{}/{}".format(bear_top, n_top))

    return min(score, 0.15), signals, direction


def _score_gex(gex_info: Optional[Dict]) -> Tuple[float, List[str], float]:
    """
    Score GEX exposure. Returns (score, signals, amplifier_multiplier).

    v3 fix: Negative GEX is just an amplifier (not directional).
    Vanna "bullish" was true for most stocks — removed as directional signal.
    Only GEX FLIPS are genuinely predictive (regime changes).
    """
    if not gex_info or not isinstance(gex_info, dict):
        return 0.0, [], 1.0

    signals = []
    score = 0.0
    amplifier = 1.0

    net_gex = gex_info.get("net_gex", 0)
    gex_flip = gex_info.get("gex_flip_today", False)
    flip_dir = gex_info.get("gex_flip_direction", "")

    # GEX flip today = regime change, genuinely predictive
    if gex_flip:
        score += 0.08
        signals.append("GEX_FLIP_{}".format(flip_dir))

    # Negative GEX = amplifier only (not a directional signal)
    if net_gex < -100_000:
        amplifier = min(1.0 + abs(net_gex) / 5_000_000, 1.10)

    return min(score, 0.08), signals, amplifier


def _score_iv_term(iv_info: Optional[Dict]) -> Tuple[float, List[str]]:
    """
    Score IV term structure — inverted = imminent move expected.

    v3: IV inversion is genuinely predictive (big move coming) but
    NOT directional. Score as catalyst conviction, not direction.
    """
    if not iv_info or not isinstance(iv_info, dict):
        return 0.0, []

    signals = []
    score = 0.0

    inverted = iv_info.get("inverted", False)
    spread = iv_info.get("term_spread", 0)
    implied_move = iv_info.get("implied_move_pct", 0) or iv_info.get("weekly_implied_move_pct", 0)
    if implied_move and implied_move < 1.0:
        implied_move *= 100.0

    front_iv = iv_info.get("front_iv", 0) or 0
    back_iv = iv_info.get("back_iv", 0) or 0
    if not inverted and front_iv and back_iv and front_iv > back_iv * 1.15:
        inverted = True
        spread = front_iv - back_iv

    if inverted and abs(spread) > 1.0:
        score += 0.08
        signals.append("IV_extreme_inversion_spread={:+.2f}".format(spread))
    elif inverted and abs(spread) > 0.3:
        score += 0.06
        signals.append("IV_inverted_spread={:+.2f}".format(spread))
    elif inverted:
        score += 0.03
        signals.append("IV_inverted")

    if implied_move > 10.0:
        score += 0.04
        signals.append("implied_move_{:.1f}%".format(implied_move))
    elif implied_move > 5.0:
        score += 0.02
        signals.append("implied_move_{:.1f}%".format(implied_move))

    return min(score, 0.10), signals


def _score_skew(skew_info: Optional[Dict]) -> Tuple[float, List[str], str]:
    """
    Score skew — extreme z-scores indicate institutional positioning.

    v3 fix: Backtest showed skew was "bullish_demand" for almost every
    high-beta stock (z=+5.48 across the board). This means the z-score
    is saturated and not discriminating. Only score REVERSALS and
    truly extreme outliers relative to peers, and reduce weight.
    """
    if not skew_info or not isinstance(skew_info, dict):
        return 0.0, [], "NEUTRAL"

    signals = []
    score = 0.0
    direction = "NEUTRAL"

    zscore = skew_info.get("skew_zscore", 0)
    trend = skew_info.get("skew_trend", "")
    bearish_hedge = skew_info.get("bearish_hedge", False)

    # Only score reversals (directional changes) not steady-state
    if "REVERSAL_TO_BEARISH" in str(trend).upper():
        score += 0.05
        direction = "BEARISH"
        signals.append("skew_reversal_bearish")
    elif "REVERSAL_TO_BULLISH" in str(trend).upper():
        score += 0.05
        direction = "BULLISH"
        signals.append("skew_reversal_bullish")
    elif bearish_hedge and zscore < -3.0:
        score += 0.04
        direction = "BEARISH"
        signals.append("skew_bearish_hedge_z={:+.1f}".format(zscore))

    return min(score, 0.05), signals, direction


def _score_dark_pool(dp_info: Optional[Dict]) -> Tuple[float, List[str], str]:
    """
    Score dark pool activity — large blocks = stealth institutional positioning.

    v3 fix: Dark pool activity is NOT directional — it means institutions
    are moving, but we don't know if they're buying or selling.
    Reduced weight. Only counts as conviction multiplier.

    v3.5 fix: Use above_ask_count / below_bid_count for weak directionality.
    Above-ask prints = aggressive buying, below-bid = aggressive selling.
    Also use total_value from the pre-computed field when available.
    """
    if not dp_info or not isinstance(dp_info, dict):
        return 0.0, [], "NEUTRAL"

    dp_prints = dp_info.get("prints", [])
    if not isinstance(dp_prints, list) or len(dp_prints) < 2:
        return 0.0, [], "NEUTRAL"

    signals = []
    score = 0.0
    direction = "NEUTRAL"

    dp_value = dp_info.get("total_value", 0) or sum(p.get("value", 0) for p in dp_prints)
    large_blocks = [p for p in dp_prints if p.get("value", 0) >= 500_000]

    if dp_value >= 10_000_000 and len(large_blocks) >= 3:
        score += 0.04
        signals.append("dark_pool_${:,.0f}_{}_blocks".format(dp_value, len(large_blocks)))
    elif dp_value >= 2_000_000:
        score += 0.02
        signals.append("dark_pool_${:,.0f}".format(dp_value))

    above_ask = dp_info.get("above_ask_count", 0)
    below_bid = dp_info.get("below_bid_count", 0)
    total_directional = above_ask + below_bid
    if total_directional >= 5:
        if above_ask > below_bid * 2:
            direction = "BULLISH"
            signals.append("dp_aggressive_buy_{}/{}".format(above_ask, total_directional))
        elif below_bid > above_ask * 2:
            direction = "BEARISH"
            signals.append("dp_aggressive_sell_{}/{}".format(below_bid, total_directional))

    return min(score, 0.04), signals, direction


def _score_institutional_radar(inst_info: Optional[Dict]) -> Tuple[float, List[str], str]:
    """Score institutional radar multi-signal convergence.
    v3.6: Score 1-2 signal tickers (not just 3+), use implied_move and
    details for richer scoring. Root cause: BYND had IV_EXTREME_INVERSION
    + CALL_OI_DOMINANT (23.97% implied move) but got score=0."""
    if not inst_info or not isinstance(inst_info, dict):
        return 0.0, [], "NEUTRAL"

    signals_list = inst_info.get("signals", [])
    signal_count = inst_info.get("signal_count", len(signals_list))
    try:
        signal_count = int(signal_count)
    except (ValueError, TypeError):
        signal_count = len(signals_list)
    details = inst_info.get("details", {})

    signals = []
    score = 0.0
    direction = "NEUTRAL"

    if signal_count >= 4:
        score += 0.08
        signals.append("inst_radar_{}_signals".format(signal_count))
    elif signal_count >= 3:
        score += 0.06
        signals.append("inst_radar_{}_signals".format(signal_count))
    elif signal_count >= 2:
        score += 0.04
        signals.append("inst_radar_{}_signals".format(signal_count))
    elif signal_count >= 1:
        score += 0.02
        signals.append("inst_radar_{}_signal".format(signal_count))

    sig_set = set()
    for s in signals_list:
        s_str = str(s).upper()
        sig_set.add(s_str)
        if "CALL_OI_DOMINANT" in s_str or "CALL_SWEEP" in s_str:
            direction = "BULLISH"
        elif "PUT_OI_DOMINANT" in s_str or "PUT_SWEEP" in s_str:
            direction = "BEARISH"
        elif "VANNA_CRUSH_BULLISH" in s_str:
            if direction != "BEARISH":
                direction = "BULLISH"

    implied_move = 0
    try:
        implied_move = float(inst_info.get("implied_move", 0) or 0)
    except (ValueError, TypeError):
        pass
    if implied_move >= 15.0:
        score += 0.03
        signals.append("inst_implied_move_{:.0f}%".format(implied_move))
    elif implied_move >= 8.0:
        score += 0.02
        signals.append("inst_implied_move_{:.0f}%".format(implied_move))

    has_iv_extreme = any("IV_EXTREME" in s for s in sig_set)
    has_oi_dominant = any("OI_DOMINANT" in s for s in sig_set)
    if has_iv_extreme and has_oi_dominant:
        score += 0.02
        signals.append("inst_iv_extreme+oi_convergence")

    has_dp_massive = any("DARK_POOL_MASSIVE" in s for s in sig_set)
    if has_dp_massive:
        score += 0.01

    conviction_label = str(inst_info.get("conviction", "")).upper()
    if conviction_label == "HIGH":
        score += 0.02
    elif conviction_label == "MEDIUM":
        score += 0.01

    return min(score, 0.12), signals, direction


def _score_insider(insider_info: Optional[Dict]) -> Tuple[float, List[str]]:
    """Score insider buying — officers buying own stock = internal conviction."""
    if not insider_info or not isinstance(insider_info, dict):
        return 0.0, []

    net_value = insider_info.get("net_value", 0)
    total_buys = insider_info.get("total_buys", 0)
    buy_value = insider_info.get("total_buy_value", 0)

    signals = []
    score = 0.0

    if net_value > 1_000_000:
        score += 0.06
        signals.append("insider_net_buy_${:,.0f}".format(net_value))
    elif net_value > 100_000:
        score += 0.04
        signals.append("insider_buying_${:,.0f}".format(net_value))
    elif net_value > 0 and total_buys >= 3:
        score += 0.02
        signals.append("insider_cluster_buy_{}x".format(total_buys))

    return min(score, 0.06), signals


def _score_tradenova_rec(rec_info: Optional[Dict]) -> Tuple[float, List[str]]:
    """Score TradeNova multi-engine recommendation."""
    if not rec_info or not isinstance(rec_info, dict):
        return 0.0, []

    signals = []
    score = 0.0

    composite = rec_info.get("composite_score", 0)
    engine_count = rec_info.get("engine_count", 0)
    catalyst = rec_info.get("catalyst_score", 0)

    if engine_count >= 3:
        score += 0.04
        signals.append("tradenova_triple_engine")
    elif engine_count >= 2:
        score += 0.02
        signals.append("tradenova_dual_engine")

    if catalyst >= 0.80:
        score += 0.02
        signals.append("catalyst_score_{:.0f}%".format(catalyst * 100))

    return min(score, 0.06), signals


def _score_congress(congress_info: Optional[Dict]) -> Tuple[float, List[str], str]:
    """Score congress trades — political insider info."""
    if not congress_info or not isinstance(congress_info, dict):
        return 0.0, [], "NEUTRAL"

    action = str(congress_info.get("action", "")).upper()
    signals = []
    score = 0.0
    direction = "NEUTRAL"

    if "BUY" in action or "PURCHASE" in action:
        score += 0.04
        direction = "BULLISH"
        politician = congress_info.get("politician", "Unknown")
        signals.append("congress_buy_{}".format(politician[:15]))
    elif "SELL" in action or "SALE" in action:
        score += 0.03
        direction = "BEARISH"
        signals.append("congress_sell")

    return min(score, 0.04), signals, direction


# ═══════════════════════════════════════════════════════════════════
# ADAPTER OUTPUT BUILDERS
# ═══════════════════════════════════════════════════════════════════

def build_moonshot_candidates_from_smart_money(
    bullish: List[Dict],
    top_n: int = 15,
) -> List[Dict[str, Any]]:
    """Convert smart money bullish picks into moonshot adapter format."""
    candidates = []
    for b in bullish[:top_n]:
        score = min(b["conviction"] * 1.5, 1.0)
        candidates.append({
            "symbol": b["symbol"],
            "score": score,
            "price": 0,
            "signals": b["signals"],
            "engine": "SmartMoney",
            "engine_type": "smart_money_flow",
            "data_source": "smart_money_scanner",
            "_is_smart_money_pick": True,
            "_smart_money_conviction": b["conviction"],
            "_smart_money_call_pct": b["call_pct"],
            "_smart_money_premium": b["total_premium"],
            "_smart_money_short_dte_ratio": b.get("short_dte_ratio", 0),
        })
    return candidates


def build_puts_candidates_from_smart_money(
    bearish: List[Dict],
    top_n: int = 15,
) -> List[Dict[str, Any]]:
    """Convert smart money bearish picks into puts adapter format."""
    candidates = []
    for b in bearish[:top_n]:
        score = min(b["conviction"] * 1.5, 1.0)
        candidates.append({
            "symbol": b["symbol"],
            "score": score,
            "price": 0,
            "signals": b["signals"],
            "engine": "SmartMoney",
            "engine_type": "smart_money_flow",
            "data_source": "smart_money_scanner",
            "_is_smart_money_pick": True,
            "_smart_money_conviction": b["conviction"],
            "_smart_money_put_pct": 1 - b["call_pct"],
            "_smart_money_premium": b["total_premium"],
            "_smart_money_short_dte_ratio": b.get("short_dte_ratio", 0),
        })
    return candidates


# ═══════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════════

def _load_json(filename: str) -> Any:
    """Generic safe JSON loader."""
    try:
        f = _TRADENOVA_DATA / filename
        if not f.exists():
            return None
        with open(f) as fh:
            return json.load(fh)
    except Exception as e:
        logger.debug("Failed to load {}: {}".format(filename, e))
        return None


def _load_uw_flow() -> Dict[str, List]:
    """Load UW flow cache, returning {symbol: [trades]}."""
    raw = _load_json("uw_flow_cache.json")
    if not raw:
        return {}
    fd = raw.get("flow_data", raw)
    if isinstance(fd, dict):
        return {k: v for k, v in fd.items() if isinstance(v, list)}
    return {}


def _load_dark_pool() -> Dict[str, Dict]:
    """Load dark pool cache, returning {symbol: {prints: [...]}}."""
    raw = _load_json("darkpool_cache.json")
    if not raw or not isinstance(raw, dict):
        return {}
    return {k: v for k, v in raw.items() if isinstance(v, dict)}


def _load_forecast() -> Dict[str, Dict]:
    """Load tomorrows_forecast, returning {symbol: forecast_dict}."""
    raw = _load_json("tomorrows_forecast.json")
    if not raw:
        return {}
    forecasts = raw.get("forecasts", [])
    return {fc.get("symbol", ""): fc for fc in forecasts if fc.get("symbol")}


def _load_oi_changes() -> Dict[str, Dict]:
    """Load OI change data, returning {symbol: oi_dict}."""
    raw = _load_json("uw_oi_change_cache.json")
    if not raw:
        return {}
    data = raw.get("data", raw)
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    return {}


def _load_gex() -> Dict[str, Dict]:
    """Load GEX/gamma exposure data."""
    raw = _load_json("uw_gex_cache.json")
    if not raw:
        return {}
    data = raw.get("data", raw)
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    return {}


def _load_iv_term() -> Dict[str, Dict]:
    """Load IV term structure data."""
    raw = _load_json("uw_iv_term_cache.json")
    if not raw:
        return {}
    data = raw.get("data", raw)
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    return {}


def _load_skew() -> Dict[str, Dict]:
    """Load skew analysis data."""
    raw = _load_json("uw_skew_cache.json")
    if not raw:
        return {}
    data = raw.get("data", raw)
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    return {}


def _load_institutional_radar() -> Dict[str, Dict]:
    """Load institutional radar daily signals."""
    raw = _load_json("institutional_radar_daily.json")
    if not raw:
        return {}
    data = raw.get("ticker_signals", raw)
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    return {}


def _load_insider_data() -> Dict[str, Dict]:
    """Load Finviz insider data (has net buy/sell per ticker)."""
    raw = _load_json("finviz_insider_cache.json")
    if not raw or not isinstance(raw, dict):
        return {}
    return {k: v for k, v in raw.items() if isinstance(v, dict)}


def _load_tradenova_recs() -> Dict[str, Dict]:
    """Load TradeNova final recommendations, keyed by symbol."""
    raw = _load_json("final_recommendations.json")
    if not raw:
        return {}
    recs = raw.get("recommendations", [])
    return {r.get("symbol", ""): r for r in recs if r.get("symbol")}


def _load_congress_trades() -> Dict[str, Dict]:
    """Load congress trades, keyed by symbol (most recent trade per ticker)."""
    raw = _load_json("congress_trades_cache.json")
    if not raw:
        return {}
    trades = raw.get("trades", [])
    result = {}
    for t in trades:
        sym = t.get("symbol", "")
        if sym and sym not in result:
            result[sym] = t
    return result


def _get_universe() -> set:
    """Get the ticker universe from PutsEngine config."""
    try:
        import sys
        sys.path.insert(0, str(Path.home() / "PutsEngine"))
        from putsengine.config import EngineConfig
        return set(EngineConfig.get_all_tickers())
    except Exception:
        return set()
