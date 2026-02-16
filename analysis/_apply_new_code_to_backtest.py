#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════════════════
  APPLY NEW CODE TO HISTORICAL BACKTEST — Feb 9-13, 2026
  "What Would the NEW System Pick?"
══════════════════════════════════════════════════════════════════════════════

Takes the historical backtest data (all picks considered) and applies
the NEW filtering logic to see what would pass with:
  - Policy B v2 gates (calibrated thresholds)
  - Regime-aware hard block (bearish flow + bear regime)
  - Feature extraction (stable schema)
  - Quality-over-quantity (no forced Top 10)

Then validates outcomes and provides detailed analysis.

Goal: 80% win rate (Tradeable ≥+10% definition)
══════════════════════════════════════════════════════════════════════════════
"""

import json, os, sys, statistics
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any, Tuple

ROOT = Path("/Users/chavala/Meta Engine")
OUTPUT = ROOT / "output"
TN_DATA = Path("/Users/chavala/TradeNova/data")

sys.path.insert(0, str(ROOT))

import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("apply_new_code")

# ═════════════════════════════════════════════════════════════════
# NEW CODE LOGIC — Replicate Policy B v2 + Regime Gates
# ═════════════════════════════════════════════════════════════════

def extract_features_from_pick(pick: Dict[str, Any], forecasts: Dict, uw_flow: Dict) -> Dict[str, Any]:
    """Extract stable feature dict from a pick (same as production code)."""
    sym = pick.get("symbol", "")
    
    signals = pick.get("signals", [])
    sig_set = set()
    if isinstance(signals, list):
        sig_set = {str(s).lower() for s in signals}
    
    # Catalysts from forecast
    fc = forecasts.get(sym, {})
    catalysts = fc.get("catalysts", [])
    if isinstance(catalysts, list):
        cat_str = " ".join(str(c) for c in catalysts).lower()
    else:
        cat_str = str(catalysts).lower()
    
    # UW flow
    flow = uw_flow.get(sym, []) if isinstance(uw_flow, dict) else []
    call_prem = 0.0
    put_prem = 0.0
    if isinstance(flow, list):
        for trade in flow:
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
        "mps": pick.get("mps", 0) or 0,
        "signal_count": len(signals) if isinstance(signals, list) else 0,
    }


def apply_policy_b_v2_moonshot(pick: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Apply Policy B v2 gates for moonshot (from moonshot_adapter.py).
    
    Returns (passed: bool, reasons: list)
    """
    reasons = []
    
    # Thresholds (v2 calibrated)
    MIN_SIGNAL_COUNT = 5
    MIN_BASE_SCORE = 0.65
    MIN_MOVE_POTENTIAL = 0.50
    MIN_EXPECTED_MOVE_VS_BREAKEVEN = 1.3
    TYPICAL_BREAKEVEN_PCT = 3.5
    
    signal_count = pick.get("signal_count", 0)
    base_score = pick.get("score", 0)
    mps = pick.get("mps", 0)
    
    if signal_count < MIN_SIGNAL_COUNT:
        reasons.append(f"signal_count={signal_count} < {MIN_SIGNAL_COUNT}")
        return False, reasons
    
    if base_score < MIN_BASE_SCORE:
        reasons.append(f"score={base_score:.2f} < {MIN_BASE_SCORE}")
        return False, reasons
    
    if mps < MIN_MOVE_POTENTIAL:
        reasons.append(f"mps={mps:.2f} < {MIN_MOVE_POTENTIAL}")
        return False, reasons
    
    # Breakeven check
    if mps > 0:
        expected_move_pct = mps * 10.0
        required = TYPICAL_BREAKEVEN_PCT * MIN_EXPECTED_MOVE_VS_BREAKEVEN
        if expected_move_pct < required:
            reasons.append(f"breakeven: expected={expected_move_pct:.1f}% < {required:.1f}%")
            return False, reasons
    
    return True, []


def apply_policy_b_v2_puts(pick: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Apply Policy B v2 gates for puts."""
    reasons = []
    
    MIN_SIGNAL_COUNT = 5
    MIN_BASE_SCORE = 0.55
    MIN_MOVE_POTENTIAL = 0.50
    MIN_EXPECTED_MOVE_VS_BREAKEVEN = 1.3
    TYPICAL_BREAKEVEN_PCT = 3.5
    
    signal_count = pick.get("signal_count", 0)
    base_score = pick.get("score", 0)
    mps = pick.get("mps", 0)
    
    if signal_count < MIN_SIGNAL_COUNT:
        reasons.append(f"signal_count={signal_count} < {MIN_SIGNAL_COUNT}")
        return False, reasons
    
    if base_score < MIN_BASE_SCORE:
        reasons.append(f"score={base_score:.2f} < {MIN_BASE_SCORE}")
        return False, reasons
    
    if mps < MIN_MOVE_POTENTIAL:
        reasons.append(f"mps={mps:.2f} < {MIN_MOVE_POTENTIAL}")
        return False, reasons
    
    if mps > 0:
        expected_move_pct = mps * 10.0
        required = TYPICAL_BREAKEVEN_PCT * MIN_EXPECTED_MOVE_VS_BREAKEVEN
        if expected_move_pct < required:
            reasons.append(f"breakeven: expected={expected_move_pct:.1f}% < {required:.1f}%")
            return False, reasons
    
    return True, []


def apply_regime_hard_block(pick: Dict[str, Any], features: Dict[str, Any],
                            regime: str) -> Tuple[bool, List[str]]:
    """
    Apply regime hard block (moonshot + bearish_flow + bear_regime).
    
    Returns (blocked: bool, reasons: list)
    """
    if pick.get("engine") != "MOONSHOT":
        return False, []  # Only applies to moonshots
    
    bear_regimes = {"STRONG_BEAR", "LEAN_BEAR"}
    
    if regime in bear_regimes and features["bearish_flow"]:
        return True, [f"HARD BLOCK: {regime} + bearish_flow (call_pct={features['call_pct']:.0%})"]
    
    return False, []


# ═════════════════════════════════════════════════════════════════
# REGIME CLASSIFICATION (from deep analysis)
# ═════════════════════════════════════════════════════════════════

REGIMES = {
    "2026-02-09": {"regime": "STRONG_BULL", "score": 0.45},
    "2026-02-10": {"regime": "LEAN_BEAR", "score": -0.10},
    "2026-02-11": {"regime": "STRONG_BEAR", "score": -0.30},
    "2026-02-12": {"regime": "STRONG_BEAR", "score": -0.60},
    "2026-02-13": {"regime": "LEAN_BEAR", "score": -0.10},
}


# ═════════════════════════════════════════════════════════════════
# MAIN ANALYSIS
# ═════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 80)
    log.info("  APPLY NEW CODE TO HISTORICAL BACKTEST — Feb 9-13, 2026")
    log.info("  'What Would the NEW System Pick?'")
    log.info("=" * 80)
    
    # Load backtest data
    with open(OUTPUT / "backtest_newcode_v2_feb9_13.json") as f:
        bt = json.load(f)
    all_picks = bt.get("results", [])
    
    # Load supporting data for feature extraction
    try:
        with open(TN_DATA / "tomorrows_forecast.json") as f:
            fc_data = json.load(f)
        forecasts = {fc["symbol"]: fc for fc in fc_data.get("forecasts", []) if fc.get("symbol")}
    except Exception:
        forecasts = {}
    
    try:
        with open(TN_DATA / "uw_flow_cache.json") as f:
            uw_raw = json.load(f)
        uw_flow = uw_raw.get("flow_data", uw_raw) if isinstance(uw_raw, dict) else {}
    except Exception:
        uw_flow = {}
    
    log.info(f"\nLoaded {len(all_picks)} total picks from backtest")
    log.info(f"Forecast data: {len(forecasts)} symbols")
    log.info(f"UW flow data: {len(uw_flow)} symbols")
    
    # Filter to OK quality
    ok_picks = [p for p in all_picks if p.get("data_quality") == "OK"]
    log.info(f"OK quality picks: {len(ok_picks)}")
    
    # Apply NEW code filtering
    log.info(f"\n{'='*80}")
    log.info(f"  APPLYING NEW CODE FILTERS")
    log.info(f"{'='*80}")
    
    new_code_passed = []
    rejected_by_policy_b = []
    rejected_by_regime = []
    
    for pick in ok_picks:
        engine = pick.get("engine", "")
        scan_date = pick.get("scan_date", "")
        regime_info = REGIMES.get(scan_date, {"regime": "UNKNOWN", "score": 0})
        regime = regime_info["regime"]
        
        # Extract features
        features = extract_features_from_pick(pick, forecasts, uw_flow)
        pick["_features"] = features
        pick["_regime"] = regime
        
        # Apply Policy B v2
        if engine == "MOONSHOT":
            passed_pb, pb_reasons = apply_policy_b_v2_moonshot(pick)
        else:
            passed_pb, pb_reasons = apply_policy_b_v2_puts(pick)
        
        if not passed_pb:
            pick["_reject_reason"] = "Policy B v2"
            pick["_reject_details"] = pb_reasons
            rejected_by_policy_b.append(pick)
            continue
        
        # Apply regime hard block
        blocked, block_reasons = apply_regime_hard_block(pick, features, regime)
        if blocked:
            pick["_reject_reason"] = "Regime Hard Block"
            pick["_reject_details"] = block_reasons
            rejected_by_regime.append(pick)
            continue
        
        # Passed all gates
        pick["_new_code_passed"] = True
        new_code_passed.append(pick)
    
    log.info(f"\n  Results:")
    log.info(f"    Policy B v2 passed: {len(new_code_passed) + len(rejected_by_regime)}")
    log.info(f"    Regime hard block: {len(rejected_by_regime)} removed")
    log.info(f"    Final NEW CODE picks: {len(new_code_passed)}")
    log.info(f"    Rejected by Policy B: {len(rejected_by_policy_b)}")
    
    # ── Win Rate Analysis ────────────────────────────────────────
    log.info(f"\n{'='*80}")
    log.info(f"  WIN RATE ANALYSIS (NEW CODE PICKS)")
    log.info(f"{'='*80}")
    
    # Apply cost model
    for p in new_code_passed:
        price = p.get("pick_price", 0)
        if price < 50:
            cost = 10.0
        elif price < 200:
            cost = 5.0
        else:
            cost = 3.0
        raw_pnl = p.get("options_pnl_pct", 0)
        p["_net_pnl_pct"] = round(raw_pnl - cost, 1)
        p["_cost_pct"] = cost
    
    # Win definitions
    for def_name, def_key, threshold in [
        ("Any Profit (>0%)", "is_winner_any", 0),
        ("Tradeable Win (≥+10%)", "is_winner_tradeable", 10),
        ("Edge Win (≥+20%)", "is_winner_edge", 20),
    ]:
        winners = [p for p in new_code_passed if p.get("options_pnl_pct", 0) >= threshold]
        wr = len(winners) / len(new_code_passed) * 100 if new_code_passed else 0
        
        # After costs
        winners_net = [p for p in new_code_passed if p.get("_net_pnl_pct", 0) >= threshold]
        wr_net = len(winners_net) / len(new_code_passed) * 100 if new_code_passed else 0
        
        log.info(f"\n  {def_name}:")
        log.info(f"    Raw: {len(winners)}/{len(new_code_passed)} = {wr:.1f}%")
        log.info(f"    Net (after costs): {len(winners_net)}/{len(new_code_passed)} = {wr_net:.1f}%")
        
        # By engine
        for eng in ["PUTS", "MOONSHOT"]:
            eng_picks = [p for p in new_code_passed if p["engine"] == eng]
            eng_winners = [p for p in eng_picks if p.get("options_pnl_pct", 0) >= threshold]
            eng_wr = len(eng_winners) / len(eng_picks) * 100 if eng_picks else 0
            log.info(f"      {eng}: {len(eng_winners)}/{len(eng_picks)} = {eng_wr:.1f}%")
    
    # Per-scan breakdown
    log.info(f"\n  Per-Scan Win Rate (Tradeable ≥+10%):")
    scan_groups = defaultdict(list)
    for p in new_code_passed:
        scan_groups[p.get("session", "")].append(p)
    
    for session in sorted(scan_groups.keys()):
        picks = scan_groups[session]
        wins = [p for p in picks if p.get("options_pnl_pct", 0) >= 10]
        wr = len(wins) / len(picks) * 100 if picks else 0
        regime = picks[0]["_regime"] if picks else "UNKNOWN"
        log.info(f"    {session:20s} | {len(wins)}/{len(picks)} = {wr:>5.1f}% | Regime: {regime}")
    
    # ── Expectancy Metrics ───────────────────────────────────────
    log.info(f"\n  Expectancy Metrics (after costs):")
    pnls = [p["_net_pnl_pct"] for p in new_code_passed]
    if pnls:
        gains = [x for x in pnls if x > 0]
        losses = [x for x in pnls if x <= 0]
        total_gain = sum(gains) if gains else 0
        total_loss = abs(sum(losses)) if losses else 0
        pf = total_gain / total_loss if total_loss > 0 else (float('inf') if total_gain > 0 else 0)
        
        log.info(f"    Mean: {statistics.mean(pnls):+.1f}%")
        log.info(f"    Median: {statistics.median(pnls):+.1f}%")
        log.info(f"    Profit Factor: {pf:.2f}x")
        log.info(f"    Best: {max(pnls):+.1f}%")
        log.info(f"    Worst: {min(pnls):+.1f}%")
        log.info(f"    Total Gain / Total Loss: {total_gain:+.1f} / {total_loss:.1f}")
    
    # ── Pick-by-Pick Table ────────────────────────────────────────
    log.info(f"\n{'='*80}")
    log.info(f"  NEW CODE PICKS — DETAILED TABLE")
    log.info(f"{'='*80}")
    
    log.info(f"\n{'Sym':<8s} {'Engine':<8s} {'Session':<20s} {'Regime':<13s} {'Stock%':>7s} "
             f"{'RawPnL':>7s} {'NetPnL':>7s} {'Gate':>12s} {'Features':<30s}")
    log.info("-" * 120)
    
    for p in sorted(new_code_passed, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
        feat = p.get("_features", {})
        feat_str = " ".join([
            "IV" if feat.get("iv_inverted") else "",
            "CB" if feat.get("call_buying") else "",
            "BF" if feat.get("bullish_flow") else "",
            "NG" if feat.get("neg_gex_explosive") else "",
        ]).strip() or "—"
        
        raw = p.get("options_pnl_pct", 0)
        w = "🏆" if raw >= 20 else ("✅" if raw >= 10 else ("🟡" if raw > 0 else "❌"))
        
        log.info(
            f"{w} {p['symbol']:<6s} {p['engine']:<8s} {p.get('session', ''):<20s} "
            f"{p['_regime']:<13s} {p.get('stock_move_pct', 0):>+6.1f}% "
            f"{raw:>+6.1f}% {p.get('_net_pnl_pct', 0):>+6.1f}% "
            f"{'ALLOW':>12s} {feat_str:<30s}"
        )
    
    # ── What Was Blocked ──────────────────────────────────────────
    log.info(f"\n{'='*80}")
    log.info(f"  WHAT WAS BLOCKED (and outcomes)")
    log.info(f"{'='*80}")
    
    # Regime hard blocks
    if rejected_by_regime:
        log.info(f"\n  Regime Hard Block ({len(rejected_by_regime)} picks):")
        for p in sorted(rejected_by_regime, key=lambda x: x.get("options_pnl_pct", 0)):
            raw = p.get("options_pnl_pct", 0)
            w = "✅" if raw > 0 else "❌"
            log.info(f"    {w} {p['symbol']:<8s} {p.get('session', ''):<20s} "
                    f"PnL={raw:>+6.1f}% | {p.get('_reject_details', [''])[0]}")
    
    # Policy B rejects (show worst losers that were correctly blocked)
    worst_rejects = sorted(rejected_by_policy_b, key=lambda x: x.get("options_pnl_pct", 0))[:10]
    if worst_rejects:
        log.info(f"\n  Policy B Rejects — Worst Losers (correctly blocked):")
        for p in worst_rejects:
            raw = p.get("options_pnl_pct", 0)
            log.info(f"    ❌ {p['symbol']:<8s} {p.get('session', ''):<20s} "
                    f"PnL={raw:>+6.1f}% | {p.get('_reject_details', [''])[0]}")
    
    # ── Save Results ─────────────────────────────────────────────
    results = {
        "generated": datetime.now().isoformat(),
        "total_picks_considered": len(ok_picks),
        "new_code_passed": len(new_code_passed),
        "rejected_policy_b": len(rejected_by_policy_b),
        "rejected_regime": len(rejected_by_regime),
        "new_code_picks": [{
            "symbol": p["symbol"],
            "engine": p["engine"],
            "session": p.get("session", ""),
            "regime": p["_regime"],
            "stock_move_pct": p.get("stock_move_pct", 0),
            "options_pnl_pct": p.get("options_pnl_pct", 0),
            "net_pnl_pct": p.get("_net_pnl_pct", 0),
            "features": p.get("_features", {}),
        } for p in new_code_passed],
        "win_rates": {
            "any_profit": len([p for p in new_code_passed if p.get("options_pnl_pct", 0) > 0]) / len(new_code_passed) * 100 if new_code_passed else 0,
            "tradeable_10": len([p for p in new_code_passed if p.get("options_pnl_pct", 0) >= 10]) / len(new_code_passed) * 100 if new_code_passed else 0,
            "edge_20": len([p for p in new_code_passed if p.get("options_pnl_pct", 0) >= 20]) / len(new_code_passed) * 100 if new_code_passed else 0,
        },
    }
    
    json_path = OUTPUT / "new_code_backtest_feb9_13.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info(f"\n  💾 Results saved: {json_path}")
    
    # Generate detailed report
    generate_detailed_report(results, new_code_passed, rejected_by_regime, rejected_by_policy_b)
    
    log.info("\n" + "=" * 80)
    log.info("  ✅ ANALYSIS COMPLETE")
    log.info("=" * 80)


def generate_detailed_report(results: Dict, passed: List, rejected_regime: List,
                            rejected_pb: List) -> None:
    """Generate institutional-grade markdown report."""
    rpt = []
    rpt.append("# NEW CODE BACKTEST — Feb 9-13, 2026")
    rpt.append("## 'What Would the NEW System Pick?'")
    rpt.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    
    rpt.append("---\n## EXECUTIVE SUMMARY\n")
    rpt.append(f"**Total Picks Considered:** {results['total_picks_considered']}")
    rpt.append(f"**NEW CODE Passed:** {results['new_code_passed']}")
    rpt.append(f"**Rejected by Policy B:** {results['rejected_policy_b']}")
    rpt.append(f"**Rejected by Regime Hard Block:** {results['rejected_regime']}")
    rpt.append(f"\n**Win Rates (NEW CODE picks):**")
    rpt.append(f"- Any Profit (>0%): {results['win_rates']['any_profit']:.1f}%")
    rpt.append(f"- Tradeable (≥+10%): {results['win_rates']['tradeable_10']:.1f}%")
    rpt.append(f"- Edge (≥+20%): {results['win_rates']['edge_20']:.1f}%")
    rpt.append(f"\n**Target:** 80% win rate (Tradeable definition)\n")
    
    # Per-scan breakdown
    rpt.append("---\n## PER-SCAN BREAKDOWN\n")
    rpt.append("| Scan | Regime | Picks | Wins (≥+10%) | WR |")
    rpt.append("|------|--------|-------|-------------|-----|")
    
    scan_groups = defaultdict(list)
    for p in passed:
        scan_groups[p.get("session", "")].append(p)
    
    for session in sorted(scan_groups.keys()):
        picks = scan_groups[session]
        wins = [p for p in picks if p.get("options_pnl_pct", 0) >= 10]
        wr = len(wins) / len(picks) * 100 if picks else 0
        regime = picks[0]["_regime"] if picks else "UNKNOWN"
        rpt.append(f"| {session} | {regime} | {len(picks)} | {len(wins)} | {wr:.0f}% |")
    
    # Recommendations
    rpt.append("\n---\n## RECOMMENDATIONS (No Fixes — Analysis Only)\n")
    
    wr_tradeable = results['win_rates']['tradeable_10']
    if wr_tradeable < 80:
        gap = 80 - wr_tradeable
        rpt.append(f"### Current Performance vs Target")
        rpt.append(f"- **Current WR:** {wr_tradeable:.1f}%")
        rpt.append(f"- **Target WR:** 80%")
        rpt.append(f"- **Gap:** {gap:.1f}pp\n")
        
        rpt.append("### Recommendations to Close the Gap:\n")
        rpt.append("1. **Tighten Policy B gates further:**")
        rpt.append("   - Raise MIN_SIGNAL_COUNT from 5 → 6 or 7")
        rpt.append("   - Raise MIN_BASE_SCORE for moonshot from 0.65 → 0.70")
        rpt.append("   - Raise MIN_MOVE_POTENTIAL from 0.50 → 0.60")
        rpt.append("")
        rpt.append("2. **Expand regime hard block:**")
        rpt.append("   - Currently only blocks: moonshot + bearish_flow + bear_regime")
        rpt.append("   - Consider blocking moonshot in STRONG_BEAR unless iv_inverted OR institutional")
        rpt.append("")
        rpt.append("3. **Signal quality filter:**")
        rpt.append("   - Require at least 1 'premium' signal (iv_inverted, call_buying, dark_pool, neg_gex)")
        rpt.append("   - Demote low-edge signals (momentum, sweep) from scoring")
        rpt.append("")
        rpt.append("4. **UW flow gate:**")
        rpt.append("   - Block moonshots with bearish_flow (put premium >60%) in ALL regimes")
        rpt.append("   - Not just bear regimes")
        rpt.append("")
        rpt.append("5. **Regime-aware scoring:**")
        rpt.append("   - Apply regime multiplier to scores (not just gates)")
        rpt.append("   - Bull regime: +10% score boost")
        rpt.append("   - Bear regime: -20% score penalty")
    else:
        rpt.append("### ✅ Target Achieved!")
        rpt.append(f"Win rate of {wr_tradeable:.1f}% exceeds 80% target.\n")
    
    rpt.append("### What Worked Well:\n")
    winners = [p for p in passed if p.get("options_pnl_pct", 0) >= 10]
    if winners:
        winner_features = defaultdict(int)
        for w in winners:
            feat = w.get("_features", {})
            for key, val in feat.items():
                if isinstance(val, bool) and val:
                    winner_features[key] += 1
        
        rpt.append("| Feature | Frequency | % of Winners |")
        rpt.append("|---------|-----------|--------------|")
        for feat, count in sorted(winner_features.items(), key=lambda x: x[1], reverse=True)[:10]:
            pct = count / len(winners) * 100
            rpt.append(f"| `{feat}` | {count} | {pct:.0f}% |")
    
    rpt.append("\n### What Was Correctly Blocked:\n")
    if rejected_regime:
        rpt.append(f"**Regime Hard Block ({len(rejected_regime)} picks):**")
        for p in sorted(rejected_regime, key=lambda x: x.get("options_pnl_pct", 0))[:5]:
            raw = p.get("options_pnl_pct", 0)
            rpt.append(f"- {p['symbol']}: {raw:+.1f}% | {p.get('_reject_details', [''])[0]}")
    
    report_path = OUTPUT / "NEW_CODE_BACKTEST_FEB9_13.md"
    with open(report_path, "w") as f:
        f.write("\n".join(rpt))
    log.info(f"  📄 Report saved: {report_path}")


if __name__ == "__main__":
    main()
