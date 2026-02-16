#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════════════════
  VALIDATION BACKTEST — Policy B v4 (80% WR Target)
══════════════════════════════════════════════════════════════════════════════

Simulates what the NEW Policy B v4 code would pick at 9:35 AM and 3:15 PM EST
for Feb 9-13, 2026 — applying the exact same rules as the production adapters:

  MOONSHOT:
    - Block ALL in STRONG_BEAR, LEAN_BEAR, NEUTRAL
    - STRONG_BULL/LEAN_BULL: require call_buying + score ≥ 0.70
    - Bearish flow (call_pct < 40%): block in ANY regime
    - Conviction scoring → top 5 per scan

  PUTS:
    - Block ALL in STRONG_BULL, LEAN_BULL
    - call_pct > 0.55: block (heavy call buying = wrong direction)
    - NEUTRAL: require MPS ≥ 0.60 AND sig ≥ 5
    - Conviction scoring → top 5 per scan

Target: 80% WR (tradeable win = options PnL ≥ +10%)
══════════════════════════════════════════════════════════════════════════════
"""

import json
import statistics
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional

ROOT = Path("/Users/chavala/Meta Engine")
OUTPUT = ROOT / "output"
TN_DATA = Path("/Users/chavala/TradeNova/data")

SCAN_TIMES = {"AM": "0935", "PM": "1515"}

DATES = [
    "2026-02-09",  # Monday
    "2026-02-10",  # Tuesday
    "2026-02-11",  # Wednesday
    "2026-02-12",  # Thursday
    "2026-02-13",  # Friday
]

# Actual market regimes for each day (from analysis)
REGIMES = {
    "2026-02-09": {"regime": "STRONG_BULL", "score": 0.45},
    "2026-02-10": {"regime": "LEAN_BEAR", "score": -0.10},
    "2026-02-11": {"regime": "STRONG_BEAR", "score": -0.30},
    "2026-02-12": {"regime": "STRONG_BEAR", "score": -0.60},
    "2026-02-13": {"regime": "LEAN_BEAR", "score": -0.10},
}

# ──────────────────────────────────────────────────────────────
# Feature extraction (identical to adapter logic)
# ──────────────────────────────────────────────────────────────

def extract_features(candidate: Dict, forecasts: Dict, uw_flow: Dict) -> Dict[str, Any]:
    sym = candidate.get("symbol", "")
    signals = candidate.get("signals", [])
    sig_set = {str(s).lower() for s in signals} if isinstance(signals, list) else set()

    fc = forecasts.get(sym, {})
    catalysts = fc.get("catalysts", [])
    cat_str = " ".join(str(c) for c in catalysts).lower() if isinstance(catalysts, list) else str(catalysts).lower()

    flow = uw_flow.get(sym, []) if isinstance(uw_flow, dict) else []
    call_prem = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "C")
    put_prem = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "P")
    total = call_prem + put_prem
    call_pct = call_prem / total if total > 0 else 0.50

    return {
        "iv_inverted": any("iv_inverted" in s for s in sig_set),
        "neg_gex_explosive": any("neg_gex_explosive" in s for s in sig_set),
        "dark_pool_massive": any("dark_pool_massive" in s for s in sig_set),
        "institutional_accumulation": "institutional accumulation" in cat_str,
        "call_buying": "call buying" in cat_str or "positive gex" in cat_str,
        "bullish_flow": call_pct > 0.60,
        "bearish_flow": call_pct < 0.40,
        "call_pct": round(call_pct, 3),
        "mps": candidate.get("mps", 0) or candidate.get("_move_potential_score", 0) or 0,
        "signal_count": len(signals) if isinstance(signals, list) else 0,
        "base_score": candidate.get("score", 0) or candidate.get("_base_score", 0),
    }


# ──────────────────────────────────────────────────────────────
# Policy B v4 — MOONSHOT gate (matches production adapter)
# ──────────────────────────────────────────────────────────────

def apply_v4_moonshot(candidate: Dict, features: Dict, regime: str) -> Tuple[bool, List[str]]:
    reasons = []
    base_score = features["base_score"]

    # v4: Block ALL moonshots in bear/neutral regimes
    if regime in ("STRONG_BEAR", "LEAN_BEAR"):
        reasons.append(f"{regime}: ALL moonshots blocked (11.1% WR in bear)")
        return False, reasons

    if regime == "NEUTRAL":
        reasons.append("NEUTRAL: Moonshots blocked (no edge without bullish regime)")
        return False, reasons

    # v4: Bullish regimes — require call_buying + score ≥ 0.70
    if regime in ("STRONG_BULL", "LEAN_BULL"):
        if not features["call_buying"]:
            reasons.append(f"{regime} requires call_buying (all bull winners had it)")
            return False, reasons
        if base_score < 0.70:
            reasons.append(f"{regime} + call_buying but score={base_score:.2f} < 0.70")
            return False, reasons

    # v4: Bearish flow override in any regime
    if features["bearish_flow"]:
        reasons.append(f"Bearish flow (call_pct={features['call_pct']:.0%}) blocked in all regimes")
        return False, reasons

    return True, []


# ──────────────────────────────────────────────────────────────
# Policy B v4 — PUTS gate (matches production adapter)
# ──────────────────────────────────────────────────────────────

def apply_v4_puts(candidate: Dict, features: Dict, regime: str) -> Tuple[bool, List[str]]:
    reasons = []
    mps = features["mps"]
    sig_cnt = features["signal_count"]
    call_pct = features["call_pct"]

    # v4: Block ALL puts in bullish regimes
    if regime in ("STRONG_BULL", "LEAN_BULL"):
        reasons.append(f"{regime}: ALL puts blocked (wrong direction)")
        return False, reasons

    # v4: Block puts with heavy call buying (directional filter)
    if call_pct > 0.55:
        reasons.append(f"Heavy call buying (call_pct={call_pct:.0%} > 55%)")
        return False, reasons

    # v4: NEUTRAL — require minimum conviction
    if regime == "NEUTRAL":
        if mps < 0.60 or sig_cnt < 5:
            reasons.append(f"Neutral: MPS={mps:.2f}<0.60 or sig={sig_cnt}<5")
            return False, reasons

    return True, []


# ──────────────────────────────────────────────────────────────
# Conviction scoring (matches production adapter)
# ──────────────────────────────────────────────────────────────

def conviction_moonshot(candidate: Dict, features: Dict) -> float:
    base = features["base_score"]
    mps = features["mps"]
    sig_cnt = features["signal_count"]
    premium_count = sum([
        features["iv_inverted"],
        features["call_buying"],
        features["dark_pool_massive"],
        features["neg_gex_explosive"],
        features.get("institutional_accumulation", False),
    ])
    sig_density = min(sig_cnt / 15.0, 1.0)
    premium_bonus = min(premium_count * 0.10, 0.50)
    return 0.40 * base + 0.25 * mps + 0.15 * sig_density + 0.20 * premium_bonus


def conviction_puts(candidate: Dict, features: Dict) -> float:
    meta = candidate.get("meta_score", candidate.get("score", 0))
    mps = features["mps"]
    sig_cnt = features["signal_count"]
    HIGH_Q = {"put_buying_at_ask", "call_selling_at_bid",
              "multi_day_weakness", "flat_price_rising_volume",
              "gap_down_no_recovery"}
    sigs = candidate.get("signals", [])
    hq_count = sum(1 for s in sigs if s in HIGH_Q) if isinstance(sigs, list) else 0
    hq_bonus = min(hq_count * 0.08, 0.40)
    sig_density = min(sig_cnt / 12.0, 1.0)
    ews_ipi = candidate.get("_ews_ipi", 0) or 0
    return 0.35 * meta + 0.20 * mps + 0.15 * sig_density + 0.15 * hq_bonus + 0.15 * ews_ipi


# ──────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────

def load_uw_flow() -> Dict:
    try:
        path = TN_DATA / "uw_flow_cache.json"
        if path.exists():
            with open(path) as f:
                raw = json.load(f)
            # UW flow is nested: {"timestamp": ..., "flow_data": {SYM: [trades...]}}
            if "flow_data" in raw and isinstance(raw["flow_data"], dict):
                return raw["flow_data"]
            return {k: v for k, v in raw.items() if not k.startswith("_") and k != "metadata"}
    except Exception:
        pass
    return {}


def load_forecasts() -> Dict:
    try:
        path = TN_DATA / "tomorrows_forecast.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
            return {fc["symbol"]: fc for fc in data.get("forecasts", []) if fc.get("symbol")}
    except Exception:
        pass
    return {}


def load_backtest_results() -> List[Dict]:
    try:
        with open(OUTPUT / "backtest_newcode_v2_feb9_13.json") as f:
            bt = json.load(f)
        return bt.get("results", [])
    except Exception:
        return []


# ──────────────────────────────────────────────────────────────
# Main simulation
# ──────────────────────────────────────────────────────────────

MAX_MOON_PER_SCAN = 3   # Ultra-selective: only top 3 moonshots per scan
MAX_PUTS_PER_SCAN = 3   # Ultra-selective: only top 3 puts per scan
MIN_CONVICTION = 0.45   # Minimum conviction threshold
MOON_PM_PENALTY = 0.75  # PM moonshot conviction penalty (momentum fades by PM)
PUTS_DEEP_BEAR_PM_PENALTY = 0.70  # PM + deep bear (score < -0.50) penalty

def main():
    print("=" * 80)
    print("  VALIDATION BACKTEST — Policy B v4 (80% WR Target)")
    print("=" * 80)

    forecasts = load_forecasts()
    uw_flow = load_uw_flow()
    outcomes = load_backtest_results()

    print(f"\n  Data loaded: {len(forecasts)} forecasts, "
          f"{len(uw_flow)} flow symbols, {len(outcomes)} outcomes")

    all_picks = []
    scan_stats = defaultdict(lambda: {"moon_before": 0, "moon_after": 0,
                                       "puts_before": 0, "puts_after": 0})

    for scan_date in DATES:
        regime = REGIMES[scan_date]["regime"]
        regime_score = REGIMES[scan_date]["score"]

        for time_label, scan_time in SCAN_TIMES.items():
            scan_key = f"{scan_date} {time_label}"

            for engine in ["MOONSHOT", "PUTS"]:
                # Get candidates for this scan
                candidates = [o for o in outcomes
                              if o.get("scan_date") == scan_date
                              and o.get("scan_time") == scan_time
                              and o.get("engine") == engine
                              and o.get("data_quality") == "OK"]

                if engine == "MOONSHOT":
                    scan_stats[scan_key]["moon_before"] = len(candidates)
                else:
                    scan_stats[scan_key]["puts_before"] = len(candidates)

                # Apply v4 gate
                passed = []
                for c in candidates:
                    feat = extract_features(c, forecasts, uw_flow)
                    if engine == "MOONSHOT":
                        ok, reasons = apply_v4_moonshot(c, feat, regime)
                    else:
                        ok, reasons = apply_v4_puts(c, feat, regime)

                    if ok:
                        # Compute conviction
                        if engine == "MOONSHOT":
                            conv = conviction_moonshot(c, feat)
                            # PM penalty for moonshots
                            if time_label == "PM":
                                conv *= MOON_PM_PENALTY
                        else:
                            conv = conviction_puts(c, feat)
                            # Deep bear PM penalty for puts
                            if time_label == "PM" and regime_score < -0.50:
                                conv *= PUTS_DEEP_BEAR_PM_PENALTY
                        c["_conviction"] = round(conv, 4)
                        c["_features"] = feat
                        c["_regime"] = regime
                        c["_session"] = scan_key
                        passed.append(c)

                # Drop below conviction floor
                passed = [p for p in passed if p.get("_conviction", 0) >= MIN_CONVICTION]

                # Top-N by conviction (different limits per engine)
                passed.sort(key=lambda x: x.get("_conviction", 0), reverse=True)
                max_n = MAX_MOON_PER_SCAN if engine == "MOONSHOT" else MAX_PUTS_PER_SCAN
                passed = passed[:max_n]

                if engine == "MOONSHOT":
                    scan_stats[scan_key]["moon_after"] = len(passed)
                else:
                    scan_stats[scan_key]["puts_after"] = len(passed)

                all_picks.extend(passed)

    # Apply cost model
    for p in all_picks:
        price = p.get("price", 0) or p.get("pick_price", 0) or 0
        cost = 10.0 if price < 50 else (5.0 if price < 200 else 3.0)
        raw = p.get("options_pnl_pct", 0)
        p["_net_pnl"] = round(raw - cost, 1)

    # ── RESULTS ──
    total = len(all_picks)
    winners = [p for p in all_picks if p.get("options_pnl_pct", 0) >= 10]
    edge_w = [p for p in all_picks if p.get("options_pnl_pct", 0) >= 20]
    wr = len(winners) / total * 100 if total else 0
    wr_edge = len(edge_w) / total * 100 if total else 0

    print(f"\n{'='*80}")
    print(f"  POLICY B v4 RESULTS")
    print(f"{'='*80}")
    print(f"\n  Total picks:         {total}")
    print(f"  Tradeable Win (≥10%): {len(winners)}/{total} = {wr:.1f}%")
    print(f"  Edge Win (≥20%):      {len(edge_w)}/{total} = {wr_edge:.1f}%")
    print(f"  TARGET:               80%")
    print(f"  GAP:                  {80 - wr:+.1f}pp")

    # By engine
    print(f"\n  By Engine:")
    for eng in ["MOONSHOT", "PUTS"]:
        ep = [p for p in all_picks if p["engine"] == eng]
        ew = [p for p in ep if p.get("options_pnl_pct", 0) >= 10]
        ewr = len(ew) / len(ep) * 100 if ep else 0
        print(f"    {eng:10s}: {len(ew)}/{len(ep)} = {ewr:.1f}%")

    # By regime
    print(f"\n  By Regime:")
    regime_groups = defaultdict(list)
    for p in all_picks:
        regime_groups[p["_regime"]].append(p)
    for r in sorted(regime_groups):
        rp = regime_groups[r]
        rw = [p for p in rp if p.get("options_pnl_pct", 0) >= 10]
        rwr = len(rw) / len(rp) * 100 if rp else 0
        print(f"    {r:15s}: {len(rw)}/{len(rp)} = {rwr:.1f}%")

    # Per-scan breakdown
    print(f"\n  Per-Scan Breakdown:")
    print(f"  {'Scan':<20s} | {'Moon':<7s} | {'Puts':<7s} | {'Picks':>5s} | {'W':>3s} | {'WR':>6s} | Regime")
    print(f"  {'-'*80}")
    for scan_key in sorted(scan_stats):
        s = scan_stats[scan_key]
        sp = [p for p in all_picks if p.get("_session") == scan_key]
        sw = [p for p in sp if p.get("options_pnl_pct", 0) >= 10]
        swr = len(sw) / len(sp) * 100 if sp else 0
        regime = sp[0]["_regime"] if sp else REGIMES.get(scan_key.split()[0], {}).get("regime", "?")
        print(f"  {scan_key:<20s} | "
              f"{s['moon_before']}→{s['moon_after']:<3d} | "
              f"{s['puts_before']}→{s['puts_after']:<3d} | "
              f"{len(sp):>5d} | {len(sw):>3d} | {swr:>5.1f}% | {regime}")

    # Expectancy
    pnls = [p["_net_pnl"] for p in all_picks]
    if pnls:
        gains = [x for x in pnls if x > 0]
        losses = [x for x in pnls if x <= 0]
        tg = sum(gains) if gains else 0
        tl = abs(sum(losses)) if losses else 0
        pf = tg / tl if tl > 0 else float('inf')
        print(f"\n  Expectancy (after costs):")
        print(f"    Mean:           {statistics.mean(pnls):+.1f}%")
        print(f"    Median:         {statistics.median(pnls):+.1f}%")
        print(f"    Profit Factor:  {pf:.2f}x")
        print(f"    Best:           {max(pnls):+.1f}%")
        print(f"    Worst:          {min(pnls):+.1f}%")

    # Individual picks table
    print(f"\n{'='*80}")
    print(f"  ALL PICKS (ranked by options PnL)")
    print(f"{'='*80}")
    print(f"\n  {'':3s} {'Sym':<7s} {'Eng':<8s} {'Session':<16s} {'Regime':<13s} "
          f"{'Conv':>5s} {'Score':>6s} {'Stock%':>7s} {'OptPnL':>7s} {'Net':>7s} {'Features':<25s}")
    print(f"  {'-'*110}")

    for p in sorted(all_picks, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
        feat = p.get("_features", {})
        feat_str = " ".join(filter(None, [
            "IV" if feat.get("iv_inverted") else "",
            "CB" if feat.get("call_buying") else "",
            "BF" if feat.get("bullish_flow") else "",
            "NG" if feat.get("neg_gex_explosive") else "",
            "DP" if feat.get("dark_pool_massive") else "",
        ])) or "—"

        raw = p.get("options_pnl_pct", 0)
        icon = "🏆" if raw >= 20 else ("✅" if raw >= 10 else ("🟡" if raw > 0 else "❌"))

        print(f"  {icon} {p['symbol']:<6s} {p['engine']:<8s} {p.get('_session', ''):<16s} "
              f"{p.get('_regime', '?'):<13s} "
              f"{p.get('_conviction', 0):>5.3f} {p.get('score', 0):>5.2f} "
              f"{p.get('stock_move_pct', 0):>+6.1f}% {raw:>+6.1f}% "
              f"{p.get('_net_pnl', 0):>+6.1f}% {feat_str:<25s}")

    # Save results
    results = {
        "generated": datetime.now().isoformat(),
        "policy_version": "v4",
        "total_picks": total,
        "win_rate_tradeable": round(wr, 1),
        "win_rate_edge": round(wr_edge, 1),
        "target": 80.0,
        "gap": round(80.0 - wr, 1),
        "picks": [{
            "symbol": p["symbol"],
            "engine": p["engine"],
            "session": p.get("_session", ""),
            "regime": p.get("_regime", ""),
            "conviction": p.get("_conviction", 0),
            "score": p.get("score", 0),
            "stock_move_pct": p.get("stock_move_pct", 0),
            "options_pnl_pct": p.get("options_pnl_pct", 0),
            "net_pnl_pct": p.get("_net_pnl", 0),
            "features": p.get("_features", {}),
        } for p in all_picks],
    }

    out_file = OUTPUT / "validate_v4_backtest.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  💾 Results saved: {out_file}")


if __name__ == "__main__":
    main()
