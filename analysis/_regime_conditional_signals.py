#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════════════════
  REGIME-CONDITIONAL SIGNAL ANALYSIS — Layer 2 Deep Dive
══════════════════════════════════════════════════════════════════════════════

Key Question: Are certain signals only predictive in bullish regimes?
If so, the regime gate + signal filter combo could push WR much higher.

Also: Per-ticker analysis of MOONSHOT winners in STRONG_BEAR (the 9/19 that won).
What made them special? Can we create a "bear-market moonshot" filter?

Also: Deeper look at day-of-week × regime × engine interactions.
══════════════════════════════════════════════════════════════════════════════
"""

import json, os, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

ROOT = Path("/Users/chavala/Meta Engine")
OUTPUT = ROOT / "output"
TN_DATA = Path("/Users/chavala/TradeNova/data")

sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

POLYGON_KEY = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")

# Load the deep analysis results
with open(OUTPUT / "regime_deep_analysis_results.json") as f:
    deep = json.load(f)

regimes = deep["regimes"]

# Load backtest results
with open(OUTPUT / "backtest_newcode_v2_feb9_13.json") as f:
    bt = json.load(f)
picks = bt.get("results", [])

# Load forecasts for signal enrichment
try:
    with open(TN_DATA / "tomorrows_forecast.json") as f:
        fc_data = json.load(f)
    forecasts = {fc["symbol"]: fc for fc in fc_data.get("forecasts", []) if fc.get("symbol")}
except Exception:
    forecasts = {}

# Load UW flow
try:
    with open(TN_DATA / "uw_flow_cache.json") as f:
        uw_data = json.load(f)
    uw_flow = uw_data.get("flow_data", uw_data) if isinstance(uw_data, dict) else {}
except Exception:
    uw_flow = {}

print("=" * 80)
print("  REGIME-CONDITIONAL SIGNAL ANALYSIS — Layer 2 Deep Dive")
print("=" * 80)

# ═══════════════════════════════════════════════════════════════
# 1. ANNOTATE EACH PICK WITH SIGNALS + REGIME
# ═══════════════════════════════════════════════════════════════

def enrich_pick(p):
    """Add signal booleans and regime info to each pick."""
    sym = p.get("symbol", "")
    scan_date = p.get("scan_date", "")
    regime_info = regimes.get(scan_date, {"regime": "UNKNOWN", "score": 0})
    
    # Forecast signals
    fc = forecasts.get(sym, {})
    catalysts = fc.get("catalysts", [])
    cat_str = " ".join(str(c) for c in catalysts).lower() if isinstance(catalysts, list) else str(catalysts).lower()
    
    # UW flow
    flow = uw_flow.get(sym, []) if isinstance(uw_flow, dict) else []
    call_prem = sum(f.get("premium", 0) for f in flow if isinstance(f, dict) and f.get("put_call") == "C")
    put_prem = sum(f.get("premium", 0) for f in flow if isinstance(f, dict) and f.get("put_call") == "P")
    total = call_prem + put_prem
    call_pct = call_prem / total if total > 0 else 0.5
    
    # Signals from the pick itself
    signals = p.get("signals", [])
    sig_str = " ".join(str(s) for s in signals).lower() if isinstance(signals, list) else str(signals).lower()
    
    return {
        **p,
        "regime": regime_info["regime"],
        "regime_score": regime_info["score"],
        "has_call_buying": "call buying" in cat_str or "positive gex" in cat_str,
        "has_institutional": "institutional accumulation" in cat_str,
        "has_dark_pool": "dark_pool_massive" in sig_str,
        "has_neg_gex": "neg_gex_explosive" in sig_str,
        "has_vanna_crush": "vanna_crush" in sig_str,
        "has_iv_inverted": "iv_inverted" in sig_str,
        "has_sweep": "sweep" in sig_str,
        "has_support": "support" in sig_str,
        "has_oversold": "oversold" in sig_str,
        "has_momentum": "momentum" in sig_str,
        "has_insider": "insider" in sig_str or "congress" in sig_str,
        "has_rvol_spike": "rvol_spike" in sig_str,
        "call_pct": call_pct,
        "bullish_flow": call_pct > 0.60,
        "bearish_flow": call_pct < 0.40,
        "high_mps": p.get("mps", 0) >= 0.70,
        "high_signal_count": p.get("signal_count", 0) >= 7,
        "bp_high": fc.get("bullish_probability", 0) > 60,
    }


# Filter to OK + passed picks
ok_picks = [enrich_pick(p) for p in picks if p.get("data_quality") == "OK" and p.get("passed_policy_b")]
all_ok = [enrich_pick(p) for p in picks if p.get("data_quality") == "OK"]

moon_passed = [p for p in ok_picks if p["engine"] == "MOONSHOT"]
puts_passed = [p for p in ok_picks if p["engine"] == "PUTS"]

print(f"\nPolicy B passed picks: {len(ok_picks)} ({len(moon_passed)} Moon, {len(puts_passed)} Puts)")

# ═══════════════════════════════════════════════════════════════
# 2. REGIME-CONDITIONAL SIGNAL WR (KEY ANALYSIS)
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("  2. REGIME-CONDITIONAL SIGNAL WIN RATES — MOONSHOT ONLY")
print("=" * 80)

signal_keys = [
    "has_call_buying", "has_institutional", "has_dark_pool", "has_neg_gex",
    "has_vanna_crush", "has_iv_inverted", "has_sweep", "has_support",
    "has_oversold", "has_momentum", "has_insider", "has_rvol_spike",
    "bullish_flow", "bearish_flow", "high_mps", "high_signal_count", "bp_high"
]

regime_groups = {
    "BULL": ["STRONG_BULL", "LEAN_BULL"],
    "BEAR": ["STRONG_BEAR", "LEAN_BEAR"],
}

print(f"\n{'Signal':<28s} | {'BULL WR':>8s} {'(n)':>5s} | {'BEAR WR':>8s} {'(n)':>5s} | {'DELTA':>7s} | {'ALL WR':>8s} {'(n)':>5s}")
print("-" * 100)

sig_results = []
for sig in signal_keys:
    row = {"signal": sig}
    for grp_name, grp_regimes in regime_groups.items():
        subset = [p for p in moon_passed if p["regime"] in grp_regimes and p.get(sig)]
        wins = sum(1 for p in subset if p.get("is_winner"))
        total = len(subset)
        wr = wins / total * 100 if total >= 2 else -1  # -1 = insufficient data
        row[f"{grp_name}_wr"] = wr
        row[f"{grp_name}_n"] = total
    
    # All regimes
    all_subset = [p for p in moon_passed if p.get(sig)]
    all_wins = sum(1 for p in all_subset if p.get("is_winner"))
    all_total = len(all_subset)
    all_wr = all_wins / all_total * 100 if all_total >= 2 else -1
    row["ALL_wr"] = all_wr
    row["ALL_n"] = all_total
    
    delta = row.get("BULL_wr", 0) - row.get("BEAR_wr", 0) if row.get("BULL_wr", -1) >= 0 and row.get("BEAR_wr", -1) >= 0 else 0
    row["delta"] = delta
    
    bull_str = f"{row['BULL_wr']:>7.0f}%" if row["BULL_wr"] >= 0 else "    N/A"
    bear_str = f"{row['BEAR_wr']:>7.0f}%" if row["BEAR_wr"] >= 0 else "    N/A"
    all_str = f"{row['ALL_wr']:>7.0f}%" if row["ALL_wr"] >= 0 else "    N/A"
    delta_str = f"{delta:>+6.0f}pp" if delta != 0 else "    N/A"
    
    star = "⭐" if (row.get("BULL_wr", 0) >= 60 and row["BULL_n"] >= 2) else "  "
    bear_star = "🔻" if (row.get("BEAR_wr", 0) <= 35 and row["BEAR_n"] >= 3) else "  "
    
    print(f"{star}{bear_star} {sig:<24s} | {bull_str} ({row['BULL_n']:>3d}) | "
          f"{bear_str} ({row['BEAR_n']:>3d}) | {delta_str} | {all_str} ({row['ALL_n']:>3d})")
    
    sig_results.append(row)

# ═══════════════════════════════════════════════════════════════
# 3. MOONSHOT WINNERS IN STRONG_BEAR — The "Survivors"
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("  3. MOONSHOT WINNERS IN STRONG_BEAR — What Made Them Special?")
print("=" * 80)

bear_moon_winners = [p for p in moon_passed 
                     if p["regime"] in ("STRONG_BEAR", "LEAN_BEAR") and p.get("is_winner")]
bear_moon_losers = [p for p in moon_passed 
                    if p["regime"] in ("STRONG_BEAR", "LEAN_BEAR") and not p.get("is_winner")]

print(f"\nBear-regime moonshot winners: {len(bear_moon_winners)}")
print(f"Bear-regime moonshot losers: {len(bear_moon_losers)}")

print(f"\n{'Sym':<8s} {'Session':<20s} {'Stock%':>7s} {'OptPnL':>7s} {'Peak':>6s} {'MPS':>5s} {'Sigs':>5s} "
      f"{'CallBuy':>8s} {'DkPool':>7s} {'NegGEX':>7s} {'IVInv':>6s} {'BFlow':>6s} {'HighMPS':>8s}")
print("-" * 120)

for p in sorted(bear_moon_winners, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
    print(f"{p['symbol']:<8s} {p.get('session', ''):<20s} {p.get('stock_move_pct', 0):>+6.1f}% "
          f"{p.get('options_pnl_pct', 0):>+6.1f}% {p.get('peak_options_pnl_pct', 0):>5.0f}% "
          f"{p.get('mps', 0):>5.2f} {p.get('signal_count', 0):>5d} "
          f"{'✅' if p.get('has_call_buying') else '  ':>8s} "
          f"{'✅' if p.get('has_dark_pool') else '  ':>7s} "
          f"{'✅' if p.get('has_neg_gex') else '  ':>7s} "
          f"{'✅' if p.get('has_iv_inverted') else '  ':>6s} "
          f"{'✅' if p.get('bullish_flow') else '  ':>6s} "
          f"{'✅' if p.get('high_mps') else '  ':>8s}")

print(f"\n--- Bear-Regime Moonshot LOSERS ---")
for p in sorted(bear_moon_losers, key=lambda x: x.get("options_pnl_pct", 0)):
    print(f"{p['symbol']:<8s} {p.get('session', ''):<20s} {p.get('stock_move_pct', 0):>+6.1f}% "
          f"{p.get('options_pnl_pct', 0):>+6.1f}% {p.get('peak_options_pnl_pct', 0):>5.0f}% "
          f"{p.get('mps', 0):>5.2f} {p.get('signal_count', 0):>5d} "
          f"{'✅' if p.get('has_call_buying') else '  ':>8s} "
          f"{'✅' if p.get('has_dark_pool') else '  ':>7s} "
          f"{'✅' if p.get('has_neg_gex') else '  ':>7s} "
          f"{'✅' if p.get('has_iv_inverted') else '  ':>6s} "
          f"{'✅' if p.get('bullish_flow') else '  ':>6s} "
          f"{'✅' if p.get('high_mps') else '  ':>8s}")

# ═══════════════════════════════════════════════════════════════
# 4. COMPOUND SIGNAL COMBOS
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("  4. COMPOUND SIGNAL COMBOS — Moonshot Policy B Passed")
print("=" * 80)

combos = [
    ("regime_bull + call_buying", lambda p: p["regime"] in ("STRONG_BULL", "LEAN_BULL") and p["has_call_buying"]),
    ("regime_bull + high_mps", lambda p: p["regime"] in ("STRONG_BULL", "LEAN_BULL") and p["high_mps"]),
    ("regime_bull + bullish_flow", lambda p: p["regime"] in ("STRONG_BULL", "LEAN_BULL") and p["bullish_flow"]),
    ("regime_bull + iv_inverted", lambda p: p["regime"] in ("STRONG_BULL", "LEAN_BULL") and p["has_iv_inverted"]),
    ("call_buying + bullish_flow", lambda p: p["has_call_buying"] and p["bullish_flow"]),
    ("call_buying + high_mps", lambda p: p["has_call_buying"] and p["high_mps"]),
    ("call_buying + dark_pool", lambda p: p["has_call_buying"] and p["has_dark_pool"]),
    ("call_buying + neg_gex", lambda p: p["has_call_buying"] and p["has_neg_gex"]),
    ("iv_inverted + support", lambda p: p["has_iv_inverted"] and p["has_support"]),
    ("iv_inverted + neg_gex", lambda p: p["has_iv_inverted"] and p["has_neg_gex"]),
    ("high_mps + bullish_flow + call_buying", lambda p: p["high_mps"] and p["bullish_flow"] and p["has_call_buying"]),
    ("high_mps + high_sigs + bullish_flow", lambda p: p["high_mps"] and p["high_signal_count"] and p["bullish_flow"]),
    ("any_premium_signal", lambda p: p["has_call_buying"] or p["has_dark_pool"] or p["has_neg_gex"] or p["has_iv_inverted"]),
    ("two_premium_signals", lambda p: sum([p["has_call_buying"], p["has_dark_pool"], p["has_neg_gex"], p["has_iv_inverted"]]) >= 2),
    ("bear_regime + bullish_flow + call_buying", lambda p: p["regime"] in ("STRONG_BEAR", "LEAN_BEAR") and p["bullish_flow"] and p["has_call_buying"]),
    ("bear_regime + two_premium", lambda p: p["regime"] in ("STRONG_BEAR", "LEAN_BEAR") and sum([p["has_call_buying"], p["has_dark_pool"], p["has_neg_gex"], p["has_iv_inverted"]]) >= 2),
]

print(f"\n{'Combo':<45s} | {'WR':>6s} {'Wins':>5s} {'Total':>6s} {'AvgPnL':>8s}")
print("-" * 80)

combo_results = []
for name, fn in combos:
    subset = [p for p in moon_passed if fn(p)]
    wins = sum(1 for p in subset if p.get("is_winner"))
    total = len(subset)
    wr = wins / total * 100 if total > 0 else 0
    avg_pnl = sum(p.get("options_pnl_pct", 0) for p in subset) / total if total > 0 else 0
    
    star = " ⭐" if wr >= 60 and total >= 3 else ""
    warn = " ⚠️" if wr <= 35 and total >= 3 else ""
    
    print(f"{name:<45s} | {wr:>5.0f}% {wins:>5d} {total:>6d} {avg_pnl:>+7.1f}%{star}{warn}")
    combo_results.append({"combo": name, "wr": wr, "wins": wins, "total": total, "avg_pnl": avg_pnl})

# ═══════════════════════════════════════════════════════════════
# 5. WHAT IF ANALYSIS — Projected WR with Combined Filters
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("  5. WHAT-IF ANALYSIS — Projected Combined WR with Regime + Signal Filters")
print("=" * 80)

# Scenario 1: BULL_ONLY moonshot + all puts
# Scenario 2: BULL_ONLY + premium signal moonshot in bear
# Scenario 3: Regime gate + signal combo filter

scenarios = [
    ("BASELINE (no gate)", 
     lambda p: True,
     "All moonshots pass"),
    
    ("BULL_ONLY gate",
     lambda p: p["regime"] in ("STRONG_BULL", "LEAN_BULL"),
     "Block all moonshots in bear/neutral"),
    
    ("NON_BEAR + premium signal in bear",
     lambda p: (p["regime"] in ("STRONG_BULL", "LEAN_BULL", "NEUTRAL") or 
                (sum([p["has_call_buying"], p["has_dark_pool"], p["has_neg_gex"], p["has_iv_inverted"]]) >= 2 
                 and p["bullish_flow"])),
     "Allow moonshots in bear ONLY if 2+ premium signals AND bullish flow"),
    
    ("Regime gate + bullish_flow required in bear",
     lambda p: (p["regime"] in ("STRONG_BULL", "LEAN_BULL", "NEUTRAL") or p["bullish_flow"]),
     "Allow moonshots in bear only if bullish UW flow > 60% calls"),
    
    ("IV_INVERTED or INSTITUTIONAL only in bear",
     lambda p: (p["regime"] in ("STRONG_BULL", "LEAN_BULL", "NEUTRAL") or 
                p["has_iv_inverted"] or p["has_institutional"]),
     "Allow moonshots in bear only with highest-edge signals"),
    
    ("Score-weighted: regime * signal quality",
     lambda p: (p["regime_score"] > -0.10 or 
                (p.get("mps", 0) >= 0.75 and p["bullish_flow"] and 
                 sum([p["has_call_buying"], p["has_dark_pool"], p["has_neg_gex"], p["has_iv_inverted"]]) >= 2)),
     "Score threshold + compound signal override for exceptional setups"),
]

print(f"\n{'Scenario':<50s} | {'Moon WR':>8s} {'M#':>4s} | {'Puts WR':>8s} {'P#':>4s} | {'COMBINED':>9s} {'Tot':>4s} {'AvgPnL':>8s}")
print("-" * 110)

for name, moon_filter, desc in scenarios:
    passed_moon = [p for p in moon_passed if moon_filter(p)]
    moon_w = sum(1 for p in passed_moon if p.get("is_winner"))
    moon_n = len(passed_moon)
    moon_wr = moon_w / moon_n * 100 if moon_n > 0 else 0
    
    # Puts always pass
    puts_w = sum(1 for p in puts_passed if p.get("is_winner"))
    puts_n = len(puts_passed)
    puts_wr = puts_w / puts_n * 100 if puts_n > 0 else 0
    
    combined = passed_moon + puts_passed
    combined_w = sum(1 for p in combined if p.get("is_winner"))
    combined_n = len(combined)
    combined_wr = combined_w / combined_n * 100 if combined_n > 0 else 0
    combined_pnl = sum(p.get("options_pnl_pct", 0) for p in combined) / combined_n if combined_n > 0 else 0
    
    star = " ⭐" if combined_wr >= 60 else ""
    print(f"{name:<50s} | {moon_wr:>7.0f}% {moon_n:>3d} | {puts_wr:>7.0f}% {puts_n:>3d} | "
          f"{combined_wr:>8.0f}% {combined_n:>3d} {combined_pnl:>+7.1f}%{star}")
    print(f"  {'→ ' + desc}")

# ═══════════════════════════════════════════════════════════════
# 6. DAY-OF-WEEK × SESSION × REGIME MATRIX
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("  6. DAY-OF-WEEK × SESSION × ENGINE MATRIX (Policy B Passed)")
print("=" * 80)

session_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "total": 0, "sum_pnl": 0})
for p in ok_picks:
    session = p.get("session", "")
    engine = p.get("engine", "")
    is_win = p.get("is_winner", False)
    pnl = p.get("options_pnl_pct", 0)
    
    for key in [f"{engine}|{session}", f"ALL|{session}", f"{engine}|ALL", "ALL|ALL"]:
        session_stats[key]["total"] += 1
        session_stats[key]["sum_pnl"] += pnl
        if is_win:
            session_stats[key]["wins"] += 1
        else:
            session_stats[key]["losses"] += 1

print(f"\n{'Key':<35s} {'WR':>6s} {'Wins':>5s} {'Tot':>5s} {'AvgPnL':>8s}")
print("-" * 65)
for key in sorted(session_stats.keys()):
    s = session_stats[key]
    wr = s["wins"] / s["total"] * 100 if s["total"] > 0 else 0
    avg = s["sum_pnl"] / s["total"] if s["total"] > 0 else 0
    if s["total"] >= 2:
        print(f"{key:<35s} {wr:>5.0f}% {s['wins']:>5d} {s['total']:>5d} {avg:>+7.1f}%")

# ═══════════════════════════════════════════════════════════════
# 7. OPTIMAL COMPOSITE FILTER — THE MONEY SHOT
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("  7. OPTIMAL COMPOSITE FILTER — MOONSHOT QUALITY GATE")
print("=" * 80)

# Test the "smart gate": regime-aware + signal quality
def smart_gate(p):
    """
    The recommended composite filter:
    - In BULL regime: allow all moonshots
    - In NEUTRAL: require MPS >= 0.65 OR has_call_buying
    - In LEAN_BEAR: require 2+ premium signals AND bullish_flow
    - In STRONG_BEAR: require iv_inverted OR institutional OR (bullish_flow AND neg_gex)
    """
    regime = p["regime"]
    if regime in ("STRONG_BULL", "LEAN_BULL"):
        return True
    elif regime == "NEUTRAL":
        return p.get("mps", 0) >= 0.65 or p["has_call_buying"]
    elif regime == "LEAN_BEAR":
        premium = sum([p["has_call_buying"], p["has_dark_pool"], p["has_neg_gex"], p["has_iv_inverted"]])
        return premium >= 2 and p["bullish_flow"]
    elif regime == "STRONG_BEAR":
        return (p["has_iv_inverted"] or p["has_institutional"] or 
                (p["bullish_flow"] and p["has_neg_gex"]))
    return True

smart_moon = [p for p in moon_passed if smart_gate(p)]
smart_w = sum(1 for p in smart_moon if p.get("is_winner"))
smart_n = len(smart_moon)
smart_wr = smart_w / smart_n * 100 if smart_n > 0 else 0

combined = smart_moon + puts_passed
comb_w = sum(1 for p in combined if p.get("is_winner"))
comb_n = len(combined)
comb_wr = comb_w / comb_n * 100 if comb_n > 0 else 0
comb_pnl = sum(p.get("options_pnl_pct", 0) for p in combined) / comb_n if comb_n > 0 else 0

print(f"\n  SMART GATE Results:")
print(f"  Moonshot WR: {smart_wr:.1f}% ({smart_w}/{smart_n})")
print(f"  Combined WR: {comb_wr:.1f}% ({comb_w}/{comb_n})")
print(f"  Combined Avg PnL: {comb_pnl:+.1f}%")
print(f"  Moonshots blocked: {len(moon_passed) - smart_n}")
print(f"  Blocked WR: {sum(1 for p in moon_passed if not smart_gate(p) and p.get('is_winner'))}/{len(moon_passed) - smart_n}")

# Compare to baseline
print(f"\n  vs BASELINE:")
print(f"  Moon WR: 44.8% → {smart_wr:.1f}% ({smart_wr - 44.8:+.1f}pp)")
print(f"  Combined WR: 54.0% → {comb_wr:.1f}% ({comb_wr - 54.0:+.1f}pp)")
print(f"  Combined PnL: +2.4% → {comb_pnl:+.1f}% ({comb_pnl - 2.4:+.1f}pp)")

# Show what the smart gate would have allowed
print(f"\n  Smart Gate — Allowed Moonshots:")
for p in sorted(smart_moon, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
    w = "✅" if p.get("is_winner") else "❌"
    print(f"  {w} {p['symbol']:<8s} {p.get('session', ''):<20s} "
          f"Regime={p['regime']:<12s} PnL={p.get('options_pnl_pct', 0):>+6.1f}% "
          f"MPS={p.get('mps', 0):.2f}")

print(f"\n  Smart Gate — Blocked Moonshots:")
for p in sorted([p for p in moon_passed if not smart_gate(p)], 
                key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
    w = "✅" if p.get("is_winner") else "❌"
    print(f"  {w} {p['symbol']:<8s} {p.get('session', ''):<20s} "
          f"Regime={p['regime']:<12s} PnL={p.get('options_pnl_pct', 0):>+6.1f}% "
          f"MPS={p.get('mps', 0):.2f}")

print("\n" + "=" * 80)
print("  ✅ LAYER 2 ANALYSIS COMPLETE")
print("=" * 80)

# Save layer 2 results
layer2 = {
    "generated": datetime.now().isoformat(),
    "regime_conditional_signals": sig_results,
    "compound_combos": combo_results,
    "smart_gate": {
        "moon_wr": smart_wr,
        "moon_n": smart_n,
        "combined_wr": comb_wr,
        "combined_n": comb_n,
        "combined_avg_pnl": comb_pnl,
        "blocked": len(moon_passed) - smart_n,
    }
}
with open(OUTPUT / "regime_layer2_results.json", "w") as f:
    json.dump(layer2, f, indent=2, default=str)
print(f"\n💾 Saved: {OUTPUT / 'regime_layer2_results.json'}")
