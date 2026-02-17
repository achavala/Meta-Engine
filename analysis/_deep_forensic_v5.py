#!/usr/bin/env python3
"""Deep forensic analysis of backtest v5 results — blocked picks + coverage gaps."""

import json
import statistics
from collections import defaultdict, Counter
from pathlib import Path

OUTPUT = Path("/Users/chavala/Meta Engine/output")

# Regimes
REGIMES = {
    "2026-02-09": "STRONG_BULL",
    "2026-02-10": "LEAN_BEAR",
    "2026-02-11": "STRONG_BEAR",
    "2026-02-12": "STRONG_BEAR",
    "2026-02-13": "LEAN_BEAR",
}

def main():
    with open(OUTPUT / "comprehensive_backtest_v5.json") as f:
        results = json.load(f)
    with open(OUTPUT / "backtest_newcode_v2_feb9_13.json") as f:
        bt = json.load(f)
    
    all_outcomes = bt.get("results", [])
    all_ok = [r for r in all_outcomes if r.get("data_quality") == "OK"]
    
    picked_tuples = set()
    for p in results.get("picks", []):
        picked_tuples.add((p["symbol"], p["session"]))
    
    print("=" * 90)
    print("  DEEP FORENSIC ANALYSIS — BLOCKED PICKS + COVERAGE GAPS")
    print("=" * 90)
    
    # ═════════════════════════════════════════════════════════════════
    # 1. FULL CANDIDATE UNIVERSE — what was available vs what we picked
    # ═════════════════════════════════════════════════════════════════
    
    print("\n  1. CANDIDATE UNIVERSE (from backtest v2 outcomes with Polygon prices)")
    print("  " + "═" * 70)
    
    sessions = defaultdict(list)
    for r in all_ok:
        sd = r["scan_date"]
        st = "AM" if r["scan_time"] == "0935" else "PM"
        key = f"{sd} {st}"
        sessions[key].append(r)
    
    for session_key in sorted(sessions.keys()):
        recs = sessions[session_key]
        moon = [r for r in recs if r["engine"] == "MOONSHOT"]
        puts = [r for r in recs if r["engine"] == "PUTS"]
        moon_w = [r for r in moon if r.get("options_pnl_pct", 0) >= 10]
        puts_w = [r for r in puts if r.get("options_pnl_pct", 0) >= 10]
        
        print(f"\n  {session_key} [{REGIMES.get(session_key.split()[0], '?')}]")
        print(f"    MOONSHOT: {len(moon)} candidates, {len(moon_w)} would-win")
        for r in sorted(moon, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
            opt_pnl = r.get("options_pnl_pct", 0)
            stock = r.get("stock_move_pct", 0)
            picked = "← PICKED" if (r["symbol"], session_key) in picked_tuples else ""
            icon = "🏆" if opt_pnl >= 20 else ("✅" if opt_pnl >= 10 else ("🟡" if opt_pnl > 0 else "❌"))
            print(f"      {icon} {r['symbol']:7s} opt={opt_pnl:+6.1f}% stock={stock:+5.1f}% "
                  f"score={r.get('score', 0):.2f} sig={r.get('signal_count', 0)} {picked}")
        
        print(f"    PUTS: {len(puts)} candidates, {len(puts_w)} would-win")
        for r in sorted(puts, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
            opt_pnl = r.get("options_pnl_pct", 0)
            stock = r.get("stock_move_pct", 0)
            picked = "← PICKED" if (r["symbol"], session_key) in picked_tuples else ""
            icon = "🏆" if opt_pnl >= 20 else ("✅" if opt_pnl >= 10 else ("🟡" if opt_pnl > 0 else "❌"))
            print(f"      {icon} {r['symbol']:7s} opt={opt_pnl:+6.1f}% stock={stock:+5.1f}% "
                  f"score={r.get('score', 0):.2f} sig={r.get('signal_count', 0)} {picked}")
    
    # ═════════════════════════════════════════════════════════════════
    # 2. BASELINE: NO FILTER WIN RATES
    # ═════════════════════════════════════════════════════════════════
    
    print(f"\n\n  2. BASELINE WIN RATES (no filter — all candidates)")
    print("  " + "═" * 70)
    
    all_w = [r for r in all_ok if r.get("options_pnl_pct", 0) >= 10]
    print(f"    ALL: {len(all_w)}/{len(all_ok)} = {len(all_w)/len(all_ok)*100:.1f}%")
    
    for eng in ["MOONSHOT", "PUTS"]:
        ep = [r for r in all_ok if r["engine"] == eng]
        ew = [r for r in ep if r.get("options_pnl_pct", 0) >= 10]
        avg = statistics.mean([r.get("options_pnl_pct", 0) for r in ep]) if ep else 0
        print(f"    {eng:10s}: {len(ew)}/{len(ep)} = {len(ew)/len(ep)*100:.1f}%  avg opt={avg:+.1f}%")
    
    # By regime
    print(f"\n    By Engine + Regime:")
    for eng in ["MOONSHOT", "PUTS"]:
        for rl in ["STRONG_BULL", "LEAN_BEAR", "STRONG_BEAR"]:
            dates = [d for d, r in REGIMES.items() if r == rl]
            recs = [r for r in all_ok if r["engine"] == eng and r["scan_date"] in dates]
            wins = [r for r in recs if r.get("options_pnl_pct", 0) >= 10]
            if recs:
                wr = len(wins) / len(recs) * 100
                avg = statistics.mean([r.get("options_pnl_pct", 0) for r in recs])
                print(f"      {eng:10s} + {rl:13s}: {len(wins):2d}/{len(recs):2d} = {wr:5.1f}%  avg={avg:+.1f}%")
    
    # ═════════════════════════════════════════════════════════════════
    # 3. BLOCKED WINNERS — DETAILED ANALYSIS
    # ═════════════════════════════════════════════════════════════════
    
    print(f"\n\n  3. BLOCKED WINNERS — WHY WERE THEY BLOCKED?")
    print("  " + "═" * 70)
    
    # Identify blocked winners from backtest outcomes
    blocked_analysis = []
    for r in all_ok:
        sd = r["scan_date"]
        st = "AM" if r["scan_time"] == "0935" else "PM"
        session_key = f"{sd} {st}"
        sym = r["symbol"]
        opt_pnl = r.get("options_pnl_pct", 0)
        
        if (sym, session_key) not in picked_tuples and opt_pnl >= 10:
            regime = REGIMES.get(sd, "?")
            eng = r["engine"]
            
            # Determine WHY it was blocked
            reasons = []
            if eng == "MOONSHOT":
                if regime in ("STRONG_BEAR", "LEAN_BEAR"):
                    reasons.append(f"Regime={regime}: ALL moonshots blocked")
                elif regime == "NEUTRAL":
                    reasons.append("Regime=NEUTRAL: moonshots blocked")
                else:
                    reasons.append(f"Score/signal filter in {regime}")
            elif eng == "PUTS":
                if regime in ("STRONG_BULL", "LEAN_BULL"):
                    reasons.append(f"Regime={regime}: ALL puts blocked")
                else:
                    reasons.append("Directional/conviction filter")
            
            blocked_analysis.append({
                "symbol": sym,
                "engine": eng,
                "session": session_key,
                "regime": regime,
                "opt_pnl": opt_pnl,
                "stock_move": r.get("stock_move_pct", 0),
                "score": r.get("score", 0),
                "signal_count": r.get("signal_count", 0),
                "reasons": reasons,
            })
    
    # Sort by opt_pnl
    blocked_analysis.sort(key=lambda x: x["opt_pnl"], reverse=True)
    
    print(f"\n    Blocked Winners: {len(blocked_analysis)} total")
    print(f"\n    Top 15 blocked winners:")
    for i, b in enumerate(blocked_analysis[:15], 1):
        print(f"      #{i:2d} {b['symbol']:7s} {b['engine']:8s} {b['session']:16s} "
              f"{b['regime']:13s} opt={b['opt_pnl']:+6.1f}% stock={b['stock_move']:+5.1f}% "
              f"score={b['score']:.2f} | {b['reasons'][0]}")
    
    # Count blocked by reason
    print(f"\n    Blocked winners by reason:")
    reason_groups = defaultdict(list)
    for b in blocked_analysis:
        for r in b["reasons"]:
            key = r.split(":")[0]
            reason_groups[key].append(b)
    
    for key in sorted(reason_groups, key=lambda k: -len(reason_groups[k])):
        items = reason_groups[key]
        total_opt = sum(b["opt_pnl"] for b in items)
        print(f"      {len(items):3d} blocked winners | {key} | total missed optPnL: {total_opt:+.0f}%")
    
    # ═════════════════════════════════════════════════════════════════
    # 4. COUNTERFACTUAL — WHAT IF WE RELAXED GATES?
    # ═════════════════════════════════════════════════════════════════
    
    print(f"\n\n  4. COUNTERFACTUAL SCENARIOS")
    print("  " + "═" * 70)
    
    # Scenario A: Allow moonshots in LEAN_BEAR (not STRONG_BEAR)
    print(f"\n    A) Allow moonshots in LEAN_BEAR (currently blocked):")
    lb_moon = [r for r in all_ok if r["engine"] == "MOONSHOT" 
               and REGIMES.get(r["scan_date"]) == "LEAN_BEAR"]
    lb_moon_w = [r for r in lb_moon if r.get("options_pnl_pct", 0) >= 10]
    if lb_moon:
        wr = len(lb_moon_w) / len(lb_moon) * 100
        avg = statistics.mean([r.get("options_pnl_pct", 0) for r in lb_moon])
        print(f"       → {len(lb_moon_w)}/{len(lb_moon)} = {wr:.1f}% WR, avg={avg:+.1f}%")
        print(f"       → Would add {len(lb_moon)} candidates but WR would drop")
    
    # Scenario B: Lower score threshold to 0.60 in STRONG_BULL
    print(f"\n    B) Lower score threshold to 0.60 (from 0.70) in STRONG_BULL:")
    sb_moon = [r for r in all_ok if r["engine"] == "MOONSHOT" 
               and REGIMES.get(r["scan_date"]) == "STRONG_BULL"
               and r.get("score", 0) >= 0.60]
    sb_moon_w = [r for r in sb_moon if r.get("options_pnl_pct", 0) >= 10]
    if sb_moon:
        wr = len(sb_moon_w) / len(sb_moon) * 100
        avg = statistics.mean([r.get("options_pnl_pct", 0) for r in sb_moon])
        print(f"       → {len(sb_moon_w)}/{len(sb_moon)} = {wr:.1f}% WR, avg={avg:+.1f}%")
    
    # Scenario C: Remove call_buying requirement in STRONG_BULL
    print(f"\n    C) Remove call_buying requirement in STRONG_BULL:")
    sb_all = [r for r in all_ok if r["engine"] == "MOONSHOT" 
              and REGIMES.get(r["scan_date"]) == "STRONG_BULL"]
    sb_all_w = [r for r in sb_all if r.get("options_pnl_pct", 0) >= 10]
    if sb_all:
        wr = len(sb_all_w) / len(sb_all) * 100
        avg = statistics.mean([r.get("options_pnl_pct", 0) for r in sb_all])
        print(f"       → {len(sb_all_w)}/{len(sb_all)} = {wr:.1f}% WR, avg={avg:+.1f}%")
    
    # Scenario D: Allow puts with call_pct up to 0.65 (from 0.55)
    print(f"\n    D) Relax puts call_pct filter to 0.65 (from 0.55):")
    # This is hard to compute without UW flow data, but note the blocked ones
    puts_blocked_by_callpct = [b for b in blocked_analysis if b["engine"] == "PUTS" 
                                and "Directional" in b["reasons"][0]]
    print(f"       → {len(puts_blocked_by_callpct)} puts were blocked by directional filter")
    if puts_blocked_by_callpct:
        avg = statistics.mean([b["opt_pnl"] for b in puts_blocked_by_callpct])
        print(f"       → Their avg optPnL: {avg:+.1f}% (includes both winners & would-be losers)")
    
    # ═════════════════════════════════════════════════════════════════
    # 5. COVERAGE GAP ANALYSIS
    # ═════════════════════════════════════════════════════════════════
    
    print(f"\n\n  5. COVERAGE GAP ANALYSIS")
    print("  " + "═" * 70)
    
    all_sessions = [
        "2026-02-09 AM", "2026-02-09 PM",
        "2026-02-10 AM", "2026-02-10 PM",
        "2026-02-11 AM", "2026-02-11 PM",
        "2026-02-12 AM", "2026-02-12 PM",
        "2026-02-13 AM", "2026-02-13 PM",
    ]
    
    for s in all_sessions:
        recs = sessions.get(s, [])
        our_picks = [p for p in results.get("picks", []) if p["session"] == s]
        regime = REGIMES.get(s.split()[0], "?")
        
        if not recs and not our_picks:
            print(f"    ⬜ {s:16s} [{regime:13s}]: NO DATA (scanner not running or no puts data)")
        elif not our_picks:
            available_w = [r for r in recs if r.get("options_pnl_pct", 0) >= 10]
            print(f"    ❌ {s:16s} [{regime:13s}]: {len(recs)} available, {len(available_w)} would-win, "
                  f"0 picked (ALL BLOCKED BY REGIME GATE)")
        else:
            our_w = [p for p in our_picks if p.get("options_pnl", 0) >= 10]
            wr = len(our_w) / len(our_picks) * 100 if our_picks else 0
            print(f"    {'✅' if wr >= 80 else '🟡'} {s:16s} [{regime:13s}]: picked {len(our_picks)}, "
                  f"won {len(our_w)} ({wr:.0f}% WR)")
    
    # ═════════════════════════════════════════════════════════════════
    # 6. NEW CODE vs OLD CODE COMPARISON
    # ═════════════════════════════════════════════════════════════════
    
    print(f"\n\n  6. NEW CODE vs OLD CODE (Policy B v4 vs No Filter)")
    print("  " + "═" * 70)
    
    # Our picks
    our_picks = results.get("picks", [])
    our_w = [p for p in our_picks if p.get("options_pnl", 0) >= 10]
    our_total = len(our_picks)
    our_wr = len(our_w) / our_total * 100 if our_total else 0
    our_avg = statistics.mean([p.get("net_pnl", 0) for p in our_picks]) if our_picks else 0
    
    # Old code (all 10/scan, no filter)
    old_w = [r for r in all_ok if r.get("options_pnl_pct", 0) >= 10]
    old_total = len(all_ok)
    old_wr = len(old_w) / old_total * 100 if old_total else 0
    old_avg = statistics.mean([r.get("options_pnl_pct", 0) for r in all_ok]) if all_ok else 0
    
    print(f"\n    {'Metric':<25s} | {'Old Code (no filter)':>20s} | {'New Code (Policy B v4)':>22s}")
    print(f"    {'-'*75}")
    print(f"    {'Total picks':<25s} | {old_total:>20d} | {our_total:>22d}")
    print(f"    {'Winners (≥10% optPnL)':<25s} | {len(old_w):>20d} | {len(our_w):>22d}")
    print(f"    {'Win Rate':<25s} | {old_wr:>19.1f}% | {our_wr:>21.1f}%")
    print(f"    {'Avg optPnL':<25s} | {old_avg:>19.1f}% | {our_avg:>21.1f}%")
    
    old_gains = sum(r.get("options_pnl_pct", 0) for r in all_ok if r.get("options_pnl_pct", 0) > 0)
    old_losses = sum(abs(r.get("options_pnl_pct", 0)) for r in all_ok if r.get("options_pnl_pct", 0) <= 0)
    our_gains = sum(p.get("options_pnl", 0) for p in our_picks if p.get("options_pnl", 0) > 0)
    our_losses = sum(abs(p.get("options_pnl", 0)) for p in our_picks if p.get("options_pnl", 0) <= 0)
    old_pf = old_gains / old_losses if old_losses else float('inf')
    our_pf = our_gains / our_losses if our_losses else float('inf')
    
    print(f"    {'Profit Factor':<25s} | {old_pf:>19.2f}x | {our_pf:>21.2f}x")
    print(f"    {'Gross gains':<25s} | {old_gains:>18.0f}% | {our_gains:>20.0f}%")
    print(f"    {'Gross losses':<25s} | {-old_losses:>18.0f}% | {-our_losses:>20.0f}%")
    
    print(f"\n    ┌───────────────────────────────────────────────────────┐")
    print(f"    │  KEY INSIGHT: New code achieved {our_wr:.0f}% WR vs {old_wr:.0f}% baseline    │")
    print(f"    │  Profit Factor improved from {old_pf:.1f}x to {our_pf:.1f}x              │")
    print(f"    │  But total exposure reduced from {old_total} to {our_total} picks         │")
    print(f"    └───────────────────────────────────────────────────────┘")
    
    # ═════════════════════════════════════════════════════════════════
    # 7. DAY-OF-WEEK ANALYSIS
    # ═════════════════════════════════════════════════════════════════
    
    print(f"\n\n  7. DAY-OF-WEEK PERFORMANCE (all outcomes)")
    print("  " + "═" * 70)
    
    day_names = {
        "2026-02-09": "Sun(pre-Mon)",
        "2026-02-10": "Monday",
        "2026-02-11": "Tuesday",
        "2026-02-12": "Wednesday",
        "2026-02-13": "Thursday",
    }
    
    for sd in sorted(day_names.keys()):
        day_recs = [r for r in all_ok if r["scan_date"] == sd]
        day_w = [r for r in day_recs if r.get("options_pnl_pct", 0) >= 10]
        regime = REGIMES.get(sd, "?")
        if day_recs:
            wr = len(day_w) / len(day_recs) * 100
            avg = statistics.mean([r.get("options_pnl_pct", 0) for r in day_recs])
            print(f"    {day_names[sd]:15s} [{regime:13s}]: "
                  f"{len(day_w):2d}/{len(day_recs):2d} = {wr:5.1f}% WR  avg={avg:+.1f}%")
        else:
            print(f"    {day_names[sd]:15s} [{regime:13s}]: NO DATA")


if __name__ == "__main__":
    main()
