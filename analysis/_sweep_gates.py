#!/usr/bin/env python3
"""Comprehensive gate sweep on backtest v2 results."""
import json
from collections import defaultdict

with open("/Users/chavala/Meta Engine/output/backtest_newcode_v2_feb9_13.json") as f:
    data = json.load(f)

results = data["results"]
ok = [r for r in results if r["data_quality"] == "OK"]
print(f"Total with price data: {len(ok)}\n")

# ── COMPREHENSIVE FILTER SWEEP ──
print("═" * 70)
print("  COMPREHENSIVE GATE COMBINATION SWEEP")
print("═" * 70 + "\n")

best_combos = []

for mps_min in [0.0, 0.40, 0.50, 0.60, 0.65, 0.70, 0.75]:
    for sig_min in [0, 3, 4, 5, 6, 7, 8]:
        for orm_rule in ["none", "puts_0.50"]:
            for be_active in [False, True]:
                passed = []
                for r in ok:
                    engine = r["engine"]
                    mps = r["mps"]
                    sigs = r["signal_count"]
                    orm = r.get("orm_score")
                    
                    if mps < mps_min:
                        continue
                    if sigs < sig_min:
                        continue
                    if orm_rule == "puts_0.50" and engine == "PUTS" and orm and orm < 0.50:
                        continue
                    if be_active and mps > 0 and mps * 10 < 6.5:
                        continue
                    
                    passed.append(r)
                
                if len(passed) >= 5:
                    winners = [r for r in passed if r["is_winner"]]
                    wr = len(winners) / len(passed) * 100
                    avg_ret = sum(r["options_pnl_pct"] for r in passed) / len(passed)
                    
                    if wr >= 55:
                        best_combos.append({
                            "mps": mps_min, "sigs": sig_min, "orm": orm_rule, "be": be_active,
                            "n": len(passed), "w": len(winners), "wr": wr, "avg": avg_ret
                        })

best_combos.sort(key=lambda x: x["wr"] * (x["n"] ** 0.5), reverse=True)

print(f"Found {len(best_combos)} combos with WR >= 55% and >= 5 picks\n")
print(f"{'MPS':>5} {'Sigs':>5} {'ORM':>12} {'BE':>4} | {'Picks':>5} {'Wins':>5} {'WR%':>6} {'AvgRet':>8} | Score")
print("-" * 75)
for c in best_combos[:30]:
    score = c["wr"] * (c["n"] ** 0.5)
    be_str = "Y" if c["be"] else "N"
    print(f"{c['mps']:>5.2f} {c['sigs']:>5} {c['orm']:>12} {be_str:>4} | "
          f"{c['n']:>5} {c['w']:>5} {c['wr']:>5.1f}% {c['avg']:>+7.1f}% | {score:.0f}")

# ── BY ENGINE ──
print("\n" + "=" * 70)
print("  BEST GATES BY ENGINE")
print("=" * 70)

for eng in ["PUTS", "MOONSHOT"]:
    eng_ok = [r for r in ok if r["engine"] == eng]
    eng_w = [r for r in eng_ok if r["is_winner"]]
    print(f"\n  {eng}: {len(eng_w)}/{len(eng_ok)} = {len(eng_w)/len(eng_ok)*100:.0f}% baseline\n")
    
    print(f"    Score threshold sweep:")
    for thresh in [0.50, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]:
        above = [r for r in eng_ok if r["score"] >= thresh]
        w = [r for r in above if r["is_winner"]]
        if len(above) >= 3:
            print(f"      Score >= {thresh:.2f}: {len(w):>3}/{len(above):>3} = {len(w)/len(above)*100:>5.1f}%")
    
    print(f"\n    Signal count sweep:")
    for thresh in [3, 4, 5, 6, 7, 8, 9, 10]:
        above = [r for r in eng_ok if r["signal_count"] >= thresh]
        w = [r for r in above if r["is_winner"]]
        if len(above) >= 3:
            print(f"      Sigs >= {thresh}: {len(w):>3}/{len(above):>3} = {len(w)/len(above)*100:>5.1f}%")
    
    print(f"\n    MPS sweep:")
    for thresh in [0.40, 0.50, 0.60, 0.65, 0.70, 0.75, 0.80]:
        above = [r for r in eng_ok if r["mps"] >= thresh]
        w = [r for r in above if r["is_winner"]]
        if len(above) >= 3:
            print(f"      MPS >= {thresh:.2f}: {len(w):>3}/{len(above):>3} = {len(w)/len(above)*100:>5.1f}%")

# ── KEY COMBINATIONS ──
print("\n" + "=" * 70)
print("  KEY GATE COMBINATIONS")
print("=" * 70)

combos = {
    "v1 Policy B (MPS≥0.75 + Sigs≥5/8 + ORM inv)": lambda r: (
        (r["engine"] == "PUTS" and r["signal_count"] >= 5 and r["mps"] >= 0.75) or
        (r["engine"] == "MOONSHOT" and r["signal_count"] >= 8 and r["mps"] >= 0.75 and
         (r.get("orm_score") is None or r.get("orm_score", 0) < 0.60))
    ),
    "★ v2 FINAL (PUTS Sigs≥5+Score≥0.55+MPS≥0.50 | MOON Sigs≥5+MPS≥0.50)": lambda r: (
        (r["engine"] == "PUTS" and r["signal_count"] >= 5 and r["mps"] >= 0.50 and r["score"] >= 0.55) or
        (r["engine"] == "MOONSHOT" and r["signal_count"] >= 5 and r["mps"] >= 0.50)
    ),
    "RELAXED A (MPS≥0.65 + Sigs≥5/6)": lambda r: (
        (r["engine"] == "PUTS" and r["signal_count"] >= 5 and r["mps"] >= 0.65) or
        (r["engine"] == "MOONSHOT" and r["signal_count"] >= 6 and r["mps"] >= 0.65)
    ),
    "RELAXED B (MPS≥0.60 + Sigs≥4/6)": lambda r: (
        (r["engine"] == "PUTS" and r["signal_count"] >= 4 and r["mps"] >= 0.60) or
        (r["engine"] == "MOONSHOT" and r["signal_count"] >= 6 and r["mps"] >= 0.60)
    ),
    "SCORE-FIRST (Score≥0.70 + Sigs≥4)": lambda r: r["score"] >= 0.70 and r["signal_count"] >= 4,
    "SCORE-FIRST (Score≥0.75 + Sigs≥5)": lambda r: r["score"] >= 0.75 and r["signal_count"] >= 5,
    "PURE SIGNAL (Sigs≥5)": lambda r: r["signal_count"] >= 5,
    "PURE SIGNAL (Sigs≥6)": lambda r: r["signal_count"] >= 6,
    "PUTS: Sigs≥5 + Score≥0.55": lambda r: r["engine"] == "PUTS" and r["signal_count"] >= 5 and r["score"] >= 0.55,
    "PUTS: Sigs≥5 + Score≥0.65": lambda r: r["engine"] == "PUTS" and r["signal_count"] >= 5 and r["score"] >= 0.65,
    "MOON: Sigs≥5 + MPS≥0.50": lambda r: r["engine"] == "MOONSHOT" and r["signal_count"] >= 5 and r["mps"] >= 0.50,
    "MOON: Sigs≥8 + Score≥0.85": lambda r: r["engine"] == "MOONSHOT" and r["signal_count"] >= 8 and r["score"] >= 0.85,
}

print(f"\n{'Gate':50s} | {'Picks':>5} {'Wins':>5} {'WR%':>6} {'AvgRet':>8}")
print("-" * 85)
for name, fn in combos.items():
    passed = [r for r in ok if fn(r)]
    winners = [r for r in passed if r["is_winner"]]
    if passed:
        wr = len(winners) / len(passed) * 100
        avg = sum(r["options_pnl_pct"] for r in passed) / len(passed)
        print(f"{name:50s} | {len(passed):>5} {len(winners):>5} {wr:>5.1f}% {avg:>+7.1f}%")
    else:
        print(f"{name:50s} |     0     0   N/A%")

# ── WHAT WOULD HAVE BEEN THE BEST TOP 5 PER SESSION? ──
print("\n" + "=" * 70)
print("  IDEAL TOP 5 PER SESSION (perfect hindsight)")
print("=" * 70)

sessions = defaultdict(list)
for r in ok:
    sessions[r["session"]].append(r)

total_ideal_picks = 0
total_ideal_winners = 0
for sess in sorted(sessions.keys()):
    picks = sessions[sess]
    # Sort by actual options P&L and take top 5
    sorted_picks = sorted(picks, key=lambda x: x["options_pnl_pct"], reverse=True)[:5]
    winners = [p for p in sorted_picks if p["is_winner"]]
    total_ideal_picks += len(sorted_picks)
    total_ideal_winners += len(winners)
    wr = len(winners) / len(sorted_picks) * 100 if sorted_picks else 0
    avg = sum(p["options_pnl_pct"] for p in sorted_picks) / len(sorted_picks) if sorted_picks else 0
    print(f"\n  {sess}: {len(winners)}/{len(sorted_picks)} = {wr:.0f}% WR, avg={avg:+.1f}%")
    for p in sorted_picks:
        status = "✅" if p["is_winner"] else "❌"
        print(f"    {status} {p['symbol']:6s} | Score={p['score']:.2f} Sigs={p['signal_count']} MPS={p['mps']:.2f} | "
              f"Move={p['stock_move_pct']:+.1f}% Opts={p['options_pnl_pct']:+.1f}%")

ideal_wr = total_ideal_winners / total_ideal_picks * 100 if total_ideal_picks else 0
print(f"\n  IDEAL overall: {total_ideal_winners}/{total_ideal_picks} = {ideal_wr:.0f}%")

# ── FEATURES OF WINNERS VS LOSERS ──
print("\n" + "=" * 70)
print("  WINNER vs LOSER FEATURE COMPARISON")
print("=" * 70)

winners_all = [r for r in ok if r["is_winner"]]
losers_all = [r for r in ok if not r["is_winner"]]

def safe_avg(lst, key):
    vals = [r.get(key, 0) or 0 for r in lst]
    return sum(vals) / len(vals) if vals else 0

print(f"\n  {'Feature':25s} | {'Winners':>10} | {'Losers':>10}")
print(f"  {'-'*25}-+-{'-'*10}-+-{'-'*10}")
print(f"  {'Count':25s} | {len(winners_all):>10} | {len(losers_all):>10}")
print(f"  {'Avg Score':25s} | {safe_avg(winners_all, 'score'):>10.3f} | {safe_avg(losers_all, 'score'):>10.3f}")
print(f"  {'Avg Signal Count':25s} | {safe_avg(winners_all, 'signal_count'):>10.1f} | {safe_avg(losers_all, 'signal_count'):>10.1f}")
print(f"  {'Avg MPS':25s} | {safe_avg(winners_all, 'mps'):>10.3f} | {safe_avg(losers_all, 'mps'):>10.3f}")
print(f"  {'Avg ORM':25s} | {safe_avg(winners_all, 'orm_score'):>10.3f} | {safe_avg(losers_all, 'orm_score'):>10.3f}")
print(f"  {'Avg Stock Move':25s} | {safe_avg(winners_all, 'stock_move_pct'):>+9.1f}% | {safe_avg(losers_all, 'stock_move_pct'):>+9.1f}%")
print(f"  {'Avg Options P&L':25s} | {safe_avg(winners_all, 'options_pnl_pct'):>+9.1f}% | {safe_avg(losers_all, 'options_pnl_pct'):>+9.1f}%")
