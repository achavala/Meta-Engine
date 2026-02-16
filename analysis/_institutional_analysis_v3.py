#!/usr/bin/env python3
"""
Institutional-Grade Analysis of Policy B v3 Ultra-Selective Backtest
"""

import json
from pathlib import Path
from collections import defaultdict
import statistics

OUTPUT = Path("/Users/chavala/Meta Engine/output")

with open(OUTPUT / "forward_backtest_v3_ultra_selective.json") as f:
    data = json.load(f)

picks = data["picks"]
winners = [p for p in picks if p.get("options_pnl_pct", 0) >= 10]
losers = [p for p in picks if p.get("options_pnl_pct", 0) < 10]

print("=" * 80)
print("  INSTITUTIONAL-GRADE ANALYSIS — Policy B v3 Ultra-Selective")
print("=" * 80)

# 1. WINNER ANALYSIS
print(f"\n{'='*80}")
print("  1. WINNER ANALYSIS")
print(f"{'='*80}")
print(f"\nTotal Winners (≥+10%): {len(winners)}/{len(picks)} = {len(winners)/len(picks)*100:.1f}%")

print(f"\nWinners by Engine:")
for eng in ["PUTS", "MOONSHOT"]:
    eng_winners = [w for w in winners if w["engine"] == eng]
    eng_picks = [p for p in picks if p["engine"] == eng]
    print(f"  {eng}: {len(eng_winners)}/{len(eng_picks)} = {len(eng_winners)/len(eng_picks)*100:.1f}%")

print(f"\nWinners by Regime:")
for regime in ["STRONG_BULL", "STRONG_BEAR", "LEAN_BEAR"]:
    reg_winners = [w for w in winners if w.get("regime") == regime]
    reg_picks = [p for p in picks if p.get("regime") == regime]
    if reg_picks:
        print(f"  {regime}: {len(reg_winners)}/{len(reg_picks)} = {len(reg_winners)/len(reg_picks)*100:.1f}%")

print(f"\nWinner Characteristics:")
for w in sorted(winners, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
    feat = w.get("features", {})
    print(f"  {w['symbol']:6s} {w['engine']:8s} Regime={w.get('regime'):<13s} "
          f"PnL={w.get('options_pnl_pct', 0):>+6.1f}% | "
          f"Score={w.get('score', 0):.2f} MPS={feat.get('mps', 0):.2f} | "
          f"IV={feat.get('iv_inverted', False)} CB={feat.get('call_buying', False)} "
          f"BF={feat.get('bullish_flow', False)} DP={feat.get('dark_pool_massive', False)}")

# 2. LOSER ANALYSIS
print(f"\n{'='*80}")
print("  2. LOSER ANALYSIS")
print(f"{'='*80}")
print(f"\nTotal Losers (<+10%): {len(losers)}/{len(picks)} = {len(losers)/len(picks)*100:.1f}%")

print(f"\nLosers by Engine:")
for eng in ["PUTS", "MOONSHOT"]:
    eng_losers = [l for l in losers if l["engine"] == eng]
    eng_picks = [p for p in picks if p["engine"] == eng]
    print(f"  {eng}: {len(eng_losers)}/{len(eng_picks)} = {len(eng_losers)/len(eng_picks)*100:.1f}%")

print(f"\nWorst Losers:")
for l in sorted(losers, key=lambda x: x.get("options_pnl_pct", 0))[:10]:
    feat = l.get("features", {})
    print(f"  {l['symbol']:6s} {l['engine']:8s} Regime={l.get('regime'):<13s} "
          f"PnL={l.get('options_pnl_pct', 0):>+6.1f}% | "
          f"StockMove={l.get('stock_move_pct', 0):>+6.1f}% | "
          f"Score={l.get('score', 0):.2f} MPS={feat.get('mps', 0):.2f} | "
          f"IV={feat.get('iv_inverted', False)} CB={feat.get('call_buying', False)} "
          f"BF={feat.get('bullish_flow', False)}")

# 3. PATTERN ANALYSIS
print(f"\n{'='*80}")
print("  3. PATTERN ANALYSIS")
print(f"{'='*80}")

# Moonshot in STRONG_BEAR
moon_bear = [p for p in picks if p["engine"] == "MOONSHOT" and p.get("regime") == "STRONG_BEAR"]
moon_bear_winners = [p for p in moon_bear if p.get("options_pnl_pct", 0) >= 10]
print(f"\nMoonshot in STRONG_BEAR: {len(moon_bear_winners)}/{len(moon_bear)} = {len(moon_bear_winners)/len(moon_bear)*100:.1f}% WR")
for p in moon_bear:
    feat = p.get("features", {})
    w = "✅" if p.get("options_pnl_pct", 0) >= 10 else "❌"
    print(f"  {w} {p['symbol']:6s} PnL={p.get('options_pnl_pct', 0):>+6.1f}% | "
          f"IV={feat.get('iv_inverted', False)} CB={feat.get('call_buying', False)} "
          f"Score={p.get('score', 0):.2f} StockMove={p.get('stock_move_pct', 0):>+6.1f}%")

# PUTS wrong-direction trades
puts_wrong_dir = [p for p in picks if p["engine"] == "PUTS" and p.get("stock_move_pct", 0) > 0]
print(f"\nPUTS Wrong-Direction (stock went UP): {len(puts_wrong_dir)}")
for p in puts_wrong_dir:
    feat = p.get("features", {})
    print(f"  {p['symbol']:6s} StockMove={p.get('stock_move_pct', 0):>+6.1f}% PnL={p.get('options_pnl_pct', 0):>+6.1f}% | "
          f"CB={feat.get('call_buying', False)} BF={feat.get('bullish_flow', False)}")

# 4. GAP ANALYSIS
print(f"\n{'='*80}")
print("  4. GAP TO 80% TARGET")
print(f"{'='*80}")
print(f"\nCurrent WR: {len(winners)/len(picks)*100:.1f}%")
print(f"Target: 80.0%")
print(f"Gap: {80.0 - len(winners)/len(picks)*100:.1f}pp")
print(f"\nTo reach 80% with {len(picks)} picks, need {int(len(picks) * 0.80)} winners")
print(f"Currently have {len(winners)} winners")
print(f"Need to convert {int(len(picks) * 0.80) - len(winners)} losers to winners")

# 5. RECOMMENDATIONS
print(f"\n{'='*80}")
print("  5. INSTITUTIONAL RECOMMENDATIONS")
print(f"{'='*80}")

print(f"\n🔴 CRITICAL ISSUES:")
print(f"  1. Moonshot WR in STRONG_BEAR: {len(moon_bear_winners)}/{len(moon_bear)} = {len(moon_bear_winners)/len(moon_bear)*100:.1f}%")
print(f"     → Even with iv_inverted/call_buying filters, moonshots struggle in bear markets")
print(f"     → Recommendation: Consider blocking ALL moonshots in STRONG_BEAR (only allow puts)")

print(f"\n  2. PUTS wrong-direction trades: {len(puts_wrong_dir)}")
print(f"     → Directional filter not catching all cases")
print(f"     → Recommendation: Strengthen directional filter or add momentum check")

print(f"\n🟡 MODERATE ISSUES:")
print(f"  3. Low coverage: Only {len(picks)} picks total (target: 15-20)")
print(f"     → Ultra-selective filters may be too restrictive")
print(f"     → Recommendation: Consider relaxing MPS threshold slightly (0.65 → 0.60)")

print(f"\n  4. Moonshot overall WR: {len([w for w in winners if w['engine'] == 'MOONSHOT'])}/{len([p for p in picks if p['engine'] == 'MOONSHOT'])} = {len([w for w in winners if w['engine'] == 'MOONSHOT'])/len([p for p in picks if p['engine'] == 'MOONSHOT'])*100:.1f}%")
print(f"     → Much lower than PUTS")
print(f"     → Recommendation: Moonshot needs even stricter regime alignment")

print(f"\n🟢 STRENGTHS:")
print(f"  5. PUTS WR: {len([w for w in winners if w['engine'] == 'PUTS'])}/{len([p for p in picks if p['engine'] == 'PUTS'])} = {len([w for w in winners if w['engine'] == 'PUTS'])/len([p for p in picks if p['engine'] == 'PUTS'])*100:.1f}%")
print(f"     → PUTS filters working well in bear regimes")
print(f"     → Keep PUTS filters as-is")

print(f"\n📊 DETAILED RECOMMENDATIONS:")
print(f"\n  A. REGIME-AWARE DEPLOYMENT:")
print(f"     - STRONG_BEAR: Block ALL moonshots (only allow puts)")
print(f"     - LEAN_BEAR: Block moonshots unless iv_inverted + institutional")
print(f"     - STRONG_BULL/LEAN_BULL: Allow moonshots with call_buying")
print(f"     - NEUTRAL: Block moonshots (too uncertain)")

print(f"\n  B. DIRECTIONAL FILTER ENHANCEMENT:")
print(f"     - Add momentum check: Block puts if stock has positive momentum")
print(f"     - Add volume check: Block puts if call volume > put volume by 2x")
print(f"     - Strengthen bull regime filter: Block puts in ANY bull regime if bullish_flow")

print(f"\n  C. PREMIUM SIGNAL REQUIREMENT:")
print(f"     - Current: Require 1 premium signal")
print(f"     - Recommendation: Require 2+ premium signals for moonshot in STRONG_BEAR")
print(f"     - This would filter out marginal setups")

print(f"\n  D. SCORE THRESHOLD ADJUSTMENT:")
print(f"     - Current: MIN_BASE_SCORE = 0.70 for moonshot")
print(f"     - Recommendation: Raise to 0.75 for moonshot in STRONG_BEAR")
print(f"     - Keep 0.70 for moonshot in STRONG_BULL")

print(f"\n  E. MPS THRESHOLD ADJUSTMENT:")
print(f"     - Current: MIN_MOVE_POTENTIAL = 0.65 for moonshot")
print(f"     - Recommendation: Keep 0.65 for moonshot in STRONG_BULL")
print(f"     - Raise to 0.70 for moonshot in STRONG_BEAR (if not blocked entirely)")

print(f"\n  F. COVERAGE vs. QUALITY TRADE-OFF:")
print(f"     - Current: {len(picks)} picks, {len(winners)/len(picks)*100:.1f}% WR")
print(f"     - If we block ALL moonshots in STRONG_BEAR: ~{len([p for p in picks if not (p['engine'] == 'MOONSHOT' and p.get('regime') == 'STRONG_BEAR')])} picks")
print(f"     - Expected WR improvement: ~{len([w for w in winners if not (w['engine'] == 'MOONSHOT' and w.get('regime') == 'STRONG_BEAR')])/len([p for p in picks if not (p['engine'] == 'MOONSHOT' and p.get('regime') == 'STRONG_BEAR')])*100:.1f}%")
