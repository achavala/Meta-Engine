"""
Validate X-Worthy Selection v2 Against Last 2 Days Real Data
══════════════════════════════════════════════════════════════════════════════

Simulates the X-worthy selector on the last 2 days of cross_analysis
to show exactly which 3 PUTs + 3 CALLs would go on X.

Usage:
  cd "Meta Engine"
  python3 analysis/validate_x_worthy_last_2_days.py
"""

import json
import re
import sys
from pathlib import Path

META_DIR = Path(__file__).parent.parent
OUTPUT_DIR = META_DIR / "output"
sys.path.insert(0, str(META_DIR))


def _find_last_n_cross_files(n: int = 2):
    pattern = re.compile(r"^cross_analysis_(\d{8})\.json$")
    dated = []
    if not OUTPUT_DIR.exists():
        return []
    for f in OUTPUT_DIR.iterdir():
        if not f.is_file():
            continue
        m = pattern.match(f.name)
        if m:
            dated.append((m.group(1), f))
    dated.sort(key=lambda x: x[0], reverse=True)
    return [(d, path) for d, path in dated[:n]]


def main():
    print("=" * 70)
    print("X-Worthy v2 Validation (last 2 days, injection + ranking)")
    print("=" * 70)

    cross_files = _find_last_n_cross_files(2)
    if not cross_files:
        print(f"No cross_analysis_YYYYMMDD.json found in {OUTPUT_DIR}")
        return 1

    from engine_adapters.x_worthy_selector import (
        select_x_worthy_3_puts_3_calls,
        _load_tradenova_tables,
        _priority_score,
    )

    sd_picks, s5_picks, fr_picks = _load_tradenova_tables()
    print(f"\nTradeNova tables loaded:")
    print(f"  same_day_1x : {len(sd_picks)} picks")
    print(f"  sure_shot_5x: {len(s5_picks)} picks")
    print(f"  final_recs  : {len(fr_picks)} picks")

    # Show TradeNova 1x top 5
    print(f"\n--- TradeNova Same-Day 1x (top 10) ---")
    for i, p in enumerate(sd_picks[:10], 1):
        sym = p.get("symbol", "?")
        d = p.get("direction", "?")
        s1x = p.get("score_1x", 0)
        est = p.get("est_multiplier", 0)
        print(f"  #{i:2d} {sym:8s} {d:5s} 1x={s1x:3d} est={est:.0f}x")

    print(f"\n--- TradeNova Sure-Shot 5x (top 10) ---")
    for i, p in enumerate(s5_picks[:10], 1):
        sym = p.get("symbol", "?")
        d = p.get("direction", "?")
        s5x = p.get("score_5x", 0)
        est = p.get("est_multiplier", 0)
        print(f"  #{i:2d} {sym:8s} {d:5s} 5x={s5x:3d} est={est:.0f}x")

    for date_str, path in cross_files:
        print(f"\n{'='*70}")
        print(f"  {date_str} — {path.name}")
        print(f"{'='*70}")
        try:
            cross = json.load(open(path))
        except Exception as e:
            print(f"  Error loading: {e}")
            continue

        puts = cross.get("puts_through_moonshot", [])[:10]
        calls = cross.get("moonshot_through_puts", [])[:10]

        print(f"\n  Meta cross puts ({len(puts)}): "
              f"{', '.join(p.get('symbol','?') for p in puts)}")
        print(f"  Meta cross calls ({len(calls)}): "
              f"{', '.join(c.get('symbol','?') for c in calls)}")

        # Run the actual selector
        puts_3, calls_3 = select_x_worthy_3_puts_3_calls(puts, calls)

        print(f"\n  >>> X-WORTHY 3 PUTs for X/Twitter:")
        for i, p in enumerate(puts_3, 1):
            sym = p.get("symbol", "?")
            reason = p.get("_x_worthy_reason", "meta_only")
            pri = _priority_score(p)
            score = p.get("score", 0)
            src = p.get("_x_source", "Meta")
            est = p.get("_x_est_mult", 0)
            print(f"      #{i} {sym:8s} [{reason:14s}] priority={pri:7.0f} "
                  f"score={score:.3f} est_mult={est:.0f}x source={src}")

        print(f"\n  >>> X-WORTHY 3 CALLs for X/Twitter:")
        for i, c in enumerate(calls_3, 1):
            sym = c.get("symbol", "?")
            reason = c.get("_x_worthy_reason", "meta_only")
            pri = _priority_score(c)
            score = c.get("score", 0)
            src = c.get("_x_source", "Meta")
            est = c.get("_x_est_mult", 0)
            print(f"      #{i} {sym:8s} [{reason:14s}] priority={pri:7.0f} "
                  f"score={score:.3f} est_mult={est:.0f}x source={src}")

        # Compare with old behavior (Meta top 3 only)
        old_puts = [p.get("symbol", "?") for p in puts[:3]]
        old_calls = [c.get("symbol", "?") for c in calls[:3]]
        new_puts = [p.get("symbol", "?") for p in puts_3]
        new_calls = [c.get("symbol", "?") for c in calls_3]

        puts_changed = old_puts != new_puts
        calls_changed = old_calls != new_calls
        print(f"\n  Old X puts:  {old_puts}")
        print(f"  New X puts:  {new_puts} {'← CHANGED' if puts_changed else '(same)'}")
        print(f"  Old X calls: {old_calls}")
        print(f"  New X calls: {new_calls} {'← CHANGED' if calls_changed else '(same)'}")

    print(f"\n{'='*70}")
    print("Done. v2 injects TradeNova 1x/5x/recs into pool before ranking.")
    print("="*70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
