#!/usr/bin/env python3
"""Test enhanced conflict resolution for ALAB."""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# Load existing cross analysis
with open("output/cross_analysis_20260209.json") as f:
    cross_results = json.load(f)

# Re-run summary generator with enhanced conflict resolution
from analysis.summary_generator import generate_all_summaries
summaries = generate_all_summaries(cross_results)

# Show conflict results
print("=" * 70)
print("ENHANCED CONFLICT RESOLUTION")
print("=" * 70)

conflicts = summaries.get("conflict_summaries", [])
if not conflicts:
    print("No conflicts found!")
else:
    for c in conflicts:
        print(f"\n{'='*70}")
        print(f"SYMBOL: {c.get('symbol', '???')}")
        print(f"TYPE: {c.get('conflict_type', 'N/A')}")
        print(f"DOMINANT: {c.get('dominant_thesis', 'N/A')}")
        print(f"PUTS SCORE: {c.get('puts_score', 0):.2f}")
        print(f"MOON SCORE: {c.get('moon_score', 0):.2f}")
        print(f"MWS SCORE: {c.get('mws_score', 0):.0f}/100")
        print(f"CURRENT PRICE: ${c.get('current_price', 0):.2f}")
        print(f"RSI: {c.get('rsi', 0):.1f}")
        print(f"5-DAY MOVE: {c.get('recent_move_pct', 0):+.1f}%")
        
        exp_range = c.get("expected_range", [])
        if exp_range and len(exp_range) >= 2:
            print(f"EXPECTED RANGE: ${exp_range[0]:.2f} - ${exp_range[1]:.2f}")
        
        print(f"SENSORS: {c.get('bullish_sensors', 0)} bullish / {c.get('bearish_sensors', 0)} bearish")
        print(f"\nSUMMARY:")
        # Word wrap the summary at 100 chars
        summary = c.get("summary", "")
        for i in range(0, len(summary), 100):
            print(f"  {summary[i:i+100]}")
        
        recs = c.get("recommendations", {})
        if recs:
            print(f"\nRECOMMENDATIONS:")
            for k, v in recs.items():
                print(f"  {k}: {v}")

# Also regenerate the report
print("\n" + "=" * 70)
print("REGENERATING REPORT...")
print("=" * 70)

with open("output/puts_top10_20260209.json") as f:
    puts_data = json.load(f)
with open("output/moonshot_top10_20260209.json") as f:
    moon_data = json.load(f)

from analysis.report_generator import generate_md_report
report_path = generate_md_report(
    puts_picks=puts_data["picks"],
    moon_picks=moon_data["picks"],
    cross_data=cross_results,
    summaries=summaries,
    output_dir="output",
    date_str="20260209",
)
print(f"\nReport saved: {report_path}")

# Save updated summaries
with open("output/summaries_20260209.json", "w") as f:
    json.dump(summaries, f, indent=2, default=str)
print("Summaries updated: output/summaries_20260209.json")
