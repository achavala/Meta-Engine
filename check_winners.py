#!/usr/bin/env python3
"""
Check and post winner updates to X/Twitter for profitable picks.

Usage:
    python check_winners.py [scan_timestamp] [session_label] [min_profit_pct]
    
Examples:
    # Check 9:35 AM scan today with default 50% threshold
    python check_winners.py "2026-02-12T09:35:00" "AM"
    
    # Check 9:35 AM scan with 75% threshold
    python check_winners.py "2026-02-12T09:35:00" "AM" 75
    
    # Check 3:15 PM scan
    python check_winners.py "2026-02-12T15:15:00" "PM"
"""

import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from notifications.x_poster import check_and_post_winners


def main():
    if len(sys.argv) < 3:
        print("Usage: python check_winners.py <scan_timestamp> <session_label> [min_profit_pct]")
        print("\nExample:")
        print('  python check_winners.py "2026-02-12T09:35:00" "AM" 50')
        sys.exit(1)
    
    scan_timestamp = sys.argv[1]
    session_label = sys.argv[2]
    min_profit_pct = float(sys.argv[3]) if len(sys.argv) > 3 else 50.0
    
    print(f"Checking winners for {session_label} scan at {scan_timestamp}")
    print(f"Minimum profit threshold: {min_profit_pct}%")
    print()
    
    success = check_and_post_winners(
        scan_timestamp=scan_timestamp,
        session_label=session_label,
        min_profit_pct=min_profit_pct,
    )
    
    if success:
        print("✅ Winner update posted to X/Twitter")
    else:
        print("❌ No winners found or posting failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
