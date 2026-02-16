"""
NYSE Trading Calendar
=====================
Authoritative list of NYSE market holidays and calendar utilities.

Used by:
  - trading/executor.py for theta guard (long-weekend detection)
  - analysis backtest scripts for next_trading_day computation
  - scheduler.py for skip-holiday logic

Source: https://www.nyse.com/markets/hours-calendars
"""

from datetime import date, timedelta
from typing import List, Set

# ═══════════════════════════════════════════════════════
# NYSE Full-Day Holidays  (market fully closed)
# ═══════════════════════════════════════════════════════
# We store 2025-2027 to cover backtests and forward scheduling.

NYSE_HOLIDAYS: Set[date] = {
    # ── 2025 ──────────────────────────────────────────
    date(2025, 1, 1),    # New Year's Day
    date(2025, 1, 20),   # MLK Jr. Day
    date(2025, 2, 17),   # Presidents' Day
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 26),   # Memorial Day
    date(2025, 6, 19),   # Juneteenth
    date(2025, 7, 4),    # Independence Day
    date(2025, 9, 1),    # Labor Day
    date(2025, 11, 27),  # Thanksgiving Day
    date(2025, 12, 25),  # Christmas Day

    # ── 2026 ──────────────────────────────────────────
    date(2026, 1, 1),    # New Year's Day
    date(2026, 1, 19),   # MLK Jr. Day
    date(2026, 2, 16),   # Presidents' Day  ← Mon Feb 16, 2026
    date(2026, 4, 3),    # Good Friday
    date(2026, 5, 25),   # Memorial Day
    date(2026, 6, 19),   # Juneteenth
    date(2026, 7, 3),    # Independence Day (observed — July 4 = Saturday)
    date(2026, 9, 7),    # Labor Day
    date(2026, 11, 26),  # Thanksgiving Day
    date(2026, 12, 25),  # Christmas Day

    # ── 2027 ──────────────────────────────────────────
    date(2027, 1, 1),    # New Year's Day
    date(2027, 1, 18),   # MLK Jr. Day
    date(2027, 2, 15),   # Presidents' Day
    date(2027, 3, 26),   # Good Friday
    date(2027, 5, 31),   # Memorial Day
    date(2027, 6, 18),   # Juneteenth (observed — June 19 = Saturday)
    date(2027, 7, 5),    # Independence Day (observed — July 4 = Sunday)
    date(2027, 9, 6),    # Labor Day
    date(2027, 11, 25),  # Thanksgiving Day
    date(2027, 12, 24),  # Christmas Day (observed — Dec 25 = Saturday)
}

# NYSE early-close days (1:00 PM ET close) — optional, for future use
NYSE_EARLY_CLOSE: Set[date] = {
    # 2026
    date(2026, 7, 2),    # Day before July 4 weekend
    date(2026, 11, 27),  # Day after Thanksgiving
    date(2026, 12, 24),  # Christmas Eve
}


def is_trading_day(d: date) -> bool:
    """Check if a given date is a NYSE trading day."""
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if d in NYSE_HOLIDAYS:
        return False
    return True


def next_trading_day(d: date) -> date:
    """
    Get the next NYSE trading day AFTER date d.
    Skips weekends and NYSE holidays.
    """
    nd = d + timedelta(days=1)
    while not is_trading_day(nd):
        nd += timedelta(days=1)
    return nd


def prev_trading_day(d: date) -> date:
    """
    Get the previous NYSE trading day BEFORE date d.
    Skips weekends and NYSE holidays.
    """
    pd = d - timedelta(days=1)
    while not is_trading_day(pd):
        pd -= timedelta(days=1)
    return pd


def calendar_days_to_next_session(d: date) -> int:
    """
    How many CALENDAR days from d to the next trading session?
    
    This is critical for theta decay calculations:
      - Normal weekday: 1 calendar day
      - Friday: 3 calendar days (Sat + Sun)
      - Friday before long weekend: 4+ calendar days
      - Thursday before Good Friday: 4 calendar days
    
    Examples for Feb 2026:
      - Wed Feb 11 → 1 (Thu Feb 12)
      - Fri Feb 13 → 3 (Mon Feb 16 is Presidents' Day → Tue Feb 17)
      - Actually: Fri Feb 13 → next = Tue Feb 17 = 4 calendar days
    """
    nxt = next_trading_day(d)
    return (nxt - d).days


def is_long_weekend_ahead(d: date) -> bool:
    """
    Check if the next trading session is >1 calendar day away.
    
    Returns True for:
      - Fridays (3 days to Monday)
      - Thursdays before a Friday holiday (4 days)
      - Any day before a multi-day holiday stretch
    
    Used by the theta guard to block short-DTE entries.
    """
    return calendar_days_to_next_session(d) > 1


def trading_days_between(start: date, end: date) -> int:
    """Count the number of trading days between two dates (exclusive of start, inclusive of end)."""
    count = 0
    d = start + timedelta(days=1)
    while d <= end:
        if is_trading_day(d):
            count += 1
        d += timedelta(days=1)
    return count


def get_n_trading_days_forward(start: date, n: int) -> List[date]:
    """Get the next N trading days after start date."""
    result = []
    d = start
    while len(result) < n:
        d = next_trading_day(d)
        result.append(d)
    return result
