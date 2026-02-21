"""
Safeguards Module — Prevents the system from breaking and keeps it profitable.

Three layers of protection:
  1. CIRCUIT BREAKERS — Kill trading if accuracy degrades
  2. DATA STALENESS GUARDS — Refuse to trade on stale/missing data
  3. ACCURACY WATCHDOG — Track real P&L, alert on degradation

This module is imported by meta_engine.py and _3pm_analysis.py before
any trading decisions are made.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("Safeguards")

_META_DIR = Path.home() / "Meta Engine"
_OUTPUT_DIR = _META_DIR / "output"
_SAFEGUARD_STATE = _OUTPUT_DIR / "safeguard_state.json"

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURABLE THRESHOLDS — Tune these based on live performance
# ═══════════════════════════════════════════════════════════════════════

# Circuit breaker trips if accuracy falls below this over the lookback window
ACCURACY_FLOOR = 0.40  # 40% — below this, we're losing money
ACCURACY_LOOKBACK_DAYS = 5  # Check last 5 trading days
MIN_PREDICTIONS_FOR_CIRCUIT = 10  # Need at least 10 predictions to evaluate

# Data staleness: refuse to trade if core data is older than this
MAX_DATA_AGE_HOURS = 18  # UW flow, OI, GEX data must be < 18 hours old (covers PM→AM gap)
MAX_PRICE_AGE_MINUTES = 15  # Price data must be < 15 minutes old

# Risk limits
MAX_DAILY_LOSS_PCT = 5.0  # Stop trading if daily P&L drops below -5%
MAX_SINGLE_POSITION_PCT = 10.0  # No single position > 10% of account
MAX_OPEN_POSITIONS = 6  # Never have more than 6 open positions
MIN_CONVICTION_FOR_TRADE = 0.30  # Don't trade below this conviction

# Consecutive loss limit
MAX_CONSECUTIVE_LOSSES = 4  # After 4 losses in a row, pause for 1 day


# ═══════════════════════════════════════════════════════════════════════
# 1. CIRCUIT BREAKERS
# ═══════════════════════════════════════════════════════════════════════

def check_circuit_breaker() -> Tuple[bool, str]:
    """
    Check if trading should be halted.

    Returns:
        (is_safe, reason) — if is_safe=False, DO NOT TRADE.
    """
    state = _load_state()

    # Check 1: Manual kill switch
    if state.get("manual_kill_switch", False):
        return False, "MANUAL KILL SWITCH IS ON — trading halted by operator"

    # Check 2: Accuracy floor
    recent_results = state.get("prediction_results", [])
    lookback_cutoff = (datetime.now() - timedelta(days=ACCURACY_LOOKBACK_DAYS)).isoformat()
    recent = [r for r in recent_results if r.get("timestamp", "") >= lookback_cutoff]

    if len(recent) >= MIN_PREDICTIONS_FOR_CIRCUIT:
        correct = sum(1 for r in recent if r.get("correct", False))
        accuracy = correct / len(recent)
        if accuracy < ACCURACY_FLOOR:
            return False, (
                "CIRCUIT BREAKER: Accuracy {:.0f}% ({}/{}) over last {} days "
                "is below {:.0f}% floor. Trading halted."
            ).format(accuracy * 100, correct, len(recent),
                     ACCURACY_LOOKBACK_DAYS, ACCURACY_FLOOR * 100)

    # Check 3: Consecutive losses
    consecutive_losses = state.get("consecutive_losses", 0)
    if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
        pause_until = state.get("loss_pause_until", "")
        if pause_until and datetime.now().isoformat() < pause_until:
            return False, (
                "LOSS PAUSE: {} consecutive losses. Paused until {}."
            ).format(consecutive_losses, pause_until)
        else:
            state["consecutive_losses"] = 0
            _save_state(state)

    # Check 4: Daily loss limit
    daily_pnl = state.get("daily_pnl_pct", 0)
    if daily_pnl < -MAX_DAILY_LOSS_PCT:
        return False, (
            "DAILY LOSS LIMIT: P&L is {:.1f}%, exceeds -{:.1f}% limit. "
            "No more trades today."
        ).format(daily_pnl, MAX_DAILY_LOSS_PCT)

    return True, "All safeguards passed"


def record_prediction_result(symbol: str, direction: str, conviction: float,
                             actual_move_pct: float):
    """Record a prediction outcome for accuracy tracking."""
    state = _load_state()
    results = state.setdefault("prediction_results", [])

    is_correct = (
        (direction == "BULLISH" and actual_move_pct > 1.0) or
        (direction == "BEARISH" and actual_move_pct < -1.0)
    )

    results.append({
        "timestamp": datetime.now().isoformat(),
        "symbol": symbol,
        "direction": direction,
        "conviction": conviction,
        "actual_move_pct": actual_move_pct,
        "correct": is_correct,
    })

    # Keep last 100 results
    state["prediction_results"] = results[-100:]

    # Track consecutive losses
    if not is_correct and abs(actual_move_pct) > 1.0:
        state["consecutive_losses"] = state.get("consecutive_losses", 0) + 1
        if state["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
            pause_until = (datetime.now() + timedelta(days=1)).isoformat()
            state["loss_pause_until"] = pause_until
            logger.warning(
                "CONSECUTIVE LOSS LIMIT: %d losses in a row. Pausing until %s",
                state["consecutive_losses"], pause_until
            )
    else:
        state["consecutive_losses"] = 0

    _save_state(state)
    return is_correct


def update_daily_pnl(pnl_pct: float):
    """Update today's P&L percentage."""
    state = _load_state()
    today = datetime.now().strftime("%Y-%m-%d")

    if state.get("pnl_date") != today:
        state["daily_pnl_pct"] = 0
        state["pnl_date"] = today

    state["daily_pnl_pct"] = pnl_pct
    _save_state(state)


def set_kill_switch(on: bool):
    """Manually enable/disable the kill switch."""
    state = _load_state()
    state["manual_kill_switch"] = on
    _save_state(state)
    logger.warning("KILL SWITCH %s", "ENABLED" if on else "DISABLED")


# ═══════════════════════════════════════════════════════════════════════
# 2. DATA STALENESS GUARDS
# ═══════════════════════════════════════════════════════════════════════

def check_data_freshness() -> Tuple[bool, str, Dict]:
    """
    Check if all critical data sources are fresh enough to trade on.

    Returns:
        (is_fresh, reason, details)
    """
    tradenova_data = Path.home() / "TradeNova" / "data"
    issues = []
    details = {}

    critical_files = {
        "uw_flow": tradenova_data / "uw_flow_cache.json",
        "oi_changes": tradenova_data / "uw_oi_change_cache.json",
        "gex_data": tradenova_data / "uw_gex_cache.json",
        "iv_term": tradenova_data / "uw_iv_term_cache.json",
        "skew_data": tradenova_data / "uw_skew_cache.json",
    }

    now = time.time()
    max_age_seconds = MAX_DATA_AGE_HOURS * 3600

    for name, path in critical_files.items():
        if not path.exists():
            issues.append("{}: FILE MISSING".format(name))
            details[name] = {"status": "MISSING", "age_hours": -1}
            continue

        age_seconds = now - path.stat().st_mtime
        age_hours = age_seconds / 3600
        details[name] = {
            "status": "OK" if age_seconds < max_age_seconds else "STALE",
            "age_hours": round(age_hours, 1),
        }

        if age_seconds > max_age_seconds:
            issues.append("{}: {:.1f}h old (max {}h)".format(name, age_hours, MAX_DATA_AGE_HOURS))

        # Also check if file is valid JSON
        try:
            with open(path) as f:
                data = json.load(f)
            if not data:
                issues.append("{}: EMPTY FILE".format(name))
                details[name]["status"] = "EMPTY"
        except json.JSONDecodeError:
            issues.append("{}: CORRUPTED JSON".format(name))
            details[name]["status"] = "CORRUPT"

    if issues:
        reason = "DATA FRESHNESS ISSUES: " + "; ".join(issues)
        try:
            from monitoring.health_alerts import alert_data_issue
            alert_data_issue(reason, details)
        except Exception:
            pass
        return False, reason, details

    return True, "All data sources fresh", details


def check_price_freshness(prices: Dict) -> Tuple[bool, str]:
    """Check if price data is recent enough for trading decisions."""
    if not prices:
        return False, "NO PRICE DATA AVAILABLE"

    stale_count = 0
    for sym, data in prices.items():
        if isinstance(data, dict):
            last_updated = data.get("last_updated", 0)
            if last_updated:
                age_minutes = (time.time() - last_updated) / 60
                if age_minutes > MAX_PRICE_AGE_MINUTES:
                    stale_count += 1

    if stale_count > len(prices) * 0.5:
        return False, "STALE PRICES: {}/{} tickers have old prices".format(
            stale_count, len(prices))

    return True, "Price data fresh"


# ═══════════════════════════════════════════════════════════════════════
# 3. RISK MANAGEMENT VALIDATORS
# ═══════════════════════════════════════════════════════════════════════

def validate_trade(symbol: str, direction: str, conviction: float,
                   position_size_pct: float, current_positions: int) -> Tuple[bool, str]:
    """
    Validate a proposed trade against risk limits.

    Returns:
        (is_valid, reason)
    """
    # Check circuit breaker first
    safe, reason = check_circuit_breaker()
    if not safe:
        return False, reason

    # Conviction threshold
    if conviction < MIN_CONVICTION_FOR_TRADE:
        return False, "CONVICTION TOO LOW: {:.2f} < {:.2f} minimum".format(
            conviction, MIN_CONVICTION_FOR_TRADE)

    # Position size limit
    if position_size_pct > MAX_SINGLE_POSITION_PCT:
        return False, "POSITION TOO LARGE: {:.1f}% > {:.1f}% max".format(
            position_size_pct, MAX_SINGLE_POSITION_PCT)

    # Max open positions
    if current_positions >= MAX_OPEN_POSITIONS:
        return False, "TOO MANY POSITIONS: {} open >= {} max".format(
            current_positions, MAX_OPEN_POSITIONS)

    # Data freshness
    fresh, reason, _ = check_data_freshness()
    if not fresh:
        return False, reason

    return True, "Trade validated"


def calculate_position_size(conviction: float, account_value: float,
                            volatility_pct: float = 5.0) -> float:
    """
    Calculate position size using Kelly-inspired sizing.

    Higher conviction + lower volatility = bigger position.
    Capped at MAX_SINGLE_POSITION_PCT of account.

    Returns dollar amount to risk.
    """
    # Kelly fraction: f = (p * b - q) / b
    # where p = win probability (use conviction as proxy)
    # b = win/loss ratio (assume 2:1 for options)
    # q = 1 - p
    p = min(conviction, 0.80)  # Cap at 80% to avoid overconfidence
    b = 2.0  # Typical options risk/reward
    q = 1 - p
    kelly = max((p * b - q) / b, 0)

    # Use half-Kelly for safety
    half_kelly = kelly * 0.5

    # Scale by inverse volatility (high vol = smaller position)
    vol_scale = max(3.0 / volatility_pct, 0.3)

    position_pct = min(half_kelly * vol_scale * 100, MAX_SINGLE_POSITION_PCT)
    position_dollars = account_value * (position_pct / 100)

    return round(position_dollars, 2)


# ═══════════════════════════════════════════════════════════════════════
# 4. ACCURACY WATCHDOG
# ═══════════════════════════════════════════════════════════════════════

def get_accuracy_report() -> Dict:
    """Generate accuracy report for monitoring."""
    state = _load_state()
    results = state.get("prediction_results", [])

    if not results:
        return {"status": "NO DATA", "total": 0}

    # Overall
    total = len(results)
    correct = sum(1 for r in results if r.get("correct", False))

    # Last 5 days
    cutoff_5d = (datetime.now() - timedelta(days=5)).isoformat()
    recent_5d = [r for r in results if r.get("timestamp", "") >= cutoff_5d]
    correct_5d = sum(1 for r in recent_5d if r.get("correct", False))

    # By direction
    bull_results = [r for r in results if r.get("direction") == "BULLISH"]
    bear_results = [r for r in results if r.get("direction") == "BEARISH"]
    bull_correct = sum(1 for r in bull_results if r.get("correct", False))
    bear_correct = sum(1 for r in bear_results if r.get("correct", False))

    # Average conviction of winners vs losers
    winners = [r for r in results if r.get("correct", False)]
    losers = [r for r in results if not r.get("correct", False) and abs(r.get("actual_move_pct", 0)) > 1]
    avg_winner_conv = sum(r["conviction"] for r in winners) / len(winners) if winners else 0
    avg_loser_conv = sum(r["conviction"] for r in losers) / len(losers) if losers else 0

    report = {
        "status": "OK" if not results or correct / total >= ACCURACY_FLOOR else "WARNING",
        "total": total,
        "correct": correct,
        "accuracy_pct": round(correct / total * 100, 1) if total else 0,
        "last_5d_total": len(recent_5d),
        "last_5d_correct": correct_5d,
        "last_5d_accuracy_pct": round(correct_5d / len(recent_5d) * 100, 1) if recent_5d else 0,
        "bullish_accuracy_pct": round(bull_correct / len(bull_results) * 100, 1) if bull_results else 0,
        "bearish_accuracy_pct": round(bear_correct / len(bear_results) * 100, 1) if bear_results else 0,
        "avg_winner_conviction": round(avg_winner_conv, 3),
        "avg_loser_conviction": round(avg_loser_conv, 3),
        "consecutive_losses": state.get("consecutive_losses", 0),
        "daily_pnl_pct": state.get("daily_pnl_pct", 0),
        "circuit_breaker_active": state.get("manual_kill_switch", False),
    }

    return report


def check_algo_decay() -> Tuple[bool, str]:
    """
    Detect if the algorithm is decaying (getting worse over time).

    Compares last 5 days accuracy vs last 20 days.
    If recent accuracy is significantly worse, flag it.
    """
    state = _load_state()
    results = state.get("prediction_results", [])

    cutoff_5d = (datetime.now() - timedelta(days=5)).isoformat()
    cutoff_20d = (datetime.now() - timedelta(days=20)).isoformat()

    recent = [r for r in results if r.get("timestamp", "") >= cutoff_5d]
    older = [r for r in results if cutoff_20d <= r.get("timestamp", "") < cutoff_5d]

    if len(recent) < 5 or len(older) < 10:
        return True, "Not enough data to detect decay"

    recent_acc = sum(1 for r in recent if r.get("correct")) / len(recent)
    older_acc = sum(1 for r in older if r.get("correct")) / len(older)

    if recent_acc < older_acc - 0.20:
        return False, (
            "ALGO DECAY DETECTED: Recent accuracy {:.0f}% vs historical {:.0f}% "
            "(dropped {:.0f}pp). Consider retuning weights."
        ).format(recent_acc * 100, older_acc * 100, (older_acc - recent_acc) * 100)

    return True, "No decay detected"


# ═══════════════════════════════════════════════════════════════════════
# 5. PRE-FLIGHT CHECK — Run before every scan
# ═══════════════════════════════════════════════════════════════════════

def pre_flight_check() -> Tuple[bool, List[str]]:
    """
    Run ALL safeguard checks before a scan/trade session.

    Returns:
        (all_passed, list_of_warnings)
    """
    warnings = []
    all_passed = True

    # Circuit breaker
    safe, reason = check_circuit_breaker()
    if not safe:
        warnings.append("CRITICAL: " + reason)
        all_passed = False
    else:
        logger.info("  ✅ Circuit breaker: OK")

    # Data freshness
    fresh, reason, details = check_data_freshness()
    if not fresh:
        warnings.append("WARNING: " + reason)
        # Data staleness is a WARNING, not a hard stop for scanning
        # (only for trading)
    else:
        logger.info("  ✅ Data freshness: OK")

    # Algo decay
    no_decay, reason = check_algo_decay()
    if not no_decay:
        warnings.append("WARNING: " + reason)
    else:
        logger.info("  ✅ Algo health: OK")

    # Accuracy report
    report = get_accuracy_report()
    if report["total"] > 0:
        logger.info(
            "  📊 Accuracy: {:.0f}% overall ({}/{}), {:.0f}% last 5d, "
            "consec losses: {}".format(
                report["accuracy_pct"], report["correct"], report["total"],
                report["last_5d_accuracy_pct"], report["consecutive_losses"])
        )

    if warnings:
        for w in warnings:
            logger.warning("  ⚠️  %s", w)

    if not all_passed:
        try:
            from monitoring.health_alerts import alert_safeguard_failure
            alert_safeguard_failure(warnings)
        except Exception:
            pass

    return all_passed, warnings


# ═══════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════════════════

def _load_state() -> Dict:
    try:
        if _SAFEGUARD_STATE.exists():
            with open(_SAFEGUARD_STATE) as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        logger.warning("Safeguard state file corrupted, starting fresh")
    return {}


def _save_state(state: Dict):
    try:
        _SAFEGUARD_STATE.parent.mkdir(parents=True, exist_ok=True)
        with open(_SAFEGUARD_STATE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except IOError as e:
        logger.error("Failed to save safeguard state: %s", e)
