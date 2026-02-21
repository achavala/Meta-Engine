"""
Health Alert System — Telegram notifications for system failures.

Sends throttled Telegram alerts when:
  - Data sources are stale, missing, or corrupted (UW, Polygon, etc.)
  - API keys are missing or APIs are unreachable
  - Pipeline crashes or safeguard checks fail
  - Scheduled runs fail or time out
  - Trading execution errors occur

Throttling prevents alert spam: the same alert category won't fire
more than once per THROTTLE_MINUTES window.

Usage (from any module):
    from monitoring.health_alerts import send_health_alert, AlertLevel
    send_health_alert(AlertLevel.CRITICAL, "pipeline_crash", "Meta Engine crashed: ...")
"""

import json
import logging
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger("HealthAlerts")

_META_DIR = Path.home() / "Meta Engine"
_THROTTLE_FILE = _META_DIR / "output" / ".health_alert_throttle.json"

THROTTLE_MINUTES = 60


class AlertLevel(Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


_LEVEL_EMOJI = {
    AlertLevel.CRITICAL: "\U0001F6A8",  # 🚨
    AlertLevel.WARNING: "\u26A0\uFE0F",  # ⚠️
    AlertLevel.INFO: "\u2139\uFE0F",     # ℹ️
}


def _load_throttle_state() -> dict:
    try:
        if _THROTTLE_FILE.exists():
            with open(_THROTTLE_FILE) as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {}


def _save_throttle_state(state: dict):
    try:
        _THROTTLE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_THROTTLE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except IOError as e:
        logger.debug("Failed to save throttle state: %s", e)


def _is_throttled(category: str) -> bool:
    """Check if this alert category was sent recently."""
    state = _load_throttle_state()
    last_sent = state.get(category, 0)
    return (time.time() - last_sent) < (THROTTLE_MINUTES * 60)


def _mark_sent(category: str):
    state = _load_throttle_state()
    state[category] = time.time()
    _save_throttle_state(state)


def send_health_alert(
    level: AlertLevel,
    category: str,
    message: str,
    details: Optional[str] = None,
    force: bool = False,
) -> bool:
    """
    Send a health alert via Telegram.

    Args:
        level: AlertLevel.CRITICAL / WARNING / INFO
        category: Throttle key (e.g. "data_stale", "pipeline_crash", "api_down")
        message: Short description of the issue
        details: Optional extra context (will be shown in smaller text)
        force: Skip throttle check (for truly critical one-off events)

    Returns:
        True if sent, False if throttled or send failed
    """
    if not force and _is_throttled(category):
        logger.debug("Health alert throttled: %s", category)
        return False

    try:
        from config import MetaConfig
        bot_token = MetaConfig.TELEGRAM_BOT_TOKEN
        chat_id = MetaConfig.TELEGRAM_CHAT_ID

        if not bot_token or not chat_id:
            logger.warning("Health alerts: Telegram not configured")
            return False

        from notifications.telegram_sender import send_telegram_message

        emoji = _LEVEL_EMOJI.get(level, "")
        now_str = datetime.now().strftime("%I:%M %p ET  %b %d")

        text_parts = [
            f"{emoji} <b>META ENGINE {level.value}</b>",
            "",
            f"<b>{message}</b>",
        ]

        if details:
            text_parts.append("")
            text_parts.append(f"<pre>{details[:2000]}</pre>")

        text_parts.extend([
            "",
            f"<i>{now_str}</i>",
        ])

        full_text = "\n".join(text_parts)

        sent = send_telegram_message(full_text, bot_token, chat_id, parse_mode="HTML")

        if sent:
            _mark_sent(category)
            logger.info("Health alert sent: [%s] %s", level.value, category)
        else:
            logger.warning("Health alert send failed: [%s] %s", level.value, category)

        return sent

    except Exception as e:
        logger.error("Health alert system error: %s", e)
        return False


# ═══════════════════════════════════════════════════════════════════════
# PRE-BUILT ALERT HELPERS — Called from integration points
# ═══════════════════════════════════════════════════════════════════════

def alert_data_issue(issues_summary: str, details_dict: Optional[dict] = None):
    """Alert when UW/data source files are stale, missing, or corrupted."""
    detail_lines = []
    if details_dict:
        for name, info in details_dict.items():
            status = info.get("status", "?")
            age = info.get("age_hours", -1)
            if status != "OK":
                detail_lines.append(f"  {name}: {status} ({age:.1f}h old)" if age >= 0
                                    else f"  {name}: {status}")
    details = "\n".join(detail_lines) if detail_lines else None
    send_health_alert(AlertLevel.WARNING, "data_freshness", issues_summary, details)


def alert_api_down(api_name: str, error: str):
    """Alert when an API (Polygon, Alpaca, UW, etc.) is unreachable or returns errors."""
    send_health_alert(
        AlertLevel.CRITICAL,
        f"api_down_{api_name.lower()}",
        f"{api_name} API is unavailable",
        f"Error: {error}",
    )


def alert_pipeline_crash(stage: str, error: str):
    """Alert when the Meta Engine pipeline crashes."""
    send_health_alert(
        AlertLevel.CRITICAL,
        "pipeline_crash",
        f"Pipeline crashed at: {stage}",
        error[:1500],
        force=True,
    )


def alert_safeguard_failure(warnings: list):
    """Alert when pre-flight safeguard checks fail."""
    details = "\n".join(f"  - {w}" for w in warnings)
    send_health_alert(
        AlertLevel.CRITICAL,
        "safeguard_failure",
        "Pre-flight safeguard check FAILED",
        details,
    )


def alert_scheduler_failure(session: str, error: str):
    """Alert when a scheduled run fails or times out."""
    send_health_alert(
        AlertLevel.CRITICAL,
        f"scheduler_{session.lower()}",
        f"Scheduled {session} run FAILED",
        error[:1500],
        force=True,
    )


def alert_trading_error(symbol: str, error: str):
    """Alert when a trade execution fails."""
    send_health_alert(
        AlertLevel.WARNING,
        "trading_error",
        f"Trade execution failed: {symbol}",
        error[:1000],
    )


def alert_api_key_missing(key_name: str):
    """Alert when a required API key is not configured."""
    send_health_alert(
        AlertLevel.CRITICAL,
        f"api_key_missing_{key_name.lower()}",
        f"API key not configured: {key_name}",
        "Check your .env file and ensure the key is set.",
    )


def check_api_keys_and_alert():
    """
    Validate that all critical API keys are present and alert on any missing ones.
    Called during pre-flight or startup.
    """
    try:
        from config import MetaConfig
    except ImportError:
        return

    checks = {
        "POLYGON_API_KEY": MetaConfig.POLYGON_API_KEY,
        "UNUSUAL_WHALES_API_KEY": MetaConfig.UNUSUAL_WHALES_API_KEY,
        "ALPACA_API_KEY": MetaConfig.ALPACA_API_KEY,
        "ALPACA_SECRET_KEY": MetaConfig.ALPACA_SECRET_KEY,
    }

    missing = [name for name, val in checks.items() if not val]
    if missing:
        send_health_alert(
            AlertLevel.CRITICAL,
            "api_keys_missing",
            f"{len(missing)} API key(s) not configured",
            "Missing:\n" + "\n".join(f"  - {k}" for k in missing),
        )
