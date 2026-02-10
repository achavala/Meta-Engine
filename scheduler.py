"""
Meta Engine Scheduler
======================
Runs the Meta Engine twice daily on every trading day:
  - 9:35 AM ET  (morning — captures pre-market + opening signals)
  - 3:15 PM ET  (afternoon — captures intraday action + power hour setup)

Methods:
1. APScheduler (preferred) — background daemon
2. macOS launchd — system-level auto-start
3. Manual — run once via command line

Usage:
    # Start scheduler daemon (runs at 9:35 AM + 3:15 PM ET):
    python scheduler.py
    
    # Run once immediately:
    python meta_engine.py --force
    
    # Run once (only on trading days):
    python meta_engine.py
"""

import sys
import os
import signal
import logging
import subprocess
from datetime import datetime, date
from pathlib import Path

import pytz

# Ensure Meta Engine path is in sys.path
META_DIR = Path(__file__).parent
if str(META_DIR) not in sys.path:
    sys.path.insert(0, str(META_DIR))

from config import MetaConfig

# Logging
LOG_DIR = Path(MetaConfig.LOGS_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "scheduler.log", mode="a"),
    ],
)
logger = logging.getLogger("MetaScheduler")

EST = pytz.timezone("US/Eastern")

# PID file for daemon management
PID_FILE = META_DIR / "meta_scheduler.pid"


def _write_pid():
    """Write current process PID to file."""
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def _cleanup_pid(*args):
    """Remove PID file on exit."""
    if PID_FILE.exists():
        PID_FILE.unlink()
    logger.info("Scheduler stopped.")
    sys.exit(0)


def _scheduled_run(session_label: str = ""):
    """Wrapper called by APScheduler at each scheduled time."""
    now_str = datetime.now(EST).strftime('%I:%M:%S %p ET')
    label = f" [{session_label}]" if session_label else ""
    logger.info("=" * 60)
    logger.info(f"⏰ Scheduled run triggered at {now_str}{label}")
    logger.info("=" * 60)
    
    try:
        from meta_engine import run_meta_engine
        result = run_meta_engine(force=False)
        run_status = result.get("status", "unknown")
        logger.info(f"Run completed with status: {run_status}")
    except Exception as e:
        logger.error(f"Scheduled run FAILED: {e}", exc_info=True)


def _start_dashboard_thread():
    """Start the Flask trading dashboard in a background thread (port 5050)."""
    try:
        import threading
        from trading.dashboard import start_dashboard
        t = threading.Thread(target=start_dashboard, daemon=True, name="dashboard")
        t.start()
        logger.info("  🖥️  Flask dashboard started on http://localhost:5050")
    except Exception as e:
        logger.warning(f"  Flask dashboard skipped: {e} (non-critical)")


def _start_streamlit_dashboard():
    """Start the Streamlit trading dashboard as a subprocess (port 8511)."""
    try:
        import socket
        # Quick check — skip if port already occupied (avoids crash-loop)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", 8511)) == 0:
                logger.info("  🖥️  Streamlit dashboard already running on :8511 — skipping")
                return

        venv_streamlit = str(META_DIR / "venv" / "bin" / "streamlit")
        app_path = str(META_DIR / "trading" / "streamlit_dashboard.py")
        proc = subprocess.Popen(
            [
                venv_streamlit, "run", app_path,
                "--server.port", "8511",
                "--server.headless", "true",
                "--server.address", "0.0.0.0",
                "--browser.gatherUsageStats", "false",
                "--theme.base", "dark",
            ],
            cwd=str(META_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"  🖥️  Streamlit dashboard started on http://localhost:8511 (PID {proc.pid})")
    except Exception as e:
        logger.warning(f"  Streamlit dashboard skipped: {e} (non-critical)")


# ═══════════════════════════════════════════════════════
# Power management — keep Mac awake during market hours
# ═══════════════════════════════════════════════════════
_caffeinate_proc: subprocess.Popen = None  # type: ignore


def _keep_awake_market_hours():
    """
    Prevent the Mac from idle-sleeping during market hours.
    Called at 9:20 AM ET by APScheduler; keeps awake for ~7 hours
    (covers both 9:35 AM and 3:15 PM runs, plus market close at 4 PM).
    Uses macOS `caffeinate` — no sudo needed.
    """
    global _caffeinate_proc
    # Kill any previous caffeinate if still running
    if _caffeinate_proc and _caffeinate_proc.poll() is None:
        _caffeinate_proc.terminate()
        _caffeinate_proc = None

    duration = 25200  # 7 hours in seconds (9:20 AM → 4:20 PM)
    try:
        _caffeinate_proc = subprocess.Popen(
            ["caffeinate", "-dims", "-t", str(duration)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"  ☕ caffeinate started (7h) — Mac stays awake until market close "
                     f"(PID {_caffeinate_proc.pid})")
    except Exception as e:
        logger.warning(f"  caffeinate failed: {e} (Mac may sleep during trading hours)")


def _midday_position_check():
    """
    Mid-day check of open positions (take-profit / stop-loss / time-stop).
    Runs at 12:00 PM ET to catch intraday moves between the AM and PM scans.
    """
    now_str = datetime.now(EST).strftime('%I:%M:%S %p ET')
    logger.info(f"📋 Mid-day position check at {now_str}")
    try:
        from trading.executor import check_and_manage_positions
        result = check_and_manage_positions()
        logger.info(f"   Positions checked: {result.get('checked', 0)} | "
                     f"Closed: {result.get('closed', 0)}")
    except Exception as e:
        logger.error(f"   Position check failed: {e}")


def start_scheduler():
    """
    Start the APScheduler daemon that runs Meta Engine twice daily:
      - 9:35 AM ET  (morning scan)
      - 3:15 PM ET  (afternoon scan)
    Also starts the trading dashboard on port 5050.
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.error("APScheduler not installed. Run: pip install apscheduler")
        sys.exit(1)
    
    # Parse both run times
    run_times = MetaConfig.RUN_TIMES_ET  # ["09:35", "15:15"]
    
    logger.info("=" * 60)
    logger.info("🏛️  META ENGINE SCHEDULER")
    logger.info(f"   Run times: {', '.join(run_times)} ET (every trading day)")
    logger.info(f"   PID: {os.getpid()}")
    logger.info("=" * 60)

    # Start dashboards in background
    _start_dashboard_thread()         # Flask on :5050
    _start_streamlit_dashboard()      # Streamlit on :8511
    
    # Write PID
    _write_pid()
    
    # Register cleanup
    signal.signal(signal.SIGTERM, _cleanup_pid)
    signal.signal(signal.SIGINT, _cleanup_pid)
    
    # Create scheduler
    scheduler = BlockingScheduler(timezone=EST)
    
    # ── Job 0: Keep Mac awake during market hours (9:20 AM) ──
    scheduler.add_job(
        _keep_awake_market_hours,
        trigger=CronTrigger(
            hour=9, minute=20,
            day_of_week="mon-fri",
            timezone=EST,
        ),
        id="caffeinate_market_hours",
        name="Keep Mac awake 9:20 AM - 4:20 PM ET",
        misfire_grace_time=3600,
    )
    logger.info("  ✅ Job scheduled: caffeinate at 9:20 AM ET, Mon-Fri")

    # ── Job 1 & 2: Meta Engine runs (scan + trade) ──
    session_labels = {0: "Morning", 1: "Afternoon"}
    for idx, run_time in enumerate(run_times):
        hour, minute = map(int, run_time.split(":"))
        label = session_labels.get(idx, f"Session-{idx+1}")
        
        scheduler.add_job(
            _scheduled_run,
            trigger=CronTrigger(
                hour=hour,
                minute=minute,
                day_of_week="mon-fri",
                timezone=EST,
            ),
            kwargs={"session_label": label},
            id=f"meta_engine_{label.lower()}",
            name=f"Meta Engine {label} Run ({run_time} ET)",
            misfire_grace_time=3600,  # 60 min grace period (handles sleep/wake)
        )
        logger.info(f"  ✅ Job scheduled: {label} at {run_time} ET, Mon-Fri")

    # ── Job 3: Mid-day position check (12:00 PM) ──
    scheduler.add_job(
        _midday_position_check,
        trigger=CronTrigger(
            hour=12, minute=0,
            day_of_week="mon-fri",
            timezone=EST,
        ),
        id="midday_position_check",
        name="Mid-day Position Check (12:00 PM ET)",
        misfire_grace_time=3600,
    )
    logger.info("  ✅ Job scheduled: Position check at 12:00 PM ET, Mon-Fri")

    # If Mac is currently awake and it's market hours, start caffeinate now
    now_et = datetime.now(EST)
    if now_et.weekday() < 5 and 9 <= now_et.hour < 16:
        _keep_awake_market_hours()

    logger.info("Waiting for next scheduled run time...")
    logger.info("(Press Ctrl+C to stop)")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        _cleanup_pid()


def stop_scheduler():
    """Stop the scheduler daemon by PID."""
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info(f"Scheduler stopped (PID: {pid})")
            PID_FILE.unlink()
        except ProcessLookupError:
            logger.info(f"Process {pid} not found. Cleaning up PID file.")
            PID_FILE.unlink()
    else:
        logger.info("No scheduler PID file found. Not running?")


def status():
    """Check scheduler status."""
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, 0)  # Check if process exists
            print(f"✅ Scheduler is running (PID: {pid})")
            return True
        except OSError:
            print(f"❌ Scheduler PID file exists but process {pid} is not running")
            PID_FILE.unlink()
            return False
    else:
        print("❌ Scheduler is not running")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Meta Engine Scheduler")
    parser.add_argument("action", nargs="?", default="start",
                       choices=["start", "stop", "status", "run-now"],
                       help="Action to perform")
    args = parser.parse_args()
    
    if args.action == "start":
        start_scheduler()
    elif args.action == "stop":
        stop_scheduler()
    elif args.action == "status":
        status()
    elif args.action == "run-now":
        logger.info("Running Meta Engine NOW...")
        from meta_engine import run_meta_engine
        run_meta_engine(force=True)
