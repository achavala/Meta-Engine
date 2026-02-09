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


def start_scheduler():
    """
    Start the APScheduler daemon that runs Meta Engine twice daily:
      - 9:35 AM ET  (morning scan)
      - 3:15 PM ET  (afternoon scan)
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
    
    # Write PID
    _write_pid()
    
    # Register cleanup
    signal.signal(signal.SIGTERM, _cleanup_pid)
    signal.signal(signal.SIGINT, _cleanup_pid)
    
    # Create scheduler
    scheduler = BlockingScheduler(timezone=EST)
    
    # Schedule each run time (Mon-Fri)
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
            misfire_grace_time=300,  # 5 min grace period
        )
        logger.info(f"  ✅ Job scheduled: {label} at {run_time} ET, Mon-Fri")
    
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
