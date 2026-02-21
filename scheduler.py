"""
Meta Engine Scheduler
======================
Runs the Meta Engine twice daily on every trading day:
  - 9:35 AM ET  (morning — real market data + gap detection)
  - 3:15 PM ET  (afternoon — intraday action + power hour setup)

CRITICAL DESIGN: Each scheduled run launches meta_engine.py as a
SUBPROCESS (not a Python import) so that code changes on disk are
always picked up immediately — no manual restart required.

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
from datetime import datetime, date, timedelta
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

# Git hash at scheduler startup — used to detect code changes
_STARTUP_GIT_HASH: str = "unknown"

# Path to venv Python interpreter
VENV_PYTHON = str(META_DIR / "venv" / "bin" / "python3")


# ═══════════════════════════════════════════════════════
# Code-Freshness Safeguards
# ═══════════════════════════════════════════════════════

def _get_git_hash() -> str:
    """Get current git commit hash (short) from disk."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(META_DIR),
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def _check_and_restart_if_code_changed():
    """
    Compare current git hash on disk to what was loaded at startup.
    If code has been updated (git commit/pull), self-restart so the
    scheduler itself picks up any changes to scheduler.py.
    launchd KeepAlive:true will immediately restart us.
    """
    global _STARTUP_GIT_HASH
    if _STARTUP_GIT_HASH == "unknown":
        return  # Can't compare
    current_hash = _get_git_hash()
    if current_hash != "unknown" and current_hash != _STARTUP_GIT_HASH:
        logger.warning(
            f"🔄 CODE CHANGE DETECTED — Startup: {_STARTUP_GIT_HASH} → "
            f"Disk: {current_hash}"
        )
        logger.warning(
            "   Self-restarting scheduler to load fresh code "
            "(launchd will restart automatically)..."
        )
        # Clean exit — launchd KeepAlive will restart us with fresh code
        if PID_FILE.exists():
            PID_FILE.unlink()
        os._exit(0)  # Hard exit to ensure clean restart


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
    """
    Run meta_engine.py as a SUBPROCESS to always use the latest code.
    
    WHY SUBPROCESS (not Python import):
      Python caches module imports globally. If meta_engine.py or any
      adapter/module it uses is updated (e.g. via git push/commit),
      a long-running scheduler process would still execute the OLD code
      from memory. This was the root cause of the 2026-02-17 AM failure.
      
      Running as a subprocess starts a fresh Python process each time,
      guaranteeing the latest on-disk code is always used.
    """
    now_str = datetime.now(EST).strftime('%I:%M:%S %p ET')
    label = f" [{session_label}]" if session_label else ""
    logger.info("=" * 60)
    logger.info(f"⏰ Scheduled run triggered at {now_str}{label}")
    logger.info("=" * 60)

    # Log code version for audit trail
    git_hash = _get_git_hash()
    logger.info(f"   Code version (on disk): {git_hash}")

    # CRITICAL FIX (Feb 18, 2026): Do NOT restart before running.
    # The old code called _check_and_restart_if_code_changed() HERE,
    # which killed the process at 09:35:00 before the scan executed.
    # Since we run meta_engine as a subprocess, it already uses the
    # latest on-disk code. We defer the restart to AFTER the run.
    _pending_restart = False
    global _STARTUP_GIT_HASH
    if _STARTUP_GIT_HASH != "unknown":
        current_hash = _get_git_hash()
        if current_hash != "unknown" and current_hash != _STARTUP_GIT_HASH:
            logger.info(
                f"   Code changed ({_STARTUP_GIT_HASH} → {current_hash}) — "
                f"will restart AFTER this run completes"
            )
            _pending_restart = True

    meta_script = str(META_DIR / "meta_engine.py")

    try:
        logger.info(f"   Launching subprocess: {VENV_PYTHON} {meta_script}")

        proc = subprocess.run(
            [VENV_PYTHON, meta_script],
            cwd=str(META_DIR),
            capture_output=True,
            text=True,
            timeout=900,  # 15 minute timeout — generous for API calls
        )

        if proc.returncode == 0:
            stdout = proc.stdout or ""
            if "META ENGINE — COMPLETED" in stdout:
                logger.info("   ✅ Run completed successfully")
                # Extract key metrics from the run's stdout
                for line in stdout.split("\n"):
                    stripped = line.strip()
                    if any(k in stripped for k in [
                        "Puts picks:", "Moonshot picks:", "Email:",
                        "Telegram:", "X/Twitter:", "Deep Options:",
                        "Trading:", "5x Potential:",
                    ]):
                        logger.info(f"      {stripped}")
            elif "Not a trading day" in stdout:
                logger.info("   📅 Skipped: Not a trading day")
            elif "concurrent_run_blocked" in stdout:
                logger.warning("   ⚠️ Skipped: Another run already in progress")
            else:
                logger.info("   ✅ Run completed (exit 0)")
        else:
            logger.error(f"   ❌ Run FAILED (exit code {proc.returncode})")
            stderr_tail = ""
            if proc.stderr:
                lines = proc.stderr.strip().split("\n")[-20:]
                stderr_tail = "\n".join(lines)
                for line in lines:
                    logger.error(f"   STDERR: {line}")
            if proc.stdout:
                for line in proc.stdout.strip().split("\n")[-10:]:
                    logger.error(f"   STDOUT: {line}")
            try:
                from monitoring.health_alerts import alert_scheduler_failure
                alert_scheduler_failure(
                    session_label or "Unknown",
                    f"Exit code {proc.returncode}\n{stderr_tail[:800]}"
                )
            except Exception:
                pass

    except subprocess.TimeoutExpired:
        err_msg = "Run TIMED OUT after 15 minutes — check for hung API calls"
        logger.error(f"   ❌ {err_msg}")
        try:
            from monitoring.health_alerts import alert_scheduler_failure
            alert_scheduler_failure(session_label or "Unknown", err_msg)
        except Exception:
            pass
    except FileNotFoundError:
        err_msg = f"Python interpreter not found: {VENV_PYTHON} — check venv exists"
        logger.error(f"   ❌ {err_msg}")
        try:
            from monitoring.health_alerts import alert_scheduler_failure
            alert_scheduler_failure(session_label or "Unknown", err_msg)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"   ❌ Scheduled run FAILED: {e}", exc_info=True)
        try:
            from monitoring.health_alerts import alert_scheduler_failure
            alert_scheduler_failure(session_label or "Unknown", str(e))
        except Exception:
            pass

    # Deferred restart: now that the scan is done, restart if code changed
    if _pending_restart:
        logger.warning(
            "🔄 Scan complete — now restarting scheduler for fresh code..."
        )
        if PID_FILE.exists():
            PID_FILE.unlink()
        os._exit(0)


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
    Called at 9:15 AM ET by APScheduler; keeps awake for ~7 hours
    (covers 9:35 AM and 3:15 PM runs, plus market close at 4 PM).
    Uses macOS `caffeinate` — no sudo needed.
    """
    global _caffeinate_proc
    # Kill any previous caffeinate if still running
    if _caffeinate_proc and _caffeinate_proc.poll() is None:
        _caffeinate_proc.terminate()
        _caffeinate_proc = None

    duration = 25200  # 7 hours in seconds (9:15 AM → 4:15 PM)
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
        try:
            from monitoring.health_alerts import alert_trading_error
            alert_trading_error("position_check", str(e))
        except Exception:
            pass


def _auto_check_winners():
    """
    Automatically check for profitable picks (>50%) from recent scans and post winner updates.
    
    Institutional-grade logic:
    - Checks scans from last 24 hours
    - Only posts if picks are >50% profitable
    - Tracks which scans have already been posted (avoids duplicates)
    - Runs every 30-45 minutes during market hours (9:30 AM - 4:00 PM ET)
    - Skips if market is closed or outside trading hours
    """
    now_et = datetime.now(EST)
    now_str = now_et.strftime('%I:%M:%S %p ET')
    
    # Only run during market hours (9:30 AM - 4:00 PM ET) on trading days
    if now_et.weekday() >= 5:  # Weekend
        return
    if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
        return  # Before 9:30 AM
    if now_et.hour >= 16:
        return  # After 4:00 PM
    
    logger.info(f"🏆 Auto winner check at {now_str}")
    
    try:
        from notifications.x_poster import check_and_post_milestones
        
        # Check all open trades for milestone crossings (50%, 100%, 150%, 200%, 300%, 400%, 500%)
        # This is more efficient than checking per-scan - it monitors all positions continuously
        stats = check_and_post_milestones(min_profit_pct=50.0)
        
        if stats["milestones_posted"] > 0:
            logger.info(f"   ✅ Posted {stats['milestones_posted']} milestone updates "
                       f"({stats['trades_with_milestones']} trades, {stats['checked']} checked)")
        else:
            logger.debug(f"   No new milestones reached ({stats['checked']} trades checked)")
            
    except Exception as e:
        logger.error(f"   Auto winner check failed: {e}", exc_info=True)


def start_scheduler():
    """
    Start the APScheduler daemon that runs Meta Engine twice daily:
      - 9:35 AM ET  (morning scan — real market data + gap detection)
      - 3:15 PM ET  (afternoon scan — full intraday data + power hour setup)
    Also starts the trading dashboard on port 5050.
    
    IMPORTANT: Each run launches meta_engine.py as a SUBPROCESS so
    code changes are always picked up without manual restart.
    The scheduler also self-restarts if it detects its own code has changed.
    """
    global _STARTUP_GIT_HASH

    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger
    except ImportError:
        logger.error("APScheduler not installed. Run: pip install apscheduler")
        sys.exit(1)
    
    # Record git hash at startup for code-change detection
    _STARTUP_GIT_HASH = _get_git_hash()

    # Parse run times (2 daily scans)
    run_times = MetaConfig.RUN_TIMES_ET  # ["09:35", "15:15"]
    
    logger.info("=" * 60)
    logger.info("🏛️  META ENGINE SCHEDULER")
    logger.info(f"   Run times: {', '.join(run_times)} ET (every trading day)")
    logger.info(f"   PID: {os.getpid()}")
    logger.info(f"   Code version: {_STARTUP_GIT_HASH}")
    logger.info(f"   Python: {VENV_PYTHON}")
    logger.info(f"   Mode: SUBPROCESS (always fresh code)")
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
    
    # ── Job 0: Keep Mac awake during market hours (9:15 AM) ──
    scheduler.add_job(
        _keep_awake_market_hours,
        trigger=CronTrigger(
            hour=9, minute=15,
            day_of_week="mon-fri",
            timezone=EST,
        ),
        id="caffeinate_market_hours",
        name="Keep Mac awake 9:15 AM - 4:15 PM ET",
        misfire_grace_time=3600,
    )
    logger.info("  ✅ Job scheduled: caffeinate at 9:15 AM ET, Mon-Fri")

    # ── Jobs 1-2: Meta Engine runs (scan + report + trade) ──
    # Labels map index → descriptive name for logging and trade DB
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

    # ── Job 4: Mid-day position check (12:00 PM) ──
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

    # ── Job 5: Automatic winner check (every 30 minutes during market hours) ──
    # Institutional-grade: Checks for profitable picks >50% and posts winner updates
    # Runs every 30 minutes from 9:30 AM - 4:00 PM ET on trading days
    # The function itself checks if it's market hours, so we can schedule it every 30 min
    scheduler.add_job(
        _auto_check_winners,
        trigger=IntervalTrigger(
            minutes=30,
            timezone=EST,
        ),
        id="auto_winner_check",
        name="Auto Winner Check (every 30 min, 9:30 AM - 4:00 PM ET)",
        misfire_grace_time=1800,  # 30 min grace period
    )
    logger.info("  ✅ Job scheduled: Auto winner check every 30 min (9:30 AM - 4:00 PM ET), Mon-Fri")

    # ── Job 6: Live Backtest Runner (every 30 min, market hours) ──
    def _run_live_backtest():
        try:
            now_et = datetime.now(EST)
            if now_et.weekday() >= 5:
                return
            t = now_et.hour * 60 + now_et.minute
            if not (570 <= t <= 960):
                return
            from monitoring.live_backtest_runner import run_checkpoint
            run_checkpoint()
        except Exception as e:
            logger.warning(f"Live backtest checkpoint failed: {e}")

    scheduler.add_job(
        _run_live_backtest,
        trigger=IntervalTrigger(
            minutes=30,
            timezone=EST,
        ),
        id="live_backtest_runner",
        name="Live Backtest Runner (every 30 min, market hours)",
        misfire_grace_time=1800,
    )
    logger.info("  ✅ Job scheduled: Live backtest runner every 30 min (market hours)")

    # ── Job 7: Code-freshness watchdog (every 15 min) ──
    # If code changes are detected (git commit/push), the scheduler
    # self-restarts so it always runs the latest version.
    # launchd KeepAlive:true ensures immediate restart.
    scheduler.add_job(
        _check_and_restart_if_code_changed,
        trigger=IntervalTrigger(
            minutes=15,
            timezone=EST,
        ),
        id="code_freshness_watchdog",
        name="Code Freshness Watchdog (every 15 min)",
        misfire_grace_time=900,
    )
    logger.info("  ✅ Job scheduled: Code freshness watchdog every 15 min")

    # If Mac is currently awake and it's market hours, start caffeinate now
    now_et = datetime.now(EST)
    if now_et.weekday() < 5 and 9 <= now_et.hour < 16:
        _keep_awake_market_hours()

    # ── Missed-run recovery ──
    # If the scheduler (re)started within the grace window after a
    # scheduled run time, fire that run immediately. This prevents
    # the scenario where a code-change restart at 09:35 swallows
    # the morning run entirely (root cause of Feb 18 outage).
    if now_et.weekday() < 5:
        _run_times = [
            (9, 35, 10, 0, "Morning"),
            (15, 15, 15, 45, "Afternoon"),
        ]
        for start_h, start_m, end_h, end_m, label in _run_times:
            run_start = now_et.replace(hour=start_h, minute=start_m, second=0)
            run_end = now_et.replace(hour=end_h, minute=end_m, second=0)
            if run_start <= now_et <= run_end:
                # Check if today's output file exists (i.e. run already completed)
                today_str = now_et.strftime('%Y%m%d')
                puts_file = META_DIR / "output" / f"puts_top10_{today_str}.json"
                moon_file = META_DIR / "output" / f"moonshot_top10_{today_str}.json"
                if not puts_file.exists() and not moon_file.exists():
                    logger.warning(
                        f"⚠️ MISSED RUN RECOVERY: {label} run was missed "
                        f"(no output files for {today_str}). Running NOW..."
                    )
                    _scheduled_run(session_label="AM" if label == "Morning" else "PM")
                else:
                    logger.info(
                        f"  ✅ {label} run already completed today (output files exist)"
                    )

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
        logger.info("Running Meta Engine NOW (subprocess, fresh code)...")
        meta_script = str(META_DIR / "meta_engine.py")
        proc = subprocess.run(
            [VENV_PYTHON, meta_script, "--force"],
            cwd=str(META_DIR),
        )
        sys.exit(proc.returncode)
