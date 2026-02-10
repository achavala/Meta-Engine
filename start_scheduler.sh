#!/bin/bash
# ============================================================
# Meta Engine — Scheduler Launcher
# ============================================================
# This script is invoked by macOS launchd to keep the
# APScheduler daemon running permanently (Mon-Fri, 9:35 AM +
# 3:15 PM ET). launchd will restart it if it ever exits.
#
# Do NOT run this manually — use:
#   python scheduler.py start       (foreground)
#   launchctl load ~/Library/LaunchAgents/com.metaengine.scheduler.plist
# ============================================================

# Paths
META_DIR="/Users/chavala/Meta Engine"
VENV_PYTHON="${META_DIR}/venv/bin/python3"
SCHEDULER="${META_DIR}/scheduler.py"
LOG_DIR="${META_DIR}/logs"

# Ensure log dir exists
mkdir -p "${LOG_DIR}"

# Change to the Meta Engine directory (so .env load works)
cd "${META_DIR}"

# ── Cleanup stale processes from previous run ────────────
# Kill any leftover Flask/Streamlit from a crashed predecessor
# (prevents port-conflict crash loops on restart)
lsof -ti:5050 2>/dev/null | xargs kill -9 2>/dev/null || true
lsof -ti:8511 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 1

# ── Load environment variables ───────────────────────────
# Load PutsEngine .env FIRST (base keys: POLYGON, UW, etc.)
PUTSENGINE_ENV="/Users/chavala/PutsEngine/.env"
if [ -f "${PUTSENGINE_ENV}" ]; then
    set +e  # don't exit on bad lines
    export $(grep -v '^#' "${PUTSENGINE_ENV}" | grep -v '^\s*$' | xargs) 2>/dev/null
    set -e
fi

# Load Meta Engine .env LAST so its keys take priority (ALPACA, TELEGRAM, X, etc.)
if [ -f "${META_DIR}/.env" ]; then
    set +e
    export $(grep -v '^#' "${META_DIR}/.env" | grep -v '^\s*$' | xargs) 2>/dev/null
    set -e
fi

# Log startup
echo "$(date '+%Y-%m-%d %H:%M:%S') | Scheduler starting (PID $$)" >> "${LOG_DIR}/scheduler_launchd.log"

# Run the scheduler — this blocks forever (APScheduler BlockingScheduler)
exec "${VENV_PYTHON}" "${SCHEDULER}" start
