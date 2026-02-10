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

set -euo pipefail

# Paths
META_DIR="/Users/chavala/Meta Engine"
VENV_PYTHON="${META_DIR}/venv/bin/python3"
SCHEDULER="${META_DIR}/scheduler.py"
LOG_DIR="${META_DIR}/logs"

# Ensure log dir exists
mkdir -p "${LOG_DIR}"

# Change to the Meta Engine directory (so .env load works)
cd "${META_DIR}"

# Load environment variables from Meta Engine .env
if [ -f "${META_DIR}/.env" ]; then
    export $(grep -v '^#' "${META_DIR}/.env" | grep -v '^\s*$' | xargs)
fi

# Also load PutsEngine .env for live scan API keys (POLYGON, ALPACA, UW)
# These are needed for the PutsEngine live scan to work
PUTSENGINE_ENV="/Users/chavala/PutsEngine/.env"
if [ -f "${PUTSENGINE_ENV}" ]; then
    export $(grep -v '^#' "${PUTSENGINE_ENV}" | grep -v '^\s*$' | xargs)
fi

# Log startup
echo "$(date '+%Y-%m-%d %H:%M:%S') | Scheduler starting (PID $$)" >> "${LOG_DIR}/scheduler_launchd.log"

# Run the scheduler — this blocks forever (APScheduler BlockingScheduler)
exec "${VENV_PYTHON}" "${SCHEDULER}" start
