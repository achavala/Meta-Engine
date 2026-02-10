#!/bin/bash
# Schedule 3 PM EST analysis - waits until 3 PM then runs the analysis
cd "/Users/chavala/Meta Engine"
source venv/bin/activate

# Calculate seconds until 3 PM EST
WAIT_SECS=$(python3 -c "
from datetime import datetime
import pytz
est = pytz.timezone('US/Eastern')
now = datetime.now(est)
target = now.replace(hour=15, minute=0, second=0, microsecond=0)
delta = (target - now).total_seconds()
print(int(max(delta, 0)))
")

echo "$(date) — Waiting $WAIT_SECS seconds until 3 PM EST..."
sleep $WAIT_SECS

echo "$(date) — 3 PM EST reached! Running analysis..."
python3 /Users/chavala/Meta\ Engine/_3pm_analysis.py 2>&1 | tee /Users/chavala/Meta\ Engine/output/3pm_analysis_log_$(date +%Y%m%d).txt

echo "$(date) — 3 PM analysis complete!"
